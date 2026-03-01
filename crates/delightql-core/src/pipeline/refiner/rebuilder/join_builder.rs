use super::schema_computation::compute_join_schema;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::refined::{self, JoinType, QualifiedName};
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::refiner::analyzer::AnalyzedSegment;
use crate::pipeline::refiner::flattener;
use crate::pipeline::refiner::rebuilder::{
    apply_top_level_filters, combine_predicates_with_and, table_to_refined,
};
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;

/// Rebuild a segment containing only joins
pub(super) fn rebuild_join_segment(
    analyzed: AnalyzedSegment,
    mut op_predicates: HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    is_top_level: bool,
) -> Result<refined::RelationalExpression> {
    // Start with the first table
    if analyzed.tables.is_empty() {
        return Err(DelightQLError::parse_error("No tables in segment"));
    }

    // Validate outer join markers before processing (only at top level)
    // Inner-relation subqueries are not validated because they're not standalone queries
    log::debug!(
        "rebuild_join_segment: is_top_level={}, {} tables",
        is_top_level,
        analyzed.tables.len()
    );
    if is_top_level {
        log::debug!("Running validation...");
        validate_outer_join_markers(&analyzed, &op_predicates)?;
    } else {
        log::debug!("Skipping validation (inner context)");
    }

    let mut result = table_to_refined(&analyzed.tables[0], &mut op_predicates)?;
    let mut table_idx = 1;

    // Process operators left to right (CPR-ltr semantics)
    for (op_idx, op) in analyzed.operators.iter().enumerate() {
        match &op.kind {
            flattener::FlatOperatorKind::Join { using_columns } => {
                let (new_result, new_table_idx) = process_single_join(
                    result,
                    &analyzed,
                    table_idx,
                    op_idx,
                    using_columns,
                    &mut op_predicates,
                )?;
                result = new_result;
                table_idx = new_table_idx;
            }
            _ => {
                return Err(DelightQLError::parse_error(
                    "Non-join operator in join segment",
                ));
            }
        }
    }

    // Apply any top-level filters
    result = apply_top_level_filters(result, &mut op_predicates)?;

    Ok(result)
}

/// Process a single join operator
pub(super) fn process_single_join(
    result: refined::RelationalExpression,
    analyzed: &AnalyzedSegment,
    table_idx: usize,
    op_idx: usize,
    using_columns: &Option<Vec<String>>,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<(refined::RelationalExpression, usize)> {
    // Get the right table for this join
    if table_idx >= analyzed.tables.len() {
        return Err(DelightQLError::parse_error("Not enough tables for join"));
    }

    // EPOCH 3: Check if right table is an anonymous table with exists_mode=true
    // If so, convert JOIN to Filter with InnerExists instead
    let right_table_flat = &analyzed.tables[table_idx];
    if let Some(ref anon_data) = right_table_flat.anonymous_data {
        if anon_data.exists_mode {
            // EPOCH 3: EXISTS-mode anonymous table in JOIN position
            // Transform: users(*), +_(status @ "active"; "pending")
            // From:      JOIN (anonymous table)
            // To:        Filter with InnerExists predicate

            log::debug!("EPOCH 3: Converting exists_mode=true anonymous table from JOIN to Filter");

            let new_table_idx = table_idx + 1;

            // EPOCH 7: Check if this is the inverted IN pattern (simple case)
            // Pattern: +_(literal @ col1; col2; col3)
            // Can be transformed directly to: literal IN (col1, col2, col3)
            if is_inverted_in_pattern(anon_data, &right_table_flat.correlation_refs) {
                log::debug!(
                    "EPOCH 7: Detected inverted IN pattern - using simplified IN predicate"
                );

                // Build IN predicate directly
                let in_predicate = build_in_predicate_from_inverted_pattern(
                    anon_data,
                    &right_table_flat.correlation_refs,
                )?;

                let filter_expr = refined::RelationalExpression::Filter {
                    source: Box::new(result),
                    condition: refined::SigmaCondition::Predicate(in_predicate),
                    origin: resolved::FilterOrigin::UserWritten,
                    cpr_schema: refined::PhaseBox::new(right_table_flat.schema.clone())
                        .into_refined(),
                };

                return Ok((filter_expr, new_table_idx));
            }

            // Fallback: Build the anonymous table as a relation (general EXISTS)
            let anon_relation =
                refined::RelationalExpression::Relation(refined::Relation::Anonymous {
                    column_headers: anon_data
                        .column_headers
                        .as_ref()
                        .map(|headers| headers.iter().map(|e| e.clone().into()).collect()),
                    rows: anon_data.rows.iter().map(|r| r.clone().into()).collect(),
                    alias: right_table_flat.alias.clone().map(|s| s.into()),
                    outer: right_table_flat.outer,
                    exists_mode: true,
                    qua_target: None,
                    cpr_schema: refined::PhaseBox::new(right_table_flat.schema.clone())
                        .into_refined(),
                });

            // Wrap result in a Filter with InnerExists
            let filter_expr = refined::RelationalExpression::Filter {
                source: Box::new(result),
                condition: refined::SigmaCondition::Predicate(
                    refined::BooleanExpression::InnerExists {
                        exists: true,
                        identifier: QualifiedName {
                            namespace_path: NamespacePath::empty(),
                            name: "_".into(),
                            grounding: None,
                        },
                        subquery: Box::new(anon_relation),
                        alias: None,
                        using_columns: vec![],
                    },
                ),
                origin: resolved::FilterOrigin::UserWritten,
                cpr_schema: refined::PhaseBox::new(right_table_flat.schema.clone()).into_refined(),
            };

            return Ok((filter_expr, new_table_idx));
        }
    }

    let right_table = table_to_refined(&analyzed.tables[table_idx], op_predicates)?;
    let new_table_idx = table_idx + 1;

    // Get FJC predicates for this join
    let op_ref = OperatorRef::Join { position: op_idx };
    let join_predicates = op_predicates.remove(&op_ref).unwrap_or_default();

    // Build join condition
    let join_condition = build_join_condition(using_columns, join_predicates)?;

    // Determine join type
    let join_type = determine_join_type(analyzed, table_idx);

    // Build the join with proper schema
    let join_expr = create_join(result, right_table, join_condition, Some(join_type));

    Ok((join_expr, new_table_idx))
}

/// Build join condition from USING columns and predicates
pub(super) fn build_join_condition(
    using_columns: &Option<Vec<String>>,
    join_predicates: Vec<AnalyzedPredicate>,
) -> Result<Option<refined::BooleanExpression>> {
    let mut join_conditions = Vec::new();
    let mut using_columns_collected = Vec::new();

    log::debug!("build_join_condition: {} predicates", join_predicates.len());

    if let Some(ref using_cols) = using_columns {
        using_columns_collected.extend(using_cols.iter().cloned());
    }

    for p in join_predicates {
        log::debug!("Processing predicate: {:?}", p.expr);
        match &p.expr {
            resolved::BooleanExpression::Using { columns } => {
                for col in columns {
                    if let resolved::UsingColumn::Regular(qname) = col {
                        let name_str = qname.name.to_string();
                        if !using_columns_collected.contains(&name_str) {
                            using_columns_collected.push(name_str);
                        }
                    }
                }
            }
            _ if matches!(p.class, PredicateClass::FJC { .. }) => {
                join_conditions.push(p.expr.into());
            }
            // Other predicates (FIC, etc.): not join conditions, skip here
            // They'll be placed as WHERE filters by the predicate placement logic
            _ => {}
        }
    }

    let using_condition = if !using_columns_collected.is_empty() {
        log::debug!(
            "Creating combined USING with columns: {:?}",
            using_columns_collected
        );
        Some(create_using_condition(&using_columns_collected))
    } else {
        None
    };

    Ok(if let Some(using) = using_condition {
        Some(using)
    } else if !join_conditions.is_empty() {
        Some(combine_predicates_with_and(join_conditions))
    } else {
        None
    })
}

pub(super) fn create_join(
    left: refined::RelationalExpression,
    right: refined::RelationalExpression,
    join_condition: Option<refined::BooleanExpression>,
    join_type: Option<JoinType>,
) -> refined::RelationalExpression {
    let jt = join_type.unwrap_or(JoinType::Inner);
    refined::RelationalExpression::Join {
        left: Box::new(left.clone()),
        right: Box::new(right.clone()),
        join_condition,
        join_type: Some(jt.clone()), // Always set join_type - default to Inner if None
        cpr_schema: compute_join_schema(&left, &right, jt),
    }
}

pub(super) fn determine_join_type(analyzed: &AnalyzedSegment, table_idx: usize) -> JoinType {
    let right_outer = analyzed.tables[table_idx].outer;

    let left_needs_preserving = (0..table_idx).any(|i| analyzed.tables[i].outer);

    // Semantic meaning:
    // - right_outer=true (? on right table) means right table is optional -> LEFT OUTER JOIN
    // - left_needs_preserving=true (? on left tables) means left tables are optional -> preserve left side
    match (left_needs_preserving, right_outer) {
        (true, true) => JoinType::FullOuter,   // Both sides optional
        (true, false) => JoinType::RightOuter, // Left side optional, right required
        (false, true) => JoinType::LeftOuter,  // Left required, right optional
        (false, false) => JoinType::Inner,     // Both required
    }
}

pub(super) fn create_using_condition(using_cols: &[String]) -> refined::BooleanExpression {
    let using_columns: Vec<refined::UsingColumn> = using_cols
        .iter()
        .map(|col_name| {
            refined::UsingColumn::Regular(QualifiedName {
                namespace_path: NamespacePath::empty(),
                name: col_name.clone().into(),
                grounding: None,
            })
        })
        .collect();
    refined::BooleanExpression::Using {
        columns: using_columns,
    }
}

/// EPOCH 7: Detect if anonymous table matches the inverted IN pattern
/// Pattern: +_(literal @ col1; col2; col3)
/// - Header must be a single literal (the value to search for)
/// - Data rows must be simple column references (one per row)
/// - correlation_refs indicates column references were detected
fn is_inverted_in_pattern(
    anon_data: &super::super::flattener::AnonymousTableData,
    correlation_refs: &[super::super::flattener::CorrelationRef],
) -> bool {
    use crate::pipeline::asts::resolved;

    // Must have correlation refs (detected in Analyze phase)
    if correlation_refs.is_empty() {
        return false;
    }

    // Header must be exactly one literal
    let has_single_literal_header = anon_data.column_headers.as_ref().is_some_and(|headers| {
        headers.len() == 1 && matches!(headers[0], resolved::DomainExpression::Literal { .. })
    });

    if !has_single_literal_header {
        return false;
    }

    // All data rows must be simple single-column refs (one value per row)
    let all_rows_simple = anon_data.rows.iter().all(|row| {
        row.values.len() == 1 && matches!(row.values[0], resolved::DomainExpression::Lvar { .. })
    });

    has_single_literal_header && all_rows_simple
}

/// EPOCH 7: Build an IN predicate from inverted pattern
/// Transforms: +_("electronics" @ description; name)
/// Into: BooleanExpression::In { value: "electronics", set: [description, name], negated: false }
fn build_in_predicate_from_inverted_pattern(
    anon_data: &super::super::flattener::AnonymousTableData,
    _correlation_refs: &[super::super::flattener::CorrelationRef],
) -> crate::error::Result<refined::BooleanExpression> {
    use crate::error::DelightQLError;
    use crate::pipeline::asts::refined;

    // Extract the literal value from header (already validated)
    let header_literal = anon_data
        .column_headers
        .as_ref()
        .and_then(|h| h.first())
        .ok_or_else(|| DelightQLError::parse_error("Expected header in inverted IN pattern"))?;

    // Convert header to refined phase
    let search_value = header_literal.clone().into();

    // Extract column names from data rows
    let column_set: Vec<refined::DomainExpression> = anon_data
        .rows
        .iter()
        .map(|row| row.values[0].clone().into())
        .collect();

    // Create IN predicate
    // Note: This creates the structure that will later be desugared back to anonymous table,
    // but the transformer can recognize this pattern and generate proper SQL
    Ok(refined::BooleanExpression::In {
        value: Box::new(search_value),
        set: column_set,
        negated: false,
    })
}

/// Validate that outer join markers have explicit join conditions
///
/// Rule: Standalone table cannot have outer join marker (nothing to join to)
fn validate_outer_join_markers(
    analyzed: &AnalyzedSegment,
    op_predicates: &HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<()> {
    log::debug!(
        "validate_outer_join_markers: checking {} tables, {} operators",
        analyzed.tables.len(),
        analyzed.operators.len()
    );

    log::debug!(
        "  Operators: {:?}",
        analyzed
            .operators
            .iter()
            .map(|op| &op.kind)
            .collect::<Vec<_>>()
    );
    log::debug!(
        "  Predicates by operator: {:?}",
        op_predicates.keys().collect::<Vec<_>>()
    );

    // Rule 1: Check for standalone table with outer marker
    if analyzed.tables.len() == 1 && analyzed.tables[0].outer {
        let table_name = &analyzed.tables[0].identifier.name;
        log::debug!("ERROR: Standalone table with outer marker: {}", table_name);
        return Err(DelightQLError::parse_error(format!(
            "Outer join marker on standalone table '{}'\n\n\
            The table has an outer join marker (?, <, or >) but there are no other tables\n\
            to join it to. Outer join markers require at least one join operation.\n\n\
            Remove the marker:\n  {}(*)",
            table_name, table_name
        )));
    }

    // Rule 2: For each join operator, check if outer markers require conditions
    // Join operator at position i connects table i to table i+1
    for (join_idx, _op) in analyzed.operators.iter().enumerate() {
        let left_table_idx = join_idx;
        let right_table_idx = join_idx + 1;

        if right_table_idx >= analyzed.tables.len() {
            continue; // No right table (shouldn't happen but be safe)
        }

        let left_table = &analyzed.tables[left_table_idx];
        let right_table = &analyzed.tables[right_table_idx];

        log::debug!(
            "  Join {}: '{}' (outer={}) <-> '{}' (outer={})",
            join_idx,
            left_table.identifier.name,
            left_table.outer,
            right_table.identifier.name,
            right_table.outer
        );

        // Check if either side has an outer marker
        let needs_condition = left_table.outer || right_table.outer;

        if needs_condition {
            // Check if this join has any FJC predicates
            let op_ref = OperatorRef::Join { position: join_idx };
            let has_join_condition = op_predicates
                .get(&op_ref)
                .map(|preds| !preds.is_empty())
                .unwrap_or(false)
                || matches!(
                    &analyzed.operators[join_idx].kind,
                    flattener::FlatOperatorKind::Join {
                        using_columns: Some(_)
                    }
                );

            log::debug!(
                "  Join {} needs_condition={}, has_condition={}",
                join_idx,
                needs_condition,
                has_join_condition
            );

            if !has_join_condition {
                // Determine which table(s) have the marker for error message
                let table_with_marker = if left_table.outer && right_table.outer {
                    format!(
                        "'{}' and '{}'",
                        left_table.identifier.name, right_table.identifier.name
                    )
                } else if left_table.outer {
                    format!("'{}'", left_table.identifier.name)
                } else {
                    format!("'{}'", right_table.identifier.name)
                };

                return Err(DelightQLError::parse_error(format!(
                    "FULL OUTER marker requires explicit join condition\n\n\
                    {} ha{} a FULL OUTER marker (?) but no join condition\n\
                    specifying how the tables connect.\n\n\
                    Add a join condition:\n  ?{}(*), ?{}(*), {}.id = {}.foreign_key\n\n\
                    Or remove the marker if a regular join is intended:\n  {}(*), {}(*), {}.id = {}.foreign_key",
                    table_with_marker,
                    if left_table.outer && right_table.outer { "ve" } else { "s" },
                    left_table.identifier.name, right_table.identifier.name,
                    left_table.identifier.name, right_table.identifier.name,
                    left_table.identifier.name, right_table.identifier.name,
                    left_table.identifier.name, right_table.identifier.name
                )));
            }
        }
    }

    Ok(())
}
