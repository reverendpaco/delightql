// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

    // Witness anonymous tables (+_/\+_) never reach the refiner: the
    // resolver routes membership shapes to a Filter and refuses the
    // rest. An exists_mode anon table here is a pipeline invariant
    // violation, not a case to lower.
    let right_table_flat = &analyzed.tables[table_idx];
    if let Some(ref anon_data) = right_table_flat.anonymous_data {
        assert!(
            !anon_data.exists_mode,
            "witness anonymous table survived to the refiner: membership routing must consume it"
        );
    }

    let right_table = table_to_refined(&analyzed.tables[table_idx], op_predicates)?;
    let new_table_idx = table_idx + 1;

    // Get FJC predicates for this join
    let op_ref = OperatorRef::Join { position: op_idx };
    let join_predicates = op_predicates.remove(&op_ref).unwrap_or_default();

    // Build join condition
    let (join_condition, leftover_conditions) =
        build_join_condition(using_columns, join_predicates)?;

    // Determine join type
    let join_type = determine_join_type(analyzed, table_idx);

    if !leftover_conditions.is_empty() && join_type != JoinType::Inner {
        return Err(crate::error::DelightQLError::validation_error_categorized(
            "join/using/extra_condition",
            "a USING-style join with an additional multi-relation condition \
is not expressible for an outer join: USING has no ON clause to carry it, \
and WHERE placement would change which rows match",
            "write the join fully explicitly: replace .(cols) with equality \
predicates alongside the extra condition",
        ));
    }

    // Build the join with proper schema
    let join_expr = create_join(result, right_table, join_condition, Some(join_type));

    // Inner join: the leftovers filter the joined rows — WHERE placement
    // is exactly equivalent to ON for an inner join.
    let join_expr = if leftover_conditions.is_empty() {
        join_expr
    } else {
        let schema = match &join_expr {
            refined::RelationalExpression::Join { cpr_schema, .. } => cpr_schema.get().clone(),
            _ => unreachable!("create_join returns Join"),
        };
        refined::RelationalExpression::Filter {
            source: Box::new(join_expr),
            condition: refined::SigmaCondition::Predicate(combine_predicates_with_and(
                leftover_conditions,
            )),
            origin: resolved::FilterOrigin::UserWritten,
            cpr_schema: refined::PhaseBox::new(schema).into_refined(),
        }
    };

    Ok((join_expr, new_table_idx))
}

/// Build join condition from USING columns and predicates
/// Returns (join condition, leftovers). When USING wins the join slot,
/// FJC predicates cannot ride the ON clause — they are RETURNED, never
/// dropped: the caller places them as WHERE (inner join) or refuses
/// (outer join, where placement changes match semantics).
pub(super) fn build_join_condition(
    using_columns: &Option<Vec<String>>,
    join_predicates: Vec<AnalyzedPredicate>,
) -> Result<(Option<refined::BooleanExpression>, Vec<refined::BooleanExpression>)> {
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
                let refined = super::refine_predicate_boolean(p.expr)?;
                join_conditions.push(downgrade_null_safe_eq(refined));
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
        (Some(using), join_conditions)
    } else if !join_conditions.is_empty() {
        (Some(combine_predicates_with_and(join_conditions)), Vec::new())
    } else {
        (None, Vec::new())
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
    // Markedness, not comma position, determines join role: the unmarked
    // tables form the required core; each ?-marked table LEFT-joins onto
    // the tree; FULL OUTER happens only when EVERY join party is marked
    // (left-fold in written order). EXISTS-mode anonymous tables lower as
    // filters, not joins, so they carry no vote here.
    let is_join_party =
        |t: &flattener::FlatTable| t.anonymous_data.as_ref().is_none_or(|a| !a.exists_mode);

    if analyzed
        .tables
        .iter()
        .filter(|t| is_join_party(t))
        .all(|t| t.outer)
    {
        return JoinType::FullOuter;
    }
    if analyzed.tables[table_idx].outer {
        return JoinType::LeftOuter;
    }
    // Right side unmarked: if everything joined so far is marked, this
    // table anchors the tree and the accumulated optional cluster
    // preserves as a unit — RIGHT here; the transformer swaps operands
    // back to LEFT.
    if (0..table_idx)
        .filter(|&i| is_join_party(&analyzed.tables[i]))
        .all(|i| analyzed.tables[i].outer)
    {
        JoinType::RightOuter
    } else {
        JoinType::Inner
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

    // Rule 2: FULL OUTER — the all-marked case — requires a join
    // condition. The ? sigil marks its relation as OPTIONAL; a one-sided
    // mark without a condition is a legal LEFT join over the cross
    // product. Only when EVERY relation is marked does the join become
    // FULL OUTER, whose two-part construction needs a condition to find
    // each side's unmatched rows.
    let all_marked = analyzed
        .tables
        .iter()
        .filter(|t| t.anonymous_data.as_ref().is_none_or(|a| !a.exists_mode))
        .all(|t| t.outer);
    if !all_marked {
        return Ok(());
    }

    for (join_idx, _op) in analyzed.operators.iter().enumerate() {
        let right_table_idx = join_idx + 1;
        if right_table_idx >= analyzed.tables.len() {
            continue; // No right table (shouldn't happen but be safe)
        }
        let left_table = &analyzed.tables[join_idx];
        let right_table = &analyzed.tables[right_table_idx];

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

        if !has_join_condition {
            return Err(DelightQLError::parse_error_categorized(
                "general",
                format!(
                "FULL OUTER JOIN requires an explicit join condition\n\n\
                Every relation in this chain is marked optional (?), which makes\n\
                the join FULL OUTER — but the join between '{l}' and '{r}' has no\n\
                condition saying how the two sides align, so there is no way to\n\
                tell which rows of each side are unmatched.\n\n\
                Add a join condition:\n  {l}?(*), {r}?(*), {l}.id = {r}.{l}_id\n\n\
                Or give the patterns a shared variable:\n  {l}?(id, x), {r}?(id, y)\n\n\
                (One-sided ? marks do not need a condition: the marked relation\n\
                LEFT-joins the rest.)",
                    l = left_table.identifier.name,
                    r = right_table.identifier.name,
                ),
            ));
        }
    }

    Ok(())
}

/// Rewrite `null_safe_eq` → `traditional_eq` in join conditions.
///
/// In join position, equality is CORRESPONDENCE: null never matches, because
/// null-matching ON clauses multiply the null groups AND assert a
/// correspondence between absences. There is deliberately no gate back into
/// INDF here — a null that is meant to match is a value wearing null's
/// clothes; the spelling is a named key (coalesce into a marker), never a
/// mode switch.
fn downgrade_null_safe_eq(expr: refined::BooleanExpression) -> refined::BooleanExpression {
    match expr {
        refined::BooleanExpression::Comparison {
            operator,
            left,
            right,
        } if operator == "null_safe_eq" => refined::BooleanExpression::Comparison {
            operator: "traditional_eq".to_string(),
            left,
            right,
        },
        refined::BooleanExpression::And { left, right } => refined::BooleanExpression::And {
            left: Box::new(downgrade_null_safe_eq(*left)),
            right: Box::new(downgrade_null_safe_eq(*right)),
        },
        refined::BooleanExpression::Or { left, right } => refined::BooleanExpression::Or {
            left: Box::new(downgrade_null_safe_eq(*left)),
            right: Box::new(downgrade_null_safe_eq(*right)),
        },
        refined::BooleanExpression::Not { expr: inner } => refined::BooleanExpression::Not {
            expr: Box::new(downgrade_null_safe_eq(*inner)),
        },
        other => other,
    }
}
