use super::schema_computation::{compute_pipe_schema, compute_setop_schema};
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::refined::{self, JoinType, PhaseBox, Refined, SetOperator};
use crate::pipeline::asts::resolved::{self, CprSchema};
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::refiner::analyzer::AnalyzedSegment;
use crate::pipeline::refiner::flattener::{self, FlatTable, OperationContext};
use crate::pipeline::refiner::rebuilder::{
    apply_filter_predicates, combine_predicates_with_and, create_join, table_to_refined,
    SetOperatorStrategy,
};
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;

/// Rebuild a segment containing set operations
pub(super) fn rebuild_setop_segment(
    analyzed: AnalyzedSegment,
    mut op_predicates: HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<refined::RelationalExpression> {
    // Handle simple cases first
    if analyzed.operators.is_empty() {
        return handle_no_operators(&analyzed, &mut op_predicates);
    }

    log::debug!(
        "rebuild_setop_segment: {} operators, {} tables",
        analyzed.operators.len(),
        analyzed.tables.len()
    );
    for (i, op) in analyzed.operators.iter().enumerate() {
        log::debug!("  Operator {}: {:?}", i, op);
    }

    // Build operands from tables
    let (operands, operand_table_groups) = build_setop_operands(&analyzed, &mut op_predicates)?;

    log::debug!("Built {} operands", operands.len());

    // Handle multiple operators by building a flat N-way SetOperation
    let result: Option<refined::RelationalExpression>;
    if analyzed.operators.len() > 1 {
        log::debug!(
            "Multiple operators detected ({}), building flat N-way union",
            analyzed.operators.len()
        );

        // Collect all FIC predicates from all operators
        let mut all_fic_predicates = Vec::new();
        for (op_idx, op) in analyzed.operators.iter().enumerate() {
            let (_, op_ref) = extract_setop_operator(op, op_idx)?;
            if let Some(preds) = op_predicates.remove(&op_ref) {
                log::debug!("Operator {} has {} predicates", op_idx, preds.len());
                for pred in preds {
                    log::debug!("  Predicate class: {:?}, expr: {:?}", pred.class, pred.expr);
                    if matches!(pred.class, PredicateClass::FIC { .. }) {
                        log::debug!("  -> Found FIC predicate: {:?}", pred.expr);
                        all_fic_predicates.push(super::refine_predicate_boolean(pred.expr)?);
                    }
                }
            }
        }

        // Expand any GlobCorrelation predicates using operand schemas
        let all_fic_predicates =
            expand_glob_correlations(all_fic_predicates, &operand_table_groups)?;

        // Assume all operators are the same type (they should be for ; syntax)
        let (setop, _) = extract_setop_operator(&analyzed.operators[0], 0)?;

        // Handle set operation semantics for all operands at once
        let (final_operator, final_operands) = match setop {
            SetOperator::UnionCorresponding => {
                handle_union_corresponding(operands, &operand_table_groups)?
            }
            SetOperator::UnionAllPositional => {
                // UA (||) - pure positional, no transformation needed
                (SetOperator::UnionAllPositional, operands)
            }
            SetOperator::SmartUnionAll => handle_smart_union_all(operands, &operand_table_groups)?,
            SetOperator::MinusCorresponding => {
                handle_minus_corresponding(operands, &operand_table_groups)?
            }
        };

        // Build correlation from all FIC predicates
        let correlation = if !all_fic_predicates.is_empty() {
            log::debug!(
                "Building correlation from {} FIC predicates",
                all_fic_predicates.len()
            );
            Some(combine_predicates_with_and(all_fic_predicates))
        } else {
            log::debug!("No FIC predicates found for correlation");
            None
        };

        // Build flat N-way SetOperation
        let cpr_schema = compute_setop_schema(final_operator, &final_operands);
        result = Some(refined::RelationalExpression::SetOperation {
            operator: final_operator,
            operands: final_operands,
            correlation: <PhaseBox<Option<refined::BooleanExpression>, Refined>>::with_correlation(
                correlation,
            ),
            cpr_schema,
        });
    } else {
        // Single operator - use existing logic
        let (setop, op_ref) = extract_setop_operator(&analyzed.operators[0], 0)?;
        log::debug!("Using single operator: {:?}, op_ref: {:?}", setop, op_ref);

        // Get FIC predicates for correlation
        let setop_predicates = op_predicates.remove(&op_ref).unwrap_or_default();
        let fic_predicates: Vec<refined::BooleanExpression> = setop_predicates
            .into_iter()
            .filter_map(|p| {
                if matches!(p.class, PredicateClass::FIC { .. }) {
                    Some(super::refine_predicate_boolean(p.expr).ok()?)
                } else {
                    None
                }
            })
            .collect();
        // Expand any GlobCorrelation predicates using operand schemas
        let fic_predicates = expand_glob_correlations(fic_predicates, &operand_table_groups)?;

        // Handle set operation semantics
        let (final_operator, final_operands) = match setop {
            SetOperator::UnionCorresponding => {
                handle_union_corresponding(operands, &operand_table_groups)?
            }
            SetOperator::SmartUnionAll => handle_smart_union_all(operands, &operand_table_groups)?,
            SetOperator::MinusCorresponding => {
                handle_minus_corresponding(operands, &operand_table_groups)?
            }
            SetOperator::UnionAllPositional => {
                // UA (||) - pure positional, no transformation needed
                (SetOperator::UnionAllPositional, operands)
            }
        };

        // Build correlation condition based on operator type
        let correlation = match final_operator {
            SetOperator::UnionAllPositional
            | SetOperator::UnionCorresponding
            | SetOperator::SmartUnionAll => {
                // These can have correlation predicates (FIC)
                if !fic_predicates.is_empty() {
                    log::debug!(
                        "Single operator: Building correlation from {} FIC predicates",
                        fic_predicates.len()
                    );
                    Some(combine_predicates_with_and(fic_predicates))
                } else {
                    log::debug!("Single operator: No FIC predicates found");
                    None
                }
            }
            SetOperator::MinusCorresponding => {
                // Minus doesn't use correlation conditions from FIC predicates
                // It compiles to NOT EXISTS with all-columns matching
                None
            }
        };

        // Build the set operation with proper schema
        let cpr_schema = compute_setop_schema(final_operator, &final_operands);

        result = Some(refined::RelationalExpression::SetOperation {
            operator: final_operator,
            operands: final_operands,
            correlation: <PhaseBox<Option<refined::BooleanExpression>, Refined>>::with_correlation(
                correlation,
            ),
            cpr_schema,
        });
    }

    let mut result = result.expect("Should have built result");

    // Apply any top-level filters
    result = apply_filter_predicates(result, &mut op_predicates, OperatorRef::TopLevel)?;

    Ok(result)
}

pub(super) fn handle_setop_with_column_alignment(
    operands: Vec<refined::RelationalExpression>,
    operand_table_groups: &[Vec<FlatTable>],
    strategy: SetOperatorStrategy,
    result_operator: SetOperator,
) -> Result<(SetOperator, Vec<refined::RelationalExpression>)> {
    let transformed_operands = match strategy {
        SetOperatorStrategy::Correspondence => {
            // Build column mapping for all columns
            let (column_order, column_presence) =
                build_column_correspondence(operand_table_groups)?;
            transform_operands_with_correspondence(operands, &column_order, &column_presence)?
        }
        SetOperatorStrategy::SameColumnsReorder => {
            // Verify same columns and reorder to match first
            verify_same_columns(operand_table_groups, &format!("{:?}", result_operator))?;
            let column_order = get_column_order_from_first(operand_table_groups)?;
            reorder_operands_to_match_first(operands, &column_order)?
        }
    };

    Ok((result_operator, transformed_operands))
}

/// Handle UnionCorresponding operation
pub(super) fn handle_union_corresponding(
    operands: Vec<refined::RelationalExpression>,
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<(SetOperator, Vec<refined::RelationalExpression>)> {
    handle_setop_with_column_alignment(
        operands,
        operand_table_groups,
        SetOperatorStrategy::Correspondence,
        SetOperator::UnionAllPositional,
    )
}

/// Build column correspondence mapping for union operations
pub(super) fn build_column_correspondence(
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<(Vec<String>, std::collections::HashMap<String, Vec<bool>>)> {
    let mut column_order: Vec<String> = Vec::new();
    let mut column_presence: std::collections::HashMap<String, Vec<bool>> =
        std::collections::HashMap::new();

    // First pass: collect all unique columns in order of first appearance
    for (op_idx, table_group) in operand_table_groups.iter().enumerate() {
        for table in table_group {
            if let resolved::CprSchema::Resolved(cols) = &table.schema {
                for col in cols {
                    let col_name = col.name().to_string();
                    if !column_order.contains(&col_name) {
                        column_order.push(col_name.clone());
                        // Initialize presence vector for all operands
                        column_presence
                            .insert(col_name.clone(), vec![false; operand_table_groups.len()]);
                    }
                    // Mark this column as present in this operand
                    if let Some(presence) = column_presence.get_mut(&col_name) {
                        presence[op_idx] = true;
                    }
                }
            }
        }
    }

    Ok((column_order, column_presence))
}

/// Transform operands with column correspondence
pub(super) fn transform_operands_with_correspondence(
    operands: Vec<refined::RelationalExpression>,
    column_order: &[String],
    column_presence: &std::collections::HashMap<String, Vec<bool>>,
) -> Result<Vec<refined::RelationalExpression>> {
    operands
        .into_iter()
        .enumerate()
        .map(|(op_idx, operand)| {
            let projections = build_projections_for_operand(op_idx, column_order, column_presence);

            // Wrap operand with projection pipe
            Ok(refined::RelationalExpression::Pipe(Box::new(
                stacksafe::StackSafe::new(refined::PipeExpression {
                    source: operand,
                    operator: refined::UnaryRelationalOperator::General {
                        containment_semantic: refined::ContainmentSemantic::Parenthesis,
                        expressions: projections.clone(),
                    },
                    cpr_schema: compute_pipe_schema(&projections),
                }),
            )))
        })
        .collect()
}

/// Build projections for a single operand
pub(super) fn build_projections_for_operand(
    op_idx: usize,
    column_order: &[String],
    column_presence: &std::collections::HashMap<String, Vec<bool>>,
) -> Vec<refined::DomainExpression> {
    let mut projections = Vec::new();

    for col_name in column_order {
        let has_column = column_presence
            .get(col_name)
            .map(|p| p[op_idx])
            .unwrap_or(false);

        if has_column {
            // Project the actual column
            projections.push(refined::DomainExpression::Lvar {
                name: col_name.clone().into(),
                qualifier: None,
                namespace_path: NamespacePath::empty(),
                alias: None,
                provenance: crate::pipeline::asts::refined::PhaseBox::new(None),
            });
        } else {
            // Project NULL with alias for missing column
            projections.push(refined::DomainExpression::Lvar {
                name: "__NULL__".into(),
                qualifier: None,
                namespace_path: NamespacePath::empty(),
                alias: Some(col_name.clone().into()),
                provenance: crate::pipeline::asts::refined::PhaseBox::new(None),
            });
        }
    }

    projections
}

/// Handle SmartUnionAll operation
pub(super) fn handle_smart_union_all(
    operands: Vec<refined::RelationalExpression>,
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<(SetOperator, Vec<refined::RelationalExpression>)> {
    // SUA (|;|) - requires same columns but allows different order

    handle_setop_with_column_alignment(
        operands,
        operand_table_groups,
        SetOperatorStrategy::SameColumnsReorder,
        SetOperator::UnionAllPositional,
    )
}

/// Handle MinusCorresponding operation
/// Minus requires same column names. Output is always the left side's schema.
pub(super) fn handle_minus_corresponding(
    operands: Vec<refined::RelationalExpression>,
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<(SetOperator, Vec<refined::RelationalExpression>)> {
    // Minus (-) - same columns required, reorder right to match left
    handle_setop_with_column_alignment(
        operands,
        operand_table_groups,
        SetOperatorStrategy::SameColumnsReorder,
        SetOperator::MinusCorresponding,
    )
}
pub(super) fn handle_no_operators(
    analyzed: &AnalyzedSegment,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<refined::RelationalExpression> {
    if analyzed.tables.len() == 1 {
        table_to_refined(&analyzed.tables[0], op_predicates)
    } else {
        Err(DelightQLError::parse_error(
            "Multiple tables without operators",
        ))
    }
}

/// Generic handler for set operations with column alignment
/// Verify all operands have the same column names
pub(super) fn verify_same_columns(
    operand_table_groups: &[Vec<FlatTable>],
    operation: &str,
) -> Result<()> {
    let mut first_columns: Option<std::collections::HashSet<String>> = None;

    for table_group in operand_table_groups {
        let columns = extract_columns_from_table_group(table_group);

        if let Some(ref first) = first_columns {
            if columns != *first {
                return Err(DelightQLError::parse_error(format!(
                    "{} requires all operands to have the same columns",
                    operation
                )));
            }
        } else {
            first_columns = Some(columns);
        }
    }

    Ok(())
}

/// Extract column names from a table group
pub(super) fn extract_columns_from_table_group(
    table_group: &[FlatTable],
) -> std::collections::HashSet<String> {
    let mut columns = std::collections::HashSet::new();
    for table in table_group {
        if let CprSchema::Resolved(cols) = &table.schema {
            for col in cols {
                columns.insert(col.name().to_string());
            }
        }
    }
    columns
}

/// Get column order from first operand
pub(super) fn get_column_order_from_first(
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<Vec<String>> {
    let mut column_order: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if let Some(first_group) = operand_table_groups.first() {
        for table in first_group {
            collect_unique_column_order(&table.schema, &mut column_order, &mut seen);
        }
    }

    Ok(column_order)
}

/// Collect column order while avoiding duplicates
pub(super) fn collect_unique_column_order(
    schema: &CprSchema,
    column_order: &mut Vec<String>,
    seen: &mut std::collections::HashSet<String>,
) {
    if let CprSchema::Resolved(cols) = schema {
        for col in cols {
            let col_name = col.name().to_string();
            if seen.insert(col_name.clone()) {
                column_order.push(col_name);
            }
        }
    }
}

/// Reorder operands to match first operand's column order
pub(super) fn reorder_operands_to_match_first(
    operands: Vec<refined::RelationalExpression>,
    column_order: &[String],
) -> Result<Vec<refined::RelationalExpression>> {
    operands
        .into_iter()
        .enumerate()
        .map(|(op_idx, operand)| {
            if op_idx == 0 {
                // First operand stays as-is
                Ok(operand)
            } else {
                // Reorder columns for other operands
                let projections = create_reordering_projections(column_order);
                Ok(wrap_with_projection(operand, projections))
            }
        })
        .collect()
}

/// Create projections for column reordering
pub(super) fn create_reordering_projections(
    column_order: &[String],
) -> Vec<refined::DomainExpression> {
    column_order
        .iter()
        .map(|col_name| refined::DomainExpression::Lvar {
            name: col_name.clone().into(),
            qualifier: None,
            namespace_path: NamespacePath::empty(),
            alias: None,
            provenance: crate::pipeline::asts::refined::PhaseBox::new(None),
        })
        .collect()
}

/// Wrap an operand with a projection pipe
pub(super) fn wrap_with_projection(
    operand: refined::RelationalExpression,
    projections: Vec<refined::DomainExpression>,
) -> refined::RelationalExpression {
    refined::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
        refined::PipeExpression {
            source: operand,
            operator: refined::UnaryRelationalOperator::General {
                containment_semantic: refined::ContainmentSemantic::Parenthesis,
                expressions: projections.clone(),
            },
            cpr_schema: compute_pipe_schema(&projections),
        },
    )))
}
/// Build an operand from a list of tables
pub(super) fn build_operand_from_tables(
    tables: Vec<FlatTable>,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<refined::RelationalExpression> {
    if tables.len() == 1 {
        table_to_refined(&tables[0], op_predicates)
    } else {
        // Multiple tables - need to join them
        let mut result = table_to_refined(&tables[0], op_predicates)?;
        for table in &tables[1..] {
            let right = table_to_refined(table, op_predicates)?;
            result = create_join(result, right, None, Some(JoinType::Inner));
        }
        Ok(result)
    }
}

/// Extract set operator from flat operator
pub(super) fn extract_setop_operator(
    op: &flattener::FlatOperator,
    position: usize,
) -> Result<(SetOperator, OperatorRef)> {
    match &op.kind {
        flattener::FlatOperatorKind::SetOp { operator } => Ok((
            *operator,
            OperatorRef::SetOp {
                position,
                operator: *operator,
            },
        )),
        _ => Err(DelightQLError::parse_error("Expected set operator")),
    }
}

/// Expand GlobCorrelation predicates using operand schemas.
///
/// `GlobCorrelation { left: "first", right: "second" }` expands to:
/// `first.col1 IS NOT DISTINCT FROM second.col1 AND first.col2 IS NOT DISTINCT FROM second.col2 AND ...`
/// for all shared column names between the operands aliased as "first" and "second".
fn expand_glob_correlations(
    predicates: Vec<refined::BooleanExpression>,
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<Vec<refined::BooleanExpression>> {
    predicates
        .into_iter()
        .map(|pred| expand_single_glob_correlation(pred, operand_table_groups))
        .collect()
}

fn expand_single_glob_correlation(
    pred: refined::BooleanExpression,
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<refined::BooleanExpression> {
    match pred {
        refined::BooleanExpression::GlobCorrelation { left, right } => {
            // Find columns for each qualifier by matching FlatTable aliases
            let left_cols = find_columns_for_qualifier(&left, operand_table_groups)?;
            let right_cols = find_columns_for_qualifier(&right, operand_table_groups)?;

            // Find shared column names (by name, case-insensitive via SqlIdentifier)
            let left_set: std::collections::HashSet<String> =
                left_cols.iter().map(|c| c.to_lowercase()).collect();
            let shared: Vec<String> = right_cols
                .iter()
                .filter(|c| left_set.contains(&c.to_lowercase()))
                .cloned()
                .collect();

            if shared.is_empty() {
                return Err(DelightQLError::validation_error(
                    &format!(
                        "Glob correlation {}.* = {}.* has no shared columns",
                        left, right
                    ),
                    "The two operands have no column names in common",
                ));
            }

            // Build per-column IS NOT DISTINCT FROM comparisons
            let comparisons: Vec<refined::BooleanExpression> = shared
                .iter()
                .map(|col_name| refined::BooleanExpression::Comparison {
                    operator: "null_safe_eq".to_string(),
                    left: Box::new(refined::DomainExpression::Lvar {
                        name: col_name.clone().into(),
                        qualifier: Some(left.clone()),
                        namespace_path: NamespacePath::empty(),
                        alias: None,
                        provenance: PhaseBox::new(None),
                    }),
                    right: Box::new(refined::DomainExpression::Lvar {
                        name: col_name.clone().into(),
                        qualifier: Some(right.clone()),
                        namespace_path: NamespacePath::empty(),
                        alias: None,
                        provenance: PhaseBox::new(None),
                    }),
                })
                .collect();

            // Combine with AND
            Ok(combine_predicates_with_and(comparisons))
        }
        refined::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            // Positional correlation: match columns by index, not by name
            let left_cols = find_columns_for_qualifier(&left, operand_table_groups)?;
            let right_cols = find_columns_for_qualifier(&right, operand_table_groups)?;

            let min_len = left_cols.len().min(right_cols.len());
            if min_len == 0 {
                return Err(DelightQLError::validation_error(
                    &format!(
                        "Ordinal glob correlation {}|*| = {}|*| has no columns to compare",
                        left, right
                    ),
                    "At least one operand has no columns",
                ));
            }

            // Build per-position IS NOT DISTINCT FROM comparisons
            let comparisons: Vec<refined::BooleanExpression> = (0..min_len)
                .map(|i| refined::BooleanExpression::Comparison {
                    operator: "null_safe_eq".to_string(),
                    left: Box::new(refined::DomainExpression::Lvar {
                        name: left_cols[i].clone().into(),
                        qualifier: Some(left.clone()),
                        namespace_path: NamespacePath::empty(),
                        alias: None,
                        provenance: PhaseBox::new(None),
                    }),
                    right: Box::new(refined::DomainExpression::Lvar {
                        name: right_cols[i].clone().into(),
                        qualifier: Some(right.clone()),
                        namespace_path: NamespacePath::empty(),
                        alias: None,
                        provenance: PhaseBox::new(None),
                    }),
                })
                .collect();

            Ok(combine_predicates_with_and(comparisons))
        }
        refined::BooleanExpression::And { left, right } => {
            let expanded_left = expand_single_glob_correlation(*left, operand_table_groups)?;
            let expanded_right = expand_single_glob_correlation(*right, operand_table_groups)?;
            Ok(refined::BooleanExpression::And {
                left: Box::new(expanded_left),
                right: Box::new(expanded_right),
            })
        }
        // Non-glob-correlation predicates: pass through unchanged
        // (regular Comparison, Or, Not, In, InnerExists, etc.)
        other => Ok(other),
    }
}

/// Find column names for an operand identified by its qualifier/alias
fn find_columns_for_qualifier(
    qualifier: &delightql_types::SqlIdentifier,
    operand_table_groups: &[Vec<FlatTable>],
) -> Result<Vec<String>> {
    for table_group in operand_table_groups {
        for table in table_group {
            let matches = table
                .alias
                .as_ref()
                .map(|a| a.eq_ignore_ascii_case(qualifier.as_ref()))
                .unwrap_or(false)
                || table
                    .identifier
                    .name
                    .as_ref()
                    .eq_ignore_ascii_case(qualifier.as_ref());

            if matches {
                if let CprSchema::Resolved(cols) = &table.schema {
                    return Ok(cols.iter().map(|c| c.name().to_string()).collect());
                }
            }
        }
    }
    Err(DelightQLError::validation_error(
        &format!(
            "Glob correlation qualifier '{}' does not match any operand alias",
            qualifier
        ),
        "Each qualifier in x.* = y.* must match an operand's alias",
    ))
}

/// Extract FIC correlation predicates for a set operation
pub(super) fn extract_fic_correlation(
    op_ref: &OperatorRef,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Option<refined::BooleanExpression> {
    let setop_predicates = op_predicates.remove(op_ref).unwrap_or_default();
    let correlation_predicates: Vec<refined::BooleanExpression> = setop_predicates
        .into_iter()
        .filter_map(|p| {
            if matches!(p.class, PredicateClass::FIC { .. }) {
                Some(super::refine_predicate_boolean(p.expr).ok()?)
            } else {
                None
            }
        })
        .collect();

    if correlation_predicates.is_empty() {
        None
    } else {
        Some(combine_predicates_with_and(correlation_predicates))
    }
}

/// Build operands for set operations
pub(super) fn build_setop_operands(
    analyzed: &AnalyzedSegment,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<(Vec<refined::RelationalExpression>, Vec<Vec<FlatTable>>)> {
    let mut operands = Vec::new();
    let mut operand_table_groups: Vec<Vec<FlatTable>> = Vec::new();
    let mut current_tables = Vec::new();

    for table in &analyzed.tables {
        // Check if this table starts a new operand
        if table.operation_context == OperationContext::FromSetOp && !current_tables.is_empty() {
            // Build the previous operand
            operands.push(build_operand_from_tables(
                current_tables.clone(),
                op_predicates,
            )?);
            operand_table_groups.push(current_tables);
            current_tables = Vec::new();
        }
        current_tables.push(table.clone());
    }

    // Add the last operand
    if !current_tables.is_empty() {
        operands.push(build_operand_from_tables(
            current_tables.clone(),
            op_predicates,
        )?);
        operand_table_groups.push(current_tables);
    }

    if operands.len() < 2 {
        return Err(DelightQLError::parse_error(
            "Set operation needs at least 2 operands",
        ));
    }

    Ok((operands, operand_table_groups))
}
