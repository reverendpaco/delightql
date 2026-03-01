// predicate_classifier.rs - Classify predicates into FIC, FJC, F, Fx categories
//
// This module handles predicate classification logic and law application

use super::reference_extraction::extract_referenced_tables;
use crate::error::Result;
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::flattener::{FlatOperatorKind, FlatPredicate, FlatSegment};
use crate::pipeline::refiner::laws;
use crate::pipeline::refiner::types::*;

/// Classify a predicate based on its references
pub(super) fn classify_predicate(
    pred: &FlatPredicate,
    flat: &FlatSegment,
    _scope_point: &ScopePoint,
) -> Result<PredicateClass> {
    log::debug!(
        "classify_predicate: expr={:?}, origin={:?}",
        pred.expr,
        pred.origin
    );

    // Special handling for PositionalLiteral filters - they always become F (WHERE)
    if let resolved::FilterOrigin::PositionalLiteral { source_table } = &pred.origin {
        // Handle the __join__ case - these are filters from join-level positional patterns
        // We need to determine which table they actually belong to
        if source_table == "__join__" {
            // Try to extract the actual table from the expression
            // This is a workaround for the resolver combining filters at join level
            if let resolved::BooleanExpression::Comparison { left, right, .. } = &pred.expr {
                // Check left side for qualified column
                if let resolved::DomainExpression::Lvar {
                    qualifier: Some(q), ..
                } = left.as_ref()
                {
                    return Ok(PredicateClass::F {
                        table: q.to_string(),
                    });
                }
                // Check right side for qualified column
                if let resolved::DomainExpression::Lvar {
                    qualifier: Some(q), ..
                } = right.as_ref()
                {
                    return Ok(PredicateClass::F {
                        table: q.to_string(),
                    });
                }
            }
            // If we can't determine the table, treat as top-level
            return Ok(PredicateClass::Fx);
        }
        return Ok(PredicateClass::F {
            table: source_table.clone(),
        });
    }

    // Check if it's an Fx (non-participating) predicate
    if pred.qualified_refs.is_empty() && pred.unqualified_refs.is_empty() {
        return Ok(PredicateClass::Fx);
    }

    // Determine which tables are referenced (now includes unqualified refs via schema)
    let referenced_tables = extract_referenced_tables(pred, flat)?;

    if referenced_tables.is_empty() {
        // No tables referenced at all - treat as Fx
        Ok(PredicateClass::Fx)
    } else if referenced_tables.len() == 1 {
        // Single table reference - regular filter F
        Ok(PredicateClass::F {
            table: referenced_tables.into_iter().next().unwrap(),
        })
    } else if referenced_tables.len() == 2 {
        // Two table reference - could be FJC or FIC
        let tables: Vec<_> = referenced_tables.into_iter().collect();
        let left = tables[0].clone();
        let right = tables[1].clone();

        // Determine relationship between tables
        if are_in_join_relationship(&left, &right, flat) {
            log::debug!(
                "Tables {} and {} are in join relationship -> FJC",
                left,
                right
            );
            Ok(PredicateClass::FJC { left, right })
        } else if are_in_setop_relationship(&left, &right, flat) {
            log::debug!(
                "Tables {} and {} are in setop relationship -> FIC",
                left,
                right
            );
            Ok(PredicateClass::FIC { left, right })
        } else {
            log::debug!("Tables {} and {} have no direct relationship", left, right);
            // Tables not directly related - need to check scope
            Ok(PredicateClass::F { table: left })
        }
    } else {
        // More than 2 tables - complex predicate
        // For now, treat as filter on first table
        if let Some(first_table) = referenced_tables.into_iter().next() {
            Ok(PredicateClass::F { table: first_table })
        } else {
            // Shouldn't happen since we checked for empty above
            Ok(PredicateClass::Fx)
        }
    }
}

/// Check if two tables are in a specific operator relationship
fn are_tables_in_operator_relationship(
    left: &str,
    right: &str,
    flat: &FlatSegment,
    operator_type: OperatorType,
) -> bool {
    for op in &flat.operators {
        let matches_type = match operator_type {
            OperatorType::Join => {
                matches!(op.kind, FlatOperatorKind::Join { .. })
            }
            OperatorType::SetOp => {
                matches!(op.kind, FlatOperatorKind::SetOp { .. })
            }
        };

        if matches_type {
            // Check if one table is on the left and the other is on the right
            if (op.left_tables.contains(&left.to_string())
                && op.right_tables.contains(&right.to_string()))
                || (op.left_tables.contains(&right.to_string())
                    && op.right_tables.contains(&left.to_string()))
            {
                return true;
            }
        }
    }
    false
}

/// Check if two tables are in a join relationship
fn are_in_join_relationship(left: &str, right: &str, flat: &FlatSegment) -> bool {
    are_tables_in_operator_relationship(left, right, flat, OperatorType::Join)
}

/// Check if two tables are in a set operation relationship
fn are_in_setop_relationship(left: &str, right: &str, flat: &FlatSegment) -> bool {
    are_tables_in_operator_relationship(left, right, flat, OperatorType::SetOp)
}

#[derive(Debug, Copy, Clone)]
enum OperatorType {
    Join,
    SetOp,
}

/// Apply Laws 1-6 to check if classification should be forbidden
pub(super) fn apply_laws(
    initial_class: PredicateClass,
    pred: &FlatPredicate,
    context: &laws::LawContext,
    _scope_point: &ScopePoint,
) -> Result<PredicateClass> {
    match &initial_class {
        PredicateClass::FJC { left, right } => {
            // Check Law 1: Forbidden UL Fragment Join
            if let Some(reason) = laws::check_law1(&pred.expr, left, right, context) {
                return Ok(PredicateClass::Forbidden { reason });
            }

            Ok(initial_class)
        }

        PredicateClass::FIC { left, right } => {
            // Check Law 3: Intersection Qualification Requirements
            if let Some(reason) = laws::check_law3(&pred.expr, left, right) {
                return Ok(PredicateClass::Forbidden { reason });
            }

            // Check Law 6: PLF Set Operation Restriction
            if let Some(reason) = laws::check_law6(&pred.expr, context) {
                return Ok(PredicateClass::Forbidden { reason });
            }

            Ok(initial_class)
        }

        PredicateClass::F { table } => {
            // Check Law 6 for single-table filters with PLFs
            if context.plf_tables.contains(table) {
                if let Some(reason) = laws::check_law6(&pred.expr, context) {
                    return Ok(PredicateClass::Forbidden { reason });
                }
            }

            Ok(initial_class)
        }

        // Fx: non-participating predicate (references no tables) — no laws apply
        PredicateClass::Fx => Ok(initial_class),
        // Forbidden: already rejected by a prior law — pass through
        PredicateClass::Forbidden { .. } => Ok(initial_class),
    }
}
