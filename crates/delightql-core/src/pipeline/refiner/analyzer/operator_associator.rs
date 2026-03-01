// operator_associator.rs - Associate predicates with their appropriate operators
//
// This module handles the logic of determining which operator owns each predicate

use crate::pipeline::refiner::flattener::{FlatOperatorKind, FlatPredicate, FlatSegment};
use crate::pipeline::refiner::types::*;

/// Determine which operator a predicate modifies
pub(super) fn determine_operator_ref(
    _pred: &FlatPredicate,
    flat: &FlatSegment,
    _scope_point: &ScopePoint,
    predicate_class: &PredicateClass,
) -> OperatorRef {
    match predicate_class {
        PredicateClass::FJC { left, right } => {
            find_operator_for_tables(left, right, flat, OperatorType::Join).unwrap_or_else(|| {
                log::debug!(
                    "No join operator found for FJC between {} and {}",
                    left,
                    right
                );
                OperatorRef::TopLevel
            })
        }
        PredicateClass::FIC { left, right } => {
            find_operator_for_tables(left, right, flat, OperatorType::SetOp)
                .unwrap_or(OperatorRef::TopLevel)
        }
        // F (single-table filter), Fx (non-participating), Forbidden:
        // These don't belong to any operator — they go at the top level (WHERE clause).
        PredicateClass::F { .. } | PredicateClass::Fx | PredicateClass::Forbidden { .. } => {
            OperatorRef::TopLevel
        }
    }
}

/// Find the operator that relates two tables
fn find_operator_for_tables(
    left: &str,
    right: &str,
    flat: &FlatSegment,
    operator_type: OperatorType,
) -> Option<OperatorRef> {
    for (i, op) in flat.operators.iter().enumerate() {
        let (matches_type, operator_ref) = match (&op.kind, operator_type) {
            (FlatOperatorKind::Join { .. }, OperatorType::Join) => {
                (true, OperatorRef::Join { position: i })
            }
            (FlatOperatorKind::SetOp { operator }, OperatorType::SetOp) => (
                true,
                OperatorRef::SetOp {
                    position: i,
                    operator: *operator,
                },
            ),
            // Mismatched operator type: looking for Join but found SetOp, or vice versa.
            (FlatOperatorKind::Join { .. }, OperatorType::SetOp)
            | (FlatOperatorKind::SetOp { .. }, OperatorType::Join) => {
                (false, OperatorRef::TopLevel)
            }
        };

        if matches_type {
            // Check if this operator involves both tables
            let left_in_left = op.left_tables.contains(&left.to_string());
            let left_in_right = op.right_tables.contains(&left.to_string());
            let right_in_left = op.left_tables.contains(&right.to_string());
            let right_in_right = op.right_tables.contains(&right.to_string());

            // The predicate belongs to this operator if one table is on the left
            // and the other is on the right (in either order)
            if (left_in_left && right_in_right) || (left_in_right && right_in_left) {
                log::debug!(
                    "{:?} predicate for {} and {} belongs to operator {}",
                    operator_type,
                    left,
                    right,
                    i
                );
                return Some(operator_ref);
            }
        }
    }
    None
}

#[derive(Debug, Copy, Clone)]
enum OperatorType {
    Join,
    SetOp,
}
