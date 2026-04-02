// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
    // For SetOps: group all operators at the same position, then check if
    // both tables appear anywhere in that group. This handles three-way+
    // unions where correlation spans non-adjacent operands.
    if matches!(operator_type, OperatorType::SetOp) {
        let mut position_groups: std::collections::HashMap<
            usize,
            (Vec<String>, Option<OperatorRef>),
        > = std::collections::HashMap::new();
        for (i, op) in flat.operators.iter().enumerate() {
            if let FlatOperatorKind::SetOp { operator } = &op.kind {
                let (tables, op_ref) = position_groups.entry(op.position).or_insert_with(|| {
                    (
                        Vec::new(),
                        Some(OperatorRef::SetOp {
                            position: i,
                            operator: *operator,
                        }),
                    )
                });
                for t in &op.left_tables {
                    if !tables.contains(t) {
                        tables.push(t.clone());
                    }
                }
                for t in &op.right_tables {
                    if !tables.contains(t) {
                        tables.push(t.clone());
                    }
                }
            }
        }
        for (tables, op_ref) in position_groups.values() {
            if tables.contains(&left.to_string()) && tables.contains(&right.to_string()) {
                return op_ref.clone();
            }
        }
        return None;
    }

    // For joins: check pairwise as before
    for (i, op) in flat.operators.iter().enumerate() {
        if matches!(op.kind, FlatOperatorKind::Join { .. }) {
            let left_in_left = op.left_tables.contains(&left.to_string());
            let left_in_right = op.right_tables.contains(&left.to_string());
            let right_in_left = op.left_tables.contains(&right.to_string());
            let right_in_right = op.right_tables.contains(&right.to_string());

            if (left_in_left && right_in_right) || (left_in_right && right_in_left) {
                return Some(OperatorRef::Join { position: i });
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
