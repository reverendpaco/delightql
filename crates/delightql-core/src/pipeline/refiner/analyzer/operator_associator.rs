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
        PredicateClass::FJC { left, right } => find_operator_for_tables(left, right, flat)
            .unwrap_or_else(|| {
                log::debug!(
                    "No join operator found for FJC between {:?} and {:?}",
                    left,
                    right
                );
                OperatorRef::TopLevel
            }),
        // F (single-table filter), Fx (non-participating), Forbidden:
        // These don't belong to any operator — they go at the top level (WHERE clause).
        PredicateClass::F { .. } | PredicateClass::Fx | PredicateClass::Forbidden { .. } => {
            OperatorRef::TopLevel
        }
    }
}

/// Find the operator that relates two tables
fn find_operator_for_tables(
    left: &crate::names::ScopeId,
    right: &crate::names::ScopeId,
    flat: &FlatSegment,
) -> Option<OperatorRef> {
    for (i, op) in flat.operators.iter().enumerate() {
        let FlatOperatorKind::Join { .. } = op.kind;
        let left_in_left = op.left_tables.contains(left);
        let left_in_right = op.right_tables.contains(left);
        let right_in_left = op.left_tables.contains(right);
        let right_in_right = op.right_tables.contains(right);

        if (left_in_left && right_in_right) || (left_in_right && right_in_left) {
            return Some(OperatorRef::Join { position: i });
        }
    }
    None
}
