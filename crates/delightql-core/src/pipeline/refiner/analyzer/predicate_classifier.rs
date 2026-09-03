// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
    identities: &crate::relation::Planning,
) -> Result<PredicateClass> {
    log::debug!(
        "classify_predicate: expr={:?}, origin={:?}",
        pred.expr,
        pred.origin
    );

    if let resolved::FilterOrigin::PositionalLiteral { source } = pred.origin {
        return Ok(PredicateClass::F { table: source });
    }

    // Check if it's an Fx (non-participating) predicate
    if pred.references.is_empty() {
        return Ok(PredicateClass::Fx);
    }

    // Determine which tables are referenced (now includes unqualified refs via schema)
    let referenced_tables = extract_referenced_tables(pred, flat, identities)?;

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
        let left = tables[0];
        let right = tables[1];

        // Determine relationship between tables
        if are_in_join_relationship(&left, &right, flat) {
            log::debug!(
                "Tables {:?} and {:?} are in join relationship -> FJC",
                left,
                right
            );
            Ok(PredicateClass::FJC { left, right })
        } else {
            log::debug!(
                "Tables {:?} and {:?} have no direct relationship",
                left,
                right
            );
            // Tables not directly related - need to check scope
            Ok(PredicateClass::F { table: left })
        }
    } else {
        // The classifier has no lawful single-owner representation for a
        // predicate spanning three or more tables. Refuse the classification
        // instead of making hash/set iteration order choose an owner.
        Ok(PredicateClass::Forbidden {
            reason: ForbiddenReason::TooManyReferencedTables {
                count: referenced_tables.len(),
            },
        })
    }
}

/// Check if two tables are in a join relationship
pub(in crate::pipeline::refiner) fn are_in_join_relationship(
    left: &crate::names::ScopeId,
    right: &crate::names::ScopeId,
    flat: &FlatSegment,
) -> bool {
    flat.operators.iter().any(|op| {
        let FlatOperatorKind::Join { .. } = op.kind;
        (op.left_tables.contains(left) && op.right_tables.contains(right))
            || (op.left_tables.contains(right) && op.right_tables.contains(left))
    })
}

/// Apply Laws 1-6 to check if classification should be forbidden
pub(super) fn apply_laws(
    initial_class: PredicateClass,
    pred: &FlatPredicate,
    context: &laws::LawContext,
    _scope_point: &ScopePoint,
    identities: &crate::relation::Planning,
) -> Result<PredicateClass> {
    match &initial_class {
        PredicateClass::FJC { left, right } => {
            // Check Law 1: Forbidden UL Fragment Join
            if let Some(reason) = laws::check_law1(&pred.expr, *left, *right, context, identities) {
                return Ok(PredicateClass::Forbidden { reason });
            }

            Ok(initial_class)
        }

        // F (single-table filter) and Fx (non-participating): no law applies
        PredicateClass::F { .. } | PredicateClass::Fx => Ok(initial_class),
        // Forbidden: already rejected by a prior law — pass through
        PredicateClass::Forbidden { .. } => Ok(initial_class),
    }
}
