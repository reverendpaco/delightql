// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// context_builder.rs - Build context structures for law checking and scope analysis
//
// This module handles construction of law checking contexts and scope sequences

use crate::pipeline::refiner::flattener::FlatSegment;
use crate::pipeline::refiner::laws;
use std::collections::HashSet;

/// Build context for law checking
pub(super) fn build_law_context(
    flat: &FlatSegment,
    _identities: &crate::relation::Planning,
) -> laws::LawContext {
    // A bag operation reaches the segment as ONE opaque relation. Which
    // tables those are is what Law 1 needs: a join condition must not reach
    // past a set operation into an arm.
    let bag_tables = flat
        .tables
        .iter()
        .filter(|table| {
            table
                .pipe_expr
                .as_ref()
                .is_some_and(|chain| chain.stands_on_bag_step())
        })
        .map(|table| table.relation.scope())
        .collect::<HashSet<_>>();

    laws::LawContext { bag_tables }
}

/// Build scope sequence for Law 5 checking
pub(super) fn build_scope_sequence(
    flat: &FlatSegment,
) -> Vec<(usize, HashSet<crate::names::ScopeId>)> {
    let mut sequence = Vec::new();
    let mut current_scope = HashSet::new();

    // Add tables to scope as they appear - use their actual positions!
    for table in &flat.tables {
        current_scope.insert(table.relation.scope());
        sequence.push((table.position, current_scope.clone()));
    }

    // Add operator positions
    for op in &flat.operators {
        sequence.push((op.position, current_scope.clone()));
    }

    sequence
}
