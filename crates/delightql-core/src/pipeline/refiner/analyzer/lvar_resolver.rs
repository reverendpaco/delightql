// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// lvar_resolver.rs - Handle Lvar bindings and USING column interpretation
//
// This module handles Lvar extraction and binding creation

use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::flattener::{FlatOperatorKind, FlatSegment};
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;

/// Extract Lvar bindings from tables with positional patterns
pub(super) fn extract_lvar_bindings(
    segment: &FlatSegment,
    identities: &crate::names::Registry,
) -> HashMap<crate::names::Sym, Vec<LvarBinding>> {
    let mut lvar_map: HashMap<crate::names::Sym, Vec<LvarBinding>> = HashMap::new();

    for table in &segment.tables {
        // Extract Lvars from positional patterns
        let lvars = extract_lvars_from_access(&table.access, identities);

        for (lvar_name, _position) in lvars {
            lvar_map.entry(lvar_name).or_default().push(LvarBinding {
                table: table.identity,
            });
        }

        // Also extract Lvars from anonymous table headers for implicit
        // unification. Only a BARE header takes part: qualification is part
        // of an lvar's complete name, so an aliased table's `x.city` and a
        // bare `city` are two names, and two names do not unify. Reading the
        // published spelling alone sees one name where there are two, and
        // merges relations that should cross.
        if let Some(ref anon_data) = table.anonymous_data {
            if let Some(ref headers) = anon_data.body.header {
                for header in headers.iter() {
                    match header.term() {
                        Some(resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence { column, .. }),
                        ))) if identities.scope_of(column) == table.identity
                            && identities.addressing(column) == crate::names::Addressing::Bare =>
                        {
                            if let Some(name) = identities.published_sym(column) {
                                lvar_map.entry(name).or_default().push(LvarBinding {
                                    table: table.identity,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    lvar_map
}

/// Extract Lvars from a access (for positional patterns)
fn extract_lvars_from_access(
    spec: &resolved::Access,
    identities: &crate::names::Registry,
) -> Vec<(crate::names::Sym, usize)> {
    match spec {
        resolved::Access::Slots(expressions) => {
            let mut lvars = Vec::new();
            for (position, slot) in expressions.iter().enumerate() {
                // Both a slot that BOUND a name and a slot constrained by a
                // qualified reference name a column of this relation, and
                // both have always taken part in positional unification.
                let column = match slot {
                    resolved::Slot::Bind(column) => Some(*column),
                    // A REUSE addresses a column by name; that is exactly the
                    // slot that takes part in positional unification.
                    resolved::Slot::Reuse(NamedReference(ColumnOccurrence { column, .. })) => {
                        Some(*column)
                    }
                    resolved::Slot::Constraint(_) | resolved::Slot::Anon => None,
                };
                if let Some(column) = column {
                    if let Some(name) = identities.published_sym(column) {
                        lvars.push((name, position));
                    }
                }
            }
            lvars
        }
        // Glob (*), Dequalify (*.(cols)), Bare (.) — no positional lvar bindings.
        // Dequalify's columns are handled separately via the USING join mechanism.
        resolved::Access::All
        | resolved::Access::Dequalify(_)
        | resolved::Access::DequalifyAll
        | resolved::Access::Unasked => Vec::new(),
    }
}

/// Make positional unification's implicit correspondence explicit.
///
/// A shared lvar name across a join's two operands IS a correspondence, and
/// it lands on the OPERATOR that directs it. As a synthetic predicate it put
/// a non-truth into the predicate pool and gave the join's correspondence a
/// second home.
pub(super) fn create_lvar_using_predicates(
    flat: &mut FlatSegment,
    identities: &crate::names::Registry,
) {
    // Extract Lvar mappings first
    let lvar_map = extract_lvar_bindings(flat, identities);
    let anonymous = {
        let spelling = identities.intern("_", false);
        identities.canonical(spelling)
    };

    // Decide first, over the whole segment, then write. The decision reads
    // each operator's operands and the operator's own correspondence slot,
    // and those cannot be read and written in one pass.
    let mut decided: Vec<(usize, Vec<crate::names::Sym>)> = Vec::new();
    for (op_idx, op) in flat.operators.iter().enumerate() {
        let FlatOperatorKind::Join { correspondence } = &op.kind;
        // A correspondence the dequalifying access already named wins.
        if correspondence.is_some() {
            continue;
        }

        // A pair of single-table operands is what positional unification
        // names; anything wider is left alone.
        let ([_left], [_right]) = (op.left_tables.as_slice(), op.right_tables.as_slice()) else {
            continue;
        };

        // Find Lvars shared between left and right operands
        let mut shared_lvars = Vec::new();
        for (lvar_name, bindings) in &lvar_map {
            // Skip anonymous variables
            if *lvar_name == anonymous {
                continue;
            }
            let in_left = bindings.iter().any(|b| op.left_tables.contains(&b.table));
            let in_right = bindings.iter().any(|b| op.right_tables.contains(&b.table));
            if in_left && in_right {
                shared_lvars.push(*lvar_name);
            }
        }
        if shared_lvars.is_empty() {
            continue;
        }
        // Sort for deterministic output
        shared_lvars.sort();
        log::debug!(
            "Correspondence for join {} from shared Lvars: {:?}",
            op_idx,
            shared_lvars
        );
        decided.push((op_idx, shared_lvars));
    }

    for (op_idx, columns) in decided {
        let FlatOperatorKind::Join { correspondence } = &mut flat.operators[op_idx].kind;
        *correspondence = Some(resolved::Correspondence::new(columns));
    }
}
