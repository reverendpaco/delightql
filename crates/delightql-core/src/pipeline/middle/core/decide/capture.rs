// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A relation-valued actual is CLOSED (top-grammar, the note on
//! relation-valued higher-order actuals): written among a call's arguments,
//! it may read its own sources, literals, ordinary arguments and target
//! callables, never a caller lvar or column. Its free binders are the
//! evidence: a fixpoint's frontier is a relation it reads, and any other
//! free binder is a row of the call's caller. A relation landed by a pipe
//! is the pipe's value, not an argument, and is not judged here.

use crate::pipeline::middle::core::graph::Judging;
use crate::pipeline::middle::core::ids::BinderId;
use crate::pipeline::middle::core::instance::Actual;
use crate::pipeline::middle::core::node::walk::{self, Child};
use crate::pipeline::middle::core::node::{ExprKind, ReadSource, RelKind};
use crate::pipeline::middle::core::refuse::{self, Refusal};

/// Every written relation actual of one application is closed.
pub(crate) fn relation_actuals(arena: &impl Judging, formals: &[Actual]) -> Result<(), Refusal> {
    for formal in formals {
        if let Actual::Relation { rel, landed: false } = formal {
            let fv = arena.rel(*rel).fv();
            if fv.is_empty() {
                continue;
            }
            let reach = walk::reachable(arena, &[Child::Rel(*rel)]);
            let frontiers: Vec<BinderId> = reach
                .rels
                .iter()
                .filter_map(|r| match arena.rel(*r).kind() {
                    RelKind::Read {
                        source: ReadSource::Frontier(b),
                        ..
                    } => Some(*b),
                    _ => None,
                })
                .collect();
            let caller: Vec<BinderId> = fv.iter().copied().filter(|b| !frontiers.contains(b)).collect();
            if caller.is_empty() {
                continue;
            }
            let read = reach.exprs.iter().find_map(|e| match arena.expr(*e).kind() {
                ExprKind::Col(b, position) if caller.contains(b) => arena
                    .binder(*b)
                    .heading()
                    .positions()
                    .get(*position as usize)
                    .and_then(|p| p.answering_name())
                    .map(|n| n.to_string()),
                _ => None,
            });
            return Err(refuse::relation_actual_capture(read.as_deref()));
        }
    }
    Ok(())
}
