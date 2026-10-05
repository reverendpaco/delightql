// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Inchoate activation (W5 #18): an inchoate read yields rows only when a
//! reference somewhere in the statement reaches one of its latent
//! dimensions. The reaching reference can stand anywhere after the read,
//! so the fact is decided over the completed graph, once, from the nodes
//! the statement holds. The heading of an inchoate read does not depend
//! on it.

use crate::pipeline::middle::core::graph::{Arena, Graph};
use crate::pipeline::middle::core::ids::{BinderId, RelId};
use crate::pipeline::middle::core::node::walk;
use crate::pipeline::middle::core::node::{ExprKind, Qual, ReadAccess, RelKind};
use std::collections::{BTreeMap, BTreeSet};

/// Which inchoate reads a statement activates.
pub(crate) struct Activation {
    activated: BTreeSet<RelId>,
}

impl Activation {
    /// Whether an inchoate read's rows are its source's (activated) or
    /// none.
    pub(crate) fn is_activated(&self, read: RelId) -> bool {
        self.activated.contains(&read)
    }
}

/// The one analysis: an inchoate read is activated when a value the
/// statement holds reads a position of the member bound to it (only a
/// position can: a name reaches no latent dimension).
pub(crate) fn of(graph: &Graph) -> Activation {
    let reach = walk::reachable(graph, &graph.statement().roots());
    let inchoate: BTreeSet<RelId> = reach
        .rels
        .iter()
        .copied()
        .filter(|r| {
            matches!(
                graph.rel(*r).kind(),
                RelKind::Read {
                    access: ReadAccess::Unasked,
                    ..
                }
            )
        })
        .collect();
    let mut bound: BTreeMap<BinderId, RelId> = BTreeMap::new();
    for r in &reach.rels {
        if let RelKind::Run(run) = graph.rel(*r).kind() {
            for qual in run.quals() {
                if let Qual::Member(m) = qual {
                    if inchoate.contains(&m.rel()) {
                        bound.insert(m.binder(), m.rel());
                    }
                }
            }
        }
    }
    let activated = reach
        .exprs
        .iter()
        .filter_map(|e| match graph.expr(*e).kind() {
            ExprKind::Col(binder, _) => bound.get(binder).copied(),
            _ => None,
        })
        .collect();
    Activation { activated }
}
