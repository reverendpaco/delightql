// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Edge selection, the walk, and the references of an edge body's
//! conditions (grounding-and-mention-law: `&` HOLDS ONLY DECLARED EDGES, A
//! CONTEXT IS A SYMBOL AND A LICENSE, A PROVABLE MISS IS AN ERROR, AN EDGE
//! IS A PAIR-SET, A CHAIN READS EACH TERM ONCE). An edge is selected by its
//! two terms' canonical spellings, an unordered pair, in one context; an
//! unknown context and an absent edge or path are provable misses and
//! refuse; a walk takes the one path the context's declared edges give.

use crate::pipeline::middle::core::graph::Judging;
use crate::pipeline::middle::core::ids::{BinderId, TruthId};
use crate::pipeline::middle::core::node::walk::{reachable, Child};
use crate::pipeline::middle::core::node::{Occurrence, TruthKind};
use crate::pipeline::middle::core::refuse::Refusal;
use delightql_types::diagnostic::Er;

/// One declared edge's selection keys.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Key<'a> {
    pub(crate) left: &'a str,
    pub(crate) right: &'a str,
    pub(crate) context: &'a str,
}

impl Key<'_> {
    fn joins(&self, a: &str, b: &str) -> bool {
        (self.left == a && self.right == b) || (self.left == b && self.right == a)
    }

    fn other(&self, end: &str) -> Option<&str> {
        match (self.left == end, self.right == end) {
            (true, _) => Some(self.right),
            (_, true) => Some(self.left),
            _ => None,
        }
    }
}

/// The edge (by index) a direct use of `a & b` in `context` selects.
pub(crate) fn select(keys: &[Key<'_>], context: &str, a: &str, b: &str) -> Result<usize, Refusal> {
    let held = in_context(keys, context)?;
    let found: Vec<usize> = held.iter().copied().filter(|k| keys[*k].joins(a, b)).collect();
    match found.as_slice() {
        [one] => Ok(*one),
        [] => Err(Er::EdgeMiss {
            message: format!(
                "no edge declared between {a} and {b} in '::{context}': `&` holds only declared edges, and a \
                 term's canonical spelling is its identity, so the use spells its terms as the declaration does \
                 (the context's edges: {})",
                listed(keys, &held)
            ),
        }
        .into()),
        [_, _, ..] => Err(ambiguous(format!(
            "the edge between {a} and {b} in '::{context}' is declared in more than one namespace in view; one \
             pair in one context names one edge"
        ))),
    }
}

/// Whether the edges of `context` form a cycle: more edges than a forest
/// over their terms holds.
pub(crate) fn cyclic(keys: &[Key<'_>], context: &str) -> bool {
    let held: Vec<&Key<'_>> = keys.iter().filter(|k| k.context == context).collect();
    let mut terms: Vec<&str> = held.iter().flat_map(|k| [k.left, k.right]).collect();
    terms.sort_unstable();
    terms.dedup();
    let mut seen = vec![false; terms.len()];
    let mut components = 0;
    for start in 0..terms.len() {
        if seen[start] {
            continue;
        }
        components += 1;
        seen[start] = true;
        let mut pending = vec![start];
        while let Some(at) = pending.pop() {
            for k in &held {
                if let Some(next) = k.other(terms[at]) {
                    if let Ok(j) = terms.binary_search(&next) {
                        if !seen[j] {
                            seen[j] = true;
                            pending.push(j);
                        }
                    }
                }
            }
        }
    }
    held.len() + components > terms.len()
}

/// The one path a walk `from && to` takes through the edges of `context`:
/// the edges (by index) in walking order, each with the term it leaves
/// from. No path, and more than one, refuse.
pub(crate) fn path(keys: &[Key<'_>], context: &str, from: &str, to: &str) -> Result<Vec<(usize, String)>, Refusal> {
    let held = in_context(keys, context)?;
    let mut found: Vec<Vec<(usize, String)>> = Vec::new();
    let mut trail: Vec<(usize, String)> = Vec::new();
    let mut visited = vec![from.to_string()];
    walk(keys, &held, from, to, &mut visited, &mut trail, &mut found);
    match found.len() {
        1 => Ok(found.remove(0)),
        0 => Err(Er::EdgeMiss {
            message: format!(
                "the edges of '::{context}' give no path from {from} to {to} (the context's edges: {})",
                listed(keys, &held)
            ),
        }
        .into()),
        n => Err(ambiguous(format!(
            "the edges of '::{context}' give {n} paths from {from} to {to}; a walk takes the one path its \
             context determines"
        ))),
    }
}

fn walk(
    keys: &[Key<'_>],
    held: &[usize],
    at: &str,
    to: &str,
    visited: &mut Vec<String>,
    trail: &mut Vec<(usize, String)>,
    found: &mut Vec<Vec<(usize, String)>>,
) {
    if found.len() > 1 {
        return;
    }
    for k in held {
        let Some(next) = keys[*k].other(at) else {
            continue;
        };
        if trail.iter().any(|(used, _)| used == k) || visited.iter().any(|v| v == next) {
            continue;
        }
        trail.push((*k, at.to_string()));
        if next == to {
            found.push(trail.clone());
        } else {
            visited.push(next.to_string());
            walk(keys, held, next, to, visited, trail, found);
            visited.pop();
        }
        trail.pop();
    }
}

/// The edges declared in `context`; a context no edge declares is unknown.
fn in_context(keys: &[Key<'_>], context: &str) -> Result<Vec<usize>, Refusal> {
    let held: Vec<usize> = (0..keys.len()).filter(|k| keys[*k].context == context).collect();
    if held.is_empty() {
        let mut contexts: Vec<&str> = keys.iter().map(|k| k.context).collect();
        contexts.sort_unstable();
        contexts.dedup();
        return Err(Er::UnknownContext {
            message: format!(
                "an unknown context: no edge in view declares '::{context}', and a context no edge declares is an \
                 error, never an empty relation (the contexts in view: {})",
                if contexts.is_empty() {
                    "none".to_string()
                } else {
                    contexts.iter().map(|c| format!("'::{c}'")).collect::<Vec<_>>().join(", ")
                }
            ),
        }
        .into());
    }
    Ok(held)
}

fn listed(keys: &[Key<'_>], held: &[usize]) -> String {
    held.iter().map(|k| format!("{} & {}", keys[*k].left, keys[*k].right)).collect::<Vec<_>>().join(", ")
}

/// THE REFERENCES of an edge body's conditions, where they resolve over
/// the edge's two endpoint reads `ends`: no condition reaches a relation
/// (a named function stands for its body), and at least one top-level
/// conjunct is a connection, a comparison of a value over one endpoint with
/// a value over the other.
pub(crate) fn conditions(arena: &impl Judging, truths: &[TruthId], ends: [BinderId; 2]) -> Result<(), Refusal> {
    if truths.iter().any(|t| !reachable(arena, &[Child::Truth(*t)]).rels.is_empty()) {
        return Err(shape("a condition reaches a relation (a probe, a subquery, or a function whose body reads one)"));
    }
    let mut conjuncts: Vec<TruthId> = Vec::new();
    let mut pending: Vec<TruthId> = truths.to_vec();
    while let Some(t) = pending.pop() {
        match arena.truth(t).kind() {
            TruthKind::And(parts) => pending.extend(parts.iter().copied()),
            _ => conjuncts.push(t),
        }
    }
    let over = |e| -> Option<BinderId> {
        let binders: Vec<BinderId> = arena
            .expr(e)
            .occurrences()
            .iter()
            .filter_map(|o| match o {
                Occurrence::Binder(b) => Some(*b),
                Occurrence::Merge(_) => None,
            })
            .collect();
        match binders.as_slice() {
            [one] if ends.contains(one) => Some(*one),
            _ => None,
        }
    };
    let connects = conjuncts.iter().any(|t| match arena.truth(*t).kind() {
        TruthKind::Cmp { left, right, .. } => matches!((over(*left), over(*right)), (Some(a), Some(b)) if a != b),
        _ => false,
    });
    if connects {
        Ok(())
    } else {
        Err(shape("no condition compares a value over one endpoint with a value over the other (a connection)"))
    }
}

fn ambiguous(message: String) -> Refusal {
    delightql_types::diagnostic::Resolution::Ambiguous { message }.into()
}

fn shape(part: &str) -> Refusal {
    Er::BodyShape {
        message: format!("an edge body outside the simple shape: {part}"),
    }
    .into()
}
