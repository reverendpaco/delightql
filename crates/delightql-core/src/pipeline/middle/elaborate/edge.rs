// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Edge steps `&` and `&&` (grounding-and-mention-law). A chain of edge
//! steps is ONE run: each term read once as a member, each selected edge's
//! conditions — read from its declared body — written into the run as its
//! conditions, each resolved over that edge's two endpoints alone (EACH EDGE
//! KEEPS ITS OWN SCOPE); the run then places and classes them as any run's
//! conditions (a connection matches occurrences; a filter over one endpoint
//! is a per-row verdict). A walk reads its path's interior terms and
//! publishes its two outer endpoints only.

use super::Elaborator;
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::decide::er::{self, Key};
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{BinderId, TruthId};
use crate::pipeline::middle::core::node::run::{Born, MergeRequest};
use crate::pipeline::middle::core::node::Consumer;
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    Chain, Continuation, DdlBody, ErJoinStep, GroundForm, GroundMention, InnerRelationPattern, MemberRole,
    Relation, TruthExpression,
};
use crate::pipeline::middle::select::Edge;

/// The edge steps of one chain read so far.
pub(super) struct EdgeRun {
    context: String,
    /// Each term read, by canonical spelling, with its member's binder.
    terms: Vec<(String, BinderId)>,
    edges: Vec<Edge>,
    walked: bool,
}

/// One edge's declared body: its two endpoint reads, in declared order,
/// and its conditions.
struct Body {
    reads: [Chain; 2],
    conditions: Vec<TruthExpression>,
}

impl Elaborator<'_, '_> {
    /// One `&` or `&&` step at chain index `at`, the run's last member being
    /// its left term.
    pub(super) fn edge_step(&mut self, chain: &Chain, at: usize, step: &ErJoinStep, run: &mut Option<EdgeRun>) -> Result<(), Refusal> {
        // The run's first left term, read before the run opened: it is
        // judged against the first edge's body as every other term is.
        let mut opening: Option<&Chain> = None;
        if run.is_none() {
            // The left term is the read written just before the step: the
            // chain's head, or the member before it.
            let read = match chain.steps()[..at].iter().rev().find(|s| !matches!(s.form(), Continuation::Access { .. })) {
                Some(s) => match s.form() {
                    Continuation::Member { rhs, .. } => Some(rhs),
                    _ => None,
                },
                None => Some(chain),
            };
            let written = read.and_then(read_name);
            opening = read;
            if written.is_none() || written != term_name(&step.left_spelling) {
                return Err(refuse::outside("an edge step whose left term is not the read written before it"));
            }
            let last = self
                .scopes
                .last()
                .and_then(|s| s.run.members().last().map(|m| m.binder()))
                .ok_or_else(|| refuse::outside("an edge step with no term to its left"))?;
            *run = Some(EdgeRun {
                context: step.context.clone(),
                terms: vec![(step.left_spelling.clone(), last)],
                edges: self.world.edges(&self.at)?,
                walked: false,
            });
        }
        // A canonical term holds no edge: a left spelling that does is a
        // relation's own interior written as an edge's left term.
        if step.left_spelling.contains('&') {
            return Err(refuse::outside("an edge step standing first in a relation's own interior"));
        }
        let state = run.as_mut().expect("the edge run just opened or continued");
        if state.walked {
            return Err(refuse::outside("an edge step after a walk in one chain"));
        }
        if state.context != step.context {
            return Err(delightql_types::diagnostic::Er::MixedContexts {
                message: format!(
                    "one chain names one context, and this one names '::{}' and '::{}'; split the chain",
                    state.context, step.context
                ),
            }
            .into());
        }
        // A CHAIN READS EACH TERM ONCE: a term written twice in one chain,
        // or a walk ending where it starts, would read one term twice.
        if state.terms.iter().any(|(t, _)| *t == step.right_spelling) {
            return Err(delightql_types::diagnostic::Er::Endpoint {
                message: format!(
                    "{} is written twice in this chain, which reads each of its terms exactly once (an interior \
                     term is one read its two edges share); to read a relation twice, give one reading its own \
                     rule and name that rule",
                    step.right_spelling
                ),
            }
            .into());
        }
        let mut edges = state.edges.clone();
        // EVERY NAME IS TOTAL: a term qualified by a namespace reaches the
        // edges that namespace declares, their terms spelled under the use's
        // qualifier.
        if let Some((_, Some(qualifier))) = read_identifier(&step.rhs) {
            if let Some(prefix) = qualifier_prefix(&step.right_spelling) {
                for edge in self.world.edges_declared_by(&self.at, &qualifier)? {
                    edges.push(Edge {
                        left: format!("{prefix}.{}", edge.left),
                        right: format!("{prefix}.{}", edge.right),
                        ..edge
                    });
                }
            }
        }
        let keys: Vec<Key<'_>> = edges
            .iter()
            .map(|e| Key {
                left: &e.left,
                right: &e.right,
                context: &e.context,
            })
            .collect();
        let context = step.context.as_str();
        let (left, right) = (step.left_spelling.as_str(), step.right_spelling.as_str());
        let path: Vec<(usize, String)> = if step.transitive {
            er::path(&keys, context, left, right)?
        } else {
            match er::select(&keys, context, left, right) {
                Ok(k) => vec![(k, left.to_string())],
                // A direct run's edges join consecutive written terms: an
                // interior term is one read shared by the two edges adjacent
                // to it (A CHAIN READS EACH TERM ONCE), so no edge from an
                // earlier term reaches past the term written before it.
                Err(miss) => return Err(miss),
            }
        };
        if er::cyclic(&keys, context) {
            return Err(refuse::unruled(
                "when a cycle in an edge context's graph refuses: at its declaration, or only at a walk",
            ));
        }
        if let (Some(read), Some((k, _))) = (opening, path.first()) {
            let site = self.world.body_of(&edges[*k].family)?;
            self.same_relation(read, &site, context)?;
        }
        let mut bodies = Vec::with_capacity(path.len());
        for (k, from) in &path {
            let body = self.edge_body(&edges[*k])?;
            let declared_left = read_name(&body.reads[0]);
            // The run's column order under a reversed direct edge is not
            // decided: the run refuses every read of it (`order_held`).
            if declared_left.as_ref() != term_name(from).as_ref() && !step.transitive {
                self.scopes.last_mut().expect("the edge's run").reversed = true;
            }
            bodies.push((*k, from.clone(), body));
        }
        let marked = step.role == MemberRole::Optional;
        let mut binders: Vec<BinderId> = vec![self.term_binder(state, left)?];
        for (i, (k, from, body)) in bodies.iter().enumerate() {
            let to = keys[*k].other_end(from).to_string();
            let last = i + 1 == bodies.len();
            let site = self.world.body_of(&edges[*k].family)?;
            let read = if last {
                step.rhs.clone()
            } else {
                body.reads
                    .iter()
                    .find(|r| read_name(r) == term_name(&to))
                    .cloned()
                    .ok_or_else(|| refuse::contract("an edge body that does not read its term"))?
            };
            self.same_relation(&read, &site, context)?;
            let mut term = self.term(&read)?;
            if read.has_steps() {
                let merge = std::mem::replace(&mut term.merge, MergeRequest::None);
                term = self.chain_term(&read, term)?;
                term.merge = merge;
            }
            if !last {
                term.scope = None;
                term.names_scope = false;
                term.born = Born::Interior;
            }
            self.push_term(term, marked && last, false)?;
            let binder = self
                .scopes
                .last()
                .and_then(|s| s.run.members().last().map(|m| m.binder()))
                .ok_or_else(|| refuse::contract("an edge term pushed and not bound"))?;
            let ends = [*binders.last().expect("the left end"), binder];
            let names = [
                read_name(&body.reads[0]).ok_or_else(|| refuse::contract("an edge body read with no name"))?,
                read_name(&body.reads[1]).ok_or_else(|| refuse::contract("an edge body read with no name"))?,
            ];
            let scope = if term_name(from).as_ref() == Some(&names[0]) {
                [(names[0].clone(), ends[0]), (names[1].clone(), ends[1])]
            } else {
                [(names[0].clone(), ends[1]), (names[1].clone(), ends[0])]
            };
            let truths = self.edge_conditions(&body.conditions, scope, site)?;
            er::conditions(&self.b, &truths, ends)?;
            if marked && truths.iter().any(|t| self.reads_one_end(*t, ends)) {
                return Err(refuse::unruled(
                    "whether an edge's filter is part of what an outer peer's edge reaches, or a condition placed \
                     after the padding",
                ));
            }
            for truth in truths {
                self.scopes.last_mut().expect("an open scope").run.push_guard(&self.b, truth);
            }
            binders.push(binder);
            state.terms.push((to, binder));
        }
        if step.transitive {
            state.walked = true;
        }
        Ok(())
    }

    /// The binder of the term read under `spelling` in this edge run.
    fn term_binder(&self, run: &EdgeRun, spelling: &str) -> Result<BinderId, Refusal> {
        run.terms
            .iter()
            .rev()
            .find(|(t, _)| t == spelling)
            .map(|(_, b)| *b)
            .ok_or_else(|| refuse::outside("an edge step whose left term is not the term written before it"))
    }

    /// One selected edge's declared body. Declaring one pair twice in one
    /// context refuses; two declarations of one pair from its two sides are
    /// held (whether a pair has an orientation is not ruled).
    fn edge_body(&self, edge: &Edge) -> Result<Body, Refusal> {
        let clauses = self.input.family_clauses(&edge.family)?;
        let mut bodies = Vec::new();
        for (_, clause, _) in clauses {
            let DdlBody::Relational(query) = clause.body else {
                return Err(refuse::contract("an edge clause whose body is not relational"));
            };
            bodies.push(body_of(query.body)?);
        }
        if bodies.len() > 1 {
            let orientations: Vec<Option<Name>> = bodies.iter().map(|b| read_name(&b.reads[0])).collect();
            if orientations.iter().any(|o| *o != orientations[0]) {
                return Err(refuse::unruled(
                    "whether one pair declared from its two sides is one edge or two",
                ));
            }
            return Err(delightql_types::diagnostic::Resolution::Ambiguous {
                message: format!(
                    "the edge between {} and {} in '::{}' is declared {} times; one pair in one context is one \
                     declaration",
                    edge.left,
                    edge.right,
                    edge.context,
                    bodies.len()
                ),
            }
            .into());
        }
        bodies.pop().ok_or_else(|| refuse::contract("an edge with no clause"))
    }

    /// An edge's conditions, each resolved over the edge's two endpoints
    /// alone, in the declaration environment of the edge's body: where the
    /// body stands, with none of the using statement's query-local blocks or
    /// formals in view (a consulted body is lexical, never dynamic).
    fn edge_conditions(
        &mut self,
        conditions: &[TruthExpression],
        scope: [(Name, BinderId); 2],
        site: crate::pipeline::middle::select::Standpoint,
    ) -> Result<Vec<TruthId>, Refusal> {
        self.apart(0, 0, |e| {
            let at = std::mem::replace(&mut e.at, site);
            e.edge_scope = Some(scope);
            let truths = conditions.iter().map(|c| e.truth(c, Consumer::Filter)).collect();
            e.at = at;
            truths
        })
    }

    /// Whether a condition reads exactly one of an edge's endpoints: a
    /// filter.
    fn reads_one_end(&self, truth: TruthId, ends: [BinderId; 2]) -> bool {
        let fv = self.b.truth(truth).occurrences();
        let reads = |b: BinderId| fv.contains(&crate::pipeline::middle::core::node::Occurrence::Binder(b));
        reads(ends[0]) != reads(ends[1])
    }

    /// A term's relation is the same identity where the edge is used (no
    /// query-local definition shadowing it there) and where its body
    /// stands; otherwise the use refuses, for the edge's conditions speak of
    /// its body's relation.
    fn same_relation(&self, read: &Chain, site: &crate::pipeline::middle::select::Standpoint, context: &str) -> Result<(), Refusal> {
        let Some((name, qualifier)) = read_identifier(read) else {
            return Ok(());
        };
        let q = qualifier.as_ref();
        let shadowed = q.is_none() && self.claimed_kind(&name).is_some();
        let here = self.world.refer(&self.at, &name, q)?.and_then(|r| r.entity_id());
        let there = self.world.refer(site, &name, q)?.and_then(|r| r.entity_id());
        if here == there && !shadowed {
            Ok(())
        } else {
            Err(refuse::edge_term_world(&name.to_string(), context))
        }
    }

}

impl Key<'_> {
    /// The other end of an edge from `end`.
    fn other_end(&self, end: &str) -> &str {
        if self.left == end {
            self.right
        } else {
            self.left
        }
    }
}

/// A declared edge's body: its head read and its member read, in declared
/// order, and its conditions.
fn body_of(chain: Chain) -> Result<Body, Refusal> {
    let first = match chain.head_access() {
        Some(access) => Chain::read_head(chain.head().form().clone(), access.clone()),
        None => Chain::authored(chain.head().form().clone()),
    };
    let mut second = None;
    let mut conditions = Vec::new();
    for step in chain.steps() {
        match step.form() {
            Continuation::Access { .. } => {}
            Continuation::Member { rhs, correlation, .. } if second.is_none() => {
                second = Some(rhs.clone());
                if let Some(c) = correlation.as_ref().and_then(|c| c.condition()) {
                    conditions.push(c.clone());
                }
            }
            Continuation::Restrict { condition, .. } => conditions.push(condition.clone()),
            _ => return Err(refuse::contract("a stored edge body outside the simple shape")),
        }
    }
    let second = second.ok_or_else(|| refuse::contract("an edge body with one read"))?;
    Ok(Body {
        reads: [first, second],
        conditions,
    })
}

/// The relation a read names, and the qualifier it is written under.
fn read_identifier(read: &Chain) -> Option<(Name, Option<crate::pipeline::middle::facade::Qualifier>)> {
    let identifier = match read.head().form() {
        GroundForm::Reference(Relation::Ground {
            mention: GroundMention::Named { identifier, .. },
        })
        | GroundForm::Reference(Relation::InnerRelation {
            pattern: InnerRelationPattern::Indeterminate { identifier, .. },
            ..
        }) => identifier,
        _ => return None,
    };
    let qualifier = (!identifier.namespace_path.is_empty()).then(|| identifier.namespace_path.qualifier());
    Some((identifier.name.clone(), qualifier))
}

fn read_name(read: &Chain) -> Option<Name> {
    read_identifier(read).map(|(n, _)| n)
}

/// The qualifier a canonical term spelling is written under: its head
/// before the last `.`, where it has one.
fn qualifier_prefix(spelling: &str) -> Option<&str> {
    let head = spelling.split('(').next()?.trim();
    head.rsplit_once('.').map(|(prefix, _)| prefix)
}

/// The relation name a canonical term spelling reads: its name before the
/// access parenthesis, after any qualifier.
fn term_name(spelling: &str) -> Option<Name> {
    let head = spelling.split('(').next()?.trim();
    let last = head.rsplit(['.', ':']).next()?;
    (!last.is_empty()).then(|| Name::new(last))
}

/// The one hold for a read of a run's column order where an edge in it is
/// traversed against its declared orientation.
pub(super) fn order_held() -> Refusal {
    refuse::unruled(
        "the column order of an edge used against its declared orientation, which this statement reads (by \
         position, as a whole heading, or by publishing the run)",
    )
}
