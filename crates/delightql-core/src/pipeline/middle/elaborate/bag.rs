// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A bag region: a run of bag steps with the comma conditions and
//! whole-heading correlations written among and after them, up to the next
//! step of another kind. Its one reading (set-operations-law THE
//! ONE-RELATION LAW, CORRELATION IS PAIR-SCOPED) builds every arm first,
//! then routes each condition's top-level conjuncts by the arms they name:
//! a conjunct naming no arm filters the result where it is written; one
//! naming one arm filters that arm's rows, as the condition written inside
//! the arm would; one naming two is their pair's correlation, every
//! conjunct of a pair being one correlation; one naming three is refused.
//! An arm is named by the qualifier it answers to (arm 0 is the first
//! step's left operand when it is one occurrence, arm k the k-th step's
//! right arm), and a bare name in a conjunct that names an arm addresses
//! the one arm carrying it. A conjunct is routed by the arms it reads, as
//! its references resolve: a nested relation's own names bind first, so a
//! spelling that matches an arm's qualifier may read no arm at all. A correlation reaches exactly its two arms:
//! owned by a union step, each arm keeps the rows that have a match in the
//! other; owned by a minus step, the earlier arm loses the rows that match
//! the minus arm, and the step subtracts nothing else. Every correlation
//! matches against the other arm as its own conditions leave it, never as
//! another correlation does.

use super::Term;
use super::Elaborator;
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::decide::setop::Gate;
use crate::pipeline::middle::core::heading::correspondence::answers_to;
use crate::pipeline::middle::core::heading::{Heading, Name};
use crate::pipeline::middle::core::ids::{BinderId, RelId, TruthId};
use crate::pipeline::middle::core::node::rel::AccessSpec;
use crate::pipeline::middle::core::node::run::{Born, MergeRequest, OpenRun};
use crate::pipeline::middle::core::node::setop::{Atom, Side};
use crate::pipeline::middle::core::node::{Consumer, Occurrence, Route, SetOpKind};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{
    self, Chain, CmpOp, Continuation, DomainExpression, Reference, SetOperator, TruthExpression, WholeHeading,
};
use std::collections::{BTreeMap, BTreeSet};

use super::integer::Site;

/// One conjunct of a condition written in a region.
pub(super) enum Role<'c> {
    /// Filters the result where it is written.
    Filter(&'c TruthExpression),
    /// Routed to the arms it names.
    Moved,
}

pub(super) struct Region<'c> {
    /// The first arm as the region's arm conditions leave it.
    first: RelId,
    /// Each later arm as its conditions leave it, by the chain index of the
    /// step it enters with.
    arms: BTreeMap<usize, RelId>,
    /// The minus steps whose correlations subtract from the arms they name.
    consumed: BTreeSet<usize>,
    /// The correlation the first step pairs by minimum multiplicity, under
    /// the acknowledged gate: its conjuncts, judged as one key.
    gated: Option<Vec<Atom>>,
    /// Each condition step's conjuncts (by chain index), in written order.
    conditions: BTreeMap<usize, Vec<Role<'c>>>,
    /// The arms' qualifiers, for the region's later steps.
    qualifiers: Vec<Name>,
}

/// Whether a step continues a bag region.
pub(super) fn continues(step: &Continuation) -> bool {
    matches!(
        step,
        Continuation::BagOp { .. } | Continuation::Restrict { .. } | Continuation::Correlate { .. }
    )
}

/// An arm as the region's conditions address it.
struct Arm {
    qualifier: Option<Name>,
    /// The chain index of the step it enters with.
    enters: usize,
    /// The operator of that step; none for the first arm.
    operator: Option<SetOperator>,
    rel: RelId,
}

/// One conjunct of a pair's correlation.
enum Conjunct<'c> {
    Written(&'c TruthExpression),
    Whole(&'c WholeHeading),
}

/// What one arm reads of another to keep its rows: the other arm, whether
/// a match keeps (a union's correlation) or removes (a minus's), and the
/// correlation's conjuncts.
struct Probe<'r, 'c> {
    other: usize,
    keeps: bool,
    conjuncts: &'r [Conjunct<'c>],
}

impl Region<'_> {
    /// The first arm, taken by the region's first step.
    pub(super) fn first(&self) -> RelId {
        self.first
    }

    /// The right arm of the bag step at `at`.
    pub(super) fn arm(&self, at: usize) -> Result<RelId, Refusal> {
        self.arms
            .get(&at)
            .copied()
            .ok_or_else(|| refuse::contract("a bag step whose arm its region did not build"))
    }

    /// Whether the minus step at `at` subtracts only through the
    /// correlations it owns.
    pub(super) fn consumed(&self, at: usize) -> bool {
        self.consumed.contains(&at)
    }

    /// The correlation the first step pairs under the gate.
    pub(super) fn gated(&self) -> Option<Vec<Atom>> {
        self.gated.clone()
    }

    /// The conjuncts of the condition step at `at`.
    pub(super) fn conjuncts(&self, at: usize) -> Option<&[Role<'_>]> {
        self.conditions.get(&at).map(Vec::as_slice)
    }

    /// The qualifiers of the region's arms.
    pub(super) fn arm_qualifiers(&self) -> &[Name] {
        &self.qualifiers
    }
}

impl Elaborator<'_, '_> {
    /// THE ONE READING of the bag region whose first step stands at
    /// `first`, its first arm the open run: `left` is the qualifier of that
    /// run when it is one occurrence, `joined` the qualifiers of the members
    /// of a run that is joined (they name no arm).
    pub(super) fn region<'c>(
        &mut self,
        chain: &'c Chain,
        first: usize,
        left: Option<Name>,
        joined: Vec<Name>,
    ) -> Result<Region<'c>, Refusal> {
        let steps = chain.steps();
        let mut arms = vec![Arm {
            qualifier: left,
            enters: first,
            operator: None,
            rel: self.close_top()?,
        }];
        for (at, step) in steps.iter().enumerate().skip(first) {
            if !continues(step.form()) {
                break;
            }
            if let Continuation::BagOp { operator, arm, .. } = step.form() {
                let qualifier = if arm.steps().is_empty() { self.head_scope(arm) } else { None };
                let rel = self.chain(arm)?;
                arms.push(Arm {
                    qualifier,
                    enters: at,
                    operator: Some(*operator),
                    rel,
                });
            }
        }
        let mut conditions: BTreeMap<usize, Vec<Role<'c>>> = BTreeMap::new();
        let mut own: BTreeMap<usize, Vec<&'c TruthExpression>> = BTreeMap::new();
        let mut pairs: BTreeMap<(usize, usize), Vec<Conjunct<'c>>> = BTreeMap::new();
        for (at, step) in steps.iter().enumerate().skip(first) {
            match step.form() {
                form if !continues(form) => break,
                Continuation::Restrict { condition, .. } => {
                    let mut parts = Vec::new();
                    conjuncts(condition, &mut parts);
                    let mut roles = Vec::with_capacity(parts.len());
                    for part in parts {
                        let named = self.named_arms(part, &arms, &joined, at)?;
                        roles.push(match named.as_slice() {
                            [] => Role::Filter(part),
                            [k] => {
                                own.entry(*k).or_default().push(part);
                                Role::Moved
                            }
                            [a, b] => {
                                pairs.entry((*a, *b)).or_default().push(Conjunct::Written(part));
                                Role::Moved
                            }
                            _ => return Err(no_pair(named.len())),
                        });
                    }
                    conditions.insert(at, roles);
                }
                Continuation::Correlate { whole } => {
                    let (l, r) = whole.arms();
                    match addressed(&arms, &joined, at, &[l.clone(), r.clone()])?.as_slice() {
                        [a, b] => pairs.entry((*a, *b)).or_default().push(Conjunct::Whole(whole)),
                        named => return Err(no_pair(named.len())),
                    }
                }
                _ => {}
            }
        }
        // The acknowledged gate pairs the first step's arms by minimum
        // multiplicity, through the step's own correlation.
        let gated = match (self.gate(), pairs.get(&(0, 1)), arms.get(1)) {
            (Gate::Closed, ..) | (_, None, _) => None,
            (_, Some(_), Some(arm)) if arm.operator == Some(SetOperator::MinusCorresponding) => None,
            (_, Some(conjuncts), _) => Some(
                conjuncts
                    .iter()
                    .map(|conjunct| self.atom(conjunct, &arms))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        };
        if gated.is_some() {
            pairs.remove(&(0, 1));
        }
        // The gate changes every correlated union its statement writes, and
        // only the first step's carrier pairs copies: a correlation another
        // union step owns under the gate is not built, never filtered.
        if self.gate() != Gate::Closed
            && pairs.keys().any(|(_, b)| arms[*b].operator != Some(SetOperator::MinusCorresponding))
        {
            return Err(refuse::outside(
                "a min_multiplicity gate over a correlation of arms other than the first step's",
            ));
        }
        let mut base: Vec<RelId> = arms.iter().map(|a| a.rel).collect();
        for (k, parts) in &own {
            let arm = &arms[*k];
            let term = arm_term(self.b.local_read(base[*k], AccessSpec::All, &self.switches)?, arm.qualifier.clone());
            base[*k] = self.restricted(term, |e, _| {
                for part in parts {
                    let truth = e.truth(part, Consumer::Filter)?;
                    e.guard(truth);
                }
                Ok(())
            })?;
        }
        let mut probes: BTreeMap<usize, Vec<Probe<'_, 'c>>> = BTreeMap::new();
        let mut consumed = BTreeSet::new();
        for ((a, b), conjuncts) in &pairs {
            let minus = arms[*b].operator == Some(SetOperator::MinusCorresponding);
            probes.entry(*a).or_default().push(Probe {
                other: *b,
                keeps: !minus,
                conjuncts,
            });
            if minus {
                consumed.insert(arms[*b].enters);
            } else {
                probes.entry(*b).or_default().push(Probe {
                    other: *a,
                    keeps: true,
                    conjuncts,
                });
            }
        }
        let mut kept = base.clone();
        for (k, list) in &probes {
            kept[*k] = self.correlated(*k, list, &arms, &base)?;
        }
        Ok(Region {
            first: kept[0],
            arms: arms.iter().zip(&kept).skip(1).map(|(arm, rel)| (arm.enters, *rel)).collect(),
            consumed,
            gated,
            conditions,
            qualifiers: arms.iter().filter_map(|a| a.qualifier.clone()).collect(),
        })
    }

    /// The arms (by index) a conjunct written at chain index `at` reads, as
    /// its references resolve where it stands (`resolve_over_arms`). One
    /// that reads no arm through a qualifier filters the result, whatever
    /// its spelling. One that does also reads the one arm carrying each
    /// name it reads from the result; a name more than one arm carries, or
    /// none, refuses (THE ONE-RELATION LAW: name one arm or none).
    fn named_arms(&mut self, part: &TruthExpression, arms: &[Arm], joined: &[Name], at: usize) -> Result<Vec<usize>, Refusal> {
        let depth = self.scopes.len();
        let judged = self.resolve_over_arms(part, arms, at);
        self.scopes.truncate(depth);
        let (mut named, bare) = match judged {
            Ok(judged) => judged,
            Err(refusal) => {
                // A qualifier naming a member of a joined first arm names no
                // arm the region can address.
                if facade::written_qualifiers(part)?.iter().any(|q| joined.contains(q)) {
                    return Err(refuse::outside("a set-operation correlation addressing a member of a joined arm"));
                }
                return Err(refusal);
            }
        };
        if named.is_empty() {
            return Ok(named);
        }
        for name in bare {
            let carriers: Vec<usize> = arms
                .iter()
                .enumerate()
                .filter(|(_, arm)| arm.enters < at || arm.operator.is_none())
                .filter(|(_, arm)| !answers_to(self.b.rel(arm.rel).heading(), &name).is_empty())
                .map(|(k, _)| k)
                .collect();
            match carriers.as_slice() {
                [k] => {
                    if !named.contains(k) {
                        named.push(*k);
                    }
                }
                _ => return Err(refuse::arm_condition_bare(&name, carriers.len())),
            }
        }
        named.sort_unstable();
        Ok(named)
    }

    /// The conjunct elaborated where it stands, over two scopes: every arm
    /// entered before `at`, each under its qualifier, in a scope that
    /// answers qualified names only (a property of that scope, so a nested
    /// region's judgment neither lifts nor replaces it), and inside them the step's result so far (the
    /// arms combined as their steps combine them), which answers a bare
    /// name as it does after the step. A nested relation's own names bind
    /// first, as everywhere. Answers the arms whose occurrences the truth
    /// reads and the names it reads from the result. The scopes are never
    /// closed and nothing built here reaches the statement; the caller
    /// drops them.
    fn resolve_over_arms(&mut self, part: &TruthExpression, arms: &[Arm], at: usize) -> Result<(Vec<usize>, Vec<Name>), Refusal> {
        let visible: Vec<usize> = (0..arms.len()).filter(|k| arms[*k].enters < at || arms[*k].operator.is_none()).collect();
        self.scopes.push(super::Scope {
            run: OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: true,
            pending: None,
        });
        // A qualifier two arms answer stands on both, naming no scope: a
        // reference that resolves to either is ambiguous.
        let shared: Vec<Name> = visible
            .iter()
            .filter_map(|k| arms[*k].qualifier.clone())
            .filter(|q| visible.iter().filter(|j| arms[**j].qualifier.as_ref() == Some(q)).count() > 1)
            .collect();
        let mut members: Vec<(BinderId, usize)> = Vec::new();
        for k in &visible {
            let arm = &arms[*k];
            let mut term = arm_term(self.b.local_read(arm.rel, AccessSpec::All, &self.switches)?, arm.qualifier.clone());
            if arm.qualifier.as_ref().is_some_and(|q| shared.contains(q)) {
                term.names_scope = false;
            }
            self.push_term(term, false, false)?;
            let binder = self
                .scopes
                .last()
                .and_then(|s| s.run.members().last().map(|m| m.binder()))
                .ok_or_else(|| refuse::contract("an arm read and not bound"))?;
            members.push((binder, *k));
        }
        let mut result: Option<RelId> = None;
        for k in &visible {
            let arm = &arms[*k];
            let read = self.b.local_read(arm.rel, AccessSpec::All, &self.switches)?;
            result = Some(match (result, arm.operator) {
                (None, _) => read,
                (Some(left), Some(SetOperator::UnionAllPositional)) => self.b.set_op(left, read, SetOpKind::Positional, None, Gate::Closed)?,
                (Some(left), Some(SetOperator::UnionCorresponding)) => self.b.set_op(left, read, SetOpKind::Corresponding, None, Gate::Closed)?,
                (Some(left), Some(SetOperator::SmartUnionAll)) => self.b.set_op(left, read, SetOpKind::Smart, None, Gate::Closed)?,
                (Some(left), Some(SetOperator::MinusCorresponding)) => self.b.minus(left, read)?,
                (Some(_), None) => return Err(refuse::contract("a later arm with no step")),
            });
        }
        let result = result.ok_or_else(|| refuse::contract("a set region with no arm"))?;
        self.scopes.push(super::Scope {
            run: OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: false,
            pending: Some(Term {
                rel: result,
                scope: None,
                route: Route::Plain,
                merge: MergeRequest::None,
                names_scope: false,
                requalifies: false,
                born: Born::Written,
            }),
        });
        self.materialize()?;
        let result_binder = self
            .scopes
            .last()
            .and_then(|s| s.run.members().last().map(|m| m.binder()))
            .ok_or_else(|| refuse::contract("the step's result read and not bound"))?;
        let truth = self.truth(part, Consumer::Correlation)?;
        let occurrences = self.b.truth(truth).occurrences();
        let named: Vec<usize> = members
            .iter()
            .filter(|(binder, _)| occurrences.contains(&Occurrence::Binder(*binder)))
            .map(|(_, k)| *k)
            .collect();
        if let Some(q) = named.iter().find_map(|k| arms[*k].qualifier.as_ref().filter(|q| shared.contains(q))) {
            return Err(refuse::ambiguous_column(&format!("{q}")));
        }
        let heading = self.b.binder(result_binder).heading().clone();
        let reach = crate::pipeline::middle::core::node::walk::reachable(
            &self.b,
            &[crate::pipeline::middle::core::node::walk::Child::Truth(truth)],
        );
        let mut bare: Vec<Name> = Vec::new();
        for e in reach.exprs {
            if let crate::pipeline::middle::core::node::ExprKind::Col(b, position) = self.b.expr(e).kind() {
                if *b == result_binder {
                    if let Some(name) = heading.positions().get(usize::from(*position)).and_then(|p| p.answering_name()) {
                        if !bare.contains(name) {
                            bare.push(name.clone());
                        }
                    }
                }
            }
        }
        Ok((named, bare))
    }

    /// Arm `k` keeping only the rows its probes admit: for each, the rows
    /// with (or, for a minus, without) a row of the other arm meeting
    /// every conjunct of their correlation.
    fn correlated(&mut self, k: usize, probes: &[Probe<'_, '_>], arms: &[Arm], base: &[RelId]) -> Result<RelId, Refusal> {
        let term = arm_term(self.b.local_read(base[k], AccessSpec::All, &self.switches)?, arms[k].qualifier.clone());
        self.restricted(term, |e, own| {
            for probe in probes {
                let other = &arms[probe.other];
                let step = arms[k.max(probe.other)].operator;
                let term = arm_term(e.b.local_read(base[probe.other], AccessSpec::All, &e.switches)?, other.qualifier.clone());
                let names = (arms[k].qualifier.clone(), other.qualifier.clone());
                let rel = e.restricted(term, |e, theirs| {
                    for conjunct in probe.conjuncts {
                        let truths = match conjunct {
                            Conjunct::Written(part) => vec![e.truth(part, Consumer::Correlation)?],
                            Conjunct::Whole(whole) => e.whole(whole, step, (own, theirs), &names)?,
                        };
                        for truth in truths {
                            e.guard(truth);
                        }
                    }
                    Ok(())
                })?;
                crate::pipeline::middle::core::decide::effect::fence(&e.b, rel, "a set-operation correlation")?;
                let truth = e.b.exists(probe.keeps, rel);
                e.guard(truth);
            }
            Ok(())
        })
    }

    /// A whole-heading correlation's equalities between the probing arm's
    /// row (`own`) and the probed arm's (`theirs`), as the step aligns its
    /// arms: every name both publish under the name modes, every position
    /// under the positional mode.
    fn whole(
        &mut self,
        whole: &WholeHeading,
        step: Option<SetOperator>,
        (own, theirs): (BinderId, BinderId),
        names: &(Option<Name>, Option<Name>),
    ) -> Result<Vec<TruthId>, Refusal> {
        let positional = step == Some(SetOperator::UnionAllPositional);
        let (mine, other) = (self.b.binder(own).heading().clone(), self.b.binder(theirs).heading().clone());
        let pairs: Vec<(usize, usize)> = match (whole, positional) {
            (WholeHeading::ByName { .. }, false) => by_name(&mine, &other),
            (WholeHeading::ByPosition { .. }, true) => {
                mine.displayed().map(|(l, _)| l).zip(other.displayed().map(|(r, _)| r)).collect()
            }
            _ => return Err(refuse::outside("a whole-heading correlation spelled against its step's alignment")),
        };
        if pairs.is_empty() {
            let spell = |n: &Option<Name>| n.as_ref().map_or_else(|| "_".to_string(), |n| n.to_string());
            return Err(refuse::whole_correlation_empty(&spell(&names.0), &spell(&names.1)));
        }
        let mut truths = Vec::with_capacity(pairs.len());
        for (l, r) in pairs {
            let (left, right) = (self.b.col(own, l as u16), self.b.col(theirs, r as u16));
            truths.push(self.b.cmp(CmpOp::NullSafeEqual, left, right, Consumer::Correlation, true, &self.switches)?);
        }
        Ok(truths)
    }

    /// A run over `term` alone, its guards written by `guards` (handed the
    /// term's binder), closed.
    fn restricted(
        &mut self,
        term: Term,
        guards: impl FnOnce(&mut Self, BinderId) -> Result<(), Refusal>,
    ) -> Result<RelId, Refusal> {
        self.scopes.push(super::Scope {
            run: OpenRun::new(),
            arms: Vec::new(),
            reversed: false,
            qualified_only: false,
            pending: Some(term),
        });
        let written = self.materialize().and_then(|()| {
            let binder = self
                .scopes
                .last()
                .and_then(|s| s.run.members().last().map(|m| m.binder()))
                .ok_or_else(|| refuse::contract("an arm read and not bound"))?;
            guards(self, binder)
        });
        match written {
            Ok(()) => self.close_top(),
            Err(e) => {
                self.scopes.pop();
                Err(e)
            }
        }
    }

    /// A guard on the open run.
    fn guard(&mut self, truth: TruthId) {
        self.scopes.last_mut().expect("an open scope").run.push_guard(&self.b, truth);
    }
}

/// An arm read as a member under the qualifier it answers to.
fn arm_term(rel: RelId, qualifier: Option<Name>) -> Term {
    Term {
        rel,
        names_scope: qualifier.is_some(),
        scope: qualifier,
        route: Route::Plain,
        merge: MergeRequest::None,
        requalifies: true,
        born: Born::Written,
    }
}

/// Every displayed position of `left` against the one position of `right`
/// answering to its name.
fn by_name(left: &Heading, right: &Heading) -> Vec<(usize, usize)> {
    left.displayed()
        .filter_map(|(l, p)| {
            let name = p.answering_name()?;
            match answers_to(right, name).as_slice() {
                [r] => Some((l, *r)),
                _ => None,
            }
        })
        .collect()
}

/// A conjunct naming other than two arms names no pair.
fn no_pair(count: usize) -> Refusal {
    delightql_types::diagnostic::ResolutionSetop::CorrelationOwner {
        message: format!(
            "a set-operation correlation names exactly two arms; a conjunct naming {count} arms names no pair, so \
             write one conjunct per pair of arms"
        ),
    }
    .into()
}

/// The top-level conjuncts of a condition, in written order.
fn conjuncts<'c>(truth: &'c TruthExpression, out: &mut Vec<&'c TruthExpression>) {
    match truth {
        TruthExpression::Conjunction(parts) => {
            for part in parts.iter() {
                conjuncts(part, out);
            }
        }
        other => out.push(other),
    }
}

/// The arms (by index) the qualifiers of a conjunct written at chain index
/// `at` answer: each qualifier answered by one arm entered before it. A
/// qualifier two arms answer is ambiguous; one naming a member of a joined
/// first arm addresses no arm.
fn addressed(arms: &[Arm], joined: &[Name], at: usize, qualifiers: &[Name]) -> Result<Vec<usize>, Refusal> {
    let mut named: Vec<usize> = Vec::new();
    for q in qualifiers {
        let answering: Vec<usize> = arms
            .iter()
            .enumerate()
            .filter(|(_, arm)| (arm.enters < at || arm.operator.is_none()) && arm.qualifier.as_ref() == Some(q))
            .map(|(k, _)| k)
            .collect();
        match answering.as_slice() {
            [] if joined.contains(q) => {
                return Err(refuse::outside("a set-operation correlation addressing a member of a joined arm"))
            }
            [] => {}
            [k] => {
                if !named.contains(k) {
                    named.push(*k);
                }
            }
            [_, _, ..] => return Err(refuse::ambiguous_column(&format!("{q}"))),
        }
    }
    named.sort_unstable();
    Ok(named)
}

impl Elaborator<'_, '_> {
    /// The gated step's correlation as written, oriented to its step (arm 0
    /// left).
    fn atom(&self, conjunct: &Conjunct<'_>, arms: &[Arm]) -> Result<Atom, Refusal> {
        let part = match conjunct {
            Conjunct::Whole(whole) => {
                let side = match whole {
                    WholeHeading::ByName { .. } => Side::AllNames,
                    WholeHeading::ByPosition { .. } => Side::AllOrdinals,
                };
                return Ok(Atom {
                    equality: true,
                    left: side.clone(),
                    right: side,
                });
            }
            Conjunct::Written(part) => part,
        };
        let TruthExpression::Comparison(c) = part else {
            return Ok(Atom {
                equality: false,
                left: Side::Other,
                right: Side::Other,
            });
        };
        let (l_arm, l) = self.side(&c.left, arms)?;
        let (r_arm, r) = self.side(&c.right, arms)?;
        let equality = c.operator == CmpOp::NullSafeEqual;
        Ok(match (l_arm, r_arm) {
            (Some(0), Some(1)) => Atom { equality, left: l, right: r },
            (Some(1), Some(0)) => Atom { equality, left: r, right: l },
            _ => Atom {
                equality,
                left: Side::Other,
                right: Side::Other,
            },
        })
    }

    /// One side of a correlation atom: the arm it addresses and what it
    /// addresses there, when it is a bare qualified reference. An ordinal's
    /// position is the one compile-time integer decision's.
    fn side(&self, value: &DomainExpression, arms: &[Arm]) -> Result<(Option<usize>, Side), Refusal> {
        let arm_of = |q: &Option<Name>| arms.iter().position(|a| a.qualifier.is_some() && a.qualifier == *q);
        Ok(match value {
            DomainExpression::Reference(Reference::Named(named)) if named.0.namespace_path.is_empty() => {
                (arm_of(&named.0.qualifier), Side::Name(named.0.name.clone()))
            }
            DomainExpression::Reference(Reference::Ordinal(o)) if o.namespace_path.is_empty() && !o.glob => {
                match usize::try_from(self.compile_time_integer(o.position, Site::Ordinal)?) {
                    Ok(n) => (arm_of(&o.qualifier), Side::Ordinal(n)),
                    Err(_) => (None, Side::Other),
                }
            }
            _ => (None, Side::Other),
        })
    }
}
