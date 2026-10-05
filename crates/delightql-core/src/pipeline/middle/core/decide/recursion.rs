// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The recursion verdict of a definition family (W5 #15), judged once its
//! clauses are elaborated and before any SQL exists
//! (recursion-contract-law).

use crate::pipeline::middle::core::refuse::{self, Refusal};

/// What one recursive clause does with the frontier, as the family's
/// constructor found it.
#[derive(Clone, Debug, Default)]
pub(crate) struct StepFacts {
    /// Reads of the frontier as a direct source of the clause.
    pub(crate) frontier_reads: usize,
    /// Reads of the frontier inside a truth or a value the clause holds
    /// (a semi-join, an anti-join, a scalar subquery).
    pub(crate) subquery_reads: usize,
    /// A reduction whose input reads the frontier.
    pub(crate) reduces_frontier: bool,
}

/// THE BADGE CHOOSES THE UNION, and a badge claims a self-reference.
/// `badges` holds each clause's badge; the definition's heads crossed the
/// one assembler, which refuses clauses that disagree about it.
pub(crate) fn badge(name: &str, badges: &[bool], recursive: bool) -> Result<bool, Refusal> {
    let badged = badges.first().copied().unwrap_or(false);
    if badged && !recursive {
        return Err(refuse::false_fixpoint(name));
    }
    Ok(badged)
}

/// NO SUBQUERY AGAINST THE TARGET, then LINEARITY, then STRATA ARE
/// TEXTUAL.
pub(crate) fn judge(steps: &[StepFacts]) -> Result<(), Refusal> {
    for step in steps {
        if step.subquery_reads > 0 {
            return Err(refuse::self_subquery());
        }
        if step.frontier_reads > 1 {
            return Err(refuse::recursion_nonlinear());
        }
        if step.reduces_frontier {
            return Err(refuse::recursion_aggregate());
        }
    }
    Ok(())
}

/// What a self-reference's actuals say about the instance being built
/// (recursion-contract-law, MONOMORPHIC PARAMETERS).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reentry {
    /// The same semantic actual identities: the self-reference reads the
    /// frontier.
    Same,
    /// A different actual: `parameter-widening`.
    Widened,
    /// Decided only by what a use supplies: the instance is built over
    /// stand-ins (A BODY IS JUDGED WHERE IT IS DECLARED reserves the
    /// instance a use admits to each use).
    Undecided,
}

impl Reentry {
    fn and(self, other: Reentry) -> Reentry {
        match (self, other) {
            (Reentry::Widened, _) | (_, Reentry::Widened) => Reentry::Widened,
            (Reentry::Undecided, _) | (_, Reentry::Undecided) => Reentry::Undecided,
            (Reentry::Same, Reentry::Same) => Reentry::Same,
        }
    }
}

/// Whether a self-reference's actuals are the semantic actual identities of
/// the instance being built: each value actual is the same value node or the
/// same constant; each relation actual denotes the same relation (a whole
/// read of a relation reads that relation; two whole reads of one catalog
/// entity are one relation; two applications of one definition with the
/// same actuals are one relation); each rule actual configures the same
/// definition with the same values. Authored spelling never enters.
///
/// `open` holds the stand-ins of the definitions being declared: each
/// becomes whatever a use supplies. A stand-in compared with a constant, a
/// catalog relation, an application or another stand-in is undecided, since
/// some use may supply exactly that; compared with anything made inside the
/// instance it differs at every use, since a caller never supplies a node
/// the instance makes.
pub(crate) fn reentry(
    arena: &impl crate::pipeline::middle::core::graph::Arena,
    built: &[crate::pipeline::middle::core::instance::Actual],
    reentry: &[crate::pipeline::middle::core::instance::Actual],
    open: &[crate::pipeline::middle::core::instance::Actual],
) -> Reentry {
    if built.len() != reentry.len() {
        return Reentry::Widened;
    }
    built
        .iter()
        .zip(reentry)
        .fold(Reentry::Same, |verdict, (a, b)| verdict.and(actual(arena, a, b, open)))
}

fn actual(
    arena: &impl crate::pipeline::middle::core::graph::Arena,
    a: &crate::pipeline::middle::core::instance::Actual,
    b: &crate::pipeline::middle::core::instance::Actual,
    open: &[crate::pipeline::middle::core::instance::Actual],
) -> Reentry {
    use crate::pipeline::middle::core::instance::Actual;
    match (a, b) {
        (Actual::Value(a), Actual::Value(b)) => value(arena, *a, *b, open),
        (Actual::Relation { rel: a, .. }, Actual::Relation { rel: b, .. }) => relation(arena, *a, *b, open),
        (Actual::Rule(a), Actual::Rule(b)) if a.definition == b.definition && a.configured.len() == b.configured.len() => a
            .configured
            .iter()
            .zip(&b.configured)
            .fold(Reentry::Same, |verdict, pair| {
                verdict.and(match pair {
                    (Some(x), Some(y)) => actual(arena, x, y, open),
                    (None, None) => Reentry::Same,
                    (Some(_), None) | (None, Some(_)) => Reentry::Widened,
                })
            }),
        (Actual::Value(_) | Actual::Relation { .. } | Actual::Rule(_), _) => Reentry::Widened,
    }
}

fn value(
    arena: &impl crate::pipeline::middle::core::graph::Arena,
    a: crate::pipeline::middle::core::ids::ExprId,
    b: crate::pipeline::middle::core::ids::ExprId,
    open: &[crate::pipeline::middle::core::instance::Actual],
) -> Reentry {
    use crate::pipeline::middle::core::instance::Actual;
    use crate::pipeline::middle::core::node::ExprKind;
    let same = a == b
        || match (arena.expr(a).kind(), arena.expr(b).kind()) {
            (ExprKind::Const(x), ExprKind::Const(y)) => x == y,
            _ => false,
        };
    if same {
        return Reentry::Same;
    }
    let stands_in = |e| open.iter().any(|o| matches!(o, Actual::Value(s) if *s == e));
    let suppliable = |e| stands_in(e) || matches!(arena.expr(e).kind(), ExprKind::Const(_));
    if (stands_in(a) && suppliable(b)) || (stands_in(b) && suppliable(a)) {
        Reentry::Undecided
    } else {
        Reentry::Widened
    }
}

fn relation(
    arena: &impl crate::pipeline::middle::core::graph::Arena,
    a: crate::pipeline::middle::core::ids::RelId,
    b: crate::pipeline::middle::core::ids::RelId,
    open: &[crate::pipeline::middle::core::instance::Actual],
) -> Reentry {
    use crate::pipeline::middle::core::instance::Actual;
    use crate::pipeline::middle::core::node::RelKind;
    let (a, b) = (whole(arena, a), whole(arena, b));
    if a == b || matches!((catalog_entity(arena, a), catalog_entity(arena, b)), (Some(x), Some(y)) if x == y) {
        return Reentry::Same;
    }
    let applied = match (arena.rel(a).kind(), arena.rel(b).kind()) {
        (RelKind::Apply { instance: x }, RelKind::Apply { instance: y }) => {
            let (x, y) = (arena.instance(*x), arena.instance(*y));
            (x.definition() == y.definition()).then(|| reentry(arena, x.formals(), y.formals(), open))
        }
        _ => None,
    };
    if let Some(verdict @ (Reentry::Same | Reentry::Undecided)) = applied {
        return verdict;
    }
    let stands_in = |r| open.iter().any(|o| matches!(o, Actual::Relation { rel, .. } if whole(arena, *rel) == r));
    let suppliable = |r| {
        stands_in(r) || catalog_entity(arena, r).is_some() || matches!(arena.rel(r).kind(), RelKind::Apply { .. })
    };
    if (stands_in(a) && suppliable(b)) || (stands_in(b) && suppliable(a)) {
        Reentry::Undecided
    } else {
        Reentry::Widened
    }
}

/// The relation a whole read of a built relation reads. A positional read
/// whose every slot binds a distinct name renames the positions and keeps
/// every row, so it reads the same relation: a relation formal bound to its
/// declared pattern is its actual under the pattern's names.
fn whole(
    arena: &impl crate::pipeline::middle::core::graph::Arena,
    rel: crate::pipeline::middle::core::ids::RelId,
) -> crate::pipeline::middle::core::ids::RelId {
    use crate::pipeline::middle::core::node::{ReadAccess, ReadSource, RelKind, Slot};
    match arena.rel(rel).kind() {
        RelKind::Read {
            source: ReadSource::Local(inner),
            access: ReadAccess::All,
        } => whole(arena, *inner),
        RelKind::Read {
            source: ReadSource::Local(inner),
            access: ReadAccess::Slots(slots),
        } if slots.iter().all(|s| matches!(s, Slot::Bind(_))) => whole(arena, *inner),
        _ => rel,
    }
}

/// The catalog row a whole, unmarked catalog read reads.
fn catalog_entity(
    arena: &impl crate::pipeline::middle::core::graph::Arena,
    rel: crate::pipeline::middle::core::ids::RelId,
) -> Option<i64> {
    use crate::pipeline::middle::core::node::{ReadAccess, ReadSource, RelKind};
    match arena.rel(rel).kind() {
        RelKind::Read {
            source:
                ReadSource::Catalog {
                    entity: Some(id),
                    locator,
                    ..
                },
            access: ReadAccess::All,
        } if locator.is_empty() => Some(*id),
        _ => None,
    }
}
