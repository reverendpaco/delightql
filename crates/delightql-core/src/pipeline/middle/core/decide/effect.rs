// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The judgments over a statement's acts: where an act may stand in a comma
//! run (THE EFFECT FENCE; SET-AT-A-TIME), and which marked reads its row
//! writes consume.

use crate::pipeline::middle::core::graph::Judging;
use crate::pipeline::middle::core::ids::{PassengerId, RelId};
use crate::pipeline::middle::core::node::effect::{self, Write};
use crate::pipeline::middle::core::node::walk::{self, Child};
use crate::pipeline::middle::core::node::RelKind;
use crate::pipeline::middle::core::refuse::{self, Refusal};
use std::collections::BTreeSet;

/// The acts a relation holds: the receipts reachable from it.
pub(crate) fn acts(arena: &impl Judging, rel: RelId) -> BTreeSet<RelId> {
    walk::reachable(arena, &[Child::Rel(rel)])
        .rels
        .into_iter()
        .filter(|r| matches!(arena.rel(*r).kind(), RelKind::Receipt { .. }))
        .collect()
}

/// A relation standing in an enclosed position holds no act (THE EFFECT
/// FENCE).
pub(crate) fn fence(arena: &impl Judging, rel: RelId, position: &str) -> Result<(), Refusal> {
    if acts(arena, rel).is_empty() {
        Ok(())
    } else {
        Err(refuse::effect_fence(position))
    }
}

/// A comma run's members against the acts they hold, and the acts each
/// member's gate opens. An act is a direct operand of the run's join: a
/// member written optional never holds one (THE EFFECT FENCE). A member
/// after the first that holds acts its earlier members do not opens them:
/// they run only when the members before it have a row (SET-AT-A-TIME:
/// conjunction is evaluated left to right; a NO empties the join and
/// nothing after it evaluates).
pub(crate) fn judge_run(arena: &impl Judging, members: &[RelId], marked: &[bool]) -> Result<Vec<BTreeSet<RelId>>, Refusal> {
    let mut earlier: BTreeSet<RelId> = BTreeSet::new();
    let mut opens = Vec::with_capacity(members.len());
    for (j, rel) in members.iter().enumerate() {
        let held = acts(arena, *rel);
        if !held.is_empty() && marked.get(j).copied().unwrap_or(false) {
            return Err(refuse::effect_fence("an outer member"));
        }
        opens.push(if j > 0 { held.difference(&earlier).copied().collect() } else { BTreeSet::new() });
        earlier.extend(held);
    }
    Ok(opens)
}

/// The row locators the acts under `rel` reach their rows by: each
/// replacement's and each removal's marked occurrence.
pub(crate) fn consumed_locators(arena: &impl Judging, rel: RelId) -> Vec<PassengerId> {
    walk::reachable(arena, &[Child::Rel(rel)])
        .rels
        .iter()
        .flat_map(|r| match arena.rel(*r).kind() {
            RelKind::Receipt { act, .. } => match act.write() {
                Write::Rows { rows, .. } => effect::locator(rows),
                Write::None | Write::Object(_) | Write::Session { .. } | Write::Terminal(_) => Vec::new(),
            },
            _ => Vec::new(),
        })
        .collect()
}

/// The row locators the marked reads under `rel` bear.
pub(crate) fn born_locators(arena: &impl Judging, rel: RelId) -> Vec<PassengerId> {
    walk::reachable(arena, &[Child::Rel(rel)])
        .rels
        .iter()
        .flat_map(|r| match arena.rel(*r).kind() {
            RelKind::Read {
                source: crate::pipeline::middle::core::node::ReadSource::Catalog { locator, .. },
                ..
            } => locator.clone(),
            _ => Vec::new(),
        })
        .collect()
}

/// A program as its statement performs it, decided once when the statement
/// freezes: its acts in the order evaluation reaches them
/// ([`evaluation_order`]), the run each belongs to, and each run's bracket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Program {
    result: RelId,
    acts: Vec<ProgramAct>,
    runs: Vec<ProgramRun>,
}

impl Program {
    /// The relation the statement returns.
    pub(crate) fn result(&self) -> RelId {
        self.result
    }
    /// The acts, in program order.
    pub(crate) fn acts(&self) -> &[ProgramAct] {
        &self.acts
    }
    /// The runs, in order; every act's run is one of them.
    pub(crate) fn runs(&self) -> &[ProgramRun] {
        &self.runs
    }
}

/// One act of a program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgramAct {
    receipt: RelId,
    run: usize,
    ordinal: usize,
    source_holds_acts: bool,
    reads: Vec<RelId>,
}

impl ProgramAct {
    /// The receipt that holds the act.
    pub(crate) fn receipt(&self) -> RelId {
        self.receipt
    }
    /// The run the act belongs to.
    pub(crate) fn run(&self) -> usize {
        self.run
    }
    /// The number naming the act's occurrence where no label does: the acts
    /// its run holds before it, the acts its own source holds aside.
    pub(crate) fn ordinal(&self) -> usize {
        self.ordinal
    }
    /// Whether the relation the act consumes holds acts of its own.
    pub(crate) fn source_holds_acts(&self) -> bool {
        self.source_holds_acts
    }
    /// The program's arguments (the relations acts read as inputs) this act
    /// reaches and that do not hold it: each is ready before the act runs.
    pub(crate) fn reads(&self) -> &[RelId] {
        &self.reads
    }
}

/// One run of a program.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgramRun {
    bracketed: bool,
}

impl ProgramRun {
    /// Whether the run plays inside one transaction bracket. A run holding a
    /// session act has none: the act's own catalog transaction is its
    /// atomicity, and the run holds no data write beside it.
    pub(crate) fn bracketed(&self) -> bool {
        self.bracketed
    }
}

/// The program a statement returning `result` performs. A run holding a
/// session act beside a data write refuses: the act commits its own change
/// to the session's world, which the run's bracket could not roll back with
/// the write.
pub(crate) fn program(arena: &impl Judging, result: RelId) -> Result<Program, Refusal> {
    let receipts = evaluation_order(arena, result)?;
    let run_of = implicit_runs(arena, result, &receipts);
    let count = run_of.iter().max().map_or(1, |r| r + 1);
    let held = |r: &RelId| match arena.rel(*r).kind() {
        RelKind::Receipt { act, .. } => Some(act),
        _ => None,
    };
    let mut runs = Vec::with_capacity(count);
    for run in 0..count {
        let writes: Vec<&Write> =
            receipts.iter().zip(&run_of).filter(|(_, k)| **k == run).filter_map(|(r, _)| held(r)).map(|a| a.write()).collect();
        let session = writes.iter().any(|w| matches!(w, Write::Session { .. }));
        if session && writes.iter().any(|w| matches!(w, Write::Rows { .. } | Write::Object(_))) {
            return Err(refuse::outside("a run holding a session act beside a data write"));
        }
        runs.push(ProgramRun { bracketed: !session });
    }
    let inputs: BTreeSet<RelId> = receipts.iter().filter_map(held).flat_map(|a| a.inputs().iter().copied()).collect();
    let mut out = Vec::with_capacity(receipts.len());
    for (index, r) in receipts.iter().enumerate() {
        let act = held(r).ok_or_else(|| refuse::contract("an act that no receipt holds"))?;
        let sourced = act.source().map(|s| acts(arena, s)).unwrap_or_default();
        let ordinal = receipts[..index]
            .iter()
            .zip(&run_of[..index])
            .filter(|(a, k)| **k == run_of[index] && !sourced.contains(a))
            .count();
        let reached = walk::reachable(arena, &[Child::Rel(*r)]).rels;
        let reads = inputs.iter().copied().filter(|i| reached.contains(i) && !acts(arena, *i).contains(r)).collect();
        out.push(ProgramAct {
            receipt: *r,
            run: run_of[index],
            ordinal,
            source_holds_acts: !sourced.is_empty(),
            reads,
        });
    }
    Ok(Program { result, acts: out, runs })
}

/// The acts under `result` in the order evaluation reaches them (EVERY
/// OCCURRENCE EFFECTUATES; SET-AT-A-TIME): every node's children in the order
/// they are written (a run's members left to right, a pipe's input before
/// its stage, a union's arms left to right, a family's clauses in definition
/// order, a local binding where it is read), and an act after every relation
/// it reads. A node reached twice is evaluated where it is first reached.
/// Where two operands of one call hold acts, the law does not say whose
/// happen first: PIPE SUBSTITUTION makes the piped spelling the direct one,
/// and left to right reads differently in each; that refuses.
fn evaluation_order(arena: &impl Judging, result: RelId) -> Result<Vec<RelId>, Refusal> {
    let mut order = Vec::new();
    let mut seen = walk::Reach::default();
    let mut pending = vec![(Child::Rel(result), false)];
    while let Some((node, expanded)) = pending.pop() {
        if expanded {
            if let Child::Rel(r) = node {
                if let RelKind::Receipt { act, .. } = arena.rel(r).kind() {
                    let operands: BTreeSet<RelId> = act.source().into_iter().chain(act.inputs().iter().copied()).collect();
                    if operands.iter().filter(|o| !acts(arena, **o).is_empty()).count() > 1 {
                        return Err(refuse::unruled(
                            "the order in which the acts held by two operands of one call happen",
                        ));
                    }
                    order.push(r);
                }
            }
            continue;
        }
        let first = match node {
            Child::Rel(r) => seen.rels.insert(r),
            Child::Expr(e) => seen.exprs.insert(e),
            Child::Truth(t) => seen.truths.insert(t),
        };
        // A read of a created object is evaluated where it is reached, so
        // the act creating the object must be complete by then.
        if let (true, Child::Rel(r)) = (first, node) {
            if let RelKind::Read {
                source: crate::pipeline::middle::core::node::ReadSource::Created { receipt, name, .. },
                ..
            } = arena.rel(r).kind()
            {
                if !order.contains(receipt) {
                    return Err(refuse::table(name.as_str()));
                }
            }
        }
        if first {
            pending.push((node, true));
            pending.extend(walk::of(arena, node).into_iter().rev().map(|c| (c, false)));
        }
    }
    Ok(order)
}

/// THE IMPLICIT RUN: the arms of the union the statement's result is,
/// through the stages after it, each open a run; an act no arm holds runs
/// in the last.
fn implicit_runs(arena: &impl Judging, result: RelId, acts: &[RelId]) -> Vec<usize> {
    let mut top = result;
    loop {
        match arena.rel(top).kind() {
            RelKind::Pipe { input, .. } | RelKind::Order { input, .. } => top = *input,
            RelKind::Run(run) if run.quals().len() == 1 => match run.quals().first() {
                Some(crate::pipeline::middle::core::node::Qual::Member(m)) => top = m.rel(),
                _ => break,
            },
            _ => break,
        }
    }
    let mut arms = Vec::new();
    let mut pending = vec![top];
    while let Some(r) = pending.pop() {
        match arena.rel(r).kind() {
            RelKind::SetOp { left, right, .. } => {
                pending.push(*right);
                pending.push(*left);
            }
            _ => arms.push(r),
        }
    }
    let reaches: Vec<BTreeSet<RelId>> = arms.iter().map(|a| walk::reachable(arena, &[Child::Rel(*a)]).rels.into_iter().collect()).collect();
    acts.iter()
        .map(|act| reaches.iter().position(|r| r.contains(act)).unwrap_or(arms.len().saturating_sub(1)))
        .collect()
}

/// The reads among `reads` that a creation's session object will shadow
/// (NAME CLASH: a session creation takes its name in the connection's
/// session pool): each read of a catalog relation of that name on the
/// creation's connection whose spelling names no schema, so that once the
/// object exists the same spelling would resolve to the object.
pub(crate) fn shadowed_reads(
    arena: &impl Judging,
    reads: impl IntoIterator<Item = RelId>,
    created: &crate::pipeline::middle::core::heading::Name,
    connection: i64,
) -> Vec<(RelId, String)> {
    use crate::pipeline::middle::core::node::{ReadSource, RelKind};
    reads
        .into_iter()
        .filter_map(|r| match arena.rel(r).kind() {
            RelKind::Read {
                source: ReadSource::Catalog { name, namespace, physical, .. },
                ..
            } if name == created && physical.connection == connection && physical.schema.is_none() => {
                Some((r, namespace.clone()))
            }
            _ => None,
        })
        .collect()
}
