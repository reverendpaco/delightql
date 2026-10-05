// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Positional provenance: what one position of a relation's heading holds,
//! one form down. Every reader that follows a position through the forms
//! (a value's kind, its SQLite affinity, whether a replacement's incoming
//! value of a generated column is the row's own) steps through this one
//! table and answers its own question at the leaves.

use super::{Cell, HeaderSlot, PipeOp, ReadAccess, ReadSource, RelKind, Slot};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::Visibility;
use crate::pipeline::middle::core::ids::{ExprId, PassengerId, RelId};

/// What a position holds, one form down.
pub(crate) enum Source {
    /// A hidden position: the passenger it carries.
    Passenger(PassengerId),
    /// Column `column` of the catalog read `read` (past its columns, no
    /// column of it).
    Catalog { read: RelId, column: usize },
    /// The same position of another relation's heading: position `.1` of
    /// `.0`, unchanged.
    Position(RelId, usize),
    /// An output cell of the run `run`.
    Cell { run: RelId, cell: Cell },
    /// A value: an expression a stage computes over its input, or a
    /// receipt's constant.
    Value { expr: ExprId },
    /// One value per row of an anonymous table's column.
    Rows(Vec<ExprId>),
    /// A set operation's two arms' positions; `None` for an arm that writes
    /// NULL there.
    Arms(Vec<Option<(RelId, usize)>>),
    /// A family's clauses, each at the same position.
    Clauses(Vec<RelId>),
    /// A fixpoint's anchors and steps, each at the same position.
    Fixpoint { anchors: Vec<RelId>, steps: Vec<RelId> },
    /// A read of a fixpoint's frontier.
    Frontier,
    /// A position of a reflection.
    Reflection(usize),
    /// A witness's verdict: 1 where its input holds a row, 0 where it holds
    /// none.
    Verdict,
    /// A relation a receipt carries as an interior value.
    Interior,
    /// Nothing the forms state.
    Unknown,
}

/// What position `i` of `rel` holds.
pub(crate) fn of(arena: &impl Arena, rel: RelId, i: usize) -> Source {
    let node = arena.rel(rel);
    if let Some(p) = node.heading().positions().get(i) {
        if let Visibility::Hidden(passenger) = p.visibility {
            return Source::Passenger(passenger);
        }
    }
    match node.kind() {
        RelKind::Read { source, access } => {
            let at = match access {
                ReadAccess::All | ReadAccess::Unasked => Some(i),
                ReadAccess::Slots(slots) => {
                    slots.iter().enumerate().filter(|(_, s)| matches!(s, Slot::Bind(_))).nth(i).map(|(k, _)| k)
                }
            };
            let Some(k) = at else {
                return Source::Unknown;
            };
            match source {
                ReadSource::Catalog { .. } => Source::Catalog { read: rel, column: k },
                ReadSource::Local(body) | ReadSource::Staged(body) => {
                    match arena.rel(*body).heading().displayed().nth(k) {
                        Some((d, _)) => Source::Position(*body, d),
                        None => Source::Unknown,
                    }
                }
                ReadSource::Frontier(_) => Source::Frontier,
                ReadSource::Function { .. } | ReadSource::Created { .. } => Source::Unknown,
            }
        }
        RelKind::Lit { header, rows, .. } => {
            let slot = header
                .iter()
                .enumerate()
                .filter(|(_, s)| !matches!(s, HeaderSlot::Reuse { .. } | HeaderSlot::Disregard | HeaderSlot::Constraint { .. }))
                .nth(i)
                .map(|(k, _)| k);
            match slot {
                Some(k) => Source::Rows(rows.iter().map(|row| row[k]).collect()),
                None => Source::Unknown,
            }
        }
        RelKind::Run(run) => match run.outputs().get(i) {
            Some(cell) => Source::Cell { run: rel, cell: *cell },
            None => Source::Unknown,
        },
        RelKind::Pipe { input, op } => {
            let input_len = arena.rel(*input).heading().len();
            let value = |expr: ExprId| Source::Value { expr };
            match op {
                PipeOp::Project(items) => items.get(i).map_or(Source::Unknown, |it| value(it.expr)),
                PipeOp::Embed(items) if i < input_len => Source::Position(*input, i),
                PipeOp::Embed(items) => items.get(i - input_len).map_or(Source::Unknown, |it| value(it.expr)),
                PipeOp::ProjectOut(selection) => (0..input_len)
                    .filter(|j| !selection.items().iter().any(|(s, _)| s.position() == *j))
                    .nth(i)
                    .map_or(Source::Unknown, |j| Source::Position(*input, j)),
                PipeOp::Cover(selection) => match selection.items().iter().find(|(t, _)| t.position() == i) {
                    Some((_, v)) => value(*v),
                    None => Source::Position(*input, i),
                },
                PipeOp::Group { keys, reductions } => {
                    keys.iter().chain(reductions).nth(i).map_or(Source::Unknown, |it| value(it.expr))
                }
                PipeOp::Distinct(keys) => keys.get(i).map_or(Source::Unknown, |it| value(it.expr)),
                PipeOp::Carry(_) if i < input_len => Source::Position(*input, i),
                PipeOp::Carry(_) => Source::Unknown,
            }
        }
        RelKind::Order { input, .. } => Source::Position(*input, i),
        RelKind::Apply { instance } => Source::Position(arena.instance(*instance).body(), i),
        RelKind::SetOp { left, right, alignment, .. } => match alignment.get(i) {
            Some((l, r)) => Source::Arms(vec![l.map(|j| (*left, j)), r.map(|j| (*right, j))]),
            None => Source::Unknown,
        },
        RelKind::Minus { left, pairs, .. } => match pairs.get(i) {
            Some((l, _)) => Source::Position(*left, *l),
            None => Source::Unknown,
        },
        RelKind::Meta { .. } => Source::Reflection(i),
        RelKind::Witnessed { input, .. } => match arena.rel(*input).heading().displayed().nth(i) {
            Some((d, _)) => Source::Position(*input, d),
            None => Source::Verdict,
        },
        RelKind::Family { clauses, .. } => Source::Clauses(clauses.iter().map(|c| c.body).collect()),
        RelKind::Fix(fix) => Source::Fixpoint {
            anchors: fix.anchors().to_vec(),
            steps: fix.steps().to_vec(),
        },
        RelKind::Receipt { cells, .. } => match cells.get(i) {
            Some(super::effect::ReceiptCell::Const(e)) => Source::Value { expr: *e },
            Some(super::effect::ReceiptCell::Interior(_)) => Source::Interior,
            None => Source::Unknown,
        },
        RelKind::Unnest { value, .. } => match super::walk::carried(arena, *value) {
            Some(carried) => Source::Position(carried, i),
            None => Source::Unknown,
        },
    }
}
