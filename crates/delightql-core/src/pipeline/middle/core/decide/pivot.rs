// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A pivot's witness, its columns and its cardinality (pipe-operators
//! FN.10). THE IN IS THE HEADING WITNESS: a pivot
//! requires an authored literal membership on its key in the same chain,
//! and that membership's candidates, in authored order, are the published
//! columns; the heading is decided before data, always.

use crate::pipeline::middle::core::graph::Judging;
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{BinderId, ExprId, RelId, TruthId};
use crate::pipeline::middle::core::node::provenance::{self, Source};
use crate::pipeline::middle::core::node::rel::referenced_cell;
use crate::pipeline::middle::core::node::{Cell, ExprKind, HeaderSlot, PipeOp, ReadSource, RelKind, TruthKind};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::LiteralValue;

/// The candidates of the membership witnessing `key`, a value of the run
/// whose conditions are `guards`: one of those conditions, or one of an
/// earlier stage of the same chain the key's column comes from. The
/// membership lists its candidates, or reads one of `literals`, the
/// anonymous tables a call wrote as its relation arguments: either way the
/// text states the candidates before any data.
pub(crate) fn witness(
    arena: &impl Judging,
    guards: &[TruthId],
    key: ExprId,
    literals: &[RelId],
) -> Result<Vec<LiteralValue>, Refusal> {
    let found = match referenced_cell(arena, key) {
        Some(cell) => search(arena, guards, cell, literals)?,
        None => None,
    };
    found.ok_or_else(|| {
        delightql_types::diagnostic::Constraint::Pivot {
            message: "a pivot requires an authored membership predicate on its key in the same chain, \
                      e.g. `k in (\"a\"; \"b\") |> %(g ~> v of k)`: the membership's candidates are the \
                      pivot's columns, and the compiler never derives a heading from data"
                .to_string(),
        }
        .into()
    })
}

/// The column each candidate publishes (FN.10): a text candidate names its
/// column by its text; a number or NULL names none. Under a name template
/// (`naming`: the template's text around the key, `None` at the key), the
/// template spells around each candidate, a number by its digits. `taken`
/// holds the columns the reduction's earlier pivots publish: two
/// candidates naming one column leave the pivot's column set ill-formed.
pub(crate) fn columns(
    values: &[LiteralValue],
    taken: &mut Vec<Name>,
    key: &str,
    naming: Option<&[Option<String>]>,
) -> Result<Vec<Name>, Refusal> {
    let spelled = |v: &LiteralValue| -> Option<String> {
        match v {
            LiteralValue::String(s) => Some(s.clone()),
            LiteralValue::Number(n) if naming.is_some() => Some(n.to_string()),
            _ => None,
        }
    };
    let mut names = Vec::with_capacity(values.len());
    for v in values {
        let Some(candidate) = spelled(v) else {
            return Err(refuse::pivot_key_spelling(key, &v.to_string()));
        };
        let name = match naming {
            None => Name::new(&candidate),
            Some(parts) => Name::new(
                parts
                    .iter()
                    .map(|part| part.as_deref().unwrap_or(&candidate))
                    .collect::<String>(),
            ),
        };
        if taken.contains(&name) {
            return Err(delightql_types::diagnostic::Constraint::Pivot {
                message: format!(
                    "two pivot candidates name the column '{name}' (column names fold case, and one reduction's \
                     pivots publish distinct columns); name a second pivot's columns apart"
                ),
            }
            .into());
        }
        taken.push(name.clone());
        names.push(name);
    }
    Ok(names)
}

/// Whether the rows a reduction's input holds are at most one per value of
/// `exprs` (its grouping keys and the pivot's key), as the chain proves it:
/// each traced through row-preserving stages of one occurrence to one
/// relation, which is a reduction grouping by a subset of them, or a
/// literal table whose rows are pairwise distinct over them.
pub(crate) fn single_row(arena: &impl Judging, members: &[(BinderId, RelId)], exprs: &[ExprId]) -> bool {
    let [(binder, rel)] = members else {
        return false;
    };
    let anchors: Option<Vec<(RelId, usize)>> = exprs
        .iter()
        .map(|e| match referenced_cell(arena, *e) {
            Some(Cell::Col(b, p)) if b == *binder => anchor(arena, *rel, usize::from(p)),
            _ => None,
        })
        .collect();
    let Some(anchors) = anchors else {
        return false;
    };
    let Some(&(source, _)) = anchors.first() else {
        return false;
    };
    if anchors.iter().any(|(r, _)| *r != source) {
        return false;
    }
    let positions: Vec<usize> = anchors.iter().map(|(_, p)| *p).collect();
    match arena.rel(source).kind() {
        RelKind::Pipe {
            op: PipeOp::Group { keys, .. },
            ..
        } => (0..keys.len()).all(|k| positions.contains(&k)),
        RelKind::Lit { rows, .. } => {
            let columns: Option<Vec<Vec<ExprId>>> = positions
                .iter()
                .map(|p| match provenance::of(arena, source, *p) {
                    Source::Rows(values) => Some(values),
                    _ => None,
                })
                .collect();
            let Some(columns) = columns else {
                return false;
            };
            let tuples: Option<Vec<Vec<&LiteralValue>>> = (0..rows.len())
                .map(|r| {
                    columns
                        .iter()
                        .map(|c| match arena.expr(c[r]).kind() {
                            ExprKind::Const(v) => Some(v),
                            _ => None,
                        })
                        .collect()
                })
                .collect();
            let Some(tuples) = tuples else {
                return false;
            };
            tuples
                .iter()
                .enumerate()
                .all(|(i, a)| tuples[i + 1..].iter().all(|b| a.iter().zip(b).any(|(x, y)| apart(x, y))))
        }
        _ => false,
    }
}

/// The relation and position a position of `rel` reaches through stages
/// that keep each of their input's rows once: a reduction or a literal
/// table ends the trace.
fn anchor(arena: &impl Judging, rel: RelId, p: usize) -> Option<(RelId, usize)> {
    match arena.rel(rel).kind() {
        RelKind::Lit { .. }
        | RelKind::Pipe {
            op: PipeOp::Group { .. },
            ..
        } => Some((rel, p)),
        RelKind::Pipe {
            input,
            op: PipeOp::Project(_) | PipeOp::Embed(_),
        }
        | RelKind::Order { input, .. } => {
            let cell = match provenance::of(arena, rel, p) {
                Source::Value { expr } => referenced_cell(arena, expr)?,
                Source::Position(r, q) if r == *input => one_member_cell(arena, *input, q)?,
                _ => return None,
            };
            through_run(arena, *input, cell)
        }
        RelKind::Read {
            source: ReadSource::Local(_),
            ..
        } => match provenance::of(arena, rel, p) {
            Source::Position(body, d) => anchor(arena, body, d),
            _ => None,
        },
        RelKind::Run(_) => {
            let cell = one_member_cell(arena, rel, p)?;
            through_run(arena, rel, cell)
        }
        _ => None,
    }
}

/// A cell of a run of one member, traced into that member's relation.
fn through_run(arena: &impl Judging, run: RelId, cell: Cell) -> Option<(RelId, usize)> {
    let RelKind::Run(r) = arena.rel(run).kind() else {
        return None;
    };
    let [member] = r.members().collect::<Vec<_>>()[..] else {
        return None;
    };
    match cell {
        Cell::Col(b, q) if b == member.binder() => anchor(arena, member.rel(), usize::from(q)),
        _ => None,
    }
}

/// Output position `q` of a run of one member: its cell.
fn one_member_cell(arena: &impl Judging, run: RelId, q: usize) -> Option<Cell> {
    let RelKind::Run(r) = arena.rel(run).kind() else {
        return None;
    };
    if r.members().count() != 1 {
        return None;
    }
    r.outputs().get(q).copied()
}

/// Whether two literal cells can never be one value under exact equality.
fn apart(x: &LiteralValue, y: &LiteralValue) -> bool {
    match (x, y) {
        (LiteralValue::Number(a), LiteralValue::Number(b)) => {
            match (a.to_string().parse::<f64>(), b.to_string().parse::<f64>()) {
                (Ok(a), Ok(b)) => a != b,
                _ => false,
            }
        }
        (LiteralValue::Null, _) | (_, LiteralValue::Null) => false,
        _ => x != y && std::mem::discriminant(x) == std::mem::discriminant(y),
    }
}

/// A membership witnessing `cell` among `guards`, else in an earlier stage
/// of the chain the cell's column comes from. A membership standing in a
/// member's own interior is no step of this chain.
fn search(
    arena: &impl Judging,
    guards: &[TruthId],
    cell: Cell,
    literals: &[RelId],
) -> Result<Option<Vec<LiteralValue>>, Refusal> {
    if let Some(values) = guards.iter().find_map(|g| membership_on(arena, *g, cell, literals)) {
        return Ok(Some(values));
    }
    let Cell::Col(binder, p) = cell else {
        return Ok(None);
    };
    let Some(rel) = arena.binder(binder).rel() else {
        return Ok(None);
    };
    let p = usize::from(p);
    match arena.rel(rel).kind() {
        RelKind::Pipe { input, .. } | RelKind::Order { input, .. } => {
            let earlier = match provenance::of(arena, rel, p) {
                Source::Value { expr } => referenced_cell(arena, expr),
                Source::Position(r, q) if r == *input => match arena.rel(*input).kind() {
                    RelKind::Run(run) => run.outputs().get(q).copied(),
                    _ => None,
                },
                _ => None,
            };
            match (arena.rel(*input).kind(), earlier) {
                (RelKind::Run(run), Some(earlier)) => {
                    search(arena, &run.guards().map(|g| g.truth()).collect::<Vec<_>>(), earlier, literals)
                }
                _ => Ok(None),
            }
        }
        RelKind::Run(run) => {
            let inner = run.outputs().get(p).copied();
            let guards: Vec<TruthId> = run.guards().map(|g| g.truth()).collect();
            match inner.map(|c| search(arena, &guards, c, literals)).transpose()? {
                Some(Some(_)) => Err(refuse::unruled(
                    "whether a pivot's witness may stand inside the interior of the relation it filters",
                )),
                _ => Ok(None),
            }
        }
        _ => Ok(None),
    }
}

/// The candidates of a membership on `cell` that `truth` states, itself or
/// as a conjunct: a list of literal candidates, or a call's literal
/// argument read through the formal it binds.
fn membership_on(arena: &impl Judging, truth: TruthId, cell: Cell, literals: &[RelId]) -> Option<Vec<LiteralValue>> {
    match arena.truth(truth).kind() {
        TruthKind::And(parts) => parts.iter().find_map(|p| membership_on(arena, *p, cell, literals)),
        TruthKind::Exists { positive: true, rel } => match arena.rel(*rel).kind() {
            RelKind::Lit { header, rows, .. } => {
                let [HeaderSlot::Constraint { value, .. }] = header.as_slice() else {
                    return None;
                };
                if referenced_cell(arena, *value) != Some(cell) {
                    return None;
                }
                rows.iter()
                    .map(|row| match row.as_slice() {
                        [one] => constant(arena, *one),
                        _ => None,
                    })
                    .collect()
            }
            RelKind::Run(run) => in_literal(arena, run, cell, literals),
            _ => None,
        },
        _ => None,
    }
}

/// A membership in a relation (`k in V(*)`) whose one member reads, through
/// local reads, one of `literals`, its one condition the key's cell equal
/// to a column of that member: the literal's values in that column.
fn in_literal(
    arena: &impl Judging,
    run: &crate::pipeline::middle::core::node::Run,
    cell: Cell,
    literals: &[RelId],
) -> Option<Vec<LiteralValue>> {
    let [member] = run.members().collect::<Vec<_>>()[..] else {
        return None;
    };
    let [guard] = run.guards().collect::<Vec<_>>()[..] else {
        return None;
    };
    let TruthKind::Cmp {
        op: crate::pipeline::middle::facade::CmpOp::NullSafeEqual,
        left,
        right,
        ..
    } = arena.truth(guard.truth()).kind()
    else {
        return None;
    };
    let column = match (referenced_cell(arena, *left), referenced_cell(arena, *right)) {
        (Some(probe), Some(Cell::Col(b, p))) | (Some(Cell::Col(b, p)), Some(probe)) if probe == cell && b == member.binder() => p,
        _ => return None,
    };
    let (mut rel, mut at) = (member.rel(), usize::from(column));
    while !literals.contains(&rel) {
        match provenance::of(arena, rel, at) {
            Source::Position(next, k) => (rel, at) = (next, k),
            _ => return None,
        }
    }
    match provenance::of(arena, rel, at) {
        Source::Rows(values) => values.into_iter().map(|v| constant(arena, v)).collect(),
        _ => None,
    }
}

fn constant(arena: &impl Judging, value: ExprId) -> Option<LiteralValue> {
    match arena.expr(value).kind() {
        ExprKind::Const(v) => Some(v.clone()),
        _ => None,
    }
}

/// A pivot key that is neither a column nor a name template interpolating
/// one column once (FN.10).
pub(crate) fn template_key() -> Refusal {
    delightql_types::diagnostic::Constraint::Pivot {
        message: "a pivot's key is a column with an authored membership predicate, or a name template that \
                  interpolates that column once (`v of :\"{k}_suffix\"`)"
            .to_string(),
    }
    .into()
}
