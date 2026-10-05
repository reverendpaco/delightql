// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The mutation contract's judgments: how a marked read's table is reached
//! row by row, which occurrence a mutation's source marks, what stands on
//! the path from that occurrence to the terminal, where the occurrence's
//! row locator arrives at the terminal, which incoming position replaces
//! each column of the target, and which source position an insertion
//! writes to each target column. A column the table's storage computes is
//! part of the target's heading and is never written.

use crate::pipeline::middle::core::graph::Judging;
use crate::pipeline::middle::core::heading::correspondence::answers_to;
use crate::pipeline::middle::core::heading::{Heading, Name, NameState, Visibility};
use crate::pipeline::middle::core::ids::{PassengerId, RelId};
use crate::pipeline::middle::core::node::rel::child_rels;
use crate::pipeline::middle::core::node::{CatalogColumn, PipeOp, Qual, ReadSource, RelKind, RowLocator};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::facade::{RowIdentityFacts, StoredRowIdentity, Unanswered};

/// The marked reads a relation's rows descend from: each row locator born
/// beneath it, its parts in order, with the read that bore it. A receipt's
/// rows are no occurrence's rows, so the marks beneath one are its own
/// act's and are not reached. A shared node is visited once.
pub(crate) fn marks(arena: &impl Judging, rel: RelId) -> Vec<(Vec<PassengerId>, RelId)> {
    let mut seen: Vec<RelId> = Vec::new();
    let mut pending = vec![rel];
    let mut found = Vec::new();
    while let Some(r) = pending.pop() {
        if seen.contains(&r) {
            continue;
        }
        seen.push(r);
        let kind = arena.rel(r).kind();
        if let RelKind::Read {
            source: ReadSource::Catalog { locator, .. },
            ..
        } = kind
        {
            if !locator.is_empty() {
                found.push((locator.clone(), r));
            }
        }
        if matches!(kind, RelKind::Receipt { .. }) {
            continue;
        }
        pending.extend(child_rels(arena, kind));
    }
    found.sort();
    found
}

/// What stands among the relations a source's rows descend from, relative
/// to the marked read: a bound on the descent from the marked read to the
/// terminal or elsewhere, and a grouping on that descent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MarkedPath {
    pub(crate) bound_on_path: bool,
    pub(crate) bound_elsewhere: bool,
    pub(crate) grouped_on_path: bool,
}

/// The facts of the relations under `rel`, relative to the marked read
/// `mark`.
pub(crate) fn marked_path(arena: &impl Judging, rel: RelId, mark: RelId) -> MarkedPath {
    walk_path(arena, rel, mark).1
}

/// Whether `r` reaches the marked read, and the facts beneath it.
fn walk_path(arena: &impl Judging, r: RelId, mark: RelId) -> (bool, MarkedPath) {
    let kind = arena.rel(r).kind();
    let (reaches, below) = child_rels(arena, kind).into_iter().fold(
        (r == mark, MarkedPath::default()),
        |(reaches, acc), child| {
            let (child_reaches, child_facts) = walk_path(arena, child, mark);
            (
                reaches || child_reaches,
                MarkedPath {
                    bound_on_path: acc.bound_on_path || child_facts.bound_on_path,
                    bound_elsewhere: acc.bound_elsewhere || child_facts.bound_elsewhere,
                    grouped_on_path: acc.grouped_on_path || child_facts.grouped_on_path,
                },
            )
        },
    );
    let bounded = match kind {
        RelKind::Run(run) => run.quals().iter().any(|q| matches!(q, Qual::Bound(_))),
        RelKind::Order { bound, .. } => bound.is_some(),
        RelKind::Read { .. }
        | RelKind::Lit { .. }
        | RelKind::Receipt { .. }
        | RelKind::Pipe { .. }
        | RelKind::SetOp { .. }
        | RelKind::Minus { .. }
        | RelKind::Meta { .. }
        | RelKind::Witnessed { .. }
        | RelKind::Unnest { .. }
        | RelKind::Family { .. }
        | RelKind::Fix(_)
        | RelKind::Apply { .. } => false,
    };
    let grouping = match kind {
        RelKind::Pipe { op, .. } => match op {
            PipeOp::Group { .. } | PipeOp::Distinct(_) => true,
            PipeOp::Project(_)
            | PipeOp::Embed(_)
            | PipeOp::ProjectOut(_)
            | PipeOp::Cover(_)
            | PipeOp::Carry(_) => false,
        },
        RelKind::Read { .. }
        | RelKind::Lit { .. }
        | RelKind::Receipt { .. }
        | RelKind::Run(_)
        | RelKind::Order { .. }
        | RelKind::SetOp { .. }
        | RelKind::Minus { .. }
        | RelKind::Meta { .. }
        | RelKind::Witnessed { .. }
        | RelKind::Unnest { .. }
        | RelKind::Family { .. }
        | RelKind::Fix(_)
        | RelKind::Apply { .. } => false,
    };
    (
        reaches,
        MarkedPath {
            bound_on_path: below.bound_on_path || (bounded && reaches),
            bound_elsewhere: below.bound_elsewhere || (bounded && !reaches),
            grouped_on_path: below.grouped_on_path || (grouping && reaches),
        },
    )
}

/// Where each part of a marked occurrence's row locator stands in a
/// mutation source's heading. Only [`arrival`] makes one, from that
/// heading; the act that holds it holds the source it was decided from,
/// which never changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Arrival {
    parts: Vec<(PassengerId, u16)>,
}

impl Arrival {
    /// The parts of the row locator that arrives, in the locator's order.
    pub(crate) fn locator(&self) -> Vec<PassengerId> {
        self.parts.iter().map(|(p, _)| *p).collect()
    }
    /// The one position of the source heading holding each part, in the
    /// locator's order.
    pub(crate) fn positions(&self) -> Vec<usize> {
        self.parts.iter().map(|(_, at)| usize::from(*at)).collect()
    }
}

/// Where each part of the marked occurrence's row locator stands in the
/// source's heading: once each, or the terminal has nothing to reach the
/// rows by.
pub(crate) fn arrival(source: &Heading, locator: &[PassengerId], verb: &str) -> Result<Arrival, Refusal> {
    let mut parts = Vec::with_capacity(locator.len());
    for part in locator {
        let at: Vec<usize> = source
            .positions()
            .iter()
            .enumerate()
            .filter(|(_, p)| p.visibility == Visibility::Hidden(*part))
            .map(|(i, _)| i)
            .collect();
        match at.as_slice() {
            [one] => parts.push((
                *part,
                u16::try_from(*one).map_err(|_| refuse::outside("a heading of more than 65535 positions"))?,
            )),
            [] => return Err(refuse::source_occurrence(verb)),
            _ => return Err(refuse::outside("a marked occurrence whose rows reach the terminal twice")),
        }
    }
    Ok(Arrival { parts })
}

/// THE ROW IDENTITY OF A MARKED READ'S TABLE (THE MUTATION MARKER: the
/// locator is minted whole at the marked read, and a target that exposes no
/// row locator to a statement refuses there), as the locator's parts in
/// order. The locator is read under the first of the target's names for it
/// that no column of the table occupies (its storage's names, hidden and
/// generated columns included, compared without regard to case as SQLite
/// does); where every name is occupied, through the stored column that is
/// the locator; a table whose every name another column occupies exposes
/// none. A table whose storage keeps no locator is reached through its
/// declared primary key, which tells its stored rows apart: one part per
/// key column, the column itself; with no key it exposes none.
pub(crate) fn row_locator(table: &Name, columns: &[CatalogColumn], facts: &RowIdentityFacts) -> Result<Vec<RowLocator>, Refusal> {
    let Some(parts) = facts.spellings else {
        return Err(refuse::row_identity(table.as_str()));
    };
    let [spellings] = parts else {
        return Err(refuse::outside("a row locator of several parts"));
    };
    let (alias, occupied) = match &facts.stored {
        Err(Unanswered::Backend) => {
            return Err(refuse::outside("a marked read of a table whose backend does not answer how its rows are identified"))
        }
        Err(Unanswered::OtherDialect) => {
            return Err(refuse::outside(
                "a marked read of a table whose backend serves another dialect than the statement's target, so its storage is not the target's",
            ))
        }
        Ok(StoredRowIdentity::Unlocated { key }) if key.is_empty() => return Err(refuse::row_identity(table.as_str())),
        Ok(StoredRowIdentity::Unlocated { key }) => {
            return key.iter().map(|part| stored_column(table, columns, part)).collect();
        }
        Ok(StoredRowIdentity::Located { alias, occupied }) => (alias, occupied),
    };
    let taken = |spelling: &str| occupied.iter().any(|name| name.as_str().eq_ignore_ascii_case(spelling));
    if let Some(free) = spellings.iter().find(|s| !taken(s)) {
        return Ok(vec![RowLocator::Pseudo(Name::new(*free))]);
    }
    match alias {
        Some(alias) => Ok(vec![stored_column(table, columns, alias)?]),
        None => Err(refuse::row_identity(table.as_str())),
    }
}

/// The catalog column a storage names as (a part of) its row locator.
fn stored_column(table: &Name, columns: &[CatalogColumn], name: &Name) -> Result<RowLocator, Refusal> {
    let k = columns
        .iter()
        .position(|c| c.name.as_str() == name.as_str())
        .ok_or_else(|| refuse::row_identity(table.as_str()))?;
    Ok(RowLocator::Column(u16::try_from(k).map_err(|_| refuse::outside("a table of more than 65535 columns"))?))
}

/// The update's incoming row: each column of the target paired with the
/// one displayed position of the source whose name answers to it, as
/// `(target position, source position)` in target order. The incoming
/// heading must be the target's heading by name, in any order: every
/// displayed position names exactly one target column, no column is named
/// twice, and every column is named. Hidden positions are not part of it.
pub(crate) fn replacement(target: &Heading, source: &Heading) -> Result<Vec<(u16, u16)>, Refusal> {
    let mut incoming: Vec<Option<usize>> = vec![None; target.len()];
    for (i, p) in source.displayed() {
        let Some(name) = p.answering_name() else {
            return Err(refuse::update_heading(&format!(
                "the incoming column '{}' has no name, so it is no column of the relation being \
                 updated",
                p.display()
            )));
        };
        let column = match answers_to(target, name).as_slice() {
            [one] => *one,
            [] => {
                return Err(refuse::update_heading(&format!(
                    "the incoming column '{name}' names no column of the relation being updated"
                )))
            }
            _ => {
                return Err(refuse::update_heading(&format!(
                    "'{name}' names more than one column of the relation being updated"
                )))
            }
        };
        if incoming[column].is_some() {
            return Err(refuse::update_heading(&format!("two incoming columns are named '{name}'")));
        }
        incoming[column] = Some(i);
    }
    target
        .displayed()
        .map(|(t, p)| match incoming[t] {
            Some(s) => Ok((t as u16, s as u16)),
            None => Err(refuse::update_heading(&format!(
                "the incoming heading has no column '{}' of the relation being updated",
                p.display()
            ))),
        })
        .collect()
}

/// THE SHAPE RULE of a deletion: the source carries every column of the
/// target by name — answered by exactly one displayed position, or, where a
/// collision with another member's column took the name, held unchanged by
/// the one position that lost it beside that member — whatever else it
/// publishes; the rows its marked read located are deleted, each once. A
/// column the table generates must arrive as the row's own value: a cover
/// of it would be dropped unread.
pub(crate) fn published(arena: &impl Judging, target: RelId, source: RelId, mark: RelId) -> Result<(), Refusal> {
    let computed = computed(arena, target);
    let heading = arena.rel(source).heading();
    for (t, column) in arena.rel(target).heading().displayed() {
        let Some(name) = column.answering_name() else {
            return Err(refuse::delete_heading(&format!("the target's column '{}' has no name", column.display())));
        };
        let at = match answers_to(heading, name).as_slice() {
            [one] => *one,
            [] => {
                let held: Vec<usize> = heading
                    .displayed()
                    .filter(|(i, p)| {
                        matches!(&p.name, NameState::Lost(lost) if lost == name)
                            && incoming_is_stored(arena, source, *i, mark, t)
                    })
                    .map(|(i, _)| i)
                    .collect();
                match held.as_slice() {
                    [one] => *one,
                    _ => {
                        return Err(refuse::delete_heading(&format!(
                            "the source carries no column '{name}' of the target relation"
                        )))
                    }
                }
            }
            _ => {
                return Err(refuse::delete_heading(&format!(
                    "the source publishes the target's column '{name}' more than once"
                )))
            }
        };
        if generated(&computed, t)? && !incoming_is_stored(arena, source, at, mark, t) {
            return Err(refuse::delete_heading(&format!(
                "'{name}' is a column the table generates, so it arrives as the row's own value: a cover of it \
                 would be dropped unread"
            )));
        }
    }
    Ok(())
}

/// Which columns of a row write's target its storage computes, by target
/// position: the target is a read of the whole stored table, one position
/// per catalog column.
pub(crate) fn computed(arena: &impl Judging, target: RelId) -> Vec<Option<bool>> {
    match arena.rel(target).kind() {
        RelKind::Read {
            source: ReadSource::Catalog { columns, .. },
            ..
        } => columns.iter().map(|c| c.computed).collect(),
        _ => Vec::new(),
    }
}

/// Whether target position `t` is a column its table generates; a backend
/// that gave no answer leaves a mutation of the table undecided.
fn generated(computed: &[Option<bool>], t: usize) -> Result<bool, Refusal> {
    match computed.get(t) {
        Some(Some(generated)) => Ok(*generated),
        Some(None) => Err(refuse::outside(
            "a mutation of a table whose serving backend does not answer which of its columns it generates",
        )),
        None => Ok(false),
    }
}

/// What a replacement of the rows the marked read `mark` located writes:
/// the shape rule's pairs, less each column the storage computes, which it
/// recomputes from the row's written columns. Such a column is still named
/// by the incoming heading, and the value paired with it must be the
/// reached row's own ([`incoming_is_stored`]).
pub(crate) fn replaced(arena: &impl Judging, target: RelId, source: RelId, mark: RelId) -> Result<Vec<(u16, u16)>, Refusal> {
    let pairs = replacement(arena.rel(target).heading(), arena.rel(source).heading())?;
    let computed = computed(arena, target);
    let mut written = Vec::with_capacity(pairs.len());
    for (t, s) in pairs {
        if !generated(&computed, usize::from(t))? {
            written.push((t, s));
        } else if !incoming_is_stored(arena, source, usize::from(s), mark, usize::from(t)) {
            let name = arena.rel(source).heading().positions()[usize::from(s)].display();
            return Err(refuse::update_heading(&format!(
                "'{name}' is a column the table generates, which update! never writes, so its incoming value \
                 must be the row's own: another value would be dropped unwritten"
            )));
        }
    }
    Ok(written)
}

/// THE ONE JUDGMENT of whether position `at` of `source` holds column
/// `column` of the marked read `mark` unchanged: the row's own stored value.
/// It follows the position down through the forms that pass a value on
/// unchanged (`node::provenance`): a same position, a member's column, a
/// reference, and a merged key, which holds the operand its rule takes (a
/// key both operands may supply by presence holds neither). `update!` and
/// `delete!` spend it for a generated column, `delete!` for a column a
/// collision took.
pub(crate) fn incoming_is_stored(arena: &impl Judging, source: RelId, at: usize, mark: RelId, column: usize) -> bool {
    use crate::pipeline::middle::core::node::provenance::{self, Source};
    use crate::pipeline::middle::core::node::{Cell, ExprKind, MergeRule};
    let mut next = Some((source, at));
    let mut seen: Vec<(RelId, usize)> = Vec::new();
    while let Some(here) = next.take() {
        if seen.contains(&here) {
            return false;
        }
        seen.push(here);
        let mut cell = match provenance::of(arena, here.0, here.1) {
            Source::Catalog { read, column: k } => return read == mark && k == column,
            Source::Position(below, j) => {
                next = Some((below, j));
                continue;
            }
            Source::Cell { run, cell } => (Some(run), cell),
            Source::Value { expr } => match arena.expr(expr).kind() {
                ExprKind::Col(b, j) => (None, Cell::Col(*b, *j)),
                _ => return false,
            },
            _ => return false,
        };
        loop {
            match cell {
                (_, Cell::Col(b, j)) => {
                    next = arena.binder(b).rel().map(|rel| (rel, usize::from(j)));
                    break;
                }
                (Some(run), Cell::Merged(m)) => {
                    let RelKind::Run(r) = arena.rel(run).kind() else {
                        return false;
                    };
                    let site = arena.merge(m);
                    let rule = r.merges().iter().find_map(|(key, rule, _)| (*key == m).then_some(*rule));
                    cell = match rule {
                        Some(MergeRule::Agreed | MergeRule::Left) => (Some(run), site.left()),
                        Some(MergeRule::Right) => (Some(run), Cell::Col(site.right().0, site.right().1)),
                        Some(MergeRule::Coalesced) | None => return false,
                    };
                }
                (None, Cell::Merged(_)) => return false,
            }
        }
    }
    false
}

/// An insertion's pairing: each displayed source position paired with the
/// one target column its name answers to, as `(target position, source
/// position)` in target order. A source position that names no column of
/// the target, or names one another position already names, refuses; a
/// target column no source position names takes NULL, and one the storage
/// computes, its computed value. Whether a source position may name a
/// computed column is not ruled. Hidden positions are not part of it.
pub(crate) fn insertion(target: &Heading, source: &Heading, computed: &[Option<bool>]) -> Result<Vec<(u16, u16)>, Refusal> {
    let mut incoming: Vec<Option<usize>> = vec![None; target.len()];
    for (i, p) in source.displayed() {
        let column = p
            .answering_name()
            .and_then(|name| match answers_to(target, name).as_slice() {
                [one] => Some(*one),
                _ => None,
            })
            .ok_or_else(|| refuse::insert_unnamed_column(&p.display()))?;
        if incoming[column].is_some() {
            return Err(refuse::insert_unnamed_column(&p.display()));
        }
        if generated(computed, column)? {
            return Err(refuse::generated_insert(&p.display()));
        }
        incoming[column] = Some(i);
    }
    Ok(incoming
        .into_iter()
        .enumerate()
        .filter_map(|(t, s)| s.map(|s| (t as u16, s as u16)))
        .collect())
}
