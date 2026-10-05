// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The realization's working records: what a core reference reads in the
//! stage being written and whose population its rows are (the
//! environment), what the relation being realized reads from outside
//! itself (the enclosing), a realized relation (a table), and the rows
//! being joined (a frame). Each lives only through the realization of one
//! statement for one target.

use crate::pipeline::middle::core::graph::{Arena, Graph};
use crate::pipeline::middle::core::ids::{BinderId, ExprId, MergeId, PassengerId, RelId};
use crate::pipeline::middle::facade::{ColId, SqlDirection, SqlExpr, TableExpression};
use std::collections::BTreeSet;

/// A column a private record of one owner member holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Private {
    /// The owner's witness: one value per occurrence of its carrier.
    Witness,
    /// The owner's KEEP flag: 1 on real rows, 0 on carriers.
    Flag,
    /// Present exactly on the rows the owner's join matched.
    Matched,
    /// A rank within the owner's partition.
    Rank,
    /// The k-th key of the ordering a member's relation hands its run.
    Order(u16),
}

/// What a column of the stage being written stands for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Key {
    /// A member's position.
    Col(BinderId, u16),
    /// A merged key.
    Merged(MergeId),
    /// A passenger, wherever its hidden position travels.
    Passenger(PassengerId),
    /// A value computed in a stage below the one that reads it.
    Value(ExprId),
    /// A private column of the owner member bound to the binder.
    Private(BinderId, Private),
    /// Position `i` of a relation realized over a carrier.
    Pos(RelId, u16),
    /// The k-th slot column a read offers its join's condition.
    Lifted(u16),
    /// A collection's nested level `j`, as a document per row of its
    /// enclosing level's group.
    Level(ExprId, u16),
    /// The one row of each group of a collection's level `j` its enclosing
    /// level collects.
    Pick(ExprId, u16),
    /// What a metadata level `j` holds for the key of each row: its rows of
    /// that key collected, or its further level's object.
    KeyedRows(ExprId, u16),
    /// The one row of each key of a metadata level `j` its object takes.
    KeyPick(ExprId, u16),
    /// The node level `j` of an expansion expands, staged as a column.
    Node(RelId, u16),
    /// The node (the value with its kind) of position `i` an expansion
    /// binds, read from the same element as its value.
    BindNode(RelId, u16),
    /// The node of a member's position, where its relation offers one.
    ColNode(BinderId, u16),
}

/// What each core reference reads in one stage, and whose population the
/// stage's rows are: the dependent members that own them, innermost last.
/// A relation's own rows (the statement's, a correlated subquery's, an
/// independent relation's) are owned by none. Entries are kept in the
/// order they were bound, which is the order a stage carries them.
#[derive(Clone)]
pub(super) struct Env {
    owners: Vec<BinderId>,
    entries: Vec<(Key, SqlExpr)>,
}

impl Env {
    /// The environment of a relation's own rows: no entry, no owner.
    pub(super) fn fresh() -> Self {
        Env {
            owners: Vec::new(),
            entries: Vec::new(),
        }
    }

    /// The environment of a stage written over this one's rows: no entry
    /// yet, and the same population.
    pub(super) fn next(&self) -> Self {
        Env {
            owners: self.owners.clone(),
            entries: Vec::new(),
        }
    }

    pub(super) fn get(&self, key: Key) -> Option<&SqlExpr> {
        self.entries.iter().rev().find(|(k, _)| *k == key).map(|(_, e)| e)
    }

    pub(super) fn bind(&mut self, key: Key, value: SqlExpr) {
        self.entries.retain(|(k, _)| *k != key);
        self.entries.push((key, value));
    }

    pub(super) fn entries(&self) -> &[(Key, SqlExpr)] {
        &self.entries
    }

    pub(super) fn retain(&mut self, keep: impl Fn(&Key) -> bool) {
        self.entries.retain(|(k, _)| keep(k));
    }

    /// The member whose population the rows are, innermost.
    pub(super) fn owner(&self) -> Option<BinderId> {
        self.owners.last().copied()
    }

    /// The rows become the population of `member`, from its carrier on.
    pub(super) fn enter(&mut self, member: BinderId) {
        self.owners.push(member);
    }

    /// The rows stop being `member`'s population when it completes; `false`
    /// when they are not its population, which leaves them unchanged.
    #[must_use]
    pub(super) fn leave(&mut self, member: BinderId) -> bool {
        if self.owner() != Some(member) {
            return false;
        }
        self.owners.pop();
        true
    }
}

/// What the relation being realized reads from outside itself: the entries
/// naming exactly its free binders (an enclosing query's row, for a
/// correlated subquery; the working table's carried columns, for a
/// recursive part), and whether it is written in place, at the level of
/// the working table a recursive part reads.
pub(super) struct Enclosing {
    free: BTreeSet<BinderId>,
    merges: BTreeSet<MergeId>,
    /// The value definitions' arguments the enclosing query computed, which
    /// the relation reads as values of the row it stands over.
    arguments: BTreeSet<ExprId>,
    entries: Env,
    pub(super) in_place: bool,
}

impl Enclosing {
    /// A relation that reads nothing from outside itself.
    pub(super) fn none() -> Self {
        Enclosing {
            free: BTreeSet::new(),
            merges: BTreeSet::new(),
            arguments: BTreeSet::new(),
            entries: Env::fresh(),
            in_place: false,
        }
    }

    /// What `rel` reads from outside itself, out of `sources` (the nearest
    /// first): the entries naming its free binders, and the merged keys
    /// whose operands are all free.
    pub(super) fn of(graph: &Graph, rel: RelId, sources: &[&Env], in_place: bool) -> Self {
        let free: BTreeSet<BinderId> = graph.rel(rel).fv().iter().copied().collect();
        let mut entries = Env::fresh();
        let mut merges = BTreeSet::new();
        let mut arguments = BTreeSet::new();
        for source in sources {
            for (key, value) in source.entries() {
                let offered = match key {
                    Key::Col(b, _) => free.contains(b),
                    Key::Merged(m) => crate::pipeline::middle::core::node::expr::merge_binders(graph, *m)
                        .iter()
                        .all(|b| free.contains(b)),
                    Key::Value(e) => {
                        matches!(graph.expr(*e).kind(), crate::pipeline::middle::core::node::ExprKind::Argument { .. })
                            && graph.expr(*e).fv().iter().all(|b| free.contains(b))
                    }
                    _ => false,
                };
                if offered && entries.get(*key).is_none() {
                    match key {
                        Key::Merged(m) => {
                            merges.insert(*m);
                        }
                        Key::Value(e) => {
                            arguments.insert(*e);
                        }
                        _ => {}
                    }
                    entries.bind(*key, value.clone());
                }
            }
        }
        Enclosing {
            free,
            merges,
            arguments,
            entries,
            in_place,
        }
    }

    /// Whether a reference is read from outside the relation: a column of
    /// a free binder, or a merged key of free binders.
    pub(super) fn offers(&self, key: Key) -> bool {
        match key {
            Key::Col(b, _) => self.free.contains(&b),
            Key::Merged(m) => self.merges.contains(&m),
            Key::Value(e) => self.arguments.contains(&e),
            _ => false,
        }
    }

    pub(super) fn get(&self, key: Key) -> Option<&SqlExpr> {
        self.entries.get(key)
    }

    /// Every entry the enclosing offers, for a relation nested in the one
    /// being realized.
    pub(super) fn entries(&self) -> &Env {
        &self.entries
    }
}

/// A realized relation: a FROM item, and the column it offers for each
/// position of the relation's heading, hidden positions included.
pub(super) struct Table {
    pub(super) from: TableExpression,
    pub(super) cols: Vec<ColId>,
    /// An ordering the relation hands its consumer (a statement's final
    /// ordering, or the bound after it), over columns it offers past its
    /// heading. Empty: the relation is a bag.
    pub(super) order: Vec<(ColId, SqlDirection)>,
}

/// Rows being joined: the FROM tree built so far (`None`: the one row with
/// no columns) and what every core reference reads in it.
pub(super) struct Frame {
    pub(super) from: Option<TableExpression>,
    pub(super) env: Env,
}

impl Frame {
    pub(super) fn unit() -> Self {
        Frame {
            from: None,
            env: Env::fresh(),
        }
    }
}
