// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! B3, B4: a comma run's rows. The members join in the order their stored
//! roles give (the required core, then each attached member once the
//! members it reads are present; or the written-order fold), each relation
//! as the condition of the join its stored placement names, each merged key
//! read by its stored rule. A member whose rows exist per row of the members
//! before it is realized over them (R1–R6). A bound closes the run so far as
//! a stage.

use super::frame::{Enclosing, Env, Frame, Key};
use super::rel::{equality, limit_of, Lifted};
use super::value::{At, Consumer};
use super::{Realizer, Result};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::Visibility;
use crate::pipeline::middle::core::ids::{BinderId, MergeId, RelId, TruthId};
use crate::pipeline::middle::core::node::{
    Bound, Dependence, FoldJoin, Guard, JoinRole, Member, MergeRule, Placement, Qual, RelKind, Run,
};
use crate::pipeline::middle::facade::{
    JoinCondition, JoinType, OrderTerm, SelectItem, SqlDirection, SqlExpr, TableExpression,
};
use std::collections::{BTreeMap, BTreeSet};

/// A frame and the filters its consumer's stage applies.
pub(super) struct Rows {
    pub(super) frame: Frame,
    pub(super) filter: Vec<SqlExpr>,
}

impl Rows {
    pub(super) fn of(frame: Frame) -> Self {
        Rows {
            frame,
            filter: Vec::new(),
        }
    }
}

/// A run's qualifiers, indexed: its members and its guards in authored
/// order, each holding the facts its run decided.
pub(super) struct Quals<'g> {
    pub(super) run: &'g Run,
    pub(super) members: Vec<&'g Member>,
    pub(super) guards: Vec<&'g Guard>,
}

impl<'g> Quals<'g> {
    pub(super) fn of(run: &'g Run) -> Self {
        Quals {
            run,
            members: run.members().collect(),
            guards: run.guards().collect(),
        }
    }

    /// Each qualifier with its member or guard index.
    pub(super) fn indexed(&self) -> Vec<Indexed> {
        let (mut mi, mut gi) = (0, 0);
        self.run
            .quals()
            .iter()
            .map(|q| match q {
                Qual::Member(_) => {
                    mi += 1;
                    Indexed::Member(mi - 1)
                }
                Qual::Guard(_) => {
                    gi += 1;
                    Indexed::Guard(gi - 1)
                }
                Qual::Bound(b) => Indexed::Bound(b.clone()),
            })
            .collect()
    }
}

/// A qualifier by its index among its kind.
#[derive(Clone)]
pub(super) enum Indexed {
    Member(usize),
    Guard(usize),
    Bound(Bound),
}

/// A run's population, one segment: its members in the order and by the
/// join their stored roles give, its filters (guard indexes), and the
/// boundary that closes it.
pub(super) struct Segment {
    pub(super) joins: Vec<(usize, JoinType)>,
    pub(super) guards: Vec<usize>,
    pub(super) boundary: Option<Boundary>,
}

/// A qualifier applied before any later join, because it does not commute
/// with one: a bound, or a filter (by its guard index).
pub(super) enum Boundary {
    Bound(Bound),
    Filter(usize),
}

/// A condition of a run, as the core placed it.
pub(super) enum Condition {
    Guard(TruthId),
    Merge(MergeId),
    /// A read's slot constraint, lifted out of the read for the join the
    /// core placed it on.
    Lifted(Lifted),
}

/// A run's conditions as the core placed them, each taken once: a member's
/// match conditions by the realization of that member's join, a filter by
/// the stage that applies it. A run realization that closes with one left
/// has not applied it, and refuses.
pub(super) struct Placed {
    on: BTreeMap<usize, Vec<Condition>>,
    filters: BTreeMap<usize, TruthId>,
}

impl Placed {
    pub(super) fn of(realizer: &Realizer<'_, '_>, q: &Quals<'_>) -> Result<Self> {
        let mut on: BTreeMap<usize, Vec<Condition>> = BTreeMap::new();
        let mut filters = BTreeMap::new();
        for (g, guard) in q.guards.iter().enumerate() {
            match guard.placement() {
                Placement::Filter => {
                    filters.insert(g, guard.truth());
                }
                Placement::On { member } => on.entry(member).or_default().push(Condition::Guard(guard.truth())),
            }
        }
        for (mid, _, placement) in q.run.merges() {
            match placement {
                Placement::On { member } => on.entry(*member).or_default().push(Condition::Merge(*mid)),
                Placement::Filter => return Err(realizer.uncovered("a merge placed as a filter")),
            }
        }
        Ok(Placed { on, filters })
    }

    /// A slot constraint lifted out of a read, for the join of `at`.
    pub(super) fn lift(&mut self, at: usize, lifted: Lifted) {
        self.on.entry(at).or_default().push(Condition::Lifted(lifted));
    }

    /// Member `j`'s match conditions, taken.
    pub(super) fn take(&mut self, j: usize) -> Vec<Condition> {
        self.on.remove(&j).unwrap_or_default()
    }

    /// The filter written as the run's `g`-th condition, taken; `None` when
    /// the core placed that condition on a join.
    pub(super) fn filter(&mut self, g: usize) -> Option<TruthId> {
        self.filters.remove(&g)
    }

    pub(super) fn close(self, realizer: &Realizer<'_, '_>) -> Result<()> {
        if self.on.values().all(|c| c.is_empty()) && self.filters.is_empty() {
            return Ok(());
        }
        Err(realizer.contract("a condition of a run that no member's join or filter stage applied"))
    }
}

impl Realizer<'_, '_> {
    /// The rows of a run: its members joined, its filters translated for
    /// the consuming stage.
    pub(super) fn run_rows(&mut self, r: RelId, outer: &Enclosing) -> Result<Rows> {
        let graph = self.graph;
        let RelKind::Run(run) = graph.rel(r).kind() else {
            return Err(self.contract("the rows of a relation that is no run"));
        };
        let q = Quals::of(run);
        let mut placed = Placed::of(self, &q)?;
        // A recursive step's cap is its member's own row clause, written
        // by the fixpoint: the run's rows are taken whole.
        let capped = self.caps.contains(&r);
        if outer.in_place {
            if let Some(rows) = self.inlined(&q, &mut placed, outer, capped)? {
                placed.close(self)?;
                return Ok(rows);
            }
        }
        let items = q.indexed();
        let boundaries = self.boundaries(&q, &items);
        let mut segments = self.run_population(run, &items, &boundaries, |_| true, &[])?;
        if capped {
            for segment in &mut segments {
                if matches!(segment.boundary, Some(Boundary::Bound(_))) {
                    segment.boundary = None;
                }
            }
        }
        let mut ordering: Vec<(Key, SqlDirection)> = Vec::new();
        let (rows, filters) = self.realize_population(Rows::of(Frame::unit()), &q, &mut placed, segments, &mut ordering, outer)?;
        placed.close(self)?;
        self.filtered(rows, &filters, outer)
    }

    /// A run's population (predicate-placement-law: population evaluation),
    /// the one judgment every realization of a run reads: the qualifiers
    /// `within` selects, in segments closed by their boundaries, each
    /// segment's members in the order and by the join their stored roles
    /// give after the members already `present`.
    pub(super) fn run_population(
        &self,
        run: &Run,
        items: &[Indexed],
        boundaries: &[bool],
        within: impl Fn(usize) -> bool,
        present: &[usize],
    ) -> Result<Vec<Segment>> {
        let mut cut: Vec<(Vec<usize>, Vec<usize>, Option<Boundary>)> = vec![(Vec::new(), Vec::new(), None)];
        for (at, item) in items.iter().enumerate().filter(|(at, _)| within(*at)) {
            let seg = cut.last_mut().expect("a segment");
            match item {
                Indexed::Member(j) => seg.0.push(*j),
                Indexed::Guard(g) if boundaries[at] => {
                    seg.2 = Some(Boundary::Filter(*g));
                    cut.push((Vec::new(), Vec::new(), None));
                }
                Indexed::Guard(g) => seg.1.push(*g),
                Indexed::Bound(b) => {
                    seg.2 = Some(Boundary::Bound(b.clone()));
                    cut.push((Vec::new(), Vec::new(), None));
                }
            }
        }
        let mut joined = present.to_vec();
        let mut segments = Vec::with_capacity(cut.len());
        for (members, guards, boundary) in cut {
            let joins = self.join_order(run, &members, &joined)?;
            joined.extend(joins.iter().map(|(j, _)| *j));
            segments.push(Segment { joins, guards, boundary });
        }
        Ok(segments)
    }

    /// Realize a population over `rows`: each segment's members joined, its
    /// boundary applied before any later join. The filters no boundary
    /// applied are returned, for the consuming stage.
    pub(super) fn realize_population(
        &mut self,
        rows: Rows,
        q: &Quals<'_>,
        placed: &mut Placed,
        segments: Vec<Segment>,
        ordering: &mut Vec<(Key, SqlDirection)>,
        outer: &Enclosing,
    ) -> Result<(Rows, Vec<TruthId>)> {
        let mut rows = rows;
        let mut present: Vec<usize> = Vec::new();
        let mut filters: Vec<TruthId> = Vec::new();
        let unit = BTreeSet::new();
        let joins_after: Vec<bool> = (0..segments.len())
            .map(|i| segments[i + 1..].iter().any(|later| !later.joins.is_empty()))
            .collect();
        for (i, seg) in segments.into_iter().enumerate() {
            let mut written: Vec<TruthId> = seg.guards.iter().filter_map(|g| placed.filter(*g)).collect();
            for (j, join) in seg.joins {
                // A condition reading only the members already joined
                // applies beneath a member realized over them: its rows are
                // the carrier that member's realization stages, so nothing
                // is evaluated for a carrier row the query discards.
                if !present.is_empty() && self.realized_over(q, j) {
                    let joined: BTreeSet<BinderId> = present.iter().map(|p| q.members[*p].binder()).collect();
                    let reads_joined = |t: &TruthId| self.graph.truth(*t).fv().iter().all(|b| joined.contains(b));
                    let ready: Vec<TruthId> = filters.iter().chain(&written).copied().filter(reads_joined).collect();
                    if !ready.is_empty() {
                        rows = self.filtered(rows, &ready, outer)?;
                        filters.retain(|t| !ready.contains(t));
                        written.retain(|t| !ready.contains(t));
                    }
                }
                if join == JoinType::Full && matches!(rows.frame.from, Some(TableExpression::Join { .. })) {
                    // DECISION(spelling): a full join's left operand is the
                    // fold so far as one stage, so its merged keys are
                    // columns.
                    let frame = self.stage(rows, &[], outer)?;
                    rows = Rows::of(frame);
                }
                rows = self.member(rows, q, placed, j, join, &unit, outer, ordering, &present)?;
                present.push(j);
            }
            filters.extend(written);
            match seg.boundary {
                Some(Boundary::Bound(bound)) => {
                    rows = self.filtered(rows, &filters, outer)?;
                    filters.clear();
                    if rows.frame.from.as_ref().is_some_and(holds_full_join) {
                        // DECISION(spelling): a bound over joined rows that
                        // hold a full join cuts them as one stage. A target
                        // that spells the full join as a compound would
                        // otherwise carry the row clause into each arm.
                        rows = Rows::of(self.stage(rows, &[], outer)?);
                    }
                    let frame = self.bounded(rows, ordering, &bound)?;
                    rows = Rows::of(frame);
                }
                Some(Boundary::Filter(g)) => {
                    // A filter that does not commute with a later join
                    // consumes the run so far: applied here, after the
                    // filters written before it, and closed as one stage the
                    // later members join onto. With no later join it stays
                    // in written order among the filters after it.
                    filters.extend(placed.filter(g));
                    if joins_after[i] {
                        rows = self.filtered(rows, &filters, outer)?;
                        filters.clear();
                        let frame = self.stage(rows, &[], outer)?;
                        rows = Rows::of(frame);
                    }
                }
                None => {}
            }
        }
        Ok((rows, filters))
    }

    /// Which of a run's qualifiers are boundaries of its population, as its
    /// run decided: a bound, and each guard its run made one.
    pub(super) fn boundaries(&self, q: &Quals<'_>, items: &[Indexed]) -> Vec<bool> {
        items
            .iter()
            .map(|item| match item {
                Indexed::Member(_) => false,
                Indexed::Bound(_) => true,
                Indexed::Guard(g) => q.guards[*g].boundary(),
            })
            .collect()
    }

    /// A run of one member whose relation is a row-wise stage or a run,
    /// written in place: the member's positions are its relation's values
    /// over that relation's own rows, so a recursive step reads its working
    /// table at its own level. Only values that read, compare and compute arithmetic are
    /// written more than once; anything else keeps its stage.
    fn inlined(&mut self, q: &Quals<'_>, placed: &mut Placed, outer: &Enclosing, capped: bool) -> Result<Option<Rows>> {
        let graph = self.graph;
        let [m] = q.members.as_slice() else {
            return Ok(None);
        };
        if !capped && q.run.quals().iter().any(|x| matches!(x, Qual::Bound(_))) {
            return Ok(None);
        }
        let inlinable = match graph.rel(m.rel()).kind() {
            RelKind::Pipe { op, .. } => match op {
                crate::pipeline::middle::core::node::PipeOp::Project(items)
                | crate::pipeline::middle::core::node::PipeOp::Embed(items) => {
                    items.iter().all(|i| simple(graph, i.expr))
                }
                crate::pipeline::middle::core::node::PipeOp::ProjectOut(_) => true,
                _ => false,
            },
            // A run's rows are already written at their own level.
            RelKind::Run(_) => true,
            _ => false,
        };
        if !inlinable {
            return Ok(None);
        }
        let (mut rows, values, group, distinct) = self.part(m.rel(), outer)?;
        if group.is_some() || distinct {
            return Ok(None);
        }
        // DECISION(recursive-part): inside a recursive part a one-member run
        // of plain values is written in place, at the working table's level,
        // instead of as a stage above it.
        let b = m.binder();
        for (i, p) in graph.binder(b).heading().positions().iter().enumerate() {
            let v = values.get(i).cloned().ok_or_else(|| self.unsupplied("an inlined position"))?;
            rows.frame.env.bind(Key::Col(b, i as u16), v.clone());
            if let Visibility::Hidden(passenger) = p.visibility {
                rows.frame.env.bind(Key::Passenger(passenger), v);
            }
        }
        // The one member's match conditions (it joins nothing) and the
        // run's filters, in written order.
        let own: Vec<TruthId> = placed
            .take(0)
            .into_iter()
            .map(|c| match c {
                Condition::Guard(t) => Ok(t),
                Condition::Merge(_) | Condition::Lifted(_) => {
                    Err(self.uncovered("a merge or a slot constraint on a run's one member"))
                }
            })
            .collect::<Result<_>>()?;
        let mut filters = Vec::new();
        for (g, guard) in q.guards.iter().enumerate() {
            match placed.filter(g) {
                Some(t) => filters.push(t),
                None if own.contains(&guard.truth()) => filters.push(guard.truth()),
                None => {}
            }
        }
        Ok(Some(self.filtered(rows, &filters, outer)?))
    }

    /// Join member `j` onto the rows under every condition the core placed
    /// on its join, taken from the run's ledger: its match conditions, which
    /// the member's own realization applies before any padding. A relation
    /// whose rows exist per row of the rows it joins (a population the core
    /// stores as such, or one reading the carrier's binders) is realized
    /// over them and ends its run with them (R1–R6); any other is joined on
    /// them (B3). On both halves the member binds the merged keys it
    /// completes (`bind_merges` over the members joined with it) once its
    /// columns are bound and before any of its conditions is applied, since
    /// a condition on its join may read them.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn member(
        &mut self,
        rows: Rows,
        q: &Quals<'_>,
        placed: &mut Placed,
        j: usize,
        join: JoinType,
        carrier: &BTreeSet<BinderId>,
        outer: &Enclosing,
        ordering: &mut Vec<(Key, SqlDirection)>,
        present: &[usize],
    ) -> Result<Rows> {
        let m = q.members[j];
        let dependence = m.dependence().map(|(d, _)| d);
        // An expansion is realized over the rows to its left whether or not
        // its value reads them: its rows are the same for every such row,
        // and its binds' nodes travel only on that road.
        let expansion = matches!(self.graph.rel(m.rel()).kind(), RelKind::Unnest { .. });
        let dependent = expansion
            || match dependence {
                Some(Dependence::Population) => true,
                Some(Dependence::Constraint) | None => super::dependent::reads(self.graph, m.rel(), carrier),
            };
        let mut joined: Vec<usize> = present.to_vec();
        joined.push(j);
        if !dependent {
            return self.join_member(rows, q, placed, j, join, outer, ordering, &joined);
        }
        // DECISION(lateral): a table function reads the rows beside it
        // through its arguments, so it joins after them as itself.
        if self.is_function(m.rel()) && !present.is_empty() {
            return self.function_member(rows, q, placed, j, join, outer, &joined);
        }
        let rows = match (present.is_empty(), expansion) {
            (false, _) => rows,
            // The expansion opens its run: its carrier is the one row with
            // no column.
            (true, true) if rows.frame.from.is_none() => self.one_row(rows)?,
            (true, _) => return Err(self.uncovered("a run whose first member reads its carrier")),
        };
        let marked = match join {
            JoinType::Inner => false,
            JoinType::Left => true,
            JoinType::Right | JoinType::Full | JoinType::Cross => {
                return Err(self.uncovered("a dependent member preserved by its join"))
            }
        };
        let conditions = placed.take(j);
        self.dependent_member(rows, m, marked, conditions, (q, &joined), outer)
    }

    /// Whether member `j` is realized over the rows joined before it: an
    /// expansion, or a relation whose rows exist per row of the members it
    /// reads (`member`'s own judgment of the same stored facts).
    fn realized_over(&self, q: &Quals<'_>, j: usize) -> bool {
        let m = q.members[j];
        matches!(self.graph.rel(m.rel()).kind(), RelKind::Unnest { .. })
            || matches!(m.dependence(), Some((Dependence::Population, _)))
    }

    /// The one row with no column, as a FROM item: the carrier of a member
    /// that is realized over the rows to its left and stands first.
    fn one_row(&mut self, rows: Rows) -> Result<Rows> {
        let at = self.out.scope(None);
        let item = SelectItem::scaffolding_value(SqlExpr::Literal(super::value::integer(1)), self.out.scaffolding());
        let query = self.select_at(at, vec![item], Vec::new(), Vec::new(), None, Vec::new(), None, false)?;
        Ok(Rows {
            frame: Frame {
                from: Some(TableExpression::subquery(query, at)),
                env: rows.frame.env,
            },
            filter: rows.filter,
        })
    }

    /// The order the members of one segment join in, with each one's join.
    pub(super) fn join_order(&self, run: &Run, seg: &[usize], present: &[usize]) -> Result<Vec<(usize, JoinType)>> {
        let roles: Vec<&JoinRole> = run.members().map(|m| m.role()).collect();
        let mut order = Vec::new();
        if roles.iter().any(|r| matches!(r, JoinRole::Folded(_))) {
            for j in seg {
                let join = match roles[*j] {
                    JoinRole::Folded(FoldJoin::Start | FoldJoin::Inner) => JoinType::Inner,
                    JoinRole::Folded(FoldJoin::Left) => JoinType::Left,
                    JoinRole::Folded(FoldJoin::Right) => JoinType::Right,
                    JoinRole::Folded(FoldJoin::Full) => JoinType::Full,
                    JoinRole::Required | JoinRole::Attached { .. } | JoinRole::Full => {
                        return Err(self.uncovered("a fold with a member outside the fold"))
                    }
                };
                order.push((*j, join));
            }
            return Ok(order);
        }
        let mut joined: Vec<usize> = present.to_vec();
        for j in seg {
            match roles[*j] {
                JoinRole::Required => order.push((*j, JoinType::Inner)),
                JoinRole::Full => order.push((*j, JoinType::Full)),
                JoinRole::Attached { .. } | JoinRole::Folded(_) => continue,
            }
            joined.push(*j);
        }
        let mut waiting: Vec<usize> = seg
            .iter()
            .copied()
            .filter(|j| matches!(roles[*j], JoinRole::Attached { .. }))
            .collect();
        while !waiting.is_empty() {
            let ready = waiting.iter().position(|j| match roles[*j] {
                JoinRole::Attached { onto } => onto.iter().all(|o| joined.contains(o)),
                _ => true,
            });
            let Some(k) = ready else {
                return Err(self.uncovered(
                    "a marked member attached onto a member that never joins before it (marked members attached onto each other, or onto a member after a dependent run's source)",
                ));
            };
            let j = waiting.remove(k);
            order.push((j, JoinType::Left));
            joined.push(j);
        }
        Ok(order)
    }

    /// Join one member whose relation reads nothing to its left onto the
    /// rows (B3), on its match conditions: the `ON` of its join, which an
    /// outer join evaluates before it pads.
    #[allow(clippy::too_many_arguments)]
    fn join_member(
        &mut self,
        rows: Rows,
        q: &Quals<'_>,
        placed: &mut Placed,
        j: usize,
        join: JoinType,
        outer: &Enclosing,
        ordering: &mut Vec<(Key, SqlDirection)>,
        joined: &[usize],
    ) -> Result<Rows> {
        let graph = self.graph;
        let m = q.members[j];
        let dependence = m.dependence();
        let lift = matches!(dependence, Some((Dependence::Constraint, _)));
        let (table, lifts) = self.member_table(m.rel(), outer, lift)?;
        if let Some((_, placement)) = dependence {
            let at = match placement {
                Placement::On { member } => member,
                Placement::Filter => j,
            };
            for l in lifts {
                placed.lift(at, l);
            }
        }
        let b = m.binder();
        let Rows { frame, filter } = rows;
        let mut env = frame.env;
        for (i, p) in graph.binder(b).heading().positions().iter().enumerate() {
            env.bind(Key::Col(b, i as u16), SqlExpr::Column(table.cols[i]));
            if let Visibility::Hidden(passenger) = p.visibility {
                env.bind(Key::Passenger(passenger), SqlExpr::Column(table.cols[i]));
            }
        }
        if j == 0 {
            ordering.extend(self.bind_order(&mut env, b, &table));
        }
        self.bind_merges(&mut env, q, joined, outer)?;
        let mut conds = Vec::new();
        for c in placed.take(j) {
            conds.push(self.match_condition(c, &env, outer)?);
        }
        let from = match frame.from {
            None => {
                if !conds.is_empty() {
                    return Err(self.uncovered("a condition on a run's first join"));
                }
                table.from
            }
            Some(left) => {
                TableExpression::Join {
                    left: Box::new(left),
                    right: Box::new(table.from),
                    join_type: join.clone(),
                    join_condition: self.condition(conds, &join),
                }
            }
        };
        Ok(Rows {
            frame: Frame {
                from: Some(from),
                env,
            },
            filter,
        })
    }

    /// One match condition, as SQL over the rows its member joins.
    pub(super) fn match_condition(&mut self, c: Condition, env: &Env, outer: &Enclosing) -> Result<SqlExpr> {
        match c {
            Condition::Guard(t) => self.truth(t, Consumer::Filter, At { local: env, outer }),
            Condition::Merge(mid) => self.merge_condition(mid, env, outer),
            Condition::Lifted(l) => {
                let v = self.value(l.value, At { local: env, outer })?;
                Ok(self.compared_reaffined((l.lost, SqlExpr::Column(l.col)), (self.unaffined(l.value), v), |c, v| {
                    equality(c, v, l.class)
                }))
            }
        }
    }

    /// A join's condition from the relations placed on it.
    pub(super) fn condition(&mut self, conds: Vec<SqlExpr>, join: &JoinType) -> JoinCondition {
        if !conds.is_empty() {
            return JoinCondition::On(SqlExpr::and(conds));
        }
        match join {
            JoinType::Inner | JoinType::Cross => JoinCondition::Cartesian,
            JoinType::Left | JoinType::Right | JoinType::Full => {
                // DECISION(spelling): an outer join nothing relates
                // matches every pair.
                JoinCondition::On(always())
            }
        }
    }

    /// Bind each merged key whose operands are present, in the order the
    /// run made them, by its stored rule; the keys it bound.
    pub(super) fn bind_merges(&mut self, env: &mut Env, q: &Quals<'_>, present: &[usize], outer: &Enclosing) -> Result<Vec<Key>> {
        let graph = self.graph;
        let mut bound = Vec::new();
        for (mid, rule, _) in q.run.merges() {
            if env.get(Key::Merged(*mid)).is_some() {
                continue;
            }
            let site = graph.merge(*mid);
            let right_member = q.members.iter().position(|m| m.binder() == site.right().0);
            if !right_member.is_some_and(|r| present.contains(&r)) {
                continue;
            }
            let at = At { local: env, outer };
            // The left operand is present only once its member has joined.
            let Some(left) = super::rel::cell_at(site.left(), at) else {
                continue;
            };
            let right = at
                .get(Key::Col(site.right().0, site.right().1))
                .ok_or_else(|| self.unsupplied("a merge's right operand"))?;
            let value = match rule {
                MergeRule::Agreed => {
                    // DECISION(spelling): a merged key whose operands agree
                    // on every row reads its left operand.
                    left
                }
                MergeRule::Left => left,
                MergeRule::Right => right,
                MergeRule::Coalesced => self.out.function("coalesce", vec![left, right]),
            };
            env.bind(Key::Merged(*mid), value);
            bound.push(Key::Merged(*mid));
        }
        Ok(bound)
    }

    /// A merge's correspondence comparison: its left operand's value
    /// against its right member's column.
    pub(super) fn merge_condition(&mut self, mid: MergeId, env: &Env, outer: &Enclosing) -> Result<SqlExpr> {
        let site = self.graph.merge(mid);
        let at = At { local: env, outer };
        let left = self.cell(site.left(), at)?;
        let right = at
            .get(Key::Col(site.right().0, site.right().1))
            .ok_or_else(|| self.unsupplied("a merge's right operand"))?;
        let right_cell = crate::pipeline::middle::core::node::Cell::Col(site.right().0, site.right().1);
        Ok(self.compared_reaffined(
            (self.unaffined_cell(site.left()), left),
            (self.unaffined_cell(right_cell), right),
            |l, r| equality(l, r, crate::pipeline::middle::core::node::EqClass::Correspondence),
        ))
    }

    /// B7 over a run so far: its rows, ordered by the ordering in force,
    /// cut to the bound, as one stage.
    pub(super) fn bounded(&mut self, rows: Rows, ordering: &[(Key, SqlDirection)], bound: &Bound) -> Result<Frame> {
        // DECISION(bound): a bound in a run no dependent member owns is a
        // row clause on the stage that orders.
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut env = rows.frame.env.next();
        let mut order_by = Vec::new();
        for (key, dir) in ordering {
            let value = rows
                .frame
                .env
                .get(*key)
                .cloned()
                .ok_or_else(|| self.unsupplied("an ordering key"))?;
            order_by.push(OrderTerm::new(value, Some(dir.clone())));
        }
        for (key, value) in rows.frame.env.entries() {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(value.clone(), col));
            env.bind(*key, SqlExpr::Column(col));
        }
        let query = self.select_at(
            at,
            items,
            rows.frame.from.into_iter().collect(),
            rows.filter,
            None,
            order_by,
            Some(limit_of(bound)),
            false,
        )?;
        Ok(Frame {
            from: Some(TableExpression::subquery(query, at)),
            env,
        })
    }
}

/// Whether a FROM entry's own join tree holds a full join.
fn holds_full_join(from: &TableExpression) -> bool {
    match from {
        TableExpression::Join { left, right, join_type, .. } => {
            *join_type == JoinType::Full || holds_full_join(left) || holds_full_join(right)
        }
        _ => false,
    }
}

/// A value that reads, compares and computes arithmetic only: writing it
/// twice evaluates the same thing twice.
pub(super) fn simple(graph: &crate::pipeline::middle::core::graph::Graph, e: crate::pipeline::middle::core::ids::ExprId) -> bool {
    use crate::pipeline::middle::core::node::ExprKind;
    match graph.expr(e).kind() {
        ExprKind::Col(..) | ExprKind::Merged(_) | ExprKind::Const(_) | ExprKind::Passenger(_) => true,
        ExprKind::Infix(_, l, r) => simple(graph, *l) && simple(graph, *r),
        _ => false,
    }
}

/// A condition every row satisfies.
pub(super) fn always() -> SqlExpr {
    SqlExpr::Binary {
        left: Box::new(SqlExpr::Literal(super::value::integer(1))),
        op: crate::pipeline::middle::facade::BinaryOperator::Equal,
        right: Box::new(SqlExpr::Literal(super::value::integer(1))),
    }
}
