// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! R1–R6: a member whose relation reads the rows to its left (its carrier)
//! is realized over them. Its source and join prefix become one relation
//! joined onto the carrier on the carrier-reading conditions (R1); every
//! step after that runs over the joined rows, a population-sensitive step
//! partitioned by the member's witness (R2, R3); a nested dependent member
//! takes the current rows as its carrier (R5); a marked member whose tail is
//! row-wise pads by a left join (R4a). Carrier columns are never rewritten;
//! a member's private columns end with it.

use super::frame::{Enclosing, Env, Key, Private};
use super::run::{Condition, Indexed, Placed, Quals, Rows};
use super::value::{integer, At, Consumer};
use super::{Realizer, Result};
use crate::pipeline::middle::core::graph::{Arena, Graph};
use crate::pipeline::middle::core::heading::Visibility;
use crate::pipeline::middle::core::ids::{BinderId, ExprId, RelId};
use crate::pipeline::middle::core::node::{
    Bound, Dependence, ExprKind, Member, PipeOp, Placement, ReadAccess,
    ReadSource, RelKind, Run, Slot,
};
use crate::pipeline::middle::facade::{
    BinaryOperator, JoinCondition, JoinType, OrderTerm, SelectItem, SqlDirection, SqlExpr,
    TableExpression,
};
use std::collections::BTreeSet;

/// The member a dependent run's steps are owned by.
pub(super) struct Owner {
    binder: BinderId,
    /// The keys the carrier offered when the member joined.
    carrier: Vec<Key>,
    witness: Option<Key>,
    /// Present exactly on the rows the member's join matched, when the
    /// member pads by a left join.
    matched: Option<Key>,
    marked: bool,
    /// KEEP mode: the member's rows that fail a step stay as carriers
    /// under its flag.
    keep: bool,
}

/// A condition of the join stage, translated once the source is joined.
enum Theta {
    Truth(crate::pipeline::middle::core::ids::TruthId),
    Slot {
        key: Key,
        value: ExprId,
        class: crate::pipeline::middle::core::node::EqClass,
    },
}

/// A step a dependent member owns.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Step {
    Yield { windows: bool },
    Group { keyless: bool },
    Distinct,
    Rank,
    /// A restriction; population-sensitive when its truth reads a window or
    /// an aggregate.
    Guard { sensitive: bool },
    Join,
    Nested,
    /// A recursion that deduplicates: its duplicate test needs the witness.
    SetRecursion,
    Other,
}

impl Step {
    fn sensitive(self) -> bool {
        matches!(
            self,
            Step::Yield { windows: true }
                | Step::Group { .. }
                | Step::Distinct
                | Step::Rank
                | Step::SetRecursion
                | Step::Guard { sensitive: true }
        )
    }
}

/// A dependent run's qualifiers in W3's segments over its carrier: the
/// source (the qualifiers before `tail` that read neither the carrier nor
/// the run, realized once as the run's own population), θ (the row-wise
/// filters before `tail` reading the carrier, the join stage's condition),
/// and the tail, from `tail` on in the run's qualifiers. No source: the
/// first member reads the carrier, and every qualifier after it is the
/// tail. One scan decides the segments for the realization and for the step
/// classification alike.
struct Segments {
    source: Option<Source>,
    tail: usize,
}

struct Source {
    members: Vec<usize>,
    /// θ, by qualifier position.
    theta: Vec<usize>,
}

impl Realizer<'_, '_> {
    /// Join a member whose relation reads the rows to its left: realized
    /// over them, its own columns then bound to its binder. The conditions
    /// the core placed on its join are the last steps of its run: applied in
    /// its mode, before it pads.
    pub(super) fn dependent_member(
        &mut self,
        rows: Rows,
        m: &Member,
        marked: bool,
        conditions: Vec<Condition>,
        (q, joined): (&Quals<'_>, &[usize]),
        outer: &Enclosing,
    ) -> Result<Rows> {
        // DECISION(dependent): a member reading its carrier is realized
        // over it, its correlation a join.
        let graph = self.graph;
        let carrier = binders(&rows.frame.env);
        let mut steps = Vec::new();
        self.steps(m.rel(), &carrier, &mut steps)?;
        // The core admitted every condition placed on a join: none reads a
        // value evaluated over a population.
        steps.extend(conditions.iter().map(|_| Step::Guard { sensitive: false }));
        let sensitive = steps.iter().any(|s| s.sensitive());
        let keyless = steps.iter().any(|s| matches!(s, Step::Group { keyless: true }));
        let row_wise = steps.iter().all(|s| matches!(s, Step::Yield { windows: false }));
        // DECISION(mode): a member owning a keyless reduction, or a marked
        // member whose tail is not row-wise, keeps its carriers under its
        // flag; otherwise nothing after the join observes an empty
        // population, so rows failing a step are removed.
        // DECISION(outer-mark): a marked member whose tail is row-wise is
        // padded by one left join; one whose tail is not row-wise is
        // collapsed per occurrence after its KEEP run.
        let keep = keyless || (marked && !row_wise);
        let b = m.binder();
        let mut owner = Owner {
            binder: b,
            carrier: rows.frame.env.entries().iter().map(|(k, _)| *k).collect(),
            witness: None,
            matched: None,
            marked,
            keep,
        };
        let mut rows = rows;
        rows.frame.env.enter(b);
        let rows = if sensitive || keep {
            self.witness(rows, &mut owner, outer)?
        } else {
            rows
        };
        let (mut rows, _) = self.over(rows, m.rel(), &mut owner, outer)?;
        let heading = graph.binder(b).heading().clone();
        let mut published: Vec<Key> = Vec::new();
        for (i, p) in heading.positions().iter().enumerate() {
            let value = rows
                .frame
                .env
                .get(Key::Pos(m.rel(), i as u16))
                .cloned()
                .ok_or_else(|| self.unsupplied("a dependent member's position"))?;
            rows.frame.env.bind(Key::Col(b, i as u16), value.clone());
            published.push(Key::Col(b, i as u16));
            if let Visibility::Hidden(passenger) = p.visibility {
                rows.frame.env.bind(Key::Passenger(passenger), value);
                published.push(Key::Passenger(passenger));
            }
            // A position an expansion bound keeps its node beside it.
            if let Some(node) = rows.frame.env.get(Key::BindNode(m.rel(), i as u16)).cloned() {
                rows.frame.env.bind(Key::ColNode(b, i as u16), node);
                published.push(Key::ColNode(b, i as u16));
            }
        }
        // The member's columns stand for its relation's positions from here.
        let rel = m.rel();
        rows.frame.env.retain(|k| !matches!(k, Key::Pos(r, _) if *r == rel));
        let completed = self.bind_merges(&mut rows.frame.env, q, joined, outer)?;
        for c in conditions {
            rows = self.discharge(rows, c, &owner, outer)?;
        }
        if keep {
            let flag = self.flag(&rows, &owner)?;
            if marked {
                rows = self.collapse(rows, &published, &owner, outer)?;
            } else {
                rows.filter.push(is_one(flag));
            }
        }
        // R4a pads with NULL: on a row the member's join did not match, every
        // position the member publishes is NULL, including one its tail
        // computed from the enclosing row.
        if let (true, Some(k)) = (marked && !keep, owner.matched) {
            let mu = rows.frame.env.get(k).cloned().ok_or_else(|| self.unsupplied("a match marker"))?;
            for key in &published {
                let value = rows.frame.env.get(*key).cloned().ok_or_else(|| self.unsupplied("a published position"))?;
                rows.frame.env.bind(
                    *key,
                    SqlExpr::Case {
                        expr: None,
                        when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(
                            SqlExpr::Binary {
                                left: Box::new(mu.clone()),
                                op: BinaryOperator::IsNot,
                                right: Box::new(SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::Null)),
                            },
                            value,
                        )],
                        else_clause: None,
                    },
                );
            }
        }
        // A merged key is a value of its operands: where padding or a
        // collapse rebound the member's columns, the keys it completes are
        // bound again over them.
        if marked && !completed.is_empty() {
            rows.frame.env.retain(|k| !completed.contains(k));
            self.bind_merges(&mut rows.frame.env, q, joined, outer)?;
        }
        let mut kept: Vec<Key> = owner.carrier.clone();
        kept.extend(published);
        kept.extend(completed);
        rows.frame.env.retain(|k| kept.contains(k));
        if !rows.frame.env.leave(b) {
            return Err(self.contract("a member completing leaves a population that is not the innermost"));
        }
        Ok(rows)
    }

    /// Apply one of a member's match conditions, or a restriction of its
    /// run, to its rows in the owner's mode: FILTER removes a failing row,
    /// KEEP turns it into a carrier. A value the restriction reads that no
    /// filtering clause may hold is computed in a stage below it.
    fn discharge(&mut self, rows: Rows, c: Condition, owner: &Owner, outer: &Enclosing) -> Result<Rows> {
        let mut rows = rows;
        match c {
            Condition::Guard(t) => {
                if !owner.keep {
                    return self.filtered(rows, &[t], outer);
                }
                rows = self.staged_for(rows, super::rel::Step::Guard(t), outer)?;
                let env = rows.frame.env.clone();
                let c = self.truth(t, Consumer::Filter, At { local: &env, outer })?;
                self.restrict(&mut rows, c, owner)?;
            }
            Condition::Merge(mid) => {
                let env = rows.frame.env.clone();
                let c = self.merge_condition(mid, &env, outer)?;
                self.restrict(&mut rows, c, owner)?;
            }
            Condition::Lifted(_) => {
                return Err(self.uncovered("a slot constraint placed on a dependent member's join"));
            }
        }
        Ok(rows)
    }

    /// Realize `rel` over the carrier `rows`: each carrier row with the
    /// rows `rel` has for it. Every position of `rel` is bound as
    /// `Key::Pos`; the ordering the relation hands its run is returned.
    fn over(&mut self, rows: Rows, rel: RelId, owner: &mut Owner, outer: &Enclosing) -> Result<(Rows, Vec<(Key, SqlDirection)>)> {
        let graph = self.graph;
        let carrier = binders(&rows.frame.env);
        // An expansion is written over its carrier whether or not its value
        // reads it (the run realizes it so, `Realizer::member`).
        if !reads(graph, rel, &carrier) && !matches!(graph.rel(rel).kind(), RelKind::Unnest { .. }) {
            let (rows, ordering) = self.source(rows, rel, owner, outer)?;
            return Ok((rows, ordering));
        }
        match graph.rel(rel).kind() {
            RelKind::Run(run) => self.over_run(rows, rel, run, owner, outer),
            RelKind::Pipe { input, op } => {
                let (rows, _) = self.over(rows, *input, owner, outer)?;
                Ok((self.over_pipe(rows, rel, *input, op, owner, outer)?, Vec::new()))
            }
            RelKind::Order { input, keys, bound } => {
                let (rows, _) = self.over(rows, *input, owner, outer)?;
                let rows = self.staged_for(rows, super::rel::Step::Order(keys), outer)?;
                let Some(b) = bound else {
                    // ORDERING IS ADMITTED; PRESERVATION IS NOT IMPLIED. Over
                    // a carrier nothing presents an ordering, so one with no
                    // bound of its own is lowered in its stage and hands its
                    // run no keys: a later bound never ranks by it.
                    let env = rows.frame.env.clone();
                    let order_by = keys
                        .iter()
                        .map(|k| {
                            let v = self.value(k.expr, At { local: &env, outer })?;
                            Ok(OrderTerm::new(v, Some(super::value::direction(k.direction))))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let frame = self.stage_in_order(rows, Vec::new(), order_by)?;
                    let mut rows = Rows::of(frame);
                    self.alias_positions(&mut rows, rel, *input)?;
                    return Ok((rows, Vec::new()));
                };
                // The rank reads every key as a column of the stage below it.
                let key_exprs: Vec<ExprId> = keys.iter().map(|k| k.expr).collect();
                let frame = self.stage(rows, &key_exprs, outer)?;
                let mut rows = Rows::of(frame);
                self.alias_positions(&mut rows, rel, *input)?;
                let ordering: Vec<(Key, SqlDirection)> = keys
                    .iter()
                    .map(|k| (Key::Value(k.expr), super::value::direction(k.direction)))
                    .collect();
                Ok((self.rank(rows, &ordering, b, owner, outer)?, Vec::new()))
            }
            RelKind::Apply { instance } => {
                let body = graph.instance(*instance).body();
                let (mut rows, ordering) = self.over(rows, body, owner, outer)?;
                self.alias_positions(&mut rows, rel, body)?;
                Ok((rows, ordering))
            }
            RelKind::Read {
                source: ReadSource::Local(body),
                access,
            } => {
                let (mut rows, ordering) = self.over(rows, *body, owner, outer)?;
                self.over_access(&mut rows, rel, *body, access, owner, outer)?;
                Ok((rows, ordering))
            }
            RelKind::Read {
                source: ReadSource::Catalog { .. },
                ..
            } => {
                let (table, lifted) = self.member_table(rel, outer, true)?;
                let mut env = Env::fresh();
                for (i, c) in table.cols.iter().enumerate() {
                    env.bind(Key::Pos(rel, i as u16), SqlExpr::Column(*c));
                }
                let mut theta = Vec::new();
                for (k, l) in lifted.into_iter().enumerate() {
                    let key = Key::Lifted(k as u16);
                    env.bind(key, SqlExpr::Column(l.col));
                    theta.push(Theta::Slot {
                        key,
                        value: l.value,
                        class: l.class,
                    });
                }
                let frame = super::frame::Frame {
                    from: Some(table.from),
                    env,
                };
                let mut rows = self.join_frame(rows, frame, theta, owner, outer)?;
                rows.frame.env.retain(|k| !matches!(k, Key::Lifted(_)));
                Ok((rows, Vec::new()))
            }
            // A table function reads its carrier row through its arguments:
            // joined after the carrier as itself.
            RelKind::Read {
                source: ReadSource::Function { name, args, columns, .. },
                access,
            } => {
                if owner.marked || owner.keep {
                    return Err(self.uncovered("a table function its carrier pads"));
                }
                let (rows, function, cols) = self.function_beside(rows, name, args, columns, outer)?;
                let Rows { frame, filter } = rows;
                let mut env = frame.env;
                let (bound, required) = self.function_access(rel, &cols, access, &env, outer)?;
                for (i, value) in bound.into_iter().enumerate() {
                    env.bind(Key::Pos(rel, i as u16), value);
                }
                let from = match frame.from {
                    Some(left) => TableExpression::Join {
                        left: Box::new(left),
                        right: Box::new(function),
                        join_type: JoinType::Inner,
                        join_condition: crate::pipeline::middle::facade::JoinCondition::Cartesian,
                    },
                    None => function,
                };
                let mut rows = Rows {
                    frame: super::frame::Frame { from: Some(from), env },
                    filter,
                };
                for condition in required {
                    self.restrict(&mut rows, condition, owner)?;
                }
                Ok((rows, Vec::new()))
            }
            RelKind::Lit { header, rows: cells, .. } => {
                Ok((self.over_lit(rows, rel, header, cells, owner, outer)?, Vec::new()))
            }
            RelKind::Fix(fix) => Ok((self.over_fix(rows, rel, fix, owner, outer)?, Vec::new())),
            RelKind::Family { clauses, .. } => Ok((self.over_family(rows, rel, clauses, owner, outer)?, Vec::new())),
            RelKind::Unnest { value, expansion } => {
                Ok((self.over_unnest(rows, rel, *value, expansion, owner, outer)?, Vec::new()))
            }
            RelKind::Read { .. }
            | RelKind::Receipt { .. }
            | RelKind::SetOp { .. }
            | RelKind::Minus { .. }
            | RelKind::Meta { .. }
        | RelKind::Witnessed { .. } => Err(self.uncovered("this relation over a carrier")),
        }
    }

    /// R1 over a run: its source joined onto the carrier on the
    /// carrier-reading conditions (θ); then its tail. Source and tail are
    /// parts of the run's one population (`population`), each realized in
    /// the order and by the joins it gives.
    fn over_run(
        &mut self,
        rows: Rows,
        r: RelId,
        run: &Run,
        owner: &mut Owner,
        outer: &Enclosing,
    ) -> Result<(Rows, Vec<(Key, SqlDirection)>)> {
        let q = Quals::of(run);
        let carrier = binders(&rows.frame.env);
        let items = q.indexed();
        let boundaries = self.boundaries(&q, &items);
        let segments = self.segments(&q, &items, &carrier)?;
        let mut placed = Placed::of(self, &q)?;
        let first = q.members[0];
        let mut ordering: Vec<(Key, SqlDirection)> = Vec::new();
        let mut present: Vec<usize> = Vec::new();
        let mut rows = rows;
        match &segments.source {
            None => {
                // The first member is realized over the carrier before any
                // other joins: its population must join it first.
                let whole = self.run_population(run, &items, &boundaries, |_| true, &[])?;
                if whole.iter().flat_map(|s| s.joins.first()).next().map(|(j, _)| *j) != Some(0) {
                    return Err(self.uncovered("a run whose first member reads its carrier and joins after a later member"));
                }
                if !placed.take(0).is_empty() {
                    return Err(self.uncovered("a condition placed on the join of a first member that reads its carrier"));
                }
                let (r2, ord) = self.over(rows, first.rel(), owner, outer)?;
                rows = r2;
                ordering = ord;
                self.bind_member(&mut rows, first, first.rel())?;
                present.push(0);
            }
            Some(source) => {
                // θ is applied by the join stage, after the whole source: it
                // must commute with every source join after it.
                for at in &source.theta {
                    let keeps = items[*at + 1..segments.tail].iter().any(|item| match item {
                        Indexed::Member(j) => q.members[*j].role().keeps_member(),
                        Indexed::Guard(_) | Indexed::Bound(_) => false,
                    });
                    if keeps {
                        return Err(self.uncovered(
                            "a condition reading the carrier before a join that keeps a later member's rows",
                        ));
                    }
                }
                // The source: its population as one relation, joined on θ.
                let tail = segments.tail;
                let within = |at: usize| at < tail && !source.theta.contains(&at);
                let population = self.run_population(run, &items, &boundaries, within, &[])?;
                let (p, filters) = self.realize_population(Rows::of(super::frame::Frame::unit()), &q, &mut placed, population, &mut ordering, outer)?;
                let p = self.filtered(p, &filters, outer)?;
                let frame = self.stage(p, &[], outer)?;
                let theta = source
                    .theta
                    .iter()
                    .filter_map(|at| match items[*at] {
                        Indexed::Guard(g) => placed.filter(g),
                        Indexed::Member(_) | Indexed::Bound(_) => None,
                    })
                    .map(Theta::Truth)
                    .collect();
                rows = self.join_frame(rows, frame, theta, owner, outer)?;
                present = source.members.clone();
            }
        }
        let tail = self.run_population(run, &items, &boundaries, |at| at >= segments.tail, &present)?;
        for seg in tail {
            for (j, join) in seg.joins {
                if owner.keep {
                    return Err(self.uncovered("a member after a KEEP member's join"));
                }
                if matches!(join, JoinType::Right | JoinType::Full) {
                    return Err(self.uncovered("a join keeping a member's rows over a carrier"));
                }
                rows = self.member(rows, &q, &mut placed, j, join, &carrier, outer, &mut ordering, &present)?;
                present.push(j);
            }
            // A condition the core placed on a join is its member's.
            for g in seg.guards {
                if let Some(t) = placed.filter(g) {
                    rows = self.discharge(rows, Condition::Guard(t), owner, outer)?;
                }
            }
            match seg.boundary {
                Some(super::run::Boundary::Filter(g)) => {
                    if let Some(t) = placed.filter(g) {
                        rows = self.discharge(rows, Condition::Guard(t), owner, outer)?;
                    }
                }
                Some(super::run::Boundary::Bound(b)) => {
                    rows = self.rank(rows, &ordering, &b, owner, outer)?;
                }
                None => {}
            }
        }
        placed.close(self)?;
        for (i, cell) in run.outputs().iter().enumerate() {
            let v = self.cell(*cell, At { local: &rows.frame.env, outer })?;
            rows.frame.env.bind(Key::Pos(r, i as u16), v);
        }
        Ok((rows, ordering))
    }

    /// W3's segments of a dependent run over its carrier, decided once for
    /// its realization and its step classification. After the first member,
    /// the source takes members and filters until a qualifier reads the
    /// carrier or the run so far: a member whose relation or match
    /// conditions read the carrier or an earlier member of the run, a bound,
    /// or a population-sensitive filter (one reading a window or an
    /// aggregate, which acts on the population in authored order and so
    /// cannot move before the correlation) begins the tail. A row-wise filter
    /// reading the carrier is θ; any other filter stays in the source's
    /// population where it is written. A population-sensitive filter begins
    /// the tail wherever it stands, so it is applied at its written position,
    /// per carrier row: before the run reads the carrier its population is
    /// the source's rows, the same for every carrier row
    /// (predicate-placement-law: population evaluation).
    fn segments(&self, q: &Quals<'_>, items: &[Indexed], carrier: &BTreeSet<BinderId>) -> Result<Segments> {
        let graph = self.graph;
        let first = q.members[0];
        if reads(graph, first.rel(), carrier) {
            return Ok(Segments { source: None, tail: 1 });
        }
        let reads_carrier = |t: crate::pipeline::middle::core::ids::TruthId| {
            graph.truth(t).fv().iter().any(|b| carrier.contains(b))
        };
        let mut source = Source {
            members: vec![0],
            theta: Vec::new(),
        };
        let mut run_binders: BTreeSet<BinderId> = BTreeSet::new();
        run_binders.insert(first.binder());
        let mut tail = items.len();
        for (at, item) in items.iter().enumerate().skip(1) {
            match item {
                Indexed::Guard(g) => {
                    let t = q.guards[*g].truth();
                    match q.guards[*g].placement() {
                        Placement::On { .. } => {}
                        Placement::Filter if self.sensitive_truth(t) => {
                            tail = at;
                            break;
                        }
                        Placement::Filter if reads_carrier(t) => source.theta.push(at),
                        Placement::Filter => {}
                    }
                }
                Indexed::Member(j) => {
                    let m = q.members[*j];
                    let mut read: BTreeSet<BinderId> = carrier.clone();
                    read.extend(run_binders.iter().copied());
                    let placed_reads_carrier = q
                        .guards
                        .iter()
                        .any(|g| g.placement() == (Placement::On { member: *j }) && reads_carrier(g.truth()));
                    if reads(graph, m.rel(), &read) || placed_reads_carrier {
                        tail = at;
                        break;
                    }
                    source.members.push(*j);
                    run_binders.insert(m.binder());
                }
                Indexed::Bound(_) => {
                    tail = at;
                    break;
                }
            }
        }
        Ok(Segments {
            source: Some(source),
            tail,
        })
    }

    /// R7: a fixpoint per occurrence. The carrier is read once, in the
    /// anchor; the binding carries its columns through every iteration, and
    /// a step's carrier references read them; the member's rows are the
    /// binding's.
    fn over_fix(&mut self, rows: Rows, rel: RelId, fix: &crate::pipeline::middle::core::node::Fix, owner: &mut Owner, outer: &Enclosing) -> Result<Rows> {
        // DECISION(dependent): a recursive member is one recursive binding
        // carrying its carrier's columns.
        if owner.keep || owner.marked {
            return Err(self.uncovered("a recursive member whose carriers are kept"));
        }
        let [anchor] = fix.anchors() else {
            return Err(self.uncovered("a recursive member with several anchors"));
        };
        // A demand cap here bounds each carrier row's unfold, which one
        // compound's row clause does not spell.
        if fix.cap().is_some() {
            return Err(self.uncovered("a demand cap on a recursive member read per carrier row"));
        }
        let accumulation = self.accumulation(fix);
        // A recursive part stands at the working table's level, which holds
        // no witness: a step evaluating a population-sensitive value over the
        // population has no realization here.
        let carrier_binders = binders(&rows.frame.env);
        for step in fix.steps() {
            let mut classified = Vec::new();
            self.steps(*step, &carrier_binders, &mut classified)?;
            if classified.iter().any(|s| s.sensitive()) {
                return Err(self.uncovered("a recursive step over a population that reads a window or an aggregate"));
            }
            // A step reads the carrier through the working table's carried
            // columns, which only the step's own level sees: a member
            // beside the working table cannot read them from inside itself.
            if step_member_reads(self.graph, *step, fix.frontier(), &carrier_binders) {
                return Err(self.uncovered("a recursive step whose member reads the caller's row beside the working table"));
            }
        }
        let carrier = self.stage(rows, &[], outer)?;
        let result_env = carrier.env.next();
        let keys: Vec<Key> = carrier.env.entries().iter().map(|(k, _)| *k).collect();
        let width = self.graph.rel(rel).heading().len();
        let scope = self.out.scope(None);
        let cols: Vec<crate::pipeline::middle::facade::ColId> = (0..width).map(|_| self.out.column(scope, None)).collect();
        let carried: Vec<(Key, crate::pipeline::middle::facade::ColId)> =
            keys.iter().map(|k| (*k, self.out.column(scope, None))).collect();
        self.frontiers.insert(
            fix.frontier(),
            super::fix::Frontier {
                scope,
                cols: cols.clone(),
                carried: carried.clone(),
            },
        );
        let (anchored, _) = self.over(Rows::of(carrier), *anchor, owner, outer)?;
        let mut values = Vec::new();
        for i in 0..width {
            values.push(
                anchored
                    .frame
                    .env
                    .get(Key::Pos(*anchor, i as u16))
                    .cloned()
                    .ok_or_else(|| self.unsupplied("an anchor's position"))?,
            );
        }
        for k in &keys {
            values.push(anchored.frame.env.get(*k).cloned().ok_or_else(|| self.unsupplied("a carried column"))?);
        }
        let publish: Vec<_> = cols.iter().copied().chain(carried.iter().map(|(_, c)| *c)).collect();
        let anchor_query = self.arm(scope, anchored, super::fix::accumulated(accumulation, values), None, false, Some(&publish))?;
        let carried_env = self.carried_env(fix.frontier());
        let mut members = Vec::new();
        for step in fix.steps() {
            let step_enclosing = Enclosing::of(self.graph, *step, &[&carried_env, outer.entries()], true);
            let (step_rows, mut step_values, group, distinct) = self.part(*step, &step_enclosing)?;
            if group.is_some() || distinct {
                return Err(self.uncovered("a recursive step that reduces"));
            }
            step_values.truncate(width);
            step_values.extend(carried.iter().map(|(_, c)| SqlExpr::Column(*c)));
            members.push(self.arm(scope, step_rows, step_values, None, false, None)?);
        }
        self.bind_fixpoint(scope, accumulation, anchor_query, members);
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut env = result_env;
        for (i, c) in cols.iter().enumerate() {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(*c), col));
            env.bind(Key::Pos(rel, i as u16), SqlExpr::Column(col));
        }
        for (k, c) in &carried {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(*c), col));
            env.bind(*k, SqlExpr::Column(col));
        }
        let query = self.select_at(at, items, vec![TableExpression::Scope(scope)], Vec::new(), None, Vec::new(), None, false)?;
        Ok(Rows::of(super::frame::Frame {
            from: Some(TableExpression::subquery(query, at)),
            env,
        }))
    }

    /// R8: a clause family over a carrier. Each carrier row is fanned out
    /// once per clause under a tag; each clause joins only the rows of its
    /// own tag, on its dispatch guard; a row survives (or, kept, is real)
    /// exactly when its own clause matched; each published column reads its
    /// clause's column by tag.
    fn over_family(
        &mut self,
        rows: Rows,
        rel: RelId,
        clauses: &[crate::pipeline::middle::core::node::FamilyClause],
        owner: &mut Owner,
        outer: &Enclosing,
    ) -> Result<Rows> {
        // DECISION(dependent): a clause family over a carrier is tagged per
        // clause, reading the carrier once.
        let graph = self.graph;
        let carrier = binders(&rows.frame.env);
        if clauses.iter().any(|c| reads(graph, c.body, &carrier)) {
            return self.over_collected(rows, rel, owner, outer);
        }
        let at = self.out.scope(None);
        let tag_col = self.out.column(at, None);
        let mut arms = Vec::new();
        for i in 0..clauses.len() {
            let v = SqlExpr::Literal(integer(i as i64 + 1));
            let item = if i == 0 {
                SelectItem::expression_with_alias(v, tag_col)
            } else {
                SelectItem::scaffolding_value(v, self.out.scaffolding())
            };
            arms.push(self.select_at(at, vec![item], Vec::new(), Vec::new(), None, Vec::new(), None, false)?);
        }
        let tags = super::rel::union_all(arms).ok_or_else(|| self.uncovered("a family with no clause"))?;
        let Rows { frame, filter } = rows;
        let mut env = frame.env;
        let tag = SqlExpr::Column(tag_col);
        let mut from = TableExpression::Join {
            left: Box::new(frame.from.ok_or_else(|| self.contract("a family with no carrier"))?),
            right: Box::new(TableExpression::subquery(tags, at)),
            join_type: JoinType::Inner,
            join_condition: JoinCondition::Cartesian,
        };
        let mut matched = Vec::new();
        for (i, clause) in clauses.iter().enumerate() {
            let table = self.table(clause.body, outer)?;
            let wrap = self.out.scope(None);
            let mut items = Vec::new();
            for (j, c) in table.cols.iter().enumerate() {
                let col = self.out.column(wrap, None);
                items.push(SelectItem::expression_with_alias(SqlExpr::Column(*c), col));
                env.bind(Key::Pos(clause.body, j as u16), SqlExpr::Column(col));
            }
            let mu = self.out.column(wrap, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Literal(integer(1)), mu));
            let query = self.select_at(wrap, items, vec![table.from], Vec::new(), None, Vec::new(), None, false)?;
            let mut conds = vec![SqlExpr::Binary {
                left: Box::new(tag.clone()),
                op: BinaryOperator::Equal,
                right: Box::new(SqlExpr::Literal(integer(i as i64 + 1))),
            }];
            if let Some(g) = clause.guard {
                conds.push(self.truth(g, Consumer::Filter, At { local: &env, outer })?);
            }
            from = TableExpression::Join {
                left: Box::new(from),
                right: Box::new(TableExpression::subquery(query, wrap)),
                join_type: JoinType::Left,
                join_condition: JoinCondition::On(SqlExpr::and(conds)),
            };
            matched.push(SqlExpr::Column(mu));
        }
        let own_clause_matched = SqlExpr::or(
            matched
                .iter()
                .enumerate()
                .map(|(i, mu)| {
                    SqlExpr::and(vec![
                        SqlExpr::Binary {
                            left: Box::new(tag.clone()),
                            op: BinaryOperator::Equal,
                            right: Box::new(SqlExpr::Literal(integer(i as i64 + 1))),
                        },
                        SqlExpr::Binary {
                            left: Box::new(mu.clone()),
                            op: BinaryOperator::IsNot,
                            right: Box::new(SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::Null)),
                        },
                    ])
                })
                .collect(),
        );
        let width = graph.rel(rel).heading().len();
        for j in 0..width {
            let mut when = Vec::new();
            for (i, clause) in clauses.iter().enumerate() {
                let v = env
                    .get(Key::Pos(clause.body, j as u16))
                    .cloned()
                    .ok_or_else(|| self.unsupplied("a clause's position"))?;
                when.push(crate::pipeline::middle::facade::WhenClause::new(SqlExpr::Literal(integer(i as i64 + 1)), v));
            }
            env.bind(
                Key::Pos(rel, j as u16),
                SqlExpr::Case {
                    expr: Some(Box::new(tag.clone())),
                    when_clauses: when,
                    else_clause: None,
                },
            );
        }
        let mut rows = Rows {
            frame: super::frame::Frame { from: Some(from), env },
            filter,
        };
        if owner.keep {
            rows.frame.env.bind(
                Key::Private(owner.binder, Private::Flag),
                SqlExpr::Case {
                    expr: None,
                    when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(
                        own_clause_matched,
                        SqlExpr::Literal(integer(1)),
                    )],
                    else_clause: Some(Box::new(SqlExpr::Literal(integer(0)))),
                },
            );
        } else {
            rows.filter.push(own_clause_matched);
        }
        Ok(rows)
    }

    /// R8c: a clause family a clause body of which reads its carrier. Per
    /// carrier row, the family's rows are one collection: the family is
    /// realized as a relation correlated to that row (each clause's guard
    /// and body read the row through the enclosing query) and its rows are
    /// collected into one document, each cell in an encoding that keeps its
    /// value; the target's array expansion then returns the rows, each
    /// position read from its element by place. The carrier is read once,
    /// and each of its rows is the occurrence its family's rows belong to:
    /// nothing numbers them.
    fn over_collected(&mut self, rows: Rows, rel: RelId, owner: &mut Owner, outer: &Enclosing) -> Result<Rows> {
        use crate::pipeline::middle::core::heading::Known;
        use crate::pipeline::middle::facade::{Intrinsic, LiteralValue, SqlDialect, WhenClause};
        // DECISION(dependent): a clause family whose bodies read the carrier
        // is collected per carrier row and expanded back.
        if self.out.dialect() != SqlDialect::SQLite {
            return Err(self.uncovered("a clause whose body reads its carrier"));
        }
        let graph = self.graph;
        let heading = graph.rel(rel).heading().clone();
        for (i, p) in heading.positions().iter().enumerate() {
            if !matches!(p.interior.known, Known::None)
                || self.kinds.position(graph, rel, i as u16) == super::kinds::Kind::Json
            {
                return Err(self.uncovered("a clause whose body reads its carrier and publishes a structured position"));
            }
        }
        let enclosing = Enclosing::of(graph, rel, &[&rows.frame.env, outer.entries()], false);
        let table = self.table(rel, &enclosing)?;
        // Each cell crosses the document with its own storage class, by the
        // product's exact document encoding (a REAL no spelling reads back
        // exactly refuses at run time); a BLOB crosses tagged as its hex.
        let mut cells: Vec<SqlExpr> = Vec::with_capacity(table.cols.len());
        for c in table.cols.iter() {
            let v = SqlExpr::Column(*c);
            let storage = |this: &Self, class: &str| SqlExpr::Binary {
                left: Box::new(this.out.function("typeof", vec![v.clone()])),
                op: BinaryOperator::Equal,
                right: Box::new(SqlExpr::Literal(LiteralValue::String(class.to_string()))),
            };
            let when_clauses = vec![WhenClause::new(
                storage(self, "blob"),
                self.out.function(
                    "json_object",
                    vec![SqlExpr::Literal(LiteralValue::String("b".to_string())), self.out.function("hex", vec![v.clone()])],
                ),
            )];
            cells.push(SqlExpr::Case {
                expr: None,
                when_clauses,
                else_clause: Some(Box::new(self.out.intrinsic(Intrinsic::JsonScalar, vec![v]))),
            });
        }
        let element = self.out.function("json_array", cells);
        let collected = self.out.function("json_group_array", vec![element]);
        let at = self.out.scope(None);
        let item = SelectItem::scaffolding_value(collected, self.out.scaffolding());
        let query = self.select_at(at, vec![item], vec![table.from], Vec::new(), None, Vec::new(), None, false)?;
        let staged = Key::Node(rel, 0);
        // The collection is one value per carrier row, which the expansion
        // reads more than once.
        let collection = super::rel::one_row_fence(SqlExpr::Subquery(Box::new(query)));
        let frame = self.stage_with(rows, vec![(staged, collection)], outer)?;
        let column = match frame.env.get(staged) {
            Some(SqlExpr::Column(c)) => *c,
            _ => return Err(self.unsupplied("a family's collected rows")),
        };
        let alias = self.out.scope(None);
        let value = SqlExpr::Column(self.out.column(alias, Some(&crate::pipeline::middle::core::heading::Name::new("value"))));
        let key = SqlExpr::Column(self.out.column(alias, Some(&crate::pipeline::middle::core::heading::Name::new("key"))));
        let tvf = self.out.table_function(Intrinsic::JsonEachArray, column, alias);
        let kept = owner.marked || owner.keep;
        let mut env = frame.env;
        env.retain(|k| *k != staged);
        let from = TableExpression::Join {
            left: Box::new(frame.from.ok_or_else(|| self.contract("a family with no carrier"))?),
            right: Box::new(tvf),
            join_type: if kept { JoinType::Left } else { JoinType::Inner },
            join_condition: JoinCondition::On(super::run::always()),
        };
        for i in 0..heading.len() {
            let at = SqlExpr::Literal(LiteralValue::String(format!("$[{i}]")));
            let tagged = SqlExpr::Literal(LiteralValue::String(format!("$[{i}].b")));
            let decoded = SqlExpr::Case {
                expr: Some(Box::new(self.out.function("json_type", vec![value.clone(), at.clone()]))),
                when_clauses: vec![WhenClause::new(
                    SqlExpr::Literal(LiteralValue::String("object".to_string())),
                    self.out.function("unhex", vec![self.out.function("json_extract", vec![value.clone(), tagged])]),
                )],
                else_clause: Some(Box::new(self.out.function("json_extract", vec![value.clone(), at]))),
            };
            env.bind(Key::Pos(rel, i as u16), decoded);
        }
        if owner.keep {
            env.bind(Key::Private(owner.binder, Private::Flag), flag_of(key));
        } else if owner.marked {
            let matched = Key::Private(owner.binder, Private::Matched);
            env.bind(matched, key);
            owner.matched = Some(matched);
        }
        Ok(Rows {
            frame: super::frame::Frame { from: Some(from), env },
            filter: Vec::new(),
        })
    }

    /// B10: the rows of a structured value the carrier row holds: the
    /// target's array expansion of the value, joined on each row; each
    /// interior position extracted by its name.
    fn over_unnest(
        &mut self,
        rows: Rows,
        rel: RelId,
        value: ExprId,
        expansion: &crate::pipeline::middle::core::node::Expansion,
        owner: &mut Owner,
        outer: &Enclosing,
    ) -> Result<Rows> {
        use crate::pipeline::middle::core::node::{BindAt, BindRole, Reach};
        if owner.keep || owner.marked {
            return Err(self.uncovered("a drill whose carriers are kept"));
        }
        let graph = self.graph;
        // A carried relation's rows are its staging's, the same for every
        // row that holds the value: the drill joins them on each row.
        if let Some(carried) = crate::pipeline::middle::core::node::walk::carried(graph, value) {
            let [level] = expansion.levels.as_slice() else {
                return Err(self.uncovered("a nested expansion of a carried relation"));
            };
            let table = self
                .staged_read(carried)?
                .ok_or_else(|| self.contract("a carried relation read before its act staged it"))?;
            let Rows { frame, mut filter } = rows;
            let mut env = frame.env;
            let from = TableExpression::Join {
                left: Box::new(frame.from.ok_or_else(|| self.contract("a drill with no carrier"))?),
                right: Box::new(table.from),
                join_type: JoinType::Inner,
                join_condition: JoinCondition::On(super::run::always()),
            };
            let mut published = 0u16;
            for bind in &level.binds {
                let BindAt::Position(k) = bind.at else {
                    return Err(self.uncovered("a pattern over a relation a receipt carries"));
                };
                let col = SqlExpr::Column(
                    *table.cols.get(k as usize).ok_or_else(|| self.contract("a bound position past the staging"))?,
                );
                match &bind.role {
                    BindRole::Publish(_) => {
                        env.bind(Key::Pos(rel, published), col);
                        published += 1;
                    }
                    BindRole::Constrain { value, class } => {
                        let term = self.value(*value, At { local: &env, outer })?;
                        filter.push(super::rel::equality(col, term, *class));
                    }
                }
            }
            return Ok(Rows {
                frame: super::frame::Frame { from: Some(from), env },
                filter,
            });
        }
        // Each level's element: the TVF row it reads (its `value`, `type`
        // and `key` columns), or, for a level of one row, its node.
        enum Element {
            Row { value: SqlExpr, kind: SqlExpr, key: SqlExpr },
            Node(SqlExpr),
        }
        let mut rows = rows;
        let mut elements: Vec<Element> = Vec::with_capacity(expansion.levels.len());
        let mut published = 0u16;
        // A level-0 node that is an extraction in this expression is read by
        // composing its path; one carried through a stage lost its kind.
        let structure = graph.structure(value)?;
        for (index, level) in expansion.levels.iter().enumerate() {
            // The node the level expands, as a document keeping its kind.
            let node = match &level.from {
                None => {
                    use crate::pipeline::middle::core::heading::Evidence;
                    let at = At { local: &rows.frame.env, outer };
                    match (graph.expr(value).kind(), structure.evidence) {
                        (ExprKind::Path { .. }, _) => self.node_document(value, at)?,
                        (_, Evidence::Extracted) => self.reached_node(value, at)?,
                        _ => self.value(value, at)?,
                    }
                }
                Some((parent, path)) => {
                    let base = match &elements[*parent] {
                        Element::Row { value, kind, .. } => self.out.intrinsic(
                            crate::pipeline::middle::facade::Intrinsic::JsonEachDocument,
                            vec![value.clone(), kind.clone()],
                        ),
                        Element::Node(node) => node.clone(),
                    };
                    match path {
                        None => base,
                        Some(path) => self.node_at(base, path),
                    }
                }
            };
            match level.reach {
                Reach::Node => elements.push(Element::Node(node)),
                Reach::Known | Reach::Sequence | Reach::Keys => {
                    let column = match node {
                        SqlExpr::Column(c) => c,
                        other => {
                            let key = Key::Node(rel, index as u16);
                            let frame = self.stage_with(rows, vec![(key, other)], outer)?;
                            rows = Rows::of(frame);
                            match rows.frame.env.get(key) {
                                Some(SqlExpr::Column(c)) => *c,
                                _ => return Err(self.unsupplied("an expanded node")),
                            }
                        }
                    };
                    let alias = self.out.scope(None);
                    let named = |this: &mut Self, n: &str| {
                        SqlExpr::Column(this.out.column(alias, Some(&crate::pipeline::middle::core::heading::Name::new(n))))
                    };
                    let element = Element::Row {
                        value: named(self, "value"),
                        kind: named(self, "type"),
                        key: named(self, "key"),
                    };
                    let intrinsic = match level.reach {
                        Reach::Keys => crate::pipeline::middle::facade::Intrinsic::JsonEachObject,
                        _ => crate::pipeline::middle::facade::Intrinsic::JsonEachArray,
                    };
                    let tvf = self.out.table_function(intrinsic, column, alias);
                    let Rows { frame, filter } = rows;
                    let from = TableExpression::Join {
                        left: Box::new(frame.from.ok_or_else(|| self.contract("a drill with no carrier"))?),
                        right: Box::new(tvf),
                        join_type: JoinType::Inner,
                        join_condition: JoinCondition::On(super::run::always()),
                    };
                    rows = Rows {
                        frame: super::frame::Frame { from: Some(from), env: frame.env },
                        filter,
                    };
                    elements.push(element);
                }
            }
            // The level's binds, read from its element.
            let interior = match level.reach {
                Reach::Known => Some(self.known_interior(value)?),
                _ => None,
            };
            for (j, bind) in level.binds.iter().enumerate() {
                // The node a structured consumer in the same stage reads,
                // where the core decided the bind offers one.
                let offers = graph.bind_offers_node(rel, index, j)?;
                let node = match (offers, &bind.at, &elements[index]) {
                    (false, _, _) => None,
                    (true, BindAt::Position(k), Element::Row { value: element, kind, .. }) => {
                        let (origin, heading) =
                            interior.clone().ok_or_else(|| self.contract("a bound position with no known interior"))?;
                        let key = self.interior_col(origin, &heading, *k as usize)?;
                        let document = self.out.intrinsic(
                            crate::pipeline::middle::facade::Intrinsic::JsonEachDocument,
                            vec![element.clone(), kind.clone()],
                        );
                        Some(self.node_at_key(document, key))
                    }
                    (true, BindAt::Path(path), Element::Row { value: element, kind, .. }) => {
                        let document = self.out.intrinsic(
                            crate::pipeline::middle::facade::Intrinsic::JsonEachDocument,
                            vec![element.clone(), kind.clone()],
                        );
                        Some(self.node_at(document, path))
                    }
                    (true, BindAt::Path(path), Element::Node(node)) => Some(self.node_at(node.clone(), path)),
                    (true, BindAt::Element, Element::Row { value: element, kind, .. }) => Some(self.out.intrinsic(
                        crate::pipeline::middle::facade::Intrinsic::JsonEachDocument,
                        vec![element.clone(), kind.clone()],
                    )),
                    (true, BindAt::Position(_) | BindAt::Element | BindAt::Key, _) => {
                        return Err(self.contract("a bind the core says offers a node, with no element to read it from"))
                    }
                };
                let read = match (&bind.at, &elements[index]) {
                    (BindAt::Position(k), Element::Row { value: element, .. }) => {
                        let (origin, heading) =
                            interior.clone().ok_or_else(|| self.contract("a bound position with no known interior"))?;
                        let key = self.interior_col(origin, &heading, *k as usize)?;
                        self.out.function("json_extract", vec![element.clone(), SqlExpr::PublishedJsonPathLiteral(key)])
                    }
                    (BindAt::Path(path), Element::Row { value: element, kind, .. }) => {
                        let document = self.out.intrinsic(
                            crate::pipeline::middle::facade::Intrinsic::JsonEachDocument,
                            vec![element.clone(), kind.clone()],
                        );
                        self.out.function("json_extract", vec![document, SqlExpr::JsonPathLiteral(path.clone())])
                    }
                    (BindAt::Path(path), Element::Node(node)) => {
                        self.out.function("json_extract", vec![node.clone(), SqlExpr::JsonPathLiteral(path.clone())])
                    }
                    (BindAt::Element, Element::Row { value: element, .. }) => element.clone(),
                    (BindAt::Key, Element::Row { key, .. }) => key.clone(),
                    (BindAt::Element, Element::Node(node)) => node.clone(),
                    (BindAt::Position(_) | BindAt::Key, Element::Node(_)) => {
                        return Err(self.contract("a positional or key bind on a level of one row"))
                    }
                };
                match &bind.role {
                    BindRole::Publish(_) => {
                        rows.frame.env.bind(Key::Pos(rel, published), read);
                        if let Some(node) = node {
                            rows.frame.env.bind(Key::BindNode(rel, published), node);
                        }
                        published += 1;
                    }
                    BindRole::Constrain { value, class } => {
                        let term = self.value(*value, At { local: &rows.frame.env, outer })?;
                        rows.filter.push(super::rel::equality(read, term, *class));
                    }
                }
            }
        }
        Ok(rows)
    }

    /// The known interior a drilled value holds, with what formed it.
    fn known_interior(
        &self,
        value: ExprId,
    ) -> Result<(crate::pipeline::middle::core::heading::Origin, crate::pipeline::middle::core::heading::Heading)> {
        use crate::pipeline::middle::core::heading::Known;
        let graph = self.graph;
        match &graph.structure(value)?.known {
            Known::Shape(h, origin) => Ok((*origin, (**h).clone())),
            Known::PerArm(arms) => arms
                .first()
                .and_then(|(h, o)| o.map(|o| (o, h.clone())))
                .ok_or_else(|| self.uncovered("a drill of a value no collection formed")),
            _ => Err(self.uncovered("a drill of a value no collection formed")),
        }
    }

    /// Bind a member's positions from the positions of its relation.
    fn bind_member(&self, rows: &mut Rows, m: &Member, rel: RelId) -> Result<()> {
        let graph = self.graph;
        let b = m.binder();
        for (i, p) in graph.binder(b).heading().positions().iter().enumerate() {
            let v = rows
                .frame
                .env
                .get(Key::Pos(rel, i as u16))
                .cloned()
                .ok_or_else(|| self.unsupplied("a member's position"))?;
            rows.frame.env.bind(Key::Col(b, i as u16), v.clone());
            if let Visibility::Hidden(passenger) = p.visibility {
                rows.frame.env.bind(Key::Passenger(passenger), v);
            }
            if let Some(node) = rows.frame.env.get(Key::BindNode(rel, i as u16)).cloned() {
                rows.frame.env.bind(Key::ColNode(b, i as u16), node);
            }
        }
        Ok(())
    }

    /// Bind `rel`'s positions as the positions of `of`, which it
    /// publishes unchanged.
    fn alias_positions(&self, rows: &mut Rows, rel: RelId, of: RelId) -> Result<()> {
        let n = self.graph.rel(rel).heading().len();
        for i in 0..n {
            let v = rows
                .frame
                .env
                .get(Key::Pos(of, i as u16))
                .cloned()
                .ok_or_else(|| self.unsupplied("a position of a relation it republishes"))?;
            rows.frame.env.bind(Key::Pos(rel, i as u16), v);
        }
        Ok(())
    }

    /// An independent relation as the source of a dependent run: one
    /// relation joined onto the carrier.
    fn source(
        &mut self,
        rows: Rows,
        rel: RelId,
        owner: &mut Owner,
        outer: &Enclosing,
    ) -> Result<(Rows, Vec<(Key, SqlDirection)>)> {
        let graph = self.graph;
        if let RelKind::Run(run) = graph.rel(rel).kind() {
            // A run as a source keeps its members' columns: a stage over
            // it reads them.
            let p = self.run_rows(rel, outer)?;
            let frame = self.stage(p, &[], outer)?;
            let mut rows = self.join_frame(rows, frame, Vec::new(), owner, outer)?;
            for (i, cell) in run.outputs().iter().enumerate() {
                let v = self.cell(*cell, At { local: &rows.frame.env, outer })?;
                rows.frame.env.bind(Key::Pos(rel, i as u16), v);
            }
            return Ok((rows, Vec::new()));
        }
        let table = self.table(rel, outer)?;
        let mut env = Env::fresh();
        for (i, c) in table.cols.iter().enumerate() {
            env.bind(Key::Pos(rel, i as u16), SqlExpr::Column(*c));
        }
        let mut ordering = Vec::new();
        for (k, (c, d)) in table.order.iter().enumerate() {
            let key = Key::Private(owner.binder, Private::Order(k as u16));
            env.bind(key, SqlExpr::Column(*c));
            ordering.push((key, d.clone()));
        }
        let frame = super::frame::Frame {
            from: Some(table.from),
            env,
        };
        let rows = self.join_frame(rows, frame, Vec::new(), owner, outer)?;
        Ok((rows, ordering))
    }

    /// The join stage (R1): the carrier joined with the source on θ, by
    /// a left join with a match marker when the owner pads (R4a).
    fn join_frame(&mut self, rows: Rows, source: super::frame::Frame, theta: Vec<Theta>, owner: &mut Owner, outer: &Enclosing) -> Result<Rows> {
        let mut source = source;
        let padding = owner.marked || owner.keep;
        let join = if padding && owner.matched.is_none() {
            let at = self.out.scope(None);
            let mut items = Vec::new();
            let mut env = source.env.next();
            for (k, v) in source.env.entries() {
                let col = self.out.column(at, None);
                items.push(SelectItem::expression_with_alias(v.clone(), col));
                env.bind(*k, SqlExpr::Column(col));
            }
            let mu = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Literal(integer(1)), mu));
            let key = Key::Private(owner.binder, Private::Matched);
            env.bind(key, SqlExpr::Column(mu));
            owner.matched = Some(key);
            let query = self.select_at(at, items, source.from.take().into_iter().collect(), Vec::new(), None, Vec::new(), None, false)?;
            source = super::frame::Frame {
                from: Some(TableExpression::subquery(query, at)),
                env,
            };
            JoinType::Left
        } else if padding {
            return Err(self.uncovered("a padding member with a second source"));
        } else {
            JoinType::Inner
        };
        let Rows { frame, filter } = rows;
        let mut env = frame.env;
        for (k, v) in source.env.entries() {
            env.bind(*k, v.clone());
        }
        let mut conds = Vec::new();
        for t in theta {
            conds.push(match t {
                Theta::Truth(t) => self.truth(t, Consumer::Filter, At { local: &env, outer })?,
                Theta::Slot { key, value, class } => {
                    let col = env.get(key).cloned().ok_or_else(|| self.unsupplied("a lifted slot"))?;
                    let v = self.value(value, At { local: &env, outer })?;
                    super::rel::equality(col, v, class)
                }
            });
        }
        let right = source.from.ok_or_else(|| self.contract("a source with no relation"))?;
        let from = match frame.from {
            Some(left) => TableExpression::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type: join.clone(),
                join_condition: if conds.is_empty() {
                    self.condition(Vec::new(), &join)
                } else {
                    JoinCondition::On(SqlExpr::and(conds))
                },
            },
            None => return Err(self.contract("a dependent member with no carrier")),
        };
        if owner.keep {
            if let Some(mu) = owner.matched.and_then(|k| env.get(k).cloned()) {
                env.bind(Key::Private(owner.binder, Private::Flag), flag_of(mu));
            }
        }
        Ok(Rows {
            frame: super::frame::Frame {
                from: Some(from),
                env,
            },
            filter,
        })
    }

    /// The owner's KEEP flag as the rows carry it.
    fn flag(&self, rows: &Rows, owner: &Owner) -> Result<SqlExpr> {
        rows.frame
            .env
            .get(Key::Private(owner.binder, Private::Flag))
            .cloned()
            .ok_or_else(|| self.unsupplied("a KEEP flag"))
    }

    /// Restrict the owner's rows: FILTER removes the failing rows; KEEP
    /// turns them into carriers.
    fn restrict(&mut self, rows: &mut Rows, condition: SqlExpr, owner: &Owner) -> Result<()> {
        if !owner.keep {
            rows.filter.push(condition);
            return Ok(());
        }
        let flag = self.flag(rows, owner)?;
        let updated = SqlExpr::Case {
            expr: None,
            when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(
                is_one(flag),
                SqlExpr::Case {
                    expr: None,
                    when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(
                        condition,
                        SqlExpr::Literal(integer(1)),
                    )],
                    else_clause: Some(Box::new(SqlExpr::Literal(integer(0)))),
                },
            )],
            else_clause: Some(Box::new(SqlExpr::Literal(integer(0)))),
        };
        rows.frame.env.bind(Key::Private(owner.binder, Private::Flag), updated);
        Ok(())
    }

    /// R4b: one row set per occurrence: its real rows, or, when it has
    /// none, one of its carriers with the member's columns NULL.
    fn collapse(&mut self, rows: Rows, published: &[Key], owner: &Owner, outer: &Enclosing) -> Result<Rows> {
        let w = owner.witness.and_then(|w| rows.frame.env.get(w).cloned()).ok_or_else(|| self.unsupplied("a witness"))?;
        let flag = self.flag(&rows, owner)?;
        let has = SqlExpr::WindowFunction {
            name: "max".to_string(),
            args: vec![flag.clone()],
            distinct: false,
            partition_by: vec![w.clone()],
            order_by: Vec::new(),
            frame: None,
        };
        let pick = SqlExpr::WindowFunction {
            name: "row_number".to_string(),
            args: Vec::new(),
            distinct: false,
            partition_by: vec![w],
            order_by: vec![(flag, SqlDirection::Desc)],
            frame: None,
        };
        let has_key = Key::Private(owner.binder, Private::Matched);
        let pick_key = Key::Private(owner.binder, Private::Rank);
        let frame = self.stage_with(rows, vec![(has_key, has), (pick_key, pick)], outer)?;
        let mut rows = Rows::of(frame);
        let flag = self.flag(&rows, owner)?;
        let has = rows.frame.env.get(has_key).cloned().ok_or_else(|| self.unsupplied("a collapse marker"))?;
        let pick = rows.frame.env.get(pick_key).cloned().ok_or_else(|| self.unsupplied("a collapse pick"))?;
        rows.filter.push(SqlExpr::or(vec![
            is_one(flag.clone()),
            SqlExpr::and(vec![
                SqlExpr::Binary {
                    left: Box::new(has),
                    op: BinaryOperator::Equal,
                    right: Box::new(SqlExpr::Literal(integer(0))),
                },
                is_one(pick),
            ]),
        ]));
        for key in published {
            let key = *key;
            let v = rows.frame.env.get(key).cloned().ok_or_else(|| self.unsupplied("a collapsed position"))?;
            rows.frame.env.bind(
                key,
                SqlExpr::Case {
                    expr: None,
                    when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(is_one(flag.clone()), v)],
                    else_clause: None,
                },
            );
        }
        rows.frame.env.retain(|k| *k != has_key && *k != pick_key);
        Ok(rows)
    }

    /// A read's access over the rows of its dependent body.
    fn over_access(&mut self, rows: &mut Rows, rel: RelId, body: RelId, access: &ReadAccess, owner: &Owner, outer: &Enclosing) -> Result<()> {
        let graph = self.graph;
        let heading = graph.rel(body).heading().clone();
        match access {
            ReadAccess::All => self.alias_positions(rows, rel, body),
            ReadAccess::Unasked if self.activation.is_activated(rel) => self.alias_positions(rows, rel, body),
            ReadAccess::Unasked => {
                self.alias_positions(rows, rel, body)?;
                self.restrict(rows, super::rel::impossible(), owner)?;
                Ok(())
            }
            ReadAccess::Slots(slots) => {
                let displayed: Vec<usize> = heading.displayed().map(|(i, _)| i).collect();
                let value = |rows: &Rows, k: usize| -> Option<SqlExpr> {
                    rows.frame.env.get(Key::Pos(body, displayed[k] as u16)).cloned()
                };
                let mut out = 0u16;
                for (k, slot) in slots.iter().enumerate() {
                    let v = value(rows, k).ok_or_else(|| self.unsupplied("a slot of a dependent read"))?;
                    match slot {
                        Slot::Bind(_) => {
                            rows.frame.env.bind(Key::Pos(rel, out), v);
                            out += 1;
                        }
                        Slot::Anon => {}
                        Slot::Reuse { first, class } => {
                            let f = value(rows, *first).ok_or_else(|| self.unsupplied("a reused slot"))?;
                            let compared = self.compared_reaffined(
                                (self.unaffined_at(body, displayed[k] as u16), v),
                                (self.unaffined_at(body, displayed[*first] as u16), f),
                                |v, f| super::rel::equality(v, f, *class),
                            );
                            self.restrict(rows, compared, owner)?;
                        }
                        Slot::Constraint { value: e, class } => {
                            let c = self.value(*e, At { local: &rows.frame.env, outer })?;
                            let compared = self.compared_reaffined(
                                (self.unaffined_at(body, displayed[k] as u16), v),
                                (self.unaffined(*e), c),
                                |v, c| super::rel::equality(v, c, *class),
                            );
                            self.restrict(rows, compared, owner)?;
                        }
                    }
                }
                for (i, p) in heading.positions().iter().enumerate() {
                    if matches!(p.visibility, Visibility::Hidden(_)) {
                        let v = rows
                            .frame
                            .env
                            .get(Key::Pos(body, i as u16))
                            .cloned()
                            .ok_or_else(|| self.unsupplied("a hidden position"))?;
                        rows.frame.env.bind(Key::Pos(rel, out), v);
                        out += 1;
                    }
                }
                Ok(())
            }
        }
    }

    /// An anonymous table whose cells read the carrier (W3 §1.3): its row
    /// numbers as the source, joined on nothing; each cell then a value of
    /// its row number.
    fn over_lit(
        &mut self,
        rows: Rows,
        rel: RelId,
        header: &[crate::pipeline::middle::core::node::HeaderSlot],
        cells: &[Vec<ExprId>],
        owner: &mut Owner,
        outer: &Enclosing,
    ) -> Result<Rows> {
        let at = self.out.scope(None);
        let rho = self.out.column(at, None);
        let union = self.bounded_union(at, &[rho], cells.len(), |this, scope, publish, ri| {
            let v = SqlExpr::Literal(integer(ri as i64 + 1));
            let item = match publish {
                Some(cols) => SelectItem::expression_with_alias(v, cols[0]),
                None => SelectItem::scaffolding_value(v, this.out.scaffolding()),
            };
            this.select_at(scope, vec![item], Vec::new(), Vec::new(), None, Vec::new(), None, false)
        })?;
        let rho_key = Key::Private(owner.binder, Private::Rank);
        let mut env = Env::fresh();
        env.bind(rho_key, SqlExpr::Column(rho));
        let frame = super::frame::Frame {
            from: Some(TableExpression::subquery(union, at)),
            env,
        };
        let mut rows = self.join_frame(rows, frame, Vec::new(), owner, outer)?;
        let rho = rows.frame.env.get(rho_key).cloned().ok_or_else(|| self.unsupplied("a row number"))?;
        let mut values = Vec::new();
        for k in 0..header.len() {
            let mut arms = Vec::new();
            for row in cells {
                arms.push(self.value(row[k], At { local: &rows.frame.env, outer })?);
            }
            values.push(if arms.len() == 1 {
                arms.pop().expect("one arm")
            } else {
                SqlExpr::Case {
                    expr: Some(Box::new(rho.clone())),
                    when_clauses: arms
                        .into_iter()
                        .enumerate()
                        .map(|(i, v)| {
                            crate::pipeline::middle::facade::WhenClause::new(
                                SqlExpr::Literal(integer(i as i64 + 1)),
                                v,
                            )
                        })
                        .collect(),
                    else_clause: None,
                }
            });
        }
        let mut out = 0u16;
        let mut firsts: Vec<SqlExpr> = Vec::new();
        for (k, slot) in header.iter().enumerate() {
            let v = self.padded(values[k].clone(), owner, &rows.frame.env);
            firsts.push(v.clone());
            match slot {
                crate::pipeline::middle::core::node::HeaderSlot::Reuse { first, class } => {
                    self.restrict(&mut rows, super::rel::equality(v, firsts[*first].clone(), *class), owner)?;
                }
                crate::pipeline::middle::core::node::HeaderSlot::Constraint { value, class } => {
                    let term = self.value(*value, At { local: &rows.frame.env, outer })?;
                    let compared = self.reaffined(*value, term, |term| super::rel::equality(v.clone(), term, *class));
                    self.restrict(&mut rows, compared, owner)?;
                }
                crate::pipeline::middle::core::node::HeaderSlot::Disregard => {}
                crate::pipeline::middle::core::node::HeaderSlot::Bind(_)
                | crate::pipeline::middle::core::node::HeaderSlot::Anon => {
                    rows.frame.env.bind(Key::Pos(rel, out), v);
                    out += 1;
                }
            }
        }
        Ok(rows)
    }

    /// A computed value of a padding member is NULL on its padded row.
    fn padded(&self, value: SqlExpr, owner: &Owner, env: &Env) -> SqlExpr {
        if owner.keep {
            return match env.get(Key::Private(owner.binder, Private::Flag)).cloned() {
                Some(flag) if !matches!(value, SqlExpr::Column(_)) => SqlExpr::Case {
                    expr: None,
                    when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(is_one(flag), value)],
                    else_clause: None,
                },
                _ => value,
            };
        }
        match owner.matched.and_then(|k| env.get(k).cloned()) {
            Some(mu) if !matches!(value, SqlExpr::Column(_)) => SqlExpr::Case {
                expr: None,
                when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(
                    SqlExpr::Binary {
                        left: Box::new(mu),
                        op: BinaryOperator::IsNot,
                        right: Box::new(SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::Null)),
                    },
                    value,
                )],
                else_clause: None,
            },
            _ => value,
        }
    }

    /// A pipe over a dependent run: one stage, the carrier's columns carried
    /// unchanged; a population-sensitive step partitioned by the owner's
    /// witness (R3a, R3c, R3f).
    fn over_pipe(&mut self, rows: Rows, rel: RelId, input: RelId, op: &PipeOp, owner: &mut Owner, outer: &Enclosing) -> Result<Rows> {
        let graph = self.graph;
        let RelKind::Run(run) = graph.rel(input).kind() else {
            return Err(self.uncovered("a pipe over a relation that is no run"));
        };
        let input_heading = graph.rel(input).heading().clone();
        let cells = run.outputs().to_vec();
        let (_, stage_exprs) = super::rel::Step::Pipe(op).values(graph);
        if matches!(op, PipeOp::Group { .. }) && self.received_reads(&stage_exprs).into_iter().any(|a| self.of_group(a)) {
            return Err(self.uncovered("a dependent reduction whose item reads a value of the group as a value definition's argument"));
        }
        let rows = self.staged_for(rows, super::rel::Step::Pipe(op), outer)?;
        let env = rows.frame.env.clone();
        let at = At { local: &env, outer };
        let witness = owner.witness.and_then(|w| env.get(w).cloned());
        let flag = if owner.keep { env.get(Key::Private(owner.binder, Private::Flag)).cloned() } else { None };
        let keyless = matches!(op, PipeOp::Group { keys, .. } if keys.is_empty());
        let outcome = (|| -> Result<(Vec<SqlExpr>, Option<Vec<SqlExpr>>, bool, bool)> {
            let cell_values = |this: &mut Self| -> Result<Vec<SqlExpr>> { cells.iter().map(|c| this.cell(*c, at)).collect() };
            Ok(match op {
                PipeOp::Project(items) => {
                    let mut values = Vec::new();
                    for item in items {
                        let v = self.value(item.expr, at)?;
                        values.push(self.padded(v, owner, &env));
                    }
                    for (i, p) in input_heading.positions().iter().enumerate() {
                        if matches!(p.visibility, Visibility::Hidden(_)) {
                            values.push(self.cell(cells[i], at)?);
                        }
                    }
                    for node in self.carried_nodes(items, at)? {
                        values.push(self.padded(node, owner, &env));
                    }
                    (values, None, false, true)
                }
                PipeOp::Embed(items) => {
                    let mut values = cell_values(self)?;
                    for item in items {
                        let v = self.value(item.expr, at)?;
                        values.push(self.padded(v, owner, &env));
                    }
                    for node in self.carried_nodes(items, at)? {
                        values.push(self.padded(node, owner, &env));
                    }
                    (values, None, false, true)
                }
                PipeOp::ProjectOut(selection) => {
                    let values = cell_values(self)?
                        .into_iter()
                        .enumerate()
                        .filter(|(i, _)| !selection.items().iter().any(|(s, _)| s.position() == *i))
                        .map(|(_, v)| v)
                        .collect();
                    (values, None, false, true)
                }
                PipeOp::Cover(selection) => {
                    let mut values = cell_values(self)?;
                    for (target, v) in selection.items() {
                        let v = self.value(*v, at)?;
                        let padded = self.padded(v, owner, &env);
                        *values
                            .get_mut(target.position())
                            .ok_or_else(|| self.contract("a cover target past its input's positions"))? = padded;
                    }
                    (values, None, false, true)
                }
                PipeOp::Carry(passengers) => {
                    let mut values = cell_values(self)?;
                    for p in passengers {
                        let crate::pipeline::middle::core::node::Passenger::Configured { value, .. } = graph.passenger(*p) else {
                            return Err(self.uncovered("a row locator carried by a stage"));
                        };
                        let v = self.value(*value, at)?;
                        values.push(self.padded(v, owner, &env));
                    }
                    (values, None, false, true)
                }
                PipeOp::Group { keys, reductions } => {
                    let mut values = Vec::new();
                    let mut group = Vec::new();
                    for k in keys {
                        let v = self.value(k.expr, at)?;
                        group.push(v.clone());
                        values.push(v);
                    }
                    for red in reductions {
                        values.push(self.value(red.expr, at)?);
                    }
                    (values, Some(group), false, false)
                }
                PipeOp::Distinct(keys) => {
                    let mut values = Vec::new();
                    for k in keys {
                        values.push(self.value(k.expr, at)?);
                    }
                    (values, None, true, false)
                }
            })
        })();
        let (values, group, distinct, carry_all) = outcome?;
        let lazy = outer.in_place
            && carry_all
            && !owner.keep
            && owner.matched.is_none()
            && stage_exprs.iter().all(|e| super::run::simple(graph, *e));
        if lazy {
            // DECISION(recursive-part): inside a recursive part a row-wise
            // stage of plain values is written in place, so the working
            // table stays at the part's own level.
            let mut rows = rows;
            for (i, v) in values.into_iter().enumerate() {
                rows.frame.env.bind(Key::Pos(rel, i as u16), v);
            }
            return Ok(rows);
        }
        if keyless && flag.is_none() {
            return Err(self.contract("a keyless reduction over a dependent population with no KEEP flag"));
        }
        // The stage: the carrier (every column, for a row-wise stage; the
        // carrier's own, and the owner's witness, for a reduction), then the
        // stage's values.
        let at_scope = self.out.scope(None);
        let mut items = Vec::new();
        let mut new_env = env.next();
        let mut group_by: Vec<SqlExpr> = Vec::new();
        let carried: Vec<(Key, SqlExpr)> = if carry_all {
            env.entries().to_vec()
        } else {
            env.entries()
                .iter()
                .filter(|(k, _)| {
                    owner.carrier.contains(k)
                        || Some(*k) == owner.witness
                        || (!keyless && *k == Key::Private(owner.binder, Private::Flag))
                })
                .cloned()
                .collect()
        };
        for (k, v) in &carried {
            let col = self.out.column(at_scope, None);
            items.push(SelectItem::expression_with_alias(v.clone(), col));
            new_env.bind(*k, SqlExpr::Column(col));
            group_by.push(v.clone());
        }
        if group.is_some() && witness.is_none() {
            return Err(self.contract("a reduction over a dependent population with no witness"));
        }
        for (i, v) in values.into_iter().enumerate() {
            let col = self.out.column(at_scope, None);
            items.push(SelectItem::expression_with_alias(v, col));
            new_env.bind(Key::Pos(rel, i as u16), SqlExpr::Column(col));
        }
        if keyless {
            // R3d: one row per occurrence, real.
            let col = self.out.column(at_scope, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Literal(integer(1)), col));
            new_env.bind(Key::Private(owner.binder, Private::Flag), SqlExpr::Column(col));
        }
        let group_by = group.map(|keys| {
            let mut all = group_by;
            all.extend(keys);
            all
        });
        if distinct && witness.is_none() {
            return Err(self.contract("a distinct over a dependent population with no witness"));
        }
        let query = self.select_at(
            at_scope,
            items,
            rows.frame.from.into_iter().collect(),
            rows.filter,
            group_by,
            Vec::new(),
            None,
            distinct,
        )?;
        Ok(Rows::of(super::frame::Frame {
            from: Some(TableExpression::subquery(query, at_scope)),
            env: new_env,
        }))
    }

    /// R3b: a bound over a dependent population: rows ranked within the
    /// owner's witness by the ordering in force, then filtered to the
    /// bound's ranks.
    fn rank(&mut self, rows: Rows, ordering: &[(Key, SqlDirection)], bound: &Bound, owner: &Owner, outer: &Enclosing) -> Result<Rows> {
        // DECISION(bound): a bound a dependent member owns ranks within its
        // witness and filters.
        let Some(w) = owner.witness.and_then(|w| rows.frame.env.get(w).cloned()) else {
            return Err(self.contract("a bound over a dependent population with no witness"));
        };
        let mut order_by = Vec::new();
        for (k, d) in ordering {
            let v = rows.frame.env.get(*k).cloned().ok_or_else(|| self.unsupplied("an ordering key"))?;
            order_by.push((v, d.clone()));
        }
        let flag = if owner.keep { Some(self.flag(&rows, owner)?) } else { None };
        let rank = SqlExpr::WindowFunction {
            name: "row_number".to_string(),
            args: Vec::new(),
            distinct: false,
            partition_by: std::iter::once(w).chain(flag.iter().cloned()).collect(),
            order_by,
            frame: None,
        };
        let key = Key::Private(owner.binder, Private::Rank);
        let frame = self.stage_with(rows, vec![(key, rank)], outer)?;
        let r = frame.env.get(key).cloned().ok_or_else(|| self.unsupplied("a rank"))?;
        let mut rows = Rows::of(frame);
        let offset = bound.offset.unwrap_or(0);
        let mut within = vec![SqlExpr::Binary {
            left: Box::new(r.clone()),
            op: BinaryOperator::GreaterThan,
            right: Box::new(SqlExpr::Literal(integer(offset))),
        }];
        if let Some(count) = bound.count {
            within.push(SqlExpr::Binary {
                left: Box::new(r),
                op: BinaryOperator::LessThanOrEqual,
                right: Box::new(SqlExpr::Literal(integer(offset + count))),
            });
        }
        self.restrict(&mut rows, SqlExpr::and(within), owner)?;
        rows.frame.env.retain(|k| *k != key);
        Ok(rows)
    }

    /// R2: the carrier numbered, over every column it carries.
    fn witness(&mut self, rows: Rows, owner: &mut Owner, outer: &Enclosing) -> Result<Rows> {
        let mut order_by = Vec::new();
        let mut faithful = true;
        for (key, value) in rows.frame.env.entries() {
            match self.faithful(*key, value.clone()) {
                Ok(terms) => order_by.extend(terms.into_iter().map(|t| (t, SqlDirection::Asc))),
                Err(_) => {
                    faithful = false;
                    break;
                }
            }
        }
        // DECISION(witness): when every carried column has a faithful key on
        // the target the numbering is representation-faithful; when one has
        // none, the numbering is unordered, an open A-NUM obligation.
        let order_by = if faithful { order_by } else { Vec::new() };
        let number = SqlExpr::WindowFunction {
            name: "row_number".to_string(),
            args: Vec::new(),
            distinct: false,
            partition_by: Vec::new(),
            order_by,
            frame: None,
        };
        let key = Key::Private(owner.binder, Private::Witness);
        let frame = self.stage_with(rows, vec![(key, number)], outer)?;
        owner.witness = Some(key);
        Ok(Rows::of(frame))
    }

    /// Write `rows` as one stage with `extra` computed values bound under
    /// their keys.
    pub(super) fn stage_with(&mut self, rows: Rows, extra: Vec<(Key, SqlExpr)>, _outer: &Enclosing) -> Result<super::frame::Frame> {
        self.stage_in_order(rows, extra, Vec::new())
    }

    fn stage_in_order(&mut self, rows: Rows, extra: Vec<(Key, SqlExpr)>, order_by: Vec<OrderTerm>) -> Result<super::frame::Frame> {
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut env = rows.frame.env.next();
        for (key, value) in rows.frame.env.entries() {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(value.clone(), col));
            env.bind(*key, SqlExpr::Column(col));
        }
        for (key, value) in extra {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(value, col));
            env.bind(key, SqlExpr::Column(col));
        }
        let query = self.select_at(at, items, rows.frame.from.into_iter().collect(), rows.filter, None, order_by, None, false)?;
        Ok(super::frame::Frame {
            from: Some(TableExpression::subquery(query, at)),
            env,
        })
    }

    fn any_sensitive(&self, exprs: impl IntoIterator<Item = ExprId>) -> bool {
        exprs.into_iter().any(|e| self.population_sensitive(e))
    }

    /// The steps a member owns, in order: its source and join prefix are
    /// not steps (they are hoisted); a nested dependent member is one step;
    /// a step is population-sensitive when it reduces, ranks or
    /// deduplicates, or when a value it evaluates reads a window or an
    /// aggregate. The segments are the realization's own.
    fn steps(&self, rel: RelId, carrier: &BTreeSet<BinderId>, out: &mut Vec<Step>) -> Result<()> {
        let graph = self.graph;
        if !reads(graph, rel, carrier) {
            return Ok(());
        }
        match graph.rel(rel).kind() {
            RelKind::Run(run) => {
                let q = Quals::of(run);
                let items = q.indexed();
                let segments = self.segments(&q, &items, carrier)?;
                if segments.source.is_none() {
                    self.steps(q.members[0].rel(), carrier, out)?;
                }
                for item in &items[segments.tail..] {
                    match item {
                        Indexed::Guard(g) => {
                            if q.guards[*g].placement() == Placement::Filter {
                                out.push(Step::Guard {
                                    sensitive: self.sensitive_truth(q.guards[*g].truth()),
                                });
                            }
                        }
                        Indexed::Member(j) => {
                            let population =
                                matches!(q.members[*j].dependence(), Some((Dependence::Population, _)));
                            out.push(if population || reads(graph, q.members[*j].rel(), carrier) {
                                Step::Nested
                            } else {
                                Step::Join
                            });
                        }
                        Indexed::Bound(_) => out.push(Step::Rank),
                    }
                }
            }
            RelKind::Pipe { input, op } => {
                self.steps(*input, carrier, out)?;
                out.push(match op {
                    PipeOp::Group { keys, .. } => Step::Group { keyless: keys.is_empty() },
                    PipeOp::Distinct(_) => Step::Distinct,
                    PipeOp::Project(items) | PipeOp::Embed(items) => Step::Yield {
                        windows: self.any_sensitive(items.iter().map(|i| i.expr)),
                    },
                    PipeOp::Cover(selection) => Step::Yield {
                        windows: self.any_sensitive(selection.items().iter().map(|(_, v)| *v)),
                    },
                    PipeOp::Carry(passengers) => Step::Yield {
                        windows: self.any_sensitive(passengers.iter().filter_map(|p| match graph.passenger(*p) {
                            crate::pipeline::middle::core::node::Passenger::Configured { value, .. } => Some(*value),
                            crate::pipeline::middle::core::node::Passenger::RowLocator(_) | crate::pipeline::middle::core::node::Passenger::Node { .. } => None,
                        })),
                    },
                    PipeOp::ProjectOut(_) => Step::Yield { windows: false },
                });
            }
            RelKind::Order { input, keys, bound } => {
                self.steps(*input, carrier, out)?;
                if self.any_sensitive(keys.iter().map(|k| k.expr)) {
                    out.push(Step::Yield { windows: true });
                }
                if bound.is_some() {
                    out.push(Step::Rank);
                }
            }
            RelKind::Apply { instance } => self.steps(graph.instance(*instance).body(), carrier, out)?,
            RelKind::Read {
                source: ReadSource::Local(body),
                access,
            } => {
                self.steps(*body, carrier, out)?;
                if let ReadAccess::Slots(slots) = access {
                    for s in slots {
                        if let Slot::Reuse { .. } | Slot::Constraint { .. } = s {
                            let sensitive = matches!(s, Slot::Constraint { value, .. } if self.population_sensitive(*value));
                            out.push(Step::Guard { sensitive });
                        }
                    }
                }
                if matches!(access, ReadAccess::Unasked) && !self.activation.is_activated(rel) {
                    out.push(Step::Guard { sensitive: false });
                }
            }
            RelKind::Read {
                access: ReadAccess::Slots(slots),
                ..
            } => {
                for s in slots {
                    if let Slot::Constraint { value, .. } = s {
                        out.push(Step::Guard {
                            sensitive: self.population_sensitive(*value),
                        });
                    }
                }
            }
            RelKind::Read { .. } => {}
            RelKind::Lit { rows, .. } => out.push(Step::Yield {
                windows: self.any_sensitive(rows.iter().flatten().copied()),
            }),
            RelKind::Fix(fix) => {
                // A recursive member's anchor is realized over the carrier
                // under the member's own population (`over_fix`), so its
                // steps are the member's.
                for anchor in fix.anchors() {
                    self.steps(*anchor, carrier, out)?;
                }
                out.push(if fix.deduplicating() { Step::SetRecursion } else { Step::Other });
            }
            RelKind::Unnest { value, .. } => {
                out.push(Step::Other);
                if self.population_sensitive(*value) {
                    out.push(Step::Yield { windows: true });
                }
            }
            RelKind::Family { clauses, .. } => {
                out.push(Step::Other);
                for c in clauses {
                    if let Some(g) = c.guard {
                        out.push(Step::Guard {
                            sensitive: self.sensitive_truth(g),
                        });
                    }
                }
            }
            RelKind::Receipt { .. } | RelKind::SetOp { .. } | RelKind::Minus { .. } | RelKind::Meta { .. }
        | RelKind::Witnessed { .. } => {
                out.push(Step::Other)
            }
        }
        Ok(())
    }

    /// The faithful key terms of one carried column on the target, or why
    /// it has none.
    fn faithful(&mut self, key: Key, value: SqlExpr) -> std::result::Result<Vec<SqlExpr>, String> {
        use crate::pipeline::middle::facade::SqlDialect;
        let f = |this: &mut Self, name: &str, args: Vec<SqlExpr>| this.out.function(name, args);
        match self.out.dialect() {
            SqlDialect::SQLite => {
                let terms = vec![f(self, "typeof", vec![value.clone()]), value.clone(), f(self, "quote", vec![value.clone()])];
                // No key without the math functions tells -0.0 from 0.0, and
                // dql's bundled SQLite has none, so none is assumed.
                if self.kinds.may_hold_negative_zero(self.graph, key) {
                    return Err("it may hold -0.0 and SQLite's math functions are not assumed".to_string());
                }
                Ok(terms)
            }
            SqlDialect::DuckDB | SqlDialect::PostgreSQL if !self.kinds.measured(self.graph, key) => {
                Err("its type on the target is not one whose key was measured".to_string())
            }
            SqlDialect::DuckDB => Ok(vec![f(self, "encode", vec![SqlExpr::cast(value, "text")])]),
            SqlDialect::PostgreSQL => Ok(vec![f(
                self,
                "convert_to",
                vec![
                    SqlExpr::cast(value, "text"),
                    SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::String("UTF8".to_string())),
                ],
            )]),
            SqlDialect::MySQL | SqlDialect::SqlServer => Err("the target has no measured key".to_string()),
        }
    }
}

/// `flag = 1`.
fn is_one(flag: SqlExpr) -> SqlExpr {
    SqlExpr::Binary {
        left: Box::new(flag),
        op: BinaryOperator::Equal,
        right: Box::new(SqlExpr::Literal(integer(1))),
    }
}

/// The KEEP flag of a join stage: 1 on matched rows, 0 on the carrier kept
/// for an occurrence with no match.
fn flag_of(matched: SqlExpr) -> SqlExpr {
    SqlExpr::Case {
        expr: None,
        when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(
            SqlExpr::Binary {
                left: Box::new(matched),
                op: BinaryOperator::Is,
                right: Box::new(SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::Null)),
            },
            SqlExpr::Literal(integer(0)),
        )],
        else_clause: Some(Box::new(SqlExpr::Literal(integer(1)))),
    }
}

/// The binders whose columns an environment carries.
pub(super) fn binders(env: &Env) -> BTreeSet<BinderId> {
    env.entries()
        .iter()
        .filter_map(|(k, _)| match k {
            Key::Col(b, _) => Some(*b),
            _ => None,
        })
        .collect()
}

/// Whether a relation reads any of `binders`.
pub(super) fn reads(graph: &Graph, rel: RelId, binders: &BTreeSet<BinderId>) -> bool {
    graph.rel(rel).fv().iter().any(|b| binders.contains(b))
}

/// Whether a member of a recursive step's run that is not a read of the
/// working table (`frontier`) reads the working table's row or one of
/// `binders`.
fn step_member_reads(graph: &Graph, rel: RelId, frontier: BinderId, binders: &BTreeSet<BinderId>) -> bool {
    match graph.rel(rel).kind() {
        RelKind::Pipe { input, .. } | RelKind::Order { input, .. } => step_member_reads(graph, *input, frontier, binders),
        RelKind::Apply { instance } => step_member_reads(graph, graph.instance(*instance).body(), frontier, binders),
        RelKind::Run(run) => run.members().any(|m| match reads_working_table(graph, m.rel(), frontier) {
            true => step_member_reads(graph, m.rel(), frontier, binders),
            false => graph.rel(m.rel()).fv().contains(&frontier) || reads(graph, m.rel(), binders),
        }),
        _ => false,
    }
}

/// Whether a relation reads the working table `frontier` as a relation.
fn reads_working_table(graph: &Graph, rel: RelId, frontier: BinderId) -> bool {
    match graph.rel(rel).kind() {
        RelKind::Read { source: ReadSource::Frontier(b), .. } => *b == frontier,
        RelKind::Pipe { input, .. } | RelKind::Order { input, .. } => reads_working_table(graph, *input, frontier),
        RelKind::Apply { instance } => reads_working_table(graph, graph.instance(*instance).body(), frontier),
        RelKind::Run(run) => run.members().any(|m| reads_working_table(graph, m.rel(), frontier)),
        _ => false,
    }
}

