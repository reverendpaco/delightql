// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The base rules over relation nodes: reads (B1, B12), anonymous tables
//! (B2), row stages and reductions (B5, B6), orderings and bounds (B7), set
//! operations (B8), reflection (B9) and applications (B15). Each realizes
//! one node as one FROM item whose columns follow the node's heading.

use super::frame::{Enclosing, Env, Key, Private, Table};
use super::run::Rows;
use super::value::{integer, At, Consumer};
use super::{Realizer, Result, Shared};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::{Heading, Visibility};
use crate::pipeline::middle::core::ids::{BinderId, ExprId, RelId};
use crate::pipeline::middle::core::node::{
    Bound, Cell, EqClass, ExprKind, HeaderSlot, Item, OrderKey, PipeOp, ReadAccess, ReadSource,
    RelKind, Slot,
};
use crate::pipeline::middle::facade::{
    BinaryOperator, ColId, Limit, OrderTerm, QueryExpression, ScopeId, Select, SelectItem,
    SqlDirection, SqlExpr, SqlSetOperator, TableExpression,
};

/// A slot constraint of a read, lifted out of it: the read's column (a
/// column its table offers past its heading), the value it equals, and the
/// equality's class. The run places it as a join condition.
pub(super) struct Lifted {
    pub(super) col: ColId,
    /// The affinity the slot's position lost, taken back where the
    /// constraint is compared.
    pub(super) lost: Option<super::value::Lost>,
    pub(super) value: ExprId,
    pub(super) class: EqClass,
}

/// A step whose values a stage computes: a pipe operation, an ordering's
/// keys, or a guard restricting rows.
#[derive(Clone, Copy)]
pub(super) enum Step<'a> {
    Pipe(&'a PipeOp),
    Order(&'a [OrderKey]),
    Guard(crate::pipeline::middle::core::ids::TruthId),
}

impl Step<'_> {
    /// Every value the step reads, and whether a window among them must be
    /// computed below it: a reduction or a restriction holds no window.
    pub(super) fn values(self, graph: &crate::pipeline::middle::core::graph::Graph) -> (bool, Vec<ExprId>) {
        let of = |items: &[Item]| items.iter().map(|i| i.expr).collect::<Vec<ExprId>>();
        match self {
            Step::Pipe(PipeOp::Group { keys, reductions }) => {
                let mut all = of(keys);
                all.extend(of(reductions));
                (true, all)
            }
            Step::Pipe(PipeOp::Distinct(keys)) => (true, of(keys)),
            Step::Pipe(PipeOp::Project(items) | PipeOp::Embed(items)) => (false, of(items)),
            Step::Pipe(PipeOp::Cover(selection)) => (false, selection.items().iter().map(|(_, v)| *v).collect()),
            Step::Pipe(PipeOp::Carry(passengers)) => (
                false,
                passengers
                    .iter()
                    .filter_map(|p| match graph.passenger(*p) {
                        crate::pipeline::middle::core::node::Passenger::Configured { value, .. } => Some(*value),
                        _ => None,
                    })
                    .collect(),
            ),
            Step::Pipe(PipeOp::ProjectOut(_)) => (false, Vec::new()),
            Step::Order(keys) => (false, keys.iter().map(|k| k.expr).collect()),
            Step::Guard(t) => (true, truth_values(graph, t)),
        }
    }
}

impl Realizer<'_, '_> {
    /// A relation realized as one FROM item. `outer` is what the enclosing
    /// query offers a correlated subquery.
    pub(super) fn table(&mut self, r: RelId, outer: &Enclosing) -> Result<Table> {
        Ok(self.member_table(r, outer, false)?.0)
    }

    /// A relation as a member's FROM item; with `lift`, a read's slot
    /// constraints are returned for the run to place instead of applied.
    #[stacksafe::stacksafe]
    pub(super) fn member_table(&mut self, r: RelId, outer: &Enclosing, lift: bool) -> Result<(Table, Vec<Lifted>)> {
        if let Some(table) = self.staged_read(r)? {
            return Ok((table, Vec::new()));
        }
        let graph = self.graph;
        match graph.rel(r).kind() {
            RelKind::Read { source, access } => self.read(r, source, access, outer, lift),
            RelKind::Lit { header, rows, .. } => self.lit(header, rows, outer, lift),
            RelKind::Run(_) => {
                let rows = self.run_rows(r, outer)?;
                Ok((self.run_output(r, rows, outer)?, Vec::new()))
            }
            RelKind::Pipe { input, op } => Ok((self.pipe(*input, op, outer)?, Vec::new())),
            RelKind::Order { input, keys, bound } => {
                Ok((self.order(r, *input, keys, bound.as_ref(), outer)?, Vec::new()))
            }
            RelKind::SetOp {
                left,
                right,
                alignment,
                correlation,
                ..
            } => Ok((self.set_op(*left, *right, alignment, correlation.as_ref(), outer)?, Vec::new())),
            RelKind::Minus {
                left,
                right,
                pairs,
                membership,
                class,
            } => Ok((self.minus(*left, *right, pairs, membership, *class, outer)?, Vec::new())),
            RelKind::Meta { input } => Ok((self.meta(*input)?, Vec::new())),
            RelKind::Witnessed { input, empty } => Ok((self.witnessed(*input, empty, outer)?, Vec::new())),
            RelKind::Apply { instance } => {
                let body = graph.instance(*instance).body();
                self.member_table(body, outer, lift)
            }
            RelKind::Receipt { .. } => Ok((self.receipt(r)?, Vec::new())),
            RelKind::Unnest { .. } => Err(self.uncovered("a drill")),
            RelKind::Family { clauses, .. } => Ok((self.family(r, clauses, outer)?, Vec::new())),
            RelKind::Fix(fix) => Ok((self.fix_table(r, fix, outer)?, Vec::new())),
        }
    }

    /// B1, B12: a read of a catalog relation or of a relation built in the
    /// statement, under the access its parens ask for.
    fn read(
        &mut self,
        r: RelId,
        source: &ReadSource,
        access: &ReadAccess,
        outer: &Enclosing,
        lift: bool,
    ) -> Result<(Table, Vec<Lifted>)> {
        let graph = self.graph;
        let (table, displayed, hidden) = match source {
            ReadSource::Catalog {
                name,
                columns,
                locator,
                physical,
                ..
            } => {
                let schema = self.past_session.get(&r).map(String::as_str).or(physical.schema.as_deref());
                let entity = self.out.entity(name, schema);
                let scope = self.out.table_scope(entity, name);
                let mut cols: Vec<ColId> = columns
                    .iter()
                    .map(|column| self.out.column(scope, Some(&column.name)))
                    .collect();
                let displayed: Vec<usize> = (0..cols.len()).collect();
                let mut hidden = Vec::new();
                for part in locator {
                    let col = self.located(*part, scope, &cols)?;
                    hidden.push(cols.len());
                    cols.push(col);
                }
                (
                    Table {
                        from: TableExpression::Entity {
                            entity,
                            alias: Some(scope),
                        },
                        cols,
                        order: Vec::new(),
                    },
                    displayed,
                    hidden,
                )
            }
            ReadSource::Local(body) => {
                let h = graph.rel(*body).heading();
                let displayed = h.displayed().map(|(i, _)| i).collect();
                let hidden = h
                    .positions()
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| matches!(p.visibility, Visibility::Hidden(_)))
                    .map(|(i, _)| i)
                    .collect();
                (self.local(*body, outer)?, displayed, hidden)
            }
            ReadSource::Frontier(b) => {
                let table = self.frontier_table(*b)?;
                let n = table.cols.len();
                (table, (0..n).collect(), Vec::new())
            }
            ReadSource::Created { name, columns, physical, .. } => {
                let entity = self.out.entity(name, physical.schema.as_deref());
                let scope = self.out.table_scope(entity, name);
                let cols: Vec<ColId> = columns.iter().map(|c| self.out.column(scope, Some(&c.name))).collect();
                let n = cols.len();
                (
                    Table {
                        from: TableExpression::Entity {
                            entity,
                            alias: Some(scope),
                        },
                        cols,
                        order: Vec::new(),
                    },
                    (0..n).collect(),
                    Vec::new(),
                )
            }
            ReadSource::Function { name, args, columns, .. } => {
                let table = self.function_table(name, args, columns, outer)?;
                let n = table.cols.len();
                (table, (0..n).collect(), Vec::new())
            }
            ReadSource::Staged(source) => {
                let h = graph.rel(*source).heading();
                let displayed = h.displayed().map(|(i, _)| i).collect();
                let hidden = h
                    .positions()
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| matches!(p.visibility, Visibility::Hidden(_)))
                    .map(|(i, _)| i)
                    .collect();
                let table = self
                    .staged_read(*source)?
                    .ok_or_else(|| self.contract("a staged relation read before its act staged it"))?;
                (table, displayed, hidden)
            }
        };
        match access {
            ReadAccess::All => Ok((table, Vec::new())),
            ReadAccess::Unasked if self.activation.is_activated(r) => Ok((table, Vec::new())),
            ReadAccess::Unasked => {
                // DECISION(spelling): an unactivated inchoate read keeps its
                // columns under a condition no row satisfies.
                let at = self.out.scope(None);
                let mut items = Vec::new();
                let mut cols = Vec::new();
                for c in &table.cols {
                    let col = self.out.column(at, None);
                    items.push(SelectItem::expression_with_alias(SqlExpr::Column(*c), col));
                    cols.push(col);
                }
                let query = self.select_at(at, items, vec![table.from], vec![impossible()], None, Vec::new(), None, false)?;
                Ok((
                    Table {
                        from: TableExpression::subquery(query, at),
                        cols,
                        order: Vec::new(),
                    },
                    Vec::new(),
                ))
            }
            ReadAccess::Slots(slots) => {
                // The relation whose positions a local or staged read's
                // slots compare (a catalog column's affinity stands).
                let compared = match source {
                    ReadSource::Local(rel) | ReadSource::Staged(rel) => Some(*rel),
                    _ => None,
                };
                self.slots(table, slots, &displayed, &hidden, outer, lift, compared)
            }
        }
    }

    /// The column of a stored table's read in `scope` holding each row's
    /// locator, as the marked read decided its table is reached: the
    /// pseudo-column under its decided name, or the stored column that is
    /// the locator (one of `cols`).
    pub(super) fn located(
        &self,
        locator: crate::pipeline::middle::core::ids::PassengerId,
        scope: crate::pipeline::middle::facade::ScopeId,
        cols: &[ColId],
    ) -> Result<ColId> {
        use crate::pipeline::middle::core::node::{Passenger, RowLocator};
        match self.graph.passenger(locator) {
            Passenger::RowLocator(RowLocator::Pseudo(name)) => Ok(self.out.column(scope, Some(name))),
            Passenger::RowLocator(RowLocator::Column(k)) => cols
                .get(usize::from(*k))
                .copied()
                .ok_or_else(|| self.contract("a row locator naming a column its table does not have")),
            Passenger::Configured { .. } | Passenger::Node { .. } => {
                Err(self.contract("a marked read whose locator is no row locator"))
            }
        }
    }

    /// A positional access: the slots' bound columns, then the source's
    /// hidden positions; a repeated binder and a ground term filter by their
    /// stored equality class.
    #[allow(clippy::too_many_arguments)]
    fn slots(
        &mut self,
        source: Table,
        slots: &[Slot],
        displayed: &[usize],
        hidden: &[usize],
        outer: &Enclosing,
        lift: bool,
        compared: Option<RelId>,
    ) -> Result<(Table, Vec<Lifted>)> {
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut cols = Vec::new();
        let mut filters = Vec::new();
        let mut lifted = Vec::new();
        let mut extra = Vec::new();
        let slot_col = |k: usize| source.cols[displayed[k]];
        for (k, slot) in slots.iter().enumerate() {
            match slot {
                Slot::Bind(_) => {
                    let col = self.out.column(at, None);
                    items.push(SelectItem::expression_with_alias(SqlExpr::Column(slot_col(k)), col));
                    cols.push(col);
                }
                Slot::Anon => {}
                Slot::Reuse { first, class } => {
                    let lost = |at: usize| compared.and_then(|rel| self.unaffined_at(rel, displayed[at] as u16));
                    filters.push(self.compared_reaffined(
                        (lost(k), SqlExpr::Column(slot_col(k))),
                        (lost(*first), SqlExpr::Column(slot_col(*first))),
                        |l, r| equality(l, r, *class),
                    ));
                }
                Slot::Constraint { value, class } => {
                    let lost = compared.and_then(|rel| self.unaffined_at(rel, displayed[k] as u16));
                    if lift {
                        let col = self.out.column(at, None);
                        extra.push(SelectItem::expression_with_alias(SqlExpr::Column(slot_col(k)), col));
                        lifted.push(Lifted {
                            col,
                            lost,
                            value: *value,
                            class: *class,
                        });
                    } else {
                        let local = Env::fresh();
                        let v = self.value(*value, At { local: &local, outer })?;
                        filters.push(self.compared_reaffined(
                            (lost, SqlExpr::Column(slot_col(k))),
                            (self.unaffined(*value), v),
                            |c, v| equality(c, v, *class),
                        ));
                    }
                }
            }
        }
        for h in hidden {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(source.cols[*h]), col));
            cols.push(col);
        }
        items.extend(extra);
        let query = self.select_at(at, items, vec![source.from], filters, None, Vec::new(), None, false)?;
        Ok((
            Table {
                from: TableExpression::subquery(query, at),
                cols,
                order: Vec::new(),
            },
            lifted,
        ))
    }

    /// A relation built in the statement, read: inline, or through the
    /// statement-level binding a body read more than once is realized as.
    fn local(&mut self, body: RelId, outer: &Enclosing) -> Result<Table> {
        let reads = self.reads.get(&body).copied().unwrap_or(0);
        let closed = self.graph.rel(body).fv().is_empty();
        if reads < 2 {
            return self.table(body, outer);
        }
        if !closed {
            // DECISION(authored-reuse): a body reading an enclosing row
            // cannot stand at the statement's head: each read realizes it.
            return self.table(body, outer);
        }
        // DECISION(authored-reuse): a closed body read more than once, an
        // authored binding or a relation formal's actual, is one
        // statement-level binding every read references.
        if !self.shared.contains_key(&body) {
            let table = self.table(body, &Enclosing::none())?;
            let scope = self.out.scope(None);
            let mut items = Vec::new();
            let mut cols = Vec::new();
            for c in &table.cols {
                let col = self.out.column(scope, None);
                items.push(SelectItem::expression_with_alias(SqlExpr::Column(*c), col));
                cols.push(col);
            }
            let query = self.select_at(scope, items, vec![table.from], Vec::new(), None, Vec::new(), None, false)?;
            self.ctes.push(crate::pipeline::middle::facade::Cte::ordinary(scope, query));
            self.shared.insert(body, Shared { scope, cols });
        }
        let (scope, shared_cols) = {
            let shared = &self.shared[&body];
            (shared.scope, shared.cols.clone())
        };
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut cols = Vec::new();
        for c in shared_cols {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(c), col));
            cols.push(col);
        }
        let query = self.select_at(at, items, vec![TableExpression::Scope(scope)], Vec::new(), None, Vec::new(), None, false)?;
        Ok(Table {
            from: TableExpression::subquery(query, at),
            cols,
            order: Vec::new(),
        })
    }

    /// B2: an anonymous table, one SELECT per row; a repeated header binder
    /// filters its cell against the first by its stored class, and a
    /// disregarded cell is written and published by nothing.
    fn lit(&mut self, header: &[HeaderSlot], rows: &[Vec<ExprId>], outer: &Enclosing, lift: bool) -> Result<(Table, Vec<Lifted>)> {
        // DECISION(literal): one SELECT per row, unioned.
        let at = self.out.scope(None);
        let cells: Vec<ColId> = header.iter().map(|_| self.out.column(at, None)).collect();
        let union = self.bounded_union(at, &cells, rows.len(), |this, scope, publish, ri| {
            let row = &rows[ri];
            // A literal row is a row: a value its cells may not hold where
            // they stand is computed beneath that row alone.
            let hoist = this.needs_row(row, false);
            let staged = this.staged_layers(Rows::of(super::frame::Frame::unit()), &hoist, outer)?;
            let local = staged.frame.env.clone();
            let mut items = Vec::with_capacity(row.len());
            for (k, cell) in row.iter().enumerate() {
                let v = this.value(*cell, At { local: &local, outer })?;
                items.push(match publish {
                    Some(cols) => SelectItem::expression_with_alias(v, cols[k]),
                    None => SelectItem::scaffolding_value(v, this.out.scaffolding()),
                });
            }
            this.select_at(scope, items, staged.frame.from.into_iter().collect(), staged.filter, None, Vec::new(), None, false)
        })?;
        let table = Table {
            from: TableExpression::subquery(union, at),
            cols: cells.clone(),
            order: Vec::new(),
        };
        if !header
            .iter()
            .any(|s| matches!(s, HeaderSlot::Reuse { .. } | HeaderSlot::Disregard | HeaderSlot::Constraint { .. }))
        {
            return Ok((table, Vec::new()));
        }
        let outer_at = self.out.scope(None);
        let mut items = Vec::new();
        let mut cols = Vec::new();
        let mut filters = Vec::new();
        let mut extra = Vec::new();
        let mut lifted = Vec::new();
        for (k, slot) in header.iter().enumerate() {
            match slot {
                HeaderSlot::Bind(_) | HeaderSlot::Anon => {
                    let col = self.out.column(outer_at, None);
                    items.push(SelectItem::expression_with_alias(SqlExpr::Column(cells[k]), col));
                    cols.push(col);
                }
                HeaderSlot::Reuse { first, class } => {
                    filters.push(equality(SqlExpr::Column(cells[k]), SqlExpr::Column(cells[*first]), *class));
                }
                // A header constraint is a slot constraint: the run places it
                // as a condition when it reads the run, and it filters here
                // otherwise.
                HeaderSlot::Constraint { value, class } => {
                    if lift {
                        let col = self.out.column(outer_at, None);
                        extra.push(SelectItem::expression_with_alias(SqlExpr::Column(cells[k]), col));
                        lifted.push(Lifted {
                            col,
                            lost: None,
                            value: *value,
                            class: *class,
                        });
                    } else {
                        let local = Env::fresh();
                        let v = self.value(*value, At { local: &local, outer })?;
                        let col = SqlExpr::Column(cells[k]);
                        filters.push(self.reaffined(*value, v, |v| equality(col.clone(), v, *class)));
                    }
                }
                HeaderSlot::Disregard => {}
            }
        }
        if items.is_empty() && extra.is_empty() {
            items.push(SelectItem::scaffolding_value(SqlExpr::Literal(super::value::integer(1)), self.out.scaffolding()));
        }
        items.extend(extra);
        let query = self.select_at(outer_at, items, vec![table.from], filters, None, Vec::new(), None, false)?;
        Ok((
            Table {
                from: TableExpression::subquery(query, outer_at),
                cols,
                order: Vec::new(),
            },
            lifted,
        ))
    }

    /// A run's rows, published as its output heading.
    pub(super) fn run_output(&mut self, r: RelId, rows: Rows, outer: &Enclosing) -> Result<Table> {
        let graph = self.graph;
        let RelKind::Run(run) = graph.rel(r).kind() else {
            return Err(self.contract("a run's output of a relation that is no run"));
        };
        let values: Vec<SqlExpr> = run
            .outputs()
            .iter()
            .map(|cell| self.cell(*cell, At { local: &rows.frame.env, outer }))
            .collect::<Result<_>>()?;
        self.publish(rows, values, None, false, Vec::new(), None)
    }

    /// The value a run output cell holds in a stage.
    pub(super) fn cell(&self, cell: Cell, at: At<'_>) -> Result<SqlExpr> {
        cell_at(cell, at).ok_or_else(|| match cell {
            Cell::Col(b, _) => self.unsupplied(&format!("a column of {b:?}")),
            Cell::Merged(m) => self.unsupplied(&format!("the merged key {m:?}")),
        })
    }

    /// B5, B6: one stage per pipe over the run it consumes.
    fn pipe(&mut self, input: RelId, op: &PipeOp, outer: &Enclosing) -> Result<Table> {
        let (rows, values, group, distinct) = self.pipe_values(input, op, outer)?;
        self.publish(rows, values, group, distinct, Vec::new(), None)
    }

    /// A pipe's rows and the values of its heading, with no stage above
    /// them: its grouping keys and whether it is distinct.
    #[allow(clippy::type_complexity)]
    pub(super) fn pipe_values(
        &mut self,
        input: RelId,
        op: &PipeOp,
        outer: &Enclosing,
    ) -> Result<(Rows, Vec<SqlExpr>, Option<Vec<SqlExpr>>, bool)> {
        let graph = self.graph;
        let RelKind::Run(run) = graph.rel(input).kind() else {
            return Err(self.uncovered("a pipe over a relation that is no run"));
        };
        let input_heading = graph.rel(input).heading().clone();
        let cells: Vec<Cell> = run.outputs().to_vec();
        let rows = self.run_rows(input, outer)?;
        let rows = self.staged_for(rows, Step::Pipe(op), outer)?;
        let at = |rows: &Rows| -> Env { rows.frame.env.clone() };
        let env = at(&rows);
        let at = At { local: &env, outer };
        let input_values = |this: &mut Self| -> Result<Vec<SqlExpr>> {
            cells.iter().map(|c| this.cell(*c, at)).collect()
        };
        match op {
            PipeOp::Project(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(self.value(item.expr, at)?);
                }
                for (i, p) in input_heading.positions().iter().enumerate() {
                    if matches!(p.visibility, Visibility::Hidden(_)) {
                        values.push(self.cell(cells[i], at)?);
                    }
                }
                values.extend(self.carried_nodes(items, at)?);
                Ok((rows, values, None, false))
            }
            PipeOp::Embed(items) => {
                let mut values = input_values(self)?;
                for item in items {
                    values.push(self.value(item.expr, at)?);
                }
                values.extend(self.carried_nodes(items, at)?);
                Ok((rows, values, None, false))
            }
            PipeOp::ProjectOut(selection) => {
                let values = input_values(self)?
                    .into_iter()
                    .enumerate()
                    .filter(|(i, _)| !selection.items().iter().any(|(s, _)| s.position() == *i))
                    .map(|(_, v)| v)
                    .collect();
                Ok((rows, values, None, false))
            }
            PipeOp::Cover(selection) => {
                let mut values = input_values(self)?;
                for (target, v) in selection.items() {
                    let value = self.value(*v, at)?;
                    *values
                        .get_mut(target.position())
                        .ok_or_else(|| self.contract("a cover target past its input's positions"))? = value;
                }
                Ok((rows, values, None, false))
            }
            PipeOp::Carry(passengers) => {
                let mut values = input_values(self)?;
                for p in passengers {
                    let crate::pipeline::middle::core::node::Passenger::Configured { value, .. } = graph.passenger(*p) else {
                        return Err(self.uncovered("a row locator carried by a stage"));
                    };
                    values.push(self.value(*value, at)?);
                }
                Ok((rows, values, None, false))
            }
            PipeOp::Group { keys, reductions } => {
                let (_, stage_values) = Step::Pipe(op).values(graph);
                let late: Vec<ExprId> =
                    self.received_reads(&stage_values).into_iter().filter(|a| self.of_group(*a)).collect();
                if !late.is_empty() {
                    return self.read_over_groups(rows, keys, reductions, &late, outer);
                }
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
                let group = (!keys.is_empty()).then_some(group);
                Ok((rows, values, group, false))
            }
            PipeOp::Distinct(keys) => {
                let mut values = Vec::new();
                for k in keys {
                    values.push(self.value(k.expr, at)?);
                }
                Ok((rows, values, None, true))
            }
        }
    }

    /// A reduction one of whose items reads a value of the group as an
    /// argument a relation it nests receives, or besides its own reductions:
    /// the reduction stage computes the keys, the items reading no such
    /// value, and every value of the group the other items read (`late` and
    /// their own reductions); those items are then computed over its rows,
    /// one per group, each reading those values as columns.
    #[allow(clippy::type_complexity)]
    fn read_over_groups(
        &mut self,
        rows: Rows,
        keys: &[Item],
        reductions: &[Item],
        late: &[ExprId],
        outer: &Enclosing,
    ) -> Result<(Rows, Vec<SqlExpr>, Option<Vec<SqlExpr>>, bool)> {
        use crate::pipeline::middle::core::node::walk::{reachable, Child};
        let graph = self.graph;
        let env = rows.frame.env.clone();
        let at = At { local: &env, outer };
        let deferred: Vec<bool> = reductions
            .iter()
            .map(|r| reachable(graph, &[Child::Expr(r.expr)]).exprs.iter().any(|e| late.contains(e)))
            .collect();
        let mut computed: Vec<ExprId> = late.to_vec();
        for (r, d) in reductions.iter().zip(&deferred) {
            if *d {
                group_values(graph, r.expr, late, &mut computed);
            }
        }
        let mut values = Vec::new();
        let mut group = Vec::new();
        for k in keys {
            let v = self.value(k.expr, at)?;
            group.push(v.clone());
            values.push(v);
        }
        for (r, d) in reductions.iter().zip(&deferred) {
            if !*d {
                values.push(self.value(r.expr, at)?);
            }
        }
        for c in &computed {
            values.push(self.value(*c, at)?);
        }
        let mut above = env.next();
        let table = self.publish(rows, values, (!keys.is_empty()).then_some(group), false, Vec::new(), None)?;
        let mut cols = table.cols.iter().copied();
        let mut next = |this: &Self| cols.next().ok_or_else(|| this.contract("a reduction stage with too few columns"));
        for k in keys {
            let c = SqlExpr::Column(next(self)?);
            match graph.expr(k.expr).kind() {
                ExprKind::Col(b, i) => above.bind(Key::Col(*b, *i), c.clone()),
                ExprKind::Merged(m) => above.bind(Key::Merged(*m), c.clone()),
                _ => {}
            }
            above.bind(Key::Value(k.expr), c);
        }
        for (r, d) in reductions.iter().zip(&deferred) {
            if !*d {
                above.bind(Key::Value(r.expr), SqlExpr::Column(next(self)?));
            }
        }
        for c in &computed {
            above.bind(Key::Value(*c), SqlExpr::Column(next(self)?));
        }
        let rows = Rows::of(super::frame::Frame {
            from: Some(table.from),
            env: above,
        });
        let env = rows.frame.env.clone();
        let at = At { local: &env, outer };
        let mut out = Vec::with_capacity(keys.len() + reductions.len());
        for item in keys.iter().chain(reductions) {
            out.push(self.value(item.expr, at)?);
        }
        Ok((rows, out, None, false))
    }

    /// The nodes a stage carries beside the extractions it publishes, in
    /// item order: each extraction's node document, read in the stage.
    pub(super) fn carried_nodes(&mut self, items: &[Item], at: At<'_>) -> Result<Vec<SqlExpr>> {
        let graph = self.graph;
        let mut out = Vec::new();
        for item in items {
            if graph.node_carrier(item.expr).is_some() {
                out.push(self.node_document(item.expr, at)?);
            }
        }
        Ok(out)
    }

    /// B7: an ordering, with the bound that consumes it. The ordering's
    /// keys travel past the heading, for the consumer that reads them.
    fn order(&mut self, r: RelId, input: RelId, keys: &[OrderKey], bound: Option<&Bound>, outer: &Enclosing) -> Result<Table> {
        let graph = self.graph;
        let RelKind::Run(run) = graph.rel(input).kind() else {
            return Err(self.uncovered("an ordering over a relation that is no run"));
        };
        let cells: Vec<Cell> = run.outputs().to_vec();
        let rows = self.run_rows(input, outer)?;
        let rows = self.staged_for(rows, Step::Order(keys), outer)?;
        let env = rows.frame.env.clone();
        let at = At { local: &env, outer };
        let mut values: Vec<SqlExpr> = cells.iter().map(|c| self.cell(*c, at)).collect::<Result<_>>()?;
        let width = values.len();
        let mut terms = Vec::new();
        for k in keys {
            let v = self.value(k.expr, at)?;
            terms.push((v.clone(), super::value::direction(k.direction)));
            values.push(v);
        }
        let limit = bound.map(|b| {
            // DECISION(bound): a bound at a relation's top is a row clause
            // on the stage that orders.
            limit_of(b)
        });
        // ORDERING IS ADMITTED; PRESERVATION IS NOT IMPLIED. The ordering the
        // statement presents is written by presentation; one with its bound
        // is the membership act, ordered and cut in this stage, its keys
        // published for what reads the chosen members. Any other ordering is
        // lowered here, in the stage it was written, and publishes no keys:
        // nothing after it inherits its order, a later bound included.
        let presented = self.presented == Some(r);
        if limit.is_none() && !presented {
            let order_by = terms.iter().map(|(v, d)| OrderTerm::new(v.clone(), Some(d.clone()))).collect();
            values.truncate(width);
            return self.publish(rows, values, None, false, order_by, None);
        }
        let order_by = if limit.is_some() {
            terms.iter().map(|(v, d)| OrderTerm::new(v.clone(), Some(d.clone()))).collect()
        } else {
            Vec::new()
        };
        let mut table = self.publish(rows, values, None, false, order_by, limit)?;
        table.order = table.cols[width..]
            .iter()
            .zip(terms.iter())
            .map(|(c, (_, d))| (*c, d.clone()))
            .collect();
        table.cols.truncate(width + terms.len());
        Ok(table)
    }

    /// B8: a bag union of the two arms, each projected to the stored
    /// alignment, a missing side NULL. A correlated step keeps, of each arm,
    /// the rows the stored correlation matches in the other arm (`EXISTS`
    /// over the stored pairs in the stored class), each with its
    /// multiplicity; under minimum-multiplicity pairing it keeps, of the left
    /// arm, each row numbered within its copies no further than the count of
    /// its matches in the right arm.
    fn set_op(
        &mut self,
        left: RelId,
        right: RelId,
        alignment: &[(Option<usize>, Option<usize>)],
        correlation: Option<&crate::pipeline::middle::core::node::Correlation>,
        outer: &Enclosing,
    ) -> Result<Table> {
        let arms: Vec<Vec<Option<(RelId, u16)>>> = alignment
            .iter()
            .map(|(l, r)| vec![l.map(|j| (left, j as u16)), r.map(|j| (right, j as u16))])
            .collect();
        // A correlated step compares the positions its pairs name, under
        // their affinities, which would convert them: those must hold. Every
        // other position crosses the compound as a plain union's does.
        let free = match correlation {
            None => self.affinity_free(&arms),
            Some(c) => {
                let compared: Vec<bool> = alignment
                    .iter()
                    .map(|(l, r)| c.pairs().iter().any(|(pl, pr)| *l == Some(*pl) || *r == Some(*pr)))
                    .collect();
                let held: Vec<Vec<Option<(RelId, u16)>>> =
                    arms.iter().zip(&compared).filter(|(_, c)| **c).map(|(a, _)| a.clone()).collect();
                self.compound_affinity_holds(&held)?;
                self.affinity_free(&arms).into_iter().zip(compared).map(|(free, c)| free && !c).collect()
            }
        };
        let at = self.out.scope(None);
        let cols: Vec<ColId> = alignment.iter().map(|_| self.out.column(at, None)).collect();
        let arm = |this: &mut Self, t: Table, side: &dyn Fn(&(Option<usize>, Option<usize>)) -> Option<usize>, first: bool, filters: Vec<SqlExpr>| -> Result<QueryExpression> {
            let mut items = Vec::new();
            for (k, pair) in alignment.iter().enumerate() {
                let v = match side(pair) {
                    Some(i) if free[k] => affinity_free(SqlExpr::Column(t.cols[i])),
                    Some(i) => SqlExpr::Column(t.cols[i]),
                    None => SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::Null),
                };
                items.push(if first {
                    SelectItem::expression_with_alias(v, cols[k])
                } else {
                    SelectItem::scaffolding_value(v, this.out.scaffolding())
                });
            }
            this.select_at(at, items, vec![t.from], filters, None, Vec::new(), None, false)
        };
        let query = match correlation {
            None => {
                let lt = self.table(left, outer)?;
                let rt = self.table(right, outer)?;
                let l = arm(self, lt, &|p| p.0, true, Vec::new())?;
                let r = arm(self, rt, &|p| p.1, false, Vec::new())?;
                union_all(vec![l, r]).expect("two arms")
            }
            Some(c) => {
                let lt = self.table(left, outer)?;
                let numbered_at = self.out.scope(None);
                let numbered_cols: Vec<ColId> = lt.cols.iter().map(|_| self.out.column(numbered_at, None)).collect();
                let copy = self.out.column(numbered_at, None);
                let mut items: Vec<SelectItem> = lt
                    .cols
                    .iter()
                    .zip(&numbered_cols)
                    .map(|(c, n)| SelectItem::expression_with_alias(SqlExpr::Column(*c), *n))
                    .collect();
                items.push(SelectItem::expression_with_alias(
                    SqlExpr::WindowFunction {
                        name: "row_number".to_string(),
                        args: Vec::new(),
                        distinct: false,
                        partition_by: c.pairs().iter().map(|(l, _)| compared(SqlExpr::Column(lt.cols[*l]), c.class())).collect(),
                        order_by: Vec::new(),
                        frame: None,
                    },
                    copy,
                ));
                let numbered = self.select_at(numbered_at, items, vec![lt.from], Vec::new(), None, Vec::new(), None, false)?;
                let nt = Table {
                    from: TableExpression::subquery(numbered, numbered_at),
                    cols: numbered_cols,
                    order: Vec::new(),
                };
                let rt = self.table(right, outer)?;
                let matches = self.correlated_matches(&nt, &rt, c, (left, right));
                let count_at = self.out.scope(None);
                let count = SelectItem::expression_with_alias(self.out.function("count", vec![SqlExpr::Star]), self.out.column(count_at, None));
                let counted = self.select_at(count_at, vec![count], vec![rt.from], matches, None, Vec::new(), None, false)?;
                let within = SqlExpr::Binary {
                    left: Box::new(SqlExpr::Column(copy)),
                    op: BinaryOperator::LessThanOrEqual,
                    right: Box::new(SqlExpr::Subquery(Box::new(counted))),
                };
                arm(self, nt, &|p| p.0, true, vec![within])?
            }
        };
        Ok(Table {
            from: TableExpression::subquery(query, at),
            cols,
            order: Vec::new(),
        })
    }

    /// The match of a stored correlation between a left-arm row and a
    /// right-arm row: each pair in the stored class, a pair whose sides are
    /// stored apart matching only where both are NULL.
    fn correlated_matches(
        &self,
        l: &Table,
        r: &Table,
        c: &crate::pipeline::middle::core::node::Correlation,
        (left, right): (RelId, RelId),
    ) -> Vec<SqlExpr> {
        c.pairs()
            .iter()
            .zip(c.membership())
            .flat_map(|((li, ri), m)| {
                self.pair_match(
                    (self.unaffined_at(left, *li as u16), SqlExpr::Column(l.cols[*li])),
                    (self.unaffined_at(right, *ri as u16), SqlExpr::Column(r.cols[*ri])),
                    *m,
                    c.class(),
                )
            })
            .collect()
    }

    /// One pair of a set step's match: the sides compared in the stored
    /// class, an affinity a side lost taken back; for sides stored apart,
    /// both NULL.
    fn pair_match(
        &self,
        l: (Option<super::value::Lost>, SqlExpr),
        r: (Option<super::value::Lost>, SqlExpr),
        membership: crate::pipeline::middle::core::decide::document::Membership,
        class: EqClass,
    ) -> Vec<SqlExpr> {
        let absent = |value: SqlExpr| SqlExpr::Binary {
            left: Box::new(value),
            op: BinaryOperator::Is,
            right: Box::new(SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::Null)),
        };
        match membership {
            crate::pipeline::middle::core::decide::document::Membership::Bytes => {
                vec![self.compared_reaffined(l, r, |l, r| equality(l, r, class))]
            }
            crate::pipeline::middle::core::decide::document::Membership::Apart => vec![absent(l.1), absent(r.1)],
        }
    }

    /// B8: minus as the anti-semijoin: each left row whose matched pairs no
    /// right row matches in the stored class, its multiplicity kept, the
    /// left arm's whole heading published; never `EXCEPT` or `EXCEPT ALL`.
    /// A pair whose sides are stored apart matches only where both are
    /// NULL. Its aligned pairs meet the target's compound analysis as a
    /// union's arms do.
    #[allow(clippy::too_many_arguments)]
    fn minus(
        &mut self,
        left: RelId,
        right: RelId,
        pairs: &[(usize, usize)],
        membership: &[crate::pipeline::middle::core::decide::document::Membership],
        class: EqClass,
        outer: &Enclosing,
    ) -> Result<Table> {
        let arms: Vec<Vec<Option<(RelId, u16)>>> =
            pairs.iter().map(|(l, r)| vec![Some((left, *l as u16)), Some((right, *r as u16))]).collect();
        self.compound_affinity_holds(&arms)?;
        let lt = self.table(left, outer)?;
        let rt = self.table(right, outer)?;
        let compared_pairs: Vec<SqlExpr> = pairs
            .iter()
            .zip(membership)
            .flat_map(|((l, r), m)| {
                self.pair_match(
                    (self.unaffined_at(left, *l as u16), SqlExpr::Column(lt.cols[*l])),
                    (self.unaffined_at(right, *r as u16), SqlExpr::Column(rt.cols[*r])),
                    *m,
                    class,
                )
            })
            .collect();
        let probe_at = self.out.scope(None);
        let probe = self.select_at(probe_at, Vec::new(), vec![rt.from], compared_pairs, None, Vec::new(), None, false)?;
        let at = self.out.scope(None);
        let cols: Vec<ColId> = pairs.iter().map(|_| self.out.column(at, None)).collect();
        let items = pairs
            .iter()
            .zip(&cols)
            .map(|((l, _), c)| SelectItem::expression_with_alias(SqlExpr::Column(lt.cols[*l]), *c))
            .collect();
        let survivors = SqlExpr::Exists {
            not: true,
            query: Box::new(probe),
        };
        let query = self.select_at(at, items, vec![lt.from], vec![survivors], None, Vec::new(), None, false)?;
        Ok(Table {
            from: TableExpression::subquery(query, at),
            cols,
            order: Vec::new(),
        })
    }

    /// A family of clauses none of which reads the family itself: the union
    /// of every clause's rows, each clause contributing its displayed
    /// positions in order (the clauses agree, or the family was refused
    /// where it was closed). A clause's dispatch guard reads only the
    /// actuals (FN.31: input-side evidence), so it filters that clause's
    /// rows where it stands.
    fn family(
        &mut self,
        r: RelId,
        clauses: &[crate::pipeline::middle::core::node::FamilyClause],
        outer: &Enclosing,
    ) -> Result<Table> {
        let graph = self.graph;
        if graph.rel(r).heading().positions().iter().any(|p| matches!(p.visibility, Visibility::Hidden(_))) {
            return Err(self.uncovered("a clause family carrying hidden positions"));
        }
        let width = graph.rel(r).heading().displayed().count();
        let displayed: Vec<Vec<usize>> = clauses
            .iter()
            .map(|c| graph.rel(c.body).heading().displayed().map(|(i, _)| i).collect())
            .collect();
        if displayed.iter().any(|d| d.len() != width) {
            return Err(self.contract("a clause family whose clauses disagree in width"));
        }
        let aligned: Vec<Vec<Option<(RelId, u16)>>> = (0..width)
            .map(|k| clauses.iter().zip(&displayed).map(|(c, d)| Some((c.body, d[k] as u16))).collect())
            .collect();
        self.compound_affinity_holds(&aligned)?;
        let at = self.out.scope(None);
        let cols: Vec<ColId> = (0..width).map(|_| self.out.column(at, None)).collect();
        let mut arms = Vec::with_capacity(clauses.len());
        for (ci, (clause, d)) in clauses.iter().zip(&displayed).enumerate() {
            let t = self.table(clause.body, outer)?;
            let items = (0..width)
                .map(|k| {
                    let v = SqlExpr::Column(t.cols[d[k]]);
                    if ci == 0 {
                        SelectItem::expression_with_alias(v, cols[k])
                    } else {
                        SelectItem::scaffolding_value(v, self.out.scaffolding())
                    }
                })
                .collect();
            let mut filter = Vec::new();
            if let Some(g) = clause.guard {
                let local = Env::fresh();
                filter.push(self.truth(g, Consumer::Filter, At { local: &local, outer })?);
            }
            arms.push(self.select_at(at, items, vec![t.from], filter, None, Vec::new(), None, false)?);
        }
        let union = union_all(arms).ok_or_else(|| self.contract("a clause family with no clause"))?;
        Ok(Table {
            from: TableExpression::subquery(union, at),
            cols,
            order: Vec::new(),
        })
    }

    /// THE TOTAL LEDGER: the operand's rows under a presence flag,
    /// left-joined from the one row with no column, so each row reads with
    /// `met = 1` and their absence as the proxy row with `met = 0` (NULL, or
    /// the empty relation where the core listed it).
    fn witnessed(&mut self, input: RelId, empty: &[usize], outer: &Enclosing) -> Result<Table> {
        let graph = self.graph;
        let width = graph.rel(input).heading().displayed().count();
        let operand = self.table(input, outer)?;
        let flagged_at = self.out.scope(None);
        let mut items = Vec::with_capacity(width + 1);
        let mut flagged = Vec::with_capacity(width + 1);
        for (i, _) in graph.rel(input).heading().displayed() {
            let col = self.out.column(flagged_at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(operand.cols[i]), col));
            flagged.push(col);
        }
        let present = self.out.column(flagged_at, None);
        items.push(SelectItem::expression_with_alias(SqlExpr::Literal(integer(1)), present));
        let flagged_query = self.select_at(flagged_at, items, vec![operand.from], Vec::new(), None, Vec::new(), None, false)?;
        let unit_at = self.out.scope(None);
        let unit_item = SelectItem::scaffolding_value(SqlExpr::Literal(integer(1)), self.out.scaffolding());
        let unit = self.select_at(unit_at, vec![unit_item], Vec::new(), Vec::new(), None, Vec::new(), None, false)?;
        let absent = SqlExpr::Binary {
            left: Box::new(SqlExpr::Column(present)),
            op: BinaryOperator::Is,
            right: Box::new(SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::Null)),
        };
        let at = self.out.scope(None);
        let mut out_items = Vec::with_capacity(width + 1);
        let mut cols = Vec::with_capacity(width + 1);
        for (k, col) in flagged.iter().enumerate() {
            let value = if empty.contains(&k) {
                let none = self.out.function(
                    "JSON",
                    vec![SqlExpr::Literal(crate::pipeline::middle::facade::LiteralValue::String("[]".to_string()))],
                );
                SqlExpr::Case {
                    expr: None,
                    when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(absent.clone(), none)],
                    else_clause: Some(Box::new(SqlExpr::Column(*col))),
                }
            } else {
                SqlExpr::Column(*col)
            };
            let c = self.out.column(at, None);
            out_items.push(SelectItem::expression_with_alias(value, c));
            cols.push(c);
        }
        let met = SqlExpr::Case {
            expr: None,
            when_clauses: vec![crate::pipeline::middle::facade::WhenClause::new(absent, SqlExpr::Literal(integer(0)))],
            else_clause: Some(Box::new(SqlExpr::Literal(integer(1)))),
        };
        let c = self.out.column(at, None);
        out_items.push(SelectItem::expression_with_alias(met, c));
        cols.push(c);
        let from = TableExpression::Join {
            left: Box::new(TableExpression::subquery(unit, unit_at)),
            right: Box::new(TableExpression::subquery(flagged_query, flagged_at)),
            join_type: crate::pipeline::middle::facade::JoinType::Left,
            join_condition: crate::pipeline::middle::facade::JoinCondition::On(super::run::always()),
        };
        let query = self.select_at(at, out_items, vec![from], Vec::new(), None, Vec::new(), None, false)?;
        Ok(Table {
            from: TableExpression::subquery(query, at),
            cols,
            order: Vec::new(),
        })
    }

    /// B9: one row per displayed position of the input heading: the scope
    /// of the member it comes from, its published name (a drawn one when it
    /// answers to none), and its ordinal.
    fn meta(&mut self, input: RelId) -> Result<Table> {
        let graph = self.graph;
        let RelKind::Run(run) = graph.rel(input).kind() else {
            return Err(self.uncovered("a reflection of a relation that is no run"));
        };
        let heading = graph.rel(input).heading().clone();
        let at = self.out.scope(None);
        let cols: Vec<ColId> = (0..3).map(|_| self.out.column(at, None)).collect();
        let mut scopes: Vec<(BinderId, ScopeId)> = Vec::new();
        let mut arms = Vec::new();
        for (ordinal, (i, p)) in heading.displayed().enumerate() {
            let binder = cell_binder(graph, run.outputs()[i]);
            let scope = match scopes.iter().find(|(b, _)| *b == binder) {
                Some((_, s)) => *s,
                None => {
                    let name = member_scope(run, binder);
                    let s = self.out.scope(name.as_ref());
                    scopes.push((binder, s));
                    s
                }
            };
            let column = self.out.column(scope, p.answering_name());
            let values = [
                SqlExpr::ScopeNameLiteral(scope),
                SqlExpr::PublishedNameLiteral(column),
                SqlExpr::Literal(integer(ordinal as i64 + 1)),
            ];
            let items = values
                .into_iter()
                .enumerate()
                .map(|(k, v)| {
                    if ordinal == 0 {
                        SelectItem::expression_with_alias(v, cols[k])
                    } else {
                        SelectItem::scaffolding_value(v, self.out.scaffolding())
                    }
                })
                .collect();
            arms.push(self.select_at(at, items, Vec::new(), Vec::new(), None, Vec::new(), None, false)?);
        }
        let union = union_all(arms).ok_or_else(|| self.uncovered("a reflection of an empty heading"))?;
        Ok(Table {
            from: TableExpression::subquery(union, at),
            cols,
            order: Vec::new(),
        })
    }

    /// Publish a stage over `rows`: each value under a fresh column.
    pub(super) fn publish(
        &mut self,
        rows: Rows,
        values: Vec<SqlExpr>,
        group: Option<Vec<SqlExpr>>,
        distinct: bool,
        order_by: Vec<OrderTerm>,
        limit: Option<Limit>,
    ) -> Result<Table> {
        let at = self.out.scope(None);
        let mut items = Vec::with_capacity(values.len());
        let mut cols = Vec::with_capacity(values.len());
        for v in values {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(v, col));
            cols.push(col);
        }
        let query = self.select_at(
            at,
            items,
            rows.frame.from.into_iter().collect(),
            rows.filter,
            group,
            order_by,
            limit,
            distinct,
        )?;
        Ok(Table {
            from: TableExpression::subquery(query, at),
            cols,
            order: Vec::new(),
        })
    }

    /// One SELECT standing at `at`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn select_at(
        &mut self,
        at: ScopeId,
        mut items: Vec<SelectItem>,
        from: Vec<TableExpression>,
        filters: Vec<SqlExpr>,
        group: Option<Vec<SqlExpr>>,
        order_by: Vec<OrderTerm>,
        limit: Option<Limit>,
        distinct: bool,
    ) -> Result<QueryExpression> {
        // Set identity tells rows apart by its keys: a grouping's, or, for
        // distinct, every value the stage publishes.
        let (group, distinct) = match group {
            Some(keys) => (Some(keys.into_iter().map(set_key).collect()), distinct),
            None if distinct && !items.is_empty() => {
                (Some(items.iter().filter_map(|item| item.expr().cloned()).map(set_key).collect()), false)
            }
            None => (None, distinct),
        };
        if items.is_empty() {
            items.push(SelectItem::scaffolding_value(
                SqlExpr::Literal(integer(1)),
                self.out.scaffolding(),
            ));
        }
        self.out.select(Select {
            at,
            distinct,
            items,
            from,
            filter: (!filters.is_empty()).then(|| SqlExpr::and(filters)),
            group_by: group,
            having: None,
            order_by,
            limit,
        })
    }

    /// Whether a value depends on the rows around its own row, as the core
    /// settled it.
    pub(super) fn population_sensitive(&self, e: ExprId) -> bool {
        self.graph.population_sensitive(e)
    }

    /// Whether a truth reads a population-sensitive value, as the core
    /// settled it.
    pub(super) fn sensitive_truth(&self, t: crate::pipeline::middle::core::ids::TruthId) -> bool {
        self.graph.truth_population_sensitive(t)
    }

    /// THE STAGING CONTRACT of a step: what must be computed below the
    /// stage that realizes it, whichever road realizes it (an ordinary
    /// stage, B5–B7, or a dependent member's, R3): each value no clause of
    /// that stage may hold (`needs_row`), and the nested levels of each
    /// collection a reduction makes (G3b). Every road meets it here and
    /// stages nothing else for the step's values.
    pub(super) fn staged_for(&mut self, rows: Rows, step: Step<'_>, outer: &Enclosing) -> Result<Rows> {
        let (windows, values) = step.values(self.graph);
        let mut hoist = self.needs_row(&values, windows);
        // An argument read more than once, or by a relation the step nests,
        // is one column of a stage below the step; one of the step's own
        // reduction is computed by the reduction itself (`pipe_values`).
        let grouped = matches!(step, Step::Pipe(PipeOp::Group { .. }));
        for a in self.received_reads(&values) {
            if !(grouped && self.of_group(a)) && !hoist.contains(&a) {
                hoist.push(a);
            }
        }
        hoist.sort();
        let mut rows = self.staged_layers(rows, &hoist, outer)?;
        if let Step::Pipe(PipeOp::Group { keys, reductions }) = step {
            let keys: Vec<ExprId> = keys.iter().map(|i| i.expr).collect();
            let reductions: Vec<ExprId> = reductions.iter().map(|i| i.expr).collect();
            rows = self.nested_levels(rows, &keys, &reductions, outer)?;
        }
        Ok(rows)
    }

    /// `hoist` computed below the rows' reader, each value in a stage above
    /// every hoisted value it reads: a select list does not see its own
    /// columns, so a value reading another hoisted value stands one layer
    /// higher.
    pub(super) fn staged_layers(&mut self, rows: Rows, hoist: &[ExprId], outer: &Enclosing) -> Result<Rows> {
        let mut depth: Vec<usize> = Vec::with_capacity(hoist.len());
        for (i, e) in hoist.iter().enumerate() {
            let below = (0..i)
                .filter(|j| reads_value(self.graph, *e, hoist[*j]))
                .map(|j| depth[j] + 1)
                .max()
                .unwrap_or(0);
            depth.push(below);
        }
        let mut rows = rows;
        let top = depth.iter().copied().max();
        for layer in 0..top.map_or(0, |t| t + 1) {
            let values: Vec<ExprId> = hoist.iter().zip(&depth).filter(|(_, d)| **d == layer).map(|(e, _)| *e).collect();
            let frame = self.stage(rows, &values, outer)?;
            rows = Rows::of(frame);
        }
        Ok(rows)
    }

    /// The values under `exprs` that must be a column of a stage below the
    /// one reading them: a window read where no window may stand (with
    /// `windows`), and an anchored case's computed anchor its null arm reads
    /// more than once.
    pub(super) fn needs_row(&self, exprs: &[ExprId], windows: bool) -> Vec<ExprId> {
        let mut out = Vec::new();
        for e in exprs {
            self.collect_row_needs(*e, windows, &mut out);
        }
        out
    }

    fn collect_row_needs(&self, e: ExprId, windows: bool, out: &mut Vec<ExprId>) {
        let graph = self.graph;
        match graph.expr(e).kind() {
            ExprKind::Window { .. } if windows => {
                if !out.contains(&e) {
                    out.push(e);
                }
                return;
            }
            ExprKind::Case { anchor: Some(a), arms, .. } => {
                let null_arm = arms.iter().any(|(t, _)| {
                    matches!(t, crate::pipeline::middle::core::node::CaseTest::Literal { value, .. }
                        if matches!(value, crate::pipeline::middle::facade::LiteralValue::Null))
                });
                let bare = matches!(
                    graph.expr(*a).kind(),
                    ExprKind::Col(..) | ExprKind::Merged(_) | ExprKind::Const(_) | ExprKind::Passenger(_)
                );
                if null_arm && !bare && !out.contains(a) {
                    self.collect_row_needs(*a, windows, out);
                    out.push(*a);
                }
            }
            ExprKind::Scalar { .. } => return,
            _ => {}
        }
        for child in crate::pipeline::middle::core::node::walk::of_expr(graph.expr(e).kind()) {
            match child {
                crate::pipeline::middle::core::node::walk::Child::Expr(c) => {
                    self.collect_row_needs(c, windows, out)
                }
                crate::pipeline::middle::core::node::walk::Child::Truth(t) => {
                    for c in truth_values(graph, t) {
                        self.collect_row_needs(c, windows, out);
                    }
                }
                crate::pipeline::middle::core::node::walk::Child::Rel(_) => {}
            }
        }
    }

    /// On SQLite, a compound's column takes one arm's affinity, which would
    /// convert another arm's value: where the arms' affinities let that
    /// happen, each arm's value is carried without its affinity, so every
    /// value crosses the compound as it is. One flag per position.
    pub(super) fn affinity_free(&self, positions: &[Vec<Option<(RelId, u16)>>]) -> Vec<bool> {
        if self.out.dialect() != crate::pipeline::middle::facade::SqlDialect::SQLite {
            return vec![false; positions.len()];
        }
        positions.iter().map(|arms| self.kinds.compound_changes(self.graph, arms)).collect()
    }

    /// On SQLite, a compound's column takes one arm's affinity, and a minus
    /// step's match compares its arms' columns under the same affinities: a
    /// set operation, minus step or recursive accumulation whose arms'
    /// declared column types would let an affinity change another arm's
    /// value is not covered. Each entry is one position's arms.
    pub(super) fn compound_affinity_holds(&self, positions: &[Vec<Option<(RelId, u16)>>]) -> Result<()> {
        if self.out.dialect() != crate::pipeline::middle::facade::SqlDialect::SQLite {
            return Ok(());
        }
        if positions.iter().any(|arms| self.kinds.compound_changes(self.graph, arms)) {
            return Err(self.uncovered(
                "a set operation whose arms' declared column types would let SQLite's compound affinity change a value",
            ));
        }
        Ok(())
    }

    /// Whether `e` is the anchor of an anchored case.
    fn is_anchor(&self, e: ExprId) -> bool {
        self.anchors.contains(&e)
    }

    /// The value definitions' arguments a step's values read that must be
    /// one column of a stage: each read by a relation nested in the step
    /// (whose enclosing value it is) or read more than once by the step, in
    /// the order they were made, so each follows the arguments its value
    /// reads.
    pub(super) fn received_reads(&self, values: &[ExprId]) -> Vec<ExprId> {
        let mut reads: std::collections::BTreeMap<ExprId, usize> = std::collections::BTreeMap::new();
        let mut nested: std::collections::BTreeSet<ExprId> = std::collections::BTreeSet::new();
        for e in values {
            self.tally(*e, &mut reads, &mut nested);
        }
        let mut out: Vec<ExprId> = reads.iter().filter(|(_, n)| **n > 1).map(|(e, _)| *e).chain(nested).collect();
        out.sort();
        out.dedup();
        out
    }

    fn tally(
        &self,
        e: ExprId,
        reads: &mut std::collections::BTreeMap<ExprId, usize>,
        nested: &mut std::collections::BTreeSet<ExprId>,
    ) {
        use crate::pipeline::middle::core::node::walk::{of_expr, Child};
        let kind = self.graph.expr(e).kind();
        if let ExprKind::Argument { value, .. } = kind {
            let n = reads.entry(e).or_insert(0);
            *n += 1;
            if *n == 1 {
                self.tally(*value, reads, nested);
            }
            return;
        }
        for c in of_expr(kind) {
            match c {
                Child::Expr(c) => self.tally(c, reads, nested),
                Child::Truth(t) => self.tally_truth(t, reads, nested),
                Child::Rel(r) => self.enclosed_arguments(r, nested),
            }
        }
    }

    fn tally_truth(
        &self,
        t: crate::pipeline::middle::core::ids::TruthId,
        reads: &mut std::collections::BTreeMap<ExprId, usize>,
        nested: &mut std::collections::BTreeSet<ExprId>,
    ) {
        use crate::pipeline::middle::core::node::walk::{of_truth, Child};
        for c in of_truth(self.graph.truth(t).kind()) {
            match c {
                Child::Expr(c) => self.tally(c, reads, nested),
                Child::Truth(t) => self.tally_truth(t, reads, nested),
                Child::Rel(r) => self.enclosed_arguments(r, nested),
            }
        }
    }

    /// The arguments a nested relation reads from outside itself: those
    /// whose free binders are all free in it.
    fn enclosed_arguments(&self, rel: RelId, nested: &mut std::collections::BTreeSet<ExprId>) {
        use crate::pipeline::middle::core::node::walk::{reachable, Child};
        let free = self.graph.rel(rel).fv();
        for e in reachable(self.graph, &[Child::Rel(rel)]).exprs {
            if matches!(self.graph.expr(e).kind(), ExprKind::Argument { .. })
                && self.graph.expr(e).fv().iter().all(|b| free.contains(b))
            {
                nested.insert(e);
            }
        }
    }

    /// Whether an argument is a value of the reduction it stands in: one
    /// its call evaluates over the group (an aggregate, a collection), which
    /// only the reduction computes.
    pub(super) fn of_group(&self, a: ExprId) -> bool {
        match self.graph.expr(a).kind() {
            ExprKind::Argument { value, .. } => reduces(self.graph, *value),
            _ => false,
        }
    }

    /// Write `rows` as one stage: every column the frame carries, then the
    /// `extra` values computed over it, each bound as computed below. An
    /// anchored case's anchor is one value per row: it goes out inside a
    /// window whose frame is its own row, which no subquery boundary of any
    /// target re-evaluates per reference.
    pub(super) fn stage(&mut self, rows: Rows, extra: &[ExprId], outer: &Enclosing) -> Result<super::frame::Frame> {
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut env = rows.frame.env.next();
        for (key, value) in rows.frame.env.entries() {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(value.clone(), col));
            env.bind(*key, SqlExpr::Column(col));
        }
        for e in extra {
            let v = self.value(*e, At { local: &rows.frame.env, outer })?;
            let once = self.is_anchor(*e) || matches!(self.graph.expr(*e).kind(), ExprKind::Argument { .. });
            let v = if once && !self.graph.evaluates_window(*e) {
                // DECISION(anchor): a computed value one row reads more than
                // once (an anchored case's anchor its null arm reads, a value
                // definition's argument) is one value per row, computed
                // inside a window framed to its own row.
                one_row_fence(v)
            } else {
                v
            };
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(v, col));
            env.bind(Key::Value(*e), SqlExpr::Column(col));
        }
        let query = self.select_at(at, items, rows.frame.from.into_iter().collect(), rows.filter, None, Vec::new(), None, false)?;
        Ok(super::frame::Frame {
            from: Some(TableExpression::subquery(query, at)),
            env,
        })
    }

    /// A correlated subquery's rows, for an existence.
    pub(super) fn exists_query(&mut self, rel: RelId, at: At<'_>) -> Result<QueryExpression> {
        let enclosing = Enclosing::of(self.graph, rel, &[at.local, at.outer.entries()], false);
        let table = self.table(rel, &enclosing)?;
        let scope = self.out.scope(None);
        self.select_at(scope, Vec::new(), vec![table.from], Vec::new(), None, Vec::new(), None, false)
    }

    /// A correlated subquery's one value, for a scalar position.
    pub(super) fn scalar_query(&mut self, rel: RelId, at: At<'_>) -> Result<QueryExpression> {
        let enclosing = Enclosing::of(self.graph, rel, &[at.local, at.outer.entries()], false);
        let heading: Heading = self.graph.rel(rel).heading().clone();
        let table = self.table(rel, &enclosing)?;
        let Some((first, _)) = heading.displayed().next() else {
            return Err(self.uncovered("a scalar relation with no column"));
        };
        let scope = self.out.scope(None);
        let item = SelectItem::scaffolding_value(SqlExpr::Column(table.cols[first]), self.out.scaffolding());
        self.select_at(scope, vec![item], vec![table.from], Vec::new(), None, Vec::new(), None, false)
    }

    /// The truths of `filters`, translated over `rows` in order; a truth
    /// reading a window is filtered in a stage above the one computing it.
    pub(super) fn filtered(&mut self, mut rows: Rows, filters: &[crate::pipeline::middle::core::ids::TruthId], outer: &Enclosing) -> Result<Rows> {
        for t in filters {
            rows = self.staged_for(rows, Step::Guard(*t), outer)?;
            let env = rows.frame.env.clone();
            let v = self.truth(*t, Consumer::Filter, At { local: &env, outer })?;
            rows.filter.push(v);
        }
        Ok(rows)
    }

    /// The private ordering keys of a member's table, bound in `env`.
    pub(super) fn bind_order(&self, env: &mut Env, binder: BinderId, table: &Table) -> Vec<(Key, SqlDirection)> {
        table
            .order
            .iter()
            .enumerate()
            .map(|(k, (c, d))| {
                let key = Key::Private(binder, Private::Order(k as u16));
                env.bind(key, SqlExpr::Column(*c));
                (key, d.clone())
            })
            .collect()
    }
}

/// The values of its group a reduction item computed above the reduction
/// reads: each argument of `late` and each maximal reducing value outside a
/// nested relation, added to `out` once.
fn group_values(graph: &crate::pipeline::middle::core::graph::Graph, e: ExprId, late: &[ExprId], out: &mut Vec<ExprId>) {
    use crate::pipeline::middle::core::node::walk::{of_expr, Child};
    if late.contains(&e) || (reduces(graph, e) && !matches!(graph.expr(e).kind(), ExprKind::Infix(..) | ExprKind::Case { .. } | ExprKind::Argument { .. } | ExprKind::Crossed(_) | ExprKind::Construct { .. } | ExprKind::Path { .. } | ExprKind::Across(_)))
    {
        if !out.contains(&e) {
            out.push(e);
        }
        return;
    }
    for c in of_expr(graph.expr(e).kind()) {
        match c {
            Child::Expr(c) => group_values(graph, c, late, out),
            Child::Truth(t) => {
                for v in truth_values(graph, t) {
                    group_values(graph, v, late, out);
                }
            }
            Child::Rel(_) => {}
        }
    }
}

/// Whether a value reduces its rows itself: an aggregate, a collection, a
/// metadata collector or a delegate's pick reached without crossing a
/// nested relation.
pub(super) fn reduces(graph: &crate::pipeline::middle::core::graph::Graph, e: ExprId) -> bool {
    use crate::pipeline::middle::core::node::walk::{of_expr, Child};
    match graph.expr(e).kind() {
        ExprKind::Call { grade, .. } if *grade == crate::pipeline::middle::core::node::Grade::Aggregate => true,
        ExprKind::Collect { .. } | ExprKind::Metadata { .. } | ExprKind::Pick { .. } => true,
        ExprKind::Scalar { .. } => false,
        kind => of_expr(kind).into_iter().any(|c| match c {
            Child::Expr(c) => reduces(graph, c),
            Child::Truth(t) => truth_values(graph, t).into_iter().any(|v| reduces(graph, v)),
            Child::Rel(_) => false,
        }),
    }
}

/// Whether `e` reads `inner` below itself, within its own row: a nested
/// relation keeps its own rows and is not entered.
fn reads_value(graph: &crate::pipeline::middle::core::graph::Graph, e: ExprId, inner: ExprId) -> bool {
    use crate::pipeline::middle::core::node::walk::{of_expr, Child};
    of_expr(graph.expr(e).kind()).into_iter().any(|c| match c {
        Child::Expr(c) => c == inner || reads_value(graph, c, inner),
        Child::Truth(t) => truth_values(graph, t).into_iter().any(|v| v == inner || reads_value(graph, v, inner)),
        Child::Rel(_) => false,
    })
}

/// `first_value(v) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW)`: the
/// value computed once for its row.
pub(super) fn one_row_fence(v: SqlExpr) -> SqlExpr {
    SqlExpr::WindowFunction {
        name: "first_value".to_string(),
        args: vec![v],
        distinct: false,
        partition_by: Vec::new(),
        order_by: Vec::new(),
        frame: Some(crate::pipeline::middle::facade::SqlWindowFrame {
            mode: crate::pipeline::middle::facade::SqlFrameMode::Rows,
            start: crate::pipeline::middle::facade::SqlFrameBound::CurrentRow,
            end: crate::pipeline::middle::facade::SqlFrameBound::CurrentRow,
        }),
    }
}

/// The values a truth reads directly, for the hoisting judgment.
pub(super) fn truth_values(graph: &crate::pipeline::middle::core::graph::Graph, t: crate::pipeline::middle::core::ids::TruthId) -> Vec<ExprId> {
    use crate::pipeline::middle::core::node::walk::{of_truth, Child};
    let mut out = Vec::new();
    let mut pending = vec![t];
    while let Some(t) = pending.pop() {
        for c in of_truth(graph.truth(t).kind()) {
            match c {
                Child::Expr(e) => out.push(e),
                Child::Truth(t) => pending.push(t),
                Child::Rel(_) => {}
            }
        }
    }
    out
}

/// The value a run output cell holds in a stage, when the stage holds it.
pub(super) fn cell_at(cell: Cell, at: At<'_>) -> Option<SqlExpr> {
    match cell {
        Cell::Col(b, i) => at.get(Key::Col(b, i)),
        Cell::Merged(m) => at.get(Key::Merged(m)),
    }
}

/// The binder whose member a run output cell's value comes from: a merged
/// key's left operand's.
fn cell_binder(graph: &crate::pipeline::middle::core::graph::Graph, cell: Cell) -> BinderId {
    match cell {
        Cell::Col(b, _) => b,
        Cell::Merged(m) => cell_binder(graph, graph.merge(m).left()),
    }
}

/// The scope name of the member a binder is bound to.
fn member_scope(run: &crate::pipeline::middle::core::node::Run, binder: BinderId) -> Option<crate::pipeline::middle::core::heading::Name> {
    run.quals().iter().find_map(|q| match q {
        crate::pipeline::middle::core::node::Qual::Member(m) if m.binder() == binder => m.scope().cloned(),
        _ => None,
    })
}

/// An equality in its stored class.
pub(super) fn equality(left: SqlExpr, right: SqlExpr, class: EqClass) -> SqlExpr {
    SqlExpr::Binary {
        left: Box::new(super::value::grouped(left)),
        op: match class {
            EqClass::NullSafe => BinaryOperator::IsNotDistinctFrom,
            EqClass::Correspondence | EqClass::Ordering => BinaryOperator::Equal,
        },
        right: Box::new(compared(super::value::grouped(right), class)),
    }
}

/// An operand as its comparison's stored class compares it: an equality
/// class compares values exactly, so the operand is the exact operand the
/// target spells, which no collation a column declares on the other operand
/// overrides; an ordering compares as written.
pub(super) fn compared(operand: SqlExpr, class: EqClass) -> SqlExpr {
    match class {
        EqClass::NullSafe | EqClass::Correspondence => {
            SqlExpr::intrinsic(crate::pipeline::middle::facade::Intrinsic::Exact, vec![operand])
        }
        EqClass::Ordering => operand,
    }
}

/// A key rows are told apart by under set identity (a grouping, distinct,
/// a partition, a distinct aggregate's argument), compared in the class the
/// equality decider gives set identity. A literal tells no rows apart and
/// is left as written: SQL reads an integer literal key as a position.
pub(super) fn set_key(key: SqlExpr) -> SqlExpr {
    if key.is_literal() {
        return key;
    }
    compared(key, crate::pipeline::middle::core::decide::equality::set_identity())
}

/// A value without the affinity its column would lend it (SQLite's unary
/// `+`): the value itself, as SQLite stores it.
pub(super) fn affinity_free(value: SqlExpr) -> SqlExpr {
    SqlExpr::Unary {
        op: crate::pipeline::middle::facade::UnaryOperator::Plus,
        expr: Box::new(value),
    }
}

/// A condition no row satisfies.
pub(super) fn impossible() -> SqlExpr {
    SqlExpr::Binary {
        left: Box::new(SqlExpr::Literal(integer(0))),
        op: BinaryOperator::Equal,
        right: Box::new(SqlExpr::Literal(integer(1))),
    }
}

impl Realizer<'_, '_> {
    /// The bag union of `n` row arms, left to right, published at `at` as
    /// `cols`: `arm` builds row `i` at a scope, naming its values by the
    /// columns it is given (the first arm of each compound) or leaving them
    /// positional. DECISION(compound-terms): where the target bounds a
    /// compound's terms, a longer bag is a compound of derived compounds,
    /// each within the bound; the rows and their order are the same.
    pub(super) fn bounded_union(
        &mut self,
        at: ScopeId,
        cols: &[ColId],
        n: usize,
        mut arm: impl FnMut(&mut Self, ScopeId, Option<&[ColId]>, usize) -> Result<QueryExpression>,
    ) -> Result<QueryExpression> {
        let limit = self.out.dialect().compound_term_limit().unwrap_or(usize::MAX).max(1);
        if n <= limit {
            let mut arms = Vec::with_capacity(n);
            for i in 0..n {
                arms.push(arm(self, at, (i == 0).then_some(cols), i)?);
            }
            return union_all(arms).ok_or_else(|| self.uncovered("an anonymous table with no rows"));
        }
        if n.div_ceil(limit) > limit {
            return Err(self.uncovered("a bag of rows longer than the target's compound bound squared"));
        }
        let mut outer_arms = Vec::new();
        for (c, start) in (0..n).step_by(limit).enumerate() {
            let chunk_at = self.out.scope(None);
            let chunk_cols: Vec<ColId> = cols.iter().map(|_| self.out.column(chunk_at, None)).collect();
            let mut arms = Vec::new();
            for i in start..(start + limit).min(n) {
                arms.push(arm(self, chunk_at, (i == start).then_some(chunk_cols.as_slice()), i)?);
            }
            let chunk = union_all(arms).ok_or_else(|| self.contract("an empty compound term"))?;
            let items = chunk_cols
                .iter()
                .enumerate()
                .map(|(k, col)| match c {
                    0 => SelectItem::expression_with_alias(SqlExpr::Column(*col), cols[k]),
                    _ => SelectItem::scaffolding_value(SqlExpr::Column(*col), self.out.scaffolding()),
                })
                .collect();
            outer_arms.push(self.select_at(at, items, vec![TableExpression::subquery(chunk, chunk_at)], Vec::new(), None, Vec::new(), None, false)?);
        }
        union_all(outer_arms).ok_or_else(|| self.contract("an empty compound"))
    }
}

/// The bag union of the arms, left to right.
pub(super) fn union_all(arms: Vec<QueryExpression>) -> Option<QueryExpression> {
    arms.into_iter().reduce(|left, right| QueryExpression::SetOperation {
        op: SqlSetOperator::UnionAll,
        left: Box::new(left),
        right: Box::new(right),
    })
}

/// A bound's row clause.
pub(super) fn limit_of(bound: &Bound) -> Limit {
    match (bound.count, bound.offset) {
        (Some(count), Some(offset)) => Limit::with_offset(count, offset),
        (Some(count), None) => Limit::new(count),
        (None, Some(offset)) => Limit::offset_only(offset),
        (None, None) => Limit::offset_only(0),
    }
}
