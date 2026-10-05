// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! R7 at the unit carrier: a fixpoint is one recursive binding at the
//! statement's head, its anchors the anchor part and each step a member
//! reading the working table directly (never through a stage above it). A
//! read of the fixpoint reads the binding.

use super::frame::{Enclosing, Env, Key, Table};
use super::run::Rows;
use super::{Realizer, Result};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::ids::{BinderId, RelId};
use crate::pipeline::middle::core::node::{Fix, PipeOp, RelKind};
use crate::pipeline::middle::facade::{ColId, Limit, QueryExpression, ScopeId, SelectItem, SqlExpr, TableExpression};

/// How a recursive binding accumulates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Accumulation {
    Bag,
    Set,
}

/// A fixpoint's binding as its steps read it: the scope, the columns of the
/// definition's heading, and the carrier columns it carries.
#[derive(Clone)]
pub(super) struct Frontier {
    pub(super) scope: ScopeId,
    pub(super) cols: Vec<ColId>,
    pub(super) carried: Vec<(Key, ColId)>,
}

impl Realizer<'_, '_> {
    /// A fixpoint no carrier row parameterizes.
    pub(super) fn fix_table(&mut self, r: RelId, fix: &Fix, outer: &Enclosing) -> Result<Table> {
        if !self.graph.rel(r).fv().is_empty() {
            return Err(self.uncovered("a recursive definition reading an enclosing row"));
        }
        let accumulation = self.accumulation(fix);
        let width = self.graph.rel(r).heading().len();
        let parts: Vec<RelId> = fix.anchors().iter().chain(fix.steps()).copied().collect();
        let positions: Vec<Vec<Option<(RelId, u16)>>> =
            (0..width).map(|i| parts.iter().map(|p| Some((*p, i as u16))).collect()).collect();
        // A capped accumulation's member is read by the target's recursive
        // legalization, which needs its columns as written: carrying a part
        // without affinity there is not covered.
        let free = if fix.cap().is_some() {
            self.compound_affinity_holds(&positions)?;
            vec![false; positions.len()]
        } else {
            self.affinity_free(&positions)
        };
        let scope = self.out.scope(None);
        self.free_positions.insert(scope, free);
        let cols: Vec<ColId> = (0..width).map(|_| self.out.column(scope, None)).collect();
        self.frontiers.insert(
            fix.frontier(),
            Frontier {
                scope,
                cols: cols.clone(),
                carried: Vec::new(),
            },
        );
        let mut anchors = Vec::new();
        for (k, a) in fix.anchors().iter().enumerate() {
            // A part publishes the positions the fixpoint's heading holds:
            // its node passengers do not cross the definition.
            let (rows, mut values, group, distinct) = self.part(*a, outer)?;
            values.truncate(width);
            let values = accumulated(accumulation, values);
            anchors.push(self.arm(scope, rows, values, group, distinct, (k == 0).then_some(cols.as_slice()))?);
        }
        let anchor = super::rel::union_all(anchors).ok_or_else(|| self.uncovered("a fixpoint with no anchor"))?;
        // DECISION(recursive-cap): where the target spells a recursive
        // binding's total cap natively, the fixpoint's decided demand cap is
        // its last member's own row clause, which the target reads as a cap
        // on the whole compound; elsewhere each bound stands where it is
        // written, and the target's legalization judges it.
        let cap = fix.cap().filter(|_| self.out.dialect().spells_recursive_total_cap());
        if let Some(cap) = cap {
            self.caps.extend(cap.runs().iter().copied());
        }
        let members = self.fix_steps(fix, scope, width, outer, cap.map(|c| c.count()));
        if let Some(cap) = cap {
            for run in cap.runs() {
                self.caps.remove(run);
            }
        }
        let members = members?;
        self.bind_fixpoint(scope, accumulation, anchor, members);
        let at = self.out.scope(None);
        let mut items = Vec::new();
        let mut occ = Vec::new();
        for c in &cols {
            let col = self.out.column(at, None);
            items.push(SelectItem::expression_with_alias(SqlExpr::Column(*c), col));
            occ.push(col);
        }
        let query = self.select_at(at, items, vec![TableExpression::Scope(scope)], Vec::new(), None, Vec::new(), None, false)?;
        Ok(Table {
            from: TableExpression::subquery(query, at),
            cols: occ,
            order: Vec::new(),
        })
    }

    /// The fixpoint's steps as members of its binding, each publishing the
    /// `width` positions the fixpoint's heading holds; a demand cap is the
    /// last member's own row clause.
    fn fix_steps(
        &mut self,
        fix: &Fix,
        scope: ScopeId,
        width: usize,
        outer: &Enclosing,
        cap: Option<i64>,
    ) -> Result<Vec<QueryExpression>> {
        let mut members = Vec::new();
        let last = fix.steps().len().saturating_sub(1);
        for (k, s) in fix.steps().iter().enumerate() {
            let step = Enclosing::of(self.graph, *s, &[outer.entries()], true);
            let (rows, mut values, group, distinct) = self.part(*s, &step)?;
            values.truncate(width);
            let limit = cap.filter(|_| k == last).map(Limit::new);
            members.push(self.arm_limited(scope, rows, values, group, distinct, None, limit)?);
        }
        Ok(members)
    }

    /// The accumulation the core decided.
    pub(super) fn accumulation(&mut self, fix: &Fix) -> Accumulation {
        if fix.deduplicating() {
            Accumulation::Set
        } else {
            Accumulation::Bag
        }
    }

    /// Bind a recursive binding at the statement's head.
    pub(super) fn bind_fixpoint(&mut self, scope: ScopeId, accumulation: Accumulation, anchor: QueryExpression, members: Vec<QueryExpression>) {
        // DECISION(recursive-placement): the recursive binding stands at
        // the statement's head, where every target reads it.
        let cte = self.out.fixpoint(scope, accumulation == Accumulation::Set, anchor, members);
        self.ctes.push(cte);
    }

    /// A relation's rows and its heading's values, written with no stage
    /// above its last step: a recursive member must read the working table
    /// at its own level.
    #[allow(clippy::type_complexity)]
    pub(super) fn part(&mut self, rel: RelId, outer: &Enclosing) -> Result<(Rows, Vec<SqlExpr>, Option<Vec<SqlExpr>>, bool)> {
        let graph = self.graph;
        match graph.rel(rel).kind() {
            RelKind::Pipe { input, op } if !matches!(op, PipeOp::Group { .. } | PipeOp::Distinct(_)) => {
                self.pipe_values(*input, op, outer)
            }
            RelKind::Run(run) => {
                let rows = self.run_rows(rel, outer)?;
                let env = rows.frame.env.clone();
                let values = run
                    .outputs()
                    .iter()
                    .map(|c| self.cell(*c, super::value::At { local: &env, outer }))
                    .collect::<Result<_>>()?;
                Ok((rows, values, None, false))
            }
            RelKind::Apply { instance } => {
                self.part(graph.instance(*instance).body(), outer)
            }
            _ => {
                let table = self.table(rel, outer)?;
                let values = table.cols.iter().map(|c| SqlExpr::Column(*c)).collect();
                let rows = Rows::of(super::frame::Frame {
                    from: Some(table.from),
                    env: Env::fresh(),
                });
                Ok((rows, values, None, false))
            }
        }
    }

    /// One part of a recursive binding, standing at its scope: the anchor
    /// publishes the binding's columns, a member fills them.
    pub(super) fn arm(
        &mut self,
        scope: ScopeId,
        rows: Rows,
        values: Vec<SqlExpr>,
        group: Option<Vec<SqlExpr>>,
        distinct: bool,
        publish: Option<&[ColId]>,
    ) -> Result<QueryExpression> {
        self.arm_limited(scope, rows, values, group, distinct, publish, None)
    }

    /// [`Self::arm`], with the member's own row clause.
    #[allow(clippy::too_many_arguments)]
    fn arm_limited(
        &mut self,
        scope: ScopeId,
        rows: Rows,
        values: Vec<SqlExpr>,
        group: Option<Vec<SqlExpr>>,
        distinct: bool,
        publish: Option<&[ColId]>,
        limit: Option<crate::pipeline::middle::facade::Limit>,
    ) -> Result<QueryExpression> {
        let free = self.free_positions.get(&scope).cloned().unwrap_or_default();
        let items = values
            .into_iter()
            .enumerate()
            .map(|(k, v)| (k, if free.get(k).copied().unwrap_or(false) { super::rel::affinity_free(v) } else { v }))
            .map(|(k, v)| match publish {
                Some(cols) => SelectItem::expression_with_alias(v, cols[k]),
                None => SelectItem::scaffolding_value(v, self.out.scaffolding()),
            })
            .collect();
        self.select_at(scope, items, rows.frame.from.into_iter().collect(), rows.filter, group, Vec::new(), limit, distinct)
    }

    /// A read of a fixpoint's frontier: its working table.
    pub(super) fn frontier_table(&self, frontier: BinderId) -> Result<Table> {
        let f = self
            .frontiers
            .get(&frontier)
            .ok_or_else(|| self.contract("a frontier read outside its fixpoint"))?;
        let mut cols = f.cols.clone();
        cols.extend(f.carried.iter().map(|(_, c)| *c));
        Ok(Table {
            from: TableExpression::Scope(f.scope),
            cols,
            order: Vec::new(),
        })
    }

    /// The carrier columns a frontier carries, as an environment for the
    /// step reading it.
    pub(super) fn carried_env(&self, frontier: BinderId) -> Env {
        let mut env = Env::fresh();
        if let Some(f) = self.frontiers.get(&frontier) {
            for (k, c) in &f.carried {
                env.bind(*k, SqlExpr::Column(*c));
            }
        }
        env
    }
}

/// An anchor's values as its accumulation tells rows apart. A deduplicating
/// fixpoint compares its rows under set identity, and the target compares a
/// compound's rows under the collation of its first arm's columns, so an
/// anchor publishes each value as a set key.
pub(super) fn accumulated(accumulation: Accumulation, values: Vec<SqlExpr>) -> Vec<SqlExpr> {
    match accumulation {
        Accumulation::Set => values.into_iter().map(super::rel::set_key).collect(),
        Accumulation::Bag => values,
    }
}
