// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A target table function as a FROM item: its arguments as columns the
//! FROM clause holds to its left (the target reads a table function's
//! arguments from the rows beside it), the function under its own name, and
//! its columns as its read's access binds them.

use super::frame::{Enclosing, Env, Frame, Key, Table};
use super::run::{Quals, Placed, Rows};
use super::value::At;
use super::{Realizer, Result};
use crate::pipeline::middle::core::graph::Arena;
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{ExprId, RelId};
use crate::pipeline::middle::core::node::{CatalogColumn, ReadAccess, ReadSource, RelKind, Slot};
use crate::pipeline::middle::facade::{ColId, JoinType, SqlExpr, TableExpression};

impl Realizer<'_, '_> {
    /// The function's rows beside `rows`: each argument read where the rows
    /// stand (a value no column holds is staged as one), the function joined
    /// after them. Answers the rows and the function's selected columns.
    pub(super) fn function_beside(
        &mut self,
        rows: Rows,
        name: &Name,
        args: &[ExprId],
        columns: &[CatalogColumn],
        outer: &Enclosing,
    ) -> Result<(Rows, TableExpression, Vec<ColId>)> {
        let mut computed = Vec::new();
        for a in args {
            if !matches!(self.value(*a, At { local: &rows.frame.env, outer })?, SqlExpr::Column(_)) {
                computed.push(*a);
            }
        }
        let rows = if computed.is_empty() {
            rows
        } else {
            Rows::of(self.stage(rows, &computed, outer)?)
        };
        let frame = &rows.frame;
        let mut arguments = Vec::with_capacity(args.len());
        for a in args {
            match self.value(*a, At { local: &frame.env, outer })? {
                SqlExpr::Column(c) => arguments.push(c),
                _ => return Err(self.contract("a table function argument no column holds")),
            }
        }
        let alias = self.out.scope(Some(name));
        let cols = columns.iter().map(|c| self.out.column(alias, Some(&c.name))).collect();
        let function = self.out.target_function(name, arguments, alias);
        Ok((rows, function, cols))
    }

    /// A table function read on its own (its arguments constants or values
    /// of the enclosing rows).
    pub(super) fn function_table(
        &mut self,
        name: &Name,
        args: &[ExprId],
        columns: &[CatalogColumn],
        outer: &Enclosing,
    ) -> Result<Table> {
        let rows = Rows::of(Frame {
            from: None,
            env: Env::fresh(),
        });
        let (rows, function, cols) = self.function_beside(rows, name, args, columns, outer)?;
        let from = match rows.frame.from {
            None => function,
            Some(left) => TableExpression::Join {
                left: Box::new(left),
                right: Box::new(function),
                join_type: JoinType::Inner,
                join_condition: crate::pipeline::middle::facade::JoinCondition::Cartesian,
            },
        };
        Ok(Table {
            from,
            cols,
            order: Vec::new(),
        })
    }

    /// A table function's columns as its read's access binds them: the
    /// bound slots' columns in slot order, and what its repeated and ground
    /// slots require of each of its rows.
    pub(super) fn function_access(
        &mut self,
        rel: RelId,
        cols: &[ColId],
        access: &ReadAccess,
        env: &Env,
        outer: &Enclosing,
    ) -> Result<(Vec<SqlExpr>, Vec<SqlExpr>)> {
        let column = |k: usize| SqlExpr::Column(cols[k]);
        let every = || (0..cols.len()).map(column).collect::<Vec<_>>();
        match access {
            ReadAccess::All => Ok((every(), Vec::new())),
            ReadAccess::Unasked if self.activation.is_activated(rel) => Ok((every(), Vec::new())),
            ReadAccess::Unasked => Ok((every(), vec![super::rel::impossible()])),
            ReadAccess::Slots(slots) => {
                let (mut bound, mut required) = (Vec::new(), Vec::new());
                for (k, slot) in slots.iter().enumerate() {
                    match slot {
                        Slot::Bind(_) => bound.push(column(k)),
                        Slot::Anon => {}
                        Slot::Reuse { first, class } => required.push(super::rel::equality(column(k), column(*first), *class)),
                        Slot::Constraint { value, class } => {
                            let c = self.value(*value, At { local: env, outer })?;
                            required.push(self.reaffined(*value, c, |c| super::rel::equality(column(k), c, *class)));
                        }
                    }
                }
                Ok((bound, required))
            }
        }
    }

    /// Whether a member's relation is a table function read.
    pub(super) fn is_function(&self, rel: RelId) -> bool {
        matches!(
            self.graph.rel(rel).kind(),
            RelKind::Read {
                source: ReadSource::Function { .. },
                ..
            }
        )
    }

    /// A table function member whose arguments read the members joined
    /// before it: joined after them by its own join, its arguments read in
    /// their rows, its match conditions its join's.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn function_member(
        &mut self,
        rows: Rows,
        q: &Quals<'_>,
        placed: &mut Placed,
        j: usize,
        join: JoinType,
        outer: &Enclosing,
        joined: &[usize],
    ) -> Result<Rows> {
        let m = q.members[j];
        let RelKind::Read {
            source: ReadSource::Function { name, args, columns, .. },
            access,
        } = self.graph.rel(m.rel()).kind()
        else {
            return Err(self.contract("a table function member that reads no function"));
        };
        if !matches!(join, JoinType::Inner | JoinType::Left) {
            return Err(self.uncovered("a table function member preserved by its join"));
        }
        let (name, args, columns, access) = (name.clone(), args.clone(), columns.clone(), access.clone());
        let (rows, function, cols) = self.function_beside(rows, &name, &args, &columns, outer)?;
        let Rows { frame, filter } = rows;
        let mut env = frame.env;
        let b = m.binder();
        let (bound, required) = self.function_access(m.rel(), &cols, &access, &env, outer)?;
        for (i, value) in bound.into_iter().enumerate() {
            env.bind(Key::Col(b, i as u16), value);
        }
        self.bind_merges(&mut env, q, joined, outer)?;
        let mut conds = required;
        for c in placed.take(j) {
            conds.push(self.match_condition(c, &env, outer)?);
        }
        let left = frame.from.ok_or_else(|| self.contract("a table function member with nothing joined before it"))?;
        let join_condition = self.condition(conds, &join);
        Ok(Rows {
            frame: Frame {
                from: Some(TableExpression::Join {
                    left: Box::new(left),
                    right: Box::new(function),
                    join_type: join,
                    join_condition,
                }),
                env,
            },
            filter,
        })
    }
}
