// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A declaration's SQL: the CREATE text of the declared table. The declared
//! row's positions are the created table's columns, which a column
//! definition reads unqualified; every condition and default is spelled by
//! the ordinary value and truth realization.

use super::frame::{Enclosing, Env, Key};
use super::value::{At, Consumer};
use super::{Realizer, Result};
use crate::pipeline::middle::api::Window;
use crate::pipeline::middle::core::graph::{Arena, Statement};
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::{BinderId, TruthId};
use crate::pipeline::middle::core::node::declaration::{ColumnConstraint, Declaration, TableConstraint};
use crate::pipeline::middle::core::ids::ExprId;
use crate::pipeline::middle::core::node::{EqClass, ExprKind, TruthKind};
use crate::pipeline::middle::facade::{
    CmpOp, ColId, LiteralValue, ScopeId, SqlColumnDef, SqlCreateTable, SqlDefaultClause, SqlExpr, SqlTableConstraint,
};

impl Window<'_, '_> {
    /// The declared table's CREATE text for the window's target.
    pub(in crate::pipeline::middle) fn declared_table(&self) -> Result<String> {
        let Statement::Declaration(declaration) = self.graph.statement() else {
            return Err(super::super::core::refuse::contract("a declared table whose graph holds no declaration"));
        };
        let mut realizer = Realizer::new(self.graph, self.input.output(self.names));
        realizer.declaration(declaration)
    }
}

/// The tables a declaration names, each once: the declared table first,
/// then each table a reference names, with the columns written for each.
struct Named {
    tables: Vec<(Name, ScopeId, Vec<(Name, ColId)>)>,
}

impl Realizer<'_, '_> {
    fn declaration(&mut self, d: &Declaration) -> Result<String> {
        let table = self.out.scope(Some(d.name()));
        let cols: Vec<ColId> = d.columns().iter().map(|c| self.out.column(table, Some(c.name()))).collect();
        let mut named = Named {
            tables: vec![(
                d.name().clone(),
                table,
                d.columns().iter().map(|c| c.name().clone()).zip(cols.iter().copied()).collect(),
            )],
        };
        let mut row = Env::fresh();
        for (i, col) in cols.iter().enumerate() {
            row.bind(Key::Col(d.row(), i as u16), SqlExpr::Column(*col));
        }
        let outer = Enclosing::none();
        let at = At {
            local: &row,
            outer: &outer,
        };
        let mut defs = Vec::with_capacity(cols.len());
        let mut table_constraints = Vec::new();
        for (i, column) in d.columns().iter().enumerate() {
            let mut def = SqlColumnDef {
                column: cols[i],
                col_type: column.declared().to_string(),
                not_null: false,
                primary_key: false,
                unique: false,
                checks: Vec::new(),
                default: match column.default() {
                    Some(value) => Some(SqlDefaultClause::Expression(self.value(value, at)?)),
                    None => None,
                },
            };
            for constraint in column.constraints() {
                match constraint {
                    // DECISION(strategy): a column's condition that is exactly
                    // its own null-distinctness is the target's NOT NULL.
                    ColumnConstraint::Check(t) if self.null_distinct(*t, d.row(), i as u16) => def.not_null = true,
                    ColumnConstraint::Check(t) => def.checks.push(self.truth(*t, Consumer::Filter, at)?),
                    ColumnConstraint::Key { primary, positions } if positions.as_slice() == [i as u16] => {
                        if *primary {
                            def.primary_key = true;
                        } else {
                            def.unique = true;
                        }
                    }
                    ColumnConstraint::Key { primary, positions } => {
                        table_constraints.push(key(*primary, positions, &cols));
                    }
                    ColumnConstraint::References { table, columns } => {
                        let (ref_table, ref_columns) = self.referenced(&mut named, table, columns);
                        table_constraints.push(SqlTableConstraint::ForeignKey {
                            columns: vec![cols[i]],
                            ref_table,
                            ref_columns,
                        });
                    }
                }
            }
            defs.push(def);
        }
        for constraint in d.table() {
            table_constraints.push(match constraint {
                TableConstraint::Check(t) => SqlTableConstraint::Check {
                    expr: self.truth(*t, Consumer::Filter, at)?,
                },
                TableConstraint::Key { primary, positions } => key(*primary, positions, &cols),
            });
        }
        self.out.create_table(&SqlCreateTable {
            table,
            temp: d.temporary(),
            columns: defs,
            table_constraints,
        })
    }

    /// A referenced table's scope and the columns written for it, each named
    /// once across the declaration.
    fn referenced(&self, named: &mut Named, table: &Name, columns: &[Name]) -> (ScopeId, Vec<ColId>) {
        let at = match named.tables.iter().position(|(n, _, _)| n == table) {
            Some(at) => at,
            None => {
                named.tables.push((table.clone(), self.out.scope(Some(table)), Vec::new()));
                named.tables.len() - 1
            }
        };
        let mut out = Vec::with_capacity(columns.len());
        for column in columns {
            let (_, scope, written) = &mut named.tables[at];
            let col = match written.iter().find(|(n, _)| n == column) {
                Some((_, col)) => *col,
                None => {
                    let col = self.out.column(*scope, Some(column));
                    written.push((column.clone(), col));
                    col
                }
            };
            out.push(col);
        }
        (named.tables[at].1, out)
    }

    /// Whether a truth is exactly the null-safe distinctness of the row's
    /// position `i` from NULL.
    fn null_distinct(&self, t: TruthId, row: BinderId, i: u16) -> bool {
        let graph = self.graph;
        let TruthKind::Cmp {
            op: CmpOp::NullSafeNotEqual,
            left,
            right,
            class: EqClass::NullSafe,
            ..
        } = graph.truth(t).kind()
        else {
            return false;
        };
        let is = |e: ExprId, want_col: bool| match graph.expr(e).kind() {
            ExprKind::Col(b, p) => want_col && *b == row && *p == i,
            ExprKind::Const(LiteralValue::Null) => !want_col,
            _ => false,
        };
        (is(*left, true) && is(*right, false)) || (is(*left, false) && is(*right, true))
    }
}

/// A key the table states over the columns at `positions`.
fn key(primary: bool, positions: &[u16], cols: &[ColId]) -> SqlTableConstraint {
    let columns = positions.iter().map(|p| cols[*p as usize]).collect();
    if primary {
        SqlTableConstraint::PrimaryKey { columns }
    } else {
        SqlTableConstraint::Unique { columns }
    }
}
