// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A declared table's cells (ddl-grammar.md), elaborated where they stand:
//! in the namespace their companion rules are declared in. A cell is a
//! truth (CHECK) or a value (DEFAULT) over the declared row: its column
//! names are the row's positions, reached as the cell's formals; `@` is the
//! column the cell is written for; every other name in it is selected as
//! any text standing in that namespace is.

use super::{ColumnSelf, Elaborator, Frame};
use crate::pipeline::middle::core::decide::grade::CallPosition;
use crate::pipeline::middle::core::graph::{Finished, Graph};
use crate::pipeline::middle::core::heading::Name;
use crate::pipeline::middle::core::ids::ExprId;
use crate::pipeline::middle::core::node::declaration::{ColumnConstraint, Declaration, DeclaredColumn, TableConstraint};
use crate::pipeline::middle::core::node::{walk, CatalogColumn, Consumer};
use crate::pipeline::middle::core::refuse::{self, Refusal};
use crate::pipeline::middle::core::switches::Switches;
use crate::pipeline::middle::facade::{DdlConstraint, DdlDefault, DeclaredTable, Input};
use crate::pipeline::middle::select::{Standpoint, World};

/// Elaborate a declared table standing at `at` in `world` into a frozen
/// graph whose statement is its declaration.
pub(crate) fn declaration<'s>(
    input: &Input<'s>,
    table: &DeclaredTable,
    world: World<'s>,
    at: Standpoint,
) -> Result<Graph, Refusal> {
    let switches = Switches::default();
    let mut e = Elaborator::new(input, world, at, switches);
    let declared: Vec<CatalogColumn> = table
        .columns
        .iter()
        .map(|c| CatalogColumn {
            name: Name::new(c.name.clone()),
            class: input.declared_class(Some(&c.col_type)),
            declared: Some(c.col_type.clone()),
            computed: Some(false),
            stored: false,
        })
        .collect();
    let row = e.b.declared_row(&declared)?;
    let named: Vec<(Name, ExprId)> = declared
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.clone(), e.b.col(row, i as u16)))
        .collect();
    e.frames.push(Frame {
        marked: None,
        values: Vec::new(),
        relations: Vec::new(),
        named: named.clone(),
        rules: Vec::new(),
        ..Frame::default()
    });
    let mut columns = Vec::with_capacity(table.columns.len());
    for (i, column) in table.columns.iter().enumerate() {
        e.column_self = ColumnSelf::Column(named[i].1);
        let default = match &column.default {
            Some(DdlDefault::Value { expr }) => Some(e.value(expr, CallPosition::Value)?),
            None => None,
        };
        let mut constraints = Vec::with_capacity(column.constraints.len());
        for cell in &column.constraints {
            constraints.push(e.column_constraint(cell, &named, i)?);
        }
        columns.push(DeclaredColumn::new(
            named[i].0.clone(),
            column.col_type.clone(),
            default,
            constraints,
        ));
    }
    e.column_self = ColumnSelf::Table;
    let mut own = Vec::with_capacity(table.table_constraints.len());
    for cell in &table.table_constraints {
        own.push(e.table_constraint(cell, &named)?);
    }
    let declaration = Declaration::new(Name::new(table.name.clone()), table.temp, row, columns, own);
    let roots = declaration.roots();
    if let Some((callee, contradiction)) =
        crate::pipeline::middle::core::node::run::contradicted_call(&e.b, &roots)
    {
        return Err(crate::pipeline::middle::core::decide::admission::grade_refusal(
            &callee,
            contradiction,
        ));
    }
    if !walk::reachable(&e.b, &roots).rels.is_empty() {
        return Err(refuse::outside("a declared table's cell that reads a relation"));
    }
    e.b.finish(Finished::Declaration(declaration), switches)
}

impl Elaborator<'_, '_> {
    /// A cell written for the column at `own`.
    fn column_constraint(
        &mut self,
        cell: &DdlConstraint,
        named: &[(Name, ExprId)],
        own: usize,
    ) -> Result<ColumnConstraint, Refusal> {
        Ok(match cell {
            DdlConstraint::Check { expr } => ColumnConstraint::Check(self.truth(expr, Consumer::Filter)?),
            DdlConstraint::PrimaryKey { columns } | DdlConstraint::Unique { columns } => ColumnConstraint::Key {
                primary: matches!(cell, DdlConstraint::PrimaryKey { .. }),
                positions: match columns {
                    Some(columns) => positions(named, columns)?,
                    None => vec![own as u16],
                },
            },
            DdlConstraint::ForeignKey { table, columns } => ColumnConstraint::References {
                table: Name::new(table.clone()),
                columns: columns.iter().map(|c| Name::new(c.clone())).collect(),
            },
        })
    }

    /// A cell written for the table as a whole.
    fn table_constraint(&mut self, cell: &DdlConstraint, named: &[(Name, ExprId)]) -> Result<TableConstraint, Refusal> {
        Ok(match cell {
            DdlConstraint::Check { expr } => TableConstraint::Check(self.truth(expr, Consumer::Filter)?),
            DdlConstraint::PrimaryKey { columns } | DdlConstraint::Unique { columns } => {
                let primary = matches!(cell, DdlConstraint::PrimaryKey { .. });
                match columns {
                    Some(columns) => TableConstraint::Key {
                        primary,
                        positions: positions(named, columns)?,
                    },
                    None => return Err(refuse::table_key_without_columns(primary)),
                }
            }
            DdlConstraint::ForeignKey { .. } => return Err(refuse::table_foreign_key()),
        })
    }
}

/// The declared row's positions a key names.
fn positions(named: &[(Name, ExprId)], columns: &[String]) -> Result<Vec<u16>, Refusal> {
    columns
        .iter()
        .map(|column| {
            let name = Name::new(column.clone());
            named
                .iter()
                .position(|(n, _)| *n == name)
                .map(|p| p as u16)
                .ok_or_else(|| refuse::column(column, "the declared table declares no such column"))
        })
        .collect()
}
