// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A declared table (ddl-grammar.md): the columns a manifest declares and
//! what each of its cells states, every condition decided over the row the
//! table holds. Each fact stands with the column it is written for; the
//! table's own cells stand with the table.

use super::walk::Child;
use super::CatalogColumn;
use crate::pipeline::middle::core::decide;
use crate::pipeline::middle::core::graph::Builder;
use crate::pipeline::middle::core::heading::form::{self, Formed};
use crate::pipeline::middle::core::heading::{Interior, Name};
use crate::pipeline::middle::core::ids::{BinderId, ExprId, TruthId};
use crate::pipeline::middle::core::refuse::Refusal;

/// A declared table: its name, its extent, the row it holds, and its
/// columns and table-level constraints.
pub(crate) struct Declaration {
    name: Name,
    temporary: bool,
    row: BinderId,
    columns: Vec<DeclaredColumn>,
    table: Vec<TableConstraint>,
}

/// One declared column: its declared type, the value a row that omits it
/// takes, and the constraints written for it.
pub(crate) struct DeclaredColumn {
    name: Name,
    declared: String,
    default: Option<ExprId>,
    constraints: Vec<ColumnConstraint>,
}

/// What a cell written for one column states.
pub(crate) enum ColumnConstraint {
    /// A condition of the row: the table refuses a row on which it is FALSE.
    Check(TruthId),
    /// A key over the row's positions: unique across the table, and present
    /// when primary.
    Key { primary: bool, positions: Vec<u16> },
    /// The column names a row of another table by that table's columns.
    References { table: Name, columns: Vec<Name> },
}

/// What a table-level cell states: a reference names its referencing
/// column only on that column's own row.
pub(crate) enum TableConstraint {
    Check(TruthId),
    Key { primary: bool, positions: Vec<u16> },
}

impl Builder {
    /// The row a declared table holds, known by its declared columns before
    /// the table exists: the occurrence a declaration's conditions read. It
    /// binds no relation.
    pub(crate) fn declared_row(&mut self, columns: &[CatalogColumn]) -> Result<BinderId, Refusal> {
        let names: Vec<Name> = columns.iter().map(|c| c.name.clone()).collect();
        let structures: Vec<Interior> = columns.iter().map(|c| decide::document::declared(c.class, c.stored)).collect();
        let heading = form::form(Formed::Catalog {
            columns: &names,
            structures: &structures,
        })?;
        Ok(self.fresh_binder(super::BinderSite { heading, rel: None }))
    }
}

impl Declaration {
    pub(crate) fn new(
        name: Name,
        temporary: bool,
        row: BinderId,
        columns: Vec<DeclaredColumn>,
        table: Vec<TableConstraint>,
    ) -> Self {
        Declaration {
            name,
            temporary,
            row,
            columns,
            table,
        }
    }

    pub(crate) fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) fn temporary(&self) -> bool {
        self.temporary
    }

    /// The declared row every condition reads.
    pub(crate) fn row(&self) -> BinderId {
        self.row
    }

    pub(crate) fn columns(&self) -> &[DeclaredColumn] {
        &self.columns
    }

    /// The constraints a table-level cell states.
    pub(crate) fn table(&self) -> &[TableConstraint] {
        &self.table
    }

    /// Every condition and default value the declaration holds.
    pub(crate) fn roots(&self) -> Vec<Child> {
        let mut out = Vec::new();
        for column in &self.columns {
            out.extend(column.default.map(Child::Expr));
            out.extend(column.constraints.iter().filter_map(|c| match c {
                ColumnConstraint::Check(t) => Some(Child::Truth(*t)),
                ColumnConstraint::Key { .. } | ColumnConstraint::References { .. } => None,
            }));
        }
        out.extend(self.table.iter().filter_map(|c| match c {
            TableConstraint::Check(t) => Some(Child::Truth(*t)),
            TableConstraint::Key { .. } => None,
        }));
        out
    }
}

impl DeclaredColumn {
    pub(crate) fn new(name: Name, declared: String, default: Option<ExprId>, constraints: Vec<ColumnConstraint>) -> Self {
        DeclaredColumn {
            name,
            declared,
            default,
            constraints,
        }
    }

    pub(crate) fn name(&self) -> &Name {
        &self.name
    }

    pub(crate) fn declared(&self) -> &str {
        &self.declared
    }

    pub(crate) fn default(&self) -> Option<ExprId> {
        self.default
    }

    pub(crate) fn constraints(&self) -> &[ColumnConstraint] {
        &self.constraints
    }
}
