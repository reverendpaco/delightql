// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A declared table as the companion cells spell it (ddl-grammar.md): each
//! cell parsed at the root its column selects, nothing in it judged.

use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::expressions::truth::TruthExpression;

/// One constraint cell.
#[derive(Debug, Clone)]
pub enum DdlConstraint {
    PrimaryKey {
        columns: Option<Vec<String>>,
    },
    Unique {
        columns: Option<Vec<String>>,
    },
    /// A CHECK's body is a TRUTH — the constraint accepts or rejects a row —
    /// so it is carried as one. A value standing here has no derivation.
    Check {
        expr: TruthExpression,
    },
    ForeignKey {
        table: String,
        columns: Vec<String>,
    },
}

/// One default cell.
#[derive(Debug, Clone)]
pub enum DdlDefault {
    Value { expr: DomainExpression },
}

/// One declared column and the cells written for it.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub col_type: String,
    pub constraints: Vec<DdlConstraint>,
    pub default: Option<DdlDefault>,
}

/// A declared table: its columns, and the cells written for the table as a
/// whole.
#[derive(Debug, Clone)]
pub struct CreateTableDef {
    pub name: String,
    pub temp: bool,
    pub columns: Vec<ColumnDef>,
    pub table_constraints: Vec<DdlConstraint>,
}
