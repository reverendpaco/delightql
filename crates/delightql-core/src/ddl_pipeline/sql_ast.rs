// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::sql_ast::DomainExpression as SqlExpression;

use super::asts::GeneratedKind;

/// SQL-layer CREATE TABLE definition.
#[derive(Debug, Clone)]
pub struct SqlCreateTable {
    pub table: crate::names::ScopeId,
    pub temp: bool,
    pub columns: Vec<SqlColumnDef>,
    pub table_constraints: Vec<SqlTableConstraint>,
}

/// SQL-layer column definition.
#[derive(Debug, Clone)]
pub struct SqlColumnDef {
    pub column: crate::names::ColId,
    pub col_type: String,
    pub not_null: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub checks: Vec<SqlExpression>,
    pub default: Option<SqlDefaultClause>,
}

/// SQL-layer DEFAULT clause.
#[derive(Debug, Clone)]
pub enum SqlDefaultClause {
    Expression(SqlExpression),
    Generated {
        expr: SqlExpression,
        kind: GeneratedKind,
    },
}

/// SQL-layer table constraint.
#[derive(Debug, Clone)]
pub enum SqlTableConstraint {
    PrimaryKey {
        columns: Vec<crate::names::ColId>,
    },
    Unique {
        columns: Vec<crate::names::ColId>,
    },
    Check {
        expr: SqlExpression,
    },
    ForeignKey {
        columns: Vec<crate::names::ColId>,
        ref_table: crate::names::ScopeId,
        ref_columns: Vec<crate::names::ColId>,
    },
}
