use crate::pipeline::sql_ast_v3::DomainExpression as SqlExpression;

use super::asts::GeneratedKind;

/// SQL-layer CREATE TABLE definition.
#[derive(Debug, Clone)]
pub struct SqlCreateTable {
    pub name: String,
    pub temp: bool,
    pub columns: Vec<SqlColumnDef>,
    pub table_constraints: Vec<SqlTableConstraint>,
}

/// SQL-layer column definition.
#[derive(Debug, Clone)]
pub struct SqlColumnDef {
    pub name: String,
    pub col_type: String,
    pub not_null: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub check: Option<SqlExpression>,
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
        _name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        _name: Option<String>,
        columns: Vec<String>,
    },
    Check {
        _name: Option<String>,
        expr: SqlExpression,
    },
    ForeignKey {
        _name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
}
