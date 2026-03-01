use crate::pipeline::asts::core::expressions::domain::DomainExpression;
use crate::pipeline::asts::core::Unresolved;

/// Kind of generated column (VIRTUAL vs STORED).
#[derive(Debug, Clone, PartialEq)]
pub enum GeneratedKind {
    Virtual,
    Stored,
}

/// A constraint on a column or table (DDL AST layer).
#[derive(Debug, Clone)]
pub enum DdlConstraint<Phase = Unresolved> {
    PrimaryKey { columns: Option<Vec<String>> },
    Unique { columns: Option<Vec<String>> },
    NotNull,
    Check { expr: DomainExpression<Phase> },
    ForeignKey { table: String, columns: Vec<String> },
}

/// A default value specification (DDL AST layer).
#[derive(Debug, Clone)]
pub enum DdlDefault<Phase = Unresolved> {
    Value {
        expr: DomainExpression<Phase>,
    },
    Generated {
        expr: DomainExpression<Phase>,
        kind: GeneratedKind,
    },
}

/// A column definition within a CREATE TABLE.
#[derive(Debug, Clone)]
pub struct ColumnDef<Phase = Unresolved> {
    pub name: String,
    pub col_type: String,
    pub constraints: Vec<DdlConstraint<Phase>>,
    pub default: Option<DdlDefault<Phase>>,
}

/// A complete CREATE TABLE definition (DDL AST layer).
#[derive(Debug, Clone)]
pub struct CreateTableDef<Phase = Unresolved> {
    pub name: String,
    pub temp: bool,
    pub columns: Vec<ColumnDef<Phase>>,
    pub table_constraints: Vec<DdlConstraint<Phase>>,
}
