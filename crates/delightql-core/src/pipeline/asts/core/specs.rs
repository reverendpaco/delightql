//! Specification types for various operations

use super::{Addressed, DomainExpression, Refined, Resolved, Unresolved};
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use serde::{Deserialize, Serialize};

/// Syntactic containment type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum ContainmentSemantic {
    #[lispy("containment_semantic:bracket")]
    Bracket, // [...]
    #[lispy("containment_semantic:parenthesis")]
    Parenthesis, // (...)
}

/// Modulo operator specifications
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum ModuloSpec<Phase = Unresolved> {
    /// Simple column list for distinct/group
    #[lispy("column_spec:reducing_by")]
    Columns(Vec<DomainExpression<Phase>>),
    /// Complex grouping with aggregations
    #[lispy("modulo_spec:group_by")]
    GroupBy {
        reducing_by: Vec<DomainExpression<Phase>>,
        reducing_on: Vec<DomainExpression<Phase>>,
        arbitrary: Vec<DomainExpression<Phase>>,
    },
}

/// Ordering direction for ORDER BY
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum OrderDirection {
    Ascending,
    Descending,
}

/// Ordering specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
#[lispy("order_spec")]
pub struct OrderingSpec<Phase = Unresolved> {
    pub column: DomainExpression<Phase>,
    pub direction: Option<OrderDirection>,
}

/// Target for renaming - either a literal name or a template
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum RenameTarget {
    /// Literal column name: "foo"
    #[lispy("rename_target:literal")]
    Literal(String),
    /// Column name template: :"{@}_{#}"
    #[lispy("rename_target:template")]
    Template(super::operators::ColumnAlias),
}

/// Rename specification
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
#[lispy("rename")]
pub struct RenameSpec<Phase = Unresolved> {
    pub from: DomainExpression<Phase>,
    pub to: RenameTarget,
}

/// Specification for repositioning a column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, PhaseConvert)]
pub struct RepositionSpec<Phase = Unresolved> {
    pub column: DomainExpression<Phase>,
    pub position: i32,
}

impl<Phase> ToLispy for RepositionSpec<Phase>
where
    DomainExpression<Phase>: ToLispy,
{
    fn to_lispy(&self) -> String {
        format!(
            "(reposition-spec {} {})",
            self.column.to_lispy(),
            self.position
        )
    }
}

/// Row in anonymous table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
#[lispy("row")]
pub struct Row<Phase = Unresolved> {
    pub values: Vec<DomainExpression<Phase>>,
}

/// Tuple ordinal operators for LIMIT/OFFSET
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum TupleOrdinalOperator {
    LessThan,    // #<
    GreaterThan, // #>
    Exactly,     // #=
}

/// Tuple ordinal clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
#[lispy("sigma_clause:tuple_ordinal")]
pub struct TupleOrdinalClause {
    pub operator: TupleOrdinalOperator,
    pub value: i64,
    pub offset: Option<i64>,
}
