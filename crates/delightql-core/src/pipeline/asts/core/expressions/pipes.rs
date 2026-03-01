//! Pipe expressions and sigma conditions

use super::super::{
    Addressed, CprSchema, PhaseBox, Refined, Resolved, TupleOrdinalClause, UnaryRelationalOperator,
    Unresolved,
};
use super::boolean::BooleanExpression;
use super::domain::DomainExpression;
use super::functions::FunctionExpression;
use super::relational::RelationalExpression;
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use serde::{Deserialize, Serialize};

/// Mapping from JSON key to output column name
/// Used in destructuring to support renaming: {"json_key": column_name}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DestructureMapping {
    /// Key in the JSON object (used in json_extract path)
    pub json_key: String,
    /// Name of the column in the result (used in AS alias)
    pub column_name: String,
}

impl ToLispy for DestructureMapping {
    fn to_lispy(&self) -> String {
        format!("(mapping {} {})", self.json_key, self.column_name)
    }
}

/// Destructuring operation mode
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DestructureMode {
    /// Scalar: ~= {pattern}
    /// Generates: json_extract(col, '$.field') for each field
    /// No row explosion, NULL for missing keys
    Scalar,

    /// Aggregate: ~= ~> {pattern}
    /// Generates: LEFT JOIN json_each(col)
    /// Row explosion, preserves rows with NULLs for empty/null arrays
    Aggregate,
}

impl ToLispy for DestructureMode {
    fn to_lispy(&self) -> String {
        match self {
            DestructureMode::Scalar => "scalar".to_string(),
            DestructureMode::Aggregate => "aggregate".to_string(),
        }
    }
}

/// Pipe transformation: relation |> operator
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
#[lispy("pipe")]
pub struct PipeExpression<Phase = Unresolved> {
    pub source: RelationalExpression<Phase>,
    pub operator: UnaryRelationalOperator<Phase>,
    // PhaseBox enforces compile-time schema access patterns
    pub cpr_schema: PhaseBox<CprSchema, Phase>,
}

/// Conditions in sigma expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum SigmaCondition<Phase = Unresolved> {
    /// Boolean predicate (WHERE/ON/HAVING conditions)
    Predicate(BooleanExpression<Phase>),
    /// Tuple ordinal: #<5, #>10
    TupleOrdinal(TupleOrdinalClause),
    /// Tree group destructuring: json_col ~= ~> {pattern}
    /// Extracts columns from JSON using tree group pattern
    #[lispy("sigma_condition:destructure")]
    Destructure {
        /// Source JSON column
        json_column: Box<DomainExpression<Phase>>,
        /// Destructuring pattern (must be Curly function)
        pattern: Box<FunctionExpression<Phase>>,
        /// Destructuring mode (strict vs permissive, scalar vs aggregate)
        mode: DestructureMode,
        /// Schema of columns produced by destructuring
        /// - Unresolved: empty/phantom
        /// - Resolved: filled with JSON key → column name mappings
        /// - Refined: preserved from resolved
        destructured_schema: PhaseBox<Vec<DestructureMapping>, Phase>,
    },
    /// Sigma predicate call: +like(arg1, arg2) or \+like(arg1, arg2)
    /// Constraint predicates that represent conceptually infinite relations
    #[lispy("sigma_condition:sigma_call")]
    SigmaCall {
        /// Functor name (e.g., "like", "=", "<")
        functor: String,
        /// Arguments to the predicate
        arguments: Vec<DomainExpression<Phase>>,
        /// True for EXISTS (+), false for NOT EXISTS (\+)
        exists: bool,
    },
}
