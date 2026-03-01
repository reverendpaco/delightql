//! Boolean expressions (predicates)

use super::super::{Addressed, Refined, Resolved, Unresolved};
use super::domain::DomainExpression;
use super::helpers::{QualifiedName, UsingColumn};
use super::relational::RelationalExpression;
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use delightql_types::SqlIdentifier;
use serde::{Deserialize, Serialize};

/// Predicate expressions for filtering and joining
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum BooleanExpression<Phase = Unresolved> {
    /// Comparison operators (domain → boolean): =, !=, <, >, <=, >=, LIKE, etc.
    #[lispy("domain_expression:predicate:comparison")]
    Comparison {
        operator: String, // Keep as String for now to minimize changes
        left: Box<DomainExpression<Phase>>,
        right: Box<DomainExpression<Phase>>,
    },
    /// Boolean AND operator (boolean → boolean)
    #[lispy("domain_expression:predicate:and")]
    And {
        left: Box<BooleanExpression<Phase>>,
        right: Box<BooleanExpression<Phase>>,
    },
    /// Boolean OR operator (boolean → boolean)
    #[lispy("domain_expression:predicate:or")]
    Or {
        left: Box<BooleanExpression<Phase>>,
        right: Box<BooleanExpression<Phase>>,
    },
    /// Boolean NOT operator (boolean → boolean)
    #[lispy("domain_expression:predicate:not")]
    Not { expr: Box<BooleanExpression<Phase>> },
    /// USING clause: ={ col1, !col2 }
    #[lispy("domain_expression:predicate:using")]
    Using { columns: Vec<UsingColumn> },
    /// Inner EXISTS: +orders(, orders.user_id = users.id)
    #[lispy("domain_expression:predicate:inner_exists")]
    InnerExists {
        /// true for EXISTS (+), false for NOT EXISTS (\+)
        exists: bool,
        /// The table/view identifier
        identifier: QualifiedName,
        /// The subquery (what we're testing for existence)
        subquery: Box<RelationalExpression<Phase>>,
        /// Optional alias
        alias: Option<String>,
        /// USING columns for semi-join correlation: +orders(*.(status))
        /// Empty when explicit conditions are used instead
        using_columns: Vec<String>,
    },
    /// IN operator: col in (val1; val2; val3)
    #[lispy("domain_expression:predicate:in")]
    In {
        /// Value to check (can be single value or tuple)
        value: Box<DomainExpression<Phase>>,
        /// The set of values to check against
        set: Vec<DomainExpression<Phase>>,
        /// Whether this is negated (NOT IN) - for future use
        negated: bool,
    },
    /// IN subquery: col in table(|> (col)) or col not in table(|> (col))
    #[lispy("domain_expression:predicate:in_relational")]
    InRelational {
        /// LHS value to check
        value: Box<DomainExpression<Phase>>,
        /// RHS subquery (inner relation)
        subquery: Box<RelationalExpression<Phase>>,
        /// Table name (for diagnostics)
        identifier: QualifiedName,
        /// Whether this is NOT IN
        negated: bool,
    },
    /// Boolean literal: true or false
    #[lispy("domain_expression:predicate:boolean_literal")]
    BooleanLiteral { value: bool },
    /// Sigma condition: constraint predicates like +like(...)
    #[lispy("domain_expression:predicate:sigma")]
    Sigma {
        condition: Box<super::pipes::SigmaCondition<Phase>>,
    },
    /// Full-tuple named correlation: x.* = y.*
    /// Compares all shared columns by name using IS NOT DISTINCT FROM
    /// Only valid in set operation correlation contexts
    #[lispy("domain_expression:predicate:glob_correlation")]
    GlobCorrelation {
        left: SqlIdentifier,
        right: SqlIdentifier,
    },
    /// Full-tuple positional correlation: x|*| = y|*|
    /// Compares all columns by position using IS NOT DISTINCT FROM
    /// Only valid in set operation correlation contexts
    #[lispy("domain_expression:predicate:ordinal_glob_correlation")]
    OrdinalGlobCorrelation {
        left: SqlIdentifier,
        right: SqlIdentifier,
    },
}
