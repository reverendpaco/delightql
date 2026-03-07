//! Domain expressions and domain specifications
//! DomainExpression, DomainSpec

use super::super::metadata::NamespacePath;
use super::super::{
    Addressed, ColumnOrdinal, ColumnRange, LiteralValue, PhaseBox, Refined, Resolved, Unresolved,
};
use super::boolean::BooleanExpression;
use super::functions::FunctionExpression;
use super::helpers::QualifiedName;
use super::relational::RelationalExpression;
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use delightql_types::SqlIdentifier;
use serde::{Deserialize, Serialize};

/// CFE substitution targets: parameter references that should be replaced
/// before transformation. If these survive to SQL generation, it's an error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum SubstitutionExpr {
    /// CFE parameter reference
    #[lispy("substitution_expr:parameter")]
    Parameter { name: String, alias: Option<String> },
    /// HOCFE curried parameter reference
    #[lispy("substitution_expr:curried_parameter")]
    CurriedParameter { name: String, alias: Option<String> },
    /// CCAFE context parameter reference
    #[lispy("substitution_expr:context_parameter")]
    ContextParameter { name: String, alias: Option<String> },
    /// CCAFE context marker (..)
    #[lispy("substitution_expr:context_marker")]
    ContextMarker,
}

/// Projection-only expressions: valid in SELECT/GROUP BY contexts,
/// error or pass-through in single-value contexts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum ProjectionExpr<Phase = Unresolved> {
    /// Glob (*) with optional qualification
    #[lispy("projection_expr:glob")]
    Glob {
        qualifier: Option<SqlIdentifier>,
        namespace_path: NamespacePath,
    },
    /// Range of column ordinals
    #[lispy("projection_expr:column_range")]
    #[phase_convert(phantom)]
    ColumnRange(PhaseBox<ColumnRange, Phase>),
    /// Pattern for column selection: /_name$/, /^user/
    #[lispy("projection_expr:pattern")]
    Pattern {
        pattern: String,
        alias: Option<SqlIdentifier>,
    },
    /// JSON path literal: .name, .scripts.dev, .items.0.name
    #[lispy("projection_expr:json_path_literal")]
    JsonPathLiteral {
        segments: Vec<super::functions::PathSegment>,
        root_is_array: bool,
        alias: Option<SqlIdentifier>,
    },
}

/// Tracks the source/origin of an Lvar to distinguish real table columns from CFE holes
///
/// During CFE precompilation, parameters and context variables are represented as fake
/// columns from fake tables (__cfe_params__, __cfe_context__, etc). After resolution
/// validates these, refinement preserves this provenance information so postprocessing
/// can convert the appropriate Lvars to Parameter/ContextParameter nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LvarProvenance {
    /// From a real table in the database schema
    RealTable { table: String },
    /// From the __cfe_params__ fake table (regular parameter)
    CfeParameter,
    /// From the __cfe_curried_params__ fake table (curried parameter)
    CfeCurriedParameter,
    /// From the __cfe_context__ fake table (context parameter)
    CfeContext,
}

impl ToLispy for LvarProvenance {
    fn to_lispy(&self) -> String {
        match self {
            LvarProvenance::RealTable { table } => format!("(provenance:real-table {})", table),
            LvarProvenance::CfeParameter => "(provenance:cfe-parameter)".to_string(),
            LvarProvenance::CfeCurriedParameter => "(provenance:cfe-curried-parameter)".to_string(),
            LvarProvenance::CfeContext => "(provenance:cfe-context)".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum DomainExpression<Phase = Unresolved> {
    /// Column reference with optional namespace qualification
    ///
    /// # Field Semantics (CRITICAL)
    ///
    /// - `name`: Column name (e.g., "last_name")
    /// - `qualifier`: Table/alias reference (e.g., "u" in `u.last_name`)
    ///   - This identifies WHICH table's column (could be alias!)
    ///   - NOT part of namespace path - it's a table reference
    /// - `namespace_path`: WHERE to find the table (catalog/schema levels)
    ///   - In `catalog.schema.table.column`: namespace_path = [schema, catalog]
    ///   - Separate from qualifier because qualifier can be an alias
    ///
    /// Example: `prod.dbo.users.name as u` then `u.email` later
    /// - First reference: namespace_path=[dbo, prod], qualifier=Some("users"), name="name"
    /// - Second reference: namespace_path=[], qualifier=Some("u"), name="email"
    #[lispy("domain_expression:lvar")]
    Lvar {
        name: SqlIdentifier,
        /// Table qualifier/reference (table name or alias)
        qualifier: Option<SqlIdentifier>,
        /// Namespace path (WHERE to find table: schema, database, catalog, etc.)
        namespace_path: NamespacePath,
        alias: Option<SqlIdentifier>,
        #[serde(skip)]
        provenance: PhaseBox<Option<LvarProvenance>, Refined>,
    },
    #[lispy("domain_expression:literal")]
    Literal {
        value: LiteralValue,
        alias: Option<SqlIdentifier>,
    },
    /// Projection-only expressions (Glob, ColumnRange, Pattern, JsonPathLiteral)
    #[lispy("domain_expression:projection")]
    Projection(ProjectionExpr<Phase>),
    NonUnifiyingUnderscore,
    /// @ placeholder for value in transforms and lambdas
    #[lispy("domain_expression:value_placeholder")]
    ValuePlaceholder {
        alias: Option<SqlIdentifier>,
    },
    /// CFE substitution targets (Parameter, CurriedParameter, ContextParameter, ContextMarker)
    ///
    /// Lifecycle: Created during CFE precompilation. Survives through ALL phases including
    /// Addressed — CFE bodies retain Substitution nodes until the transformer expands them
    /// at call sites via substitute_cfe_parameters(). After substitution, no Substitution
    /// nodes should remain in expressions that reach SQL generation.
    #[lispy("domain_expression:substitution")]
    Substitution(SubstitutionExpr),
    Function(FunctionExpression<Phase>),
    /// Boolean expression used as a domain value in projection context
    ///
    /// This variant represents the SQL semantic where boolean expressions become
    /// domain values when used in SELECT clauses. For example:
    /// `SELECT age > 30 FROM users` returns a boolean column (0/1 or true/false).
    ///
    /// This wrapping is NOT a hack - it accurately models SQL's type conversion
    /// from predicate to value in projection contexts. The boolean expression
    /// retains its structure but is treated as a value that can be aliased and
    /// included in result sets.
    ///
    /// Note: This is different from the old predicate-in-predicate issue where
    /// AND/OR operators incorrectly expected domain expressions. That has been
    /// fixed with proper BooleanExpression::And/Or variants.
    Predicate {
        expr: Box<BooleanExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Value piped through transformations: (expr /-> func1 /-> func2)
    #[lispy("domain_expression:piped")]
    PipedExpression {
        value: Box<DomainExpression<Phase>>,
        transforms: Vec<FunctionExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Parenthesized expression - preserves user's explicit parentheses
    ///
    /// We preserve ALL user parentheses exactly as written because:
    /// 1. User intent: They added parentheses for a reason (clarity, emphasis)
    /// 2. No implicit precedence: We don't rely on "tribal knowledge" of operator precedence
    /// 3. SQL compatibility: Generated SQL matches the source more closely for debugging
    /// 4. Explicit is better than implicit: Makes expression evaluation order crystal clear
    ///
    /// Note: This may generate redundant parentheses like ((age)) but that's intentional -
    /// we prioritize preserving user intent over optimization.
    #[lispy("domain_expression:parenthesized")]
    Parenthesized {
        inner: Box<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Tuple constructor for multi-column patterns
    /// Example: (age, status) in multi-column IN expressions
    /// EPOCH 5: Added for tuple IN support
    #[lispy("domain_expression:tuple")]
    Tuple {
        elements: Vec<DomainExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    #[lispy("domain_expression:column_ordinal")]
    #[phase_convert(phantom, unreachable_after(Refined))]
    ColumnOrdinal(PhaseBox<ColumnOrdinal, Phase>),
    /// Scalar subquery: relation:(inner-cpr)
    /// Returns a single scalar value from a subquery
    /// Example: orders:(, o.user_id = u.id ~> sum:(total))
    #[lispy("domain_expression:scalar_subquery")]
    ScalarSubquery {
        identifier: QualifiedName,
        subquery: Box<RelationalExpression<Phase>>,
        alias: Option<SqlIdentifier>,
    },
    /// Pivot expression: value_col of pivot_key
    /// Used inside %() GROUP BY to rotate row values into columns
    /// Example: score of subject → generates one column per IN-predicate value
    /// pivot_values is empty at parse/unresolved time, populated by resolver from IN predicate
    #[lispy("domain_expression:pivot_of")]
    PivotOf {
        value_column: Box<DomainExpression<Phase>>,
        pivot_key: Box<DomainExpression<Phase>>,
        /// Populated by resolver: literal values from the IN predicate
        pivot_values: Vec<String>,
    },
}

/// Domain specification for tables (column selections)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum DomainSpec<Phase = Unresolved> {
    /// Glob pattern: * - qualified column names
    #[lispy("domain_spec:glob")]
    Glob,
    /// Glob with inline USING: *{col1, col2}
    #[lispy("domain_spec:glob_with_using")]
    GlobWithUsing(Vec<String>),
    /// Glob with USING all shared columns: .* or *.*
    #[lispy("domain_spec:glob_with_using_all")]
    GlobWithUsingAll,
    /// Positional/explicit columns: (id, name)
    #[lispy("domain_spec:positional")]
    Positional(Vec<DomainExpression<Phase>>),
    /// Bare/empty parens: () - unqualified column names (natural join candidate)
    #[lispy("domain_spec:bare")]
    Bare,
}
