//! Relational expressions and base relations
//! RelationalExpression, Relation, InnerRelationPattern

use super::super::metadata::{GroundedPath, NamespacePath};
use super::super::{Addressed, CprSchema, JoinType, PhaseBox, Refined, Resolved, Row, Unresolved};
use super::boolean::BooleanExpression;
use super::domain::{DomainExpression, DomainSpec};
use super::helpers::QualifiedName;
use super::metadata_types::SetOperator;
use super::pipes::{PipeExpression, SigmaCondition};
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use delightql_types::SqlIdentifier;
use serde::{Deserialize, Serialize};

use super::metadata_types::FilterOrigin;

/// Any expression that produces a relation
#[derive(Debug, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
#[phase_convert(only(Refined => Addressed))]
pub enum RelationalExpression<Phase = Unresolved> {
    /// Base relation (table/view or anonymous)
    Relation(Relation<Phase>),
    /// Direct join between relations
    Join {
        left: Box<RelationalExpression<Phase>>,
        right: Box<RelationalExpression<Phase>>,
        // Optional fields for resolved/refined phases
        join_condition: Option<BooleanExpression<Phase>>,
        join_type: Option<JoinType>,
        cpr_schema: PhaseBox<CprSchema, Phase>,
    },
    /// Filter/WHERE condition
    Filter {
        source: Box<RelationalExpression<Phase>>,
        condition: SigmaCondition<Phase>,
        origin: FilterOrigin,
        cpr_schema: PhaseBox<CprSchema, Phase>,
    },
    /// Pipe transformation
    Pipe(Box<stacksafe::StackSafe<PipeExpression<Phase>>>),
    /// Set operations (UNION, INTERSECT, etc.)
    ///
    /// SetOperation is a universal relational algebra operation that can appear in any phase:
    /// - Unresolved: Created by parser from |;| syntax
    /// - Resolved: Also created by resolver when merging duplicate CTEs
    /// - Refined: Passed through or optimized (e.g., merging adjacent unions)
    ///
    /// This is NOT a phase-specific construct - it represents a fundamental relational
    /// operation that can originate from syntax or be synthesized during compilation.
    SetOperation {
        operator: SetOperator,
        operands: Vec<RelationalExpression<Phase>>,
        // Correlation predicates for INTERSECT ON semantics (only settable in refiner)
        correlation: PhaseBox<Option<BooleanExpression<Phase>>, Phase>,
        cpr_schema: PhaseBox<CprSchema, Phase>,
    },
    /// ER-context join chain: A(*) & B(*) & C(*)
    /// Unresolved-only — resolver expands into standard Joins.
    #[lispy("er_join_chain")]
    #[phase_convert(unreachable)]
    ErJoinChain {
        /// Relations in the chain, left-to-right. Always >= 2 elements.
        relations: Vec<Relation<Phase>>,
    },
    /// ER-context transitive join: A(*) && B(*)
    /// Unresolved-only — resolver expands via graph path-finding.
    #[lispy("er_transitive_join")]
    #[phase_convert(unreachable)]
    ErTransitiveJoin {
        left: Box<RelationalExpression<Phase>>,
        right: Box<RelationalExpression<Phase>>,
    },
}

// Manual Clone: uses #[stacksafe] to prevent stack overflow on deeply nested ASTs.
// The derived Clone recurses through Box<RelationalExpression> → Pipe → source → ... which
// overflows on spawned threads (8 MB default stack) with deep pipe chains.
impl<Phase: Clone> Clone for RelationalExpression<Phase> {
    #[stacksafe::stacksafe]
    fn clone(&self) -> Self {
        self.clone_fields()
    }
}

impl<Phase: Clone> RelationalExpression<Phase> {
    fn clone_fields(&self) -> Self {
        match self {
            Self::Relation(r) => Self::Relation(r.clone()),
            Self::Join {
                left,
                right,
                join_condition,
                join_type,
                cpr_schema,
            } => Self::Join {
                left: left.clone(),
                right: right.clone(),
                join_condition: join_condition.clone(),
                join_type: join_type.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Self::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            } => Self::Filter {
                source: source.clone(),
                condition: condition.clone(),
                origin: origin.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Self::Pipe(pipe) => Self::Pipe(pipe.clone()),
            Self::SetOperation {
                operator,
                operands,
                correlation,
                cpr_schema,
            } => Self::SetOperation {
                operator: operator.clone(),
                operands: operands.clone(),
                correlation: correlation.clone(),
                cpr_schema: cpr_schema.clone(),
            },
            Self::ErJoinChain { relations } => Self::ErJoinChain {
                relations: relations.clone(),
            },
            Self::ErTransitiveJoin { left, right } => Self::ErTransitiveJoin {
                left: left.clone(),
                right: right.clone(),
            },
        }
    }
}

// NOTE: No manual Drop impl needed. The Pipe variant wraps PipeExpression in StackSafe<T>,
// which provides a #[stacksafe]-annotated Drop impl. This breaks the drop recursion chain
// (Pipe → StackSafe<PipeExpression>.source → Pipe → ...) by inserting stacker::maybe_grow
// at each level, preventing stack overflow during drop of deep pipe chains.

// Phase conversion for RelationalExpression
impl From<RelationalExpression<Resolved>> for RelationalExpression<Refined> {
    #[stacksafe::stacksafe]
    fn from(expr: RelationalExpression<Resolved>) -> RelationalExpression<Refined> {
        match expr {
            RelationalExpression::Relation(rel) => RelationalExpression::Relation(rel.into()),
            RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            } => RelationalExpression::Filter {
                source: Box::new((*source).into()),
                condition: condition.into(),
                origin,
                cpr_schema: cpr_schema.into(),
            },
            RelationalExpression::Join {
                left,
                right,
                join_condition,
                join_type,
                cpr_schema,
            } => RelationalExpression::Join {
                left: Box::new((*left).into()),
                right: Box::new((*right).into()),
                join_condition: join_condition.map(Into::into),
                join_type: Some(join_type.unwrap_or(JoinType::Inner)), // Ensure join_type is always Some
                cpr_schema: cpr_schema.into(),
            },
            RelationalExpression::Pipe(pipe) => RelationalExpression::Pipe(Box::new(
                stacksafe::StackSafe::new((*pipe).into_inner().into()),
            )),
            RelationalExpression::SetOperation {
                operator,
                operands,
                correlation,
                cpr_schema,
            } => RelationalExpression::SetOperation {
                operator,
                operands: operands.into_iter().map(Into::into).collect(),
                correlation: correlation.into(),
                cpr_schema: cpr_schema.into(),
            },
            RelationalExpression::ErJoinChain { .. }
            | RelationalExpression::ErTransitiveJoin { .. } => {
                panic!(
                    "INTERNAL ERROR: ER-join expression found in Resolved phase. \
                     Must be consumed by resolver."
                )
            }
        }
    }
}

/// Semantic patterns for INNER-RELATION
/// These capture the distinct compilation strategies for derived tables
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum InnerRelationPattern<Phase = Unresolved> {
    /// Indeterminate: Builder couldn't determine pattern yet
    /// Will be classified by refiner based on subquery structure
    #[lispy("pattern:indeterminate")]
    Indeterminate {
        identifier: QualifiedName,
        subquery: Box<RelationalExpression<Phase>>,
    },

    /// UDT: Uncorrelated Derived Table
    /// Simple projection/transformation with no correlation to outer query
    /// Compiles to: (SELECT ... FROM table) AS derived
    #[lispy("pattern:udt")]
    UncorrelatedDerivedTable {
        identifier: QualifiedName,
        subquery: Box<RelationalExpression<Phase>>,
        /// Whether this UDT wraps a consulted view (vs a regular table(|> pipeline)).
        /// When true and option://generation/rule/inlining/view is ON, the transformer
        /// lifts this to a CTE instead of inlining as a subquery.
        #[serde(default)]
        is_consulted_view: bool,
    },

    /// CDT-SJ: Correlated Derived Table - Scalar Join
    /// Has correlation predicate, no aggregation, no LIMIT
    /// Compiles to: JOIN with correlation predicate hoisted to ON clause
    #[lispy("pattern:cdt-sj")]
    CorrelatedScalarJoin {
        identifier: QualifiedName,
        correlation_filters: Vec<BooleanExpression<Phase>>,
        subquery: Box<RelationalExpression<Phase>>,
        /// Hygienic column injections: (original_column_name, hygienic_alias)
        /// Only present in Refined phase after rebuilder processes it
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        hygienic_injections: Vec<(String, String)>,
    },

    /// CDT-GJ: Correlated Derived Table - Group Join
    /// Has correlation + aggregation
    /// Compiles to: JOIN with GROUP BY on correlation key
    #[lispy("pattern:cdt-gj")]
    CorrelatedGroupJoin {
        identifier: QualifiedName,
        correlation_filters: Vec<BooleanExpression<Phase>>,
        aggregations: Vec<DomainExpression<Phase>>,
        subquery: Box<RelationalExpression<Phase>>,
        /// Hygienic column injections: (original_column_name, hygienic_alias)
        /// Only present in Refined phase after rebuilder processes it
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        hygienic_injections: Vec<(String, String)>,
    },

    /// CDT-WJ: Correlated Derived Table - Window Join
    /// Has correlation + LIMIT (with optional ORDER BY)
    /// Compiles to: CTE with ROW_NUMBER() OVER (PARTITION BY correlation_key ...)
    #[lispy("pattern:cdt-wj")]
    CorrelatedWindowJoin {
        identifier: QualifiedName,
        correlation_filters: Vec<BooleanExpression<Phase>>,
        order_by: Vec<DomainExpression<Phase>>,
        limit: Option<i64>,
        subquery: Box<RelationalExpression<Phase>>,
    },
}

/// Base relations - sources of data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum Relation<Phase = Unresolved> {
    /// Named table/view: users(*), orders(id, total)
    #[lispy("relation:ground")]
    Ground {
        identifier: QualifiedName,
        /// Canonical entity name from bootstrap (what the DB stores).
        /// Only accessible in Resolved/Refined phases via PhaseBox.
        /// Used at SQL generation boundary; identifier.name (user-typed) used for error messages.
        canonical_name: PhaseBox<Option<SqlIdentifier>, Phase>,
        domain_spec: DomainSpec<Phase>,
        alias: Option<SqlIdentifier>,
        outer: bool,
        /// DML mutation target marker: `!!` on source relation
        mutation_target: bool,
        /// Passthrough: skip entity catalog, use schema introspector directly.
        /// Syntax: `ns/raw_table(*)` — slash separates namespace from raw backend table name.
        #[serde(default)]
        passthrough: bool,
        cpr_schema: PhaseBox<CprSchema, Phase>,
        /// Hygienic column injections for positional literal/expression constraints
        /// (original_column_name, hygienic_alias)
        /// Populated by transformer when building positional patterns with constraints
        #[serde(skip_serializing_if = "Vec::is_empty", default)]
        hygienic_injections: Vec<(String, String)>,
    },
    #[lispy("relation:anonymous")]
    Anonymous {
        column_headers: Option<Vec<DomainExpression<Phase>>>,
        rows: Vec<Row<Phase>>,
        alias: Option<SqlIdentifier>,
        outer: bool,
        /// EXISTS mode: true = +_(...) (filtering/semi-join), false = _(...) (cartesian/melt)
        exists_mode: bool,
        /// Schema conformance target: `_(cols @ data) qua target_table`
        /// Only present in Unresolved phase; resolver consumes it and sets to None.
        #[serde(skip_serializing_if = "Option::is_none", default)]
        qua_target: Option<SqlIdentifier>,
        cpr_schema: PhaseBox<CprSchema, Phase>,
    },
    /// Table-Valued Function: json_each(...), pragma_table_info(...)
    /// Also used for higher-order view invocations: active_only(users)(*)
    #[lispy("relation:tvf")]
    TVF {
        function: SqlIdentifier,
        /// Structured &-separated groups from HO call site (when present).
        #[serde(skip_serializing_if = "Option::is_none", default)]
        argument_groups: Option<Vec<super::super::operators::HoCallGroup>>,
        /// Rich HO argument list — single source of truth for TVF arguments.
        /// Table args carry full relational expressions (preserving interior filters,
        /// projections, pipes). Scalar args carry domain expressions.
        #[serde(skip_serializing_if = "Vec::is_empty", default = "Vec::new")]
        ho_arguments: Vec<super::super::operators::HoArgument<Phase>>,
        domain_spec: DomainSpec<Phase>,
        alias: Option<SqlIdentifier>,
        /// Namespace qualification for namespace-qualified TVFs / HO view invocations
        #[serde(skip_serializing_if = "Option::is_none", default)]
        namespace: Option<NamespacePath>,
        /// Grounding context for grounded HO view invocations (e.g., data::test^lib::ho.active_only)
        #[serde(skip_serializing_if = "Option::is_none", default)]
        grounding: Option<GroundedPath>,
        cpr_schema: PhaseBox<CprSchema, Phase>,
    },
    /// INNER-RELATION (aka SNEAKY-PARENTHESES): table(|> pipeline) or table(, correlation |> pipeline)
    /// Derived tables with semantic pattern classification
    #[lispy("relation:inner")]
    InnerRelation {
        pattern: InnerRelationPattern<Phase>,
        alias: Option<SqlIdentifier>,
        outer: bool,
        cpr_schema: PhaseBox<CprSchema, Phase>,
    },
    /// Consulted view expansion: view body inlined as a subquery.
    /// Holds a full Query (not just RelationalExpression) to support CTEs in view definitions.
    /// Created by the resolver when expanding `consult!`/`engage!` view references.
    #[lispy("relation:consulted-view")]
    ConsultedView {
        identifier: QualifiedName,
        body: Box<super::super::Query<Phase>>,
        scoped: PhaseBox<super::super::ScopedSchema, Phase>,
        outer: bool,
    },
    /// Pseudo-predicate: State-mutating relations with `!` suffix
    /// Examples: import!("nba.db", "nba"), enlist!("std::string")
    ///
    /// Pseudo-predicates execute at Phase 1.X (Effect Executor) and are replaced
    /// with inline tables containing their return values before resolution.
    ///
    /// Key characteristics:
    /// - Always have `!` suffix (e.g., "mount!", "enlist!")
    /// - Execute with side effects (register namespaces, modify bootstrap state)
    /// - Return relations (single row with operation metadata)
    /// - Only exist in Unresolved phase (replaced before Resolved phase)
    #[lispy("relation:pseudo-predicate")]
    #[phase_convert(unreachable)]
    PseudoPredicate {
        /// Pseudo-predicate name (includes `!` suffix)
        name: String,
        /// Arguments (literal expressions in MVP, complex expressions in future)
        arguments: Vec<DomainExpression<Phase>>,
        /// Optional alias for result table (enables dependency chains in Phase 2+)
        alias: Option<String>,
        /// Result schema (populated during effect execution)
        cpr_schema: PhaseBox<CprSchema, Phase>,
    },
}
