use super::{
    Addressed, DomainExpression, PhaseBox, Refined, RelationalExpression, Resolved, Unresolved,
};
use crate::{lispy::ToLispy, PhaseConvert, ToLispy};
use serde::{Deserialize, Serialize};
use std::fmt;

// ============================================================================
// Assertion Types
// ============================================================================

/// Predicate type for data assertions — determines pass/fail semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AssertionPredicate {
    /// Assertion passes if the sub-query returns ≥1 row
    Exists,
    /// Assertion passes if the sub-query returns 0 rows
    NotExists,
    /// Assertion passes if all rows from the base relation survive filtering
    Forall,
    /// Assertion passes if two relations have identical bags of rows
    Equals,
}

impl fmt::Display for AssertionPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AssertionPredicate::Exists => write!(f, "exists(*)"),
            AssertionPredicate::NotExists => write!(f, "notexists(*)"),
            AssertionPredicate::Forall => write!(f, "forall(*)"),
            AssertionPredicate::Equals => write!(f, "equals(*)"),
        }
    }
}

/// A data assertion — a forked sub-query that validates a property of the
/// relation at the assertion point. The main pipeline continues unchanged.
///
/// Created by the builder when it encounters `(~~assert ... ~~)` in the CST.
/// The body is a `RelationalExpression<Unresolved>` that goes through the
/// normal pipeline (resolve → refine → transform → SQL) independently.
///
/// For `forall(*)`, the builder rewrites the body at parse time:
/// `, P |> forall(*)` becomes `, NOT(P) |> notexists(*)`.
/// The terminal comma predicates are negated and the body goes through
/// the pipeline as NOT EXISTS — no special SQL generation needed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssertionSpec {
    /// The forked sub-query (assertion body without assertion view).
    /// For Forall, terminal predicates are already negated by the builder.
    pub body: RelationalExpression<Unresolved>,
    /// Pass/fail predicate (Exists, NotExists, Forall, or Equals)
    pub predicate: AssertionPredicate,
    /// The right operand for Equals (from reverse pipe `<|`).
    pub right_operand: Option<RelationalExpression<Unresolved>>,
    /// Source location for error reporting (byte start, byte end)
    pub source_location: Option<(usize, usize)>,
}

// ============================================================================
// Emit Types
// ============================================================================

/// An emit specification — a forked sub-query that fans out rows to a named
/// sink. The main pipeline continues unchanged; the emit body is compiled
/// independently to a separate SQL query that the host executes and routes.
///
/// Created by the builder when it encounters `(~~emit:name ... ~~)` in the CST.
/// If the emit has a body (`, predicate`), the forked relation is filtered.
/// If no body, the full relation at that point is captured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmitSpec {
    /// Instance name from `(~~emit:name ~~)`.
    pub name: String,
    /// The forked sub-query (optionally filtered by the emit body).
    pub body: RelationalExpression<Unresolved>,
    /// Source location for error reporting (byte start, byte end).
    pub source_location: Option<(usize, usize)>,
}

// ============================================================================
// Danger Gate Types
// ============================================================================

/// Toggle state for a danger gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DangerState {
    /// Dangerous behavior is enabled
    On,
    /// Dangerous behavior is disabled (safe default)
    Off,
    /// Compiler may use the dangerous path if needed but is not required to
    Allow,
    /// Graduated severity level (1-9) for host-defined policies
    Severity(u8),
}

impl fmt::Display for DangerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DangerState::On => write!(f, "ON"),
            DangerState::Off => write!(f, "OFF"),
            DangerState::Allow => write!(f, "ALLOW"),
            DangerState::Severity(n) => write!(f, "{}", n),
        }
    }
}

/// A danger gate specification — a per-query override for a named safety boundary.
///
/// Created by the builder when it encounters `(~~danger://path STATE~~)` in the CST.
/// The URI identifies the danger; the state controls it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DangerSpec {
    /// The hierarchical danger URI path (e.g. "dql/cardinality/nulljoin")
    pub uri: String,
    /// The toggle state for this query
    pub state: DangerState,
    /// Source location for error reporting (byte start, byte end)
    pub source_location: Option<(usize, usize)>,
}

// ============================================================================
// Option Types
// ============================================================================

/// Toggle state for an option (strategy/preference selection).
/// Same values as DangerState — ON, OFF, ALLOW, or graduated severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptionState {
    /// Option is enabled
    On,
    /// Option is disabled (default)
    Off,
    /// Compiler may use the option if beneficial
    Allow,
    /// Graduated preference level (1-9) for host-defined behavior
    Severity(u8),
}

impl fmt::Display for OptionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OptionState::On => write!(f, "ON"),
            OptionState::Off => write!(f, "OFF"),
            OptionState::Allow => write!(f, "ALLOW"),
            OptionState::Severity(n) => write!(f, "{}", n),
        }
    }
}

/// An option specification — a per-query strategy/preference override.
///
/// Created by the builder when it encounters `(~~option://path STATE~~)` in the CST.
/// The URI identifies the option; the state controls it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OptionSpec {
    /// The hierarchical option URI path (e.g. "generation/rule/inlining/view")
    pub uri: String,
    /// The toggle state for this query
    pub state: OptionState,
    /// Source location for error reporting (byte start, byte end)
    pub source_location: Option<(usize, usize)>,
}

// ============================================================================
// Inline DDL Types
// ============================================================================

/// An inline DDL block extracted from `(~~ddl ... ~~)` annotations.
///
/// Created by the builder when it encounters a `ddl_annotation` node in the CST.
/// The body is raw DDL text that gets parsed and registered via the DDL parser.
#[derive(Debug, Clone)]
pub struct InlineDdlSpec {
    /// The raw DDL body text (view definitions, rules, etc.)
    pub body: String,
    /// Optional namespace for the definitions (default: "user")
    pub namespace: Option<String>,
}

/// The root of any DelightQL query (identical across all phases)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub enum Query<Phase = Unresolved> {
    /// A relational expression (all current queries)
    #[lispy("query:relational")]
    Relational(RelationalExpression<Phase>),
    /// Query with CTE bindings
    #[lispy("query:with_ctes")]
    WithCtes {
        /// CTE definitions
        #[lispy("ctes")]
        ctes: Vec<CteBinding<Phase>>,
        /// Main query expression
        #[lispy("query")]
        query: RelationalExpression<Phase>,
    },
    /// Query with CFE definitions (Unresolved phase only)
    /// After precompilation, this becomes WithPrecompiledCfes
    #[lispy("query:with_cfes")]
    #[phase_convert(unreachable)]
    WithCfes {
        /// CFE definitions (unresolved)
        #[lispy("cfes")]
        cfes: Vec<CfeDefinition>,
        /// Main query (can be another WithCfes, WithCtes, or Relational)
        #[lispy("query")]
        query: Box<Query<Phase>>,
    },
    /// Query with precompiled CFE definitions (Resolved/Refined phases)
    /// Created by precompilation step after builder
    #[lispy("query:with_precompiled_cfes")]
    WithPrecompiledCfes {
        /// Precompiled CFE definitions
        #[lispy("cfes")]
        cfes: Vec<PrecompiledCfeDefinition>,
        /// Main query (can be another WithPrecompiledCfes, WithCtes, or Relational)
        #[lispy("query")]
        query: Box<Query<Phase>>,
    },
    /// REPL-only command to create a temporary table
    #[lispy("query:repl_temp_table")]
    ReplTempTable {
        /// The query to store in the temp table (can include CTEs)
        #[lispy("query")]
        query: Box<Query<Phase>>,
        /// The name of the temporary table to create
        #[lispy("table_name")]
        table_name: String,
    },
    /// ER-context scoping: under context: query
    /// Unresolved-only — resolver strips this wrapper and threads context through config.
    #[lispy("query:with_er_context")]
    #[phase_convert(unreachable)]
    WithErContext {
        context: ErContextSpec,
        query: Box<Query<Phase>>,
    },
    /// REPL-only command to create a temporary view
    #[lispy("query:repl_temp_view")]
    ReplTempView {
        /// The query to store in the temp view (can include CTEs)
        #[lispy("query")]
        query: Box<Query<Phase>>,
        /// The name of the temporary view to create
        #[lispy("view_name")]
        view_name: String,
    },
}

/// ER-context specification: identifies which context to use for & and && operators
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
#[lispy("er_context_spec")]
pub struct ErContextSpec {
    /// Optional namespace qualification (e.g., "lib::er_grounded")
    pub namespace: Option<String>,
    /// Context name (e.g., "normal", "audit")
    pub context_name: String,
}

/// CTE (Common Table Expression) binding: expression : name
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy, PhaseConvert)]
pub struct CteBinding<Phase = Unresolved> {
    /// The relational expression that defines the CTE
    #[lispy("expression")]
    pub expression: RelationalExpression<Phase>,
    /// The name to bind this expression to
    #[lispy("name")]
    pub name: String,
    /// Whether this CTE references itself (recursive CTE).
    /// Populated by the addresser; phantom in earlier phases.
    #[lispy("is_recursive")]
    #[phase_convert(phantom)]
    pub is_recursive: PhaseBox<bool, Phase>,
}

/// CFE (Common Function Expression) definition from parser
/// Example: double:(x) : (x * 2)
/// Context mode for CCAFE (Context-Aware CFE) support
/// Determines how a CFE handles column references beyond its declared parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum ContextMode {
    /// Regular CFE - no context capture, parameters only
    /// Syntax: name:(params) : body
    /// Any non-parameter Lvar in body is an ERROR
    #[lispy("context:none")]
    None,

    /// Implicit context - auto-discover context params from body
    /// Syntax: name:(.., params) : body
    /// Non-parameter Lvars become context params (discovered at precompile time)
    /// Can only be called context-aware: name:(.., args)
    #[lispy("context:implicit")]
    Implicit,

    /// Explicit context - declared context params
    /// Syntax: name:(..{ctx1, ctx2}, params) : body
    /// Only declared context params + parameters allowed in body
    /// Can be called context-aware OR positionally: name:(.., args) or name:(ctx1, ctx2, args)
    #[lispy("context:explicit")]
    Explicit(Vec<String>),
}

/// Higher-order example: apply_transform:(transform)(value) : value /-> transform:()
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub struct CfeDefinition {
    /// The name of the function
    #[lispy("name")]
    pub name: String,
    /// Curried parameter names (code parameters)
    /// Empty for lower-order CFEs, non-empty for higher-order CFEs
    /// Example: in `apply_transform:(f)(x)`, curried_params = ["f"]
    #[lispy("curried_params")]
    pub curried_params: Vec<String>,
    /// Regular parameter names (data parameters)
    /// Example: in `apply_transform:(f)(x)`, parameters = ["x"]
    #[lispy("parameters")]
    pub parameters: Vec<String>,
    /// Context mode for CCAFE support
    #[lispy("context_mode")]
    pub context_mode: ContextMode,
    /// The unresolved body expression
    #[lispy("body")]
    pub body: DomainExpression<Unresolved>,
}

/// Precompiled CFE definition (after resolver + refiner)
/// The body has been resolved and refined with parameters as fake columns
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub struct PrecompiledCfeDefinition {
    /// The name of the function
    #[lispy("name")]
    pub name: String,
    /// Curried parameter names (code parameters)
    /// Empty for lower-order CFEs, non-empty for higher-order CFEs
    #[lispy("curried_params")]
    pub curried_params: Vec<String>,
    /// Regular parameter names (data parameters)
    #[lispy("parameters")]
    pub parameters: Vec<String>,
    /// Context parameters (discovered for Implicit or declared for Explicit)
    /// Empty for regular CFEs (ContextMode::None)
    #[lispy("context_params")]
    pub context_params: Vec<String>,
    /// Whether this CFE allows positional context calls
    /// true for Explicit context (can call positionally or context-aware)
    /// false for Implicit context (can only call context-aware)
    /// false for None (no context params)
    #[lispy("allows_positional_context_call")]
    pub allows_positional_context_call: bool,
    /// The resolved and refined body expression
    #[lispy("body")]
    pub body: DomainExpression<Refined>,
}

