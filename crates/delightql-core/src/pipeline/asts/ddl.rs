//! DDL AST — typed in-memory representation of definitions.
//!
//! The DDL AST is ephemeral: produced by parsing definition text, used for
//! validation and body extraction, then discarded. The database stores text;
//! ASTs are re-parsed on demand.
//!
//! Bodies reference regular DQL AST types (`DomainExpression`, `RelationalExpression`)
//! in the `Unresolved` phase — definitions are parsed before resolution context exists.
//!
//! The DDL AST itself is NOT phase-parameterized. It's a static structural
//! container. Only the DQL expressions it references carry the phase marker.

use super::core::{ContextMode, DomainExpression, Query, RelationalExpression, Unresolved};

/// A parsed DDL definition — typed in-memory representation.
///
/// Produced by re-parsing `full_source` text from the entity table.
/// Never stored; always ephemeral.
///
/// Lifecycle:
/// - Consult time: parse → validate → store text → discard AST
/// - Query time: read text → re-parse → DDL AST → extract body → resolve → discard
#[derive(Debug, Clone)]
pub struct DdlDefinition {
    pub name: String,
    pub head: DdlHead,
    pub _neck: DdlNeck,
    pub body: DdlBody,
    pub full_source: String,
    pub doc: Option<String>,
}

/// Definition neck — persistence/scope level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlNeck {
    /// `:-` rule neck (view)
    Session,
    /// `:=` data neck (table)
    TemporaryTable,
}

/// Companion table sigil — which metadata aspect this companion defines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompanionKind {
    /// `(^)` — schema: column names and types
    Schema,
    /// `(+)` — constraints
    Constraint,
    /// `($)` — defaults
    Default,
}

impl crate::lispy::ToLispy for CompanionKind {
    fn to_lispy(&self) -> String {
        match self {
            CompanionKind::Schema => "schema".to_string(),
            CompanionKind::Constraint => "constraint".to_string(),
            CompanionKind::Default => "default".to_string(),
        }
    }
}

/// An item in an argumentative view head: either a free variable or a ground literal.
#[derive(Debug, Clone, PartialEq)]
pub enum ViewHeadItem {
    /// Free variable (column name): projected from the body
    Free(String),
    /// Ground term (literal value): constant injected into every row.
    /// String includes quotes for string literals (e.g., `"old"`).
    Ground(String),
}

/// Definition head — the structural form of the definition.
#[derive(Debug, Clone)]
pub enum DdlHead {
    /// Function: `name:(params)` with optional guards and optional context marker
    Function {
        params: Vec<FunctionParam>,
        context_mode: ContextMode,
    },
    /// View: `name(*)` — no parameters
    View,
    /// Argumentative view: `name(col1, "lit", col2)` — closed schema contract with optional ground terms
    ArgumentativeView { items: Vec<ViewHeadItem> },
    /// Higher-order view: `name(T(*), Config(x,y), n)(output)`
    HoView {
        params: Vec<HoParam>,
        /// Output head: None means glob (*) (open schema), Some means argumentative (closed schema contract)
        output_head: Option<Vec<ViewHeadItem>>,
    },
    /// Sigma predicate: `name(params)` — boolean-valued, used with +/\+ prefix
    SigmaPredicate { params: Vec<String> },
    /// Fact: `name(values)` — inline data literal, no parameters
    Fact,
    /// ER-context rule: `left&right(*) within context :- body`
    ErRule {
        left_table: String,
        right_table: String,
        context: String,
    },
    /// Companion table: `name(sigil)` where sigil is ^, +, or $
    Companion { kind: CompanionKind },
}

/// A function parameter with optional guard expression.
#[derive(Debug, Clone)]
pub struct FunctionParam {
    pub name: String,
    pub guard: Option<DomainExpression<Unresolved>>,
}

/// HO parameter kind — declares how a parameter is bound at call sites.
#[derive(Debug, Clone, PartialEq)]
pub enum HoParamKind {
    /// `T(*)` — structural/duck-typed table parameter (glob functor)
    Glob,
    /// `T(x, y)` — positionally-typed table parameter (argumentative functor)
    Argumentative(Vec<String>),
    /// `n` — scalar value parameter, or legacy bare table name
    Scalar,
    /// `"value"` or `42` — ground scalar literal (constant in this clause)
    GroundScalar(String),
}

/// A higher-order view parameter with kind metadata.
#[derive(Debug, Clone)]
pub struct HoParam {
    pub name: String,
    pub kind: HoParamKind,
}

/// Definition body — the DQL expression(s) after the neck.
#[derive(Debug, Clone)]
pub enum DdlBody {
    /// Scalar body: function definitions produce domain expressions
    Scalar(DomainExpression<Unresolved>),
    /// Relational body: view/ho-view definitions produce full queries (may include CTEs)
    Relational(Query<Unresolved>),
}

impl DdlHead {
    /// Extract parameter names from the head.
    ///
    /// - `Function { params }` → function parameter names
    /// - `HoView { params }` → higher-order parameter names
    /// - `View` → empty
    /// Count total parameter positions (including GroundScalar).
    ///
    /// Unlike `param_names()` which excludes GroundScalar positions,
    /// this counts all positions for arity validation across clauses.
    pub fn param_count(&self) -> usize {
        match self {
            DdlHead::Function { params, .. } => params.len(),
            DdlHead::HoView { params, .. } => params.len(),
            DdlHead::SigmaPredicate { params } => params.len(),
            DdlHead::View
            | DdlHead::ArgumentativeView { .. }
            | DdlHead::Fact
            | DdlHead::ErRule { .. }
            | DdlHead::Companion { .. } => 0,
        }
    }

    pub fn param_names(&self) -> Vec<&str> {
        match self {
            DdlHead::Function { params, .. } => params.iter().map(|p| p.name.as_str()).collect(),
            DdlHead::HoView { params, .. } => params
                .iter()
                .filter_map(|p| {
                    if matches!(p.kind, HoParamKind::GroundScalar(_)) {
                        None
                    } else {
                        Some(p.name.as_str())
                    }
                })
                .collect(),
            DdlHead::SigmaPredicate { params } => params.iter().map(|s| s.as_str()).collect(),
            DdlHead::View
            | DdlHead::ArgumentativeView { .. }
            | DdlHead::Fact
            | DdlHead::ErRule { .. }
            | DdlHead::Companion { .. } => Vec::new(),
        }
    }

    /// Extract HO parameter names only (empty for non-HO heads).
    pub fn ho_param_names(&self) -> Vec<&str> {
        match self {
            DdlHead::HoView { params, .. } => params
                .iter()
                .filter_map(|p| {
                    if matches!(p.kind, HoParamKind::GroundScalar(_)) {
                        None
                    } else {
                        Some(p.name.as_str())
                    }
                })
                .collect(),
            DdlHead::Function { .. }
            | DdlHead::View
            | DdlHead::ArgumentativeView { .. }
            | DdlHead::Fact
            | DdlHead::SigmaPredicate { .. }
            | DdlHead::ErRule { .. }
            | DdlHead::Companion { .. } => vec![],
        }
    }

    /// Entity type integer for storage in the bootstrap database.
    ///
    /// Maps head form → entity_type_enum.id:
    /// - Function → 1 (DqlFunctionExpression)
    /// - View / ArgumentativeView → 4 (DqlTemporaryViewExpression)
    /// - HoView → 8 (DqlHoTemporaryViewExpression)
    /// - SigmaPredicate → 9 (DqlTemporarySigmaRule)
    pub fn entity_type_id(&self) -> i32 {
        match self {
            DdlHead::Function { context_mode, .. } => {
                if matches!(context_mode, ContextMode::None) {
                    1
                } else {
                    3
                }
            }
            DdlHead::View | DdlHead::ArgumentativeView { .. } => 4,
            DdlHead::HoView { .. } => 8,
            DdlHead::SigmaPredicate { .. } => 9,
            DdlHead::Fact => 16,
            DdlHead::ErRule { .. } => 17,
            DdlHead::Companion { .. } => 18,
        }
    }
}

impl DdlDefinition {
    /// Extract the body as a `DomainExpression` (for function definitions).
    pub fn as_domain_expr(&self) -> Option<&DomainExpression<Unresolved>> {
        match &self.body {
            DdlBody::Scalar(expr) => Some(expr),
            DdlBody::Relational(_) => None,
        }
    }

    /// Consume the definition and return the body as a `DomainExpression`.
    pub fn into_domain_expr(self) -> Option<DomainExpression<Unresolved>> {
        match self.body {
            DdlBody::Scalar(expr) => Some(expr),
            DdlBody::Relational(_) => None,
        }
    }

    /// Consume the definition and return the body as a full `Query` (may include CTEs).
    pub fn into_query(self) -> Option<Query<Unresolved>> {
        match self.body {
            DdlBody::Relational(query) => Some(query),
            DdlBody::Scalar(_) => None,
        }
    }

    /// Consume the definition and return the body as a flat `RelationalExpression`.
    ///
    /// Returns None if the body has CTEs. Only valid for fact definitions
    /// (which are always flat anonymous tables). For views and HO views,
    /// use `into_query()` to preserve CTEs.
    pub fn into_flat_relational_expr(self) -> Option<RelationalExpression<Unresolved>> {
        match self.body {
            DdlBody::Relational(Query::Relational(expr)) => Some(expr),
            DdlBody::Relational(Query::WithCtes { .. })
            | DdlBody::Relational(Query::WithCfes { .. })
            | DdlBody::Relational(Query::WithPrecompiledCfes { .. })
            | DdlBody::Relational(Query::ReplTempTable { .. })
            | DdlBody::Relational(Query::WithErContext { .. })
            | DdlBody::Relational(Query::ReplTempView { .. })
            | DdlBody::Scalar(_) => None,
        }
    }
}
