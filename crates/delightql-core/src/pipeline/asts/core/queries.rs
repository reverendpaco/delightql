// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::{Chain, OutValue, Phase, Unresolved};
use crate::{lispy::ToLispy, ToLispy};
use std::fmt;

// ============================================================================
// Assertion Types
// ============================================================================

/// A data assertion — a forked sub-query that validates a property of the
/// relation at the assertion point. The main pipeline continues unchanged.
///
/// Created by the builder when it encounters `(~~assert ... ~~)` in the CST.
/// The body is a `Chain<Unresolved>` that goes through the
/// normal pipeline (resolve → refine → transform → SQL) independently.
///
/// **An assertion asks whether the body has a row.** `exists` and
/// `notexists` are ordinary views in the body and answer with a receipt —
/// a row means YES, absence means NO — so for them the annotation's whole
/// job is turning that absence into a stopped run.
///
/// `equals` is the one exception left, and it is an exception because its
/// comparison is POSITIONAL: it lowers to SQL `EXCEPT`, which pairs
/// columns by position. The relational minus (`-`) pairs them by name, so
/// a prelude view built on it is a different predicate wherever a heading
/// repeats or reorders names — not the same operator spelled differently.
/// Until that is ruled, the right operand travels here.
#[derive(Debug, Clone, PartialEq)]
pub struct AssertionSpec {
    /// The forked sub-query, exactly as written.
    pub body: Chain<Unresolved>,
    /// The author's name for this check, from `(~~assert:"…" ~~)`. Its
    /// only purpose is to say WHICH assertion failed, so it travels all
    /// the way to the failure message; an ordinal is the fallback, not
    /// the answer.
    pub name: Option<String>,
    /// The relation passed to `equals(...)`.
    /// Present exactly when the body's assertion view is `equals`.
    pub right_operand: Option<Chain<Unresolved>>,
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
// ============================================================================
// Danger Gate Types
// ============================================================================

/// Toggle state for a danger gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct DangerSpec {
    /// The canonical danger URI (e.g. "delightql-danger://cardinality/cartesian")
    pub uri: String,
    /// The toggle state for this query
    pub state: DangerState,
}

// ============================================================================
// Option Types
// ============================================================================

/// Toggle state for an option (strategy/preference selection).
/// Same values as DangerState — ON, OFF, ALLOW, or graduated severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct OptionSpec {
    /// The canonical config URI (e.g. "delightql-config://generation/rule/inlining/view")
    pub uri: String,
    /// The toggle state for this query
    pub state: OptionState,
}

// ============================================================================
// Inline DDL Types
// ============================================================================

/// An inline DDL block from a `(~~ddl ... ~~)` annotation.
///
/// The body is TYPED definition content, parsed and normalized with the
/// enclosing submission — never inline text. Registration remains a
/// consultation-time act: which namespace the block lands in, collisions,
/// redefinition, and rollback are judged against live session state.
#[derive(Debug, Clone)]
pub struct InlineDdlSpec {
    /// The block's definition content. Empty for `(~~ddl ~~)` — the lawful
    /// empty block, which declares nothing and creates nothing.
    pub body: InlineDdlBody,
    /// Optional namespace name for the definitions. `Some("chz")` routes to the
    /// scratch child `home::chz`; `None` (unnamed block) lands the entities directly
    /// in `home`.
    pub namespace: Option<String>,
}

/// A block's body is FILE-SHAPED: many subjects and nested blocks, not one
/// definition. Clauses stay unassembled — sibling agreement is the
/// consultation-time assembler's judgment (`DefinitionGroup::assemble`),
/// exactly as for a consulted file.
#[derive(Debug, Clone, Default)]
pub struct InlineDdlBody {
    /// One clause per authored definition, in authored order.
    pub definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    /// Nested blocks, same carrier, subordinate to this block's namespace.
    pub ddl_blocks: Vec<InlineDdlSpec>,
}

impl InlineDdlBody {
    /// Whether the block declares anything at all.
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty() && self.ddl_blocks.is_empty()
    }
}

/// The root of any DelightQL query: a binding preamble and ONE body.
///
/// The bindings are fields, not wrappers — a query cannot nest a second
/// query around itself to smuggle ordering or ownership, and every
/// consumer handles the one shape instead of selecting among carriers.
/// Compiler-built and authored queries use this same carrier.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("query")]
pub struct Query<P: Phase = Unresolved> {
    /// The query-scoped CFE definitions, in authored order — bindings the
    /// resolver SPENDS at their call sites. Phase-selected: the authored
    /// definitions before resolution, nothing after — not an empty list,
    /// no slot at all — so no resolved or refined query can carry one.
    #[lispy("cfes")]
    pub cfes: P::CfeBindings,
    /// The CTE bindings, one ordered collection in authored order.
    #[lispy("ctes")]
    pub ctes: Vec<CteBinding<P>>,
    /// The one body the query runs.
    #[lispy("body")]
    pub body: Chain<P>,
}

impl<P: Phase> Query<P> {
    /// A bare body: no bindings of either kind.
    pub fn relational(body: Chain<P>) -> Self {
        Query {
            cfes: P::no_cfe_bindings(),
            ctes: Vec::new(),
            body,
        }
    }

    /// Whether the query is its body alone — no binding of either kind.
    pub fn is_bare(&self) -> bool {
        self.ctes.is_empty() && P::cfe_bindings(&self.cfes).is_empty()
    }

    /// The body of a query that carries no bindings; the caller keeps the
    /// whole query otherwise.
    pub fn into_bare_body(self) -> std::result::Result<Chain<P>, Box<Query<P>>> {
        if self.is_bare() {
            Ok(self.body)
        } else {
            Err(Box::new(self))
        }
    }
}

/// ER-context specification: identifies which context to use for & and && operators
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("er_context_spec")]
pub struct ErContextSpec {
    /// Optional namespace qualification (e.g., "lib::er_grounded")
    pub namespace: Option<String>,
    /// Context name (e.g., "normal", "audit")
    pub context_name: String,
}

/// What a CTE label DECLARES about its expression's relationship to effects.
///
/// A bare label asserts a pure expression; a `!`-marked label asserts the
/// expression demands a directive. The declaration is an assertion, never a
/// coercion: the effect authority judges it against the body once — a mark
/// on a pure body refuses (`effect/cte/pure_mark`) before a marked binding
/// can be constructed, and an effectful body without the mark refuses where
/// the rule registers (`effect/cte/label`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ToLispy)]
pub enum CteEffectDeclaration {
    /// A bare label: the expression is asserted pure.
    #[lispy("effect:pure")]
    Pure,
    /// A `!`-marked label or head: the expression is asserted to demand a
    /// directive. Read from the CST's `effect_marker` field by the builder
    /// (pinned by `effect_cte_marker_is_read_by_builder`).
    #[lispy("effect:demands_directive")]
    DemandsDirective,
}

/// The subject a CTE binding stands on before resolution spends it.
///
/// One carrier, two honest cases: an authored binding carries the spelling
/// the author wrote — strop bit intact, agreement by the identifier law —
/// and a compiler-built binding stands directly on its pre-minted carrier
/// scope, pretending to no spelling at all. An authored subject has no room
/// for a resolved scope and a structural subject none for a name, so neither
/// fact can be faked from the other side.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum CteSubject {
    /// An authored label or head: `expression : name`, `name(*) : body`.
    #[lispy("subject:authored")]
    Authored {
        /// The spelling as written. Grouping, registration, and duplicate
        /// judgment compare by the identifier law — an unstropped spelling
        /// folds, a stropped one keeps its authored bytes.
        #[lispy("name")]
        name: delightql_types::SqlIdentifier,
        /// The label's effect declaration.
        #[lispy("effect")]
        effect: CteEffectDeclaration,
    },
    /// A compiler-built binding that must still ANSWER TO A NAME, because
    /// its equally generated body references it by this spelling. The
    /// spelling is the compiler's — no author wrote it — so it carries no
    /// effect declaration (a compiler asserts nothing about effects) and
    /// grouping never merges it with an authored clause set.
    #[lispy("subject:generated")]
    Generated {
        /// The generated spelling the generated body references.
        #[lispy("name")]
        name: delightql_types::SqlIdentifier,
    },
    /// A compiler-built binding standing on its carrier scope. Grouping
    /// keys on the scope, never on a diagnostic spelling.
    #[lispy("subject:structural")]
    Structural(crate::names::ScopeId),
}

impl CteSubject {
    /// The authored spelling, where one exists. A generated or structural
    /// subject has none — not a synthetic one, none: a generated name is
    /// the compiler's and never stands where an author's spelling is asked
    /// for.
    pub fn authored_name(&self) -> Option<&delightql_types::SqlIdentifier> {
        match self {
            CteSubject::Authored { name, .. } => Some(name),
            CteSubject::Generated { .. } | CteSubject::Structural(_) => None,
        }
    }

    /// Whether the label declares the expression demands a directive.
    pub fn declares_effect(&self) -> bool {
        matches!(
            self,
            CteSubject::Authored {
                effect: CteEffectDeclaration::DemandsDirective,
                ..
            }
        )
    }
}

/// What resolution consumes of an authored CTE binding beyond its subject:
/// the head it groups and spends, and the two provenance judgments that pick
/// its naming hint and its resolution scope. SPENT WHOLE at resolution — the
/// phase system deletes the slot afterwards, so no bound phase can carry a
/// spent copy.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct CteAuthority {
    /// The authored head, in the SAME type the `:-` and `:=` necks carry.
    /// A glob head passes the body's heading through; a listed head is the
    /// closed contract the one assembler enforces across the subject's
    /// clauses. `body : name` is `name(*) : body`, so the labeling
    /// shorthand and a compiler-built binding both glob.
    #[lispy("head")]
    pub head: crate::pipeline::asts::core::definitions::Head,
    /// TYPED provenance: who authored this CTE — set at CONSTRUCTION,
    /// never inferred from the name (a user may legally write `_ho_*`
    /// identifiers). The squished-weave scope override keys on this.
    #[lispy("origin")]
    pub origin: crate::pipeline::asts::core::provenance::CteOrigin,
    /// WHOSE scope resolves this CTE's names — Caller only for the
    /// squished weave's caller-side carriers; the entity's own
    /// clause-body wrappers stay Entity even though they are
    /// compiler-CONSTRUCTED. Distinct from `origin` above.
    #[lispy("resolution_owner")]
    pub resolution_owner: crate::pipeline::asts::core::provenance::CteResolutionOwner,
}

/// CTE (Common Table Expression) binding: expression : name
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct CteBinding<P: Phase = Unresolved> {
    /// The chain that defines the CTE
    #[lispy("expression")]
    pub expression: Chain<P>,
    /// What this binding stands on: the authored subject before resolution,
    /// the exact bound scope after. Resolution SPENDS the authored spelling
    /// and effect declaration where it mints the binding's scope, so later
    /// phases address the binding by identity and never compare characters
    /// again — a resolved binding cannot carry an authored-only name or a
    /// "maybe bound" state, and an authored one cannot claim a bound scope.
    #[lispy("subject")]
    pub subject: P::CteSubject,
    /// The head and provenance resolution spends (`CteAuthority`) — present
    /// before resolution, deleted by the phase system after, exactly as the
    /// subject's spelling is.
    #[lispy("authority")]
    pub authority: P::CteAuthority,
    /// Whether this binding's body references the binding itself.
    ///
    /// DECIDED ONCE, by the resolver, at the moment a body's reference is
    /// known to be this binding's own scope — and never re-derived. A
    /// stored decision has no second opinion to disagree with.
    #[lispy("recursion")]
    pub recursion: P::Recursion,
}

impl crate::pipeline::asts::core::definitions::HeadedClause for CteBinding<Unresolved> {
    fn head(&self) -> &crate::pipeline::asts::core::definitions::Head {
        &self.authority.head
    }

    fn body_publishes_names(&self) -> bool {
        crate::pipeline::asts::core::definitions::chain_publishes_names(&self.expression)
    }

    fn spend_head(
        mut self,
        items: &[crate::pipeline::asts::core::definitions::HeadItem],
        canonical_names: &[delightql_types::SqlIdentifier],
    ) -> Self {
        self.expression = crate::pipeline::asts::core::definitions::project_body_through_head(
            self.expression,
            items,
            canonical_names,
        );
        self.authority.head = crate::pipeline::asts::core::definitions::Head::glob();
        self
    }
}

/// CFE (Common Function Expression) definition from parser
/// Example: double:(x) : (x * 2)
/// Context mode for CCAFE (Context-Aware CFE) support
/// Determines how a CFE handles column references beyond its declared parameters
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum ContextMode {
    /// Regular CFE - no context capture, parameters only
    /// Syntax: name:(params) : body
    /// Any non-parameter Lvar in body is an ERROR
    #[lispy("context:none")]
    None,

    /// Implicit context - auto-discover context params from body
    /// Syntax: name:(.., params) : body
    /// Free body names capture from the caller's row at the call site.
    /// Can only be called context-aware: name:(.., args)
    #[lispy("context:implicit")]
    Implicit,

    /// Explicit context - declared context params
    /// Syntax: name:(..{ctx1, ctx2}, params) : body
    /// Only declared context params + parameters allowed in body
    /// Can be called context-aware OR positionally: name:(.., args) or name:(ctx1, ctx2, args)
    #[lispy("context:explicit")]
    Explicit(Vec<delightql_types::SqlIdentifier>),
}

/// What a value definition's formal RECEIVES at a call site.
///
/// The role rides the formal itself, so no consumer reconstructs it from
/// which of two lists a name sat in, and a formal cannot land in a carrier
/// that disagrees with its role: a `Callable` formal fills the frame's
/// callables, a `Scalar` formal its values, and the payload types make the
/// other assignment unwritable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ToLispy)]
pub enum CfeFormalRole {
    /// A data parameter: the call site supplies a value.
    #[lispy("role:scalar")]
    Scalar,
    /// A curried code parameter (the first list of an HO-CFE): the call
    /// site supplies a callable — a mention, a lambda, or a template.
    #[lispy("role:callable")]
    Callable,
}

/// One declared formal of a value definition: the authored spelling — strop
/// bit intact, agreement by the identifier law — and its exact role.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct CfeFormal {
    #[lispy("name")]
    pub name: delightql_types::SqlIdentifier,
    #[lispy("role")]
    pub role: CfeFormalRole,
}

/// The declared formals of a value definition, in BINDING order: every
/// callable formal precedes every scalar one, because a call site supplies
/// code first.
///
/// The order is the TYPE's guarantee, not a producer convention: the inner
/// vector is private, `from_role_groups` cannot build a misordered value,
/// and `in_binding_order` refuses one in every build — so a role can never
/// disagree with the binding region a consumer reads it from, whoever the
/// next producer is.
#[derive(Debug, Clone, PartialEq)]
pub struct CfeFormals(Vec<CfeFormal>);

impl crate::lispy::ToLispy for CfeFormals {
    fn to_lispy(&self) -> String {
        self.0.to_lispy()
    }
}

impl CfeFormals {
    /// The infallible door: the two role groups, code first. A caller
    /// holding the groups separately cannot express a misordering.
    pub fn from_role_groups(
        callable: impl IntoIterator<Item = delightql_types::SqlIdentifier>,
        scalar: impl IntoIterator<Item = delightql_types::SqlIdentifier>,
    ) -> Self {
        Self(
            callable
                .into_iter()
                .map(|name| CfeFormal {
                    name,
                    role: CfeFormalRole::Callable,
                })
                .chain(scalar.into_iter().map(|name| CfeFormal {
                    name,
                    role: CfeFormalRole::Scalar,
                }))
                .collect(),
        )
    }

    /// The checked door for a caller holding one ordered list: refuses a
    /// callable formal standing after a scalar one, in every build.
    pub fn in_binding_order(formals: Vec<CfeFormal>) -> crate::error::Result<Self> {
        let mut scalar_seen = false;
        for formal in &formals {
            match formal.role {
                CfeFormalRole::Scalar => scalar_seen = true,
                CfeFormalRole::Callable if scalar_seen => {
                    return Err(crate::error::DelightQLError::transformation_error(
                        format!(
                            "the callable formal '{}' stands after a scalar one: a call \
                             site supplies code first, so binding order is \
                             callable-then-scalar",
                            formal.name
                        ),
                        "cfe_formals",
                    ));
                }
                CfeFormalRole::Callable => {}
            }
        }
        Ok(Self(formals))
    }

    /// The formals split at the binding boundary: the callable prefix,
    /// then the scalar rest. Total, because the constructors are the only
    /// producers and both uphold the order.
    pub fn split(&self) -> (&[CfeFormal], &[CfeFormal]) {
        let boundary = self
            .0
            .iter()
            .position(|formal| formal.role == CfeFormalRole::Scalar)
            .unwrap_or(self.0.len());
        self.0.split_at(boundary)
    }

    /// The curried (code) formals: the leading `Callable` run.
    pub fn callable(&self) -> &[CfeFormal] {
        self.split().0
    }

    /// The data formals: everything after the callable prefix.
    pub fn scalar(&self) -> &[CfeFormal] {
        self.split().1
    }

    /// Every declared formal, in binding order.
    pub fn iter(&self) -> std::slice::Iter<'_, CfeFormal> {
        self.0.iter()
    }
}

/// Higher-order example: apply_transform:(transform)(value) : value /-> transform:()
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub struct CfeDefinition {
    /// The name of the function, AS AUTHORED — the strop bit rides with the
    /// characters, so agreement is the identifier law's, not `==` on text.
    #[lispy("name")]
    pub name: delightql_types::SqlIdentifier,
    /// The declared formals, in the carrier whose type owns binding order.
    #[lispy("formals")]
    pub formals: CfeFormals,
    /// Context mode for CCAFE support
    #[lispy("context_mode")]
    pub context_mode: ContextMode,
    /// What the body COMPUTES: a domain value, or the licensed crossing.
    ///
    /// A rule body is a publication position — it names the one value the
    /// rule denotes — so it admits the crossing in the same carrier an out
    /// item's value does. `f:( … ) : +orders(, … )` is the pre-carved
    /// existence spelling standing here.
    #[lispy("body")]
    pub body: OutValue<Unresolved>,
    /// The consulted definition's OWNING namespace: its body's sibling
    /// lookups resolve under this scope at instantiation.
    /// None for inline CFEs (defined in user query text).
    #[lispy(skip)]
    pub source_namespace: Option<String>,
}

impl CfeDefinition {
    /// The formals split at the binding boundary.
    pub fn split_formals(&self) -> (&[CfeFormal], &[CfeFormal]) {
        self.formals.split()
    }

    /// The curried (code) formals: the leading `Callable` run.
    pub fn callable_formals(&self) -> &[CfeFormal] {
        self.formals.callable()
    }

    /// The data formals: everything after the callable prefix.
    pub fn scalar_formals(&self) -> &[CfeFormal] {
        self.formals.scalar()
    }
}

#[cfg(test)]
mod cfe_formal_tests {
    use super::{CfeFormal, CfeFormalRole, CfeFormals};
    use delightql_types::SqlIdentifier;

    fn formal(name: &str, role: CfeFormalRole) -> CfeFormal {
        CfeFormal {
            name: SqlIdentifier::new(name),
            role,
        }
    }

    /// The binding boundary splits the callable prefix from the scalar
    /// rest — the role rides each formal, so the split reads roles, never
    /// list membership.
    #[test]
    fn formals_split_at_the_binding_boundary() {
        let formals = CfeFormals::from_role_groups(
            [SqlIdentifier::new("f"), SqlIdentifier::new("g")],
            [SqlIdentifier::new("x")],
        );
        let (callable, scalar) = formals.split();
        assert_eq!(callable.len(), 2);
        assert_eq!(scalar.len(), 1);
        assert!(callable.iter().all(|f| f.role == CfeFormalRole::Callable));
        assert!(scalar.iter().all(|f| f.role == CfeFormalRole::Scalar));

        let none = CfeFormals::from_role_groups([], [SqlIdentifier::new("x")]);
        assert!(none.callable().is_empty());
        let all = CfeFormals::from_role_groups([SqlIdentifier::new("f")], []);
        assert!(all.scalar().is_empty());
    }

    /// A misordered list cannot become a carrier in ANY build: the checked
    /// door refuses a callable formal standing after a scalar one, and the
    /// group door cannot express the shape at all.
    #[test]
    fn a_callable_after_a_scalar_is_refused_at_construction() {
        let refused = CfeFormals::in_binding_order(vec![
            formal("x", CfeFormalRole::Scalar),
            formal("f", CfeFormalRole::Callable),
        ]);
        assert!(refused.is_err());

        let ordered = CfeFormals::in_binding_order(vec![
            formal("f", CfeFormalRole::Callable),
            formal("x", CfeFormalRole::Scalar),
        ])
        .expect("callable-then-scalar is the binding order");
        assert_eq!(ordered.callable().len(), 1);
        assert_eq!(ordered.scalar().len(), 1);
    }
}
