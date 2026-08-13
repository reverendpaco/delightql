// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
use crate::pipeline::asts::core::ColumnOccurrence;
use delightql_types::error::{DelightQLError, Result};
use std::collections::HashMap;
use std::rc::Rc;

mod pattern_resolver;
pub use pattern_resolver::{JoinContext, PatternResolver};

mod string_templates;

/// What publication decides, across the phases: which occurrence an item
/// publishes, whether it publishes one at all, and what it answers to.
#[cfg(test)]
mod publication_boundary_tests;

/// Probe tests: plan-note schema injection via the query-local
/// registry (test code only — see the module header for the guarantee).
#[cfg(test)]
mod plan_note_injection_tests;

/// SQL-shape pins for argumentative semi/anti-join correlation: the `+rel(col)` guard must
/// compare the OUTER column to the fact column, never `_fact` to itself.
#[cfg(test)]
mod semijoin_correlation_tests;

/// Classification pins for bare guards on enlisted tables / consulted rules
/// (the torture--99 blocker): a
/// guard functor resolvable through enlistment or the Some(ns) resolution
/// scope must classify as table-as-sigma, never fall to PredicateRewrite.
#[cfg(test)]
mod enlisted_guard_classification_tests;

/// Scope pins for sigma-predicate rule guards: a sigma rule visible in the
/// Some(ns) consulted scope must expand to its boolean body — scope first,
/// enlisted-into-main as fallback.
#[cfg(test)]
mod sigma_guard_scope_tests;

/// Shadowing pins: temp shadows main for UNQUALIFIED names only;
/// qualified reads reach the physical entity; the shadow is a resolution
/// preference, never a catalog delete.
#[cfg(test)]
mod session_shadow_tests;

/// In-flight consulted-definition expansions, shared across config clones
/// and guarding the view/rule inliner against non-terminating expansion:
/// re-encountering a name that is
/// already being expanded means the self-reference did NOT resolve as the
/// in-progress CTE (recursive clause before base, or an indirect cycle
/// through another view) — refuse with a teaching error, never spin.
#[derive(Debug, Clone, Default)]
pub struct ExpansionGuard(Rc<std::cell::RefCell<Vec<String>>>);

impl ExpansionGuard {
    /// Push `key` and return an RAII frame that pops on drop. Errors with
    /// the current expansion chain if `key` is already in flight.
    pub fn enter(&self, key: String, context: &str) -> Result<ExpansionFrame> {
        {
            let stack = self.0.borrow();
            if stack.contains(&key) {
                let chain = stack
                    .iter()
                    .chain(std::iter::once(&key))
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" → ");
                return Err(DelightQLError::ValidationError {
                    message: format!(
                        "circular consulted-definition expansion: '{key}' is already \
                         being expanded ({chain}). If this is a recursive rule, the \
                         base (non-recursive) clause must come FIRST in the consulted \
                         file — a self-reference is only recursive once a prior clause \
                         has established the name. If the cycle runs through another \
                         view, break the cycle. SEMANTICS/recursion-contract-law.md B5."
                    ),
                    context: context.to_string(),
                    subcategory: Some(
                        crate::uri_registry::subcat::RECURSION_CONSULTED_CLAUSE_ORDER,
                    ),
                });
            }
        }
        self.0.borrow_mut().push(key);
        Ok(ExpansionFrame(Rc::clone(&self.0)))
    }
}

/// RAII frame for [`ExpansionGuard`]: pops the most recent entry on drop,
/// so every return path (including `?` error propagation) unwinds the stack.
pub struct ExpansionFrame(Rc<std::cell::RefCell<Vec<String>>>);

impl Drop for ExpansionFrame {
    fn drop(&mut self) {
        self.0.borrow_mut().pop();
    }
}

/// THE ONE instantiation allowance, owned by the compilation: every road
/// that opens a definition — the ordinary value position and the pattern
/// slot alike — spends the SAME state. Clones share the cell, so nested
/// resolutions inherit spent depth rather than starting a private count.
#[derive(Debug, Clone, Default)]
pub struct InstantiationDepth {
    spent: std::rc::Rc<std::cell::Cell<usize>>,
}

impl InstantiationDepth {
    /// Open one instantiation; the returned frame closes it on drop.
    /// Refuses when the compilation's allowance is exhausted.
    pub(crate) fn enter(&self, name: &str) -> crate::error::Result<InstantiationFrame> {
        if self.spent.get() >= grounding::INSTANTIATION_DEPTH_LIMIT {
            return Err(grounding::instantiation_depth_refusal(name));
        }
        self.spent.set(self.spent.get() + 1);
        Ok(InstantiationFrame {
            spent: std::rc::Rc::clone(&self.spent),
        })
    }
}

pub(crate) struct InstantiationFrame {
    spent: std::rc::Rc<std::cell::Cell<usize>>,
}

impl Drop for InstantiationFrame {
    fn drop(&mut self) {
        self.spent.set(self.spent.get().saturating_sub(1));
    }
}

/// EVERYTHING a pattern slot needs to instantiate a definition, or
/// nothing: the definition sources travel WITH the compilation's
/// allowance, so a road that can instantiate cannot lack the bound.
#[derive(Clone, Copy)]
pub(crate) struct SlotInstantiation<'a> {
    pub scoped_cfes: &'a std::collections::HashMap<
        delightql_types::SqlIdentifier,
        crate::pipeline::asts::core::CfeDefinition,
    >,
    pub consult: &'a crate::resolution::ConsultRegistry,
    pub lookup_scope: Option<&'a str>,
    pub depth: &'a InstantiationDepth,
}

/// The code bound to a curried formal at an instantiation site.
#[derive(Debug, Clone)]
pub enum CallableBinding {
    /// `upper:()` — a mention. Where the formal is invoked, this callee
    /// stands and the invocation's own arguments replace the mention's.
    Named(Box<crate::pipeline::asts::unresolved::StandardApplication>),
    /// `:(@ + 5)` / `:"…{@}…"` — an open body, carried AS AUTHORED with
    /// the caller's scope captured beside it. Invoking the formal resolves
    /// the body in that scope with the supplied value standing in its
    /// slots — the applying position spends the leaf, so no resolved tree
    /// ever carries one.
    Open(Box<OpenBinding>),
}

/// An open body a caller handed to a curried formal, and the scope it was
/// written in. The interior is the caller's text: resolving it anywhere
/// else would let the definition's formals capture the caller's names.
#[derive(Debug, Clone)]
pub struct OpenBinding {
    pub body: crate::pipeline::asts::unresolved::DomainExpression,
    pub available: Vec<crate::names::ColId>,
    pub local_available: Vec<crate::names::ColId>,
    pub qualifier_scope: Vec<crate::names::ScopeId>,
}

/// The innermost open instantiation's formals: canonical identity -> the
/// argument as the CALLER resolved it. A spent formal never re-resolves, so
/// no scope the body opens can capture it. The key is the identifier LAW's
/// agreement — an unstropped spelling folds, a stropped one keeps its
/// authored identity — never `==` on characters.
#[derive(Debug, Default)]
pub struct FormalFrame {
    /// Value formals, resolved at the call site.
    pub values: std::collections::HashMap<
        delightql_types::SqlIdentifier,
        crate::pipeline::asts::resolved::DomainExpression,
    >,
    /// Curried (code) formals.
    pub callables: std::collections::HashMap<delightql_types::SqlIdentifier, CallableBinding>,
}

/// Configuration for TVF resolution behavior
#[derive(Debug, Clone)]
pub struct ResolutionConfig {
    /// Allow unknown TVFs to pass through with Unknown schema
    pub permissive: bool,
    /// SERVE BOOTSTRAP READS (materialization-law §2). While a
    /// materialization source resolves, a relation on the bootstrap
    /// connection is answered as a literal snapshot read at plan build —
    /// so connection 1 never enters the attribution set (exemption is
    /// ABSENCE, not a tie-break) and the compiled source executes whole on
    /// whatever connection attribution selects.
    pub serve_bootstrap_reads: bool,
    /// When true, outer_context provides reachable columns for validation
    /// but does NOT trigger deferred (skip) validation mode. Used for
    /// EXISTS/semi-join/anti-join subqueries where the full column set
    /// (outer + inner) is known and validation is safe.
    pub validate_in_correlation: bool,
    /// Namespace to scope ER-rule lookups to during qualified view body resolution.
    /// Set when resolving a namespace-qualified view (`ns.view(*)`), so that ER-rules
    /// from the view's namespace are found without requiring engage.
    pub resolution_namespace: Option<String>,
    /// In-flight consulted-definition expansions (shared across clones).
    pub expansion_guard: ExpansionGuard,
    /// Set only while a consulted value definition's body resolves; nested
    /// resolutions inherit it with the config, so a formal reaches into the
    /// subqueries and probes the body opens — and nothing else, because a
    /// consulted BODY road clears it before resolving foreign text.
    pub cfe_formal_frame: Option<std::sync::Arc<FormalFrame>>,
    /// The one instantiation allowance (shared across clones).
    pub instantiation_depth: InstantiationDepth,
}

impl Default for ResolutionConfig {
    fn default() -> Self {
        Self {
            permissive: true, // Default to permissive mode
            serve_bootstrap_reads: false,
            validate_in_correlation: false,
            resolution_namespace: None,
            expansion_guard: ExpansionGuard::default(),
            cfe_formal_frame: None,
            instantiation_depth: InstantiationDepth::default(),
        }
    }
}

pub mod unification;
use unification::ColumnReference;

pub(crate) mod helpers;
use self::helpers::*;
mod bubbling;
use self::bubbling::*;
mod cte_validation;
pub(crate) mod resolving;
use self::cte_validation::*;
mod type_conversion;

mod set_operations;
mod tvf;
use self::set_operations::*;
mod schema_utils;
use self::schema_utils::*;
mod join_resolver;
use self::join_resolver::*;
pub(crate) mod grounding;
mod relation_resolver;
mod resolver_fold;
use crate::pipeline::asts::core::{
    Comparison, Existence, Membership, RelationalMembership, SigmaApplication,
};
use crate::pipeline::asts::core::{NamedReference, Reference};
use resolver_fold::ResolverFold;

#[derive(Debug, Clone)]
pub struct BubbledState {
    /// Columns produced by this relational expression. Unqualified references
    /// resolve against this schema.
    pub i_provide: Vec<crate::names::ColId>,
    pub i_need: Vec<ColumnReference>,
    /// Columns grouped under the lexical qualifiers which remain visible while
    /// resolving a condition attached to this expression. This deliberately
    /// differs from `i_provide`: a set operation produces one merged output
    /// schema while its correlation condition can still name its operand
    /// aliases. A pipe result, conversely, is Fresh and carries no old aliases.
    pub qualifier_scope: Vec<crate::names::ScopeId>,
}

impl BubbledState {
    pub fn empty() -> Self {
        Self {
            i_provide: Vec::new(),
            i_need: Vec::new(),
            qualifier_scope: Vec::new(),
        }
    }

    pub fn resolved(columns: Vec<crate::names::ColId>, registry: &crate::names::Registry) -> Self {
        let mut qualifier_scope = Vec::new();
        for column in &columns {
            let scope = registry.scope_of(*column);
            if !qualifier_scope.contains(&scope) {
                qualifier_scope.push(scope);
            }
        }
        Self {
            qualifier_scope,
            i_provide: columns,
            i_need: Vec::new(),
        }
    }

    /// A relation whose dimensions the target does not publish.
    ///
    /// It offers no columns to enumerate and still carries its scope: a
    /// reference standing over it must be able to find out that nothing was
    /// enumerated, rather than be told the name is absent.
    pub fn opaque(scope: crate::names::ScopeId) -> Self {
        Self {
            i_provide: Vec::new(),
            i_need: Vec::new(),
            qualifier_scope: vec![scope],
        }
    }

    pub fn with_unresolved(
        columns: Vec<crate::names::ColId>,
        unresolved: Vec<ColumnReference>,
    ) -> Self {
        Self {
            i_provide: columns,
            i_need: unresolved,
            qualifier_scope: Vec::new(),
        }
    }

    pub fn combine(left: BubbledState, right: BubbledState) -> Self {
        let mut combined_provide = left.i_provide;
        combined_provide.extend(right.i_provide);

        let mut combined_need = left.i_need;
        combined_need.extend(right.i_need);

        let mut combined_scope = left.qualifier_scope;
        combined_scope.extend(right.qualifier_scope);

        Self {
            i_provide: combined_provide,
            i_need: combined_need,
            qualifier_scope: combined_scope,
        }
    }
}

// Re-export DatabaseSchema from delightql-types: it lives in the types
// crate, not core, to avoid circular dependencies.
pub use delightql_types::schema::DatabaseSchema;

/// Result of query resolution including connection routing information
pub struct ResolvedQueryResult {
    /// The resolved query AST
    pub query: ast_resolved::Query,
    /// The single connection_id if all tables are on the same connection,
    /// or None if no tables were resolved (pure literal query).
    /// Cross-connection queries will have already errored during resolution.
    pub connection_id: Option<i64>,
}

/// Group a flat list of CTE bindings by subject, preserving first-appearance
/// order, then validate inter-CTE dependencies (forward references, cycles).
///
/// An authored key compares by the identifier law — `SqlIdentifier`'s
/// equality folds an unstropped spelling and keeps a stropped one verbatim —
/// and a structural key is the carrier scope itself, so two same-spelled
/// carriers can never merge and two fold-equal authored clauses always do.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CteGroupKey {
    Authored(delightql_types::SqlIdentifier),
    /// A compiler-generated spelling. Its own key kind, so a generated
    /// binding can never merge with an authored clause set that happens
    /// to fold to the same characters.
    Generated(delightql_types::SqlIdentifier),
    Structural(crate::names::ScopeId),
}

fn bind_schema_to_scope(
    input: crate::names::ScopeId,
    scope: crate::names::ScopeId,
    identities: &crate::names::Registry,
) -> Result<crate::names::ScopeId> {
    if !matches!(
        identities.origin_of(scope),
        crate::names::ScopeOrigin::HoCarrier {
            role: crate::names::HoRole::Argument
                | crate::names::HoRole::PipeSource
                | crate::names::HoRole::ScalarInput
        }
    ) {
        return Err(DelightQLError::parse_error(
            "a structural CTE binding must be an argument carrier",
        ));
    }
    if !identities.known_heading(scope)?.is_empty() {
        return Err(DelightQLError::parse_error(
            "a structural CTE binding was published more than once",
        ));
    }
    // A hygienic carrier does not enter the binding's heading. It stands for a
    // slot that introduced no name, and the constraint reading it is applied
    // below this point — inside the body being bound — so it arrives spent.
    // Publishing it anyway puts a target in the carrier's heading that the
    // body no longer offers, and the two disagree the moment anything
    // reconciles against it. `republish_heading` copies the whole heading,
    // hygienic columns included, which is right where a carrier rides on to a
    // JOIN above and wrong here, where nothing above can address it.
    for column in identities.known_heading(input)? {
        if identities.addressing(column) == crate::names::Addressing::Hygienic {
            continue;
        }
        identities.republish_column(
            column,
            scope,
            crate::names::Republish::BoundaryExport,
            identities.published(column),
            identities.addressing(column),
            |_| {},
        );
    }
    Ok(scope)
}

fn group_ctes(
    ctes: Vec<ast_unresolved::CteBinding>,
) -> Result<(
    HashMap<CteGroupKey, Vec<ast_unresolved::CteBinding>>,
    Vec<CteGroupKey>,
)> {
    use crate::pipeline::asts::core::CteSubject;

    let mut authored_groups: HashMap<
        delightql_types::SqlIdentifier,
        Vec<ast_unresolved::CteBinding>,
    > = HashMap::new();
    let mut authored_order: Vec<delightql_types::SqlIdentifier> = Vec::new();
    for cte in &ctes {
        let CteSubject::Authored { name, .. } = &cte.subject else {
            continue;
        };
        let is_new = !authored_groups.contains_key(name);
        authored_groups
            .entry(name.clone())
            .or_default()
            .push(cte.clone());
        if is_new {
            authored_order.push(name.clone());
        }
    }
    validate_grouped_cte_dependencies(&authored_groups, &authored_order)?;

    let mut cte_groups: HashMap<CteGroupKey, Vec<ast_unresolved::CteBinding>> = HashMap::new();
    let mut cte_order: Vec<CteGroupKey> = Vec::new();

    for cte in ctes {
        let key = match &cte.subject {
            CteSubject::Authored { name, .. } => CteGroupKey::Authored(name.clone()),
            CteSubject::Generated { name } => CteGroupKey::Generated(name.clone()),
            CteSubject::Structural(scope) => CteGroupKey::Structural(*scope),
        };
        let is_new = !cte_groups.contains_key(&key);
        cte_groups.entry(key.clone()).or_default().push(cte);
        if is_new {
            cte_order.push(key);
        }
    }

    Ok((cte_groups, cte_order))
}

#[cfg(test)]
mod structural_cte_group_tests {
    use super::*;

    fn binding(
        subject: crate::pipeline::asts::core::CteSubject,
        relation_scope: crate::names::ScopeId,
    ) -> ast_unresolved::CteBinding {
        ast_unresolved::CteBinding {
            expression: ast_unresolved::Chain::read(
                ast_unresolved::Relation::Ground {
                    mention: ast_unresolved::GroundMention::Plan {
                        scope: relation_scope,
                        authored_name: None,
                        alias: None,
                    },
                    outer: false,
                    cpr_schema: (),
                },
                ast_unresolved::Access::All,
                (),
            ),
            subject,
            authority: crate::pipeline::asts::core::CteAuthority {
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: crate::pipeline::asts::core::provenance::CteOrigin::CompilerGenerated,
                resolution_owner:
                    crate::pipeline::asts::core::provenance::CteResolutionOwner::Entity,
            },
            recursion: (),
        }
    }

    fn authored(name: &str) -> crate::pipeline::asts::core::CteSubject {
        crate::pipeline::asts::core::CteSubject::Authored {
            name: delightql_types::SqlIdentifier::new(name),
            effect: crate::pipeline::asts::core::CteEffectDeclaration::Pure,
        }
    }

    #[test]
    fn structural_ctes_do_not_merge_by_diagnostic_name() {
        let identities = crate::names::Registry::new(&[]);
        let carrier = || {
            identities.mint_derived_scope(
                crate::names::ScopeOrigin::HoCarrier {
                    role: crate::names::HoRole::Argument,
                },
                crate::names::Hint::Prefix("ho"),
            )
        };
        let first = carrier();
        let second = carrier();

        let (groups, order) = group_ctes(vec![
            binding(authored("formal"), first),
            binding(
                crate::pipeline::asts::core::CteSubject::Structural(first),
                first,
            ),
            binding(
                crate::pipeline::asts::core::CteSubject::Structural(second),
                second,
            ),
        ])
        .expect("grouping should remain structural");

        assert_eq!(
            order,
            vec![
                CteGroupKey::Authored(delightql_types::SqlIdentifier::new("formal")),
                CteGroupKey::Structural(first),
                CteGroupKey::Structural(second),
            ]
        );
        assert_eq!(groups.len(), 3);
        assert_eq!(
            groups[&CteGroupKey::Authored(delightql_types::SqlIdentifier::new("formal"))].len(),
            1
        );
        assert_eq!(groups[&CteGroupKey::Structural(first)].len(), 1);
        assert_eq!(groups[&CteGroupKey::Structural(second)].len(), 1);
    }

    /// Two unstropped clauses that differ only by case are ONE subject under
    /// the identifier law, and a stropped spelling with different canonical
    /// bytes is another.
    #[test]
    fn authored_grouping_follows_the_identifier_law() {
        let identities = crate::names::Registry::new(&[]);
        let scope = identities.mint_derived_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::None,
        );

        let (groups, order) = group_ctes(vec![
            binding(authored("c"), scope),
            binding(authored("C"), scope),
            binding(
                crate::pipeline::asts::core::CteSubject::Authored {
                    name: delightql_types::SqlIdentifier::stropped("C"),
                    effect: crate::pipeline::asts::core::CteEffectDeclaration::Pure,
                },
                scope,
            ),
        ])
        .expect("fold-equal clauses group; a stropped case survivor does not");

        assert_eq!(order.len(), 2);
        assert_eq!(
            groups[&CteGroupKey::Authored(delightql_types::SqlIdentifier::new("c"))].len(),
            2
        );
        assert_eq!(
            groups[&CteGroupKey::Authored(delightql_types::SqlIdentifier::stropped("C"))].len(),
            1
        );
    }
}

/// The recursion decision, taken WHERE THE SELF-REFERENCE BINDS.
///
/// A binding's own scope is registered before its later clauses resolve,
/// so a self-reference is a resolved reference to that scope and nothing
/// else — no name comparison, no shadowing question (a shadowed name
/// resolves to the inner scope by construction, which is why this is
/// decided here and not over spellings later).
///
/// The answer is stored, decided exactly once, here. Deriving it again
/// independently in a later pass — an advisory walk, a structural
/// re-marking over the SQL AST, the transformer's own walk — invites
/// disagreement between the passes.
fn decide_recursion(
    expr: &ast_resolved::Chain,
    binding: crate::names::ScopeId,
    own_scopes: &[crate::names::ScopeId],
    identities: &crate::names::Registry,
) -> crate::pipeline::asts::vocabulary::RecursionState {
    use crate::pipeline::asts::vocabulary::RecursionState;
    // A group can answer to more than one scope while it is being built:
    // a later clause reaches the ANCHOR the first clause registered, and
    // the finished group publishes under that anchor only when the clauses
    // agree on a heading. A reference to either is a reference to this
    // definition, so both are asked; the decision names the binding.
    let mut own: Vec<crate::names::ScopeId> = own_scopes.to_vec();
    if !own.contains(&binding) {
        own.push(binding);
    }
    let mut finder = SelfReferenceFinder {
        own,
        identities,
        found: false,
    };
    walk_visit_relational(&mut finder, expr)
        .expect("self-reference detection is infallible (hooks never return Err)");
    if finder.found {
        RecursionState::Recursive {
            self_scope: binding,
        }
    } else {
        RecursionState::NonRecursive
    }
}

/// Finds any resolved relation standing on the binding's own scope,
/// anywhere in the body — a predicate subquery, a pipe argument, a
/// consulted view's body, a nested binding. The shared whole-tree descent
/// names every recursive edge once, so a self-reference cannot hide in a
/// field some hand-rolled walker forgot.
struct SelfReferenceFinder<'a> {
    own: Vec<crate::names::ScopeId>,
    identities: &'a crate::names::Registry,
    found: bool,
}

impl AstVisit<crate::pipeline::asts::core::Resolved> for SelfReferenceFinder<'_> {
    fn enter_relation(&mut self, rel: &ast_resolved::Relation) -> Result<Descent> {
        if let ast_resolved::Relation::Ground { cpr_schema, .. } = rel {
            if self
                .own
                .iter()
                .any(|own| self.identities.contains_scope(*cpr_schema, *own))
            {
                self.found = true;
                return Ok(Descent::Break);
            }
        }
        Ok(Descent::Continue)
    }
}

/// Run one subject's clause heads through the one assembler and project
/// each clause body through its own head.
///
/// A glob group comes back untouched — a glob head publishes the body's
/// heading, names and order as they are. A listed group comes back with
/// every head spent: the contract has been applied to the bodies, so what
/// leaves here is what the subject publishes.
fn apply_group_head(
    name: &str,
    group: Vec<ast_unresolved::CteBinding>,
) -> Result<Vec<ast_unresolved::CteBinding>> {
    use crate::pipeline::asts::core::definitions::{assemble, spend_heads};

    let heads: Vec<&crate::pipeline::asts::core::definitions::Head> =
        group.iter().map(|cte| &cte.authority.head).collect();
    let assembly = assemble(
        name,
        &heads,
        crate::pipeline::asts::core::definitions::GroundNaming::Refuse,
    )?;
    spend_heads(group, &assembly, name)
}

/// Trait abstracting CTE resolution + registration so that `resolve_cte_bindings`
/// can be shared between `resolve_query` (which uses a `ResolverFold`) and
/// `resolve_query_inline` (which calls `resolve_relational_expression_with_pipe_cfes`).
trait CteResolver {
    /// Resolve an unresolved relational expression, returning the resolved form
    /// plus any pipe-collected CFE definitions discovered during resolution.
    /// `owner` is the CTE's TYPED resolution ownership, keyed on the
    /// `Caller`/`Entity` enum: the caller-authored carrier CTEs resolve
    /// under Caller, never inferred from a naming convention or from
    /// construction site — the squished entity's own clause bodies are
    /// also compiler-CONSTRUCTED, but the entity scope owns their names.
    fn resolve_cte_expression(
        &mut self,
        owner: crate::pipeline::asts::core::provenance::CteResolutionOwner,
        expr: ast_unresolved::Chain,
    ) -> Result<ast_resolved::Chain>;

    /// Register a resolved CTE's schema so subsequent CTEs can reference it.
    /// The key is the authored spelling; the map compares by the identifier
    /// law, so a folded reference reaches it and a stropped case survivor
    /// stays distinct.
    fn register_cte(&mut self, name: delightql_types::SqlIdentifier, scope: crate::names::ScopeId);

    /// Preserve mutation-target provenance when a DML source is named by a
    /// CTE. The CTE heading alone cannot carry the authored `!!` marker.

    fn identities(&self) -> Rc<crate::names::Registry>;
}

/// `ResolverFold` as a CTE resolver — used by the top-level `resolve_query`.
impl CteResolver for ResolverFold<'_, '_> {
    fn resolve_cte_expression(
        &mut self,
        _owner: crate::pipeline::asts::core::provenance::CteResolutionOwner,
        expr: ast_unresolved::Chain,
    ) -> Result<ast_resolved::Chain> {
        let (resolved, _bubbled) = self.resolve_relational(expr)?;
        Ok(resolved)
    }

    fn register_cte(&mut self, name: delightql_types::SqlIdentifier, scope: crate::names::ScopeId) {
        self.registry.query_local.register_cte(name, scope);
    }

    fn identities(&self) -> Rc<crate::names::Registry> {
        Rc::clone(&self.registry.identities)
    }
}

/// Wrapper for inline resolution — used by `resolve_query_inline`.
struct InlineCteResolver<'a, 'db> {
    registry: &'a mut crate::resolution::EntityRegistry<'db>,
    outer_context: Option<&'a [crate::names::ColId]>,
    config: &'a ResolutionConfig,
    grounding: Option<&'a ast_unresolved::GroundedPath>,
}

impl CteResolver for InlineCteResolver<'_, '_> {
    fn resolve_cte_expression(
        &mut self,
        owner: crate::pipeline::asts::core::provenance::CteResolutionOwner,
        expr: ast_unresolved::Chain,
    ) -> Result<ast_resolved::Chain> {
        // The caller-authored carrier CTEs — the piped source, join input,
        // and HO arguments, TYPED Caller-owned at construction — resolve
        // under the CALLER's scope, while the entity's own body CTEs
        // (Entity-owned, from its file text) keep the entity scope. One
        // squished query, two honest scopes; typed ownership, never a
        // naming convention and never construction provenance.
        let caller_config;
        let config: &ResolutionConfig = match owner {
            crate::pipeline::asts::core::provenance::CteResolutionOwner::Caller {
                resolution_namespace,
            } => {
                caller_config = ResolutionConfig {
                    resolution_namespace,
                    ..self.config.clone()
                };
                &caller_config
            }
            crate::pipeline::asts::core::provenance::CteResolutionOwner::Entity => self.config,
        };
        let (resolved, _bubbled) = resolve_relational_expression_with_registry(
            expr,
            self.registry,
            self.outer_context,
            config,
            self.grounding,
        )?;
        Ok(resolved)
    }

    fn register_cte(&mut self, name: delightql_types::SqlIdentifier, scope: crate::names::ScopeId) {
        self.registry.query_local.register_cte(name, scope);
    }

    fn identities(&self) -> Rc<crate::names::Registry> {
        Rc::clone(&self.registry.identities)
    }
}

/// Resolve grouped CTE bindings, registering each CTE in the entity registry
/// so that later CTEs (and the main query) can reference earlier ones.
///
/// The `resolver` handles both expression resolution and CTE registration,
/// avoiding borrow conflicts by bundling both operations behind a single
/// `&mut self`.
///
/// The helper handles schema extraction, table-name transformation, CTE
/// registration, and multi-head UNION assembly.
fn resolve_cte_bindings(
    mut cte_groups: HashMap<CteGroupKey, Vec<ast_unresolved::CteBinding>>,
    cte_order: &[CteGroupKey],
    resolver: &mut dyn CteResolver,
) -> Result<Vec<ast_resolved::CteBinding>> {
    let mut resolved_ctes = Vec::new();

    for key in cte_order {
        let group = cte_groups
            .remove(key)
            .expect("CTE should exist after ordering - invariant violation");
        // The spelling, for registration and teaching — the author's or the
        // compiler's. A structural group has none: its key is the carrier
        // scope, and the assembler below sees only glob heads, which never
        // name the subject.
        let name = match key {
            CteGroupKey::Authored(name) | CteGroupKey::Generated(name) => Some(name.clone()),
            CteGroupKey::Structural(_) => None,
        };
        let teaching_name = name
            .as_ref()
            .map(|name| name.as_str().to_string())
            .unwrap_or_default();

        // The subject's clauses meet HERE, at the one assembler, before any
        // scope is minted: mixed head forms, arity, the per-position
        // name-offer contest, the Ground-Position rule, and output-heading
        // collision are decided once for the whole group, and each clause
        // body then carries the projection its head declares. A per-clause
        // head applied at build time could not see its siblings, so a
        // disagreement had nowhere to be caught and became a NULL-padded
        // union instead.
        let group = apply_group_head(&teaching_name, group)?;

        if group.len() == 1 {
            // Single CTE — resolve normally
            let cte = group
                .into_iter()
                .next()
                .expect("Group has len==1, must have element - invariant");
            let crate::pipeline::asts::core::CteAuthority {
                origin,
                resolution_owner,
                // Spent by apply_group_head: the group's contract is in the
                // bodies.
                head: _,
            } = cte.authority;
            let resolved_expr =
                resolver.resolve_cte_expression(resolution_owner.clone(), cte.expression)?;
            let mut cte_schema = extract_cpr_schema(&resolved_expr);
            cte_schema = match (key, &name) {
                (CteGroupKey::Authored(_) | CteGroupKey::Generated(_), Some(name)) => {
                    transform_schema_table_names(
                        cte_schema,
                        name,
                        origin,
                        crate::names::CteRole::Materialize,
                        &resolver.identities(),
                    )
                }
                (CteGroupKey::Structural(scope), _) => {
                    bind_schema_to_scope(cte_schema, *scope, &resolver.identities())?
                }
                (CteGroupKey::Authored(_) | CteGroupKey::Generated(_), None) => {
                    unreachable!("a named group key carries its spelling")
                }
            };
            if let Some(name) = &name {
                resolver.register_cte(name.clone(), cte_schema);
            }
            let recursion =
                decide_recursion(&resolved_expr, cte_schema, &[], &resolver.identities());

            resolved_ctes.push(ast_resolved::CteBinding {
                expression: resolved_expr,
                // The authored spelling and effect declaration end here:
                // the binding IS its scope from this point on. The head and
                // provenance were spent above; the phase deletes their slot.
                subject: cte_schema,
                authority: (),
                recursion,
            });
        } else {
            let (CteGroupKey::Authored(_), Some(name)) = (key, &name) else {
                return Err(DelightQLError::parse_error(
                    "a compiler-built CTE binding was defined more than once",
                ));
            };
            // Multiple CTEs with same name — create UNION
            let mut operands = Vec::new();
            let mut schemas = Vec::new();
            let mut all_schemas_same = true;

            // The scope a later head could already see, kept so the finished
            // group can answer under it rather than under a second one.
            let mut anchor_scope: Option<crate::names::ScopeId> = None;

            for (idx, cte) in group.iter().enumerate() {
                let resolved_expr = resolver.resolve_cte_expression(
                    cte.authority.resolution_owner.clone(),
                    cte.expression.clone(),
                )?;
                let expr_schema = extract_cpr_schema(&resolved_expr);

                // After first head, register the CTE so recursive heads can reference it
                if idx == 0 {
                    let mut base_schema = expr_schema.clone();
                    base_schema = transform_schema_table_names(
                        base_schema,
                        name,
                        cte.authority.origin,
                        crate::names::CteRole::Recursive,
                        &resolver.identities(),
                    );
                    anchor_scope = Some(base_schema);
                    resolver.register_cte(name.clone(), base_schema);
                }

                if !schemas.is_empty() {
                    if validate_union_compatible_schemas(
                        schemas[0],
                        expr_schema,
                        &resolver.identities(),
                    )
                    .is_err()
                    {
                        all_schemas_same = false;
                    }
                }

                schemas.push(expr_schema);
                operands.push(resolved_expr);
            }

            let operator = if all_schemas_same {
                ast_resolved::SetOperator::UnionAllPositional
            } else {
                ast_resolved::SetOperator::UnionCorresponding
            };

            // The clauses accumulate as a SEQUENCE of binary steps, so each
            // step publishes the heading its own two operands make. The
            // group's heading is the last step's — for a positional
            // accumulation that is the first clause's throughout, and for a
            // corresponding one the left-to-right merge, which is the order
            // the arms are written in.
            let mut step_schemas = Vec::with_capacity(schemas.len());
            let mut accumulated = schemas[0].clone();
            for schema in &schemas[1..] {
                accumulated = match operator {
                    ast_resolved::SetOperator::UnionCorresponding => build_corresponding_schema(
                        &[accumulated, schema.clone()],
                        &resolver.identities(),
                    )?,
                    _ => accumulated,
                };
                step_schemas.push(accumulated.clone());
            }
            let final_schema = accumulated;

            // A later head that referenced the name reached the anchor's
            // scope, so the finished group has to answer under that same one:
            // minting a second scope for the union strands the reference on a
            // table no WITH clause binds, which is how a recursive member ends
            // up naming `c_2`. The anchor can only answer for the group when
            // the heads agree on a heading — a corresponding union publishes
            // one the anchor does not have, and there the anchor was never a
            // reference target either, because a self-reference forces
            // agreement.
            let published = match anchor_scope.filter(|_| all_schemas_same) {
                Some(scope) => scope,
                None => transform_schema_table_names(
                    final_schema,
                    name,
                    group.first().map(|c| c.authority.origin).unwrap_or_default(),
                    crate::names::CteRole::Recursive,
                    &resolver.identities(),
                ),
            };
            resolver.register_cte(name.clone(), published);

            let mut operands = operands.into_iter();
            let mut union_expr = operands.next().expect("a CTE group has a first clause");
            for (arm, schema) in operands.zip(step_schemas) {
                union_expr = union_expr.bag_op(operator, arm, None, schema);
            }
            let recursion = decide_recursion(
                &union_expr,
                published,
                anchor_scope.as_ref().map_or(&[][..], std::slice::from_ref),
                &resolver.identities(),
            );

            resolved_ctes.push(ast_resolved::CteBinding {
                expression: union_expr,
                // The authored spelling and effect declaration end here:
                // the binding IS its scope from this point on. The head and
                // provenance were spent above; the phase deletes their slot.
                subject: published,
                authority: (),
                recursion,
            });
        }
    }

    Ok(resolved_ctes)
}

/// Resolve a full Query (which may contain CTEs)
///
/// Returns the resolved query along with connection routing information.
/// If tables from multiple connections are referenced, returns an error.
pub fn resolve_query(
    query: ast_unresolved::Query,
    schema: &dyn DatabaseSchema,
    system: Option<&crate::system::DelightQLSystem>,
    config: &ResolutionConfig,
    identities: std::rc::Rc<crate::names::Registry>,
) -> Result<ResolvedQueryResult> {
    // Create EntityRegistry from the schema (with optional system for namespace resolution)
    let mut registry = if let Some(sys) = system {
        crate::resolution::EntityRegistry::new_with_system(schema, sys, identities)
    } else {
        crate::resolution::EntityRegistry::new(schema, identities)
    };
    // Definitions are spent at their call sites during resolution — the
    // borrowed and query-scoped roads alike — so nothing pre-expands here.

    // All relational resolution goes through the fold. The fold delegates to
    // existing free functions; later steps absorb them.
    let mut fold = ResolverFold::new(&mut registry, config.clone(), None, None);

    let ast_unresolved::Query { cfes, ctes, body } = query;

    // A definition is registered as authored and spent at its call sites
    // during resolution; no carrier survives to later phases. Registered
    // before any CTE resolves, so a binding's body can call it.
    for cfe in cfes {
        refuse_empty_explicit_context(&cfe)?;
        fold.registry.query_local.register_scoped_cfe(cfe);
    }

    let resolved_ctes = if ctes.is_empty() {
        Vec::new()
    } else {
        let (cte_groups, cte_order) = group_ctes(ctes)?;
        resolve_cte_bindings(cte_groups, &cte_order, &mut fold)?
    };

    // Now resolve the body with all CTEs in registry
    let (resolved_body, _) = fold.resolve_relational(body)?;

    let resolved_query = ast_resolved::Query {
        cfes: (),
        ctes: resolved_ctes,
        body: resolved_body,
    };

    // Validate that all resolved tables belong to the same connection
    let connection_id = fold.registry.validate_single_connection()?;

    // THE INCHOATE LAW, applied over the whole resolved tree: an unaccessed
    // inchoate occurrence yields zero rows under its opaque displayed
    // heading, a name reaching a latent dimension refuses, and a positional
    // reach was its activation.
    let mut resolved_query = resolved_query;
    apply_inchoate_law(&mut resolved_query, &fold.registry.identities)?;

    Ok(ResolvedQueryResult {
        query: resolved_query,
        connection_id,
    })
}

/// THE INCHOATE LOWERING's resolution half (RULINGS sitting 26, amended by
/// POSITION REACHES WHAT NAMES CANNOT).
///
/// An occurrence written `R()` starts with every dimension latent. An
/// access-site act — a slot group, `(*)`, a later access step in its own
/// chain — activates it totally; a positional reference into it activates
/// it (recorded where ordinals resolve); a NAME reaches nothing, because a
/// latent dimension has none. What nothing activated is marked for the
/// zero-row lowering, and its columns are depublished so the displayed
/// heading spells the mints of dimensions nobody named.
fn apply_inchoate_law(
    query: &mut ast_resolved::Query,
    identities: &std::rc::Rc<crate::names::Registry>,
) -> Result<()> {
    use crate::pipeline::ast_visit::{walk_visit_query, AstVisit, Descent};

    struct Unaccessed<'r> {
        identities: &'r crate::names::Registry,
        latent: std::collections::HashSet<crate::names::ScopeId>,
    }
    impl AstVisit<crate::pipeline::asts::core::Resolved> for Unaccessed<'_> {
        fn enter_relational(&mut self, chain: &ast_resolved::Chain) -> Result<Descent> {
            use crate::pipeline::asts::core::{Access, Continuation, Grelex, Relation};
            let Grelex::Reference(Relation::Ground { cpr_schema, .. }) = &chain.head else {
                return Ok(Descent::Continue);
            };
            let mut steps = chain.continuations.iter();
            let Some(Continuation::Access {
                access: Access::Unasked,
                ..
            }) = steps.next()
            else {
                return Ok(Descent::Continue);
            };
            // A later access step in the occurrence's own chain is an
            // access-site act: `users() *` and `users() .(id)` are total
            // activation, spelled after the read.
            if steps.any(|step| matches!(step, Continuation::Access { .. })) {
                return Ok(Descent::Continue);
            }
            // A positional reach activated it where the ordinal resolved.
            if self.identities.ordinal_reached(*cpr_schema) {
                return Ok(Descent::Continue);
            }
            // Only an enumerable heading has dimensions to depublish; an
            // opaque passthrough keeps its own contract.
            if matches!(
                self.identities.heading(*cpr_schema),
                crate::names::HeadingKnowledge::Known(_)
            ) {
                self.latent.insert(*cpr_schema);
            }
            Ok(Descent::Continue)
        }
    }

    let mut unaccessed = Unaccessed {
        identities,
        latent: std::collections::HashSet::new(),
    };
    walk_visit_query(&mut unaccessed, query)?;
    if unaccessed.latent.is_empty() {
        return Ok(());
    }
    let latent = unaccessed.latent;

    // A NAME REACHES NOTHING LATENT. Any resolved occurrence of a latent
    // scope's column outside its own read was bound by name — positional
    // reaches activated their scope above — so it refuses with the teaching
    // rather than binding a dimension nobody activated.
    struct NameReach<'r> {
        identities: &'r crate::names::Registry,
        latent: &'r std::collections::HashSet<crate::names::ScopeId>,
        reached: Option<crate::names::ColId>,
    }
    impl AstVisit<crate::pipeline::asts::core::Resolved> for NameReach<'_> {
        fn enter_domain(&mut self, expression: &ast_resolved::DomainExpression) -> Result<Descent> {
            use crate::pipeline::asts::core::{NamedReference, Reference};
            if let ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                occurrence,
            ))) = expression
            {
                if self
                    .latent
                    .contains(&self.identities.scope_of(occurrence.column))
                {
                    self.reached.get_or_insert(occurrence.column);
                }
            }
            Ok(Descent::Continue)
        }
    }
    let mut names = NameReach {
        identities,
        latent: &latent,
        reached: None,
    };
    walk_visit_query(&mut names, query)?;
    if let Some(column) = names.reached {
        let mut text = String::new();
        identities.describe(
            self_scope_of(identities, column),
            &mut crate::names::Teaching(&mut text),
        );
        return Err(DelightQLError::validation_error_categorized(
            "inchoate/latent_name",
            format!(
                "the dimension is latent: '{text}()' names no columns until the \
                 occurrence is accessed"
            ),
            "access the relation — write `(*)` or a slot group — or reach the \
             dimension by position (`|N|`)",
        ));
    }

    for scope in &latent {
        identities.note_annihilated(*scope);
    }

    // THE DISPLAYED HEADING SPELLS MINTS. A terminal latent read — the
    // chain that is nothing but the read — publishes its dimensions under
    // née mints: a synthesized projection republishes each column into a
    // fresh unnamed occurrence, and baptism names what nobody did.
    republish_latent_terminals(query, &latent, identities);
    Ok(())
}

/// Append the née republication to every chain that IS a latent read and
/// nothing else.
fn republish_latent_terminals(
    query: &mut ast_resolved::Query,
    latent: &std::collections::HashSet<crate::names::ScopeId>,
    identities: &std::rc::Rc<crate::names::Registry>,
) {
    for cte in &mut query.ctes {
        republish_latent_terminal_chain(&mut cte.expression, latent, identities);
    }
    republish_latent_terminal_chain(&mut query.body, latent, identities);
}

fn republish_latent_terminal_chain(
    chain: &mut ast_resolved::Chain,
    latent: &std::collections::HashSet<crate::names::ScopeId>,
    identities: &std::rc::Rc<crate::names::Registry>,
) {
    use crate::pipeline::asts::core::{
        Access, ColumnOccurrence, Continuation, Grelex, NamedReference, Reference, Relation,
    };
    let Grelex::Reference(Relation::Ground { cpr_schema, .. }) = &chain.head else {
        return;
    };
    let scope = *cpr_schema;
    if !latent.contains(&scope) {
        return;
    }
    let [Continuation::Access {
        access: Access::Unasked,
        ..
    }] = chain.continuations.as_slice()
    else {
        return;
    };
    let crate::names::HeadingKnowledge::Known(columns) = identities.heading(scope) else {
        return;
    };
    let nee = identities.mint_scope(
        crate::names::ScopeOrigin::UserAlias { of: scope },
        crate::names::Hint::None,
        None,
    );
    let items: Vec<ast_resolved::OutItem> = columns
        .into_iter()
        .map(|column| {
            let published = identities.republish_column(
                column,
                nee,
                crate::names::Republish::Passthrough,
                None,
                crate::names::Addressing::Bare,
                |_| {},
            );
            ast_resolved::OutItem::One(ast_resolved::OneOut {
                expr: ast_resolved::OutValue::Domain(ast_resolved::DomainExpression::Reference(
                    Reference::Named(NamedReference(ColumnOccurrence {
                        column,
                        explicit_qualifier: false,
                    })),
                )),
                naming: None,
                output: Some(published),
            })
        })
        .collect();
    chain.continuations.push(Continuation::Pipe {
        operator: ast_resolved::PipeOp::Project(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(items)
                .expect("the boundary projection publishes at least one column"),
        ),
        named: (),
        cpr_schema: nee,
    });
}

fn self_scope_of(
    identities: &crate::names::Registry,
    column: crate::names::ColId,
) -> crate::names::ScopeId {
    identities.scope_of(column)
}

/// Resolve a Query using an existing registry context.
///
/// Used by view expansion to resolve view bodies (including CTEs)
/// within the outer query's resolution context. Unlike `resolve_query()`,
/// this takes an existing `EntityRegistry` instead of creating a new one,
/// so CTEs and tables visible in the outer context remain accessible.
pub(crate) fn resolve_query_inline(
    query: ast_unresolved::Query,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Query, BubbledState)> {
    // THE EXTENT IS THE ENTRANCE'S. Every complete inline query resolves
    // inside its own lexical binding extent: the bindings it introduces —
    // CTE registrations, a recursive rule's self-registration, and CFE
    // definitions — end when it returns, resolved and refused alike, so a
    // body's binding can never replace or outlive its caller's. No
    // unscoped implementation exists beside this one; a caller that
    // wanted bindings to escape would need a differently named operation
    // nobody has written.
    registry.with_binding_extent(|registry| {
        let ast_unresolved::Query { cfes, ctes, body } = query;

        // A definition is registered as authored and spent at its call sites
        // during resolution; no carrier survives to later phases. Registered
        // before any CTE resolves, so a binding's body can call it.
        for cfe in cfes {
            refuse_empty_explicit_context(&cfe)?;
            registry.query_local.register_scoped_cfe(cfe);
        }

        let resolved_ctes = if ctes.is_empty() {
            Vec::new()
        } else {
            let (cte_groups, cte_order) = group_ctes(ctes)?;

            // Resolve CTEs using the InlineCteResolver wrapper
            let mut inline_resolver = InlineCteResolver {
                registry: &mut *registry,
                outer_context,
                config,
                grounding,
            };
            resolve_cte_bindings(cte_groups, &cte_order, &mut inline_resolver)?
        };

        // Resolve the body with all CTEs registered
        let (resolved_body, bubbled) = resolve_relational_expression_with_registry(
            body,
            registry,
            outer_context,
            config,
            grounding,
        )?;

        Ok((
            ast_resolved::Query {
                cfes: (),
                ctes: resolved_ctes,
                body: resolved_body,
            },
            bubbled,
        ))
    })
}

/// `..{}` declares a capture of nothing, which is a regular definition
/// wearing a context marker; the marker must go.
fn refuse_empty_explicit_context(cfe: &ast_unresolved::CfeDefinition) -> Result<()> {
    if let crate::pipeline::asts::core::ContextMode::Explicit(captures) = &cfe.context_mode {
        if captures.is_empty() {
            return Err(DelightQLError::parse_error(format!(
                "CFE '{}' declares empty explicit context '..{{}}' but this is unnecessary. \
                 Remove the context marker entirely: {}:({}): ...",
                cfe.name,
                cfe.name,
                cfe.formals
                    .iter()
                    .map(|formal| formal.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }
    Ok(())
}

/// Collect the columns that sibling EXISTS subqueries in this scope publish,
/// read off the resolution that survives.
///
/// Interdependent EXISTS reference each other's tables — in
/// `+orders(...), +order_items(, orders.id = order_items.order_id)` the second
/// subquery's `orders.id` names a relation that only exists inside the first.
/// Those columns have to reach the correlation context, and the context must
/// carry the very occurrences the emitted statement establishes: resolving the
/// unresolved sibling a second time to read its heading mints a parallel set,
/// binds the reference to it, and then discards the relation that owned it,
/// which is how `orders_2.id` came to name a FROM entry no statement contains.
///
/// SCOPE-LOCAL: walks this scope's Filter spine and reaches each EXISTS's own
/// innermost source. It does not descend into arbitrary nested subquery scopes,
/// and it resolves nothing.
fn collect_exists_table_columns_in_scope(
    expr: &ast_resolved::Chain,
    identities: &crate::names::Registry,
    context: &mut Vec<crate::names::ColId>,
) -> Result<()> {
    for continuation in &expr.continuations {
        match continuation {
            ast_resolved::Continuation::Restrict { condition, .. } => {
                if let ast_resolved::TruthExpression::Existence(Existence {
                    relation: subquery,
                    ..
                }) = condition
                {
                    let source = resolved_innermost_source(subquery);
                    let scope = helpers::extraction::extract_cpr_schema(&source);
                    context.extend(identities.known_heading(scope)?);
                }
            }
            ast_resolved::Continuation::Member { rhs, .. } => {
                collect_exists_table_columns_in_scope(rhs, identities, context)?;
            }
            ast_resolved::Continuation::BagOp { arm, .. } => {
                collect_exists_table_columns_in_scope(arm, identities, context)?;
            }
            ast_resolved::Continuation::Access { .. }
            | ast_resolved::Continuation::Bound { .. }
            | ast_resolved::Continuation::Correlate { .. }
            | ast_resolved::Continuation::Destructure { .. }
            | ast_resolved::Continuation::Pipe { .. }
            | ast_resolved::Continuation::Structural(_) => {}
            ast_resolved::Continuation::ErJoin(_) => {
                unreachable!("ER chains are expanded during resolution")
            }
        }
    }
    Ok(())
}

/// The innermost source of a resolved subquery: peel `Filter` only, and stop at
/// any node that is itself a boundary. The resolved twin of
/// `extract_innermost_source`, and it stops in the same places for the same
/// reason — a `Pipe` publishes its own heading, so descending past one would
/// answer with a heading the subquery's table does not have.
fn resolved_innermost_source(expr: &ast_resolved::Chain) -> ast_resolved::Chain {
    let mut peeled = expr.continuations.as_slice();
    while let Some((
        ast_resolved::Continuation::Restrict { .. }
        | ast_resolved::Continuation::Bound { .. }
        | ast_resolved::Continuation::Destructure { .. },
        rest,
    )) = peeled.split_last()
    {
        peeled = rest;
    }
    ast_resolved::Chain {
        head: expr.head.clone(),
        continuations: peeled.to_vec(),
    }
}

/// Resolves a relational expression via `EntityRegistry`.
///
/// Thin wrapper: delegates to `ResolverFold::resolve_relational_impl`.
fn resolve_relational_expression_with_registry(
    expr: ast_unresolved::Chain,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    resolve_interior_expression(expr, registry, outer_context, config, grounding, None)
}

/// Which dequalifying run a correlation is answering, borrowed for the call.
#[derive(Clone, Copy)]
pub(crate) enum CorrelatingRun<'a> {
    Named(&'a [delightql_types::SqlIdentifier]),
    All,
}

/// The same, owned, while the interior is resolved and the access is spent.
enum OwnedCorrelatingRun {
    Named(Vec<delightql_types::SqlIdentifier>),
    All,
}

impl OwnedCorrelatingRun {
    fn borrow(&self) -> CorrelatingRun<'_> {
        match self {
            OwnedCorrelatingRun::Named(columns) => CorrelatingRun::Named(columns),
            OwnedCorrelatingRun::All => CorrelatingRun::All,
        }
    }
}

/// Same entry, carrying the self-name of the access whose interior this
/// is. Only `resolve_inner_relation` has one to pass.
fn resolve_interior_expression(
    expr: ast_unresolved::Chain,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
    interior_self: Option<crate::names::Sym>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    // LOOKING LEFT REACHES THE ENCLOSING ROW. A dequalifying access on the
    // interior's OWN head has nothing to its left inside, so the lvar it
    // renames onto is the outer one and the step is a correlation. Any other
    // position keeps its claimant: a member's dequalify is the join's USING,
    // and the pipe carrier correlates on its own road.
    // BOTH SPELLINGS OF THE RUN. `.(cols)` names the shared columns and `.*`
    // asks for every one there is; they are one step with two spellings, so
    // one reaching this road and the other not is the run answering
    // differently for the same query.
    let correlating = outer_context.and_then(|outer| {
        if !matches!(
            expr.head,
            ast_unresolved::Grelex::Reference(ast_unresolved::Relation::Ground { .. })
        ) {
            return None;
        }
        match expr.head_access()? {
            ast_unresolved::Access::Dequalify(columns) => {
                Some((OwnedCorrelatingRun::Named(columns.clone()), outer.to_vec()))
            }
            ast_unresolved::Access::DequalifyAll => {
                Some((OwnedCorrelatingRun::All, outer.to_vec()))
            }
            ast_unresolved::Access::Unasked
            | ast_unresolved::Access::All
            | ast_unresolved::Access::Slots(_) => None,
        }
    });
    let mut expr = expr;
    if correlating.is_some() {
        if let Some(ast_unresolved::Continuation::Access { access, .. }) =
            expr.continuations.first_mut()
        {
            *access = ast_unresolved::Access::All;
        }
    }

    let mut fold = ResolverFold::new(
        registry,
        config.clone(),
        outer_context.map(|c| c.to_vec()),
        grounding.cloned(),
    );
    fold.interior_self = interior_self;
    let (resolved, bubbled) = fold.resolve_relational(expr)?;
    let resolved = match &correlating {
        Some((run, outer)) => fold.correlate_using(resolved, run.borrow(), outer)?,
        None => resolved,
    };
    Ok((resolved, bubbled))
}

// ============================================================================
// ER-Rule Expansion
// ============================================================================

/// Extract the table name from an unresolved Relation.
/// The context for an ER expression, from its operator symbols. A bare
/// operator (the removed `under` dialect's spelling) refuses with the
/// symbol-form teaching; a chain names ONE context.
pub(crate) fn er_chain_context(
    contexts: &[Option<String>],
) -> Result<ast_unresolved::ErContextSpec> {
    let named: Vec<&String> = contexts.iter().flatten().collect();
    if named.is_empty() || contexts.iter().any(|c| c.is_none()) {
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/bare_operator",
            "the ER operators take their context as a symbol on the operator: \
             &(::your_context) for a direct edge, &&(::your_context) for the \
             transitive walk"
                .to_string(),
            "contexts are symbols; the edge set per context is finite and declared",
        ));
    }
    let first = named[0];
    if named.iter().any(|context| *context != first) {
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/mixed_contexts",
            format!(
                "one chain, one context — this chain names multiple contexts including ::{first}"
            ),
            "split the chain, or declare the edges in one context",
        ));
    }
    Ok(ast_unresolved::ErContextSpec {
        namespace: None,
        context_name: first.clone(),
    })
}

/// The edge-selection failure, in two teachings: an unknown context is
/// its own error (the edge set per context is finite and declared); a
/// known context without the requested pair enumerates what IS declared.
fn er_edge_miss_error(
    registry: &crate::resolution::EntityRegistry,
    context_name: &str,
    left_spelling: &str,
    right_spelling: &str,
) -> DelightQLError {
    let known = registry
        .consult
        .er_context_known(context_name)
        .unwrap_or(false);
    if !known {
        let contexts = registry.consult.list_er_contexts().unwrap_or_default();
        let listing = if contexts.is_empty() {
            "no contexts have declared edges in the enlisted scope".to_string()
        } else {
            format!(
                "contexts with declared edges: {}",
                contexts
                    .iter()
                    .map(|c| format!("::{c}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        return DelightQLError::validation_error_categorized(
            "grounding/er/unknown_context",
            format!("unknown context '::{context_name}' — {listing}"),
            "a context exists exactly where an edge declares it",
        );
    }
    let edges = registry
        .consult
        .lookup_er_rules_in_context(context_name)
        .unwrap_or_default();
    let listing = edges
        .iter()
        .map(|(l, r, _)| format!("{l} & {r}"))
        .collect::<Vec<_>>()
        .join("; ");
    DelightQLError::validation_error_categorized(
        "grounding/er/edge_miss",
        format!(
            "no edge declared for {left_spelling} & {right_spelling} in \
             '::{context_name}' — a term selects an edge by its exact canonical \
             spelling, and emptiness by absent declaration is an error, not a \
             result. Declared edges: {listing}"
        ),
        "restriction is downstream: select a declared edge, then filter its \
         relation",
    )
}

/// The endpoint's (table name, user alias) — the alias is OUTSIDE the
/// term: selection used the spelling, exports answer to the alias.
fn er_endpoint(read: &ast_unresolved::Chain) -> (String, Option<delightql_types::SqlIdentifier>) {
    match read.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier, alias, ..
                },
            ..
        }) => (identifier.name.to_string(), alias.clone()),
        _ => (String::new(), None),
    }
}

/// Publish an edge's boundary: of everything the body provides, keep the two
/// endpoints' columns, in endpoint order, each answering to its endpoint.
/// Returns the name of an endpoint the body publishes nothing for.
///
/// The body is a body like any other — resolved as written, its own law, no
/// ER dialect. The narrowing is NOT a projection the compiler writes over it:
/// spelling it as `|> (a.*, b.*)` appended to the body makes the compiler
/// author `a(*) … |> (a.*) |> (a.*)` whenever the author already projected,
/// which is the one shape the language refuses. Which endpoint a column
/// belongs to is a fact the arena already holds, so the boundary asks the
/// arena instead of asking by name past a boundary that consumed the name.
///
/// Which endpoint a column answers to is a fact about the COLUMN. One hop
/// exports both of its endpoints into a single boundary scope, so deciding
/// per scope can record only one of the two, and the other endpoint's columns
/// come out answering to the wrong name — which is invisible until a second
/// hop tries to pair on the shared endpoint and finds nothing answering to it.
/// The scope a column was born in is what tells the two apart; the scope it
/// sits in now is shared by both and tells them apart from nothing.
/// Stand the edge's boundary over its resolved body: one projection, built
/// from the occurrences the export selected, publishing the boundary scope.
///
/// Built resolved rather than appended as `|> (a.*, b.*)` before resolution.
/// The written form makes the compiler author `a(*) … |> (a.*) |> (a.*)`
/// whenever the body already projected — the one shape the language refuses —
/// and then needs a qualifier to survive a projection so its own output can
/// compile. Which occurrence belongs to which endpoint is a fact the arena
/// holds; asking it directly costs no licence.
fn er_boundary_projection(
    expr: ast_resolved::Chain,
    exported: &[crate::names::ColId],
    identities: &crate::names::Registry,
) -> ast_resolved::Chain {
    let Some(scope) = identities.common_scope(exported) else {
        return expr;
    };
    let items = exported
        .iter()
        .map(|column| {
            ast_resolved::OutItem::plain(
                ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence {
                        column: *column,
                        explicit_qualifier: false,
                    },
                ))),
                Some(*column),
            )
        })
        .collect();
    // An edge exports its endpoints, so the empty case is unreachable; it
    // answers with the body unchanged rather than an itemless projection.
    let Some(items) = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(items) else {
        return expr;
    };
    ast_resolved::Chain::pipe_builder(expr, scope)
        .with_projection(items)
        .build()
}

/// An edge whose body does not publish one of its endpoints, taught as the
/// pair-set violation it is rather than as whatever the narrowing tripped on.
fn er_pair_schema_error(missing: &str, context: &str, subject: String) -> DelightQLError {
    DelightQLError::validation_error_categorized(
        "grounding/er/pair_schema",
        format!(
            "{subject} in '::{context}' does not publish '{missing}' — an edge \
             is a PAIR-SET: its body may derive the pairs freely (filter, \
             helper joins, computed keys, aggregates) but its final heading \
             must carry both endpoints' columns; they are the edge's published \
             schema. Rename and narrow at the call site, after selection, not \
             inside the edge"
        ),
        "the published schema of an edge is schema(A) + schema(B); \
         the boundary exports those columns and hides the rest",
    )
}

fn er_export_endpoints(
    bubbled: &mut BubbledState,
    published: &[String],
    identities: &crate::names::Registry,
) -> Option<String> {
    let mut endpoints = Vec::with_capacity(published.len());
    for name in published {
        let Some(endpoint) = identities.known_sym(name, false) else {
            return Some(name.clone());
        };
        endpoints.push(endpoint);
    }
    let endpoint_of = |column: crate::names::ColId| {
        // The current answering reach is the endpoint's own published schema.
        // A derived endpoint may legitimately name its columns differently
        // from their catalog progenitors, so that boundary wins.
        if let Some(answer) = identities.answering_reach(column) {
            if endpoints.contains(&answer) {
                return Some(answer);
            }
        }

        // A projection may consume that qualifier while retaining the
        // endpoint column unchanged. Fall back to catalog birth only in that
        // case. A rename publishes something of the body's own making and is
        // not part of the endpoint schema.
        let progenitor = identities.progenitor(column);
        if identities.published_sym(column) != identities.published_sym(progenitor) {
            return None;
        }
        let birth = identities.scope_of(progenitor);
        let matches: Vec<_> = endpoints
            .iter()
            .copied()
            .filter(|endpoint| identities.answers_to(birth) == Some(*endpoint))
            .collect();
        match matches.as_slice() {
            [endpoint] => Some(*endpoint),
            [] | [..] => None,
        }
    };

    // An edge is a PAIR-SET: its body derives the pairs freely, but its final
    // heading has to carry both endpoints, because that heading IS the edge's
    // published schema. A body that renamed or projected an endpoint away
    // publishes no column born under it, and there is nothing to export.
    for (name, endpoint) in published.iter().zip(&endpoints) {
        if !bubbled
            .i_provide
            .iter()
            .any(|column| endpoint_of(*column) == Some(*endpoint))
        {
            return Some(name.clone());
        }
    }
    // ONE boundary, because an edge publishes ONE heading: schema(A) +
    // schema(B). Minting a boundary per input scope leaves the two endpoints'
    // columns with no scope in common, so nothing downstream can name the
    // edge's result — and the alias a caller wrote outside the term lands on
    // the base table instead of on the edge.
    //
    // A helper join's columns reach here too; the body may join whatever it
    // likes. Only the endpoints' columns cross, which is what "the boundary
    // exports those columns and hides the rest" means — the rest are dropped
    // by not being republished.
    let exported: Vec<_> = bubbled
        .i_provide
        .iter()
        .copied()
        .filter(|column| endpoint_of(*column).is_some())
        .collect();
    let input = identities.scope_of(exported[0]);
    let boundary = identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input,
            why: crate::names::WrapReason::Projection,
        },
        crate::names::Hint::None,
    );
    let inputs: Vec<_> = {
        let mut seen = Vec::new();
        for column in &exported {
            let scope = identities.scope_of(*column);
            if !seen.contains(&scope) {
                seen.push(scope);
            }
        }
        seen
    };
    let carried: Vec<_> = exported
        .iter()
        .map(|column| {
            // The endpoint is a qualifier, not a name: `depts.id` is how the
            // chain's exports are written, and `id` is still what the column
            // is called. Answering to the endpoint INSTEAD would make every
            // column of one endpoint answer to one name — all of them
            // ambiguous under it, and none reachable by its own.
            let addressing = endpoint_of(*column)
                .map(crate::names::Addressing::BareAnswering)
                .unwrap_or_else(|| identities.addressing(*column));
            identities.republish_column(
                *column,
                boundary,
                crate::names::Republish::BoundaryExport,
                identities.published(*column),
                addressing,
                |_| {},
            )
        })
        .collect();
    for column in &mut bubbled.i_provide {
        if let Some(position) = exported.iter().position(|source| source == column) {
            *column = carried[position];
        }
    }
    for scope in &mut bubbled.qualifier_scope {
        if inputs.contains(scope) {
            *scope = boundary;
        }
    }
    bubbled
        .i_provide
        .retain(|column| endpoint_of(*column).is_some());
    None
}

fn er_table_name(read: &ast_unresolved::Chain) -> Result<delightql_types::SqlIdentifier> {
    match read.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { identifier, .. },
            ..
        }) => Ok(identifier.name.clone()),
        _ => Err(DelightQLError::validation_error(
            "ER-join operands must be table references (e.g., users_t(*))",
            "Invalid ER-join operand",
        )),
    }
}

/// Expand a direct edge run by looking up ER-rules for each consecutive pair
/// and compiling their bodies through the pipeline.
///
/// For simple pairs (`A & B`): expands the single rule body directly.
///
/// For chains (`A & B & C`): parses each pair's rule body into an unresolved AST,
/// flattens them into (relations, conditions), deduplicates shared intermediate
/// tables, combines into a single unresolved expression, and resolves once.
/// This avoids the duplicate-intermediate-table problem that arises from resolving
/// each pair's body independently.
fn expand_er_join_chain(
    relations: Vec<ast_unresolved::Chain>,
    spellings: &[String],
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
    endpoints_only: Option<Vec<String>>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    if relations.len() < 2 || spellings.len() != relations.len() {
        return Err(DelightQLError::validation_error(
            "ER-join chain requires at least two relations",
            "Invalid ER-join chain",
        ));
    }

    // A self-pair edge publishes the same table twice: every column name
    // collides with its twin, the endpoint globs bind one operand twice,
    // and the rows come back silently self-paired. Refuse until the
    // boundary can mask the two sides apart.
    if let Some(published) = &endpoints_only {
        let mut seen: Vec<&String> = Vec::new();
        for name in published {
            if seen
                .iter()
                .any(|s| delightql_types::SqlIdentifier::str_eq(s, name))
            {
                return Err(DelightQLError::validation_error_categorized(
                    "grounding/er/self_pair",
                    format!(
                        "the edge publishes '{name}' at two endpoints — a \
                         self-pair edge's sides share every column name and \
                         cannot yet be masked apart, so the pairs would come \
                         back silently self-joined. Spell one side as a \
                         renamed rule view (boss(*) :- employees(*)) and \
                         declare the edge over the distinct terms"
                    ),
                    "an edge's published schema is schema(A) + schema(B); \
                     the two sides must be distinguishable",
                ));
            }
            seen.push(name);
        }
    }

    // The alias is OUTSIDE the term: selection used the spellings;
    // exports answer to the endpoint aliases, threaded after resolution.
    let (left_endpoint_name, left_endpoint_alias) = er_endpoint(&relations[0]);
    let (right_endpoint_name, right_endpoint_alias) =
        er_endpoint(relations.last().expect("len checked"));

    // If no resolution_namespace is set, use enlisted-scope edge lookup only.
    // Edges from non-enlisted namespaces are NOT visible at the call site —
    // the caller must enlist!() the namespace that declares them.
    // (When resolution_namespace IS set, lookup_er_rule_for_namespace handles scoping.)
    let effective_config: std::borrow::Cow<'_, ResolutionConfig>;
    if config.resolution_namespace.is_none() {
        let engaged_rule =
            registry
                .consult
                .lookup_er_rule(&context.context_name, &spellings[0], &spellings[1])?;
        if let Some(rule) = engaged_rule {
            effective_config = std::borrow::Cow::Owned(ResolutionConfig {
                resolution_namespace: Some(rule.namespace.clone()),
                ..config.clone()
            });
        } else {
            effective_config = std::borrow::Cow::Borrowed(config);
        }
    } else {
        effective_config = std::borrow::Cow::Borrowed(config);
    }
    let config = &*effective_config;

    // For the simple pair case (A & B), just expand the single rule body
    if relations.len() == 2 {
        let (resolved_expr, mut bubbled) = expand_single_er_pair(
            &spellings[0],
            &spellings[1],
            context,
            registry,
            outer_context,
            config,
            grounding,
        )?;
        if let Some(published) = &endpoints_only {
            if let Some(missing) =
                er_export_endpoints(&mut bubbled, published, &registry.identities)
            {
                return Err(er_pair_schema_error(
                    &missing,
                    &context.context_name,
                    format!("the edge body for ({}, {})", spellings[0], spellings[1]),
                ));
            }
        }
        // The boundary stands BEFORE the aliases are threaded: an alias a
        // caller wrote outside the term names the EDGE, and threading it into
        // a body with no boundary over it renames the base table instead.
        let resolved_expr = if endpoints_only.is_some() {
            er_boundary_projection(resolved_expr, &bubbled.i_provide, &registry.identities)
        } else {
            resolved_expr
        };
        let (resolved_expr, bubbled) = er_thread_endpoint_aliases(
            resolved_expr,
            bubbled,
            (&left_endpoint_name, &left_endpoint_alias),
            (&right_endpoint_name, &right_endpoint_alias),
            &registry.identities,
        );
        return Ok((resolved_expr, bubbled));
    }

    // For chains (A & B & C & ...), combine all pair bodies into one expression.
    //
    // Each pair's body is something like: `A(*), B(*), A.id = B.aid`
    // For chains, consecutive pairs share an intermediate table (B appears in both
    // (A,B) and (B,C) bodies). We flatten all bodies, deduplicate the shared tables,
    // and build one combined expression that resolves cleanly through the pipeline.
    let mut all_relations: Vec<ast_unresolved::Chain> = Vec::new();
    let mut all_conditions: Vec<ast_unresolved::TruthExpression> = Vec::new();
    let mut seen_table_names: std::collections::HashSet<delightql_types::SqlIdentifier> =
        std::collections::HashSet::new();

    for i in 0..relations.len() - 1 {
        let left_name = spellings[i].clone();
        let right_name = spellings[i + 1].clone();

        let body_query = parse_er_rule_body(
            &left_name,
            &right_name,
            context,
            registry,
            grounding,
            config.resolution_namespace.as_deref(),
        )?;

        // Extract the relational expression from the query
        let body_expr = match body_query.into_bare_body() {
            Ok(expr) => expr,
            Err(_) => return Err(DelightQLError::validation_error(
                format!(
                    "ER-rule body for ({}, {}) in context '{}' contains CTEs (not supported in chains)",
                    left_name, right_name, context.context_name,
                ),
                "Invalid ER-rule body",
            )),
        };

        // Flatten the body into relations and conditions
        let pair_desc = format!("{left_name} & {right_name} in '::{}'", context.context_name);
        let (body_rels, body_conds) = flatten_unresolved_body(body_expr, &pair_desc)?;

        // Merge relations. Adjacent bodies share EXACTLY their common
        // endpoint (this body's left term, introduced by the previous
        // body): that one occurrence deduplicates, once. Any OTHER
        // repeat — a self-join inside a body, a helper relation used by
        // two bodies, a cyclic chain revisiting an endpoint — cannot be
        // aliased apart during composition, and dropping it silently
        // rewrites the join, so it refuses.
        // The spelling carries the term shape ("components(*)"); the
        // shared occurrence is keyed by the endpoint's TABLE name.
        let shared_table = delightql_types::SqlIdentifier::new(er_endpoint(&relations[i]).0);
        let mut shared_endpoint_budget = if i > 0 { 1usize } else { 0 };
        for read in body_rels {
            if let Ok(name) = er_table_name(&read) {
                if seen_table_names.insert(name.clone()) {
                    all_relations.push(read);
                } else if shared_endpoint_budget > 0 && name == shared_table {
                    shared_endpoint_budget -= 1;
                } else {
                    return Err(DelightQLError::validation_error_categorized(
                        "grounding/er/chain_shared_repeat",
                        format!(
                            "composing the chain repeats relation '{name}' beyond \
                             the shared endpoint — the edge body for {pair_desc} \
                             reintroduces it after an earlier body (or the same \
                             body) already did. Adjacent edge bodies share only \
                             their common endpoint; other repeats cannot be \
                             aliased apart during composition. Restructure the \
                             bodies, or call the edges directly with &"
                        ),
                        "a chain merges adjacent bodies on their shared endpoint only",
                    ));
                }
            } else {
                // Non-Ground relation — keep it unconditionally
                all_relations.push(read);
            }
        }

        // Keep all conditions (conditions from different pairs don't duplicate)
        all_conditions.extend(body_conds);
    }

    // Rebuild a single unresolved expression from the combined parts
    let combined_expr = rebuild_flat_expression(all_relations, all_conditions)?;

    // Add self-aliases and resolve through the pipeline (same path as single-pair)
    let combined_query =
        add_self_aliases_to_query(ast_unresolved::Query::relational(combined_expr));

    // Determine effective grounding (same logic as expand_single_er_pair)
    // Use the first pair's rule to determine the namespace for grounding.
    let first_rule = if let Some(ns) = &config.resolution_namespace {
        registry.consult.lookup_er_rule_for_namespace(
            &context.context_name,
            &spellings[0],
            &spellings[1],
            ns,
        )?
    } else {
        registry
            .consult
            .lookup_er_rule(&context.context_name, &spellings[0], &spellings[1])?
    }
    .ok_or_else(|| {
        er_edge_miss_error(
            registry,
            &context.context_name,
            &spellings[0],
            &spellings[1],
        )
    })?;
    let rule_ns = first_rule.namespace.clone();
    let auto_grounding = registry
        .consult
        .get_namespace_default_data_ns(&rule_ns)
        .and_then(|data_ns_fq| {
            let data_ns = ast_unresolved::NamespacePath::from_fq_string(&data_ns_fq).ok()?;
            let grounded_ns = ast_unresolved::NamespacePath::from_fq_string(&rule_ns).ok()?;
            Some(ast_unresolved::GroundedPath {
                data_ns,
                grounded_ns: vec![grounded_ns],
            })
        });
    let effective_grounding = auto_grounding.as_ref().or(grounding);

    let (resolved_query, body_bubbled) = resolve_query_inline(
        combined_query,
        registry,
        outer_context,
        config,
        effective_grounding,
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Error resolving ER-chain body in context '{}': {}",
                context.context_name, e
            ),
            e.to_string(),
        )
    })?;

    match resolved_query.into_bare_body() {
        Ok(expr) => {
            let mut body_bubbled = body_bubbled;
            if let Some(published) = &endpoints_only {
                if let Some(missing) =
                    er_export_endpoints(&mut body_bubbled, published, &registry.identities)
                {
                    return Err(er_pair_schema_error(
                        &missing,
                        &context.context_name,
                        "the composed chain".to_string(),
                    ));
                }
            }
            let expr = if endpoints_only.is_some() {
                er_boundary_projection(expr, &body_bubbled.i_provide, &registry.identities)
            } else {
                expr
            };
            let (expr, body_bubbled) = er_thread_endpoint_aliases(
                expr,
                body_bubbled,
                (&left_endpoint_name, &left_endpoint_alias),
                (&right_endpoint_name, &right_endpoint_alias),
                &registry.identities,
            );
            Ok((expr, body_bubbled))
        }
        Err(_) => Err(DelightQLError::validation_error(
            format!(
                "ER-chain body in context '{}' resolved to a non-relational query",
                context.context_name,
            ),
            "Invalid ER-chain body",
        )),
    }
}

/// The transformer reads the pipe node's OWN schema, not the bubbled
/// state — after stamping and alias-threading, the endpoints-only
/// pipe's schema syncs from the threaded columns so both agree.
fn er_sync_pipe_schema(
    expr: ast_resolved::Chain,
    bubbled: &BubbledState,
    identities: &crate::names::Registry,
) -> ast_resolved::Chain {
    let mut expr = expr;
    if let Some(ast_resolved::Continuation::Pipe { cpr_schema, .. }) = expr.continuations.last_mut()
    {
        *cpr_schema = identities
            .common_scope(&bubbled.i_provide)
            .expect("an ER endpoint projection has one heading");
    }
    expr
}

/// Rename endpoint tables to their user aliases throughout a resolved
/// ER result (exports answer to the alias; selection already happened
/// by spelling).
fn er_thread_endpoint_aliases(
    mut expr: ast_resolved::Chain,
    mut bubbled: BubbledState,
    left: (&str, &Option<delightql_types::SqlIdentifier>),
    right: (&str, &Option<delightql_types::SqlIdentifier>),
    identities: &crate::names::Registry,
) -> (ast_resolved::Chain, BubbledState) {
    if let (name, Some(alias)) = left {
        expr = rename_in_resolved_expr(expr, name, alias, identities);
        rename_bubbled_columns(&mut bubbled, name, alias, identities);
    }
    if let (name, Some(alias)) = right {
        expr = rename_in_resolved_expr(expr, name, alias, identities);
        rename_bubbled_columns(&mut bubbled, name, alias, identities);
    }
    (expr, bubbled)
}

/// `&&` composes RELATIONS, not syntax: each hop
/// of the walked path resolves WHOLE through the ordinary direct-edge
/// road (its body free per the pair-set ruling, its boundary export
/// publishing schema(X) + schema(Y)), the hops join on the shared
/// endpoint's full heading (null-safe, row identity by value), and the
/// result publishes the outer endpoints only. Bodies never merge, so
/// nothing is flattened, restricted, or deduplicated.
fn compose_er_chain_relational(
    path: &[String],
    hop_tables: &[String],
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    use ast_resolved::Chain as RE;
    let identity_arena = std::rc::Rc::clone(&registry.identities);
    // Reads the name, never interns it: this asks a question per column per
    // hop, and interning appends a spelling every time it is asked.
    let belongs_to = |column: crate::names::ColId, table: &str| {
        let Some(endpoint) = identity_arena.known_sym(table, false) else {
            return false;
        };
        matches!(
            identity_arena.addressing(column),
            crate::names::Addressing::AnsweringTo(answer)
                | crate::names::Addressing::BareAnswering(answer)
                if answer == endpoint
        )
    };
    let mut composed: Option<RE> = None;
    let mut all_columns: Vec<crate::names::ColId> = Vec::new();
    let mut chain_scope: Option<crate::names::ScopeId> = None;

    for i in 0..path.len() - 1 {
        let (hop_expr, mut hop_bubbled) = expand_single_er_pair(
            &path[i],
            &path[i + 1],
            context,
            registry,
            outer_context,
            config,
            grounding,
        )?;
        // The answering channel is the pairing key: stamp each hop's
        // columns with their endpoint names (the len==2 road's caller
        // does this; here we are the caller).
        //
        // A hop that publishes no column for an endpoint refuses here, as the
        // direct road does. The export builds nothing when it answers a
        // missing endpoint, so a caller that dropped the answer would compose
        // the un-narrowed, un-stamped body and carry a path whose outer
        // endpoint quietly went missing — a wrong answer where the same edge
        // written directly is a refusal.
        if let Some(missing) = er_export_endpoints(
            &mut hop_bubbled,
            &[hop_tables[i].clone(), hop_tables[i + 1].clone()],
            &registry.identities,
        ) {
            return Err(er_pair_schema_error(
                &missing,
                &context.context_name,
                format!(
                    "the edge body for ({}, {})",
                    hop_tables[i],
                    hop_tables[i + 1]
                ),
            ));
        }
        let source_scope = registry
            .identities
            .common_scope(&hop_bubbled.i_provide)
            .unwrap_or_else(|| {
                registry.identities.mint_scope(
                    crate::names::ScopeOrigin::AnonRelation,
                    crate::names::Hint::None,
                    None,
                )
            });
        let chain = *chain_scope.get_or_insert(source_scope);
        let hop_scope = registry.identities.mint_derived_scope(
            crate::names::ScopeOrigin::ErHop {
                chain,
                hop: i as u16,
            },
            crate::names::Hint::Prefix("_er_hop"),
        );
        hop_bubbled.i_provide = hop_bubbled
            .i_provide
            .into_iter()
            .map(|column| {
                registry.identities.republish_column(
                    column,
                    hop_scope,
                    crate::names::Republish::Rename,
                    registry.identities.published(column),
                    registry.identities.addressing(column),
                    |_| {},
                )
            })
            .collect();
        let hop_rel = RE::relation(ast_resolved::Relation::InnerRelation {
            pattern: ast_resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                identifier: ast_resolved::QualifiedName {
                    namespace_path: ast_resolved::NamespacePath::empty(),
                    name: hop_tables[i].clone().into(),
                },
                subquery: Box::new(hop_expr),
                is_consulted_view: false,
            },
            preminted_scope: Some(hop_scope),
            alias: None,
            outer: false,
            cpr_schema: hop_scope,
        });

        let hop_columns = hop_bubbled.i_provide.clone();

        if let Some(acc) = composed.take() {
            let shared = &hop_tables[i];
            let mut conditions = Vec::new();
            for right in hop_columns
                .iter()
                .copied()
                .filter(|column| belongs_to(*column, shared))
            {
                let name = registry.identities.published_sym(right);
                let matches: Vec<_> = all_columns
                    .iter()
                    .copied()
                    .filter(|column| {
                        belongs_to(*column, shared)
                            && registry.identities.published_sym(*column) == name
                    })
                    .collect();
                let [left] = matches.as_slice() else {
                    crate::probe::probing!(er, {
                        crate::probe::probe!(
                            er,
                            "hop {i} shared {shared:?}: {right:?} published={name:?} \
                             found {} candidates among {} accumulated",
                            matches.len(),
                            all_columns.len()
                        );
                        for column in &all_columns {
                            crate::probe::probe!(
                                er,
                                "  acc {column:?} published={:?} addressing={:?} answers_to={}",
                                registry.identities.published_sym(*column),
                                registry.identities.addressing(*column),
                                belongs_to(*column, shared)
                            );
                        }
                    });
                    return Err(DelightQLError::validation_error(
                        "ER chain composition cannot uniquely pair a shared-endpoint column",
                        "Invalid ER-chain composition",
                    ));
                };
                let reference = |column| {
                    Box::new(ast_resolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(ColumnOccurrence {
                            column,
                            explicit_qualifier: false,
                        }),
                    )))
                };
                conditions.push(ast_resolved::TruthExpression::Comparison(Comparison {
                    operator: crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual,
                    left: reference(*left),
                    right: reference(right),
                }));
            }
            let join_scope = registry.identities.mint_derived_scope(
                crate::names::ScopeOrigin::Wrap {
                    input: registry
                        .identities
                        .common_scope(&all_columns)
                        .expect("accumulated ER chain has one heading"),
                    why: crate::names::WrapReason::Projection,
                },
                crate::names::Hint::None,
            );
            let mut joined = Vec::new();
            for column in all_columns.iter().chain(&hop_columns) {
                joined.push(registry.identities.republish_column(
                    *column,
                    join_scope,
                    crate::names::Republish::Passthrough,
                    registry.identities.published(*column),
                    registry.identities.addressing(*column),
                    |_| {},
                ));
            }
            let mut joined_expr = acc.then(ast_resolved::Continuation::Member {
                rhs: hop_rel,
                correlation: None,
                join_type: None,
                cpr_schema: join_scope,
            });
            for condition in conditions {
                joined_expr = joined_expr.then(ast_resolved::Continuation::Restrict {
                    condition: condition,
                    origin: crate::pipeline::asts::core::FilterOrigin::Generated,
                    cpr_schema: join_scope,
                });
            }
            all_columns = joined;
            composed = Some(joined_expr);
        } else {
            all_columns = hop_columns;
            composed = Some(hop_rel);
        }
    }

    let expr = composed.expect("path has at least two spellings");
    let first_table = &hop_tables[0];
    let last_table = hop_tables.last().expect("nonempty");
    let kept: Vec<_> = all_columns
        .iter()
        .copied()
        .filter(|column| belongs_to(*column, first_table) || belongs_to(*column, last_table))
        .collect();
    let output_scope = registry.identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input: registry
                .identities
                .common_scope(&all_columns)
                .expect("composed ER chain has one heading"),
            why: crate::names::WrapReason::Projection,
        },
        crate::names::Hint::None,
    );
    let published: Vec<_> = kept
        .iter()
        .map(|column| {
            registry.identities.republish_column(
                *column,
                output_scope,
                crate::names::Republish::Passthrough,
                registry.identities.published(*column),
                registry.identities.addressing(*column),
                |_| {},
            )
        })
        .collect();
    // Each kept endpoint column republishes itself: the composed chain names
    // nothing anew, so every item carries the occurrence just minted for it.
    let projection: Vec<ast_resolved::OutItem> = kept
        .iter()
        .zip(published.iter())
        .map(|(column, output)| {
            ast_resolved::OutItem::plain(
                ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence {
                        column: *column,
                        explicit_qualifier: false,
                    },
                ))),
                Some(*output),
            )
        })
        .collect();
    let expr = expr.then(ast_resolved::Continuation::Pipe {
        operator: ast_resolved::PipeOp::Project(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(projection)
                .expect("the boundary projection publishes at least one column"),
        ),
        named: (),
        cpr_schema: output_scope,
    });

    Ok((
        expr,
        BubbledState::resolved(published, &registry.identities),
    ))
}

/// Flatten an unresolved relational expression into a list of relations and conditions.
/// Walks the Join/Filter tree and collects all leaf Relation nodes and all Filter conditions.
/// Transitive composition (&&) merges edge bodies BEFORE resolution, so a
/// body that carries anything beyond join/filter normal form — a pipe
/// stage, a set operation, a nested edge call — cannot be merged without
/// discarding its semantics; it refuses instead (dropped semantics or a
/// downstream panic is not an admissible fallback).
fn flatten_unresolved_body(
    expr: ast_unresolved::Chain,
    pair_desc: &str,
) -> Result<(
    Vec<ast_unresolved::Chain>,
    Vec<ast_unresolved::TruthExpression>,
)> {
    let mut reads = Vec::new();
    let mut conditions = Vec::new();
    flatten_unresolved_body_inner(expr, &mut reads, &mut conditions, pair_desc)?;
    Ok((reads, conditions))
}

/// A body's READS, not its bare relations: a mention travels with the access
/// its own parens asked for, so merging two bodies cannot leave one holding a
/// relation nobody parameterized.
fn flatten_unresolved_body_inner(
    expr: ast_unresolved::Chain,
    reads: &mut Vec<ast_unresolved::Chain>,
    conditions: &mut Vec<ast_unresolved::TruthExpression>,
    pair_desc: &str,
) -> Result<()> {
    let refuse = |what: &str| -> DelightQLError {
        DelightQLError::validation_error_categorized(
            "grounding/er/chain_normal_form",
            format!(
                "the edge body for {pair_desc} carries {what} — a transitive \
                     chain (&&) merges its edge bodies into one join before \
                     resolution, so each body must be join/filter normal form: \
                     relations and conditions only. Restructure the edge body, \
                     or call the edge directly with &"
            ),
            "transitive composition is structural: bodies merge before resolution",
        )
    };

    if !matches!(expr.head, ast_unresolved::Grelex::Reference(_)) {
        return Err(refuse("an anonymous table"));
    }
    let (read, steps) = expr.split_read();
    reads.push(read);
    for continuation in steps {
        match continuation {
            ast_unresolved::Continuation::Member { rhs, .. } => {
                flatten_unresolved_body_inner(rhs, reads, conditions, pair_desc)?;
            }
            ast_unresolved::Continuation::Restrict { condition, .. } => {
                conditions.push(condition);
            }
            // A second access is a step on the read's RESULT — it reshapes,
            // which normal form does not admit.
            ast_unresolved::Continuation::Access { .. } => {
                return Err(refuse("a further dimension access"));
            }
            ast_unresolved::Continuation::Bound { .. } => {
                return Err(refuse("a row bound (#<n)"));
            }
            ast_unresolved::Continuation::Correlate { .. } => {
                return Err(refuse("a whole-heading correlation"));
            }
            ast_unresolved::Continuation::Destructure { .. } => {
                return Err(refuse("a destructure (~=)"));
            }
            ast_unresolved::Continuation::Pipe { .. } => {
                return Err(refuse("a pipe stage (|>)"));
            }
            ast_unresolved::Continuation::Structural(step) => {
                return Err(refuse(match &step.form {
                    ast_unresolved::StructuralForm::Ordering { .. } => "an ordering (#(…))",
                    ast_unresolved::StructuralForm::Reposition { .. } => "a reposition (*[…])",
                    ast_unresolved::StructuralForm::Meta => "a meta-ize (^)",
                    ast_unresolved::StructuralForm::Witness { .. } => "a witness (+/\\+)",
                    ast_unresolved::StructuralForm::SignedWitness => "a signed witness (+-)",
                    ast_unresolved::StructuralForm::Drill { .. } => "an interior drill (.col(…))",
                    ast_unresolved::StructuralForm::Narrow { .. } => {
                        "a narrowing destructure (.col{…})"
                    }
                }));
            }
            ast_unresolved::Continuation::BagOp { .. } => {
                return Err(refuse("a set operation"));
            }
            ast_unresolved::Continuation::ErJoin(_) => {
                return Err(refuse("a nested edge call"));
            }
        }
    }
    Ok(())
}

/// Rebuild a flat unresolved expression from a list of relations and conditions.
/// Produces a left-deep Join tree of all relations, then wraps with Filter layers
/// for each condition.
fn rebuild_flat_expression(
    reads: Vec<ast_unresolved::Chain>,
    conditions: Vec<ast_unresolved::TruthExpression>,
) -> Result<ast_unresolved::Chain> {
    // Build left-deep join tree from the reads
    let mut iter = reads.into_iter();
    let mut expr = iter.next().ok_or_else(|| {
        DelightQLError::validation_error(
            "ER chain composed to zero relations — the normal-form and \
                 shared-endpoint refusals should have caught this earlier; \
                 this is a dql bug",
            "Invalid ER-join chain",
        )
    })?;
    for read in iter {
        expr = expr.then(ast_unresolved::Continuation::Member {
            rhs: read,
            correlation: None,
            join_type: None,
            cpr_schema: (),
        });
    }

    // Wrap with filter layers for each condition
    for cond in conditions {
        expr = expr.then(ast_unresolved::Continuation::Restrict {
            condition: cond,
            origin: crate::pipeline::asts::core::FilterOrigin::UserWritten,
            cpr_schema: (),
        });
    }

    Ok(expr)
}

/// Look up an ER-rule for a pair and parse its body into an unresolved Query.
/// Shared between `expand_single_er_pair` and the chain expansion in `expand_er_join_chain`.
fn parse_er_rule_body(
    left_name: &str,
    right_name: &str,
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    grounding: Option<&ast_unresolved::GroundedPath>,
    resolution_namespace: Option<&str>,
) -> Result<ast_unresolved::Query> {
    let rule = if let Some(ns) = resolution_namespace {
        registry.consult.lookup_er_rule_for_namespace(
            &context.context_name,
            left_name,
            right_name,
            ns,
        )?
    } else {
        registry
            .consult
            .lookup_er_rule(&context.context_name, left_name, right_name)?
    }
    .ok_or_else(|| er_edge_miss_error(registry, &context.context_name, left_name, right_name))?;

    let rule_ns = rule.namespace.clone();

    let auto_grounding = registry
        .consult
        .get_namespace_default_data_ns(&rule_ns)
        .and_then(|data_ns_fq| {
            let data_ns = ast_unresolved::NamespacePath::from_fq_string(&data_ns_fq).ok()?;
            let grounded_ns = ast_unresolved::NamespacePath::from_fq_string(&rule_ns).ok()?;
            Some(ast_unresolved::GroundedPath {
                data_ns,
                grounded_ns: vec![grounded_ns],
            })
        });

    let effective_grounding = auto_grounding.as_ref().or(grounding);

    if let Some(grounding) = effective_grounding {
        grounding::expand_consulted_view(&rule.definition, grounding).map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Error expanding ER-rule body for ({}, {}) in context '{}': {}",
                    left_name, right_name, context.context_name, e
                ),
                e.to_string(),
            )
        })
    } else {
        let group = crate::ddl::reconstruct::group(&rule.definition).map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Error parsing ER-rule body for ({}, {}) in context '{}': {}",
                    left_name, right_name, context.context_name, e
                ),
                e.to_string(),
            )
        })?;
        let mut clauses = group.spend_heads()?;
        let clause = clauses.remove(0);
        clause.into_query().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "ER-rule body for ({}, {}) in context '{}' is not a relational expression",
                left_name, right_name, context.context_name,
            ))
        })
    }
}

/// Expand a single ER pair (A, B) by looking up the rule and compiling its body.
fn expand_single_er_pair(
    left_name: &str,
    right_name: &str,
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    // Parse the rule body into an unresolved AST
    let query = parse_er_rule_body(
        left_name,
        right_name,
        context,
        registry,
        grounding,
        config.resolution_namespace.as_deref(),
    )?;

    // Add self-aliases to Ground relations in the body (e.g., users_t(*) → users_t(*) as users_t).
    // Without this, ConsultedView expansion assigns auto-generated aliases (t0, t1...)
    // which break qualified references like `users_t.id` in the body's predicates.
    let query = add_self_aliases_to_query(query);

    // Determine effective grounding for resolution
    let rule = if let Some(ns) = &config.resolution_namespace {
        registry.consult.lookup_er_rule_for_namespace(
            &context.context_name,
            left_name,
            right_name,
            ns,
        )?
    } else {
        registry
            .consult
            .lookup_er_rule(&context.context_name, left_name, right_name)?
    }
    .ok_or_else(|| er_edge_miss_error(registry, &context.context_name, left_name, right_name))?;
    let rule_ns = rule.namespace.clone();
    let auto_grounding = registry
        .consult
        .get_namespace_default_data_ns(&rule_ns)
        .and_then(|data_ns_fq| {
            let data_ns = ast_unresolved::NamespacePath::from_fq_string(&data_ns_fq).ok()?;
            let grounded_ns = ast_unresolved::NamespacePath::from_fq_string(&rule_ns).ok()?;
            Some(ast_unresolved::GroundedPath {
                data_ns,
                grounded_ns: vec![grounded_ns],
            })
        });
    let effective_grounding = auto_grounding.as_ref().or(grounding);

    // Resolve the parsed body through the pipeline.
    // The body is a complete relational expression (e.g., a join with conditions).
    // We inline the resolved expression directly — no ConsultedView wrapper needed.
    let (resolved_query, body_bubbled) =
        resolve_query_inline(query, registry, outer_context, config, effective_grounding).map_err(
            |e| {
                DelightQLError::database_error(
                    format!(
                        "Error resolving ER-rule body for ({}, {}) in context '{}': {}",
                        left_name, right_name, context.context_name, e
                    ),
                    e.to_string(),
                )
            },
        )?;

    // Extract the relational expression from the resolved query.
    match resolved_query.into_bare_body() {
        Ok(expr) => Ok((expr, body_bubbled)),
        Err(_) => Err(DelightQLError::validation_error(
            format!(
                "ER-rule body for ({}, {}) in context '{}' resolved to a non-relational query (CTEs in ER-rule bodies are not supported)",
                left_name, right_name, context.context_name,
            ),
            "Invalid ER-rule body",
        )),
    }
}

/// Add self-aliases to Ground relations in a query that don't already have aliases.
/// Transforms `table(*)` into `table(*) as table`. This ensures ConsultedView expansion
/// preserves the original table name as the SQL alias, so qualified references
/// (like `table.col`) in predicates continue to resolve correctly.
fn add_self_aliases_to_query(mut query: ast_unresolved::Query) -> ast_unresolved::Query {
    query.body = add_self_aliases_to_expr(query.body);
    query
}

/// Self-aliasing reaches the joined relations and stops where the chain
/// stops being a plain conjunction: a pipe publishes its own heading, so a
/// relation under one is no longer a self-reference to baptize.
fn add_self_aliases_to_expr(mut expr: ast_unresolved::Chain) -> ast_unresolved::Chain {
    // The trailing run of conjunctive steps is what self-aliasing reaches.
    // Rebuilding it BY VALUE is what lets the run be rewritten without a
    // stand-in relation standing in the slot for a statement: an unresolved
    // ground read must say how it is addressed, and there is no honest thing
    // for a placeholder to say.
    // The head's own access is not a step: it says what the read asks for,
    // so the run reaches past it to the relation it names.
    let span = expr.head_span();
    let stop = expr.continuations[span..]
        .iter()
        .rposition(|continuation| {
            !matches!(
                continuation,
                ast_unresolved::Continuation::Member { .. }
                    | ast_unresolved::Continuation::Restrict { .. }
                    | ast_unresolved::Continuation::Bound { .. }
                    | ast_unresolved::Continuation::Destructure { .. }
            )
        })
        .map_or(span, |index| span + index + 1);
    let reached_head = stop == span;
    expr.continuations = std::mem::take(&mut expr.continuations)
        .into_iter()
        .enumerate()
        .map(|(index, continuation)| match continuation {
            ast_unresolved::Continuation::Member {
                rhs,
                correlation,
                join_type,
                cpr_schema,
            } if index >= stop => ast_unresolved::Continuation::Member {
                rhs: add_self_aliases_to_expr(rhs),
                correlation,
                join_type,
                cpr_schema,
            },
            other => other,
        })
        .collect();
    if reached_head {
        if let ast_unresolved::Grelex::Reference(rel) = expr.head {
            expr.head = ast_unresolved::Grelex::Reference(add_self_alias_to_relation(rel));
        }
    }
    expr
}

fn add_self_alias_to_relation(rel: ast_unresolved::Relation) -> ast_unresolved::Relation {
    match rel {
        ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier,
                    alias: None,
                    mutation_target,
                    passthrough,
                },
            outer,
            cpr_schema,
        } => ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                alias: Some(identifier.name.clone()),
                identifier,
                mutation_target,
                passthrough,
            },
            outer,
            cpr_schema,
        },
        other => other,
    }
}

#[cfg(test)]
mod self_alias_tests {
    use super::*;

    fn ground(name: &str) -> ast_unresolved::Chain {
        ast_unresolved::Chain::read(
            ast_unresolved::Relation::Ground {
                mention: ast_unresolved::GroundMention::named(ast_unresolved::QualifiedName {
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                    name: name.into(),
                }),
                outer: false,
                cpr_schema: (),
            },
            ast_unresolved::Access::All,
            (),
        )
    }

    fn member(chain: ast_unresolved::Chain, rhs: ast_unresolved::Chain) -> ast_unresolved::Chain {
        chain.then(ast_unresolved::Continuation::Member {
            rhs,
            correlation: None,
            join_type: None,
            cpr_schema: (),
        })
    }

    fn qualify(chain: ast_unresolved::Chain) -> ast_unresolved::Chain {
        chain.then(ast_unresolved::Continuation::Access {
            access: ast_unresolved::Access::All,
            cpr_schema: (),
        })
    }

    /// The aliases the walk baptized, head first, `None` where it left the
    /// relation unnamed.
    fn aliases(expr: &ast_unresolved::Chain) -> Vec<Option<String>> {
        let mut out = vec![head_alias(&expr.head)];
        for continuation in &expr.continuations {
            if let ast_unresolved::Continuation::Member { rhs, .. } = continuation {
                out.extend(aliases(rhs));
            }
        }
        out
    }

    fn head_alias(head: &ast_unresolved::Grelex) -> Option<String> {
        match head {
            ast_unresolved::Grelex::Reference(ast_unresolved::Relation::Ground {
                mention, ..
            }) => mention.alias().map(|alias| alias.to_string()),
            _ => None,
        }
    }

    /// Every relation of a plain conjunction is baptized. The types cannot
    /// say this: the walk decides which steps it reaches.
    #[test]
    fn a_plain_conjunction_baptizes_every_relation() {
        let expr = member(member(ground("a"), ground("b")), ground("c"));
        assert_eq!(
            aliases(&add_self_aliases_to_expr(expr)),
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string())
            ]
        );
    }

    /// A pipe publishes its own heading, so the relation under one is no
    /// longer a self-reference to baptize — and the relations AFTER the pipe
    /// still are. This is the boundary the walk draws, and the one the
    /// by-value rewrite has to keep drawing in the same place.
    #[test]
    fn a_pipe_stops_the_baptism_and_the_steps_after_it_resume() {
        let expr = member(qualify(member(ground("a"), ground("b"))), ground("c"));
        assert_eq!(
            aliases(&add_self_aliases_to_expr(expr)),
            vec![None, None, Some("c".to_string())]
        );
    }

    /// An alias the author wrote is not overwritten.
    #[test]
    fn an_authored_alias_survives() {
        let mut written = ground("a");
        if let ast_unresolved::Grelex::Reference(ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named { alias, .. },
            ..
        }) = &mut written.head
        {
            *alias = Some("mine".into());
        }
        assert_eq!(
            aliases(&add_self_aliases_to_expr(written)),
            vec![Some("mine".to_string())]
        );
    }
}

/// Expand a transitive edge by building a graph of all ER-rules in the
/// context, finding a path from left to right, and expanding that path as a
/// direct edge run.
fn expand_er_transitive_join(
    left: ast_unresolved::Chain,
    right: ast_unresolved::Chain,
    left_spelling: &str,
    right_spelling: &str,
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[crate::names::ColId]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Chain, BubbledState)> {
    // Extract table names (and alias/access) from endpoints
    let (left_name, left_alias) = match left.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier, alias, ..
                },
            ..
        }) => (identifier.name.to_string(), alias.clone()),
        _ => {
            return Err(DelightQLError::validation_error(
                "Left side of && must be a table reference",
                "Invalid ER-transitive-join operand",
            ))
        }
    };
    let (right_name, right_alias) = match right.as_read_relation() {
        Some(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Named {
                    identifier, alias, ..
                },
            ..
        }) => (identifier.name.to_string(), alias.clone()),
        _ => {
            return Err(DelightQLError::validation_error(
                "Right side of && must be a table reference",
                "Invalid ER-transitive-join operand",
            ))
        }
    };

    // Build graph from all ER-rules in context (scoped to namespace if qualified).
    // ER-rules from non-enlisted namespaces are NOT visible at the call site —
    // the caller must enlist!() the namespace to access its ER-rules.
    let (rules, effective_config) = if let Some(ns) = &config.resolution_namespace {
        let r = registry
            .consult
            .lookup_er_rules_in_context_for_namespace(&context.context_name, ns)?;
        (r, std::borrow::Cow::Borrowed(config))
    } else {
        let r = registry
            .consult
            .lookup_er_rules_in_context(&context.context_name)?;
        if r.is_empty() {
            return Err(er_edge_miss_error(
                registry,
                &context.context_name,
                left_spelling,
                right_spelling,
            ));
        }
        // Check for cross-namespace ambiguity
        let namespaces: std::collections::HashSet<&str> = r
            .iter()
            .map(|(_, _, entity)| entity.namespace.as_str())
            .collect();
        if namespaces.len() > 1 {
            let ns_list: Vec<&str> = namespaces.into_iter().collect();
            return Err(DelightQLError::validation_error(
                format!(
                    "Ambiguous ER-context '{}': rules found in multiple namespaces ({}). \
                     Engage exactly one namespace or use qualified access (ns.view(*)).",
                    context.context_name,
                    ns_list.join(", "),
                ),
                "Ambiguous ER-context across namespaces",
            ));
        }
        // Single namespace — scope all downstream lookups to it
        let discovered_ns = r[0].2.namespace.clone();
        let scoped_config = ResolutionConfig {
            resolution_namespace: Some(discovered_ns),
            ..config.clone()
        };
        (r, std::borrow::Cow::Owned(scoped_config))
    };

    if rules.is_empty() {
        return Err(er_edge_miss_error(
            registry,
            &context.context_name,
            left_spelling,
            right_spelling,
        ));
    }

    // Build adjacency list (undirected graph — rules are symmetric)
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    for (left_t, right_t, _) in &rules {
        adjacency
            .entry(left_t.clone())
            .or_default()
            .push(right_t.clone());
        adjacency
            .entry(right_t.clone())
            .or_default()
            .push(left_t.clone());
    }

    // BFS over SPELLINGS: the graph's nodes are canonical spellings —
    // an endpoint participates exactly when its written spelling is a
    // declared edge term.
    let path = bfs_path(&adjacency, left_spelling, right_spelling)?;

    // Convert the spelling path to chain relations. Endpoints keep the
    // caller's relations (alias threading); interior hops are entity
    // boundaries whose Relation is only a carrier — the pair bodies
    // supply the real joined relations. A hop's table name is its
    // spelling's functor head.
    let chain_relations: Vec<ast_unresolved::Chain> = path
        .iter()
        .enumerate()
        .map(|(i, spelling)| {
            if i == 0 && left.as_read_relation().is_some() {
                return left.clone();
            }
            if i == path.len() - 1 && right.as_read_relation().is_some() {
                return right.clone();
            }
            let head = spelling.split('(').next().unwrap_or(spelling).trim();
            ast_unresolved::Chain::read(
                ast_unresolved::Relation::Ground {
                    mention: ast_unresolved::GroundMention::named(ast_unresolved::QualifiedName {
                        namespace_path: ast_unresolved::NamespacePath::empty(),
                        name: head.into(),
                    }),
                    outer: false,
                    cpr_schema: (),
                },
                ast_unresolved::Access::All,
                (),
            )
        })
        .collect();

    // Endpoints only: intermediate hops contribute nothing to the
    // schema.
    if path.len() > 2 {
        // Relational composition: each hop resolves whole, hops join on
        // the shared endpoint's heading, outer endpoints publish.
        let hop_tables: Vec<String> = path
            .iter()
            .map(|spelling| {
                spelling
                    .split('(')
                    .next()
                    .unwrap_or(spelling)
                    .trim()
                    .to_string()
            })
            .collect();
        let (expr, mut bubbled) = compose_er_chain_relational(
            &path,
            &hop_tables,
            context,
            registry,
            outer_context,
            &effective_config,
            grounding,
        )?;
        if let Some(missing) = er_export_endpoints(
            &mut bubbled,
            &[left_name.clone(), right_name.clone()],
            &registry.identities,
        ) {
            return Err(er_pair_schema_error(
                &missing,
                &context.context_name,
                "the composed chain".to_string(),
            ));
        }
        let (expr, bubbled) = er_thread_endpoint_aliases(
            expr,
            bubbled,
            (&left_name, &left_alias),
            (&right_name, &right_alias),
            &registry.identities,
        );
        let expr = er_sync_pipe_schema(expr, &bubbled, &registry.identities);
        return Ok((expr, bubbled));
    }
    // Adjacent pair: the direct road, endpoints only.
    expand_er_join_chain(
        chain_relations,
        &path,
        context,
        registry,
        outer_context,
        &effective_config,
        grounding,
        Some(vec![left_name.clone(), right_name.clone()]),
    )
}

/// Rename a table's alias and all qualifier references throughout a resolved
/// expression tree. Takes ownership and returns the modified expression.
/// Used to apply user aliases from `&&` endpoints after the chain is resolved.
fn rename_in_resolved_expr(
    mut expr: ast_resolved::Chain,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
    identities: &crate::names::Registry,
) -> ast_resolved::Chain {
    // Renaming reaches the conjoined relations and stops where the chain
    // stops being a plain conjunction: past a pipe the heading is the
    // pipe's own, and nothing there answers to the old name.
    let mut reached_head = true;
    let mut prefix_len = 0;
    for (index, continuation) in expr.continuations.iter().enumerate().rev() {
        match continuation {
            ast_resolved::Continuation::Member { .. }
            | ast_resolved::Continuation::Restrict { .. } => {}
            _ => {
                reached_head = false;
                prefix_len = index + 1;
                break;
            }
        }
    }
    for continuation in expr.continuations[prefix_len..].iter_mut() {
        match continuation {
            ast_resolved::Continuation::Member {
                rhs, cpr_schema, ..
            } => {
                // The placeholder stands in the slot for one statement while
                // the relation is rewritten and put back. The member already
                // carries what that slot publishes, so the placeholder says
                // the same thing rather than inventing a relation.
                let moved_schema = *cpr_schema;
                let renamed = rename_in_resolved_expr(
                    std::mem::replace(
                        rhs,
                        ast_resolved::Chain::ground(placeholder_head(moved_schema)),
                    ),
                    old_name,
                    new_name,
                    identities,
                );
                *rhs = renamed;
                *cpr_schema = rename_schema(*cpr_schema, old_name, new_name, identities);
            }
            ast_resolved::Continuation::Restrict { cpr_schema, .. } => {
                *cpr_schema = rename_schema(*cpr_schema, old_name, new_name, identities);
            }
            _ => {}
        }
    }
    if reached_head {
        if let ast_resolved::Grelex::Reference(rel) = expr.head {
            expr.head = ast_resolved::Grelex::Reference(match rel {
                ast_resolved::Relation::Ground {
                    mention,
                    outer,
                    cpr_schema,
                } => ast_resolved::Relation::Ground {
                    mention,
                    outer,
                    cpr_schema: rename_schema(cpr_schema, old_name, new_name, identities),
                },
                ast_resolved::Relation::ConsultedView {
                    body,
                    scoped,
                    outer,
                } => {
                    let old = identities.canonical(identities.intern(old_name, false));
                    let answers_to_old =
                        identities
                            .heading(scoped)
                            .columns_seen()
                            .into_iter()
                            .any(|column| {
                                matches!(
                                    identities.addressing(column),
                                    crate::names::Addressing::AnsweringTo(answer)
                                        | crate::names::Addressing::BareAnswering(answer)
                                        if answer == old
                                )
                            });
                    if answers_to_old {
                        ast_resolved::Relation::ConsultedView {
                            body,
                            scoped: rename_schema(scoped, old_name, new_name, identities),
                            outer,
                        }
                    } else {
                        ast_resolved::Relation::ConsultedView {
                            body,
                            scoped,
                            outer,
                        }
                    }
                }
                ast_resolved::Relation::InnerRelation {
                    pattern,
                    preminted_scope,
                    alias,
                    outer,
                    cpr_schema,
                } => {
                    let current = alias.as_ref().map(|a| a.to_string()).unwrap_or_default();
                    if current == old_name {
                        ast_resolved::Relation::InnerRelation {
                            pattern,
                            preminted_scope,
                            alias: Some(new_name.clone()),
                            outer,
                            cpr_schema: rename_schema(cpr_schema, old_name, new_name, identities),
                        }
                    } else {
                        ast_resolved::Relation::InnerRelation {
                            pattern,
                            preminted_scope,
                            alias,
                            outer,
                            cpr_schema,
                        }
                    }
                }
                other => other,
            });
        }
    }
    expr
}

/// A stand-in head used only while a member's chain is moved out for
/// rewriting; it never survives the statement that creates it.
fn placeholder_head(scope: crate::names::ScopeId) -> ast_resolved::Grelex {
    ast_resolved::Grelex::Reference(ast_resolved::Relation::ground(false, scope))
}

/// Rename table references in a relation's published scope.
fn rename_schema(
    input: crate::names::ScopeId,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
    identities: &crate::names::Registry,
) -> crate::names::ScopeId {
    let old = identities.canonical(identities.intern(old_name, false));
    if identities.answers_to(input) != Some(old) {
        return input;
    }
    let spelling = identities.intern(new_name.as_str(), new_name.is_stropped());
    let output = identities.mint_derived_scope(
        crate::names::ScopeOrigin::UserAlias { of: input },
        crate::names::Hint::User(spelling),
    );
    identities.republish_heading(input, output, crate::names::Republish::Rename);
    output
}

/// Rename table references in both halves of the bubbled lexical state.
fn rename_bubbled_columns(
    bubbled: &mut BubbledState,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
    identities: &crate::names::Registry,
) {
    let old = identities.canonical(identities.intern(old_name, false));
    let spelling = identities.intern(new_name.as_str(), new_name.is_stropped());
    let mut scopes = Vec::new();
    for column in &bubbled.i_provide {
        let scope = identities.scope_of(*column);
        let answering = matches!(
            identities.addressing(*column),
            crate::names::Addressing::AnsweringTo(answer)
                | crate::names::Addressing::BareAnswering(answer)
                if answer == old
        );
        if (identities.answers_to(scope) == Some(old) || answering) && !scopes.contains(&scope) {
            scopes.push(scope);
        }
    }
    let new = identities.canonical(spelling);
    for input in scopes {
        // The rename rides each column's ANSWERING channel, never the scope:
        // both endpoints share one boundary heading, so a scope answering to
        // the alias would put the other endpoint's columns under it too, and
        // the second endpoint's rename would shadow the first's entirely.
        // Only a column answering the old endpoint name changes its answer;
        // its neighbors keep theirs.
        let output = identities.mint_derived_scope(
            crate::names::ScopeOrigin::UserAlias { of: input },
            crate::names::Hint::None,
        );
        let old_heading = identities.heading(input).columns_seen();
        let new_heading: Vec<_> = old_heading
            .iter()
            .map(|&column| {
                let addressing = match identities.addressing(column) {
                    crate::names::Addressing::AnsweringTo(answer) if answer == old => {
                        crate::names::Addressing::AnsweringTo(new)
                    }
                    crate::names::Addressing::BareAnswering(answer) if answer == old => {
                        crate::names::Addressing::BareAnswering(new)
                    }
                    other => other,
                };
                identities.republish_column(
                    column,
                    output,
                    crate::names::Republish::Rename,
                    identities.published(column),
                    addressing,
                    |_| {},
                )
            })
            .collect();
        for column in &mut bubbled.i_provide {
            if let Some(position) = old_heading.iter().position(|old| old == column) {
                *column = new_heading[position];
            }
        }
        for scope in &mut bubbled.qualifier_scope {
            if *scope == input {
                *scope = output;
            }
        }
    }
}

/// Path-finding in the ER graph: enumerate ALL simple paths between the
/// endpoints. Exactly one → that path; zero → no-path error; two or
/// more → the ambiguity error, regardless of relative length — the
/// contract is "if multiple paths exist, the query fails", so a direct
/// edge never silently outranks a longer business path. Enumeration
/// must be exhaustive: a search that stops early (at the shortest, or
/// with a global visited set that suppresses paths sharing an
/// intermediate node) refuses some competitor shapes and silently
/// selects through others, which is worse than either consistent rule.
fn bfs_path(adjacency: &HashMap<String, Vec<String>>, from: &str, to: &str) -> Result<Vec<String>> {
    if from == to {
        return Err(DelightQLError::validation_error(
            "ER-transitive join endpoints must be different tables",
            "Same-table transitive join",
        ));
    }

    // ER contexts are hand-authored and small; simple-path enumeration is
    // cheap there. The expansion cap is a refuse-loudly backstop for a
    // pathologically dense context — uniqueness that cannot be verified
    // is reported, never assumed.
    const MAX_EXPANSIONS: usize = 100_000;
    let mut expansions = 0usize;

    let mut found_paths: Vec<Vec<String>> = Vec::new();
    let mut stack: Vec<Vec<String>> = vec![vec![from.to_string()]];

    while let Some(path) = stack.pop() {
        let current = path.last().unwrap();
        if let Some(neighbors) = adjacency.get(current.as_str()) {
            for neighbor in neighbors {
                expansions += 1;
                if expansions > MAX_EXPANSIONS {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "ER-context too dense to verify a unique join path \
                             from '{}' to '{}'; spell the join explicitly with `&`.",
                            from, to,
                        ),
                        "ER path search cap",
                    ));
                }
                if neighbor == to {
                    let mut p = path.clone();
                    p.push(neighbor.clone());
                    found_paths.push(p);
                } else if !path.contains(neighbor) {
                    let mut p = path.clone();
                    p.push(neighbor.clone());
                    stack.push(p);
                }
            }
        }
    }

    // Deterministic order (shortest first) — the adjacency map is a
    // HashMap, so discovery order is not stable across runs.
    found_paths.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));

    match found_paths.len() {
        0 => Err(DelightQLError::validation_error(
            format!(
                "No path from '{}' to '{}' in ER-context. \
                 Check that ER-rules connect these tables (directly or transitively).",
                from, to,
            ),
            "No ER path",
        )),
        1 => Ok(found_paths.into_iter().next().unwrap()),
        _ => {
            let path_strs: Vec<String> = found_paths.iter().map(|p| p.join(" -> ")).collect();
            Err(DelightQLError::validation_error(
                format!(
                    "Ambiguous: {} paths from '{}' to '{}':\n  {}",
                    found_paths.len(),
                    from,
                    to,
                    path_strs.join("\n  "),
                ),
                "Ambiguous ER path",
            ))
        }
    }
}

/// THE ONE AUTHORITY on which columns a pivot's keys come from.
///
/// It runs after resolution because that is the earliest point at which the
/// question can be asked at all: a pivot key is matched by PUBLISHED symbol,
/// and an ordinal has no name until the heading is known. Asking earlier as
/// well, and merging, gave the two addressings two different laws — `|2| in
/// (…)` and `subject in (…)` over the same column then disagreed about
/// whether an `IN` beneath an `or` supplies the columns.
///
/// A pivot's keys become the output HEADING, so only a genuinely exhaustive
/// constraint may supply them: an `IN` under `Or` leaves the other arm free
/// to admit rows outside the key set, and one under `Not` or a negated `IN`
/// excludes rather than enumerates. Those shapes yield nothing here, and the
/// pivot refuses for want of a matching predicate rather than silently
/// dropping the columns they would have omitted.
fn extract_in_predicate_values_from_resolved(
    source: &ast_resolved::Chain,
    identities: &crate::names::Registry,
) -> HashMap<crate::names::Sym, Vec<String>> {
    let mut result = HashMap::new();
    scan_resolved_for_in_predicates(source, &mut result, identities);
    result
}

#[stacksafe::stacksafe]
fn scan_resolved_for_in_predicates(
    expr: &ast_resolved::Chain,
    result: &mut HashMap<crate::names::Sym, Vec<String>>,
    identities: &crate::names::Registry,
) {
    for continuation in &expr.continuations {
        match continuation {
            ast_resolved::Continuation::Restrict { condition, .. } => {
                extract_in_from_resolved_boolean(condition, result, identities);
            }
            ast_resolved::Continuation::Member { rhs, .. } => {
                scan_resolved_for_in_predicates(rhs, result, identities);
            }
            ast_resolved::Continuation::BagOp { arm, .. } => {
                scan_resolved_for_in_predicates(arm, result, identities);
            }
            ast_resolved::Continuation::Access { .. }
            | ast_resolved::Continuation::Bound { .. }
            | ast_resolved::Continuation::Correlate { .. }
            | ast_resolved::Continuation::Destructure { .. }
            | ast_resolved::Continuation::Pipe { .. }
            | ast_resolved::Continuation::Structural(_) => {}
            ast_resolved::Continuation::ErJoin(_) => {
                unreachable!("ER chains should be resolved before IN predicate scanning")
            }
        }
    }
}

#[stacksafe::stacksafe]
fn extract_in_from_resolved_boolean(
    expr: &ast_resolved::TruthExpression,
    result: &mut HashMap<crate::names::Sym, Vec<String>>,
    identities: &crate::names::Registry,
) {
    match expr {
        // ONE construct, one carrier: `in` is the same relational membership
        // before and after resolution, so this post-resolution scan matches
        // the form the author wrote. It used to need both spellings, and
        // matching only one of them found nothing.
        ast_resolved::TruthExpression::RelationalMembership(RelationalMembership {
            probe,
            relation: subquery,
            negated: false,
            ..
        }) => {
            // Extract the resolved column name from LHS
            let column = match probe.sole_value() {
                Some(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence { column, .. }),
                ))) => Some(*column),
                // Non-column LHS (function call, literal, parenthesized, etc.) — can't
                // provide a column name for pivot optimization. Dispensation: any new
                // DomainExpression variant would also not be a bare column reference.
                _ => None,
            };
            if let Some(column) = column {
                // Walk through Pipe/Qualify wrappers to find the anonymous table
                let inner = unwrap_resolved_pipe(subquery.as_ref());
                if let Some(rows) = extract_literal_rows_from_resolved(&inner) {
                    if !rows.is_empty() {
                        if let Some(name) = identities.published_sym(column) {
                            result.insert(name, rows);
                        }
                    }
                }
            }
        }
        // THE SET SPELLING, READ AFTER RESOLUTION. `c in ("a"; "b")` carries
        // its values inline, and the unresolved scan already reads it — but
        // only when the left side is a NAME. A column addressed by position
        // has no name until the heading is known, so `|2| in ("a"; "b")`
        // reached the pivot with no values and refused. Both addressings name
        // the same occurrence here, which is the whole point of resolving
        // first; the pivot key is matched by published symbol either way.
        ast_resolved::TruthExpression::Membership(Membership {
            probe,
            rows,
            negated: false,
            ..
        }) => {
            let Some(ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence { column, .. },
            )))) = probe.sole_value()
            else {
                return;
            };
            // A pivot's heading witness is a one-column IN, so only the rows
            // that carry exactly one ground value contribute.
            let values: Vec<String> = rows
                .iter()
                .filter_map(|row| match row.0.clone().into_vec().as_slice() {
                    [ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(
                            ast_resolved::LiteralValue::String(text),
                        ),
                    )] => Some(text.clone()),
                    _ => None,
                })
                .collect();
            if values.is_empty() {
                return;
            }
            if let Some(name) = identities.published_sym(*column) {
                result.insert(name, values);
            }
        }
        ast_resolved::TruthExpression::Conjunction(parts) => {
            for part in parts.iter() {
                extract_in_from_resolved_boolean(part, result, identities);
            }
        }
        // Negated: pivot only uses positive IN predicates. Both spellings,
        // for the same reason the positive arm names both.
        ast_resolved::TruthExpression::RelationalMembership(RelationalMembership {
            negated: true,
            ..
        })
        | ast_resolved::TruthExpression::Membership(Membership { negated: true, .. }) => {}
        // Or: IN predicates inside OR branches change semantics — don't extract.
        // Not: negation wrapper — no positive IN to extract.
        ast_resolved::TruthExpression::Disjunction(_)
        | ast_resolved::TruthExpression::Not { .. } => {}
        // Remaining boolean expressions: no InRelational predicates inside.
        ast_resolved::TruthExpression::Comparison(Comparison { .. })
        | ast_resolved::TruthExpression::Existence(Existence { .. })
        | ast_resolved::TruthExpression::Sigma(SigmaApplication { .. }) => {}
    }
}

/// The chain with its trailing pipes peeled: the relation the pipes shaped.
fn unwrap_resolved_pipe(expr: &ast_resolved::Chain) -> ast_resolved::Chain {
    let mut peeled = expr.continuations.as_slice();
    while let Some((ast_resolved::Continuation::Pipe { .. }, rest)) = peeled.split_last() {
        peeled = rest;
    }
    ast_resolved::Chain {
        head: expr.head.clone(),
        continuations: peeled.to_vec(),
    }
}

/// Classifications of pipe operators in the chain before a DML terminal.
/// Used for DML shape validation.
#[derive(Debug)]
enum DmlPipeKind {
    Transform,
    ProjectOut,
    Rename,
    TupleOrdering,
    Group,
    General,
}

/// Classify a single unresolved operator into a DmlPipeKind.
/// Used by linearized pipe resolution to build DML pipe ops from collected segments.
fn classify_single_dml_op(op: &ast_unresolved::PipeOp) -> DmlPipeKind {
    match op {
        ast_unresolved::PipeOp::Transform { .. } => DmlPipeKind::Transform,
        ast_unresolved::PipeOp::Project(_) | ast_unresolved::PipeOp::Embed(_) => {
            DmlPipeKind::General
        }
        ast_unresolved::PipeOp::ProjectOut(_) => DmlPipeKind::ProjectOut,
        ast_unresolved::PipeOp::Rename(_) => DmlPipeKind::Rename,
        ast_unresolved::PipeOp::Group(_) => DmlPipeKind::Group,
        ast_unresolved::PipeOp::MapCover { .. } | ast_unresolved::PipeOp::EmbedMapCover { .. } => {
            DmlPipeKind::General
        }
    }
}

/// Insert correlation filters at the base of a pipe chain, directly above
/// the innermost non-Pipe expression (typically a Ground relation).
/// This ensures the filter's qualifiers match the Ground table name.
#[stacksafe::stacksafe]
fn insert_filters_at_base(
    expr: ast_resolved::Chain,
    filters: Vec<ast_resolved::TruthExpression>,
) -> Result<ast_resolved::Chain> {
    if filters.is_empty() {
        return Ok(expr);
    }
    // Filters land BELOW the pipes: a pipe publishes its own heading, so a
    // filter written against the base cannot address what the pipe made.
    let mut expr = expr;
    let mut trailing = Vec::new();
    while matches!(
        expr.continuations.last(),
        Some(ast_resolved::Continuation::Pipe { .. })
    ) {
        trailing.push(expr.continuations.pop().expect("just matched"));
    }
    // A filter publishes what it filters. Every resolved relation carries
    // a scope, so a base that cannot say what it publishes is an error and
    // not a relation to invent a schema for.
    let schema = extract_cpr_schema(&expr);
    for filter in filters {
        expr = expr.then(ast_resolved::Continuation::Restrict {
            condition: filter,
            origin: ast_resolved::FilterOrigin::Generated,
            cpr_schema: schema,
        });
    }
    expr.continuations.extend(trailing.into_iter().rev());
    Ok(expr)
}

fn extract_literal_rows_from_resolved(expr: &ast_resolved::Chain) -> Option<Vec<String>> {
    if let (ast_resolved::Grelex::Literal(anon), true) = (&expr.head, expr.continuations.is_empty())
    {
        let rows = &anon.table.body.rows;
        let values: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                if row.len() == 1 {
                    if let ast_resolved::DomainExpression::Application(
                        ast_resolved::FunctionApplication::Ground(
                            ast_resolved::LiteralValue::String(s),
                        ),
                    ) = row.0.first().value()
                    {
                        return Some(s.clone());
                    }
                }
                None
            })
            .collect();
        Some(values)
    } else {
        None
    }
}

/// A reference stood over a relation whose dimensions the target does not
/// publish, so the search that would have found it never happened.
pub(crate) fn opaque_reference_refusal() -> DelightQLError {
    DelightQLError::validation_error_categorized(
        crate::uri_registry::subcat::RESOLUTION_SCHEMA,
        "a relation in view has a heading the target does not publish, so this \
         reference cannot be settled against it",
        "declare the dimensions at the mention — `f(...)(a, b)` names one slot per \
         dimension of the full width",
    )
}
