// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use delightql_types::error::{DelightQLError, Result};
use std::cell::Cell;
use std::collections::HashMap;
use std::rc::Rc;

mod pattern_resolver;
pub use pattern_resolver::{JoinContext, PatternResolver};

mod string_templates;

/// Epic 3.0b probe tests: plan-note schema injection via the query-local
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

/// Scope pins for sigma-predicate rule guards
/// (IMPLEMENTATION-PLAN §4.2):
/// a sigma rule visible in the Some(ns) consulted scope must expand to its
/// boolean body — scope first, enlisted-into-main as fallback.
#[cfg(test)]
mod sigma_guard_scope_tests;

/// F2 shadowing pins (COMMENTS-ON-EFFECT-IMPLEMENTATION.md RULINGS item 2,
/// materialize-pipe §6): temp shadows main for UNQUALIFIED names only;
/// qualified reads reach the physical entity; the shadow is a resolution
/// preference, never a catalog delete.
#[cfg(test)]
mod session_shadow_tests;

/// Per-resolution alias counter. Shared across clones so that all
/// resolution phases within a single query use the same sequence.
#[derive(Debug, Clone)]
pub struct ResolverAliasCounter(Rc<Cell<usize>>);

impl ResolverAliasCounter {
    pub fn new() -> Self {
        Self(Rc::new(Cell::new(0)))
    }

    /// Generate a unique `_rN` alias paired with an opaque ResolverId.
    /// The string is for backward-compatible scope lookup; the ResolverId
    /// flows through identity stacks to the transformer.
    pub fn next_alias_with_id(
        &self,
    ) -> (String, crate::pipeline::asts::core::provenance::ResolverId) {
        let n = self.0.get();
        self.0.set(n + 1);
        (
            format!("_r{}", n),
            crate::pipeline::asts::core::provenance::ResolverId::new(n as u64),
        )
    }
}

impl Default for ResolverAliasCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// In-flight consulted-definition expansions, shared across config clones
/// (same idiom as [`ResolverAliasCounter`]). Guards the view/rule inliner
/// against non-terminating expansion: re-encountering a name that is
/// already being expanded means the self-reference did NOT resolve as the
/// in-progress CTE (recursive clause before base, or an indirect cycle
/// through another view) — refuse with a teaching error, never spin.
/// RECURSION-CONTRACT.md B5.
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
                         view, break the cycle. RECURSION-CONTRACT.md B5."
                    ),
                    context: context.to_string(),
                    subcategory: Some(crate::uri_registry::subcat::RECURSION_CONSULTED_CLAUSE_ORDER),
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

/// Configuration for TVF resolution behavior
#[derive(Debug, Clone)]
pub struct ResolutionConfig {
    /// Allow unknown TVFs to pass through with Unknown schema
    pub permissive: bool,
    /// Skip all validation (for transpile-only mode)
    pub transpile_only: bool,
    /// When true, outer_context provides reachable columns for validation
    /// but does NOT trigger deferred (skip) validation mode. Used for
    /// EXISTS/semi-join/anti-join subqueries where the full column set
    /// (outer + inner) is known and validation is safe.
    pub validate_in_correlation: bool,
    /// Active ER-context for & and && operators.
    /// Set by WithErContext wrapper, consumed by ErJoinChain/ErTransitiveJoin resolution.
    pub er_context: Option<ast_unresolved::ErContextSpec>,
    /// Namespace to scope ER-rule lookups to during qualified view body resolution.
    /// Set when resolving a namespace-qualified view (`ns.view(*)`), so that ER-rules
    /// from the view's namespace are found without requiring engage.
    pub resolution_namespace: Option<String>,
    /// Per-resolution alias counter (shared across clones).
    pub alias_counter: ResolverAliasCounter,
    /// In-flight consulted-definition expansions (shared across clones).
    pub expansion_guard: ExpansionGuard,
}

impl Default for ResolutionConfig {
    fn default() -> Self {
        Self {
            permissive: true, // Default to permissive mode
            transpile_only: false,
            validate_in_correlation: false,
            er_context: None,
            resolution_namespace: None,
            alias_counter: ResolverAliasCounter::new(),
            expansion_guard: ExpansionGuard::default(),
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
use self::type_conversion::*;

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
use resolver_fold::ResolverFold;

#[derive(Debug, Clone)]
pub struct BubbledState {
    /// Columns produced by this relational expression. Unqualified references
    /// resolve against this schema.
    pub i_provide: Vec<ast_resolved::ColumnMetadata>,
    pub i_need: Vec<ColumnReference>,
    /// Columns grouped under the lexical qualifiers which remain visible while
    /// resolving a condition attached to this expression. This deliberately
    /// differs from `i_provide`: a set operation produces one merged output
    /// schema while its correlation condition can still name its operand
    /// aliases. A pipe result, conversely, is Fresh and carries no old aliases.
    pub qualifier_scope: Vec<ast_resolved::ColumnMetadata>,
}

impl BubbledState {
    pub fn empty() -> Self {
        Self::resolved(Vec::new())
    }

    pub fn resolved(columns: Vec<ast_resolved::ColumnMetadata>) -> Self {
        Self {
            qualifier_scope: columns.clone(),
            i_provide: columns,
            i_need: Vec::new(),
        }
    }

    pub fn with_unresolved(
        columns: Vec<ast_resolved::ColumnMetadata>,
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

// Re-export DatabaseSchema from delightql-types (Phase 2)
// Core no longer defines these - they live in the types crate to avoid circular dependencies
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

/// Group a flat list of CTE bindings by name, preserving first-appearance order,
/// then validate inter-CTE dependencies (forward references, cycles).
fn group_ctes(
    ctes: Vec<ast_unresolved::CteBinding>,
) -> Result<(
    HashMap<String, Vec<ast_unresolved::CteBinding>>,
    Vec<String>,
)> {
    let mut cte_groups: HashMap<String, Vec<ast_unresolved::CteBinding>> = HashMap::new();
    let mut cte_order: Vec<String> = Vec::new();

    for cte in ctes {
        let name = cte.name.clone();
        let is_new = !cte_groups.contains_key(&name);
        cte_groups.entry(name.clone()).or_default().push(cte);
        if is_new {
            cte_order.push(name);
        }
    }

    validate_grouped_cte_dependencies(&cte_groups, &cte_order)?;

    Ok((cte_groups, cte_order))
}

/// Trait abstracting CTE resolution + registration so that `resolve_cte_bindings`
/// can be shared between `resolve_query` (which uses a `ResolverFold`) and
/// `resolve_query_inline` (which calls `resolve_relational_expression_with_pipe_cfes`).
trait CteResolver {
    /// Resolve an unresolved relational expression, returning the resolved form
    /// plus any pipe-collected CFE definitions discovered during resolution.
    /// `owner` is the CTE's TYPED resolution ownership — the 462-weave
    /// override keys on Caller (the caller-authored carrier CTEs), never on
    /// a naming convention (round 2, P2) and never on construction
    /// provenance (round 3, P1: the squished entity's own clause bodies are
    /// also compiler-CONSTRUCTED, but the entity scope owns their names).
    fn resolve_cte_expression(
        &mut self,
        owner: crate::pipeline::asts::core::provenance::CteResolutionOwner,
        expr: ast_unresolved::RelationalExpression,
    ) -> Result<(
        ast_resolved::RelationalExpression,
        Vec<ast_unresolved::CfeDefinition>,
    )>;

    /// Register a resolved CTE's schema so subsequent CTEs can reference it.
    fn register_cte(&mut self, name: String, schema: ast_resolved::CprSchema);
}

/// `ResolverFold` as a CTE resolver — used by the top-level `resolve_query`.
impl CteResolver for ResolverFold<'_, '_> {
    fn resolve_cte_expression(
        &mut self,
        _owner: crate::pipeline::asts::core::provenance::CteResolutionOwner,
        expr: ast_unresolved::RelationalExpression,
    ) -> Result<(
        ast_resolved::RelationalExpression,
        Vec<ast_unresolved::CfeDefinition>,
    )> {
        let (resolved, _bubbled) = self.resolve_relational(expr)?;
        let cfes = std::mem::take(&mut self.collected_pipe_cfes);
        Ok((resolved, cfes))
    }

    fn register_cte(&mut self, name: String, schema: ast_resolved::CprSchema) {
        self.registry.query_local.register_cte(name, schema);
    }
}

/// Wrapper for inline resolution — used by `resolve_query_inline`.
struct InlineCteResolver<'a, 'db> {
    registry: &'a mut crate::resolution::EntityRegistry<'db>,
    outer_context: Option<&'a [ast_resolved::ColumnMetadata]>,
    config: &'a ResolutionConfig,
    grounding: Option<&'a ast_unresolved::GroundedPath>,
}

impl CteResolver for InlineCteResolver<'_, '_> {
    fn resolve_cte_expression(
        &mut self,
        owner: crate::pipeline::asts::core::provenance::CteResolutionOwner,
        expr: ast_unresolved::RelationalExpression,
    ) -> Result<(
        ast_resolved::RelationalExpression,
        Vec<ast_unresolved::CfeDefinition>,
    )> {
        // THE 462 WEAVE (Phase 10 slice c): the caller-authored carrier
        // CTEs — the piped source, join input, and HO arguments, TYPED
        // Caller-owned at construction — resolve under the CALLER's
        // scope, while the entity's own body CTEs (Entity-owned, from its
        // file text) keep the entity scope. One squished query, two
        // honest scopes; typed ownership, never a naming convention and
        // never construction provenance (review qmqwqlms round 3, P1).
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
        let (resolved, _bubbled, pipe_cfes) = resolve_relational_expression_with_pipe_cfes(
            expr,
            self.registry,
            self.outer_context,
            config,
            self.grounding,
        )?;
        Ok((resolved, pipe_cfes))
    }

    fn register_cte(&mut self, name: String, schema: ast_resolved::CprSchema) {
        self.registry.query_local.register_cte(name, schema);
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
    mut cte_groups: HashMap<String, Vec<ast_unresolved::CteBinding>>,
    cte_order: &[String],
    resolver: &mut dyn CteResolver,
) -> Result<(
    Vec<ast_resolved::CteBinding>,
    Vec<ast_unresolved::CfeDefinition>,
)> {
    let mut resolved_ctes = Vec::new();
    let mut all_pipe_cfes = Vec::new();

    for name in cte_order {
        let group = cte_groups
            .remove(name)
            .expect("CTE should exist after ordering - invariant violation");

        if group.len() == 1 {
            // Single CTE — resolve normally
            let cte = group
                .into_iter()
                .next()
                .expect("Group has len==1, must have element - invariant");
            let effect_label = cte.effect_label;
            let origin = cte.origin;
            let resolution_owner = cte.resolution_owner;
            let (resolved_expr, pipe_cfes) =
                resolver.resolve_cte_expression(resolution_owner.clone(), cte.expression)?;
            all_pipe_cfes.extend(pipe_cfes);
            let mut cte_schema = extract_cpr_schema(&resolved_expr)?;
            cte_schema = transform_schema_table_names(cte_schema, name, origin);
            resolver.register_cte(name.clone(), cte_schema);

            resolved_ctes.push(ast_resolved::CteBinding {
                expression: resolved_expr,
                name: name.clone(),
                origin,
                resolution_owner,
                effect_label,
                is_recursive: ast_resolved::PhaseBox::phantom(),
            });
        } else {
            // Multiple CTEs with same name — create UNION
            let mut operands = Vec::new();
            let mut schemas = Vec::new();
            let mut all_schemas_same = true;

            for (idx, cte) in group.iter().enumerate() {
                let (resolved_expr, pipe_cfes) =
                    resolver.resolve_cte_expression(
                        cte.resolution_owner.clone(),
                        cte.expression.clone(),
                    )?;
                all_pipe_cfes.extend(pipe_cfes);
                let expr_schema = extract_cpr_schema(&resolved_expr)?;

                // After first head, register the CTE so recursive heads can reference it
                if idx == 0 {
                    let mut base_schema = expr_schema.clone();
                    base_schema = transform_schema_table_names(base_schema, name, cte.origin);
                    resolver.register_cte(name.clone(), base_schema);
                }

                if !schemas.is_empty() {
                    if validate_union_compatible_schemas(&schemas[0], &expr_schema).is_err() {
                        all_schemas_same = false;
                    }
                }

                schemas.push(expr_schema);
                operands.push(resolved_expr);
            }

            let (operator, final_schema) = if all_schemas_same {
                (
                    ast_resolved::SetOperator::UnionAllPositional,
                    schemas[0].clone(),
                )
            } else {
                let unified = build_corresponding_schema(&schemas)?;
                (ast_resolved::SetOperator::UnionCorresponding, unified)
            };

            let mut final_schema = final_schema;
            final_schema = transform_schema_table_names(
                final_schema,
                name,
                group.first().map(|c| c.origin).unwrap_or_default(),
            );
            resolver.register_cte(name.clone(), final_schema.clone());

            let union_expr = ast_resolved::RelationalExpression::SetOperation {
                operator,
                operands,
                correlation: ast_resolved::PhaseBox::pass_through_correlation(
                    ast_unresolved::PhaseBox::no_correlation(),
                ),
                cpr_schema: ast_resolved::PhaseBox::new(final_schema),
            };

            resolved_ctes.push(ast_resolved::CteBinding {
                expression: union_expr,
                name: name.clone(),
                origin: group
                    .first()
                    .map(|c| c.origin)
                    .unwrap_or_default(),
                resolution_owner: group
                    .first()
                    .map(|c| c.resolution_owner.clone())
                    .unwrap_or_default(),
                effect_label: group.iter().any(|c| c.effect_label),
                is_recursive: ast_resolved::PhaseBox::phantom(),
            });
        }
    }

    Ok((resolved_ctes, all_pipe_cfes))
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
) -> Result<ResolvedQueryResult> {
    // Create EntityRegistry from the schema (with optional system for namespace resolution)
    let mut registry = if let Some(sys) = system {
        crate::resolution::EntityRegistry::new_with_system(schema, sys)
    } else {
        crate::resolution::EntityRegistry::new(schema)
    };

    // Inline consulted functions across the entire query tree before resolution.
    // This ensures functions from borrowed namespaces (consult!() / inline DDL)
    // are expanded in ALL positions (filters, join conditions, argumentative
    // grounding, etc.) — not just inside pipe operators.
    let (query, ccafe_cfes) = grounding::inline_in_query_borrowed(query, &registry.consult, None, config.resolution_namespace.as_deref())?;

    log::debug!(
        "inline_in_query_borrowed collected {} CFE definitions",
        ccafe_cfes.len()
    );
    for cfe in &ccafe_cfes {
        log::debug!(
            "  CFE: '{}' params={:?} curried={:?}",
            cfe.name,
            cfe.parameters,
            cfe.curried_params
        );
    }

    // If any DDL functions were discovered during inlining,
    // precompile them and inject as WithPrecompiledCfes so the resolver can handle them.
    let query = if !ccafe_cfes.is_empty() {
        let precompiled: Vec<_> = ccafe_cfes
            .into_iter()
            .map(|cfe| {
                crate::pipeline::cfe_precompiler::definition::precompile_cfe_definition(
                    cfe, schema, system,
                )
            })
            .collect::<Result<_>>()?;
        ast_unresolved::Query::WithPrecompiledCfes {
            cfes: precompiled,
            query: Box::new(query),
        }
    } else {
        query
    };

    // All relational resolution goes through the fold (Step 0b delegation shell).
    // The fold delegates to existing free functions; later steps absorb them.
    let mut fold = ResolverFold::new(&mut registry, config.clone(), None, None);

    let resolved_query = match query {
        ast_unresolved::Query::Relational(expr) => {
            let (resolved_expr, _) = fold.resolve_relational(expr)?;
            ast_resolved::Query::Relational(resolved_expr)
        }
        ast_unresolved::Query::ReplTempTable { query, table_name } => {
            // Recursively resolve the nested query
            let inner_result = resolve_query(*query, schema, system, config)?;
            // Merge connection_ids from inner query
            if let Some(conn_id) = inner_result.connection_id {
                fold.registry.track_connection_id(conn_id);
            }
            ast_resolved::Query::ReplTempTable {
                query: Box::new(inner_result.query),
                table_name,
            }
        }
        ast_unresolved::Query::ReplTempView { query, view_name } => {
            // Recursively resolve the nested query
            let inner_result = resolve_query(*query, schema, system, config)?;
            // Merge connection_ids from inner query
            if let Some(conn_id) = inner_result.connection_id {
                fold.registry.track_connection_id(conn_id);
            }
            ast_resolved::Query::ReplTempView {
                query: Box::new(inner_result.query),
                view_name,
            }
        }
        ast_unresolved::Query::WithCtes {
            ctes,
            query: main_query,
        } => {
            let (cte_groups, cte_order) = group_ctes(ctes)?;
            let (resolved_ctes, cte_pipe_cfes) =
                resolve_cte_bindings(cte_groups, &cte_order, &mut fold)?;
            fold.collected_pipe_cfes.extend(cte_pipe_cfes);

            // Now resolve the main query with all CTEs in registry
            let (resolved_main_query, _) = fold.resolve_relational(main_query)?;

            ast_resolved::Query::WithCtes {
                ctes: resolved_ctes,
                query: resolved_main_query,
            }
        }
        ast_unresolved::Query::WithCfes { .. } => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "CFE queries must be precompiled before resolution".to_string(),
                source: None,
                subcategory: None,
            });
        }
        ast_unresolved::Query::WithPrecompiledCfes { cfes, query } => {
            // CFE bodies are already precompiled (resolved+refined) - just pass them through
            // and resolve the main query
            // Register CFE definitions in the existing registry for context validation
            for cfe in &cfes {
                fold.registry.query_local.register_cfe(cfe.clone());
            }

            // Resolve the inner query. We avoid calling resolve_query() here to prevent
            // re-running inline_in_query_borrowed (which would cause infinite recursion).
            // For Relational inner queries, resolve using the OUTER fold so that any
            // pipe-collected CFEs are preserved (resolve_query_inline creates a fresh fold
            // whose collected_pipe_cfes would be silently lost).
            let resolved_inner = match *query {
                ast_unresolved::Query::Relational(rel_expr) => {
                    let (resolved, _bubbled) = fold.resolve_relational(rel_expr)?;
                    Box::new(ast_resolved::Query::Relational(resolved))
                }
                other => {
                    let (inner, _bubbled) =
                        resolve_query_inline(other, fold.registry, None, &config, None)?;
                    Box::new(inner)
                }
            };

            ast_resolved::Query::WithPrecompiledCfes {
                cfes,
                query: resolved_inner,
            }
        }
        ast_unresolved::Query::WithErContext { context, query } => {
            // Thread ER-context into config so ErJoinChain/ErTransitiveJoin can find it.
            let config_with_ctx = ResolutionConfig {
                er_context: Some(context),
                ..config.clone()
            };
            let inner_result = resolve_query(*query, schema, system, &config_with_ctx)?;
            if let Some(conn_id) = inner_result.connection_id {
                fold.registry.track_connection_id(conn_id);
            }
            inner_result.query
        }
    };

    // If any DDL functions were collected during per-pipe inlining,
    // precompile them and merge into the resolved query's WithPrecompiledCfes
    // (or create a new wrapper if none exists).
    let pipe_cfes = std::mem::take(&mut fold.collected_pipe_cfes);
    log::debug!("pipe_cfes after resolution: {} entries", pipe_cfes.len());
    let resolved_query = if !pipe_cfes.is_empty() {
        // Deduplicate by name (top-level pass may have already collected some)
        let mut seen = std::collections::HashSet::new();
        let unique_cfes: Vec<_> = pipe_cfes
            .into_iter()
            .filter(|c| seen.insert(c.name.clone()))
            .collect();

        log::debug!(
            "Precompiling {} pipe-collected DDL function CFEs",
            unique_cfes.len()
        );

        let mut precompiled: Vec<_> = unique_cfes
            .into_iter()
            .map(|cfe| {
                crate::pipeline::cfe_precompiler::definition::precompile_cfe_definition(
                    cfe, schema, system,
                )
            })
            .collect::<Result<_>>()?;

        // Merge into existing WithPrecompiledCfes to avoid nesting
        match resolved_query {
            ast_resolved::Query::WithPrecompiledCfes {
                mut cfes,
                query: inner,
            } => {
                cfes.append(&mut precompiled);
                ast_resolved::Query::WithPrecompiledCfes { cfes, query: inner }
            }
            other => ast_resolved::Query::WithPrecompiledCfes {
                cfes: precompiled,
                query: Box::new(other),
            },
        }
    } else {
        resolved_query
    };

    // Validate that all resolved tables belong to the same connection
    let connection_id = fold.registry.validate_single_connection()?;

    Ok(ResolvedQueryResult {
        query: resolved_query,
        connection_id,
    })
}

/// Output column names of a resolved query, for callers outside the
/// resolver (the effect transformer builds plan notes and witness-union
/// alignment from them — IMPLEMENTATION-PLAN §3.1). Thin wrapper over the
/// resolver-internal `extract_cpr_schema_from_query`. `None` when the
/// schema did not resolve to named columns.
pub(crate) fn resolved_output_columns(query: &ast_resolved::Query) -> Option<Vec<String>> {
    let schema = extract_cpr_schema_from_query(query).ok()?;
    match schema {
        ast_resolved::CprSchema::Resolved(cols) => {
            Some(cols.iter().map(|c| c.name().to_string()).collect())
        }
        ast_resolved::CprSchema::Unresolved(cols) => {
            Some(cols.iter().map(|c| c.name().to_string()).collect())
        }
        ast_resolved::CprSchema::Failed { .. } | ast_resolved::CprSchema::Unknown => None,
    }
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
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::Query, BubbledState)> {
    match query {
        ast_unresolved::Query::Relational(expr) => {
            // Discover consulted function references (e.g. dbl:(x) calling a
            // DDL-defined functional view) before resolution. Without this,
            // such calls pass through as literal SQL function calls.
            let wrapper_query = ast_unresolved::Query::Relational(expr);
            let (wrapper_query, ccafe_cfes) =
                grounding::inline_in_query_borrowed(wrapper_query, &registry.consult, None, config.resolution_namespace.as_deref())?;
            let expr = match wrapper_query {
                ast_unresolved::Query::Relational(e) => e,
                _ => unreachable!("inline_in_query_borrowed preserves Relational variant"),
            };
            let (resolved_expr, bubbled, mut pipe_cfes) =
                resolve_relational_expression_with_pipe_cfes(
                    expr,
                    registry,
                    outer_context,
                    config,
                    grounding,
                )?;
            // Merge query-level CFEs (from inline_in_query_borrowed) with
            // pipe-level CFEs (from inline_consulted_functions_in_operator)
            pipe_cfes.extend(ccafe_cfes);
            let resolved_query = ast_resolved::Query::Relational(resolved_expr);
            let resolved_query = wrap_with_pipe_cfes(resolved_query, pipe_cfes, registry)?;
            Ok((resolved_query, bubbled))
        }
        ast_unresolved::Query::WithCtes {
            ctes,
            query: main_query,
        } => {
            // Discover consulted function references in the WithCtes query
            let wrapper_query = ast_unresolved::Query::WithCtes {
                ctes,
                query: main_query,
            };
            let (wrapper_query, ccafe_cfes) =
                grounding::inline_in_query_borrowed(wrapper_query, &registry.consult, None, config.resolution_namespace.as_deref())?;
            let (ctes, main_query) = match wrapper_query {
                ast_unresolved::Query::WithCtes { ctes, query } => (ctes, query),
                _ => unreachable!("inline_in_query_borrowed preserves WithCtes variant"),
            };

            let (cte_groups, cte_order) = group_ctes(ctes)?;

            // Resolve CTEs using the InlineCteResolver wrapper
            let (resolved_ctes, mut all_pipe_cfes) = {
                let mut inline_resolver = InlineCteResolver {
                    registry: &mut *registry,
                    outer_context,
                    config,
                    grounding,
                };
                resolve_cte_bindings(cte_groups, &cte_order, &mut inline_resolver)?
            };

            // Resolve the main query with all CTEs registered
            let (resolved_main, bubbled, main_pipe_cfes) =
                resolve_relational_expression_with_pipe_cfes(
                    main_query,
                    registry,
                    outer_context,
                    config,
                    grounding,
                )?;
            all_pipe_cfes.extend(main_pipe_cfes);
            all_pipe_cfes.extend(ccafe_cfes);

            let resolved_query = ast_resolved::Query::WithCtes {
                ctes: resolved_ctes,
                query: resolved_main,
            };
            let resolved_query = wrap_with_pipe_cfes(resolved_query, all_pipe_cfes, registry)?;
            Ok((resolved_query, bubbled))
        }
        ast_unresolved::Query::WithPrecompiledCfes { cfes, query } => {
            // Register CFE definitions in the registry for resolution
            for cfe in &cfes {
                registry.query_local.register_cfe(cfe.clone());
            }
            let (resolved_inner, bubbled) =
                resolve_query_inline(*query, registry, outer_context, config, grounding)?;
            Ok((
                ast_resolved::Query::WithPrecompiledCfes {
                    cfes,
                    query: Box::new(resolved_inner),
                },
                bubbled,
            ))
        }
        ast_unresolved::Query::WithCfes { .. } => {
            // WithCfes needs precompilation before resolution.
            // This happens when a DDL view body contains local CFE definitions
            // (e.g. `v(*) :- dbl:(a) : a * 2  T(dbl:(x))`).
            let schema = registry.database.schema();
            let system = registry.database.system;
            let precompiled =
                crate::pipeline::cfe_precompiler::precompile_query_cfes(query, schema, system)?;
            resolve_query_inline(precompiled, registry, outer_context, config, grounding)
        }
        ast_unresolved::Query::WithErContext { context, query } => {
            // Thread ER-context into config, same as top-level resolve_query.
            let config_with_ctx = ResolutionConfig {
                er_context: Some(context),
                ..config.clone()
            };
            resolve_query_inline(*query, registry, outer_context, &config_with_ctx, grounding)
        }
        other => Err(DelightQLError::ParseError {
            message: format!(
                "Unexpected query type in view body: {:?}",
                std::mem::discriminant(&other)
            ),
            source: None,
            subcategory: None,
        }),
    }
}

/// If any pipe-collected CFE definitions were gathered during resolution,
/// precompile them and wrap the query in WithPrecompiledCfes. This ensures
/// that DDL-defined functional views referenced inside view bodies (e.g.
/// `dbl:(x)` called from another view's body) are available for substitution
/// during the transformer phase.
fn wrap_with_pipe_cfes(
    resolved_query: ast_resolved::Query,
    pipe_cfes: Vec<ast_unresolved::CfeDefinition>,
    registry: &crate::resolution::EntityRegistry,
) -> Result<ast_resolved::Query> {
    if pipe_cfes.is_empty() {
        return Ok(resolved_query);
    }

    let mut seen = std::collections::HashSet::new();
    let unique_cfes: Vec<_> = pipe_cfes
        .into_iter()
        .filter(|c| seen.insert(c.name.clone()))
        .collect();

    let schema = registry.database.schema();
    let system = registry.database.system;

    let mut precompiled: Vec<_> = unique_cfes
        .into_iter()
        .map(|cfe| {
            crate::pipeline::cfe_precompiler::definition::precompile_cfe_definition(
                cfe, schema, system,
            )
        })
        .collect::<Result<_>>()?;

    // Merge into existing WithPrecompiledCfes to avoid nesting
    match resolved_query {
        ast_resolved::Query::WithPrecompiledCfes {
            mut cfes,
            query: inner,
        } => {
            cfes.append(&mut precompiled);
            Ok(ast_resolved::Query::WithPrecompiledCfes { cfes, query: inner })
        }
        other => Ok(ast_resolved::Query::WithPrecompiledCfes {
            cfes: precompiled,
            query: Box::new(other),
        }),
    }
}

/// Walk the unresolved source expression tree and collect columns from all
/// EXISTS subquery table sources. This enriches the combined_context for
/// interdependent EXISTS subqueries so that cross-EXISTS column references
/// (e.g., `order_items.product_id` inside an EXISTS for `products`, where
/// `order_items` is a sibling EXISTS) can be validated.
///
/// SCOPE-LOCAL (INVENTORY L2): walks the current scope's Filter/spine and reaches
/// each EXISTS's own innermost source, but does NOT recurse whole-tree into
/// arbitrary nested subquery scopes. The `_in_scope` name marks that boundary.
fn collect_exists_table_columns_in_scope(
    expr: &ast_unresolved::RelationalExpression,
    registry: &mut crate::resolution::EntityRegistry,
    context: &mut Vec<ast_resolved::ColumnMetadata>,
) -> Result<()> {
    match expr {
        ast_unresolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            // Recurse into the source to find deeper EXISTS
            collect_exists_table_columns_in_scope(source, registry, context)?;

            // If this filter's condition is an EXISTS, resolve the EXISTS
            // table source to get its columns and add them to context.
            if let ast_unresolved::SigmaCondition::Predicate(pred) = condition {
                if let ast_unresolved::BooleanExpression::InnerExists { subquery, .. } = pred {
                    // The subquery body is typically Filter(source=Relation(table), ...).
                    // Extract the innermost source relation and resolve it.
                    let inner_source = extract_innermost_source(subquery);
                    if let Some(rel_expr) = inner_source {
                        let (resolved_source, _) = resolve_relational_expression_with_registry(
                            rel_expr.clone(),
                            registry,
                            None,
                            &ResolutionConfig::default(),
                            None,
                        )?;
                        let source_schema =
                            helpers::extraction::extract_cpr_schema(&resolved_source)?;
                        if let ast_resolved::CprSchema::Resolved(cols) = source_schema {
                            context.extend(cols);
                        }
                    }
                }
            }
            Ok(())
        }
        // Pipe: recurse into source for nested Filters with EXISTS.
        ast_unresolved::RelationalExpression::Pipe(pipe) => {
            collect_exists_table_columns_in_scope(&pipe.source, registry, context)
        }
        // Join: EXISTS could be in Filter nodes inside either branch.
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            collect_exists_table_columns_in_scope(left, registry, context)?;
            collect_exists_table_columns_in_scope(right, registry, context)
        }
        // SetOperation: recurse into operands.
        ast_unresolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                collect_exists_table_columns_in_scope(operand, registry, context)?;
            }
            Ok(())
        }
        // Relation: leaf node — no Filters or EXISTS to collect.
        ast_unresolved::RelationalExpression::Relation(_) => Ok(()),
        // ER chains consumed before EXISTS collection.
        ast_unresolved::RelationalExpression::ErJoinChain { .. }
        | ast_unresolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before EXISTS collection")
        }
        ast_unresolved::RelationalExpression::IntersectCorresponding { .. } => {
            unreachable!("IntersectCorresponding only exists in Refined/Addressed phases")
        }
    }
}

/// Extract the innermost source from a relational expression.
/// Traverses through Filter nodes to find the bottom source (usually a Relation).
///
/// KEPT LOCAL (not routed through Helper A `source_spine`): this peels `Filter`
/// ONLY and STOPS at a Pipe (`_ => Some(self)`) — it belongs with the
/// terminal-filter-peel family (S9–S11), NOT the base/source spine, which
/// descends `Filter` AND `Pipe`. Routing it through `source_spine_terminal`
/// would descend past a Pipe and return a different node (INVENTORY §4 grouped
/// it with Helper A, but its Pipe boundary diverges — a named local accessor is
/// correct here).
fn extract_innermost_source(
    expr: &ast_unresolved::RelationalExpression,
) -> Option<&ast_unresolved::RelationalExpression> {
    match expr {
        // Peel Filter → source ONLY. `condition` is a recursive field this peel
        // DELIBERATELY does not follow (spelled `_` per R-I3); origin/cpr_schema
        // are non-recursive metadata under `..`.
        ast_unresolved::RelationalExpression::Filter {
            source,
            condition: _,
            ..
        } => extract_innermost_source(source),
        // STOP at the WHOLE node: Pipe (this peel stops at a Pipe, unlike the
        // base spine), Relation, Join, SetOperation, IntersectCorresponding, ER.
        // Returning the whole node hides no recursive field — the node IS the
        // boundary. Variants spelled per R-I3 so a new relational variant forces a
        // decision instead of a silent `_ => Some(self)`.
        ast_unresolved::RelationalExpression::Relation(_)
        | ast_unresolved::RelationalExpression::Pipe(_)
        | ast_unresolved::RelationalExpression::Join { .. }
        | ast_unresolved::RelationalExpression::SetOperation { .. }
        | ast_unresolved::RelationalExpression::IntersectCorresponding { .. }
        | ast_unresolved::RelationalExpression::ErJoinChain { .. }
        | ast_unresolved::RelationalExpression::ErTransitiveJoin { .. } => Some(expr),
    }
}

/// New resolution function using EntityRegistry.
///
/// Thin wrapper: delegates to `ResolverFold::resolve_relational_impl`.
fn resolve_relational_expression_with_registry(
    expr: ast_unresolved::RelationalExpression,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    let mut fold = ResolverFold::new(
        registry,
        config.clone(),
        outer_context.map(|c| c.to_vec()),
        grounding.cloned(),
    );
    fold.resolve_relational(expr)
}

/// Like `resolve_relational_expression_with_registry` but also returns any
/// pipe-collected CFE definitions from the sub-fold. Used by interior relation
/// resolution to propagate CFEs back to the outer fold.
fn resolve_relational_expression_with_pipe_cfes(
    expr: ast_unresolved::RelationalExpression,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(
    ast_resolved::RelationalExpression,
    BubbledState,
    Vec<ast_unresolved::CfeDefinition>,
)> {
    let mut fold = ResolverFold::new(
        registry,
        config.clone(),
        outer_context.map(|c| c.to_vec()),
        grounding.cloned(),
    );
    let (resolved, bubbled) = fold.resolve_relational(expr)?;
    let pipe_cfes = std::mem::take(&mut fold.collected_pipe_cfes);
    Ok((resolved, bubbled, pipe_cfes))
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
    let mut named = contexts.iter().flatten();
    let first = named.next();
    if first.is_none() || contexts.iter().any(|c| c.is_none()) {
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/bare_operator",
            "the ER operators take their context as a symbol on the operator: \
             &(::your_context) for a direct edge, &&(::your_context) for the \
             transitive walk"
                .to_string(),
            "contexts are symbols; the edge set per context is finite and declared",
        ));
    }
    let first = first.expect("checked above");
    if let Some(other) = named.find(|c| *c != first) {
        return Err(DelightQLError::validation_error_categorized(
            "grounding/er/mixed_contexts",
            format!(
                "one chain, one context — this chain names both ::{first} and ::{other}"
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
fn er_endpoint(rel: &ast_unresolved::Relation) -> (String, Option<delightql_types::SqlIdentifier>) {
    match rel {
        ast_unresolved::Relation::Ground {
            identifier, alias, ..
        } => (identifier.name.to_string(), alias.clone()),
        _ => (String::new(), None),
    }
}

/// Endpoints only (`&&`): intermediate hops are entity boundaries and
/// contribute nothing to the schema — the result is the two endpoint
/// terms' exports. Built as a qualified-glob projection over the
/// combined body; `er_stamp_endpoint_access_names` then restores the
/// answering channel (the lvar law's access_name) so the caller's
/// qualified references (`suppliers.name`) still resolve post-pipe.
fn er_endpoints_projection(
    query: ast_unresolved::Query,
    published: &[String],
) -> ast_unresolved::Query {
    use crate::pipeline::asts::core::expressions::ProjectionExpr;
    let glob = |name: &str| {
        ast_unresolved::DomainExpression::Projection(ProjectionExpr::Glob {
            qualifier: Some(name.into()),
            namespace_path: ast_unresolved::NamespacePath::empty(),
        })
    };
    match query {
        ast_unresolved::Query::Relational(expr) => ast_unresolved::Query::Relational(
            ast_unresolved::RelationalExpression::pipe_builder(expr)
                .with_projection(published.iter().map(|n| glob(n)).collect())
                .build(),
        ),
        other => other,
    }
}

/// After the endpoints-only projection resolves, each output column
/// ANSWERS TO its endpoint's name: the identity stack's most recent
/// endpoint qualifier becomes access_name, so exports follow the lvar
/// law across the pipe boundary.
fn er_stamp_endpoint_access_names(bubbled: &mut BubbledState, published: &[String]) {
    let is_published =
        |n: &str| published.iter().any(|p| delightql_types::SqlIdentifier::str_eq(n, p));
    let endpoint_of = |col: &ast_resolved::ColumnMetadata| -> Option<String> {
        // Current qualifier first, then the identity stack.
        if let ast_resolved::TableName::Named(n) = col.qualifier() {
            let n = n.to_string();
            if is_published(&n) {
                return Some(n);
            }
        }
        for frame in col.info.identity_stack().iter().rev() {
            if let ast_resolved::TableName::Named(n) = &frame.table_qualifier {
                let n = n.to_string();
                if is_published(&n) {
                    return Some(n);
                }
            }
            // The pipe barrier stamps Fresh as the frame qualifier and
            // records where the column CAME FROM in the context.
            if let ast_resolved::IdentityContext::PipeBarrier {
                previous_table: ast_resolved::TableName::Named(n),
                ..
            } = &frame.context
            {
                let n = n.to_string();
                if is_published(&n) {
                    return Some(n);
                }
            }
        }
        None
    };
    for col in bubbled
        .i_provide
        .iter_mut()
        .chain(bubbled.qualifier_scope.iter_mut())
    {
        if let Some(name) = endpoint_of(col) {
            col.answer_if_silent(name.into());
        }
    }
}

fn er_table_name(rel: &ast_unresolved::Relation) -> Result<String> {
    match rel {
        ast_unresolved::Relation::Ground { identifier, .. } => Ok(identifier.name.to_string()),
        _ => Err(DelightQLError::validation_error(
            "ER-join operands must be table references (e.g., users_t(*))",
            "Invalid ER-join operand",
        )),
    }
}

/// Expand an ErJoinChain by looking up ER-rules for each consecutive pair
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
    relations: Vec<ast_unresolved::Relation>,
    spellings: &[String],
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
    endpoints_only: Option<Vec<String>>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
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
            endpoints_only.clone(),
        )?;
        if let Some(published) = &endpoints_only {
            er_stamp_endpoint_access_names(&mut bubbled, published);
        }
        let (resolved_expr, bubbled) = er_thread_endpoint_aliases(
            resolved_expr,
            bubbled,
            (&left_endpoint_name, &left_endpoint_alias),
            (&right_endpoint_name, &right_endpoint_alias),
        );
        let resolved_expr = if endpoints_only.is_some() {
            er_sync_pipe_schema(resolved_expr, &bubbled)
        } else {
            resolved_expr
        };
        return Ok((resolved_expr, bubbled));
    }

    // For chains (A & B & C & ...), combine all pair bodies into one expression.
    //
    // Each pair's body is something like: `A(*), B(*), A.id = B.aid`
    // For chains, consecutive pairs share an intermediate table (B appears in both
    // (A,B) and (B,C) bodies). We flatten all bodies, deduplicate the shared tables,
    // and build one combined expression that resolves cleanly through the pipeline.
    let mut all_relations: Vec<ast_unresolved::Relation> = Vec::new();
    let mut all_conditions: Vec<ast_unresolved::SigmaCondition> = Vec::new();
    let mut seen_table_names: std::collections::HashSet<String> = std::collections::HashSet::new();

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
        let body_expr = match body_query {
            ast_unresolved::Query::Relational(expr) => expr,
            _ => return Err(DelightQLError::validation_error(
                format!(
                    "ER-rule body for ({}, {}) in context '{}' contains CTEs (not supported in chains)",
                    left_name, right_name, context.context_name,
                ),
                "Invalid ER-rule body",
            )),
        };

        // Flatten the body into relations and conditions
        let pair_desc = format!(
            "{left_name} & {right_name} in '::{}'",
            context.context_name
        );
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
        let shared_table = er_endpoint(&relations[i]).0;
        let mut shared_endpoint_budget = if i > 0 { 1usize } else { 0 };
        for rel in body_rels {
            if let Ok(name) = er_table_name(&rel) {
                if seen_table_names.insert(name.clone()) {
                    all_relations.push(rel);
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
                all_relations.push(rel);
            }
        }

        // Keep all conditions (conditions from different pairs don't duplicate)
        all_conditions.extend(body_conds);
    }

    // Rebuild a single unresolved expression from the combined parts
    let combined_expr = rebuild_flat_expression(all_relations, all_conditions)?;

    // Add self-aliases and resolve through the pipeline (same path as single-pair)
    let combined_query =
        add_self_aliases_to_query(ast_unresolved::Query::Relational(combined_expr));
    // `&&`: intermediate hops are entity boundaries — endpoints only.
    let combined_query = match &endpoints_only {
        Some(published) => er_endpoints_projection(combined_query, published),
        None => combined_query,
    };

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
        er_edge_miss_error(registry, &context.context_name, &spellings[0], &spellings[1])
    })?;
    let rule_ns = first_rule.namespace.clone();
    let auto_grounding = registry
        .consult
        .get_namespace_default_data_ns(&rule_ns)
        .and_then(|data_ns_fq| {
            let parts: Vec<String> = data_ns_fq.split("::").map(|s| s.to_string()).collect();
            let data_ns = ast_unresolved::NamespacePath::from_parts(parts).ok()?;
            let ns_parts: Vec<String> = rule_ns.split("::").map(|s| s.to_string()).collect();
            let grounded_ns = ast_unresolved::NamespacePath::from_parts(ns_parts).ok()?;
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
        // Same pair-schema teaching as the single-pair road: an endpoint
        // glob that matches nothing is a pair-set violation, not a bare
        // glob miss.
        let msg = e.to_string();
        if endpoints_only.is_some() && msg.contains("matched no columns") {
            let missing = msg
                .split('\'')
                .nth(1)
                .unwrap_or("an endpoint")
                .trim_end_matches(".*");
            return DelightQLError::validation_error_categorized(
                "grounding/er/pair_schema",
                format!(
                    "the composed chain in '::{}' does not publish '{missing}' — \
                     an edge is a PAIR-SET and the chain's published schema is \
                     its written terms' columns; a body renamed or projected \
                     that endpoint away. Rename and narrow at the call site, \
                     after selection, not inside the edge",
                    context.context_name
                ),
                "the published schema of an edge is schema(A) + schema(B); \
                 the boundary exports those columns and hides the rest",
            );
        }
        DelightQLError::database_error(
            format!(
                "Error resolving ER-chain body in context '{}': {}",
                context.context_name, e
            ),
            e.to_string(),
        )
    })?;

    match resolved_query {
        ast_resolved::Query::Relational(expr) => {
            let mut body_bubbled = body_bubbled;
            if let Some(published) = &endpoints_only {
                er_stamp_endpoint_access_names(&mut body_bubbled, published);
            }
            let (expr, body_bubbled) = er_thread_endpoint_aliases(
                expr,
                body_bubbled,
                (&left_endpoint_name, &left_endpoint_alias),
                (&right_endpoint_name, &right_endpoint_alias),
            );
            let expr = if endpoints_only.is_some() {
                er_sync_pipe_schema(expr, &body_bubbled)
            } else {
                expr
            };
            Ok((expr, body_bubbled))
        }
        _ => Err(DelightQLError::validation_error(
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
    expr: ast_resolved::RelationalExpression,
    bubbled: &BubbledState,
) -> ast_resolved::RelationalExpression {
    match expr {
        ast_resolved::RelationalExpression::Pipe(pipe) => {
            let mut inner = (*pipe).into_inner();
            inner.cpr_schema = ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                bubbled.i_provide.clone(),
            ));
            ast_resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(inner)))
        }
        other => other,
    }
}

/// Rename endpoint tables to their user aliases throughout a resolved
/// ER result (exports answer to the alias; selection already happened
/// by spelling).
fn er_thread_endpoint_aliases(
    mut expr: ast_resolved::RelationalExpression,
    mut bubbled: BubbledState,
    left: (&str, &Option<delightql_types::SqlIdentifier>),
    right: (&str, &Option<delightql_types::SqlIdentifier>),
) -> (ast_resolved::RelationalExpression, BubbledState) {
    if let (name, Some(alias)) = left {
        expr = rename_in_resolved_expr(expr, name, alias);
        rename_bubbled_columns(&mut bubbled, name, alias);
    }
    if let (name, Some(alias)) = right {
        expr = rename_in_resolved_expr(expr, name, alias);
        rename_bubbled_columns(&mut bubbled, name, alias);
    }
    (expr, bubbled)
}

/// `&&` composes RELATIONS, not syntax (GROUNDING-AND-MENTION): each hop
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
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    use ast_resolved::RelationalExpression as RE;

    // A column's underlying identity for cross-hop pairing: the endpoint
    // it answers to, plus its base name within that endpoint — the
    // spelling with the collision suffix (|N|) and the endpoint's own
    // prefix stripped. Two hops may number the same column differently,
    // but the base name under one endpoint is unique and stable.
    let endpoint_key = |col: &ast_resolved::ColumnMetadata, table: &str| -> String {
        let base = col.name().split('|').next().unwrap_or(col.name());
        base.strip_prefix(&format!("{table}."))
            .unwrap_or(base)
            .to_string()
    };
    let belongs_to = |col: &ast_resolved::ColumnMetadata, table: &str| -> bool {
        col.access_name()
            .is_some_and(|a| delightql_types::SqlIdentifier::str_eq(a.as_str(), table))
    };

    let mut composed: Option<RE> = None;
    let mut all_columns: Vec<ast_resolved::ColumnMetadata> = Vec::new();
    let mut prev_hop_alias = String::new();
    let mut conditions: Vec<ast_resolved::BooleanExpression> = Vec::new();

    for i in 0..path.len() - 1 {
        let hop_alias = format!("_er_hop_{i}");
        let (hop_expr, mut hop_bubbled) = expand_single_er_pair(
            &path[i],
            &path[i + 1],
            context,
            registry,
            outer_context,
            config,
            grounding,
            Some(vec![hop_tables[i].clone(), hop_tables[i + 1].clone()]),
        )?;
        // The answering channel is the pairing key: stamp each hop's
        // columns with their endpoint names (the len==2 road's caller
        // does this; here we are the caller).
        er_stamp_endpoint_access_names(
            &mut hop_bubbled,
            &[hop_tables[i].clone(), hop_tables[i + 1].clone()],
        );

        // Wrap the hop as an aliased derived table so its columns are
        // addressable hop-distinctly (the shared endpoint's columns
        // exist in BOTH adjacent hops; only the hop alias tells them
        // apart).
        let hop_schema = ast_resolved::CprSchema::Resolved(hop_bubbled.i_provide.clone());
        let hop_rel = RE::Relation(ast_resolved::Relation::InnerRelation {
            pattern: ast_resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                identifier: ast_resolved::QualifiedName {
                    namespace_path: ast_resolved::NamespacePath::empty(),
                    name: hop_alias.clone().into(),
                    grounding: None,
                },
                subquery: Box::new(hop_expr),
                is_consulted_view: false,
            },
            alias: Some(hop_alias.clone().into()),
            outer: false,
            cpr_schema: ast_resolved::PhaseBox::new(hop_schema),
        });

        let mut hop_columns = hop_bubbled.i_provide.clone();
        for col in &mut hop_columns {
            let prev = match col.qualifier() {
                ast_resolved::TableName::Named(t) => t.to_string(),
                ast_resolved::TableName::Fresh => "_".to_string(),
            };
            col.push_scope(
                ast_resolved::TableName::Named(hop_alias.clone().into()),
                ast_resolved::IdentityContext::SubqueryAlias {
                    alias: hop_alias.clone(),
                    previous_context: prev,
                    resolver_id: None,
                },
            );
        }

        if let Some(acc) = composed.take() {
            // Join to the accumulated chain on the shared endpoint's
            // full heading — every column of hop_tables[i], null-safe.
            let shared = &hop_tables[i];
            for right_col in hop_columns.iter().filter(|c| belongs_to(c, shared)) {
                let rb = endpoint_key(right_col, shared);
                let left_col = all_columns
                    .iter()
                    .filter(|c| belongs_to(c, shared))
                    .find(|c| {
                        delightql_types::SqlIdentifier::str_eq(&endpoint_key(c, shared), &rb)
                    })
                    .ok_or_else(|| {
                        DelightQLError::validation_error(
                            format!(
                                "ER chain composition cannot pair shared-endpoint \
                                 column '{rb}' of '{shared}' between hops — this \
                                 is a dql bug (both hops publish the endpoint's \
                                 pair schema by construction)",
                            ),
                            "Invalid ER-chain composition",
                        )
                    })?;
                let lvar = |name: &str, qual: &str| {
                    Box::new(ast_resolved::DomainExpression::Lvar {
                        name: name.into(),
                        qualifier: Some(qual.into()),
                        namespace_path: ast_resolved::NamespacePath::empty(),
                        alias: None,
                        provenance: ast_resolved::PhaseBox::phantom(),
                    })
                };
                conditions.push(ast_resolved::BooleanExpression::Comparison {
                    operator: "null_safe_eq".to_string(),
                    left: lvar(left_col.name(), &prev_hop_alias),
                    right: lvar(right_col.name(), &hop_alias),
                });
            }
            composed = Some(RE::Join {
                left: Box::new(acc),
                right: Box::new(hop_rel),
                join_condition: None,
                join_type: None,
                cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                    Vec::new(),
                )),
            });
        } else {
            composed = Some(hop_rel);
        }
        all_columns.extend(hop_columns);
        prev_hop_alias = hop_alias;
    }

    let mut expr = composed.expect("path has at least two spellings");
    for cond in conditions {
        expr = RE::Filter {
            source: Box::new(expr),
            condition: ast_resolved::SigmaCondition::Predicate(cond),
            origin: crate::pipeline::asts::core::FilterOrigin::Generated,
            cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                Vec::new(),
            )),
        };
    }

    // Publish the outer endpoints only (R-c): the first hop's left
    // term's columns and the last hop's right term's columns.
    let first_table = &hop_tables[0];
    let last_table = hop_tables.last().expect("nonempty");
    let last_alias = prev_hop_alias;
    let kept: Vec<(ast_resolved::ColumnMetadata, String)> = all_columns
        .iter()
        .filter_map(|c| {
            let qual = match c.qualifier() {
                ast_resolved::TableName::Named(t) => t.to_string(),
                ast_resolved::TableName::Fresh => return None,
            };
            if qual == "_er_hop_0" && belongs_to(c, first_table) {
                Some((c.clone(), qual))
            } else if qual == *last_alias && belongs_to(c, last_table) {
                Some((c.clone(), qual))
            } else {
                None
            }
        })
        .collect();

    let projection: Vec<ast_resolved::DomainExpression> = kept
        .iter()
        .map(|(col, qual)| ast_resolved::DomainExpression::Lvar {
            name: col.name().into(),
            qualifier: Some(qual.as_str().into()),
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: None,
            provenance: ast_resolved::PhaseBox::phantom(),
        })
        .collect();
    let published: Vec<ast_resolved::ColumnMetadata> =
        kept.into_iter().map(|(c, _)| c).collect();

    let pipe = ast_resolved::PipeExpression {
        source: expr,
        operator: ast_resolved::UnaryRelationalOperator::General {
            containment_semantic:
                crate::pipeline::asts::core::ContainmentSemantic::Parenthesis,
            expressions: projection,
        },
        cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
            published.clone(),
        )),
    };
    let expr = RE::Pipe(Box::new(stacksafe::StackSafe::new(pipe)));

    Ok((expr, BubbledState::resolved(published)))
}

/// Flatten an unresolved relational expression into a list of relations and conditions.
/// Walks the Join/Filter tree and collects all leaf Relation nodes and all Filter conditions.
/// Transitive composition (&&) merges edge bodies BEFORE resolution, so a
/// body that carries anything beyond join/filter normal form — a pipe
/// stage, a set operation, a nested edge call — cannot be merged without
/// discarding its semantics; it refuses instead (dropped semantics or a
/// downstream panic is not an admissible fallback).
fn flatten_unresolved_body(
    expr: ast_unresolved::RelationalExpression,
    pair_desc: &str,
) -> Result<(
    Vec<ast_unresolved::Relation>,
    Vec<ast_unresolved::SigmaCondition>,
)> {
    let mut relations = Vec::new();
    let mut conditions = Vec::new();
    flatten_unresolved_body_inner(expr, &mut relations, &mut conditions, pair_desc)?;
    Ok((relations, conditions))
}

fn flatten_unresolved_body_inner(
    expr: ast_unresolved::RelationalExpression,
    relations: &mut Vec<ast_unresolved::Relation>,
    conditions: &mut Vec<ast_unresolved::SigmaCondition>,
    pair_desc: &str,
) -> Result<()> {
    match expr {
        ast_unresolved::RelationalExpression::Relation(rel) => {
            relations.push(rel);
            Ok(())
        }
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            flatten_unresolved_body_inner(*left, relations, conditions, pair_desc)?;
            flatten_unresolved_body_inner(*right, relations, conditions, pair_desc)
        }
        ast_unresolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            flatten_unresolved_body_inner(*source, relations, conditions, pair_desc)?;
            conditions.push(condition);
            Ok(())
        }
        other => {
            let what = match &other {
                ast_unresolved::RelationalExpression::Pipe(_) => "a pipe stage (|>)",
                ast_unresolved::RelationalExpression::SetOperation { .. }
                | ast_unresolved::RelationalExpression::IntersectCorresponding { .. } => {
                    "a set operation"
                }
                ast_unresolved::RelationalExpression::ErJoinChain { .. }
                | ast_unresolved::RelationalExpression::ErTransitiveJoin { .. } => {
                    "a nested edge call"
                }
                _ => "an operator beyond relations, joins, and conditions",
            };
            Err(DelightQLError::validation_error_categorized(
                "grounding/er/chain_normal_form",
                format!(
                    "the edge body for {pair_desc} carries {what} — a transitive \
                     chain (&&) merges its edge bodies into one join before \
                     resolution, so each body must be join/filter normal form: \
                     relations and conditions only. Restructure the edge body, \
                     or call the edge directly with &"
                ),
                "transitive composition is structural: bodies merge before resolution",
            ))
        }
    }
}

/// Rebuild a flat unresolved expression from a list of relations and conditions.
/// Produces a left-deep Join tree of all relations, then wraps with Filter layers
/// for each condition.
fn rebuild_flat_expression(
    relations: Vec<ast_unresolved::Relation>,
    conditions: Vec<ast_unresolved::SigmaCondition>,
) -> Result<ast_unresolved::RelationalExpression> {
    // Build left-deep join tree from relations
    let mut iter = relations.into_iter();
    let mut expr = ast_unresolved::RelationalExpression::Relation(iter.next().ok_or_else(
        || {
            DelightQLError::validation_error(
                "ER chain composed to zero relations — the normal-form and \
                 shared-endpoint refusals should have caught this earlier; \
                 this is a dql bug",
                "Invalid ER-join chain",
            )
        },
    )?);
    for rel in iter {
        expr = ast_unresolved::RelationalExpression::Join {
            left: Box::new(expr),
            right: Box::new(ast_unresolved::RelationalExpression::Relation(rel)),
            join_condition: None,
            join_type: None,
            cpr_schema: ast_unresolved::PhaseBox::phantom(),
        };
    }

    // Wrap with filter layers for each condition
    for cond in conditions {
        expr = ast_unresolved::RelationalExpression::Filter {
            source: Box::new(expr),
            condition: cond,
            origin: crate::pipeline::asts::core::FilterOrigin::UserWritten,
            cpr_schema: ast_unresolved::PhaseBox::phantom(),
        };
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
            let parts: Vec<String> = data_ns_fq.split("::").map(|s| s.to_string()).collect();
            let data_ns = ast_unresolved::NamespacePath::from_parts(parts).ok()?;
            let ns_parts: Vec<String> = rule_ns.split("::").map(|s| s.to_string()).collect();
            let grounded_ns = ast_unresolved::NamespacePath::from_parts(ns_parts).ok()?;
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
        let ddl_def =
            crate::ddl::ddl_builder::build_single_definition(&rule.definition).map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Error parsing ER-rule body for ({}, {}) in context '{}': {}",
                        left_name, right_name, context.context_name, e
                    ),
                    e.to_string(),
                )
            })?;
        ddl_def.into_query().ok_or_else(|| {
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
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
    endpoints_only: Option<Vec<String>>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
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
    // `&&` over a directly-declared edge: endpoints only, same law.
    let query = match &endpoints_only {
        Some(published) => er_endpoints_projection(query, published),
        None => query,
    };


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
            let parts: Vec<String> = data_ns_fq.split("::").map(|s| s.to_string()).collect();
            let data_ns = ast_unresolved::NamespacePath::from_parts(parts).ok()?;
            let ns_parts: Vec<String> = rule_ns.split("::").map(|s| s.to_string()).collect();
            let grounded_ns = ast_unresolved::NamespacePath::from_parts(ns_parts).ok()?;
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
                // The boundary export IS the pair-schema proof: the
                // endpoint globs locate each published term's columns in
                // the body's final heading. A glob that matches nothing
                // means the body renamed or projected an endpoint away —
                // a pair-set violation, taught as such, not as a bare
                // glob miss.
                let msg = e.to_string();
                if endpoints_only.is_some() && msg.contains("matched no columns") {
                    let missing = msg
                        .split('\'')
                        .nth(1)
                        .unwrap_or("an endpoint")
                        .trim_end_matches(".*");
                    return DelightQLError::validation_error_categorized(
                        "grounding/er/pair_schema",
                        format!(
                            "the edge body for ({left_name}, {right_name}) in \
                             '::{}' does not publish '{missing}' — an edge is a \
                             PAIR-SET: its body may derive the pairs freely \
                             (filter, helper joins, computed keys, aggregates) \
                             but its final heading must carry both endpoints' \
                             columns; they are the edge's published schema. \
                             Rename and narrow at the call site, after \
                             selection, not inside the edge",
                            context.context_name
                        ),
                        "the published schema of an edge is schema(A) + schema(B); \
                         the boundary exports those columns and hides the rest",
                    );
                }
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
    match resolved_query {
        ast_resolved::Query::Relational(expr) => Ok((expr, body_bubbled)),
        _ => Err(DelightQLError::validation_error(
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
fn add_self_aliases_to_query(query: ast_unresolved::Query) -> ast_unresolved::Query {
    match query {
        ast_unresolved::Query::Relational(expr) => {
            ast_unresolved::Query::Relational(add_self_aliases_to_expr(expr))
        }
        ast_unresolved::Query::WithCtes { ctes, query } => ast_unresolved::Query::WithCtes {
            ctes,
            query: add_self_aliases_to_expr(query),
        },
        other => other,
    }
}

fn add_self_aliases_to_expr(
    expr: ast_unresolved::RelationalExpression,
) -> ast_unresolved::RelationalExpression {
    match expr {
        ast_unresolved::RelationalExpression::Relation(rel) => {
            ast_unresolved::RelationalExpression::Relation(add_self_alias_to_relation(rel))
        }
        ast_unresolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => ast_unresolved::RelationalExpression::Join {
            left: Box::new(add_self_aliases_to_expr(*left)),
            right: Box::new(add_self_aliases_to_expr(*right)),
            join_condition,
            join_type,
            cpr_schema,
        },
        ast_unresolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => ast_unresolved::RelationalExpression::Filter {
            source: Box::new(add_self_aliases_to_expr(*source)),
            condition,
            origin,
            cpr_schema,
        },
        other => other,
    }
}

fn add_self_alias_to_relation(rel: ast_unresolved::Relation) -> ast_unresolved::Relation {
    match rel {
        ast_unresolved::Relation::Ground {
            identifier,
            canonical_name,
            backend_schema,
            domain_spec,
            alias: None,
            outer,
            mutation_target,
            passthrough,
            cpr_schema,
            hygienic_injections,
        } => ast_unresolved::Relation::Ground {
            alias: Some(identifier.name.clone()),
            identifier,
            canonical_name,
            backend_schema,
            domain_spec,
            outer,
            mutation_target,
            passthrough,
            cpr_schema,
            hygienic_injections,
        },
        other => other,
    }
}

/// Expand an ErTransitiveJoin by building a graph of all ER-rules in the context,
/// finding a path from left to right, and expanding it as an ErJoinChain.
fn expand_er_transitive_join(
    left: ast_unresolved::RelationalExpression,
    right: ast_unresolved::RelationalExpression,
    left_spelling: &str,
    right_spelling: &str,
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    // Extract table names (and alias/domain_spec) from endpoints
    let (left_name, left_alias, _left_domain_spec) = match &left {
        ast_unresolved::RelationalExpression::Relation(rel) => match rel {
            ast_unresolved::Relation::Ground {
                identifier,
                alias,
                domain_spec,
                ..
            } => (
                identifier.name.to_string(),
                alias.clone(),
                domain_spec.clone(),
            ),
            _ => {
                return Err(DelightQLError::validation_error(
                    "Left side of && must be a table reference",
                    "Invalid ER-transitive-join operand",
                ))
            }
        },
        _ => {
            return Err(DelightQLError::validation_error(
                "Left side of && must be a table reference",
                "Invalid ER-transitive-join operand",
            ))
        }
    };
    let (right_name, right_alias, _right_domain_spec) = match &right {
        ast_unresolved::RelationalExpression::Relation(rel) => match rel {
            ast_unresolved::Relation::Ground {
                identifier,
                alias,
                domain_spec,
                ..
            } => (
                identifier.name.to_string(),
                alias.clone(),
                domain_spec.clone(),
            ),
            _ => {
                return Err(DelightQLError::validation_error(
                    "Right side of && must be a table reference",
                    "Invalid ER-transitive-join operand",
                ))
            }
        },
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
    let chain_relations: Vec<ast_unresolved::Relation> = path
        .iter()
        .enumerate()
        .map(|(i, spelling)| {
            if i == 0 {
                if let ast_unresolved::RelationalExpression::Relation(rel) = &left {
                    return rel.clone();
                }
            }
            if i == path.len() - 1 {
                if let ast_unresolved::RelationalExpression::Relation(rel) = &right {
                    return rel.clone();
                }
            }
            let head = spelling.split('(').next().unwrap_or(spelling).trim();
            ast_unresolved::Relation::Ground {
                identifier: ast_unresolved::QualifiedName {
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                    name: head.into(),
                    grounding: None,
                },
                canonical_name: ast_unresolved::PhaseBox::phantom(),
                backend_schema: ast_unresolved::PhaseBox::phantom(),
                domain_spec: ast_unresolved::DomainSpec::Glob,
                alias: None,
                outer: false,
                mutation_target: false,
                passthrough: false,
                cpr_schema: ast_unresolved::PhaseBox::phantom(),
                hygienic_injections: Vec::new(),
            }
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
        er_stamp_endpoint_access_names(
            &mut bubbled,
            &[left_name.clone(), right_name.clone()],
        );
        let (expr, bubbled) = er_thread_endpoint_aliases(
            expr,
            bubbled,
            (&left_name, &left_alias),
            (&right_name, &right_alias),
        );
        let expr = er_sync_pipe_schema(expr, &bubbled);
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
    expr: ast_resolved::RelationalExpression,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
) -> ast_resolved::RelationalExpression {
    match expr {
        ast_resolved::RelationalExpression::Relation(rel) => {
            ast_resolved::RelationalExpression::Relation(match rel {
                ast_resolved::Relation::Ground {
                    identifier,
                    canonical_name,
                    backend_schema,
                    domain_spec,
                    alias,
                    outer,
                    mutation_target,
                    passthrough,
                    cpr_schema,
                    hygienic_injections,
                } => {
                    let current = alias.as_ref().map(|a| a.to_string()).unwrap_or_default();
                    if current == old_name {
                        let schema = rename_schema(cpr_schema.get().clone(), old_name, new_name);
                        ast_resolved::Relation::Ground {
                            identifier,
                            canonical_name,
                            backend_schema,
                            domain_spec,
                            alias: Some(new_name.clone()),
                            outer,
                            mutation_target,
                            passthrough,
                            cpr_schema: ast_resolved::PhaseBox::new(schema),
                            hygienic_injections,
                        }
                    } else {
                        ast_resolved::Relation::Ground {
                            identifier,
                            canonical_name,
                            backend_schema,
                            domain_spec,
                            alias,
                            outer,
                            mutation_target,
                            passthrough,
                            cpr_schema,
                            hygienic_injections,
                        }
                    }
                }
                ast_resolved::Relation::ConsultedView {
                    identifier,
                    body,
                    scoped,
                    outer,
                } => {
                    let current_alias = scoped.get().alias().to_string();
                    if current_alias == old_name {
                        let old_schema = scoped.get().schema().clone();
                        let renamed_schema = rename_schema(old_schema, old_name, new_name);
                        let new_scoped = ast_resolved::ScopedSchema::from_parts(
                            new_name.clone(),
                            renamed_schema,
                        );
                        ast_resolved::Relation::ConsultedView {
                            identifier,
                            body,
                            scoped: ast_resolved::PhaseBox::new(new_scoped),
                            outer,
                        }
                    } else {
                        ast_resolved::Relation::ConsultedView {
                            identifier,
                            body,
                            scoped,
                            outer,
                        }
                    }
                }
                ast_resolved::Relation::InnerRelation {
                    pattern,
                    alias,
                    outer,
                    cpr_schema,
                } => {
                    let current = alias.as_ref().map(|a| a.to_string()).unwrap_or_default();
                    if current == old_name {
                        let schema = rename_schema(cpr_schema.get().clone(), old_name, new_name);
                        ast_resolved::Relation::InnerRelation {
                            pattern,
                            alias: Some(new_name.clone()),
                            outer,
                            cpr_schema: ast_resolved::PhaseBox::new(schema),
                        }
                    } else {
                        ast_resolved::Relation::InnerRelation {
                            pattern,
                            alias,
                            outer,
                            cpr_schema,
                        }
                    }
                }
                other => other,
            })
        }
        ast_resolved::RelationalExpression::Join {
            left,
            right,
            mut join_condition,
            join_type,
            cpr_schema,
        } => {
            let left = Box::new(rename_in_resolved_expr(*left, old_name, new_name));
            let right = Box::new(rename_in_resolved_expr(*right, old_name, new_name));
            if let Some(ref mut cond) = join_condition {
                rename_qualifier_in_resolved_boolean(cond, old_name, new_name);
            }
            let schema = rename_schema(cpr_schema.get().clone(), old_name, new_name);
            ast_resolved::RelationalExpression::Join {
                left,
                right,
                join_condition,
                join_type,
                cpr_schema: ast_resolved::PhaseBox::new(schema),
            }
        }
        ast_resolved::RelationalExpression::Filter {
            source,
            mut condition,
            origin,
            cpr_schema,
        } => {
            let source = Box::new(rename_in_resolved_expr(*source, old_name, new_name));
            if let ast_resolved::SigmaCondition::Predicate(ref mut pred) = condition {
                rename_qualifier_in_resolved_boolean(pred, old_name, new_name);
            }
            let schema = rename_schema(cpr_schema.get().clone(), old_name, new_name);
            ast_resolved::RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema: ast_resolved::PhaseBox::new(schema),
            }
        }
        other => other,
    }
}

/// Rename table references in a CprSchema.
fn rename_schema(
    schema: ast_resolved::CprSchema,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
) -> ast_resolved::CprSchema {
    match schema {
        ast_resolved::CprSchema::Resolved(cols) => ast_resolved::CprSchema::Resolved(
            cols.into_iter()
                .map(|mut col| {
                    if let ast_resolved::TableName::Named(tn) = col.qualifier() {
                        if tn == old_name {
                            let prev = tn.to_string();
                            col.push_scope(
                                ast_resolved::TableName::Named(new_name.clone()),
                                ast_resolved::IdentityContext::SubqueryAlias {
                                    alias: new_name.to_string(),
                                    previous_context: prev,
                                    resolver_id: None,
                                },
                            );
                        }
                    }
                    col
                })
                .collect(),
        ),
        other => other,
    }
}

/// Rename table references in both halves of the bubbled lexical state.
fn rename_bubbled_columns(
    bubbled: &mut BubbledState,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
) {
    // The answering channel renames with the table: access_name and
    // full-name spellings ("users_t.name|2|") answer to the alias after
    // an endpoint rename, or qualified refs through it die.
    let rename_answering = |col: &mut ast_resolved::ColumnMetadata| {
        col.rename_answering_from(old_name, new_name);
        // Full-name spellings rename with the endpoint: a column named
        // "users_t.name|2|" answers to "u.name" after `as u` — the
        // unify full-name tier matches the NEW spelling, so the
        // reference rewrites to the real column instead of riding
        // through unvalidated and dying at the transformer.
        let name = col.name().to_string();
        if let Some(rest) = name.strip_prefix(&format!("{}.", old_name)) {
            col.info = col
                .info
                .clone()
                .with_alias(format!("{}.{}", new_name, rest));
        }
    };
    for col in &mut bubbled.i_provide {
        rename_answering(col);
        if let ast_resolved::TableName::Named(tn) = col.qualifier() {
            if tn == old_name {
                let prev = tn.to_string();
                col.push_scope(
                    ast_resolved::TableName::Named(new_name.clone()),
                    ast_resolved::IdentityContext::SubqueryAlias {
                        alias: new_name.to_string(),
                        previous_context: prev,
                        resolver_id: None,
                    },
                );
            }
        }
    }
    for col in &mut bubbled.qualifier_scope {
        rename_answering(col);
        if let ast_resolved::TableName::Named(tn) = col.qualifier() {
            if tn == old_name {
                let prev = tn.to_string();
                col.push_scope(
                    ast_resolved::TableName::Named(new_name.clone()),
                    ast_resolved::IdentityContext::SubqueryAlias {
                        alias: new_name.to_string(),
                        previous_context: prev,
                        resolver_id: None,
                    },
                );
            }
        }
    }
}

/// Rename qualifiers in a resolved boolean expression.
fn rename_qualifier_in_resolved_boolean(
    expr: &mut ast_resolved::BooleanExpression,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
) {
    match expr {
        ast_resolved::BooleanExpression::Comparison { left, right, .. } => {
            rename_qualifier_in_resolved_domain(left, old_name, new_name);
            rename_qualifier_in_resolved_domain(right, old_name, new_name);
        }
        ast_resolved::BooleanExpression::And { left, right }
        | ast_resolved::BooleanExpression::Or { left, right } => {
            rename_qualifier_in_resolved_boolean(left, old_name, new_name);
            rename_qualifier_in_resolved_boolean(right, old_name, new_name);
        }
        ast_resolved::BooleanExpression::Not { expr } => {
            rename_qualifier_in_resolved_boolean(expr, old_name, new_name);
        }
        _ => {}
    }
}

/// Rename qualifiers in a resolved domain expression.
fn rename_qualifier_in_resolved_domain(
    expr: &mut ast_resolved::DomainExpression,
    old_name: &str,
    new_name: &delightql_types::SqlIdentifier,
) {
    match expr {
        ast_resolved::DomainExpression::Lvar {
            qualifier: Some(q), ..
        } if q == old_name => {
            *q = new_name.clone();
        }
        ast_resolved::DomainExpression::Function(func) => match func {
            ast_resolved::FunctionExpression::Infix { left, right, .. } => {
                rename_qualifier_in_resolved_domain(left, old_name, new_name);
                rename_qualifier_in_resolved_domain(right, old_name, new_name);
            }
            ast_resolved::FunctionExpression::Regular { arguments, .. }
            | ast_resolved::FunctionExpression::Curried { arguments, .. } => {
                for arg in arguments {
                    rename_qualifier_in_resolved_domain(arg, old_name, new_name);
                }
            }
            _ => {}
        },
        ast_resolved::DomainExpression::Parenthesized { inner, .. } => {
            rename_qualifier_in_resolved_domain(inner, old_name, new_name);
        }
        _ => {}
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

/// Extract grounding from a pipe source expression.
/// Walks through Filter/Pipe wrappers to find the root Relation::Ground and
/// its grounding annotation. Used to extract the data namespace for patching
/// table holes in borrowed function bodies (see test 305).
#[stacksafe::stacksafe]
fn extract_grounding_from_source(
    expr: &ast_unresolved::RelationalExpression,
) -> Option<ast_unresolved::GroundedPath> {
    match expr {
        ast_unresolved::RelationalExpression::Relation(rel) => {
            if let ast_unresolved::Relation::Ground { identifier, .. } = rel {
                identifier.grounding.clone()
            } else {
                None
            }
        }
        // Source-spine descent: follow `source` only. `condition` is a recursive
        // field the base-spine contract DELIBERATELY ignores — grounding comes
        // from the base relation, not a predicate subquery (spelled `_` per R-I3;
        // origin/cpr_schema are non-recursive metadata under `..`).
        ast_unresolved::RelationalExpression::Filter {
            source,
            condition: _,
            ..
        } => extract_grounding_from_source(source),
        ast_unresolved::RelationalExpression::Pipe(pipe) => {
            extract_grounding_from_source(&pipe.source)
        }
        // Join/SetOp/ER sources don't have a single grounding — return None.
        ast_unresolved::RelationalExpression::Join { .. }
        | ast_unresolved::RelationalExpression::SetOperation { .. }
        | ast_unresolved::RelationalExpression::ErJoinChain { .. }
        | ast_unresolved::RelationalExpression::ErTransitiveJoin { .. }
        | ast_unresolved::RelationalExpression::IntersectCorresponding { .. } => None,
    }
}

/// Extract IN predicate values from an unresolved source relational expression.
/// Returns a mapping of column_name → literal values for each IN predicate found.
/// Used by pivot resolution to determine what columns to generate.
fn extract_in_predicate_values(
    source: &ast_unresolved::RelationalExpression,
) -> HashMap<String, Vec<String>> {
    let mut result = HashMap::new();
    scan_for_in_predicates_in_scope(source, &mut result);
    result
}

/// SCOPE-LOCAL (INVENTORY L1): collects IN-predicate values for pivot within the
/// CURRENT query scope — reads `Filter.condition` and recurses the relational
/// spine, but deliberately does NOT enter nested subquery scopes (those are
/// scanned at their own level). The `_in_scope` name marks that stop boundary.
#[stacksafe::stacksafe]
fn scan_for_in_predicates_in_scope(
    expr: &ast_unresolved::RelationalExpression,
    result: &mut HashMap<String, Vec<String>>,
) {
    match expr {
        ast_unresolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            if let ast_unresolved::SigmaCondition::Predicate(bool_expr) = condition {
                extract_in_from_boolean(bool_expr, result);
            }
            scan_for_in_predicates_in_scope(source, result);
        }
        ast_unresolved::RelationalExpression::Pipe(pipe) => {
            scan_for_in_predicates_in_scope(&pipe.source, result);
        }
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            scan_for_in_predicates_in_scope(left, result);
            scan_for_in_predicates_in_scope(right, result);
        }
        ast_unresolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                scan_for_in_predicates_in_scope(operand, result);
            }
        }
        // Base cases: leaf relations have no predicates to scan.
        ast_unresolved::RelationalExpression::Relation(
            ast_unresolved::Relation::Ground { .. }
            | ast_unresolved::Relation::Anonymous { .. }
            | ast_unresolved::Relation::TVF { .. }
            | ast_unresolved::Relation::InnerRelation { .. }
            | ast_unresolved::Relation::PseudoPredicate { .. }
            | ast_unresolved::Relation::ConsultedView { .. },
        ) => {}
        // ER chains are unresolved-only and shouldn't appear in pivot context,
        // but recurse defensively.
        ast_unresolved::RelationalExpression::ErJoinChain { .. }
        | ast_unresolved::RelationalExpression::ErTransitiveJoin { .. } => {}
        ast_unresolved::RelationalExpression::IntersectCorresponding { .. } => {
            unreachable!("IntersectCorresponding only exists in Refined/Addressed phases")
        }
    }
}

#[stacksafe::stacksafe]
fn extract_in_from_boolean(
    expr: &ast_unresolved::BooleanExpression,
    result: &mut HashMap<String, Vec<String>>,
) {
    match expr {
        ast_unresolved::BooleanExpression::In {
            value,
            set,
            negated: false,
        } => {
            let col_name = match value.as_ref() {
                ast_unresolved::DomainExpression::Lvar { name, .. } => Some(name.clone()),
                // Non-lvar IN values (expressions, functions, etc.) can't be
                // mapped to a column name for pivot — skip.
                _ => None,
            };
            if let Some(name) = col_name {
                let values: Vec<String> = set
                    .iter()
                    .filter_map(|e| {
                        if let ast_unresolved::DomainExpression::Literal {
                            value: ast_unresolved::LiteralValue::String(s),
                            ..
                        } = e
                        {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !values.is_empty() {
                    result.insert(name.to_string(), values);
                }
            }
        }
        ast_unresolved::BooleanExpression::And { left, right }
        | ast_unresolved::BooleanExpression::Or { left, right } => {
            extract_in_from_boolean(left, result);
            extract_in_from_boolean(right, result);
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            extract_in_from_boolean(expr, result);
        }
        // Leaf boolean expressions: no IN predicates to extract.
        ast_unresolved::BooleanExpression::Comparison { .. }
        | ast_unresolved::BooleanExpression::Using { .. }
        | ast_unresolved::BooleanExpression::InnerExists { .. }
        | ast_unresolved::BooleanExpression::InRelational { .. }
        | ast_unresolved::BooleanExpression::BooleanLiteral { .. }
        | ast_unresolved::BooleanExpression::Sigma { .. }
        | ast_unresolved::BooleanExpression::GlobCorrelation { .. }
        | ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { .. } => {}
        // Negated IN: not extracted (pivot only uses positive IN)
        ast_unresolved::BooleanExpression::In { negated: true, .. } => {}
    }
}

/// Extract IN predicate values from a **resolved** source expression.
/// Handles `InRelational` where the subquery is an anonymous fact table with literal rows.
/// This catches cases that the unresolved extractor misses (e.g., ordinal references
/// like `|2| in V(*)` from HO expansion, where the column name is only known after resolution).
fn extract_in_predicate_values_from_resolved(
    source: &ast_resolved::RelationalExpression,
) -> HashMap<String, Vec<String>> {
    let mut result = HashMap::new();
    scan_resolved_for_in_predicates(source, &mut result);
    result
}

#[stacksafe::stacksafe]
fn scan_resolved_for_in_predicates(
    expr: &ast_resolved::RelationalExpression,
    result: &mut HashMap<String, Vec<String>>,
) {
    match expr {
        ast_resolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            if let ast_resolved::SigmaCondition::Predicate(bool_expr) = condition {
                extract_in_from_resolved_boolean(bool_expr, result);
            }
            scan_resolved_for_in_predicates(source, result);
        }
        ast_resolved::RelationalExpression::Pipe(pipe) => {
            scan_resolved_for_in_predicates(&pipe.source, result);
        }
        // Join: IN predicates could exist in Filter nodes inside either branch.
        ast_resolved::RelationalExpression::Join { left, right, .. } => {
            scan_resolved_for_in_predicates(left, result);
            scan_resolved_for_in_predicates(right, result);
        }
        // SetOperation: recurse into operands for nested Filters.
        ast_resolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                scan_resolved_for_in_predicates(operand, result);
            }
        }
        // Leaf node: no boolean predicates to extract from.
        ast_resolved::RelationalExpression::Relation(_) => {}
        // ER chains consumed during resolution — should never reach here.
        ast_resolved::RelationalExpression::ErJoinChain { .. }
        | ast_resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before IN predicate scanning")
        }
        ast_resolved::RelationalExpression::IntersectCorresponding { .. } => {
            unreachable!("IntersectCorresponding only exists in Refined/Addressed phases")
        }
    }
}

#[stacksafe::stacksafe]
fn extract_in_from_resolved_boolean(
    expr: &ast_resolved::BooleanExpression,
    result: &mut HashMap<String, Vec<String>>,
) {
    match expr {
        ast_resolved::BooleanExpression::InRelational {
            value,
            subquery,
            negated: false,
            ..
        } => {
            // Extract the resolved column name from LHS
            let col_name = match value.as_ref() {
                ast_resolved::DomainExpression::Lvar { name, .. } => Some(name.clone()),
                // Non-column LHS (function call, literal, parenthesized, etc.) — can't
                // provide a column name for pivot optimization. Dispensation: any new
                // DomainExpression variant would also not be a bare column reference.
                _ => None,
            };
            if let Some(name) = col_name {
                // Walk through Pipe/Qualify wrappers to find the anonymous table
                let inner = unwrap_resolved_pipe(subquery.as_ref());
                if let Some(rows) = extract_literal_rows_from_resolved(inner) {
                    if !rows.is_empty() {
                        result.insert(name.to_string(), rows);
                    }
                }
            }
        }
        ast_resolved::BooleanExpression::And { left, right } => {
            extract_in_from_resolved_boolean(left, result);
            extract_in_from_resolved_boolean(right, result);
        }
        // Negated InRelational: pivot only uses positive IN predicates.
        ast_resolved::BooleanExpression::InRelational { negated: true, .. } => {}
        // Or: IN predicates inside OR branches change semantics — don't extract.
        // Not: negation wrapper — no positive IN to extract.
        ast_resolved::BooleanExpression::Or { .. }
        | ast_resolved::BooleanExpression::Not { .. } => {}
        // Remaining boolean expressions: no InRelational predicates inside.
        ast_resolved::BooleanExpression::Comparison { .. }
        | ast_resolved::BooleanExpression::Using { .. }
        | ast_resolved::BooleanExpression::InnerExists { .. }
        | ast_resolved::BooleanExpression::In { .. }
        | ast_resolved::BooleanExpression::BooleanLiteral { .. }
        | ast_resolved::BooleanExpression::Sigma { .. }
        | ast_resolved::BooleanExpression::GlobCorrelation { .. }
        | ast_resolved::BooleanExpression::OrdinalGlobCorrelation { .. } => {}
    }
}

/// Unwrap Pipe/Qualify wrappers to get the inner relation.
#[stacksafe::stacksafe]
fn unwrap_resolved_pipe(
    expr: &ast_resolved::RelationalExpression,
) -> &ast_resolved::RelationalExpression {
    match expr {
        ast_resolved::RelationalExpression::Pipe(pipe) => unwrap_resolved_pipe(&pipe.source),
        other => other,
    }
}

/// Extract string literal values from a resolved anonymous table's rows.
/// Strip Glob→Bare on a Ground relation after consuming a Using pipe.
///
/// The `*` in `*.(cols)` produces DomainSpec::Glob on the Ground. After converting
/// the Using pipe to correlation filters, the Glob is redundant — the table still
/// exposes all columns (Bare does this too). Stripping prevents the transformer from
/// wrapping the inner table in a `(SELECT * FROM ...) AS tN` derived table, which
/// would break qualifier resolution for correlated references.
/// Walk a resolved expression to find the base Ground relation's table name.
/// Traverses through Pipes and Filters to reach the Ground node.
#[stacksafe::stacksafe]
fn extract_base_ground_name(
    expr: &ast_resolved::RelationalExpression,
) -> Option<delightql_types::SqlIdentifier> {
    match expr {
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Ground {
            identifier,
            ..
        }) => Some(identifier.name.clone()),
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::ConsultedView {
            identifier,
            ..
        }) => Some(identifier.name.clone()),
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::InnerRelation {
            alias,
            pattern,
            ..
        }) => {
            // InnerRelation: use alias if present, otherwise extract from pattern
            if let Some(a) = alias {
                Some(a.clone())
            } else {
                match pattern {
                    ast_resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                        identifier,
                        ..
                    } => Some(identifier.name.clone()),
                    _ => None,
                }
            }
        }
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Anonymous {
            alias,
            ..
        }) => alias.clone(),
        ast_resolved::RelationalExpression::Pipe(pipe) => extract_base_ground_name(&pipe.source),
        ast_resolved::RelationalExpression::Filter { source, .. } => {
            extract_base_ground_name(source)
        }
        ast_resolved::RelationalExpression::Join { left, .. } => extract_base_ground_name(left),
        _ => None,
    }
}

/// Walk an unresolved relational expression to collect Ground relations
/// marked with `mutation_target: true`, returning their table names.
#[stacksafe::stacksafe]
fn find_mutation_targets(expr: &ast_unresolved::RelationalExpression) -> Vec<String> {
    let mut targets = Vec::new();
    match expr {
        ast_unresolved::RelationalExpression::Relation(rel) => {
            if let ast_unresolved::Relation::Ground {
                identifier,
                mutation_target: true,
                ..
            } = rel
            {
                targets.push(identifier.name.to_string());
            }
        }
        ast_unresolved::RelationalExpression::Pipe(pipe) => {
            targets.extend(find_mutation_targets(&pipe.source));
        }
        ast_unresolved::RelationalExpression::Filter { source, .. } => {
            targets.extend(find_mutation_targets(source));
        }
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            targets.extend(find_mutation_targets(left));
            targets.extend(find_mutation_targets(right));
        }
        ast_unresolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                targets.extend(find_mutation_targets(operand));
            }
        }
        // ER chains: wrap relations and recurse
        ast_unresolved::RelationalExpression::ErJoinChain { relations, .. } => {
            for rel in relations {
                targets.extend(find_mutation_targets(
                    &ast_unresolved::RelationalExpression::Relation(rel.clone()),
                ));
            }
        }
        ast_unresolved::RelationalExpression::ErTransitiveJoin { left, right, .. } => {
            targets.extend(find_mutation_targets(left));
            targets.extend(find_mutation_targets(right));
        }
        ast_unresolved::RelationalExpression::IntersectCorresponding { .. } => {
            unreachable!("IntersectCorresponding only exists in Refined/Addressed phases")
        }
    }
    targets
}

/// Classifications of pipe operators in the chain before a DML terminal.
/// Used for DML shape validation.
#[derive(Debug)]
enum DmlPipeKind {
    Transform,
    ProjectOut,
    RenameCover,
    TupleOrdering,
    Modulo,
    AggregatePipe,
    General,
}

/// Classify a single unresolved operator into a DmlPipeKind.
/// Used by linearized pipe resolution to build DML pipe ops from collected segments.
fn classify_single_dml_op(op: &ast_unresolved::UnaryRelationalOperator) -> DmlPipeKind {
    match op {
        ast_unresolved::UnaryRelationalOperator::Transform { .. } => DmlPipeKind::Transform,
        ast_unresolved::UnaryRelationalOperator::General { .. } => DmlPipeKind::General,
        ast_unresolved::UnaryRelationalOperator::ProjectOut { .. } => DmlPipeKind::ProjectOut,
        ast_unresolved::UnaryRelationalOperator::RenameCover { .. } => DmlPipeKind::RenameCover,
        ast_unresolved::UnaryRelationalOperator::TupleOrdering { .. } => DmlPipeKind::TupleOrdering,
        ast_unresolved::UnaryRelationalOperator::Modulo { .. } => DmlPipeKind::Modulo,
        ast_unresolved::UnaryRelationalOperator::AggregatePipe { .. } => DmlPipeKind::AggregatePipe,
        ast_unresolved::UnaryRelationalOperator::MapCover { .. }
        | ast_unresolved::UnaryRelationalOperator::EmbedMapCover { .. }
        | ast_unresolved::UnaryRelationalOperator::Reposition { .. }
        | ast_unresolved::UnaryRelationalOperator::MetaIze
        | ast_unresolved::UnaryRelationalOperator::Witness { .. }
        | ast_unresolved::UnaryRelationalOperator::Qualify
        | ast_unresolved::UnaryRelationalOperator::Using { .. }
        | ast_unresolved::UnaryRelationalOperator::UsingAll
        | ast_unresolved::UnaryRelationalOperator::HoViewApplication { .. }
        | ast_unresolved::UnaryRelationalOperator::InteriorDrillDown { .. }
        | ast_unresolved::UnaryRelationalOperator::NarrowingDestructure { .. }
        | ast_unresolved::UnaryRelationalOperator::DirectiveTerminal { .. }
        | ast_unresolved::UnaryRelationalOperator::SignedWitness
        | ast_unresolved::UnaryRelationalOperator::DirectivePipeInvocation { .. }
        | ast_unresolved::UnaryRelationalOperator::DmlTerminal { .. } => DmlPipeKind::General,
    }
}

/// Insert correlation filters at the base of a pipe chain, directly above
/// the innermost non-Pipe expression (typically a Ground relation).
/// This ensures the filter's qualifiers match the Ground table name.
#[stacksafe::stacksafe]
fn insert_filters_at_base(
    expr: ast_resolved::RelationalExpression,
    filters: Vec<ast_resolved::SigmaCondition>,
) -> ast_resolved::RelationalExpression {
    if filters.is_empty() {
        return expr;
    }
    match expr {
        ast_resolved::RelationalExpression::Pipe(pipe) => {
            let pipe = (*pipe).into_inner();
            let wrapped_source = insert_filters_at_base(pipe.source, filters);
            ast_resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                ast_resolved::PipeExpression {
                    source: wrapped_source,
                    operator: pipe.operator,
                    cpr_schema: pipe.cpr_schema,
                },
            )))
        }
        base => {
            let schema = extract_cpr_schema(&base).unwrap_or(ast_resolved::CprSchema::Unknown);
            let mut result = base;
            for filter in filters {
                result = ast_resolved::RelationalExpression::Filter {
                    source: Box::new(result),
                    condition: filter,
                    origin: ast_resolved::FilterOrigin::Generated,
                    cpr_schema: ast_resolved::PhaseBox::new(schema.clone()),
                };
            }
            result
        }
    }
}

fn extract_literal_rows_from_resolved(
    expr: &ast_resolved::RelationalExpression,
) -> Option<Vec<String>> {
    if let ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Anonymous {
        rows,
        ..
    }) = expr
    {
        let values: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                if row.values.len() == 1 {
                    if let ast_resolved::DomainExpression::Literal {
                        value: ast_resolved::LiteralValue::String(s),
                        ..
                    } = &row.values[0]
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
