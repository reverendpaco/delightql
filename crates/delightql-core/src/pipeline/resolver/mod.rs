use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use delightql_types::error::{DelightQLError, Result};
use std::collections::HashMap;

/// Case-insensitive column name comparison, matching SQL semantics
/// where unquoted identifiers are case-insensitive.
pub(crate) fn col_name_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

mod pattern_resolver;
pub use pattern_resolver::{JoinContext, PatternResolver};

mod string_templates;

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
}

impl Default for ResolutionConfig {
    fn default() -> Self {
        Self {
            permissive: true, // Default to permissive mode
            transpile_only: false,
            validate_in_correlation: false,
            er_context: None,
            resolution_namespace: None,
        }
    }
}

pub mod unification;
use unification::ColumnReference;

pub(crate) mod helpers;
use self::helpers::*;
mod bubbling;
use self::bubbling::*;
pub(crate) mod resolving;
mod cte_validation;
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
mod relation_resolver;
pub(crate) mod grounding;
mod resolver_fold;
use resolver_fold::ResolverFold;

#[derive(Debug, Clone)]
pub struct BubbledState {
    pub i_provide: Vec<ast_resolved::ColumnMetadata>,
    pub i_need: Vec<ColumnReference>,
}

impl BubbledState {
    pub fn resolved(columns: Vec<ast_resolved::ColumnMetadata>) -> Self {
        Self {
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
        }
    }

    pub fn combine(left: BubbledState, right: BubbledState) -> Self {
        let mut combined_provide = left.i_provide;
        combined_provide.extend(right.i_provide);

        let mut combined_need = left.i_need;
        combined_need.extend(right.i_need);

        Self {
            i_provide: combined_provide,
            i_need: combined_need,
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
    let (query, ccafe_cfes) = grounding::inline_in_query_borrowed(query, &registry.consult, None)?;

    // If any context-aware DDL functions (type=3) were discovered during inlining,
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
            // Group CTEs by name for merging
            let mut cte_groups: HashMap<String, Vec<ast_unresolved::CteBinding>> = HashMap::new();
            let mut cte_order: Vec<String> = Vec::new(); // Track order of first appearance

            for cte in ctes {
                let name = cte.name.clone();
                let is_new = !cte_groups.contains_key(&name);
                cte_groups.entry(name.clone()).or_default().push(cte);

                // Track the order of first appearance for each unique name
                if is_new {
                    cte_order.push(name);
                }
            }

            // Validate CTE dependencies on the grouped structure
            validate_grouped_cte_dependencies(&cte_groups, &cte_order)?;

            // Process each group of CTEs in order of first appearance
            let mut resolved_ctes = Vec::new();

            for name in &cte_order {
                let group = cte_groups
                    .remove(name)
                    .expect("CTE should exist after topological sort - invariant violation");
                if group.len() == 1 {
                    // Single CTE - process normally
                    let cte = group
                        .into_iter()
                        .next()
                        .expect("Group has len==1, must have element - invariant");
                    let (resolved_expr, _) = fold.resolve_relational(cte.expression)?;
                    let mut cte_schema = extract_cpr_schema(&resolved_expr)?;
                    // Transform the schema to use the CTE's name as the table name
                    cte_schema = transform_schema_table_names(cte_schema, name);
                    // Register the CTE in the EntityRegistry
                    fold.registry
                        .query_local
                        .register_cte(name.clone(), cte_schema);

                    resolved_ctes.push(ast_resolved::CteBinding {
                        expression: resolved_expr,
                        name: name.clone(),
                        is_recursive: ast_resolved::PhaseBox::phantom(),
                    });
                } else {
                    // Multiple CTEs with same name - create UNION
                    let mut operands = Vec::new();
                    let mut schemas = Vec::new();
                    let mut all_schemas_same = true;

                    for (idx, cte) in group.iter().enumerate() {
                        let (resolved_expr, _) =
                            fold.resolve_relational(cte.expression.clone())?;
                        let expr_schema = extract_cpr_schema(&resolved_expr)?;

                        // CRITICAL: After first head, register the CTE so recursive heads can reference it!
                        // This enables recursive CTEs where later heads reference the CTE being defined
                        if idx == 0 {
                            let mut base_schema = expr_schema.clone();
                            base_schema = transform_schema_table_names(base_schema, name);
                            fold.registry
                                .query_local
                                .register_cte(name.clone(), base_schema);
                        }

                        // Check if schemas are the same
                        if !schemas.is_empty() {
                            // Try strict validation to see if schemas match
                            if validate_union_compatible_schemas(&schemas[0], &expr_schema).is_err()
                            {
                                all_schemas_same = false;
                            }
                        }

                        schemas.push(expr_schema);
                        operands.push(resolved_expr);
                    }

                    // Choose operator based on whether schemas match
                    let (operator, final_schema) = if all_schemas_same {
                        // All schemas are the same - use positional union
                        (
                            ast_resolved::SetOperator::UnionAllPositional,
                            schemas[0].clone(),
                        )
                    } else {
                        // Different schemas - use UNION CORRESPONDING
                        let unified = build_corresponding_schema(&schemas)?;
                        (ast_resolved::SetOperator::UnionCorresponding, unified)
                    };

                    // Transform the schema to use the CTE's name as the table name
                    let mut final_schema = final_schema;
                    final_schema = transform_schema_table_names(final_schema, name);
                    // Register the CTE in the EntityRegistry
                    fold.registry
                        .query_local
                        .register_cte(name.clone(), final_schema.clone());

                    // Create SetOperation node (resolver can't set correlation)
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
                        is_recursive: ast_resolved::PhaseBox::phantom(),
                    });
                }
            }

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

            // Resolve the query with the registry that has CFE definitions
            let resolved_inner = match *query {
                ast_unresolved::Query::Relational(expr) => {
                    let (resolved_expr, _) = fold.resolve_relational(expr)?;
                    Box::new(ast_resolved::Query::Relational(resolved_expr))
                }
                other => {
                    // For non-relational queries, fall back to regular resolution
                    // (though they shouldn't appear inside WithPrecompiledCfes)
                    let inner_result = resolve_query(other, schema, system, config)?;
                    if let Some(conn_id) = inner_result.connection_id {
                        fold.registry.track_connection_id(conn_id);
                    }
                    Box::new(inner_result.query)
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

    // Validate that all resolved tables belong to the same connection
    let connection_id = fold.registry.validate_single_connection()?;

    Ok(ResolvedQueryResult {
        query: resolved_query,
        connection_id,
    })
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
            let (resolved_expr, bubbled) = resolve_relational_expression_with_registry(
                expr,
                registry,
                outer_context,
                config,
                grounding,
            )?;
            Ok((ast_resolved::Query::Relational(resolved_expr), bubbled))
        }
        ast_unresolved::Query::WithCtes {
            ctes,
            query: main_query,
        } => {
            // Group CTEs by name for merging (same logic as resolve_query)
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

            let mut resolved_ctes = Vec::new();

            for name in &cte_order {
                let group = cte_groups
                    .remove(name)
                    .expect("CTE should exist after ordering - invariant violation");
                if group.len() == 1 {
                    let cte = group
                        .into_iter()
                        .next()
                        .expect("Group has len==1, must have element - invariant");
                    let (resolved_expr, _) = resolve_relational_expression_with_registry(
                        cte.expression,
                        registry,
                        outer_context,
                        config,
                        grounding,
                    )?;
                    let mut cte_schema = extract_cpr_schema(&resolved_expr)?;
                    cte_schema = transform_schema_table_names(cte_schema, name);
                    registry.query_local.register_cte(name.clone(), cte_schema);

                    resolved_ctes.push(ast_resolved::CteBinding {
                        expression: resolved_expr,
                        name: name.clone(),
                        is_recursive: ast_resolved::PhaseBox::phantom(),
                    });
                } else {
                    // Multiple CTEs with same name — create UNION
                    let mut operands = Vec::new();
                    let mut schemas = Vec::new();
                    let mut all_schemas_same = true;

                    for (idx, cte) in group.iter().enumerate() {
                        let (resolved_expr, _) = resolve_relational_expression_with_registry(
                            cte.expression.clone(),
                            registry,
                            outer_context,
                            config,
                            grounding,
                        )?;
                        let expr_schema = extract_cpr_schema(&resolved_expr)?;

                        if idx == 0 {
                            let mut base_schema = expr_schema.clone();
                            base_schema = transform_schema_table_names(base_schema, name);
                            registry.query_local.register_cte(name.clone(), base_schema);
                        }

                        if !schemas.is_empty() {
                            if validate_union_compatible_schemas(&schemas[0], &expr_schema).is_err()
                            {
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
                    final_schema = transform_schema_table_names(final_schema, name);
                    registry
                        .query_local
                        .register_cte(name.clone(), final_schema.clone());

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
                        is_recursive: ast_resolved::PhaseBox::phantom(),
                    });
                }
            }

            // Resolve the main query with all CTEs registered
            let (resolved_main, bubbled) = resolve_relational_expression_with_registry(
                main_query,
                registry,
                outer_context,
                config,
                grounding,
            )?;

            Ok((
                ast_resolved::Query::WithCtes {
                    ctes: resolved_ctes,
                    query: resolved_main,
                },
                bubbled,
            ))
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

/// Walk the unresolved source expression tree and collect columns from all
/// EXISTS subquery table sources. This enriches the combined_context for
/// interdependent EXISTS subqueries so that cross-EXISTS column references
/// (e.g., `order_items.product_id` inside an EXISTS for `products`, where
/// `order_items` is a sibling EXISTS) can be validated.
fn collect_exists_table_columns(
    expr: &ast_unresolved::RelationalExpression,
    registry: &mut crate::resolution::EntityRegistry,
    context: &mut Vec<ast_resolved::ColumnMetadata>,
) -> Result<()> {
    match expr {
        ast_unresolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            // Recurse into the source to find deeper EXISTS
            collect_exists_table_columns(source, registry, context)?;

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
            collect_exists_table_columns(&pipe.source, registry, context)
        }
        // Join: EXISTS could be in Filter nodes inside either branch.
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            collect_exists_table_columns(left, registry, context)?;
            collect_exists_table_columns(right, registry, context)
        }
        // SetOperation: recurse into operands.
        ast_unresolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                collect_exists_table_columns(operand, registry, context)?;
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
    }
}

/// Extract the innermost source from a relational expression.
/// Traverses through Filter nodes to find the bottom source (usually a Relation).
fn extract_innermost_source(
    expr: &ast_unresolved::RelationalExpression,
) -> Option<&ast_unresolved::RelationalExpression> {
    match expr {
        ast_unresolved::RelationalExpression::Filter { source, .. } => {
            extract_innermost_source(source)
        }
        ast_unresolved::RelationalExpression::Relation(_) => Some(expr),
        _ => Some(expr),
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

// ============================================================================
// ER-Rule Expansion
// ============================================================================

/// Extract the table name from an unresolved Relation.
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
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    if relations.len() < 2 {
        return Err(DelightQLError::validation_error(
            "ER-join chain requires at least two relations",
            "Invalid ER-join chain",
        ));
    }

    // If no resolution_namespace is set, use enlisted-scope ER-rule lookup only.
    // ER-rules from non-enlisted namespaces are NOT visible at the call site —
    // the caller must enlist!() the namespace to access its ER-rules.
    // (When resolution_namespace IS set, lookup_er_rule_for_namespace handles scoping.)
    let effective_config: std::borrow::Cow<'_, ResolutionConfig>;
    if config.resolution_namespace.is_none() {
        let first_left = er_table_name(&relations[0])?;
        let first_right = er_table_name(&relations[1])?;
        let engaged_rule =
            registry
                .consult
                .lookup_er_rule(&context.context_name, &first_left, &first_right)?;
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
        let left_name = er_table_name(&relations[0])?;
        let right_name = er_table_name(&relations[1])?;

        return expand_single_er_pair(
            &left_name,
            &right_name,
            context,
            registry,
            outer_context,
            config,
            grounding,
        );
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
        let left_name = er_table_name(&relations[i])?;
        let right_name = er_table_name(&relations[i + 1])?;

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
        let (body_rels, body_conds) = flatten_unresolved_body(body_expr);

        // Add relations, deduplicating by table name
        for rel in body_rels {
            if let Ok(name) = er_table_name(&rel) {
                if seen_table_names.insert(name) {
                    all_relations.push(rel);
                }
                // If already seen, skip this relation (it's the shared intermediate)
            } else {
                // Non-Ground relation — keep it unconditionally
                all_relations.push(rel);
            }
        }

        // Keep all conditions (conditions from different pairs don't duplicate)
        all_conditions.extend(body_conds);
    }

    // Rebuild a single unresolved expression from the combined parts
    let combined_expr = rebuild_flat_expression(all_relations, all_conditions);

    // Add self-aliases and resolve through the pipeline (same path as single-pair)
    let combined_query =
        add_self_aliases_to_query(ast_unresolved::Query::Relational(combined_expr));

    // Determine effective grounding (same logic as expand_single_er_pair)
    // Use the first pair's rule to determine the namespace for grounding.
    let first_left = er_table_name(&relations[0])?;
    let first_right = er_table_name(&relations[1])?;
    let first_rule = if let Some(ns) = &config.resolution_namespace {
        registry.consult.lookup_er_rule_for_namespace(
            &context.context_name,
            &first_left,
            &first_right,
            ns,
        )?
    } else {
        registry
            .consult
            .lookup_er_rule(&context.context_name, &first_left, &first_right)?
    }
    .ok_or_else(|| {
        DelightQLError::validation_error(
            format!(
                "No ER-rule for ({}, {}) in context '{}'",
                first_left, first_right, context.context_name
            ),
            "Missing ER-rule",
        )
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
        DelightQLError::database_error(
            format!(
                "Error resolving ER-chain body in context '{}': {}",
                context.context_name, e
            ),
            e.to_string(),
        )
    })?;

    match resolved_query {
        ast_resolved::Query::Relational(expr) => Ok((expr, body_bubbled)),
        _ => Err(DelightQLError::validation_error(
            format!(
                "ER-chain body in context '{}' resolved to a non-relational query",
                context.context_name,
            ),
            "Invalid ER-chain body",
        )),
    }
}

/// Flatten an unresolved relational expression into a list of relations and conditions.
/// Walks the Join/Filter tree and collects all leaf Relation nodes and all Filter conditions.
fn flatten_unresolved_body(
    expr: ast_unresolved::RelationalExpression,
) -> (
    Vec<ast_unresolved::Relation>,
    Vec<ast_unresolved::SigmaCondition>,
) {
    let mut relations = Vec::new();
    let mut conditions = Vec::new();
    flatten_unresolved_body_inner(expr, &mut relations, &mut conditions);
    (relations, conditions)
}

fn flatten_unresolved_body_inner(
    expr: ast_unresolved::RelationalExpression,
    relations: &mut Vec<ast_unresolved::Relation>,
    conditions: &mut Vec<ast_unresolved::SigmaCondition>,
) {
    match expr {
        ast_unresolved::RelationalExpression::Relation(rel) => {
            relations.push(rel);
        }
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            flatten_unresolved_body_inner(*left, relations, conditions);
            flatten_unresolved_body_inner(*right, relations, conditions);
        }
        ast_unresolved::RelationalExpression::Filter {
            source, condition, ..
        } => {
            flatten_unresolved_body_inner(*source, relations, conditions);
            conditions.push(condition);
        }
        // Other variants shouldn't appear in ER-rule bodies
        other => {
            // Wrap as a single relation? No — ER-rule bodies are restricted to
            // joins + filters. If we get here, it's a validation failure that
            // should have been caught earlier. For robustness, treat as a relation
            // with a single synthetic wrapper — but this shouldn't happen in practice.
            log::warn!(
                "Unexpected expression variant in ER-rule body during flattening: {:?}",
                other
            );
        }
    }
}

/// Rebuild a flat unresolved expression from a list of relations and conditions.
/// Produces a left-deep Join tree of all relations, then wraps with Filter layers
/// for each condition.
fn rebuild_flat_expression(
    relations: Vec<ast_unresolved::Relation>,
    conditions: Vec<ast_unresolved::SigmaCondition>,
) -> ast_unresolved::RelationalExpression {
    // Build left-deep join tree from relations
    let mut iter = relations.into_iter();
    let mut expr = ast_unresolved::RelationalExpression::Relation(
        iter.next()
            .expect("ER chain must have at least one relation"),
    );
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

    expr
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
    .ok_or_else(|| {
        DelightQLError::validation_error(
            format!(
                "No ER-rule for ({}, {}) in context '{}'",
                left_name, right_name, context.context_name
            ),
            "Missing ER-rule",
        )
    })?;

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
    .ok_or_else(|| {
        DelightQLError::validation_error(
            format!(
                "No ER-rule for ({}, {}) in context '{}'",
                left_name, right_name, context.context_name
            ),
            "Missing ER-rule",
        )
    })?;
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
    context: &ast_unresolved::ErContextSpec,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    // Extract table names from endpoints
    let left_name = match &left {
        ast_unresolved::RelationalExpression::Relation(rel) => er_table_name(rel)?,
        _ => {
            return Err(DelightQLError::validation_error(
                "Left side of && must be a table reference",
                "Invalid ER-transitive-join operand",
            ))
        }
    };
    let right_name = match &right {
        ast_unresolved::RelationalExpression::Relation(rel) => er_table_name(rel)?,
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
            return Err(DelightQLError::validation_error(
                format!(
                    "No ER-rules found in context '{}'. \
                     Make sure you have enlisted the namespace that defines the rules, \
                     or use qualified access (ns.view(*)).",
                    context.context_name,
                ),
                "Empty ER-context",
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
        return Err(DelightQLError::validation_error(
            format!(
                "No ER-rules found in context '{}'. Define rules with `A&B(*) within {} :- ...`",
                context.context_name, context.context_name,
            ),
            "Empty ER-context",
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

    // BFS to find path from left_name to right_name
    let path = bfs_path(&adjacency, &left_name, &right_name)?;

    // Convert path to relations and expand as ErJoinChain
    let chain_relations: Vec<ast_unresolved::Relation> = path
        .iter()
        .map(|table_name| ast_unresolved::Relation::Ground {
            identifier: ast_unresolved::QualifiedName {
                namespace_path: ast_unresolved::NamespacePath::empty(),
                name: table_name.clone().into(),
                grounding: None,
            },
            canonical_name: ast_unresolved::PhaseBox::phantom(),
            domain_spec: ast_unresolved::DomainSpec::Glob,
            alias: None,
            outer: false,
            mutation_target: false,
            passthrough: false,
            cpr_schema: ast_unresolved::PhaseBox::phantom(),
            hygienic_injections: Vec::new(),
        })
        .collect();

    expand_er_join_chain(
        chain_relations,
        context,
        registry,
        outer_context,
        &effective_config,
        grounding,
    )
}

/// BFS path-finding in the ER graph. Returns the shortest path, or an error
/// if no path exists or multiple shortest paths exist (ambiguity).
fn bfs_path(adjacency: &HashMap<String, Vec<String>>, from: &str, to: &str) -> Result<Vec<String>> {
    use std::collections::VecDeque;

    if from == to {
        return Err(DelightQLError::validation_error(
            "ER-transitive join endpoints must be different tables",
            "Same-table transitive join",
        ));
    }

    let mut queue: VecDeque<Vec<String>> = VecDeque::new();
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut found_paths: Vec<Vec<String>> = Vec::new();
    let mut shortest_len: Option<usize> = None;

    queue.push_back(vec![from.to_string()]);
    visited.insert(from.to_string());

    while let Some(path) = queue.pop_front() {
        let current = path.last().unwrap();

        // If we've already found shortest paths and this path is longer, stop
        if let Some(len) = shortest_len {
            if path.len() > len {
                break;
            }
        }

        if let Some(neighbors) = adjacency.get(current.as_str()) {
            for neighbor in neighbors {
                let mut new_path = path.clone();
                new_path.push(neighbor.clone());

                if neighbor == to {
                    if shortest_len.is_none() {
                        shortest_len = Some(new_path.len());
                    }
                    found_paths.push(new_path);
                } else if !visited.contains(neighbor) {
                    visited.insert(neighbor.clone());
                    queue.push_back(new_path);
                }
            }
        }
    }

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
        ast_unresolved::RelationalExpression::Filter { source, .. } => {
            extract_grounding_from_source(source)
        }
        ast_unresolved::RelationalExpression::Pipe(pipe) => {
            extract_grounding_from_source(&pipe.source)
        }
        // Join/SetOp/ER sources don't have a single grounding — return None.
        ast_unresolved::RelationalExpression::Join { .. }
        | ast_unresolved::RelationalExpression::SetOperation { .. }
        | ast_unresolved::RelationalExpression::ErJoinChain { .. }
        | ast_unresolved::RelationalExpression::ErTransitiveJoin { .. } => None,
    }
}

/// Extract IN predicate values from an unresolved source relational expression.
/// Returns a mapping of column_name → literal values for each IN predicate found.
/// Used by pivot resolution to determine what columns to generate.
fn extract_in_predicate_values(
    source: &ast_unresolved::RelationalExpression,
) -> HashMap<String, Vec<String>> {
    let mut result = HashMap::new();
    scan_for_in_predicates(source, &mut result);
    result
}

#[stacksafe::stacksafe]
fn scan_for_in_predicates(
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
            scan_for_in_predicates(source, result);
        }
        ast_unresolved::RelationalExpression::Pipe(pipe) => {
            scan_for_in_predicates(&pipe.source, result);
        }
        ast_unresolved::RelationalExpression::Join { left, right, .. } => {
            scan_for_in_predicates(left, result);
            scan_for_in_predicates(right, result);
        }
        ast_unresolved::RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                scan_for_in_predicates(operand, result);
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
        ast_resolved::RelationalExpression::Join { left, .. } => {
            extract_base_ground_name(left)
        }
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
        other => panic!(
            "catch-all hit in mod.rs classify_single_dml_op (UnaryRelationalOperator): {:?}",
            other
        ),
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

