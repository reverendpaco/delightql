use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::NamespacePath;
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
use self::resolving::*;
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
use self::relation_resolver::*;
pub(crate) mod grounding;

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

// Re-export ColumnInfo and DatabaseSchema from delightql-types (Phase 2)
// Core no longer defines these - they live in the types crate to avoid circular dependencies
pub use delightql_types::schema::{ColumnInfo, DatabaseSchema};

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

    let resolved_query = match query {
        ast_unresolved::Query::Relational(expr) => {
            let (resolved_expr, _) = resolve_relational_expression_with_registry(
                expr,
                &mut registry,
                None,
                config,
                None,
            )?;
            ast_resolved::Query::Relational(resolved_expr)
        }
        ast_unresolved::Query::ReplTempTable { query, table_name } => {
            // Recursively resolve the nested query
            let inner_result = resolve_query(*query, schema, system, config)?;
            // Merge connection_ids from inner query
            if let Some(conn_id) = inner_result.connection_id {
                registry.track_connection_id(conn_id);
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
                registry.track_connection_id(conn_id);
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
                    let (resolved_expr, _) = resolve_relational_expression_with_registry(
                        cte.expression,
                        &mut registry,
                        None,
                        config,
                        None,
                    )?;
                    let mut cte_schema = extract_cpr_schema(&resolved_expr)?;
                    // Transform the schema to use the CTE's name as the table name
                    cte_schema = transform_schema_table_names(cte_schema, name);
                    // Register the CTE in the EntityRegistry
                    registry.query_local.register_cte(name.clone(), cte_schema);

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
                        let (resolved_expr, _) = resolve_relational_expression_with_registry(
                            cte.expression.clone(),
                            &mut registry,
                            None,
                            config,
                            None,
                        )?;
                        let expr_schema = extract_cpr_schema(&resolved_expr)?;

                        // CRITICAL: After first head, register the CTE so recursive heads can reference it!
                        // This enables recursive CTEs where later heads reference the CTE being defined
                        if idx == 0 {
                            let mut base_schema = expr_schema.clone();
                            base_schema = transform_schema_table_names(base_schema, name);
                            registry.query_local.register_cte(name.clone(), base_schema);
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
                    registry
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
            let (resolved_main_query, _) = resolve_relational_expression_with_registry(
                main_query,
                &mut registry,
                None,
                config,
                None,
            )?;

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
                registry.query_local.register_cfe(cfe.clone());
            }

            // Resolve the query with the registry that has CFE definitions
            let resolved_inner = match *query {
                ast_unresolved::Query::Relational(expr) => {
                    let (resolved_expr, _) = resolve_relational_expression_with_registry(
                        expr,
                        &mut registry,
                        None,
                        config,
                        None,
                    )?;
                    Box::new(ast_resolved::Query::Relational(resolved_expr))
                }
                other => {
                    // For non-relational queries, fall back to regular resolution
                    // (though they shouldn't appear inside WithPrecompiledCfes)
                    let inner_result = resolve_query(other, schema, system, config)?;
                    if let Some(conn_id) = inner_result.connection_id {
                        registry.track_connection_id(conn_id);
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
                registry.track_connection_id(conn_id);
            }
            inner_result.query
        }
    };

    // Validate that all resolved tables belong to the same connection
    let connection_id = registry.validate_single_connection()?;

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

/// New resolution function using EntityRegistry
#[stacksafe::stacksafe]
fn resolve_relational_expression_with_registry(
    expr: ast_unresolved::RelationalExpression,
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    match expr {
        // Handle Relations specially to use resolve_entity
        ast_unresolved::RelationalExpression::Relation(rel) => {
            resolve_relation_with_registry(rel, registry, outer_context, config, grounding)
        }

        // Handle Filter through registry (but check for EXISTS first)
        ast_unresolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema: _,
        } => {
            // Handle EXISTS filters through registry path
            // EXISTS filters need outer context passed to subquery for correlation
            let mut handle_exists_subquery = |subquery: ast_unresolved::RelationalExpression| -> Result<ast_resolved::RelationalExpression> {
                // Resolve the EXISTS subquery with current context for correlation
                let combined_context = if let Some(outer) = outer_context {
                    // Combine outer context with current source columns
                    let (resolved_source_temp, _source_bubbled_temp) =
                        resolve_relational_expression_with_registry(*source.clone(), registry, outer_context, config, grounding)?;
                    let source_schema_temp = extract_cpr_schema(&resolved_source_temp)?;
                    let source_columns_temp = match &source_schema_temp {
                        ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                        other => panic!("catch-all hit in mod.rs resolve_relational_expression (EXISTS outer+source schema): {:?}", other),
                    };
                    let mut combined = outer.to_vec();
                    combined.extend(source_columns_temp);
                    Some(combined)
                } else {
                    // Just use source columns for context
                    let (resolved_source_temp, _) =
                        resolve_relational_expression_with_registry(*source.clone(), registry, None, config, grounding)?;
                    let source_schema_temp = extract_cpr_schema(&resolved_source_temp)?;
                    match &source_schema_temp {
                        ast_resolved::CprSchema::Resolved(cols) => Some(cols.clone()),
                        other => panic!("catch-all hit in mod.rs resolve_relational_expression (EXISTS source schema): {:?}", other),
                    }
                };

                // For EXISTS subqueries, the combined context contains outer
                // source columns. Interdependent EXISTS (e.g.,
                // +orders(...), +order_items(...), +products(, order_items.x = products.y))
                // reference tables from sibling EXISTS scopes. Enrich the
                // context with columns from all EXISTS tables found in the
                // source expression so that cross-EXISTS references validate.
                let mut enriched_context = combined_context.unwrap_or_default();
                collect_exists_table_columns(&*source, registry, &mut enriched_context)?;

                let exists_config = ResolutionConfig {
                    validate_in_correlation: true,
                    ..config.clone()
                };

                let (resolved_subquery, _) = resolve_relational_expression_with_registry(
                    subquery,
                    registry,
                    Some(&enriched_context),
                    &exists_config,
                    grounding,
                )?;

                Ok(resolved_subquery)
            };

            // Check for EXISTS in the condition and handle through registry
            if let ast_unresolved::SigmaCondition::Predicate(pred) = &condition {
                if let ast_unresolved::BooleanExpression::InnerExists {
                    subquery,
                    exists,
                    identifier,
                    alias,
                    using_columns,
                } = pred
                {
                    // Handle EXISTS subquery through registry with proper context
                    let resolved_subquery = handle_exists_subquery(*subquery.clone())?;

                    // Continue with normal filter processing but with resolved EXISTS
                    let (resolved_source, source_bubbled) =
                        resolve_relational_expression_with_registry(
                            *source,
                            registry,
                            outer_context,
                            config,
                            grounding,
                        )?;

                    let source_schema = extract_cpr_schema(&resolved_source)?;
                    let available_columns = match &source_schema {
                        ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                        other => panic!("catch-all hit in mod.rs resolve_relational_expression (TVF source schema): {:?}", other),
                    };

                    let resolved_identifier = ast_resolved::QualifiedName {
                        namespace_path: identifier.namespace_path.clone(),
                        name: identifier.name.clone(),
                        grounding: None,
                    };

                    // Synthesize correlation predicates from USING columns
                    let final_subquery = resolving::synthesize_using_correlation(
                        resolved_subquery,
                        using_columns,
                        &resolved_identifier,
                        &available_columns,
                    );

                    // Create resolved EXISTS condition
                    let resolved_exists = ast_resolved::BooleanExpression::InnerExists {
                        exists: *exists,
                        identifier: resolved_identifier,
                        subquery: Box::new(final_subquery),
                        alias: alias.clone(),
                        using_columns: using_columns.clone(),
                    };
                    let resolved_condition =
                        ast_resolved::SigmaCondition::Predicate(resolved_exists);

                    return Ok((
                        ast_resolved::RelationalExpression::Filter {
                            source: Box::new(resolved_source),
                            condition: resolved_condition,
                            origin,
                            cpr_schema: ast_resolved::PhaseBox::new(source_schema),
                        },
                        source_bubbled,
                    ));
                }
            }

            let (resolved_source, source_bubbled) = resolve_relational_expression_with_registry(
                *source,
                registry,
                outer_context,
                config,
                grounding,
            )?;

            let source_schema = extract_cpr_schema(&resolved_source)?;

            // Get columns for condition resolution.
            // Prefer source_bubbled.i_provide — it carries the user alias (e.g., `as a`)
            // so qualified refs like `a.first_name` can match. The cpr_schema on the
            // AST node may have internal body names (e.g., from ConsultedView expansion)
            // that don't reflect the alias.
            let source_columns = if !source_bubbled.i_provide.is_empty() {
                source_bubbled.i_provide.clone()
            } else {
                match &source_schema {
                    ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                    ast_resolved::CprSchema::Failed {
                        resolved_columns, ..
                    } => resolved_columns.clone(),
                    ast_resolved::CprSchema::Unresolved(cols) => cols.clone(),
                    ast_resolved::CprSchema::Unknown => vec![],
                }
            };

            // Combine outer context with source columns for correlation support
            // This allows correlated predicates to reference both:
            // - Columns from the current source (e.g., orders.user_id)
            // - Columns from outer context (e.g., CFE parameters like buyer_id)
            let available_columns = if let Some(outer) = outer_context {
                let mut combined = outer.to_vec();
                combined.extend(source_columns);
                combined
            } else {
                source_columns
            };

            // Resolve condition using combined schema (source + outer context)
            // Use outer_context presence as heuristic for correlation contexts,
            // unless validate_in_correlation is set (EXISTS subqueries where
            // the full column set is known and validation is safe)
            let in_correlation = outer_context.is_some() && !config.validate_in_correlation;
            let resolved_condition = resolve_sigma_condition_with_registry(
                condition,
                &available_columns,
                registry,
                in_correlation,
                config,
            )?;

            // If this is a destructuring filter, add the destructured columns to the schema
            let final_schema = match &resolved_condition {
                ast_resolved::SigmaCondition::Destructure {
                    destructured_schema,
                    ..
                } => {
                    if std::env::var("DQL_DEBUG").is_ok() {
                        eprintln!("DESTRUCTURE FILTER DETECTED - adding columns to schema");
                    }
                    // Add destructured columns to source schema
                    let mut updated_columns = match &source_schema {
                        ast_resolved::CprSchema::Resolved(cols) => {
                            if std::env::var("DQL_DEBUG").is_ok() {
                                eprintln!("Source has {} columns:", cols.len());
                                for col in cols {
                                    eprintln!(
                                        "  - {}",
                                        col.info.original_name().unwrap_or("<no name>")
                                    );
                                }
                            }
                            cols.clone()
                        }
                        other => {
                            panic!("catch-all hit in mod.rs resolve_relational_expression (destructure filter schema): {:?}", other);
                        }
                    };
                    for mapping in destructured_schema.data() {
                        if std::env::var("DQL_DEBUG").is_ok() {
                            eprintln!("Adding destructured column: {}", mapping.column_name);
                        }
                        updated_columns.push(ast_resolved::ColumnMetadata {
                            info: ast_resolved::ColumnProvenance::from_column(
                                mapping.column_name.clone(),
                            ),
                            fq_table: ast_resolved::FqTable {
                                parents_path: NamespacePath::empty(),
                                name: ast_resolved::TableName::Fresh,
                                backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                            },
                            table_position: None,
                            has_user_name: true,
                            needs_hygienic_alias: false,
                            needs_sql_rename: false,
                            interior_schema: None,
                        });
                    }
                    if std::env::var("DQL_DEBUG").is_ok() {
                        eprintln!("Final schema has {} columns", updated_columns.len());
                    }
                    ast_resolved::CprSchema::Resolved(updated_columns)
                }
                _ => source_schema,
            };

            // Update bubbled state for destructuring filters
            let final_bubbled = match &resolved_condition {
                ast_resolved::SigmaCondition::Destructure {
                    destructured_schema,
                    ..
                } => {
                    // Add destructured columns to bubbled i_provide
                    let mut updated_bubbled = source_bubbled;
                    for mapping in destructured_schema.data() {
                        // Create ColumnMetadata for the destructured column
                        updated_bubbled
                            .i_provide
                            .push(ast_resolved::ColumnMetadata {
                                info: ast_resolved::ColumnProvenance::from_column(
                                    mapping.column_name.clone(),
                                ),
                                fq_table: ast_resolved::FqTable {
                                    parents_path: NamespacePath::empty(),
                                    name: ast_resolved::TableName::Fresh,
                                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(
                                        None,
                                    ),
                                },
                                table_position: None,
                                has_user_name: true,
                                needs_hygienic_alias: false,
                                needs_sql_rename: false,
                                interior_schema: None,
                            });
                    }
                    updated_bubbled
                }
                _ => source_bubbled,
            };

            Ok((
                ast_resolved::RelationalExpression::Filter {
                    source: Box::new(resolved_source),
                    condition: resolved_condition,
                    origin,
                    cpr_schema: ast_resolved::PhaseBox::new(final_schema),
                },
                final_bubbled,
            ))
        }

        // Handle Join through registry
        ast_unresolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema: _,
        } => {
            let (resolved_left, left_bubbled) = resolve_relational_expression_with_registry(
                *left,
                registry,
                outer_context,
                config,
                grounding,
            )?;

            let left_schema = extract_cpr_schema(&resolved_left)?;
            let left_columns = match &left_schema {
                ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
                other => panic!("catch-all hit in mod.rs resolve_relational_expression (join left_columns): {:?}", other),
            };

            // For EXISTS joins, we need to combine outer context with left columns
            let combined_context;
            let right_context = if let Some(outer) = outer_context {
                let mut combined = outer.to_vec();
                combined.extend(left_columns.clone());
                combined_context = combined;
                Some(combined_context.as_slice())
            } else {
                Some(left_columns.as_slice())
            };

            // Check if right side uses positional patterns and needs unification
            let (resolved_right, right_bubbled, positional_join_condition, where_constraints) =
                if let ast_unresolved::RelationalExpression::Relation(ref rel) = right.as_ref() {
                    match rel {
                        ast_unresolved::Relation::Ground {
                            identifier,
                            canonical_name: _,
                            alias,
                            domain_spec: ast_unresolved::DomainSpec::Positional(patterns),
                            outer,
                            mutation_target: _,
                            passthrough: _,
                            cpr_schema: _,
                            hygienic_injections: _,
                        } => {
                            // Use the SAME pattern resolver that single tables use!
                            let table_name = &identifier.name;
                            let schema = registry.database.schema();

                            // Get table schema — check CTEs first, then database
                            let maybe_table_columns = if let Some(cte_schema) =
                                registry.query_local.lookup_cte(table_name)
                            {
                                match cte_schema {
                                    ast_resolved::CprSchema::Resolved(cols) => Some(
                                        cols.iter()
                                            .enumerate()
                                            .map(|(idx, col)| ColumnInfo {
                                                name: col.name().into(),
                                                nullable: true,
                                                position: idx + 1,
                                            })
                                            .collect(),
                                    ),
                                    _ => {
                                        return Err(DelightQLError::TableNotFoundError {
                                            table_name: table_name.to_string(),
                                            context:
                                                "CTE schema not resolved for positional pattern"
                                                    .to_string(),
                                        });
                                    }
                                }
                            } else {
                                schema.get_table_columns(None, table_name)
                            };

                            if let Some(table_columns) = maybe_table_columns {
                                // CTE or database table — use existing mini-pipeline

                                // VALIDATE: Positional pattern length must match table columns
                                if patterns.len() != table_columns.len() {
                                    return Err(DelightQLError::validation_error(
                                    format!(
                                        "Positional pattern incomplete - table '{}' has {} columns but pattern specifies {} elements",
                                        table_name, table_columns.len(), patterns.len()
                                    ),
                                    "Pattern references unknown table".to_string()
                                ));
                                }

                                // Convert to ColumnMetadata for pattern resolver.
                                // Use alias as fq_table.name when present — this is the
                                // SQL-visible name, so qualified refs like `t.val` match.
                                let visible_name = alias.as_deref().unwrap_or(table_name);
                                let table_schema: Vec<ast_resolved::ColumnMetadata> = table_columns
                                    .iter()
                                    .enumerate()
                                    .map(|(idx, col)| {
                                        ast_resolved::ColumnMetadata::new(
                                            ast_resolved::ColumnProvenance::from_column(
                                                col.name.clone(),
                                            ),
                                            ast_resolved::FqTable {
                                                parents_path: NamespacePath::empty(),
                                                name: ast_resolved::TableName::Named(
                                                    visible_name.into(),
                                                ),
                                                backend_schema:
                                                    ast_resolved::PhaseBox::from_optional_schema(
                                                        None,
                                                    ),
                                            },
                                            Some(idx + 1),
                                        )
                                    })
                                    .collect();

                                // Create join context with left columns
                                let join_ctx = JoinContext {
                                    left_columns: left_columns.clone(),
                                };

                                // Use the SAME pattern resolver!
                                let pattern_resolver = PatternResolver::new();
                                let pattern_result = pattern_resolver.resolve_pattern(
                                    &ast_unresolved::DomainSpec::Positional(patterns.clone()),
                                    &table_schema,
                                    table_name,
                                    Some(&join_ctx),
                                )?;

                                // Build the resolved relation from pattern result
                                // Create positional domain spec with resolved columns as Lvar expressions
                                let resolved_exprs: Vec<ast_resolved::DomainExpression> =
                                    pattern_result
                                        .output_columns
                                        .iter()
                                        .map(|col| ast_resolved::DomainExpression::Lvar {
                                            name: col.name().into(),
                                            qualifier: Some(table_name.clone()),
                                            namespace_path: NamespacePath::empty(),
                                            alias: None,
                                            provenance: ast_resolved::PhaseBox::phantom(),
                                        })
                                        .collect();

                                let resolved_relation = ast_resolved::Relation::Ground {
                                    identifier: ast_resolved::QualifiedName {
                                        namespace_path: identifier.namespace_path.clone(),
                                        name: table_name.clone(),
                                        grounding: None,
                                    },
                                    canonical_name: ast_resolved::PhaseBox::new(None),
                                    domain_spec: ast_resolved::DomainSpec::Positional(
                                        resolved_exprs,
                                    ),
                                    alias: alias.clone(),
                                    outer: *outer,
                                    mutation_target: false,
                                    passthrough: false,
                                    cpr_schema: ast_resolved::PhaseBox::new(
                                        ast_resolved::CprSchema::Resolved(
                                            pattern_result.output_columns.clone(),
                                        ),
                                    ),
                                    hygienic_injections: Vec::new(),
                                };

                                let resolved_expr =
                                    ast_resolved::RelationalExpression::Relation(resolved_relation);

                                // Get bubbled state
                                let bubbled =
                                    BubbledState::resolved(pattern_result.output_columns.clone());

                                // Generate USING condition if there are unification columns
                                let join_cond =
                                    if let Some(using_cols) = pattern_result.using_columns {
                                        if !using_cols.is_empty() {
                                            Some(create_using_condition(using_cols)?)
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    };

                                // Return WHERE constraints to be handled at join level
                                (
                                    resolved_expr,
                                    bubbled,
                                    join_cond,
                                    pattern_result.where_constraints,
                                )
                            } else {
                                // Not CTE or database — likely a consulted entity.
                                // Route through the full resolver which handles consulted
                                // entities (views, facts) and applies positional patterns.
                                let right_expr =
                                    ast_unresolved::RelationalExpression::Relation(rel.clone());
                                let (resolved, bubbled) =
                                    resolve_relational_expression_with_registry(
                                        right_expr,
                                        registry,
                                        right_context,
                                        config,
                                        grounding,
                                    )?;

                                // Derive join conditions: check which lvar names in the
                                // positional pattern match left-side column names.
                                let mut using_cols: Vec<String> = Vec::new();
                                for pattern in patterns {
                                    if let ast_unresolved::DomainExpression::Lvar { name, .. } =
                                        pattern
                                    {
                                        let lvar_name = name.as_str();
                                        let matches_left =
                                            left_columns.iter().any(|col| col.name() == lvar_name);
                                        if matches_left
                                            && !using_cols.iter().any(|c| c == lvar_name)
                                        {
                                            using_cols.push(lvar_name.to_string());
                                        }
                                    }
                                }
                                let join_cond = if using_cols.is_empty() {
                                    None
                                } else {
                                    Some(create_using_condition(using_cols)?)
                                };

                                (resolved, bubbled, join_cond, vec![])
                            }
                        }
                        ast_unresolved::Relation::Anonymous { column_headers, .. } => {
                            // Handle anonymous table unification
                            let (resolved, bubbled) = resolve_relational_expression_with_registry(
                                *right.clone(),
                                registry,
                                right_context,
                                config,
                                grounding,
                            )?;

                            // Extract right-side columns from resolved anonymous table
                            let right_cpr_schema =
                                helpers::extraction::extract_cpr_schema(&resolved)?;
                            let right_columns = match right_cpr_schema {
                                ast_resolved::CprSchema::Resolved(cols) => cols,
                                other => panic!("catch-all hit in mod.rs resolve_relational_expression (anonymous table right_columns): {:?}", other),
                            };

                            // Check for unification opportunities based on column names
                            let anon_join_condition = if let Some(headers) = column_headers {
                                detect_anonymous_table_unification(
                                    headers,
                                    &left_columns,
                                    &right_columns,
                                )?
                            } else {
                                None
                            };

                            (resolved, bubbled, anon_join_condition, vec![])
                        }
                        ast_unresolved::Relation::Ground {
                            domain_spec: ast_unresolved::DomainSpec::GlobWithUsing(ref using_cols),
                            ..
                        } => {
                            // GlobWithUsing on consulted views (or any non-positional entity):
                            // resolve the entity, then create USING join condition from the
                            // specified columns.
                            let using_cols = using_cols.clone();
                            let (resolved, bubbled) = resolve_relational_expression_with_registry(
                                *right,
                                registry,
                                right_context,
                                config,
                                grounding,
                            )?;
                            let join_cond = if !using_cols.is_empty() {
                                Some(join_resolver::create_using_condition(using_cols)?)
                            } else {
                                None
                            };
                            (resolved, bubbled, join_cond, vec![])
                        }
                        _ => {
                            let (resolved, bubbled) = resolve_relational_expression_with_registry(
                                *right,
                                registry,
                                right_context,
                                config,
                                grounding,
                            )?;
                            (resolved, bubbled, None, vec![])
                        }
                    }
                } else {
                    let (resolved, bubbled) = resolve_relational_expression_with_registry(
                        *right,
                        registry,
                        right_context,
                        config,
                        grounding,
                    )?;
                    (resolved, bubbled, None, vec![])
                };

            // Join conditions need to be preserved and bubbled
            let mut join_bubbled = BubbledState::resolved(vec![]);
            let resolved_condition = if let Some(cond) = join_condition {
                match cond {
                    ast_unresolved::BooleanExpression::Using { columns } => {
                        // USING is structural, not a predicate — pass through directly
                        Some(ast_resolved::BooleanExpression::Using { columns })
                    }
                    _ => {
                        // For now, keep the condition as None but bubble the needs
                        // The condition will be resolved later when filters are processed
                        let schema = registry.database.schema();
                        let cte_context = &mut registry.query_local.ctes;
                        let (_unresolved_cond, cond_bubbled) = bubble_predicate_expression(
                            cond,
                            schema,
                            cte_context,
                            Some(&left_columns),
                        )?;
                        join_bubbled = cond_bubbled;
                        None // Will be attached later via filter-to-join transformation
                    }
                }
            } else {
                positional_join_condition
            };

            // Handle USING deduplication if present
            let using_columns = extract_inline_using_columns(&resolved_right).or_else(|| {
                // For positional patterns, extract USING columns from the join condition
                if let Some(ast_resolved::BooleanExpression::Using { columns }) =
                    &resolved_condition
                {
                    Some(
                        columns
                            .iter()
                            .map(|col| match col {
                                ast_resolved::UsingColumn::Regular(qname) => qname.name.to_string(),
                                ast_resolved::UsingColumn::Negated(qname) => qname.name.to_string(),
                            })
                            .collect(),
                    )
                } else {
                    None
                }
            });

            // Combine schemas with USING deduplication.
            // Use i_provide (which carries user aliases like "a", "s") rather than
            // extract_cpr_schema (which may have internal body names from ConsultedView).
            // This ensures the join's cpr_schema reflects the external interface.
            let combined_schema = {
                let left_cols = &left_bubbled.i_provide;
                let right_cols = &right_bubbled.i_provide;
                if left_cols.is_empty() && right_cols.is_empty() {
                    ast_resolved::CprSchema::Unknown
                } else {
                    let mut combined = left_cols.clone();
                    if let Some(using_cols) = &using_columns {
                        let using_names: std::collections::HashSet<String> =
                            using_cols.iter().cloned().collect();
                        let filtered_right: Vec<_> = right_cols
                            .iter()
                            .filter(|col| !using_names.contains(col.name()))
                            .cloned()
                            .collect();
                        combined.extend(filtered_right);
                    } else {
                        combined.extend(right_cols.clone());
                    }
                    ast_resolved::CprSchema::Resolved(combined)
                }
            };

            // Also deduplicate in the bubbled state
            let final_right_bubbled = if let Some(using_cols) = using_columns {
                let using_names: std::collections::HashSet<String> =
                    using_cols.into_iter().collect();
                let filtered_i_provide: Vec<_> = right_bubbled
                    .i_provide
                    .into_iter()
                    .filter(|col| !using_names.contains(col.name()))
                    .collect();
                BubbledState {
                    i_provide: filtered_i_provide,
                    i_need: right_bubbled.i_need,
                }
            } else {
                right_bubbled
            };

            // Create the join
            let mut result_expr = ast_resolved::RelationalExpression::Join {
                left: Box::new(resolved_left),
                right: Box::new(resolved_right),
                join_condition: resolved_condition,
                join_type,
                cpr_schema: ast_resolved::PhaseBox::new(combined_schema.clone()),
            };

            // Apply WHERE constraints from positional patterns if any
            if !where_constraints.is_empty() {
                // Combine multiple constraints with AND
                let combined_constraint = if where_constraints.len() == 1 {
                    where_constraints
                        .into_iter()
                        .next()
                        .expect("Checked len==1 above")
                } else {
                    where_constraints
                        .into_iter()
                        .reduce(|left, right| ast_resolved::BooleanExpression::And {
                            left: Box::new(left),
                            right: Box::new(right),
                        })
                        .expect("Checked non-empty above, reduce must succeed")
                };

                // Wrap the join in a Filter
                // Note: These are combined constraints from multiple tables in a join
                result_expr = ast_resolved::RelationalExpression::Filter {
                    source: Box::new(result_expr),
                    condition: ast_resolved::SigmaCondition::Predicate(combined_constraint),
                    origin: ast_resolved::FilterOrigin::PositionalLiteral {
                        source_table: "__join__".to_string(), // Special marker for combined join constraints
                    },
                    cpr_schema: ast_resolved::PhaseBox::new(combined_schema),
                };
            }

            Ok((
                result_expr,
                BubbledState::combine(
                    BubbledState::combine(left_bubbled, final_right_bubbled),
                    join_bubbled,
                ),
            ))
        }

        // Handle Pipe through registry — LINEARIZED
        // Collects the pipe chain into a flat list, resolves the base once,
        // then iterates operators bottom-up. Eliminates pipe-spine recursion.
        ast_unresolved::RelationalExpression::Pipe(boxed_pipe_expr) => {
            let pipe_expr = (*boxed_pipe_expr).into_inner();

            // Early intercept: piped HO view application desugars BEFORE source resolution
            if let ast_unresolved::UnaryRelationalOperator::HoViewApplication {
                ref function,
                ref arguments,
                ..
            } = pipe_expr.operator
            {
                return expand_piped_ho_view(
                    pipe_expr.source,
                    function,
                    arguments,
                    registry,
                    outer_context,
                    config,
                );
            }

            // Collect the pipe chain into a flat list, stopping at HoViewApplication
            // (which needs unresolved source for expansion and is handled recursively).
            let mut segments: Vec<ast_unresolved::UnaryRelationalOperator> = Vec::new();
            let mut current = ast_unresolved::RelationalExpression::Pipe(Box::new(
                stacksafe::StackSafe::new(pipe_expr),
            ));
            while let ast_unresolved::RelationalExpression::Pipe(pipe) = current {
                let pipe = (*pipe).into_inner();
                if matches!(
                    &pipe.operator,
                    ast_unresolved::UnaryRelationalOperator::HoViewApplication { .. }
                ) {
                    // Leave this Pipe (and everything below) as the base
                    // for recursive resolution via resolve_relational_expression_with_registry
                    current = ast_unresolved::RelationalExpression::Pipe(Box::new(
                        stacksafe::StackSafe::new(pipe),
                    ));
                    break;
                }
                segments.push(pipe.operator);
                current = pipe.source;
            }
            segments.reverse(); // source-code order: innermost first
            let base = current;

            // Companion query intercept: entity(^), entity(+), entity($)
            // If the first operator is a companion query and the base is a Ground
            // relation with companion data, resolve as inline VALUES table.
            let companion_intercept = if !segments.is_empty() {
                try_resolve_companion(&base, &segments[0], registry, grounding)?
            } else {
                None
            };

            let mut pivot_in_values;
            let source_grounding;
            let mutation_targets;
            let dml_pipe_ops: Vec<DmlPipeKind>;
            let mut resolved_source;
            let mut source_bubbled;

            if let Some((res, bub)) = companion_intercept {
                segments.remove(0); // Consume the companion operator
                resolved_source = res;
                source_bubbled = bub;
                pivot_in_values = HashMap::new();
                source_grounding = None;
                mutation_targets = vec![];
                dml_pipe_ops = vec![];
            } else {
                // Pre-processing extractions from the base (once, not per-pipe).
                // These functions walk through Pipes/Filters to find data at the Ground level.
                pivot_in_values = extract_in_predicate_values(&base);
                source_grounding = extract_grounding_from_source(&base);
                mutation_targets = find_mutation_targets(&base);

                // Pre-compute DML pipe ops for shape validation.
                // The DML terminal is always the last segment; classify all preceding
                // segments in outermost-first order (reversed from source-code order).
                dml_pipe_ops = if segments.last().map_or(false, |op| {
                    matches!(
                        op,
                        ast_unresolved::UnaryRelationalOperator::DmlTerminal { .. }
                    )
                }) {
                    segments[..segments.len() - 1]
                        .iter()
                        .rev()
                        .map(|op| classify_single_dml_op(op))
                        .collect()
                } else {
                    vec![]
                };

                // Resolve the base expression through registry.
                // If base is Pipe(HoView, ...), recursion handles the expansion.
                let (rs, sb) = resolve_relational_expression_with_registry(
                    base,
                    registry,
                    outer_context,
                    config,
                    grounding,
                )?;
                resolved_source = rs;
                source_bubbled = sb;

                // Extract IN values from the resolved base (catches InRelational
                // with anonymous fact tables, e.g., from HO scalar-lifted params).
                let resolved_pivot_values =
                    extract_in_predicate_values_from_resolved(&resolved_source);
                for (k, v) in resolved_pivot_values {
                    pivot_in_values.entry(k).or_insert(v);
                }
            }

            // Iterate pipe segments bottom-up (innermost operator first)
            for operator in segments {
                // Check for unresolved columns before pipe (scope barrier)
                if !source_bubbled.i_need.is_empty() {
                    let first_unresolved = &source_bubbled.i_need[0];
                    let qual_str = match first_unresolved {
                        ColumnReference::Named {
                            name, qualifier, ..
                        } => qualifier
                            .as_ref()
                            .map(|q| format!("{}.{}", q, name))
                            .unwrap_or_else(|| name.clone()),
                        ColumnReference::Ordinal {
                            position, reverse, ..
                        } => {
                            if *reverse {
                                format!("|-{}|", position)
                            } else {
                                format!("|{}|", position)
                            }
                        }
                    };

                    return Err(DelightQLError::ColumnNotFoundError {
                        column: qual_str,
                        context: "Column reference before pipe operator cannot be resolved (scope barrier)".to_string(),
                    });
                }

                // Get available columns from source
                let mut source_has_unknown_schema = false;
                let available_columns = if source_bubbled.i_provide.is_empty() {
                    let source_schema = extract_cpr_schema(&resolved_source)?;
                    if std::env::var("DQL_DEBUG").is_ok() {
                        eprintln!("PIPE: Extracted schema from source");
                    }
                    match &source_schema {
                        ast_resolved::CprSchema::Resolved(cols) => {
                            if std::env::var("DQL_DEBUG").is_ok() {
                                eprintln!("PIPE: Source has {} columns:", cols.len());
                                for col in cols {
                                    eprintln!(
                                        "  PIPE: - {}",
                                        col.info.original_name().unwrap_or("<no name>")
                                    );
                                }
                            }
                            cols.clone()
                        }
                        ast_resolved::CprSchema::Failed { .. } => {
                            return Err(DelightQLError::ParseError {
                                message: "Cannot pipe from a relation with unresolved columns"
                                    .to_string(),
                                source: None,
                                subcategory: None,
                            });
                        }
                        ast_resolved::CprSchema::Unresolved(_) => {
                            return Err(DelightQLError::ParseError {
                                message: "Cannot pipe from an unresolved relation".to_string(),
                                source: None,
                                subcategory: None,
                            });
                        }
                        ast_resolved::CprSchema::Unknown => {
                            source_has_unknown_schema = true;
                            vec![]
                        }
                    }
                } else {
                    source_bubbled.i_provide.clone()
                };

                // USING→correlation intercept
                if let ast_unresolved::UnaryRelationalOperator::Using { ref columns } = operator {
                    if let Some(outer) = outer_context {
                        let inner_table_name = extract_base_ground_name(&resolved_source);
                        let inner_qn = ast_resolved::QualifiedName {
                            namespace_path: ast_resolved::NamespacePath::empty(),
                            name: inner_table_name.unwrap_or_else(|| "unknown".into()),
                            grounding: None,
                        };

                        let correlation_filters =
                            resolving::build_using_correlation_filters(columns, &inner_qn, outer);

                        resolved_source =
                            insert_filters_at_base(resolved_source, correlation_filters);
                        continue;
                    }
                }

                // Validate !! mutation target markers for DML terminals
                if let ast_unresolved::UnaryRelationalOperator::DmlTerminal {
                    ref kind,
                    ref target,
                    ..
                } = operator
                {
                    use crate::pipeline::asts::core::operators::DmlKind;

                    if mutation_targets.len() > 1 {
                        return Err(DelightQLError::validation_error_categorized(
                            "dml/marker/multiple",
                            format!("DML source has !! on multiple relations: {}", mutation_targets.join(", ")),
                            "Only one relation can be marked with !! — the mutation target must be unambiguous",
                        ));
                    }

                    match kind {
                        DmlKind::Insert => {
                            if !mutation_targets.is_empty() {
                                return Err(DelightQLError::validation_error_categorized(
                                    "dml/marker/forbidden",
                                    format!("insert! source must not have !! marker (found on: {})", mutation_targets.join(", ")),
                                    "Remove !! from the source relation — insert reads from source, it does not mutate it".to_string(),
                                ));
                            }
                        }
                        DmlKind::Update | DmlKind::Delete | DmlKind::Keep => {
                            let kind_name = match kind {
                                DmlKind::Update => "update!",
                                DmlKind::Delete => "delete!",
                                DmlKind::Keep => "keep!",
                                _ => unreachable!(),
                            };
                            if mutation_targets.is_empty() {
                                return Err(DelightQLError::validation_error_categorized(
                                    "dml/marker/missing",
                                    format!("{} requires !! on the source relation that will be mutated", kind_name),
                                    format!("Mark the source with !!: {}!!(*)  — this makes the mutation target explicit", target),
                                ));
                            }
                            if !mutation_targets.iter().any(|t| t == target) {
                                return Err(DelightQLError::validation_error_categorized(
                                    "dml/marker/mismatch",
                                    format!("!! source table '{}' does not match {} target '{}'", mutation_targets[0], kind_name, target),
                                    format!("The !! marker must be on the same table as the DML target: {}!!(*)  |> {}({}(*))", target, kind_name, target),
                                ));
                            }
                        }
                    }

                    // Shape validation using pre-computed dml_pipe_ops
                    let pipe_ops = &dml_pipe_ops;

                    match kind {
                        DmlKind::Update => {
                            let has_transform = pipe_ops
                                .iter()
                                .any(|op| matches!(op, DmlPipeKind::Transform));
                            if !has_transform {
                                let has_non_filter_ops = pipe_ops.iter().any(|op| {
                                    matches!(
                                        op,
                                        DmlPipeKind::ProjectOut
                                            | DmlPipeKind::RenameCover
                                            | DmlPipeKind::TupleOrdering
                                            | DmlPipeKind::General
                                            | DmlPipeKind::Modulo
                                            | DmlPipeKind::AggregatePipe
                                    )
                                });
                                if has_non_filter_ops {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/shape/update_no_transform",
                                        "update! requires a Transform ($$) to specify column assignments — embed (+), project-out (-), rename (*), ordering (#), and projection do not produce SET clauses",
                                        "Use $$(new_value as column_name) before update! to specify what to change",
                                    ));
                                }
                            } else {
                                let has_aggregate = pipe_ops.iter().any(|op| {
                                    matches!(op, DmlPipeKind::Modulo | DmlPipeKind::AggregatePipe)
                                });
                                if has_aggregate {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/source/aggregate",
                                        "Cannot aggregate/group data before update! — aggregation changes the row identity, making it impossible to map results back to source rows",
                                        "Remove the aggregate/group-by pipe before the DML operation",
                                    ));
                                }
                                let transform_count = pipe_ops
                                    .iter()
                                    .filter(|op| matches!(op, DmlPipeKind::Transform))
                                    .count();
                                if transform_count > 1 {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/shape/update_no_transform",
                                        "update! requires exactly one Transform ($$) — multiple covers produce ambiguous SET clauses",
                                        "Combine the transforms into a single $$(expr1 as col1, expr2 as col2) before update!",
                                    ));
                                }
                                let has_ordering = pipe_ops
                                    .iter()
                                    .any(|op| matches!(op, DmlPipeKind::TupleOrdering));
                                if has_ordering {
                                    return Err(DelightQLError::validation_error_categorized(
                                        "dml/shape/update_no_transform",
                                        "Ordering (#) before update! is meaningless — UPDATE does not preserve row order",
                                        "Remove the ordering pipe from the DML pipeline",
                                    ));
                                }
                            }
                        }
                        DmlKind::Delete | DmlKind::Keep => {
                            let kind_name = if matches!(kind, DmlKind::Delete) {
                                "delete!"
                            } else {
                                "keep!"
                            };
                            let has_transform = pipe_ops
                                .iter()
                                .any(|op| matches!(op, DmlPipeKind::Transform));
                            if has_transform {
                                let sub = if matches!(kind, DmlKind::Delete) {
                                    "dml/shape/delete_with_cover"
                                } else {
                                    "dml/shape/keep_with_cover"
                                };
                                return Err(DelightQLError::validation_error_categorized(
                                    sub,
                                    format!("{} discards column data — a Transform ($$) before it is wasted", kind_name),
                                    format!("Remove the Transform before {} — only filters affect which rows are deleted/kept", kind_name),
                                ));
                            }
                            let has_shape_ops = pipe_ops.iter().any(|op| {
                                matches!(
                                    op,
                                    DmlPipeKind::ProjectOut
                                        | DmlPipeKind::RenameCover
                                        | DmlPipeKind::General
                                )
                            });
                            if has_shape_ops {
                                let sub = if matches!(kind, DmlKind::Delete) {
                                    "dml/shape/delete_with_cover"
                                } else {
                                    "dml/shape/keep_with_cover"
                                };
                                return Err(DelightQLError::validation_error_categorized(
                                    sub,
                                    format!("{} discards column data — shape-changing operators (embed, project-out, rename, projection) before it are wasted", kind_name),
                                    format!("Remove shape-changing pipes before {} — only filters affect which rows are deleted/kept", kind_name),
                                ));
                            }
                            let has_aggregate = pipe_ops.iter().any(|op| {
                                matches!(op, DmlPipeKind::Modulo | DmlPipeKind::AggregatePipe)
                            });
                            if has_aggregate {
                                return Err(DelightQLError::validation_error_categorized(
                                    "dml/source/aggregate",
                                    format!("Cannot aggregate/group data before {} — aggregation changes the row identity", kind_name),
                                    "Remove the aggregate/group-by pipe before the DML operation",
                                ));
                            }
                        }
                        DmlKind::Insert => {
                            // Insert is more permissive — projections, transforms, etc. are valid
                            // for shaping the data before insertion. Aggregates are suspicious but
                            // not necessarily wrong (e.g., insert aggregated results into a summary table).
                        }
                    }
                }

                // Bubble the operator to collect column needs
                let schema = registry.database.schema();
                let cte_context = &mut registry.query_local.ctes;
                let (unresolved_operator, operator_bubbled) =
                    bubbling::bubble_unary_operator(operator, schema, cte_context)?;

                // Validate that all operator needs can be satisfied
                if !operator_bubbled.i_need.is_empty() && !source_has_unknown_schema {
                    validate_and_get_resolved(
                        operator_bubbled.i_need.clone(),
                        &available_columns,
                        "in pipe operator",
                    )?;
                }

                // Inline consulted functions before resolution
                let unresolved_operator = if let Some(grounding) = grounding {
                    grounding::inline_consulted_functions_in_operator(
                        unresolved_operator,
                        grounding,
                        &registry.consult,
                    )?
                } else {
                    let source_data_ns = source_grounding.as_ref().map(|g| &g.data_ns);
                    grounding::inline_consulted_functions_in_operator_borrowed(
                        unresolved_operator,
                        &registry.consult,
                        source_data_ns,
                    )?
                };

                // Resolve the operator at the pipe boundary with the source schema
                let (resolved_operator, mut output_columns) =
                    resolving::resolve_operator_with_registry(
                        unresolved_operator,
                        &available_columns,
                        registry,
                        &pivot_in_values,
                    )?;

                // After a pipe, columns become Fresh (scope barrier).
                // Exception: value-level covers ($$ and $) preserve table provenance.
                let preserves_scope = matches!(
                    &resolved_operator,
                    ast_resolved::UnaryRelationalOperator::Transform { .. }
                        | ast_resolved::UnaryRelationalOperator::InteriorDrillDown { .. }
                );

                for (idx, col) in output_columns.iter_mut().enumerate() {
                    let previous_table = col.fq_table.name.clone();

                    if !preserves_scope {
                        col.fq_table.name = ast_resolved::TableName::Fresh;
                    }
                    col.table_position = Some(idx + 1);

                    col.info = col
                        .info
                        .clone()
                        .with_identity(ast_resolved::ColumnIdentity {
                            name: col.info.name().unwrap_or("<unnamed>").into(),
                            context: ast_resolved::IdentityContext::PipeBarrier {
                                previous_table,
                                fresh_scope: idx + 1,
                            },
                            phase: ast_resolved::TransformationPhase::Resolved,
                            table_qualifier: if preserves_scope {
                                col.fq_table.name.clone()
                            } else {
                                ast_resolved::TableName::Fresh
                            },
                        });

                    // Seal column identity: after a pipe barrier, the column's
                    // public name is its effective name. original_name() == name().
                    col.info = col.info.clone().promote_at_barrier();
                }

                // Construct resolved pipe, accumulate as new source
                let pipe = ast_resolved::PipeExpression {
                    source: resolved_source,
                    operator: resolved_operator,
                    cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                        output_columns.clone(),
                    )),
                };
                resolved_source = ast_resolved::RelationalExpression::Pipe(Box::new(
                    stacksafe::StackSafe::new(pipe),
                ));
                source_bubbled = BubbledState::resolved(output_columns);
            }

            Ok((resolved_source, source_bubbled))
        }

        // Handle SetOperation through registry
        ast_unresolved::RelationalExpression::SetOperation {
            operator,
            operands,
            correlation: unresolved_corr,
            cpr_schema: _,
        } => {
            // Resolve each operand
            let mut resolved_operands = Vec::new();
            let mut bubbled_states = Vec::new();

            for operand in operands {
                let (resolved, bubbled) = resolve_relational_expression_with_registry(
                    operand,
                    registry,
                    outer_context,
                    config,
                    grounding,
                )?;
                resolved_operands.push(resolved);
                bubbled_states.push(bubbled);
            }

            // Ensure all operands have compatible schemas
            if resolved_operands.is_empty() {
                return Err(DelightQLError::ParseError {
                    message: "SetOperation requires at least one operand".to_string(),
                    source: None,
                    subcategory: None,
                });
            }

            // Collect all schemas
            let mut schemas = Vec::new();
            for operand in &resolved_operands {
                schemas.push(extract_cpr_schema(operand)?);
            }

            // Validate and build final schema based on operator
            let final_schema = match operator {
                ast_unresolved::SetOperator::UnionAllPositional => {
                    // Positional union - use first operand's schema
                    schemas[0].clone()
                }
                ast_unresolved::SetOperator::SmartUnionAll => {
                    // Smart union - all must have same column names (order can differ)
                    for i in 1..schemas.len() {
                        validate_set_operation_schemas(&operator, &schemas[0], &schemas[i])?;
                    }
                    schemas[0].clone()
                }
                ast_unresolved::SetOperator::UnionCorresponding => {
                    // Build unified schema for CORRESPONDING
                    build_corresponding_schema(&schemas)?
                }
                ast_unresolved::SetOperator::MinusCorresponding => {
                    // Minus uses left operand's schema (rows in left not in right)
                    // Require same column names by name match
                    for i in 1..schemas.len() {
                        validate_set_operation_schemas(&operator, &schemas[0], &schemas[i])?;
                    }
                    schemas[0].clone()
                }
            };

            // Pass through correlation (resolver doesn't set it, refiner will)
            let resolved_correlation = unresolved_corr.into();

            Ok((
                ast_resolved::RelationalExpression::SetOperation {
                    operator,
                    operands: resolved_operands,
                    correlation: resolved_correlation,
                    cpr_schema: ast_resolved::PhaseBox::new(final_schema),
                },
                BubbledState::resolved(vec![]), // SetOperations don't bubble anything
            ))
        }

        ast_unresolved::RelationalExpression::ErJoinChain { relations } => {
            let context = config.er_context.as_ref().ok_or_else(|| {
                DelightQLError::validation_error(
                    "ER-join operator & requires an 'under context:' directive",
                    "Missing ER-context",
                )
            })?;

            Ok(expand_er_join_chain(
                relations,
                context,
                registry,
                outer_context,
                config,
                grounding,
            )?)
        }

        ast_unresolved::RelationalExpression::ErTransitiveJoin { left, right } => {
            let context = config.er_context.as_ref().ok_or_else(|| {
                DelightQLError::validation_error(
                    "ER-transitive-join operator && requires an 'under context:' directive",
                    "Missing ER-context",
                )
            })?;

            Ok(expand_er_transitive_join(
                *left,
                *right,
                context,
                registry,
                outer_context,
                config,
                grounding,
            )?)
        }
    }
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

/// Expand a piped HO view invocation: `source |> ho_view(args)(cols)`
///
/// Desugars the pipe into the HO view body with the source expression substituted
/// for the first table parameter, then resolves the result.
fn expand_piped_ho_view(
    source: ast_unresolved::RelationalExpression,
    function: &str,
    arguments: &[crate::pipeline::asts::core::operators::HoCallGroup],
    registry: &mut crate::resolution::EntityRegistry,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
    config: &ResolutionConfig,
) -> Result<(ast_resolved::RelationalExpression, BubbledState)> {
    // 1. Look up the HO view entity via engaged namespace
    let entity = registry
        .consult
        .lookup_enlisted_ho_view(function)?
        .ok_or_else(|| {
            DelightQLError::validation_error(
                format!(
                    "Unknown piped HO view '{}'. Ensure the namespace is consulted and engaged.",
                    function
                ),
                "Piped HO view not found",
            )
        })?;

    log::debug!(
        "Expanding piped HO view '{}' from namespace '{}' with {} extra args",
        function,
        entity.namespace,
        arguments.len()
    );

    // 2. Build bindings for the piped invocation.
    // The first param binds to a CTE wrapping the pipe source.
    // Remaining params bind from the call-site argument groups.
    let first_param = entity.params.first().ok_or_else(|| {
        DelightQLError::validation_error_categorized(
            "constraint/ho_param",
            format!(
                "HO view '{}' has no parameters but is used in piped invocation",
                function
            ),
            "HO view has no table parameter",
        )
    })?;

    let cte_name = "_ho_pipe_src".to_string();

    // Build remaining param bindings (params after the first)
    let remaining_params = &entity.params[1..];
    let mut bindings = if !remaining_params.is_empty() && !arguments.is_empty() {
        grounding::bind_ho_params_from_groups(remaining_params, arguments)?
    } else {
        grounding::HoParamBindings::default()
    };

    // Bind first param to the CTE name.
    // For Argumentative: build a Ground relation with positional columns (x, y)
    // For Glob/Scalar: simple table name substitution
    match &first_param.kind {
        crate::resolution::registry::HoParamKind::Argumentative(columns) => {
            // Build _ho_pipe_src(x, y) as a RelationalExpression
            let col_exprs: Vec<ast_unresolved::DomainExpression> = columns
                .iter()
                .map(|c| ast_unresolved::DomainExpression::lvar_builder(c.clone()).build())
                .collect();
            let cte_rel =
                ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Ground {
                    identifier: ast_unresolved::QualifiedName {
                        namespace_path: ast_unresolved::NamespacePath::empty(),
                        name: cte_name.clone().into(),
                        grounding: None,
                    },
                    canonical_name: ast_unresolved::PhaseBox::phantom(),
                    domain_spec: ast_unresolved::DomainSpec::Positional(col_exprs),
                    alias: None,
                    outer: false,
                    mutation_target: false,
                    passthrough: false,
                    cpr_schema: ast_unresolved::PhaseBox::phantom(),
                    hygienic_injections: Vec::new(),
                });
            bindings
                .table_expr_params
                .insert(first_param.name.clone(), cte_rel);
        }
        crate::resolution::registry::HoParamKind::Glob
        | crate::resolution::registry::HoParamKind::Scalar => {
            bindings
                .table_params
                .insert(first_param.name.clone(), cte_name.clone());
        }
    }

    // Validate arity for argumentative params that received table references.
    grounding::validate_argumentative_arity(&bindings, registry)?;

    // 3. Parse the body with builder-integrated HO param substitution.
    let body_query_raw =
        crate::ddl::body_parser::parse_view_body_with_bindings(&entity.definition, bindings)?;

    // Wrap in WithCtes: the CTE binds the pipe source expression
    let cte_binding = ast_unresolved::CteBinding {
        expression: source,
        name: cte_name,
        is_recursive: ast_unresolved::PhaseBox::phantom(),
    };
    let body_query = match body_query_raw {
        ast_unresolved::Query::Relational(expr) => ast_unresolved::Query::WithCtes {
            ctes: vec![cte_binding],
            query: expr,
        },
        ast_unresolved::Query::WithCtes { mut ctes, query } => {
            ctes.insert(0, cte_binding);
            ast_unresolved::Query::WithCtes { ctes, query }
        }
        other => {
            return Err(DelightQLError::parse_error(format!(
                "Unexpected query structure in piped HO view '{}' body: {:?}",
                function,
                std::mem::discriminant(&other)
            )));
        }
    };

    // 5. Build grounding context for consulted function inlining during body resolution.
    //    The pipe source already carries its namespace (e.g., data::test.users), so we
    //    skip patch_data_ns. The grounding only needs grounded_ns for function lookup.
    let ns_parts: Vec<String> = entity.namespace.split("::").map(String::from).collect();
    let entity_ns = ast_unresolved::NamespacePath::from_parts(ns_parts).map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Invalid namespace '{}' for HO view '{}': {:?}",
                entity.namespace, function, e
            ),
            format!("{:?}", e),
        )
    })?;
    let ho_grounding = ast_unresolved::GroundedPath {
        data_ns: ast_unresolved::NamespacePath::empty(),
        grounded_ns: vec![entity_ns],
    };

    // 6. Resolve the full query (handles CTEs) with grounding context
    let (resolved_query, bubbled) = resolve_query_inline(
        body_query,
        registry,
        outer_context,
        config,
        Some(&ho_grounding),
    )?;

    relation_resolver::ho_view_query_to_relational(resolved_query, bubbled, function)
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
fn extract_base_ground_name(
    expr: &ast_resolved::RelationalExpression,
) -> Option<delightql_types::SqlIdentifier> {
    match expr {
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Ground {
            identifier,
            ..
        }) => Some(identifier.name.clone()),
        ast_resolved::RelationalExpression::Pipe(pipe) => extract_base_ground_name(&pipe.source),
        ast_resolved::RelationalExpression::Filter { source, .. } => {
            extract_base_ground_name(source)
        }
        other => panic!(
            "catch-all hit in mod.rs extract_base_ground_name: {:?}",
            other
        ),
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

/// Try to resolve a companion query (entity(^), entity(+), entity($)).
/// Returns Some((resolved_source, bubbled_state)) if the base is a companion entity
/// and the operator is MetaIze(^) or CompanionAccess(+/$). Returns None to fall through.
fn try_resolve_companion(
    base: &ast_unresolved::RelationalExpression,
    operator: &ast_unresolved::UnaryRelationalOperator,
    registry: &mut crate::resolution::EntityRegistry,
    grounding: Option<&ast_unresolved::GroundedPath>,
) -> Result<Option<(ast_resolved::RelationalExpression, BubbledState)>> {
    use crate::pipeline::asts::ddl::CompanionKind;

    // Determine companion kind from operator
    let kind = match operator {
        ast_unresolved::UnaryRelationalOperator::MetaIze { detailed: false } => {
            CompanionKind::Schema
        }
        ast_unresolved::UnaryRelationalOperator::CompanionAccess { kind } => *kind,
        _ => return Ok(None),
    };

    // Extract entity name and namespace from base Ground relation
    let identifier = match base {
        ast_unresolved::RelationalExpression::Relation(ast_unresolved::Relation::Ground {
            identifier,
            ..
        }) => identifier,
        _ => return Ok(None),
    };

    // Look up entity — try identifier grounding, then ambient grounding, then namespace path
    let companion_data = if let Some(grounding) = &identifier.grounding {
        let mut found = None;
        for ns in &grounding.grounded_ns {
            let fq = grounding::namespace_path_to_fq(ns);
            if let Some(data) = registry
                .consult
                .lookup_companion_data(&identifier.name, &fq, kind)
            {
                found = Some(data);
                break;
            }
        }
        found
    } else if let Some(grounding) = grounding {
        let mut found = None;
        for ns in &grounding.grounded_ns {
            let fq = grounding::namespace_path_to_fq(ns);
            if let Some(data) = registry
                .consult
                .lookup_companion_data(&identifier.name, &fq, kind)
            {
                found = Some(data);
                break;
            }
        }
        found
    } else if !identifier.namespace_path.is_empty() {
        let fq = identifier
            .namespace_path
            .iter()
            .map(|i| i.name.as_str())
            .collect::<Vec<_>>()
            .join("::");
        registry
            .consult
            .lookup_companion_data(&identifier.name, &fq, kind)
    } else {
        // Unqualified — try main namespace
        registry
            .consult
            .lookup_companion_data(&identifier.name, "main", kind)
    };

    let (_entity_id, rows) = match companion_data {
        Some(data) => data,
        None => {
            // For CompanionAccess (+ / $), error if entity doesn't have companion data
            if matches!(
                operator,
                ast_unresolved::UnaryRelationalOperator::CompanionAccess { .. }
            ) {
                return Err(DelightQLError::database_error(
                    format!(
                        "No companion {} data found for entity '{}'",
                        match kind {
                            CompanionKind::Schema => "schema",
                            CompanionKind::Constraint => "constraint",
                            CompanionKind::Default => "default",
                        },
                        identifier.name
                    ),
                    "Entity may not be a companion or may not have this companion type defined"
                        .to_string(),
                ));
            }
            // MetaIze on non-companion: fall through to normal meta-ize
            return Ok(None);
        }
    };

    // Build column names and schema for the companion kind
    let (col_names, schema_columns) = companion_output_schema(kind);

    // Construct resolved Anonymous relation with VALUES
    let column_headers: Vec<ast_resolved::DomainExpression> = col_names
        .iter()
        .map(|name| ast_resolved::DomainExpression::Lvar {
            name: (*name).into(),
            qualifier: None,
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: None,
            provenance: ast_resolved::PhaseBox::phantom(),
        })
        .collect();

    let value_rows: Vec<ast_resolved::Row> = rows
        .iter()
        .map(|row| ast_resolved::Row {
            values: row
                .iter()
                .map(|val| match val {
                    Some(s) => ast_resolved::DomainExpression::Literal {
                        value: ast_resolved::LiteralValue::String(s.clone()),
                        alias: None,
                    },
                    None => ast_resolved::DomainExpression::Literal {
                        value: ast_resolved::LiteralValue::Null,
                        alias: None,
                    },
                })
                .collect(),
        })
        .collect();

    let resolved =
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Anonymous {
            column_headers: Some(column_headers),
            rows: value_rows,
            alias: Some("_companion".into()),
            outer: false,
            exists_mode: false,
            qua_target: None,
            cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Resolved(
                schema_columns.clone(),
            )),
        });

    let bubbled = BubbledState::resolved(schema_columns);
    Ok(Some((resolved, bubbled)))
}

/// Output schema for each companion kind
fn companion_output_schema(
    kind: crate::pipeline::asts::ddl::CompanionKind,
) -> (Vec<&'static str>, Vec<ast_resolved::ColumnMetadata>) {
    use crate::pipeline::asts::ddl::CompanionKind;

    let make_col = |name: &str, pos: usize| -> ast_resolved::ColumnMetadata {
        ast_resolved::ColumnMetadata::new(
            ast_resolved::ColumnProvenance::from_column(name.to_string()),
            ast_resolved::FqTable {
                parents_path: ast_resolved::NamespacePath::empty(),
                name: ast_resolved::TableName::Named("_companion".into()),
                backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
            },
            Some(pos),
        )
    };

    match kind {
        CompanionKind::Schema => (
            vec!["name", "type", "position"],
            vec![
                make_col("name", 1),
                make_col("type", 2),
                make_col("position", 3),
            ],
        ),
        CompanionKind::Constraint => (
            vec!["column", "constraint", "constraint_name"],
            vec![
                make_col("column", 1),
                make_col("constraint", 2),
                make_col("constraint_name", 3),
            ],
        ),
        CompanionKind::Default => (
            vec!["column", "default", "generated"],
            vec![
                make_col("column", 1),
                make_col("default", 2),
                make_col("generated", 3),
            ],
        ),
    }
}
