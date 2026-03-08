// Query transformation orchestration for transformer_v3
//
// This module contains the main entry points and orchestration logic for transforming
// DelightQL AST nodes into SQL statements. It coordinates between different transformation
// stages and manages the overall transformation pipeline.

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::{
    Cte, QueryExpression, SelectBuilder, SelectItem, SetOperator, SqlStatement, TableExpression,
};

use super::context::TransformContext;
use super::cpr_laws::detect_cpr_laws;
use super::cte_extractor::extract_ctes;
use super::cte_handling::collect_cfes;
use super::finalization::hide_hygienic_columns_from_output;
use super::helpers::alias_generator::next_alias;
use super::pipe_operators::apply_pipe_operator_unified;
use super::predicate_utils::extract_aliases_from_predicate;
use super::query_wrapper::update_query_provenance;
use super::relation_transformer::transform_relation;
use super::schema_utils::get_relational_schema;
use super::segment_handler::finalize_to_query;
use super::set_operations::{
    transform_min_multiplicity_intersection, transform_set_operation_with_correlation,
    wrap_with_explicit_columns_unified, wrap_with_null_padding,
};
use super::types::QueryBuildState;

// Import join and filter transformers
use super::filter_transformer::transform_filter;
use super::join_handler::transform_join;

/// Transform a complete Query (with CTEs or REPL commands) to SQL
pub fn transform_query(query: ast_addressed::Query, dialect: SqlDialect) -> Result<SqlStatement> {
    transform_query_with_options(query, true, dialect, None, None, None)
}

/// Transform a complete Query with options for CTE handling
pub fn transform_query_with_options(
    query: ast_addressed::Query,
    force_ctes: bool,
    dialect: SqlDialect,
    bin_registry: Option<std::sync::Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>>,
    danger_gates: Option<crate::pipeline::danger_gates::DangerGateMap>,
    option_map: Option<crate::pipeline::option_map::OptionMap>,
) -> Result<SqlStatement> {
    log::debug!("=== TRANSFORMER V3 QUERY ENTRY ===");
    log::debug!("Query type: {:?}", std::mem::discriminant(&query));
    log::debug!("force_ctes: {}", force_ctes);

    // Collect all CFE definitions from the query tree
    let all_cfes = collect_cfes(&query);
    log::debug!("Collected {} CFE definitions total", all_cfes.len());

    // Build danger gate map (defaults + per-query overrides)
    let danger_gate_map = std::sync::Arc::new(
        danger_gates.unwrap_or_else(crate::pipeline::danger_gates::DangerGateMap::with_defaults),
    );

    // Build option map (defaults + per-query overrides)
    let option_map = std::sync::Arc::new(
        option_map.unwrap_or_else(crate::pipeline::option_map::OptionMap::with_defaults),
    );

    // Helper: apply danger gates and option map to a fresh context
    let apply_gates = |ctx: TransformContext| -> TransformContext {
        TransformContext {
            danger_gates: danger_gate_map.clone(),
            option_map: option_map.clone(),
            ..ctx
        }
    };

    match query {
        ast_addressed::Query::Relational(expr) => {
            log::debug!("Query contains Relational expression");
            // For simple relational expressions, use the full transform() pipeline
            // which includes hygiene handling and CTE extraction
            // CFEs will be passed through context
            if all_cfes.is_empty() {
                // No CFEs - use standard transform (but still need bin_registry!)
                let ctx = apply_gates(TransformContext::new(dialect).with_bin_registry(
                    bin_registry.clone().unwrap_or_else(|| {
                        std::sync::Arc::new(
                            crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                        )
                    }),
                ));
                let state = transform_relational(expr, &ctx)?;

                // DML statements bypass query finalization
                if let QueryBuildState::DmlStatement(dml_stmt) = state {
                    return Ok(dml_stmt);
                }

                let query = finalize_to_query(state)?;
                let query = hide_hygienic_columns_from_output(query)?;
                let mut statement = extract_ctes(query)?;

                // EPOCH 7: Add generated CTEs (e.g., premelt CTEs from melt patterns)
                let generated_ctes = ctx.generated_ctes.borrow().clone();
                if !generated_ctes.is_empty() {
                    statement = match statement {
                        SqlStatement::Query { with_clause, query } => {
                            let combined_ctes = match with_clause {
                                Some(mut existing) => {
                                    existing.extend(generated_ctes);
                                    Some(existing)
                                }
                                None => Some(generated_ctes),
                            };
                            SqlStatement::Query {
                                with_clause: combined_ctes,
                                query,
                            }
                        }
                        other => other, // Don't modify other statement types
                    };
                }
                Ok(statement)
            } else {
                // Has CFEs - need to pass them through context
                // Do the full transform pipeline manually with CFE context
                let ctx = apply_gates(
                    TransformContext::new(dialect)
                        .with_cfe_definitions(all_cfes.clone())
                        .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                            std::sync::Arc::new(
                                crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                            )
                        })),
                );
                let state = transform_relational(expr, &ctx)?;

                // DML statements bypass query finalization
                if let QueryBuildState::DmlStatement(dml_stmt) = state {
                    return Ok(dml_stmt);
                }

                let query = finalize_to_query(state)?;

                // Apply post-processing steps (same as transform() function)
                let query = hide_hygienic_columns_from_output(query)?;
                let mut statement = extract_ctes(query)?;

                // EPOCH 7: Add generated CTEs (e.g., premelt CTEs from melt patterns)
                let generated_ctes = ctx.generated_ctes.borrow().clone();
                if !generated_ctes.is_empty() {
                    statement = match statement {
                        SqlStatement::Query { with_clause, query } => {
                            let combined_ctes = match with_clause {
                                Some(mut existing) => {
                                    existing.extend(generated_ctes);
                                    Some(existing)
                                }
                                None => Some(generated_ctes),
                            };
                            SqlStatement::Query {
                                with_clause: combined_ctes,
                                query,
                            }
                        }
                        other => other, // Don't modify other statement types
                    };
                }
                Ok(statement)
            }
        }
        ast_addressed::Query::WithCtes {
            ctes,
            query: main_query,
        } => {
            if force_ctes {
                // Original behavior: emit WITH clauses
                let sql_ctes = ctes
                    .into_iter()
                    .map(|cte| {
                        log::debug!(
                            "Transforming CTE '{}', expression type: {:?}",
                            cte.name,
                            std::mem::discriminant(&cte.expression)
                        );
                        let cte_name = cte.name;
                        let cte_is_recursive = cte.is_recursive.get();
                        let ctx = apply_gates(
                            TransformContext::new(dialect)
                                .with_cfe_definitions(all_cfes.clone())
                                .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                                    std::sync::Arc::new(
                                        crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                                    )
                                })),
                        );
                        let state = transform_relational(cte.expression, &ctx)?;
                        let mut sql_query = finalize_to_query(state)?;

                        // Check if this CTE generated intermediate CTEs (e.g., tree groups)
                        let generated_ctes = ctx.generated_ctes.borrow().clone();
                        if !generated_ctes.is_empty() {
                            // Wrap the query with nested WITH clause
                            sql_query = QueryExpression::WithCte {
                                ctes: generated_ctes,
                                query: Box::new(sql_query),
                            };
                        }

                        let result_cte = if cte_is_recursive {
                            Cte::new_recursive(cte_name, sql_query)
                        } else {
                            Cte::new(cte_name, sql_query)
                        };
                        Ok(result_cte)
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Transform main query
                let ctx = apply_gates(
                    TransformContext::new(dialect)
                        .with_cfe_definitions(all_cfes.clone())
                        .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                            std::sync::Arc::new(
                                crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                            )
                        })),
                );
                let state = transform_relational(main_query, &ctx)?;

                // Check if main query generated intermediate CTEs (e.g., premelt for melt patterns)
                let generated_ctes = ctx.generated_ctes.borrow().clone();
                let combined_ctes = if !generated_ctes.is_empty() {
                    let mut all_ctes = sql_ctes;
                    all_ctes.extend(generated_ctes);
                    all_ctes
                } else {
                    sql_ctes
                };

                // DML statements (delete!/keep!/insert!/update!) are already
                // SqlStatements — attach CTEs directly instead of finalizing.
                if let QueryBuildState::DmlStatement(mut dml_stmt) = state {
                    match &mut dml_stmt {
                        SqlStatement::Delete { with_clause, .. }
                        | SqlStatement::Update { with_clause, .. }
                        | SqlStatement::Insert { with_clause, .. } => {
                            *with_clause = Some(combined_ctes);
                        }
                        _ => {}
                    }
                    return Ok(dml_stmt);
                }

                let main_sql = finalize_to_query(state)?;
                Ok(SqlStatement::with_ctes(Some(combined_ctes), main_sql))
            } else {
                // New behavior: inline CTEs as subqueries
                log::debug!("Inlining {} CTEs as subqueries", ctes.len());

                // Transform CTEs into QueryExpressions and store in map
                let mut cte_map = std::collections::HashMap::new();
                for cte in ctes {
                    log::debug!("Transforming CTE '{}' for inlining", cte.name);
                    let ctx = apply_gates(
                        TransformContext::with_force_ctes(false, dialect)
                            .with_cfe_definitions(all_cfes.clone())
                            .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                                std::sync::Arc::new(
                                    crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                                )
                            })),
                    );
                    let state = transform_relational(cte.expression, &ctx)?;
                    let query_expr = finalize_to_query(state)?;

                    cte_map.insert(cte.name, query_expr);
                }

                // Transform main query with CTE definitions available for inlining
                let ctx = apply_gates(
                    TransformContext::with_force_ctes(false, dialect)
                        .with_cte_definitions(cte_map)
                        .with_cfe_definitions(all_cfes.clone())
                        .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                            std::sync::Arc::new(
                                crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                            )
                        })),
                );
                let state = transform_relational(main_query, &ctx)?;

                // DML statements are already SqlStatements — return directly
                // (inlined CTEs are already resolved in the subqueries)
                if let QueryBuildState::DmlStatement(dml_stmt) = state {
                    return Ok(dml_stmt);
                }

                let main_query_expr = finalize_to_query(state)?;

                // Wrap in SqlStatement - no WITH clause since CTEs are inlined
                Ok(SqlStatement::Query {
                    with_clause: None,
                    query: main_query_expr,
                })
            }
        }
        ast_addressed::Query::ReplTempTable { query, table_name } => {
            // Recursively transform the nested query
            let inner_sql = transform_query(*query, dialect)?;

            // For CREATE TEMPORARY TABLE, we keep the CTEs as part of the statement
            // SQLite supports: CREATE TEMPORARY TABLE ... AS WITH ... SELECT ...
            match inner_sql {
                SqlStatement::Query { with_clause, query } => Ok(SqlStatement::create_temp_table(
                    table_name,
                    with_clause,
                    query,
                )),
                _ => Err(crate::error::DelightQLError::ParseError {
                    message: "Nested REPL command not supported".to_string(),
                    source: None,
                    subcategory: None,
                }),
            }
        }
        ast_addressed::Query::ReplTempView { query, view_name } => {
            // Recursively transform the nested query
            let inner_sql = transform_query(*query, dialect)?;

            // For CREATE TEMPORARY VIEW, we keep the CTEs as part of the statement
            // SQLite supports: CREATE TEMPORARY VIEW ... AS WITH ... SELECT ...
            match inner_sql {
                SqlStatement::Query { with_clause, query } => Ok(SqlStatement::create_temp_view(
                    view_name,
                    with_clause,
                    query,
                )),
                _ => Err(crate::error::DelightQLError::ParseError {
                    message: "Nested REPL command not supported".to_string(),
                    source: None,
                    subcategory: None,
                }),
            }
        }
        ast_addressed::Query::WithCfes { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "CFE queries must be precompiled before transformation".to_string(),
            source: None,
            subcategory: None,
        }),
        ast_addressed::Query::WithErContext { .. } => {
            unreachable!("ER-context consumed by resolver")
        }
        ast_addressed::Query::WithPrecompiledCfes { cfes, query } => {
            // CFEs have been collected at top level in all_cfes, so just unwrap and process the inner query
            // Don't recursively call transform_query_with_options - that would lose the CFEs!
            log::debug!(
                "Query contains {} precompiled CFEs at this level",
                cfes.len()
            );

            // Unwrap the inner query and process it with CFEs in context
            match *query {
                ast_addressed::Query::Relational(expr) => {
                    log::debug!("Inner query is Relational");
                    // Do the full transform pipeline with CFE context
                    let ctx = apply_gates(
                        TransformContext::new(dialect)
                            .with_cfe_definitions(all_cfes.clone())
                            .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                                std::sync::Arc::new(
                                    crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                                )
                            })),
                    );
                    let state = transform_relational(expr, &ctx)?;

                    if let QueryBuildState::DmlStatement(dml_stmt) = state {
                        return Ok(dml_stmt);
                    }

                    let query = finalize_to_query(state)?;

                    // Apply post-processing steps (same as transform() function)
                    let query = hide_hygienic_columns_from_output(query)?;
                    let statement = extract_ctes(query)?;
                    Ok(statement)
                }
                ast_addressed::Query::WithCtes {
                    ctes,
                    query: main_query,
                } => {
                    // CTEs with CFEs
                    log::debug!("Inner query has CTEs");
                    if force_ctes {
                        let sql_ctes = ctes
                            .into_iter()
                            .map(|cte| {
                                let cte_name = cte.name;
                                let cte_is_recursive = cte.is_recursive.get();
                                let ctx = apply_gates(TransformContext::new(dialect)
                                    .with_cfe_definitions(all_cfes.clone()).with_bin_registry(bin_registry.clone().unwrap_or_else(|| std::sync::Arc::new(crate::bin_cartridge::registry::BinCartridgeRegistry::new()))));
                                let state = transform_relational(cte.expression, &ctx)?;
                                let sql_query = finalize_to_query(state)?;

                                let result_cte = if cte_is_recursive {
                                    Cte::new_recursive(cte_name, sql_query)
                                } else {
                                    Cte::new(cte_name, sql_query)
                                };
                                Ok(result_cte)
                            })
                            .collect::<Result<Vec<_>>>()?;

                        let ctx = apply_gates(
                            TransformContext::new(dialect)
                                .with_cfe_definitions(all_cfes.clone())
                                .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                                    std::sync::Arc::new(
                                        crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                                    )
                                })),
                        );
                        let state = transform_relational(main_query, &ctx)?;

                        // Check if main query generated intermediate CTEs (e.g., premelt for melt patterns)
                        let generated_ctes = ctx.generated_ctes.borrow().clone();
                        let combined_ctes = if !generated_ctes.is_empty() {
                            let mut all_ctes = sql_ctes;
                            all_ctes.extend(generated_ctes);
                            all_ctes
                        } else {
                            sql_ctes
                        };

                        if let QueryBuildState::DmlStatement(mut dml_stmt) = state {
                            match &mut dml_stmt {
                                SqlStatement::Delete { with_clause, .. }
                                | SqlStatement::Update { with_clause, .. }
                                | SqlStatement::Insert { with_clause, .. } => {
                                    *with_clause = Some(combined_ctes);
                                }
                                _ => {}
                            }
                            return Ok(dml_stmt);
                        }

                        let main_sql = finalize_to_query(state)?;
                        Ok(SqlStatement::with_ctes(Some(combined_ctes), main_sql))
                    } else {
                        let mut cte_map = std::collections::HashMap::new();
                        for cte in ctes {
                            let ctx = apply_gates(
                                TransformContext::with_force_ctes(false, dialect)
                                    .with_cfe_definitions(all_cfes.clone())
                                    .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                                        std::sync::Arc::new(
                                        crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                                    )
                                    })),
                            );
                            let state = transform_relational(cte.expression, &ctx)?;
                            let query_expr = finalize_to_query(state)?;
                            cte_map.insert(cte.name, query_expr);
                        }

                        let ctx = apply_gates(
                            TransformContext::with_force_ctes(false, dialect)
                                .with_cte_definitions(cte_map)
                                .with_cfe_definitions(all_cfes.clone())
                                .with_bin_registry(bin_registry.clone().unwrap_or_else(|| {
                                    std::sync::Arc::new(
                                        crate::bin_cartridge::registry::BinCartridgeRegistry::new(),
                                    )
                                })),
                        );
                        let state = transform_relational(main_query, &ctx)?;

                        if let QueryBuildState::DmlStatement(dml_stmt) = state {
                            return Ok(dml_stmt);
                        }

                        let main_query_expr = finalize_to_query(state)?;

                        Ok(SqlStatement::Query {
                            with_clause: None,
                            query: main_query_expr,
                        })
                    }
                }
                // Nested CFEs or REPL commands - recursively call transform_query_with_options
                _ => transform_query_with_options(
                    *query,
                    force_ctes,
                    dialect,
                    bin_registry.clone(),
                    None,
                    None,
                ),
            }
        }
    }
}

/// Entry point - transforms refined AST to SQL AST V3
/// Pure function: AST in, SQL out, no side effects
pub fn transform(
    ast: ast_addressed::RelationalExpression,
    dialect: SqlDialect,
) -> Result<SqlStatement> {
    log::debug!("=== TRANSFORMER V3 ENTRY (transform) ===");
    log::debug!("AST type: {:?}", std::mem::discriminant(&ast));
    log::debug!("Continuing after discriminant log...");

    // Check if it's a SetOperation with correlation
    if let ast_addressed::RelationalExpression::SetOperation {
        ref correlation, ..
    } = ast
    {
        log::debug!(
            "Root is SetOperation, has correlation: {}",
            correlation.get_correlation().is_some()
        );
    }

    // Build danger gate map (defaults only - transform() has no overrides)
    let danger_gate_map =
        std::sync::Arc::new(crate::pipeline::danger_gates::DangerGateMap::with_defaults());

    // Build option map (defaults only - transform() has no overrides)
    let option_map_arc =
        std::sync::Arc::new(crate::pipeline::option_map::OptionMap::with_defaults());

    // Helper: apply danger gates and option map to a fresh context
    let apply_gates = |ctx: TransformContext| -> TransformContext {
        TransformContext {
            danger_gates: danger_gate_map.clone(),
            option_map: option_map_arc.clone(),
            ..ctx
        }
    };

    // Create initial transform context
    let ctx = apply_gates(TransformContext::new(dialect));

    // Step 1: Pure transformation, accumulating within pipe segments
    log::debug!("About to call transform_relational");
    let state = transform_relational(ast, &ctx)?;
    log::debug!("transform_relational returned");
    let query = finalize_to_query(state)?;
    log::debug!("finalize_to_query returned");

    // Step 1.5: Top-level hygiene handling for non-positional patterns
    // Positional patterns handle hygiene locally (via BuilderWithHygienic)
    // CDT-SJ/CDT-GJ still rely on top-level wrapping (TODO: migrate them to local)
    let query = hide_hygienic_columns_from_output(query)?;

    // Step 2: CTE extraction pass (separate module)
    let mut statement = extract_ctes(query)?;

    // EPOCH 7: Add generated CTEs (e.g., premelt CTEs from melt patterns)
    let generated_ctes = ctx.generated_ctes.borrow().clone();
    if !generated_ctes.is_empty() {
        statement = match statement {
            SqlStatement::Query { with_clause, query } => {
                let combined_ctes = match with_clause {
                    Some(mut existing) => {
                        existing.extend(generated_ctes);
                        Some(existing)
                    }
                    None => Some(generated_ctes),
                };
                SqlStatement::Query {
                    with_clause: combined_ctes,
                    query,
                }
            }
            other => other, // Don't modify other statement types
        };
    }

    Ok(statement)
}

/// Transform a relational expression - the core recursive function
/// Returns QueryBuildState to allow accumulation within pipe segments
#[stacksafe::stacksafe]
pub(crate) fn transform_relational(
    expr: ast_addressed::RelationalExpression,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    log::debug!(
        "transform_relational called with type: {:?}",
        std::mem::discriminant(&expr)
    );
    match expr {
        // BASE CASE: Ground relation (table)
        ast_addressed::RelationalExpression::Relation(rel) => transform_relation(rel, ctx),

        // LINEARIZED: Pipe chain collected into flat list, processed iteratively.
        // Value-level covers ($$ and $) return Builder to keep FROM flat;
        // all other operators return Expression (complete query).
        ast_addressed::RelationalExpression::Pipe(_) => {
            let (base, segments) = crate::pipeline::pipe_chain::collect_pipe_chain(expr);

            // Extract the base schema BEFORE consuming it
            let mut source_schema = get_relational_schema(&base);

            // Transform the base (non-pipe expression)
            let mut state = transform_relational(base, ctx)?;

            // Iterate pipe segments (source-code order: innermost first)
            for segment in segments {
                let pipe_cpr_schema = segment.cpr_schema.get().clone();
                let laws = detect_cpr_laws(&pipe_cpr_schema);
                state = apply_pipe_operator_unified(
                    state,
                    source_schema,
                    segment.operator,
                    pipe_cpr_schema.clone(),
                    laws.law1_qualified_columns,
                    ctx,
                )?;
                source_schema = pipe_cpr_schema;
            }

            Ok(state)
        }

        // RECURSIVE CASE: And (join or filter) - accumulates within pipe segment
        ast_addressed::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => transform_join(
            *left,
            *right,
            join_condition,
            join_type.expect("Join should have join_type"),
            cpr_schema.get().clone(),
            ctx,
        ),
        ast_addressed::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => transform_filter(*source, condition, origin, cpr_schema.get().clone(), ctx), // Note: And(AndExpression) has been removed - Join and Filter are now direct variants

        ast_addressed::RelationalExpression::ErJoinChain { .. }
        | ast_addressed::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER-join consumed by resolver")
        }

        // Set operations (UNION ALL, etc.)
        ast_addressed::RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => {
            log::debug!(
                "TRANSFORMING SETOPERATION: operator={:?}, num_operands={}",
                operator,
                operands.len()
            );
            // Handle correlation predicates if present (accessed via PhaseBox)
            log::debug!("SetOperation: checking for correlation...");
            log::debug!(
                "Correlation PhaseBox data: {:?}",
                correlation.get_correlation()
            );
            if let Some(ref correlation_pred) = correlation.get_correlation() {
                log::debug!("Found correlation predicate: {:?}", correlation_pred);
                let involved = extract_aliases_from_predicate(correlation_pred);
                log::debug!("Extracted aliases from correlation: {:?}", involved);

                // min_multiplicity gate: ROW_NUMBER + JOIN instead of bidirectional semijoin
                if ctx
                    .danger_gates
                    .is_enabled("dql/semantics/min_multiplicity")
                {
                    log::debug!("min_multiplicity gate ON — using ROW_NUMBER path");
                    let query = transform_min_multiplicity_intersection(
                        operator,
                        operands,
                        correlation_pred.clone(),
                        cpr_schema.get().clone(),
                        ctx,
                    )?;
                    return Ok(QueryBuildState::Expression(query));
                }

                // Default: bidirectional semijoin (UNION ALL + EXISTS)
                let query = transform_set_operation_with_correlation(
                    operator,
                    operands,
                    correlation_pred.clone(),
                    cpr_schema.get().clone(),
                    ctx,
                )?;
                return Ok(QueryBuildState::Expression(query));
            } else {
                log::debug!("No correlation predicate on SetOperation");
            }

            // For CORRESPONDING operations without correlation, handle NULL padding
            let mut queries = Vec::new();

            log::debug!("SetOperation operator: {:?}", operator);
            log::debug!(
                "Operator discriminant: {:?}",
                std::mem::discriminant(&operator)
            );
            match operator {
                ast_addressed::SetOperator::UnionAllPositional => {
                    log::debug!("Matched UnionAll! Handling positional UnionAll - need explicit column lists");
                    // For positional UNION ALL, we need to explicitly list columns
                    // Get the unified schema to ensure all operands use the same column names
                    let unified_schema = cpr_schema.get();
                    log::debug!("UnionAll unified schema: {:?}", unified_schema);

                    for (i, operand) in operands.into_iter().enumerate() {
                        log::debug!("About to wrap operand {} for UnionAll", i);
                        let wrapped = wrap_with_explicit_columns_unified(
                            operand,
                            unified_schema,
                            i == 0,
                            ctx,
                        )?;
                        log::debug!("Wrapped operand {} for UnionAll", i);
                        queries.push(wrapped);
                    }
                }
                ast_addressed::SetOperator::UnionCorresponding => {
                    log::debug!("Using NULL padding for UnionAllOuter");
                    // For CORRESPONDING OUTER, generate explicit column lists with NULL padding
                    let unified_schema = cpr_schema.get();
                    log::debug!("Unified schema: {:?}", unified_schema);

                    for (i, operand) in operands.into_iter().enumerate() {
                        log::debug!(
                            "Processing operand {} for UnionAllOuter: {:?}",
                            i,
                            std::mem::discriminant(&operand)
                        );
                        // Create a wrapped query with explicit column selection
                        let wrapped = wrap_with_null_padding(operand, unified_schema, ctx)?;
                        queries.push(wrapped);
                    }
                }
                _ => {
                    // Other set operations (UnionAllInner etc)
                    for operand in operands {
                        let state = transform_relational(operand, ctx)?;
                        let query = finalize_to_query(state)?;
                        queries.push(query);
                    }
                }
            }

            // Build the set operation
            if queries.is_empty() {
                return Err(crate::error::DelightQLError::ParseError {
                    message: "Set operation requires at least one operand".to_string(),
                    source: None,
                    subcategory: None,
                });
            }

            // Build nested set operations (right-associative)
            // Wrap queries with LIMIT/ORDER BY in subqueries for SQL compatibility
            let wrapped_queries: Vec<QueryExpression> = queries
                .into_iter()
                .map(|q| {
                    match &q {
                        QueryExpression::Select(select)
                            if select.limit().is_some() || select.order_by().is_some() =>
                        {
                            // Wrap in SELECT * FROM (...) AS alias
                            let alias = next_alias();
                            let q_updated = update_query_provenance(q, &alias);
                            let table_expr = TableExpression::Subquery {
                                query: Box::new(stacksafe::StackSafe::new(q_updated)),
                                alias,
                            };
                            QueryExpression::Select(Box::new(
                                SelectBuilder::new()
                                    .select(SelectItem::star())
                                    .from_tables(vec![table_expr])
                                    .build()
                                    .expect("SELECT * FROM subquery should always be valid"),
                            ))
                        }
                        _ => q,
                    }
                })
                .collect();

            let mut queries_iter = wrapped_queries.into_iter();
            let mut result = queries_iter
                .next()
                .expect("wrapped_queries cannot be empty - checked queries.is_empty() above");
            for query in queries_iter {
                result = QueryExpression::SetOperation {
                    op: match operator {
                        ast_addressed::SetOperator::UnionAllPositional => SetOperator::UnionAll,
                        ast_addressed::SetOperator::UnionCorresponding => SetOperator::UnionAll, // SQL uses same UNION ALL
                        ast_addressed::SetOperator::SmartUnionAll => SetOperator::UnionAll, // SQL uses same UNION ALL
                        ast_addressed::SetOperator::MinusCorresponding => SetOperator::Except,
                    },
                    left: Box::new(result),
                    right: Box::new(query),
                };
            }

            Ok(QueryBuildState::Expression(result))
        }
    }
}

/// Transform a pipe expression - creates a query boundary
/// RECURSIVE: transform source, then apply operator (with Law 1 detection)
pub(crate) fn transform_pipe(
    pipe: ast_addressed::PipeExpression,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    // Extract the source's schema before transforming
    let source_schema = get_relational_schema(&pipe.source);

    // Recursively transform the source
    let source_state = transform_relational(pipe.source, ctx)?;

    // DETECT FIRST: Check if Law 1 applies
    let laws = detect_cpr_laws(pipe.cpr_schema.get());

    // Apply the operator - let it decide if a subquery is needed
    // Value-level covers ($$ and $) return Builder; others return Expression.
    apply_pipe_operator_unified(
        source_state,
        source_schema, // Pass the source's schema for alias generation
        pipe.operator,
        pipe.cpr_schema.get().clone(),
        laws.law1_qualified_columns, // Pass law1 detection for reference
        ctx,
    )
}
