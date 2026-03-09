// Filter Transformation Module
// Handles filter expressions (sigma) and tuple ordinal operations

use std::collections::HashMap;
use std::sync::Arc;

use super::QualifierScope;
use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{
    DomainExpression as SqlDomainExpression, QueryExpression, SelectItem, SelectStatement,
    TableExpression,
};

use super::context::TransformContext;
use super::helpers::{alias_generator::next_alias, extract_table_alias};
use super::segment_handler::{finalize_segment_to_query, finalize_to_query, SegmentSource};
use super::transform_domain_expression;
use super::types::QueryBuildState;
use crate::pipeline::transformer_v3::transform_relational;

/// Transform a filter expression (sigma)
/// Accumulates filters within pipe segments
pub fn transform_filter(
    relation: ast_addressed::RelationalExpression,
    condition: ast_addressed::SigmaCondition,
    origin: ast_addressed::FilterOrigin,
    cpr_schema: ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    // Special case: PositionalLiteral filter on a positional pattern
    // Check BEFORE transforming to avoid double-wrapping
    if let ast_addressed::FilterOrigin::PositionalLiteral { .. } = origin {
        if let ast_addressed::RelationalExpression::Relation(ast_addressed::Relation::Ground {
            domain_spec: ast_addressed::DomainSpec::Positional(_),
            ..
        }) = &relation
        {
            // Extract schema from the relation being filtered
            let _schema =
                crate::pipeline::transformer_v3::schema_utils::get_relational_schema(&relation);
            let mut schema_ctx =
                crate::pipeline::transformer_v3::SchemaContext::new(cpr_schema.clone());

            // Transform the positional relation - this will create a Builder with SELECT
            let source_state = transform_relational(relation.clone(), ctx)?;

            match source_state {
                QueryBuildState::Builder(builder) => {
                    // Merge the WHERE clause into the positional SELECT
                    log::debug!("Merging PositionalLiteral filter into positional SELECT");

                    if let ast_addressed::SigmaCondition::Predicate(expr) = condition {
                        let where_expr = transform_domain_expression(
                            ast_addressed::DomainExpression::Predicate {
                                expr: Box::new(expr),
                                alias: None,
                            },
                            ctx,
                            &mut schema_ctx,
                        )?;

                        return Ok(QueryBuildState::Builder(builder.and_where(where_expr)));
                    }
                }
                QueryBuildState::BuilderWithHygienic {
                    builder,
                    hygienic_injections,
                } => {
                    // Merge the WHERE clause AND preserve hygienic_injections
                    log::debug!("Merging PositionalLiteral filter into positional SELECT with hygienic columns");

                    if let ast_addressed::SigmaCondition::Predicate(expr) = condition {
                        let where_expr = transform_domain_expression(
                            ast_addressed::DomainExpression::Predicate {
                                expr: Box::new(expr),
                                alias: None,
                            },
                            ctx,
                            &mut schema_ctx,
                        )?;

                        return Ok(QueryBuildState::BuilderWithHygienic {
                            builder: builder.and_where(where_expr),
                            hygienic_injections,
                        });
                    }
                }
                _other => panic!("catch-all hit in filter_transformer.rs apply_filter (PositionalLiteral optimization)"),
            }
        }
    }

    // Normal case: transform the source first, then apply filter
    let source_state = transform_relational(relation, ctx)?;

    // Use the filter's cpr_schema so the expression transformer has provenance info.
    // Clone before move so we can check for hygienic columns later.
    let cpr_schema_ref = cpr_schema.clone();
    let mut filter_schema_ctx = crate::pipeline::transformer_v3::SchemaContext::new(cpr_schema);

    // Handle different types of sigma conditions
    match condition {
        ast_addressed::SigmaCondition::Predicate(expr) => {
            // Normal filter handling
            match source_state {
                QueryBuildState::Table(table) | QueryBuildState::AnonymousTable(table) => {
                    // First filter - convert table to SELECT with WHERE
                    // Both regular and anonymous tables are handled the same way here
                    // Only set correlation context if we're not already inside an EXISTS
                    let should_set_context = ctx.correlation_alias.is_none();

                    let correlation_alias = if should_set_context {
                        extract_table_alias(&table)
                    } else {
                        None
                    };

                    let new_ctx = if let Some(alias) = &correlation_alias {
                        TransformContext {
                            correlation_alias: Some(alias.clone()),
                            alias_remappings: ctx.alias_remappings.clone(),
                            force_ctes: ctx.force_ctes,
                            cte_definitions: ctx.cte_definitions.clone(),
                            cfe_definitions: ctx.cfe_definitions.clone(),
                            generated_ctes: ctx.generated_ctes.clone(),
                            in_aggregate: ctx.in_aggregate,
                            qualifier_scope: None,
                            dialect: ctx.dialect,
                            bin_registry: ctx.bin_registry.clone(),
                            danger_gates: ctx.danger_gates.clone(),
                            option_map: ctx.option_map.clone(),
                            drill_column_mappings: ctx.drill_column_mappings.clone(),
                        }
                    } else {
                        ctx.clone()
                    };

                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(expr),
                            alias: None,
                        },
                        &new_ctx,
                        &mut filter_schema_ctx,
                    )?;

                    // Strip hygienic columns ONLY for HO ground scalar filters.
                    // Regular PositionalLiteral filters keep all columns (SELECT *).
                    let select_items =
                        if matches!(origin, ast_addressed::FilterOrigin::HoGroundScalar { .. }) {
                            build_select_items_stripping_hygienic(
                                &cpr_schema_ref,
                                &correlation_alias,
                            )
                        } else {
                            vec![SelectItem::star()]
                        };

                    let builder = SelectStatement::builder()
                        .select_all(select_items)
                        .from_tables(vec![table])
                        .where_clause(where_expr);
                    Ok(QueryBuildState::Builder(builder))
                }
                QueryBuildState::Builder(builder) => {
                    // Additional filter - add AND to existing WHERE
                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(expr),
                            alias: None,
                        },
                        ctx,
                        &mut filter_schema_ctx,
                    )?;
                    Ok(QueryBuildState::Builder(builder.and_where(where_expr)))
                }
                QueryBuildState::BuilderWithHygienic {
                    builder,
                    hygienic_injections,
                } => {
                    // Additional filter on builder with hygienic columns - preserve metadata
                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(expr),
                            alias: None,
                        },
                        ctx,
                        &mut filter_schema_ctx,
                    )?;
                    Ok(QueryBuildState::BuilderWithHygienic {
                        builder: builder.and_where(where_expr),
                        hygienic_injections,
                    })
                }
                QueryBuildState::Expression(query_expr) => {
                    // This shouldn't happen within a pipe segment
                    // but if it does, we need to wrap it
                    let alias = next_alias();

                    // Build local remappings for inner FROM aliases so the WHERE clause
                    // uses the wrapper alias instead of stale inner aliases.
                    // Also include raw table names (no AS clause) like CTE references,
                    // since P3 may qualify columns with the CTE name.
                    let mut local_remaps = HashMap::new();
                    let inner_aliases =
                        super::join_handler::extract_inner_from_aliases(&query_expr);
                    for inner_alias in inner_aliases {
                        local_remaps.insert(inner_alias, alias.clone());
                    }
                    // Also extract raw (unaliased) table names from inner FROM
                    let inner_table_names = extract_inner_from_table_names(&query_expr);
                    for table_name in inner_table_names {
                        local_remaps
                            .entry(table_name)
                            .or_insert_with(|| alias.clone());
                    }

                    // Set this alias as correlation context for any EXISTS in the WHERE clause
                    let expr_ctx = {
                        let mut merged = (*ctx.alias_remappings).clone();
                        merged.extend(local_remaps.iter().map(|(k, v)| (k.clone(), v.clone())));
                        TransformContext {
                            correlation_alias: Some(alias.clone()),
                            alias_remappings: Arc::new(merged),
                            force_ctes: ctx.force_ctes,
                            cte_definitions: ctx.cte_definitions.clone(),
                            cfe_definitions: ctx.cfe_definitions.clone(),
                            generated_ctes: ctx.generated_ctes.clone(),
                            in_aggregate: ctx.in_aggregate,
                            qualifier_scope: None,
                            dialect: ctx.dialect,
                            bin_registry: ctx.bin_registry.clone(),
                            danger_gates: ctx.danger_gates.clone(),
                            option_map: ctx.option_map.clone(),
                            drill_column_mappings: ctx.drill_column_mappings.clone(),
                        }
                    };

                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(expr),
                            alias: None,
                        },
                        &expr_ctx,
                        &mut filter_schema_ctx,
                    )?;

                    let builder = SelectStatement::builder()
                        .select(SelectItem::star())
                        .from_subquery(query_expr, &alias)
                        .where_clause(where_expr);

                    Ok(QueryBuildState::Builder(builder))
                }
                QueryBuildState::Segment {
                    source,
                    mut filters,
                    order_by,
                    limit_offset,
                    cpr_schema,
                    dialect,
                    remappings,
                } => {
                    // Extract correlation alias from the left side of join chain if present
                    let base_ctx = ctx.with_additional_remappings(&remappings);
                    let filter_ctx = match &source {
                        SegmentSource::JoinChain { tables, .. } => {
                            // For join chains, use the first table's alias as correlation context
                            // This allows CPR references (_) to resolve to the left side
                            if !tables.is_empty() {
                                if let Some(alias) = extract_table_alias(&tables[0]) {
                                    TransformContext {
                                        correlation_alias: Some(alias),
                                        ..base_ctx.clone()
                                    }
                                } else {
                                    base_ctx.clone()
                                }
                            } else {
                                base_ctx.clone()
                            }
                        }
                        SegmentSource::Single(table) => {
                            // For single tables, use the table's alias if available
                            if let Some(alias) = extract_table_alias(table) {
                                TransformContext {
                                    correlation_alias: Some(alias),
                                    ..base_ctx.clone()
                                }
                            } else {
                                base_ctx.clone()
                            }
                        }
                    };

                    // Add filter to the segment's filter list
                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(expr),
                            alias: None,
                        },
                        &filter_ctx,
                        &mut filter_schema_ctx,
                    )?;
                    filters.push(where_expr);
                    Ok(QueryBuildState::Segment {
                        source,
                        filters,
                        order_by,
                        limit_offset,
                        cpr_schema,
                        dialect,
                        remappings,
                    })
                }
                QueryBuildState::MeltTable { .. } => {
                    // Standalone melt table — materialize to query, wrap, continue.
                    let melt_query = super::segment_handler::finalize_to_query(source_state)?;
                    let alias = super::helpers::alias_generator::next_alias();
                    let builder = crate::pipeline::sql_ast_v3::SelectStatement::builder()
                        .select(crate::pipeline::sql_ast_v3::SelectItem::star())
                        .from_subquery(melt_query, &alias);
                    Ok(QueryBuildState::Builder(builder))
                }
                QueryBuildState::DmlStatement(_) => Err(crate::error::DelightQLError::ParseError {
                    message: "Cannot apply filter after DML terminal operator".to_string(),
                    source: None,
                    subcategory: None,
                }),
            }
        }

        ast_addressed::SigmaCondition::TupleOrdinal(ordinal) => {
            // Tuple ordinal becomes LIMIT/OFFSET
            // Apply limit to the current source state
            match ordinal.operator {
                ast_addressed::TupleOrdinalOperator::LessThan => {
                    // #<N means LIMIT N
                    match source_state {
                        QueryBuildState::Builder(builder) => {
                            // CPR LTR semantics: If builder already has a limit, wrap it in a subquery
                            // users(*), #<5, #<20 → SELECT * FROM (SELECT * FROM users LIMIT 5) AS t0 LIMIT 20
                            if builder.has_limit() {
                                // Finalize current builder and wrap in subquery
                                let stmt = builder.build().map_err(|e| {
                                    crate::error::DelightQLError::ParseError {
                                        message: e,
                                        source: None,
                                        subcategory: None,
                                    }
                                })?;
                                let expr = QueryExpression::Select(Box::new(stmt));
                                let alias = next_alias();
                                let new_builder = SelectStatement::builder()
                                    .select(SelectItem::star())
                                    .from_subquery(expr, &alias)
                                    .limit(ordinal.value);
                                Ok(QueryBuildState::Builder(new_builder))
                            } else {
                                // No existing limit - just add it
                                Ok(QueryBuildState::Builder(builder.limit(ordinal.value)))
                            }
                        }
                        QueryBuildState::BuilderWithHygienic {
                            builder,
                            hygienic_injections,
                        } => {
                            // CPR LTR semantics: If builder already has a limit, wrap it in a subquery
                            if builder.has_limit() {
                                // Finalize current builder and wrap in subquery
                                // Note: hygienic injections are lost here since we're wrapping
                                // This is correct - the hygienic columns were for the inner query
                                let stmt = builder.build().map_err(|e| {
                                    crate::error::DelightQLError::ParseError {
                                        message: e,
                                        source: None,
                                        subcategory: None,
                                    }
                                })?;
                                let expr = QueryExpression::Select(Box::new(stmt));
                                let alias = next_alias();
                                let new_builder = SelectStatement::builder()
                                    .select(SelectItem::star())
                                    .from_subquery(expr, &alias)
                                    .limit(ordinal.value);
                                Ok(QueryBuildState::Builder(new_builder))
                            } else {
                                Ok(QueryBuildState::BuilderWithHygienic {
                                    builder: builder.limit(ordinal.value),
                                    hygienic_injections,
                                })
                            }
                        }
                        QueryBuildState::Table(table) | QueryBuildState::AnonymousTable(table) => {
                            // Need to convert table to SELECT with LIMIT
                            let builder = SelectStatement::builder()
                                .select(SelectItem::star())
                                .from_tables(vec![table])
                                .limit(ordinal.value);
                            Ok(QueryBuildState::Builder(builder))
                        }
                        QueryBuildState::Expression(expr) => {
                            // Already finalized - wrap with LIMIT
                            let alias = next_alias();
                            let builder = SelectStatement::builder()
                                .select(SelectItem::star())
                                .from_subquery(expr, &alias)
                                .limit(ordinal.value);
                            Ok(QueryBuildState::Builder(builder))
                        }
                        QueryBuildState::Segment {
                            source,
                            filters,
                            order_by,
                            limit_offset,
                            cpr_schema,
                            dialect,
                            remappings,
                        } => {
                            // CPR LTR semantics: If segment already has a limit, finalize and wrap
                            if limit_offset.is_some() {
                                // Finalize current segment with its limit
                                let expr = finalize_segment_to_query(
                                    source,
                                    filters,
                                    order_by,
                                    limit_offset,
                                )?;
                                let alias = next_alias();
                                let new_builder = SelectStatement::builder()
                                    .select(SelectItem::star())
                                    .from_subquery(expr, &alias)
                                    .limit(ordinal.value);
                                Ok(QueryBuildState::Builder(new_builder))
                            } else {
                                // No existing limit - just add it to the segment
                                Ok(QueryBuildState::Segment {
                                    source,
                                    filters,
                                    order_by,
                                    limit_offset: Some((ordinal.value, 0)),
                                    cpr_schema,
                                    dialect,
                                    remappings,
                                })
                            }
                        }
                        QueryBuildState::MeltTable { .. } => {
                            Err(crate::error::DelightQLError::ParseError {
                                message: "Melt tables can only appear as the right side of a join"
                                    .to_string(),
                                source: None,
                                subcategory: None,
                            })
                        }
                        QueryBuildState::DmlStatement(_) => {
                            Err(crate::error::DelightQLError::ParseError {
                                message: "Cannot apply filter after DML terminal operator"
                                    .to_string(),
                                source: None,
                                subcategory: None,
                            })
                        }
                    }
                }
                ast_addressed::TupleOrdinalOperator::GreaterThan => {
                    // #>N means OFFSET N
                    // SQLite requires a LIMIT when using OFFSET, so we use a large number
                    let large_limit = 9223372036854775807i64; // max i64
                    match source_state {
                        QueryBuildState::Builder(builder) => {
                            // CPR LTR semantics: If builder already has a limit, wrap it in a subquery
                            if builder.has_limit() {
                                let stmt = builder.build().map_err(|e| {
                                    crate::error::DelightQLError::ParseError {
                                        message: e,
                                        source: None,
                                        subcategory: None,
                                    }
                                })?;
                                let expr = QueryExpression::Select(Box::new(stmt));
                                let alias = next_alias();
                                let new_builder = SelectStatement::builder()
                                    .select(SelectItem::star())
                                    .from_subquery(expr, &alias)
                                    .limit_offset(large_limit, ordinal.value);
                                Ok(QueryBuildState::Builder(new_builder))
                            } else {
                                Ok(QueryBuildState::Builder(
                                    builder.limit_offset(large_limit, ordinal.value),
                                ))
                            }
                        }
                        QueryBuildState::BuilderWithHygienic {
                            builder,
                            hygienic_injections,
                        } => {
                            // CPR LTR semantics: If builder already has a limit, wrap it in a subquery
                            if builder.has_limit() {
                                let stmt = builder.build().map_err(|e| {
                                    crate::error::DelightQLError::ParseError {
                                        message: e,
                                        source: None,
                                        subcategory: None,
                                    }
                                })?;
                                let expr = QueryExpression::Select(Box::new(stmt));
                                let alias = next_alias();
                                let new_builder = SelectStatement::builder()
                                    .select(SelectItem::star())
                                    .from_subquery(expr, &alias)
                                    .limit_offset(large_limit, ordinal.value);
                                Ok(QueryBuildState::Builder(new_builder))
                            } else {
                                Ok(QueryBuildState::BuilderWithHygienic {
                                    builder: builder.limit_offset(large_limit, ordinal.value),
                                    hygienic_injections,
                                })
                            }
                        }
                        QueryBuildState::Table(table) | QueryBuildState::AnonymousTable(table) => {
                            // Need to convert table to SELECT with OFFSET
                            let builder = SelectStatement::builder()
                                .select(SelectItem::star())
                                .from_tables(vec![table])
                                .limit_offset(large_limit, ordinal.value);
                            Ok(QueryBuildState::Builder(builder))
                        }
                        QueryBuildState::Expression(expr) => {
                            // Already finalized - wrap with OFFSET
                            let alias = next_alias();
                            let builder = SelectStatement::builder()
                                .select(SelectItem::star())
                                .from_subquery(expr, &alias)
                                .limit_offset(large_limit, ordinal.value);
                            Ok(QueryBuildState::Builder(builder))
                        }
                        QueryBuildState::Segment {
                            source,
                            filters,
                            order_by,
                            limit_offset,
                            cpr_schema,
                            dialect,
                            remappings,
                        } => {
                            // CPR LTR semantics: If segment already has a limit, finalize and wrap
                            if limit_offset.is_some() {
                                // Finalize current segment with its limit
                                let expr = finalize_segment_to_query(
                                    source,
                                    filters,
                                    order_by,
                                    limit_offset,
                                )?;
                                let alias = next_alias();
                                let new_builder = SelectStatement::builder()
                                    .select(SelectItem::star())
                                    .from_subquery(expr, &alias)
                                    .limit_offset(large_limit, ordinal.value);
                                Ok(QueryBuildState::Builder(new_builder))
                            } else {
                                // No existing limit - just add offset to the segment
                                Ok(QueryBuildState::Segment {
                                    source,
                                    filters,
                                    order_by,
                                    limit_offset: Some((large_limit, ordinal.value)),
                                    cpr_schema,
                                    dialect,
                                    remappings,
                                })
                            }
                        }
                        QueryBuildState::MeltTable { .. } => {
                            Err(crate::error::DelightQLError::ParseError {
                                message: "Melt tables can only appear as the right side of a join"
                                    .to_string(),
                                source: None,
                                subcategory: None,
                            })
                        }
                        QueryBuildState::DmlStatement(_) => {
                            Err(crate::error::DelightQLError::ParseError {
                                message: "Cannot apply filter after DML terminal operator"
                                    .to_string(),
                                source: None,
                                subcategory: None,
                            })
                        }
                    }
                }
                _ => {
                    // Other operators not yet supported
                    Err(crate::error::DelightQLError::ParseError {
                        message: format!(
                            "Tuple ordinal operator {:?} not yet supported",
                            ordinal.operator
                        ),
                        source: None,
                        subcategory: None,
                    })
                }
            }
        }

        ast_addressed::SigmaCondition::Destructure {
            json_column,
            pattern,
            mode,
            destructured_schema,
        } => {
            use ast_addressed::DestructureMode;

            match mode {
                DestructureMode::Scalar => {
                    // Scalar destructuring: Add json_extract columns, no row explosion
                    transform_scalar_destructuring(
                        source_state,
                        json_column,
                        pattern,
                        destructured_schema,
                        ctx,
                    )
                }
                DestructureMode::Aggregate => {
                    // Aggregate destructuring: Add json_each joins with row explosion
                    transform_aggregate_destructuring(
                        source_state,
                        json_column,
                        pattern,
                        destructured_schema,
                        ctx,
                    )
                }
            }
        }

        ast_addressed::SigmaCondition::SigmaCall {
            functor,
            arguments,
            exists,
        } => {
            // Transform sigma predicate call to SQL WHERE condition
            // Wrap it in BooleanExpression::Sigma and treat like Predicate
            let sigma_expr = ast_addressed::BooleanExpression::Sigma {
                condition: Box::new(ast_addressed::SigmaCondition::SigmaCall {
                    functor: functor.clone(),
                    arguments: arguments.clone(),
                    exists,
                }),
            };

            // Same handling as Predicate case
            match source_state {
                QueryBuildState::Table(table) | QueryBuildState::AnonymousTable(table) => {
                    // First filter - convert table to SELECT with WHERE
                    let should_set_context = ctx.correlation_alias.is_none();

                    let correlation_alias = if should_set_context {
                        extract_table_alias(&table)
                    } else {
                        None
                    };

                    let new_ctx = if let Some(alias) = &correlation_alias {
                        TransformContext {
                            correlation_alias: Some(alias.clone()),
                            alias_remappings: ctx.alias_remappings.clone(),
                            force_ctes: ctx.force_ctes,
                            cte_definitions: ctx.cte_definitions.clone(),
                            cfe_definitions: ctx.cfe_definitions.clone(),
                            generated_ctes: ctx.generated_ctes.clone(),
                            in_aggregate: ctx.in_aggregate,
                            qualifier_scope: None,
                            dialect: ctx.dialect,
                            bin_registry: ctx.bin_registry.clone(),
                            danger_gates: ctx.danger_gates.clone(),
                            option_map: ctx.option_map.clone(),
                            drill_column_mappings: ctx.drill_column_mappings.clone(),
                        }
                    } else {
                        ctx.clone()
                    };

                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(sigma_expr),
                            alias: None,
                        },
                        &new_ctx,
                        &mut filter_schema_ctx,
                    )?;

                    let builder = SelectStatement::builder()
                        .select(SelectItem::star())
                        .from_tables(vec![table])
                        .where_clause(where_expr);
                    Ok(QueryBuildState::Builder(builder))
                }
                QueryBuildState::Builder(builder) => {
                    // Additional filter - add AND to existing WHERE
                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(sigma_expr),
                            alias: None,
                        },
                        ctx,
                        &mut filter_schema_ctx,
                    )?;
                    Ok(QueryBuildState::Builder(builder.and_where(where_expr)))
                }
                QueryBuildState::BuilderWithHygienic {
                    builder,
                    hygienic_injections,
                } => {
                    // Additional filter on builder with hygienic columns
                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(sigma_expr),
                            alias: None,
                        },
                        ctx,
                        &mut filter_schema_ctx,
                    )?;
                    Ok(QueryBuildState::BuilderWithHygienic {
                        builder: builder.and_where(where_expr),
                        hygienic_injections,
                    })
                }
                QueryBuildState::Expression(query_expr) => {
                    let alias = next_alias();

                    let expr_ctx = TransformContext {
                        correlation_alias: Some(alias.clone()),
                        alias_remappings: ctx.alias_remappings.clone(),
                        force_ctes: ctx.force_ctes,
                        cte_definitions: ctx.cte_definitions.clone(),
                        cfe_definitions: ctx.cfe_definitions.clone(),
                        generated_ctes: ctx.generated_ctes.clone(),
                        in_aggregate: ctx.in_aggregate,
                        qualifier_scope: None,
                        dialect: ctx.dialect,
                        bin_registry: ctx.bin_registry.clone(),
                        danger_gates: ctx.danger_gates.clone(),
                        option_map: ctx.option_map.clone(),
                        drill_column_mappings: ctx.drill_column_mappings.clone(),
                    };

                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(sigma_expr),
                            alias: None,
                        },
                        &expr_ctx,
                        &mut filter_schema_ctx,
                    )?;

                    let builder = SelectStatement::builder()
                        .select(SelectItem::star())
                        .from_subquery(query_expr, &alias)
                        .where_clause(where_expr);

                    Ok(QueryBuildState::Builder(builder))
                }
                QueryBuildState::Segment {
                    source,
                    mut filters,
                    order_by,
                    limit_offset,
                    cpr_schema,
                    dialect,
                    remappings,
                } => {
                    // Extract correlation alias from the left side of join chain if present
                    let base_ctx = ctx.with_additional_remappings(&remappings);
                    let filter_ctx = match &source {
                        SegmentSource::JoinChain { tables, .. } => {
                            // For join chains, use the first table's alias as correlation context
                            if !tables.is_empty() {
                                if let Some(alias) = extract_table_alias(&tables[0]) {
                                    TransformContext {
                                        correlation_alias: Some(alias),
                                        ..base_ctx.clone()
                                    }
                                } else {
                                    base_ctx.clone()
                                }
                            } else {
                                base_ctx.clone()
                            }
                        }
                        SegmentSource::Single(table) => {
                            // For single tables, use the table's alias if available
                            if let Some(alias) = extract_table_alias(table) {
                                TransformContext {
                                    correlation_alias: Some(alias),
                                    ..base_ctx.clone()
                                }
                            } else {
                                base_ctx.clone()
                            }
                        }
                    };

                    // Transform sigma expression to SQL and add to filter list
                    let where_expr = transform_domain_expression(
                        ast_addressed::DomainExpression::Predicate {
                            expr: Box::new(sigma_expr),
                            alias: None,
                        },
                        &filter_ctx,
                        &mut filter_schema_ctx,
                    )?;
                    filters.push(where_expr);
                    Ok(QueryBuildState::Segment {
                        source,
                        filters,
                        order_by,
                        limit_offset,
                        cpr_schema,
                        dialect,
                        remappings,
                    })
                }
                QueryBuildState::MeltTable { .. } => {
                    // Standalone melt table — materialize to query, wrap, continue.
                    let melt_query = super::segment_handler::finalize_to_query(source_state)?;
                    let alias = super::helpers::alias_generator::next_alias();
                    let builder = crate::pipeline::sql_ast_v3::SelectStatement::builder()
                        .select(crate::pipeline::sql_ast_v3::SelectItem::star())
                        .from_subquery(melt_query, &alias);
                    Ok(QueryBuildState::Builder(builder))
                }
                QueryBuildState::DmlStatement(_) => Err(crate::error::DelightQLError::ParseError {
                    message: "Cannot apply filter after DML terminal operator".to_string(),
                    source: None,
                    subcategory: None,
                }),
            }
        }
    }
}

// =============================================================================
// Tree Group Destructuring Helpers (Epoch 3-4)
// =============================================================================

/// Transform scalar destructuring: json_col ~= {field1, field2}
/// Uses RECURSIVE PATTERN WALKING for nested objects
/// No json_each joins (scalar mode), just nested json_extract paths
fn transform_scalar_destructuring(
    source_state: QueryBuildState,
    json_column: Box<ast_addressed::DomainExpression>,
    pattern: Box<ast_addressed::FunctionExpression>,
    _destructured_schema: ast_addressed::PhaseBox<
        Vec<ast_addressed::DestructureMapping>,
        ast_addressed::Addressed,
    >,
    ctx: &TransformContext,
) -> Result<QueryBuildState> {
    use crate::pipeline::sql_ast_v3::SelectItem;

    // For scalar mode, we need a simple column name or expression string
    // Try to extract column name if simple, otherwise transform to SQL
    let json_source_str = match json_column.as_ref() {
        ast_addressed::DomainExpression::Lvar { name, .. } => name.clone(),
        _ => {
            // For complex expressions, transform and convert to string representation
            // This is a simplification - ideally we'd materialize in CTE
            // unknown() OK: this path always returns NotImplemented error below
            let _json_expr = super::expression_transformer::transform_domain_expression(
                *json_column.clone(),
                ctx,
                &mut crate::pipeline::transformer_v3::SchemaContext::unknown(),
            )?;
            // Convert SqlDomainExpression to string
            // For now, just reject complex expressions
            return Err(crate::error::DelightQLError::not_implemented(
                "Scalar destructuring currently only supports simple column references. Complex expressions coming soon."
            ));
        }
    };

    // RECURSIVELY walk the pattern
    // For scalar mode, this generates nested json_extract paths but NO json_each joins
    // The walker will see nested objects like {"sub": {field}} and generate:
    //   json_extract(source, '$.sub.field') - NOTE: This doesn't work! Need to think...
    //
    // Actually for nested objects without arrays, we need:
    //   json_extract(json_extract(source, '$.sub'), '$.field')
    // OR build the full path: json_extract(source, '$.sub.field')
    //
    // SQLite supports both. Let's use full paths for simplicity.
    //
    // But our walker generates separate json_each joins for nested ~>
    // For scalar nested objects (no ~>), we need different handling...
    //
    // WAIT: In scalar mode, nested objects DON'T use ~>!
    // Example: {name, "address": {city, zip}}
    // The "address": {city, zip} is KeyValue with value=Curly, NOT nested_reduction!
    //
    // So the walker needs to handle KeyValue where value is Curly (nested object)
    // vs KeyValue where nested_reduction=true (nested array)

    // For now, use the recursive walker with scalar mode
    // It generates json_extract calls, and may generate joins for nested aggregates
    let result = super::destructuring_recursive::walk_pattern_recursive(
        &pattern,
        json_source_str.to_string(),
        ast_addressed::DestructureMode::Scalar,
    )?;

    // Scalar mode with nested aggregates (e.g., {a, "b":~>{foo}}) generates joins
    // This is semantically equivalent to the two-step form:
    //   a ~= {a, "b": f}, f ~= ~> {foo}
    // So we need to handle the joins just like aggregate mode does.

    // Add the json_extract columns to the current query state
    // If there are joins (nested aggregates), we need to handle them like aggregate mode
    if !result.joins.is_empty() {
        // Wrap source in subquery and chain joins
        let wrapped = finalize_to_query(source_state)?;
        let source_alias = next_alias();
        let mut source_table = TableExpression::subquery(wrapped, &source_alias);

        // Chain all the json_each joins with LEFT JOIN ON 1=1
        for join_table in result.joins {
            use crate::pipeline::sql_ast_v3::DomainExpression as SqlDomainExpression;
            use ast_addressed::LiteralValue;
            let on_true = SqlDomainExpression::eq(
                SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
                SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
            );
            source_table = TableExpression::left_join(source_table, join_table, on_true);
        }

        // Build final SELECT with all columns
        // Include source table columns (source_alias.*) plus destructured columns
        let mut all_items = vec![SelectItem::qualified_star(&source_alias)];
        all_items.extend(result.select_items);

        let builder = SelectStatement::builder()
            .select_all(all_items)
            .from_tables(vec![source_table]);

        // Materialize in subquery
        let stmt = builder
            .build()
            .map_err(|e| crate::error::DelightQLError::ParseError {
                message: e,
                source: None,
                subcategory: None,
            })?;
        let expr = QueryExpression::Select(Box::new(stmt));
        let materialized_builder = SelectStatement::builder()
            .select(SelectItem::star())
            .from_subquery(expr, &next_alias());

        Ok(QueryBuildState::Builder(materialized_builder))
    } else {
        // No joins - simple scalar destructuring
        match source_state {
            QueryBuildState::Builder(builder) => {
                // Add json_extract columns to existing builder
                let builder_with_extracts = builder.select_all(result.select_items);

                // EPOCH 1 FIX: Materialize destructured columns in subquery
                let stmt = builder_with_extracts.build().map_err(|e| {
                    crate::error::DelightQLError::ParseError {
                        message: e,
                        source: None,
                        subcategory: None,
                    }
                })?;

                // Try to preserve original alias from the FROM clause (before moving stmt)
                let materialized_alias = stmt
                    .from()
                    .and_then(|from| {
                        if from.len() == 1 {
                            extract_table_alias(&from[0])
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| next_alias());

                let expr = QueryExpression::Select(Box::new(stmt));

                let materialized_builder = SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_subquery(expr, &materialized_alias);

                Ok(QueryBuildState::Builder(materialized_builder))
            }
            other => {
                // For other states, wrap in a subquery first
                let wrapped = finalize_to_query(other)?;

                // Try to preserve original alias from the wrapped query to maintain
                // qualified column references (e.g., t.x should still work after wrapping)
                let alias = if let QueryExpression::Select(ref stmt) = wrapped {
                    // Try to extract alias from the FROM clause
                    stmt.from()
                        .and_then(|from| {
                            if from.len() == 1 {
                                extract_table_alias(&from[0])
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| next_alias())
                } else {
                    next_alias()
                };

                // Build SELECT with all original columns plus json_extract columns
                let mut all_items = vec![SelectItem::star()];
                all_items.extend(result.select_items);

                let builder = SelectStatement::builder()
                    .select_all(all_items)
                    .from_subquery(wrapped, &alias);

                // EPOCH 1 FIX: Materialize again
                // Use the same alias to preserve qualified references through the materialization
                let stmt =
                    builder
                        .build()
                        .map_err(|e| crate::error::DelightQLError::ParseError {
                            message: e,
                            source: None,
                            subcategory: None,
                        })?;
                let expr = QueryExpression::Select(Box::new(stmt));
                let materialized_builder = SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_subquery(expr, &alias);

                Ok(QueryBuildState::Builder(materialized_builder))
            }
        }
    }
}

/// Transform aggregate destructuring: json_col ~= ~> {field1, field2}
/// Uses RECURSIVE PATTERN WALKING (INDUCTIVE PRINCIPLE)
/// Handles depth N uniformly - depth 1, 2, 3, ... all work automatically
fn transform_aggregate_destructuring(
    source_state: QueryBuildState,
    json_column: Box<ast_addressed::DomainExpression>,
    pattern: Box<ast_addressed::FunctionExpression>,
    _destructured_schema: ast_addressed::PhaseBox<
        Vec<ast_addressed::DestructureMapping>,
        ast_addressed::Addressed,
    >,
    _ctx: &TransformContext,
) -> Result<QueryBuildState> {
    use crate::pipeline::sql_ast_v3::{SelectItem, TableExpression};

    // Step 1: Ensure JSON column is simple (materialize complex expressions in CTE if needed)
    let json_col_name = match json_column.as_ref() {
        ast_addressed::DomainExpression::Lvar { name, .. } => name.clone(),
        _ => {
            // TODO: Implement CTE materialization for complex expressions
            // See: DESTRUCTURE-DOMAIN-EXPRESSION-GENERALIZATION-LIMITATION.md
            return Err(crate::error::DelightQLError::not_implemented(
                "Aggregate destructuring currently only supports simple column references. Complex expressions coming soon."
            ));
        }
    };

    // Step 2: Wrap source and get alias FIRST (needed for qualified column references)
    // This fixes the ambiguous column name bug when column is named "json"
    let wrapped = finalize_to_query(source_state)?;
    let source_alias = next_alias();

    // Step 3: For aggregate mode, handle based on pattern type
    //
    // For regular patterns: json_data ~= ~> {first_name, country}
    // 1. Create top-level json_each(json_data) AS j1
    // 2. Walk pattern with source="j1.value"
    // 3. Generates: json_extract(j1.value, '$.first_name')
    //
    // For MetadataTreeGroup: json_data ~= ~> country:~> {first_name}
    // 1. NO top-level join - pass json_data directly (but qualified!)
    // 2. Walker creates json_each for object iteration
    // 3. Walker handles array explosion internally

    // Qualify the column reference to avoid ambiguity
    // E.g., "json" becomes "t6.json"
    let qualified_json_col = format!("{}.{}", source_alias, json_col_name);

    let result = match pattern.as_ref() {
        ast_addressed::FunctionExpression::MetadataTreeGroup { .. } => {
            // MetadataTreeGroup: start with qualified column, no top-level join
            super::destructuring_recursive::walk_pattern_recursive(
                &pattern,
                qualified_json_col,
                ast_addressed::DestructureMode::Aggregate,
            )?
        }
        _ => {
            // Regular patterns: create top-level json_each first
            let top_level_alias = next_alias();
            let top_level_join = TableExpression::TVF {
                schema: None,
                function: "json_each".to_string(),
                arguments: vec![crate::pipeline::sql_ast_v3::TvfArgument::parse(
                    &qualified_json_col,
                )],
                alias: Some(top_level_alias.clone()),
            };

            let value_source = format!("{}.value", top_level_alias);
            let mut result = super::destructuring_recursive::walk_pattern_recursive(
                &pattern,
                value_source,
                ast_addressed::DestructureMode::Aggregate,
            )?;

            // Prepend the top-level join to the results
            result.joins.insert(0, top_level_join);
            result
        }
    };

    // Step 4: Assemble final query — always use LEFT JOIN (preserves rows with empty/null arrays)
    let mut source_table = TableExpression::subquery(wrapped, &source_alias);

    for join_table in result.joins {
        use crate::pipeline::sql_ast_v3::DomainExpression as SqlDomainExpression;
        use ast_addressed::LiteralValue;
        let on_true = SqlDomainExpression::eq(
            SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
            SqlDomainExpression::literal(LiteralValue::Number("1".to_string())),
        );
        source_table = TableExpression::left_join(source_table, join_table, on_true);
    }

    // Build final SELECT: source_alias.*, <all extracted columns>
    // Use qualified star to exclude json_each metadata columns (key, value, type, atom, etc.)
    let mut all_items = vec![SelectItem::qualified_star(&source_alias)];
    all_items.extend(result.select_items);

    let builder = SelectStatement::builder()
        .select_all(all_items)
        .from_tables(vec![source_table]);

    // EPOCH 1 FIX: Materialize destructured columns in subquery
    // This ensures json_extract() expressions become actual columns
    // that downstream projections can reference
    let stmt = builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })?;
    let expr = QueryExpression::Select(Box::new(stmt));
    let materialized_alias = next_alias();
    let materialized_builder = SelectStatement::builder()
        .select(SelectItem::star())
        .from_subquery(expr, &materialized_alias);

    Ok(QueryBuildState::Builder(materialized_builder))
}

/// Extract raw (unaliased) table names from inner FROM clauses.
/// Unlike extract_inner_from_aliases, this includes tables without an AS clause
/// (like CTE references: `FROM _ho_pipe_src`). These need remapping when the
/// query is wrapped in a subquery, because P3 may qualify columns with the
/// table name which becomes stale after wrapping.
fn extract_inner_from_table_names(query: &QueryExpression) -> Vec<String> {
    match query {
        QueryExpression::Select(select) => {
            if let Some(tables) = select.from() {
                tables
                    .iter()
                    .filter_map(|t| match t {
                        TableExpression::Table {
                            name, alias: None, ..
                        } => Some(name.clone()),
                        TableExpression::Table { alias: Some(_), .. } => None, // Has alias, skip
                        // Subquery/Values/UnionTable/Join/TVF: all have aliases or are composite — not raw table names
                        TableExpression::Subquery { .. }
                        | TableExpression::Values { .. }
                        | TableExpression::UnionTable { .. }
                        | TableExpression::Join { .. }
                        | TableExpression::TVF { .. } => None,
                    })
                    .collect()
            } else {
                Vec::new()
            }
        }
        QueryExpression::WithCte { query, .. } => extract_inner_from_table_names(query),
        // SetOperation/Values: no FROM clause with raw table names
        QueryExpression::SetOperation { .. } | QueryExpression::Values { .. } => Vec::new(),
    }
}

/// Build SELECT items, stripping hygienic columns if any exist.
/// When no hygienic columns are present, returns `[SELECT *]`.
/// When hygienic columns exist (e.g., _label_0 from HO ground scalars),
/// returns explicit column list excluding them (the WHERE can still reference
/// them because SQL WHERE operates on FROM scope, not SELECT scope).
fn build_select_items_stripping_hygienic(
    cpr_schema: &ast_addressed::CprSchema,
    qualifier: &Option<String>,
) -> Vec<SelectItem> {
    let cols = match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        _ => return vec![SelectItem::star()],
    };

    let has_hygienic = cols.iter().any(|col| col.needs_hygienic_alias);
    if !has_hygienic {
        return vec![SelectItem::star()];
    }

    cols.iter()
        .filter(|col| !col.needs_hygienic_alias)
        .map(|col| {
            // Use original_name for the SQL column reference (what the inner query produces)
            // and name() for the alias (what the user expects to see)
            let sql_col_name = col.original_name();
            let output_name = col.name();
            let sql_expr = if let Some(qual) = qualifier {
                SqlDomainExpression::Column {
                    name: sql_col_name.to_string(),
                    qualifier: Some(QualifierScope::structural(qual)),
                }
            } else {
                SqlDomainExpression::Column {
                    name: sql_col_name.to_string(),
                    qualifier: None,
                }
            };
            SelectItem::Expression {
                expr: sql_expr,
                alias: Some(output_name.to_string()),
            }
        })
        .collect()
}
