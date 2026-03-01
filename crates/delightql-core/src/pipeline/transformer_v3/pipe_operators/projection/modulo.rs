// Modulo operator: |> %[...] (DISTINCT or GROUP BY)

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{SelectBuilder, SelectStatement};

use super::super::super::context::TransformContext;
use super::super::super::domain_to_select_item_with_name_and_flag;
use super::super::super::expression_transformer::transform_domain_expression;
use super::super::super::schema_context::SchemaContext;
use super::tree_group_support::*;
use crate::pipeline::transformer_v3::QualifierScope;

/// Handle Modulo operator: |> %[...] (DISTINCT or GROUP BY)
pub fn apply_modulo(
    mut builder: SelectBuilder,
    spec: ast_addressed::ModuloSpec,
    source_schema: &ast_addressed::CprSchema,
    cpr_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<SelectStatement> {
    match spec {
        ast_addressed::ModuloSpec::Columns(cols) => {
            // Create schema context from source schema
            let mut schema_ctx = SchemaContext::new(source_schema.clone());

            // Extract column metadata from CprSchema to get generated names
            let columns = match cpr_schema {
                ast_addressed::CprSchema::Resolved(cols) => cols,
                other => panic!("catch-all hit in modulo.rs apply_modulo_projection (CprSchema columns): {:?}", other),
            };

            let select_items = cols
                .into_iter()
                .enumerate()
                .map(|(idx, expr)| {
                    let col_metadata = columns.get(idx);
                    let generated_name = col_metadata.map(|col| col.name().to_string());
                    let has_user_name = col_metadata.map(|col| col.has_user_name).unwrap_or(false);
                    domain_to_select_item_with_name_and_flag(
                        expr,
                        generated_name,
                        has_user_name,
                        ctx,
                        &mut schema_ctx,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            builder.set_select(select_items).distinct().build()
        }

        ast_addressed::ModuloSpec::GroupBy {
            mut reducing_by,
            mut reducing_on,
            arbitrary,
        } => {
            // Create schema context from source schema
            let mut schema_ctx = SchemaContext::new(source_schema.clone());

            // Step 0: Extract actual grouping keys from reducing_by
            // Tree groups in reducing_by (scalar context) define grouping keys via their non-nested members
            let mut actual_grouping_keys = Vec::new();

            for expr in reducing_by.clone() {
                // Check if this is a tree group
                if crate::pipeline::transformer_v3::tree_group_ctes::has_nested_reductions(&expr) {
                    // Tree group with nested reductions in reducing_by
                    // Extract its non-nested members as grouping keys
                    let keys = crate::pipeline::transformer_v3::tree_group_ctes::extract_grouping_members(&expr);
                    actual_grouping_keys.extend(keys);
                } else {
                    // Not a tree group, or tree group without nesting - use as-is
                    actual_grouping_keys.push(expr);
                }
            }

            // Check for pivot expressions - handle with JSON CTE pattern
            let has_pivot = reducing_on
                .iter()
                .any(|e| matches!(e, ast_addressed::DomainExpression::PivotOf { .. }));
            if has_pivot {
                return super::pivot_support::apply_pivot_modulo(
                    builder,
                    reducing_by,
                    reducing_on,
                    arbitrary,
                    actual_grouping_keys,
                    source_schema,
                    cpr_schema,
                    ctx,
                );
            }

            // Check if resolver populated cte_requirements
            // If yes, generate independent CTEs with JOINs and modify expressions
            let has_cte_requirements = reducing_by.iter().any(|expr| {
                matches!(
                    expr,
                    ast_addressed::DomainExpression::Function(
                        ast_addressed::FunctionExpression::Curly {
                            cte_requirements: Some(_),
                            ..
                        } | ast_addressed::FunctionExpression::MetadataTreeGroup {
                            cte_requirements: Some(_),
                            ..
                        }
                    )
                )
            }) || reducing_on.iter().any(|expr| {
                matches!(
                    expr,
                    ast_addressed::DomainExpression::Function(
                        ast_addressed::FunctionExpression::Curly {
                            cte_requirements: Some(_),
                            ..
                        } | ast_addressed::FunctionExpression::MetadataTreeGroup {
                            cte_requirements: Some(_),
                            ..
                        }
                    )
                )
            });

            let mut used_wrapper_ctes = false;

            if has_cte_requirements {
                // Generate independent CTEs, build JOINs, and modify expressions
                let cte_infos = collect_cte_requirements(&reducing_by, &reducing_on);

                if !cte_infos.is_empty() {
                    // Get FROM source and WHERE clause from builder
                    let from_source = builder.get_from()
                        .and_then(|tables| tables.first())
                        .cloned()
                        .unwrap_or_else(|| {
                            crate::pipeline::sql_ast_v3::TableExpression::table("unknown")
                        });

                    let where_clause = builder.get_where_clause().cloned();

                    // Generate all independent CTEs
                    let cte_result = generate_all_independent_ctes(
                        &cte_infos,
                        &reducing_by,
                        &reducing_on,
                        &from_source,
                        where_clause.as_ref(),
                        ctx,
                    )?;

                    // Add nested CTEs to context
                    for cte in cte_result.ctes {
                        ctx.generated_ctes.borrow_mut().push(cte);
                    }

                    // INDEPENDENT AGGREGATES ARCHITECTURE:
                    // Generate wrapper CTEs for each aggregate in reducing_on
                    // Each wrapper has its own GROUP BY to avoid Cartesian products
                    //
                    // ONLY use wrapper CTEs if reducing_by has no CTE requirements
                    // (wrapper CTE architecture works best when reducing_by is simple)
                    let reducing_by_has_ctes = cte_result.cte_joins.iter()
                        .any(|cte| cte.location == ast_addressed::TreeGroupLocation::InReducingBy);

                    let wrapper_ctes = if !reducing_by_has_ctes {
                        generate_wrapper_ctes_for_aggregates(
                            &reducing_on,
                            &actual_grouping_keys,
                            &from_source,
                            &cte_result.cte_joins,
                            ctx,
                            source_schema,
                        )?
                    } else {
                        Vec::new()  // Don't use wrapper CTEs if reducing_by is complex
                    };

                    // Add wrapper CTEs to context
                    for cte in &wrapper_ctes {
                        ctx.generated_ctes.borrow_mut().push(cte.clone());
                    }

                    // Build final SELECT that joins all wrapper CTEs
                    used_wrapper_ctes = !wrapper_ctes.is_empty();

                    if used_wrapper_ctes {
                        // INDEPENDENT AGGREGATES: Join wrapper CTEs
                        use crate::pipeline::sql_ast_v3::{SelectBuilder, TableExpression};

                        let mut final_select = SelectBuilder::new();

                        // Build FROM: agg_0 JOIN agg_1 JOIN agg_2 ... JOIN nested_ctes_for_reducing_by
                        let mut from_table = TableExpression::table("agg_0");

                        // Join other wrapper CTEs
                        for i in 1..wrapper_ctes.len() {
                            let agg_name = format!("agg_{}", i);

                            // Build USING clause (grouping keys)
                            let join_keys: Vec<_> = actual_grouping_keys.iter()
                                .map(|key| transform_domain_expression(key.clone(), ctx, &mut schema_ctx))
                                .collect::<Result<Vec<_>>>()?;

                            // Build ON: agg_0.key1 = agg_i.key1 AND ...
                            let conditions: Vec<_> = join_keys.iter()
                                .map(|key| {
                                    let key_name = match key {
                                        crate::pipeline::sql_ast_v3::DomainExpression::Column { name, .. } => name.clone(),
                                        _ => return Err(crate::error::DelightQLError::ParseError {
                                            message: "Expected column in grouping key".to_string(),
                                            source: None,
                                            subcategory: None,
                                        }),
                                    };

                                    let left = crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                        name: key_name.clone(),
                                        qualifier: Some(QualifierScope::structural("agg_0")),
                                    };

                                    let right = crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                        name: key_name,
                                        qualifier: Some(QualifierScope::structural(agg_name.clone())),
                                    };

                                    Ok(crate::pipeline::sql_ast_v3::DomainExpression::eq(left, right))
                                })
                                .collect::<Result<Vec<_>>>()?;

                            let on_condition = if conditions.len() == 1 {
                                conditions.into_iter().next().unwrap()
                            } else {
                                crate::pipeline::sql_ast_v3::DomainExpression::and(conditions)
                            };

                            from_table = TableExpression::inner_join(
                                from_table,
                                TableExpression::table(&agg_name),
                                Some(on_condition),
                            );
                        }

                        // Also join CTEs referenced by reducing_by expressions
                        let reducing_by_ctes: Vec<_> = cte_result.cte_joins.iter()
                            .filter(|cte_join| cte_join.location == ast_addressed::TreeGroupLocation::InReducingBy)
                            .collect();

                        for cte_join in reducing_by_ctes {
                            // Build join condition
                            let join_condition = if cte_join.join_keys.is_empty() {
                                crate::pipeline::sql_ast_v3::DomainExpression::literal(
                                    ast_addressed::LiteralValue::Boolean(true)
                                )
                            } else {
                                let conditions: Vec<_> = cte_join.join_keys.iter()
                                    .map(|key| {
                                        let sql_key = transform_domain_expression(key.clone(), ctx, &mut schema_ctx)?;
                                        let key_name = match &sql_key {
                                            crate::pipeline::sql_ast_v3::DomainExpression::Column { name, .. } => name.clone(),
                                            _ => return Err(crate::error::DelightQLError::ParseError {
                                                message: "Expected column in join key".to_string(),
                                                source: None,
                                                subcategory: None,
                                            }),
                                        };

                                        let left = crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                            name: key_name.clone(),
                                            qualifier: Some(QualifierScope::structural("agg_0")),
                                        };

                                        let right = crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                            name: key_name,
                                            qualifier: Some(QualifierScope::structural(cte_join.cte_name.clone())),
                                        };

                                        Ok(crate::pipeline::sql_ast_v3::DomainExpression::eq(left, right))
                                    })
                                    .collect::<Result<Vec<_>>>()?;

                                if conditions.len() == 1 {
                                    conditions.into_iter().next().unwrap()
                                } else {
                                    crate::pipeline::sql_ast_v3::DomainExpression::and(conditions)
                                }
                            };

                            from_table = TableExpression::left_join(
                                from_table,
                                TableExpression::table(&cte_join.cte_name),
                                join_condition,
                            );
                        }

                        final_select = final_select.from_tables(vec![from_table]);

                        // SELECT: Build select items same as old path
                        // Extract column metadata from CprSchema
                        let columns = match cpr_schema {
                            ast_addressed::CprSchema::Resolved(cols) => cols,
                            other => panic!("catch-all hit in modulo.rs apply_modulo_projection wrapper CTE (CprSchema columns): {:?}", other),
                        };

                        let mut col_idx = 0;
                        let mut select_items = Vec::new();

                        // Add grouping columns (reducing_by expressions)
                        // In wrapper CTE mode, reducing_by is always simple (no CTEs)
                        // so we qualify columns with agg_0
                        for key in &actual_grouping_keys {
                            let sql_key = transform_domain_expression(key.clone(), ctx, &mut schema_ctx)?;
                            let qualified = match sql_key {
                                crate::pipeline::sql_ast_v3::DomainExpression::Column { name, .. } => {
                                    crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                        name: name.clone(),
                                        qualifier: Some(QualifierScope::structural("agg_0")),
                                    }
                                }
                                other => other,
                            };

                            let col_metadata = columns.get(col_idx);
                            let generated_name = col_metadata.map(|col| col.name().to_string());

                            let select_item = if let Some(name) = generated_name {
                                crate::pipeline::sql_ast_v3::SelectItem::expression_with_alias(qualified, &name)
                            } else {
                                crate::pipeline::sql_ast_v3::SelectItem::expression(qualified)
                            };
                            select_items.push(select_item);
                            col_idx += 1;
                        }

                        // Add result columns from wrapper CTEs (reducing_on expressions)
                        for i in 0..wrapper_ctes.len() {
                            let agg_name = format!("agg_{}", i);
                            let result_col = crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                name: "result".to_string(),
                                qualifier: Some(QualifierScope::structural(agg_name)),
                            };

                            // Get the generated name from schema
                            let col_metadata = columns.get(col_idx);
                            let generated_name = col_metadata.map(|col| col.name().to_string());

                            let select_item = if let Some(name) = generated_name {
                                crate::pipeline::sql_ast_v3::SelectItem::expression_with_alias(result_col, &name)
                            } else {
                                crate::pipeline::sql_ast_v3::SelectItem::expression(result_col)
                            };
                            select_items.push(select_item);
                            col_idx += 1;
                        }

                        // Add arbitrary columns (after ~?)
                        for expr in arbitrary.clone() {
                            let col_metadata = columns.get(col_idx);
                            let generated_name = col_metadata.map(|col| col.name().to_string());
                            let has_user_name = col_metadata.map(|col| col.has_user_name).unwrap_or(false);
                            select_items.push(domain_to_select_item_with_name_and_flag(
                                expr,
                                generated_name,
                                has_user_name,
                                ctx,
                                &mut schema_ctx,
                            )?);
                            col_idx += 1;
                        }

                        final_select = final_select.set_select(select_items);

                        builder = final_select;

                        // Skip the old SELECT building below - we already built the complete SELECT
                        // Jump to the final builder.build() step
                    } else if !cte_result.cte_joins.is_empty() {
                        // INDUCTIVE ARCHITECTURE DECISION:
                        // Check if any expression needs base table columns
                        //
                        // Need base table if:
                        // 1. reducing_by has columns NOT in last CTE (requires base columns)
                        // 2. Any reducing_on expression is NOT a tree group with CTE (simple aggregate, needs base)
                        //
                        // Don't need base table if:
                        // - All reducing_by columns are in last CTE's join_keys (promoted columns case)
                        // - All reducing_on expressions have CTEs (whole-table aggregation)
                        let last_cte_join_keys = cte_result.cte_joins.last().map(|cte| &cte.join_keys);

                        let reducing_by_needs_base = if let Some(join_keys) = last_cte_join_keys {
                            // Check if all reducing_by expressions match join_keys
                            // If reducing_by == join_keys, they come from CTE
                            reducing_by.len() != join_keys.len() ||
                            !reducing_by.iter().zip(join_keys.iter()).all(|(rb, jk)| {
                                // Compare column names (simple heuristic)
                                match (rb, jk) {
                                    (ast_addressed::DomainExpression::Lvar { name: n1, .. },
                                     ast_addressed::DomainExpression::Lvar { name: n2, .. }) => n1 == n2,
                                    // Non-Lvar pairs (e.g., Curly tree group vs Lvar) don't match
                                    _ => false,
                                }
                            })
                        } else {
                            !reducing_by.is_empty()
                        };

                        let reducing_on_needs_base = reducing_on.iter().any(|expr| {
                            // Expression needs base if it's NOT a tree group with CTE
                            !matches!(
                                expr,
                                ast_addressed::DomainExpression::Function(
                                    ast_addressed::FunctionExpression::Curly {
                                        cte_requirements: Some(_),
                                        ..
                                    } | ast_addressed::FunctionExpression::MetadataTreeGroup {
                                        cte_requirements: Some(_),
                                        ..
                                    }
                                )
                            )
                        });

                        let needs_base_table = reducing_by_needs_base || reducing_on_needs_base;

                        let from_with_joins = build_from_with_joins(
                            builder.get_from().cloned(),
                            &cte_result.cte_joins,
                            needs_base_table,
                            ctx,
                            source_schema,
                        )?;

                        // Update builder with new FROM clause
                        builder = builder.from_tables(vec![from_with_joins.clone()]);

                        // Extract base table qualifier for column references
                        // In the inductive architecture, we ALWAYS use FROM base + JOINs
                        // so base_qualifier is always the leftmost table (the base table)
                        //
                        // Helper function to recursively extract leftmost table from nested JOINs
                        fn extract_leftmost_table(table: &crate::pipeline::sql_ast_v3::TableExpression) -> String {
                            match table {
                                crate::pipeline::sql_ast_v3::TableExpression::Table { alias: Some(a), .. } => a.clone(),
                                crate::pipeline::sql_ast_v3::TableExpression::Table { name, .. } => name.clone(),
                                crate::pipeline::sql_ast_v3::TableExpression::Subquery { alias, .. } => alias.clone(),
                                crate::pipeline::sql_ast_v3::TableExpression::Join { left, .. } => {
                                    // Recursively extract from nested JOIN
                                    extract_leftmost_table(left.as_ref())
                                }
                                _ => "base".to_string(),
                            }
                        }
                        let base_qualifier = extract_leftmost_table(&from_with_joins);

                        // Phase R8: Modify expressions to reference CTE columns
                        modify_expressions_for_ctes(&mut reducing_by, &mut reducing_on, &cte_result.cte_joins)?;

                        // Build set of CTE names to preserve their references
                        let cte_names: std::collections::HashSet<String> = cte_result.cte_joins.iter()
                            .map(|cte| cte.cte_name.clone())
                            .collect();

                        // Qualify actual_grouping_keys with base table name (needed after JOIN)
                        actual_grouping_keys = actual_grouping_keys.into_iter().map(|expr| {
                            qualify_expression_with_table(expr, &base_qualifier)
                        }).collect();

                        // Qualify ALL base table references in reducing_by (needed after JOIN)
                        // Tree groups in reducing_by need their non-CTE columns qualified
                        // Preserve CTE references (don't replace qualifiers that match CTE names)
                        reducing_by = reducing_by.into_iter().map(|expr| {
                            qualification::qualify_base_table_references_inner(expr, &base_qualifier, &cte_names)
                        }).collect();

                        // Qualify ALL base table references in reducing_on (needed after JOIN)
                        // This handles simple tree groups, regular aggregates, and any other expressions
                        // that reference base table columns
                        // Preserve CTE references (don't replace qualifiers that match CTE names)
                        reducing_on = reducing_on.into_iter().map(|expr| {
                            qualification::qualify_base_table_references_inner(expr, &base_qualifier, &cte_names)
                        }).collect();

                        // Qualify WHERE clause (needed after JOIN)
                        // The WHERE clause was built before CTEs were added, so column references
                        // need to be qualified with the base table name to avoid ambiguity
                        if let Some(where_expr) = builder.get_where_clause().cloned() {
                            let qualified_where = qualify_sql_expression(where_expr, &base_qualifier, &cte_names);
                            builder = builder.where_clause(qualified_where);
                        }
                    }
                }
            } else {
                // No CTE requirements, but check if we still need qualification
                // (e.g., metadata tree groups over joins that get wrapped in subqueries)
                if let Some(from_table) = builder.get_from().and_then(|tables| tables.first()) {
                    if let crate::pipeline::sql_ast_v3::TableExpression::Subquery { alias, .. } = from_table {
                        // Subquery FROM means there were joins - qualify all expressions
                        let base_qualifier = alias.clone();

                        actual_grouping_keys = actual_grouping_keys.into_iter().map(|expr| {
                            qualify_expression_with_table(expr, &base_qualifier)
                        }).collect();

                        reducing_on = reducing_on.into_iter().map(|expr| {
                            qualify_base_table_references(expr, &base_qualifier)
                        }).collect();
                    }
                }
            }

            // Only build SELECT items the old way if we didn't use wrapper CTEs
            if !used_wrapper_ctes {
                let mut select_items = Vec::new();

            // Extract column metadata from CprSchema to get generated names
            let columns = match cpr_schema {
                ast_addressed::CprSchema::Resolved(cols) => cols,
                other => panic!("catch-all hit in modulo.rs apply_modulo_projection basic aggregation (CprSchema columns): {:?}", other),
            };

            let mut col_idx = 0;

            // Add grouping columns
            // Use reducing_by (which may contain modified tree groups with JSON construction)
            // Tree groups in reducing_by will be transformed to JSON_OBJECT in SELECT
            // but GROUP BY uses actual_grouping_keys (extracted identifiers)
            for expr in reducing_by.clone() {
                let col_metadata = columns.get(col_idx);
                let generated_name = col_metadata.map(|col| col.name().to_string());
                let has_user_name = col_metadata.map(|col| col.has_user_name).unwrap_or(false);
                select_items.push(domain_to_select_item_with_name_and_flag(
                    expr,
                    generated_name,
                    has_user_name,
                    ctx,
                    &mut schema_ctx,
                )?);
                col_idx += 1;
            }

            // Add aggregations (set aggregate context for tree groups)
            let agg_ctx = ctx.set_aggregate(true);
            for expr in reducing_on {
                let col_metadata = columns.get(col_idx);
                let generated_name = col_metadata.map(|col| col.name().to_string());
                let has_user_name = col_metadata.map(|col| col.has_user_name).unwrap_or(false);
                select_items.push(domain_to_select_item_with_name_and_flag(
                    expr,
                    generated_name,
                    has_user_name,
                    &agg_ctx,
                    &mut schema_ctx,
                )?);
                col_idx += 1;
            }

            // Add arbitrary columns (after ~?)
            for expr in arbitrary {
                let col_metadata = columns.get(col_idx);
                let generated_name = col_metadata.map(|col| col.name().to_string());
                let has_user_name = col_metadata.map(|col| col.has_user_name).unwrap_or(false);
                select_items.push(domain_to_select_item_with_name_and_flag(
                    expr,
                    generated_name,
                    has_user_name,
                    ctx,
                    &mut schema_ctx,
                )?);
                col_idx += 1;
            }

            // Build GROUP BY clause
            // Use actual_grouping_keys (extracted from reducing_by tree groups)
            // Tree groups in reducing_by: non-nested members become GROUP BY keys
            // Example: %({country, "people": ~> {...}} ~> ...)
            //   - Groups by: country (extracted from tree)
            //   - Not by: the whole JSON object
            let group_by_exprs = actual_grouping_keys
                .into_iter()
                .map(|expr| transform_domain_expression(expr, ctx, &mut schema_ctx))
                .collect::<Result<Vec<_>>>()?;

            if !group_by_exprs.is_empty() {
                builder
                    .set_select(select_items)
                    .group_by(group_by_exprs)
                    .build()
            } else {
                builder.set_select(select_items).build()
            }
            } else {
                // Used wrapper CTEs - builder is already complete
                builder.build()
            }
        }
    }
    .map_err(|e| crate::error::DelightQLError::ParseError {
        message: e,
        source: None,
        subcategory: None,
    })
}
