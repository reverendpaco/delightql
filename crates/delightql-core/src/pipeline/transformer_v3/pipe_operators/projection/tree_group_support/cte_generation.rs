// CTE generation for tree groups with nested reductions

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{Cte, SelectBuilder, TableExpression};
use crate::pipeline::transformer_v3::context::TransformContext;
use crate::pipeline::transformer_v3::expression_transformer::transform_domain_expression;
use crate::pipeline::transformer_v3::QualifierScope;

use super::{CteGenerationResult, CteJoinInfo, TreeGroupCteInfo};

/// Generate independent CTEs for all tree groups with requirements
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn generate_all_independent_ctes(
    cte_infos: &[TreeGroupCteInfo],
    reducing_by: &[ast_addressed::DomainExpression],
    reducing_on: &[ast_addressed::DomainExpression],
    base_source: &TableExpression,
    where_clause: Option<&crate::pipeline::sql_ast_v3::DomainExpression>,
    ctx: &TransformContext,
) -> Result<CteGenerationResult> {
    let mut all_ctes = Vec::new();
    let mut cte_joins = Vec::new();

    for cte_info in cte_infos {
        let tree_group_expr = match cte_info.location {
            ast_addressed::TreeGroupLocation::InReducingBy => &reducing_by[cte_info.index],
            ast_addressed::TreeGroupLocation::InReducingOn => &reducing_on[cte_info.index],
        };

        match tree_group_expr {
            ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Curly {
                members,
                ..
            }) => {
                let (nested_members, non_nested) =
                    crate::pipeline::transformer_v3::tree_group_ctes::extract_nested_members(
                        members.clone(),
                    );

                if !nested_members.is_empty() {
                    let cte_name_from_ast = cte_info.requirements.cte_name.get().clone();
                    let mut result =
                        crate::pipeline::transformer_v3::tree_group_ctes::generate_nested_reduction_cte(
                            &cte_info.requirements.accumulated_grouping_keys,
                            nested_members,
                            non_nested,
                            base_source.clone(),
                            where_clause,
                            ctx,
                            cte_name_from_ast,
                        )?;

                    all_ctes.extend(result.take_ctes());
                    cte_joins.push(CteJoinInfo {
                        cte_name: result.cte_name().to_string(),
                        join_keys: cte_info.requirements.join_keys.clone(),
                        accumulated_grouping_keys: cte_info
                            .requirements
                            .accumulated_grouping_keys
                            .iter()
                            .map(|(_, e)| e.clone())
                            .collect(),
                        original_index: cte_info.index,
                        column_aliases: result.column_aliases().to_vec(),
                        location: cte_info.location,
                        grouping_dress_keys: result.grouping_dress_keys().to_vec(),
                    });
                }
            }
            ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::MetadataTreeGroup { constructor, .. },
            ) => {
                let nested_members = vec![(
                    "constructor".to_string(),
                    Box::new(ast_addressed::DomainExpression::Function(
                        *constructor.clone(),
                    )),
                )];

                let cte_name_from_ast = cte_info.requirements.cte_name.get().clone();
                let mut result =
                    crate::pipeline::transformer_v3::tree_group_ctes::generate_nested_reduction_cte(
                        &cte_info.requirements.accumulated_grouping_keys,
                        nested_members,
                        Vec::new(), // No non_nested for metadata TGs
                        base_source.clone(),
                        where_clause,
                        ctx,
                        cte_name_from_ast,
                    )?;

                all_ctes.extend(result.take_ctes());
                cte_joins.push(CteJoinInfo {
                    cte_name: result.cte_name().to_string(),
                    join_keys: cte_info.requirements.join_keys.clone(),
                    accumulated_grouping_keys: cte_info
                        .requirements
                        .accumulated_grouping_keys
                        .iter()
                        .map(|(_, e)| e.clone())
                        .collect(),
                    original_index: cte_info.index,
                    column_aliases: result.column_aliases().to_vec(),
                    location: cte_info.location,
                    grouping_dress_keys: result.grouping_dress_keys().to_vec(),
                });
            }
            other => panic!("catch-all hit in cte_generation.rs generate_ctes_for_tree_groups (DomainExpression): {:?}", other),
        }
    }

    Ok(CteGenerationResult {
        ctes: all_ctes,
        cte_joins,
    })
}
/// Generate wrapper CTEs for each aggregate in reducing_on
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn generate_wrapper_ctes_for_aggregates(
    reducing_on: &[ast_addressed::DomainExpression],
    grouping_keys: &[ast_addressed::DomainExpression],
    base_table: &TableExpression,
    nested_cte_joins: &[CteJoinInfo],
    ctx: &TransformContext,
    source_schema: &ast_addressed::CprSchema,
) -> Result<Vec<Cte>> {
    let mut schema_ctx = crate::pipeline::transformer_v3::SchemaContext::new(source_schema.clone());
    let mut wrapper_ctes = Vec::new();

    for (agg_idx, agg_expr) in reducing_on.iter().enumerate() {
        let wrapper_cte_name = format!("agg_{}", agg_idx);

        let needed_ctes: Vec<_> = nested_cte_joins
            .iter()
            .filter(|cte_join| {
                cte_join.location == ast_addressed::TreeGroupLocation::InReducingOn
                    && cte_join.original_index == agg_idx
            })
            .collect();

        let mut from_table = if needed_ctes.len() == 1 {
            TableExpression::table(&needed_ctes[0].cte_name)
        } else {
            base_table.clone()
        };

        let ctes_to_join = if needed_ctes.len() == 1 {
            Vec::new()
        } else {
            needed_ctes.clone()
        };

        for cte_join in &ctes_to_join {
            let join_condition = if cte_join.join_keys.is_empty() {
                crate::pipeline::sql_ast_v3::DomainExpression::literal(
                    ast_addressed::LiteralValue::Boolean(true),
                )
            } else {
                let conditions: Vec<_> = cte_join
                    .join_keys
                    .iter()
                    .map(|key| {
                        let sql_key =
                            transform_domain_expression(key.clone(), ctx, &mut schema_ctx)?;
                        let base_key = match &sql_key {
                            crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                name, ..
                            } => crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                name: name.clone(),
                                qualifier: Some(QualifierScope::structural(get_table_name(
                                    base_table,
                                ))),
                            },
                            _ => sql_key.clone(),
                        };
                        let cte_key = match &sql_key {
                            crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                name, ..
                            } => crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                name: name.clone(),
                                qualifier: Some(QualifierScope::structural(
                                    cte_join.cte_name.clone(),
                                )),
                            },
                            _ => sql_key.clone(),
                        };
                        Ok(crate::pipeline::sql_ast_v3::DomainExpression::eq(
                            base_key, cte_key,
                        ))
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

        let mut select_builder = SelectBuilder::new();
        let source_table_name = get_table_name(&from_table);
        let has_joins = !ctes_to_join.is_empty();

        for group_key in grouping_keys {
            let sql_key = transform_domain_expression(group_key.clone(), ctx, &mut schema_ctx)?;
            let qualified_key = if has_joins {
                match sql_key {
                    crate::pipeline::sql_ast_v3::DomainExpression::Column { name, .. } => {
                        crate::pipeline::sql_ast_v3::DomainExpression::Column {
                            name,
                            qualifier: Some(QualifierScope::structural(source_table_name.clone())),
                        }
                    }
                    other => other,
                }
            } else {
                // Reading from a CTE — drop stale qualifiers from the original AST
                sql_key.unqualified()
            };
            select_builder = select_builder.select(
                crate::pipeline::sql_ast_v3::SelectItem::expression(qualified_key),
            );
        }

        log::debug!(
            "generate_wrapper_ctes_for_aggregates: agg_idx={}, needed_ctes.len()={}",
            agg_idx,
            needed_ctes.len()
        );
        if needed_ctes.len() == 1 {
            log::debug!(
                "  cte_name={}, column_aliases={:?}, grouping_dress_keys={:?}",
                needed_ctes[0].cte_name,
                needed_ctes[0].column_aliases,
                needed_ctes[0].grouping_dress_keys
            );
        }

        let transformed_expr = if needed_ctes.len() == 1 {
            super::expression_rewriting::replace_nested_reductions_with_cte_columns(
                agg_expr.clone(),
                &needed_ctes[0].cte_name,
                &needed_ctes[0].column_aliases,
                &needed_ctes[0].grouping_dress_keys,
            )?
        } else {
            agg_expr.clone()
        };

        let agg_ctx = ctx.set_aggregate(true);
        let sql_agg = transform_domain_expression(transformed_expr, &agg_ctx, &mut schema_ctx)?;
        select_builder = select_builder.select(
            crate::pipeline::sql_ast_v3::SelectItem::expression_with_alias(sql_agg, "result"),
        );
        select_builder = select_builder.from_tables(vec![from_table.clone()]);

        let group_by_exprs: Vec<_> = grouping_keys
            .iter()
            .map(|key| {
                let sql_key = transform_domain_expression(key.clone(), ctx, &mut schema_ctx)?;
                if has_joins {
                    Ok(match sql_key {
                        crate::pipeline::sql_ast_v3::DomainExpression::Column { name, .. } => {
                            crate::pipeline::sql_ast_v3::DomainExpression::Column {
                                name,
                                qualifier: Some(QualifierScope::structural(
                                    source_table_name.clone(),
                                )),
                            }
                        }
                        other => other,
                    })
                } else {
                    // Reading from a CTE — drop stale qualifiers from the original AST
                    Ok(sql_key.unqualified())
                }
            })
            .collect::<Result<Vec<_>>>()?;

        if !group_by_exprs.is_empty() {
            select_builder = select_builder.group_by(group_by_exprs);
        }

        let select_stmt =
            select_builder
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: format!("Failed to build wrapper CTE: {}", e),
                    source: None,
                    subcategory: None,
                })?;
        let cte = Cte::new(
            wrapper_cte_name,
            crate::pipeline::sql_ast_v3::QueryExpression::Select(Box::new(select_stmt)),
        );
        wrapper_ctes.push(cte);
    }

    Ok(wrapper_ctes)
}

fn get_table_name(table: &TableExpression) -> String {
    match table {
        TableExpression::Table { alias: Some(a), .. } => a.clone(),
        TableExpression::Table { name, .. } => name.clone(),
        TableExpression::Subquery { alias, .. } => alias.clone(),
        _ => "base".to_string(),
    }
}
