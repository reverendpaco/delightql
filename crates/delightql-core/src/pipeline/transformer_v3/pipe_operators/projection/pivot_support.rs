// Pivot support: generates json_group_object CTE + json_extract SELECT pattern
//
// Simple case (value columns are plain column references):
//   WITH _prepivot AS (
//     SELECT name,
//       json_group_object(subject, json_object('score', score)) AS _pivot_packet
//     FROM <source> WHERE <filter> GROUP BY name
//   )
//   SELECT name,
//     json_extract(_pivot_packet, '$.Maths.score') AS maths, ...
//   FROM _prepivot
//
// Aggregate case (value columns contain aggregate functions like sum:(total)):
//   WITH _preagg AS (
//     SELECT user_id, status, sum(total) AS _pivot_val_0_0
//     FROM <source> WHERE <filter> GROUP BY user_id, status
//   ),
//   _prepivot AS (
//     SELECT user_id,
//       json_group_object(status, json_object('value', _pivot_val_0_0)) AS _pivot_packet
//     FROM _preagg GROUP BY user_id
//   )
//   SELECT user_id,
//     json_extract(_pivot_packet, '$.completed.value') AS completed, ...
//   FROM _prepivot

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::DomainExpression as SqlExpr;
use crate::pipeline::sql_ast_v3::{
    Cte, QueryExpression, SelectBuilder, SelectItem, SelectStatement, TableExpression,
};

use super::super::super::context::TransformContext;
use super::super::super::expression_transformer::transform_domain_expression;
use super::super::super::schema_context::SchemaContext;

/// A group of PivotOf expressions sharing the same pivot key column.
struct PivotGroup {
    pivot_key_expr: ast_addressed::DomainExpression,
    /// (value_column_name, value_column_ast_expr) pairs
    value_columns: Vec<(String, ast_addressed::DomainExpression)>,
    _pivot_values: Vec<String>,
}

/// Handle GROUP BY with pivot expressions via a JSON-based CTE pattern.
pub fn apply_pivot_modulo(
    builder: SelectBuilder,
    _reducing_by: Vec<ast_addressed::DomainExpression>,
    reducing_on: Vec<ast_addressed::DomainExpression>,
    _arbitrary: Vec<ast_addressed::DomainExpression>,
    actual_grouping_keys: Vec<ast_addressed::DomainExpression>,
    source_schema: &ast_addressed::CprSchema,
    cpr_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<SelectStatement> {
    let _schema_ctx = SchemaContext::new(source_schema.clone());

    // Extract FROM and WHERE from the incoming builder
    let from_source = builder
        .get_from()
        .and_then(|tables| tables.first())
        .cloned()
        .unwrap_or_else(|| TableExpression::table("unknown"));
    let where_clause = builder.get_where_clause().cloned();

    // Transform grouping keys to SQL expressions
    let mut schema_ctx = SchemaContext::new(source_schema.clone());
    let group_by_sql: Vec<SqlExpr> = actual_grouping_keys
        .iter()
        .map(|e| transform_domain_expression(e.clone(), ctx, &mut schema_ctx))
        .collect::<Result<Vec<_>>>()?;

    // === Classify reducing_on into regular aggregates and pivot groups ===

    let agg_ctx = ctx.set_aggregate(true);
    let mut agg_aliases = Vec::new();
    let mut agg_idx = 0;

    // Regular aggregate SQL expressions (for CTE)
    let mut regular_agg_items: Vec<(String, SqlExpr)> = Vec::new();

    let mut pivot_groups: Vec<PivotGroup> = Vec::new();
    let mut key_to_group: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for expr in &reducing_on {
        match expr {
            ast_addressed::DomainExpression::PivotOf {
                value_column,
                pivot_key,
                pivot_values,
            } => {
                let key_name =
                    extract_lvar_name(pivot_key).unwrap_or_else(|| "pivot_key".to_string());
                let val_name =
                    extract_lvar_name(value_column).unwrap_or_else(|| "value".to_string());

                if let Some(&group_idx) = key_to_group.get(&key_name) {
                    pivot_groups[group_idx]
                        .value_columns
                        .push((val_name, value_column.as_ref().clone()));
                } else {
                    let group_idx = pivot_groups.len();
                    key_to_group.insert(key_name, group_idx);
                    pivot_groups.push(PivotGroup {
                        pivot_key_expr: pivot_key.as_ref().clone(),
                        value_columns: vec![(val_name, value_column.as_ref().clone())],
                        _pivot_values: pivot_values.clone(),
                    });
                }
            }
            other => {
                let alias = format!("_agg_{}", agg_idx);
                let sql_expr =
                    transform_domain_expression(other.clone(), &agg_ctx, &mut schema_ctx)?;
                regular_agg_items.push((alias.clone(), sql_expr));
                agg_aliases.push(alias);
                agg_idx += 1;
            }
        }
    }

    // Check if any pivot value column contains an aggregate function.
    // If so, we need a pre-aggregation CTE to avoid nesting aggregates.
    let needs_preagg = pivot_groups.iter().any(|g| {
        g.value_columns
            .iter()
            .any(|(_, expr)| matches!(expr, ast_addressed::DomainExpression::Function(_)))
    });

    // === Build CTE(s) ===

    // Determine the effective source and value expressions for _prepivot.
    // With preagg: source is _preagg, value exprs are alias references.
    // Without: source is original FROM, value exprs are raw column references.

    // Per-group, per-value-column: the SQL expression to use inside json_object in _prepivot
    let mut pivot_val_exprs: Vec<Vec<SqlExpr>> = Vec::new();
    let prepivot_from: TableExpression;
    let prepivot_where: Option<crate::pipeline::sql_ast_v3::DomainExpression>;

    if needs_preagg {
        // Build _preagg CTE
        let mut preagg_items = Vec::new();

        // Group keys
        for sql_key in &group_by_sql {
            preagg_items.push(SelectItem::expression(sql_key.clone()));
        }

        // Pivot keys (one per group)
        let mut pivot_key_sqls = Vec::new();
        for group in &pivot_groups {
            let key_sql =
                transform_domain_expression(group.pivot_key_expr.clone(), ctx, &mut schema_ctx)?;
            preagg_items.push(SelectItem::expression(key_sql.clone()));
            pivot_key_sqls.push(key_sql);
        }

        // Value columns (transformed with aggregate context)
        for (group_idx, group) in pivot_groups.iter().enumerate() {
            let mut group_exprs = Vec::new();
            for (col_idx, (_val_name, val_expr)) in group.value_columns.iter().enumerate() {
                let alias = format!("_pivot_val_{}_{}", group_idx, col_idx);
                let sql_expr =
                    transform_domain_expression(val_expr.clone(), &agg_ctx, &mut schema_ctx)?;
                preagg_items.push(SelectItem::expression_with_alias(sql_expr, &alias));
                group_exprs.push(SqlExpr::column(&alias));
            }
            pivot_val_exprs.push(group_exprs);
        }

        // GROUP BY: group_keys + pivot_keys
        let mut preagg_group_by = group_by_sql.clone();
        preagg_group_by.extend(pivot_key_sqls);

        let mut preagg_builder = SelectBuilder::new()
            .set_select(preagg_items)
            .from_tables(vec![from_source]);

        if let Some(w) = where_clause {
            preagg_builder = preagg_builder.where_clause(w);
        }
        if !preagg_group_by.is_empty() {
            preagg_builder = preagg_builder.group_by(preagg_group_by);
        }

        let preagg_stmt =
            preagg_builder
                .build()
                .map_err(|e| crate::error::DelightQLError::ParseError {
                    message: e,
                    source: None,
                    subcategory: None,
                })?;

        ctx.generated_ctes.borrow_mut().push(Cte::new(
            "_preagg",
            QueryExpression::Select(Box::new(preagg_stmt)),
        ));

        prepivot_from = TableExpression::table("_preagg");
        prepivot_where = None; // Already filtered in _preagg
    } else {
        // No pre-aggregation needed. Transform value columns directly.
        for group in &pivot_groups {
            let mut group_exprs = Vec::new();
            for (_val_name, val_expr) in &group.value_columns {
                group_exprs.push(transform_domain_expression(
                    val_expr.clone(),
                    ctx,
                    &mut schema_ctx,
                )?);
            }
            pivot_val_exprs.push(group_exprs);
        }

        prepivot_from = from_source;
        prepivot_where = where_clause;
    }

    // === Build _prepivot CTE ===

    let mut cte_select_items = Vec::new();

    // Group keys
    if needs_preagg {
        // Reference group key columns by name (from _preagg)
        for sql_key in &group_by_sql {
            let col_name = match sql_key {
                SqlExpr::Column { name, .. } => name.clone(),
                _ => continue,
            };
            cte_select_items.push(SelectItem::expression(SqlExpr::column(&col_name)));
        }
    } else {
        for sql_key in &group_by_sql {
            cte_select_items.push(SelectItem::expression(sql_key.clone()));
        }
    }

    // Regular aggregates (only in non-preagg path; preagg+regular aggs is deferred)
    if !needs_preagg {
        for (alias, sql_expr) in &regular_agg_items {
            cte_select_items.push(SelectItem::expression_with_alias(sql_expr.clone(), alias));
        }
    }

    // json_group_object per pivot group
    let mut packet_aliases = Vec::new();
    for (group_idx, group) in pivot_groups.iter().enumerate() {
        let packet_alias = if pivot_groups.len() == 1 {
            "_pivot_packet".to_string()
        } else {
            format!("_pivot_packet_{}", group_idx)
        };

        // json_object('col_name', val_expr, ...)
        let mut json_obj_args = Vec::new();
        for (col_idx, (val_name, _)) in group.value_columns.iter().enumerate() {
            json_obj_args.push(SqlExpr::Literal(ast_addressed::LiteralValue::String(
                val_name.clone(),
            )));
            json_obj_args.push(pivot_val_exprs[group_idx][col_idx].clone());
        }
        let json_obj = SqlExpr::function("json_object", json_obj_args);

        // json_group_object(pivot_key, json_object(...))
        let key_sql = if needs_preagg {
            // Reference pivot key by column name from _preagg
            let key_name =
                extract_lvar_name(&group.pivot_key_expr).unwrap_or_else(|| "pivot_key".to_string());
            SqlExpr::column(&key_name)
        } else {
            transform_domain_expression(group.pivot_key_expr.clone(), ctx, &mut schema_ctx)?
        };
        let json_group = SqlExpr::function("json_group_object", vec![key_sql, json_obj]);

        cte_select_items.push(SelectItem::expression_with_alias(json_group, &packet_alias));
        packet_aliases.push(packet_alias);
    }

    // Assemble _prepivot CTE
    let mut prepivot_builder = SelectBuilder::new()
        .set_select(cte_select_items)
        .from_tables(vec![prepivot_from]);

    if let Some(w) = prepivot_where {
        prepivot_builder = prepivot_builder.where_clause(w);
    }

    // GROUP BY: use column names (works for both preagg and direct paths)
    let prepivot_group_by: Vec<SqlExpr> = if needs_preagg {
        group_by_sql
            .iter()
            .filter_map(|e| match e {
                SqlExpr::Column { name, .. } => Some(SqlExpr::column(name)),
                other => panic!(
                    "catch-all hit in pivot_support.rs prepivot_group_by (SqlExpr): {:?}",
                    other
                ),
            })
            .collect()
    } else {
        group_by_sql.clone()
    };

    if !prepivot_group_by.is_empty() {
        prepivot_builder = prepivot_builder.group_by(prepivot_group_by);
    }

    let prepivot_stmt =
        prepivot_builder
            .build()
            .map_err(|e| crate::error::DelightQLError::ParseError {
                message: e,
                source: None,
                subcategory: None,
            })?;

    ctx.generated_ctes.borrow_mut().push(Cte::new(
        "_prepivot",
        QueryExpression::Select(Box::new(prepivot_stmt)),
    ));

    // === Build outer SELECT from _prepivot ===

    let columns = match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        other => panic!(
            "catch-all hit in pivot_support.rs apply_pivot_projection (CprSchema columns): {:?}",
            other
        ),
    };
    let mut col_idx = 0;
    let mut outer_items = Vec::new();

    // Group keys
    for sql_key in &group_by_sql {
        let col_name = match sql_key {
            SqlExpr::Column { name, .. } => name.clone(),
            _ => format!("key_{}", col_idx),
        };
        let alias = columns
            .get(col_idx)
            .map(|c| c.name().to_string())
            .unwrap_or_else(|| col_name.clone());

        outer_items.push(SelectItem::expression_with_alias(
            SqlExpr::column(&col_name),
            &alias,
        ));
        col_idx += 1;
    }

    // Reducing_on items in original order
    let mut agg_idx = 0;
    for expr in &reducing_on {
        match expr {
            ast_addressed::DomainExpression::PivotOf {
                value_column,
                pivot_key,
                pivot_values,
            } => {
                let key_name =
                    extract_lvar_name(pivot_key).unwrap_or_else(|| "pivot_key".to_string());
                let val_name =
                    extract_lvar_name(value_column).unwrap_or_else(|| "value".to_string());
                let group_idx = key_to_group[&key_name];
                let packet_alias = &packet_aliases[group_idx];

                for pivot_value in pivot_values {
                    let alias = columns
                        .get(col_idx)
                        .map(|c| c.name().to_string())
                        .unwrap_or_else(|| pivot_value.to_lowercase());

                    let path = format!("$.{}.{}", pivot_value, val_name);
                    let extract = SqlExpr::function(
                        "json_extract",
                        vec![
                            SqlExpr::column(packet_alias),
                            SqlExpr::Literal(ast_addressed::LiteralValue::String(path)),
                        ],
                    );

                    outer_items.push(SelectItem::expression_with_alias(extract, &alias));
                    col_idx += 1;
                }
            }
            _ => {
                let agg_alias = &agg_aliases[agg_idx];
                let alias = columns
                    .get(col_idx)
                    .map(|c| c.name().to_string())
                    .unwrap_or_else(|| agg_alias.clone());

                outer_items.push(SelectItem::expression_with_alias(
                    SqlExpr::column(agg_alias),
                    &alias,
                ));
                agg_idx += 1;
                col_idx += 1;
            }
        }
    }

    SelectBuilder::new()
        .set_select(outer_items)
        .from_tables(vec![TableExpression::table("_prepivot")])
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}

fn extract_lvar_name(expr: &ast_addressed::DomainExpression) -> Option<String> {
    match expr {
        ast_addressed::DomainExpression::Lvar { name, .. } => Some(name.to_string()),
        ast_addressed::DomainExpression::Parenthesized { inner, .. } => extract_lvar_name(inner),
        ast_addressed::DomainExpression::Literal { .. }
        | ast_addressed::DomainExpression::Function(_)
        | ast_addressed::DomainExpression::Predicate { .. }
        | ast_addressed::DomainExpression::PipedExpression { .. }
        | ast_addressed::DomainExpression::Tuple { .. }
        | ast_addressed::DomainExpression::ScalarSubquery { .. }
        | ast_addressed::DomainExpression::PivotOf { .. }
        | ast_addressed::DomainExpression::Projection(_)
        | ast_addressed::DomainExpression::ValuePlaceholder { .. }
        | ast_addressed::DomainExpression::NonUnifiyingUnderscore => None,
        ast_addressed::DomainExpression::Substitution(_)
        | ast_addressed::DomainExpression::ColumnOrdinal(_) => {
            unreachable!("Substitution/ColumnOrdinal should not survive to Addressed phase")
        }
    }
}
