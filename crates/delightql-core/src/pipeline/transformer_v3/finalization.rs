// Query finalization utilities for transformer_v3
//
// This module handles the final processing steps for SQL queries, particularly
// managing "hygienic" columns that are used internally for correlation tracking
// but should not appear in final query output.
//
// Hygienic columns follow naming patterns:
// - __dql_corr_*: Correlation tracking columns
// - __dql_literal_*: Literal value columns
// - __dql_anon_*: Anonymous/temporary columns

use crate::error::Result;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, QueryExpression, SelectBuilder, SelectItem, TableExpression,
};

use super::QualifierScope;

/// Hide hygienic columns from final query output
///
/// Wraps the query if hygienic columns (__dql_corr_*, __dql_literal_*, __dql_anon_*)
/// are present in the SELECT list. Instead of wrapping in a subquery, this function
/// modifies the SELECT list in-place to exclude hygienic columns.
///
/// # Arguments
/// * `query` - The query to process
///
/// # Returns
/// A modified query with hygienic columns removed from the output
pub(crate) fn hide_hygienic_columns_from_output(query: QueryExpression) -> Result<QueryExpression> {
    // Extract hygienic column names from the query
    let hygienic_columns = collect_hygienic_column_names(&query);

    if hygienic_columns.is_empty() {
        // No hygienic columns - return query as-is
        return Ok(query);
    }

    // Extract the inner SELECT statement
    let inner_stmt = match query {
        QueryExpression::Select(boxed_select) => *boxed_select,
        other => {
            // Can't wrap non-SELECT queries (shouldn't happen with hygienic columns)
            return Ok(other);
        }
    };

    // Build SELECT list excluding hygienic columns
    let select_items = inner_stmt
        .select_list()
        .iter()
        .flat_map(|item| match item {
            SelectItem::Expression { expr, alias } => {
                // Check if this is a hygienic column
                let col_name = alias.as_ref().or({
                    if let DomainExpression::Column { name, .. } = expr {
                        Some(name)
                    } else {
                        None
                    }
                });

                if let Some(name) = col_name {
                    if hygienic_columns.contains(name) {
                        return vec![]; // Exclude hygienic column
                    }
                }
                vec![item.clone()]
            }
            SelectItem::Star => {
                // For SELECT *, we need to expand to explicit columns from the wrapper
                // Use wrapper.* to get all columns, but we can't filter in SQL
                // Instead, we'll reference each table individually and exclude hygienic subqueries

                // Collect all table aliases from FROM clause and build qualified stars
                let mut items = vec![];
                if let Some(from_tables) = inner_stmt.from() {
                    for table in from_tables {
                        collect_qualified_stars_excluding_hygienic(
                            table,
                            &hygienic_columns,
                            &mut items,
                        );
                    }
                }

                // If we couldn't expand, just keep the * (will include hygienic columns)
                if items.is_empty() {
                    vec![item.clone()]
                } else {
                    items
                }
            }
            other => vec![other.clone()],
        })
        .collect::<Vec<_>>();

    // ACTUALLY - just don't wrap. Instead, modify the inner SELECT list to exclude hygienic columns
    // Rebuild the inner statement with filtered select list
    if select_items.is_empty() {
        // All columns were hygienic? This shouldn't happen
        return Ok(QueryExpression::Select(Box::new(inner_stmt)));
    }

    let mut builder = SelectBuilder::new()
        .select_all(select_items)
        .from_tables(inner_stmt.from().map(|f| f.to_vec()).unwrap_or_default());

    if let Some(where_clause) = inner_stmt.where_clause() {
        builder = builder.where_clause(where_clause.clone());
    }

    if let Some(group_by) = inner_stmt.group_by() {
        builder = builder.group_by(group_by.to_vec());
    }

    if let Some(having) = inner_stmt.having() {
        builder = builder.having(having.clone());
    }

    if let Some(order_by) = inner_stmt.order_by() {
        for term in order_by {
            builder = builder.order_by(term.clone());
        }
    }

    if let Some(limit) = inner_stmt.limit() {
        if let Some(offset) = limit.offset() {
            builder = builder.limit_offset(limit.count(), offset);
        } else {
            builder = builder.limit(limit.count());
        }
    }

    let modified_stmt = builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("Failed to rebuild SELECT excluding hygienic columns: {}", e),
            source: None,
            subcategory: None,
        })?;

    Ok(QueryExpression::Select(Box::new(modified_stmt)))
}

/// Collect all hygienic column names referenced in the query
///
/// Searches through SELECT lists, FROM clauses, WHERE clauses, and HAVING clauses
/// to find all columns matching hygienic naming patterns.
fn collect_hygienic_column_names(query: &QueryExpression) -> std::collections::HashSet<String> {
    let mut hygienic_cols = std::collections::HashSet::new();

    match query {
        QueryExpression::Select(stmt) => {
            // Check SELECT list
            for item in stmt.select_list() {
                if let SelectItem::Expression { expr, alias } = item {
                    // Check both the expression and the alias
                    if let Some(name) = alias {
                        if name.starts_with("__dql_corr_")
                            || name.starts_with("__dql_literal_")
                            || name.starts_with("__dql_anon_")
                        {
                            hygienic_cols.insert(name.clone());
                        }
                    }
                    collect_hygienic_from_domain_expr(expr, &mut hygienic_cols);
                }
            }

            // Check FROM clause for JOINs
            if let Some(from_tables) = stmt.from() {
                for table in from_tables {
                    collect_hygienic_from_table(table, &mut hygienic_cols);
                }
            }

            // Check WHERE clause
            if let Some(where_expr) = stmt.where_clause() {
                collect_hygienic_from_domain_expr(where_expr, &mut hygienic_cols);
            }

            // Check HAVING clause
            if let Some(having) = stmt.having() {
                collect_hygienic_from_domain_expr(having, &mut hygienic_cols);
            }
        }
        QueryExpression::SetOperation { left, right, .. } => {
            hygienic_cols.extend(collect_hygienic_column_names(left));
            hygienic_cols.extend(collect_hygienic_column_names(right));
        }
        // Values: inline data rows, no hygienic columns.
        QueryExpression::Values { .. } => {}
        // WithCte: recurse into inner query (CTEs could contain hygienic columns).
        QueryExpression::WithCte { query, ctes, .. } => {
            hygienic_cols.extend(collect_hygienic_column_names(query));
            for cte in ctes {
                hygienic_cols.extend(collect_hygienic_column_names(cte.query()));
            }
        }
    }

    hygienic_cols
}

/// Collect hygienic column references from a DomainExpression
fn collect_hygienic_from_domain_expr(
    expr: &DomainExpression,
    hygienic_cols: &mut std::collections::HashSet<String>,
) {
    match expr {
        DomainExpression::Column { name, .. } => {
            if name.starts_with("__dql_corr_") {
                hygienic_cols.insert(name.clone());
            }
        }
        DomainExpression::Binary { left, right, .. } => {
            collect_hygienic_from_domain_expr(left, hygienic_cols);
            collect_hygienic_from_domain_expr(right, hygienic_cols);
        }
        DomainExpression::Unary { expr, .. } => {
            collect_hygienic_from_domain_expr(expr, hygienic_cols);
        }
        DomainExpression::Function { args, .. } => {
            for arg in args {
                collect_hygienic_from_domain_expr(arg, hygienic_cols);
            }
        }
        DomainExpression::Parens(inner) => {
            collect_hygienic_from_domain_expr(inner, hygienic_cols);
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            if let Some(e) = expr {
                collect_hygienic_from_domain_expr(e, hygienic_cols);
            }
            for when_clause in when_clauses {
                collect_hygienic_from_domain_expr(when_clause.when(), hygienic_cols);
                collect_hygienic_from_domain_expr(when_clause.then(), hygienic_cols);
            }
            if let Some(e) = else_clause {
                collect_hygienic_from_domain_expr(e, hygienic_cols);
            }
        }
        DomainExpression::InList { expr, values, .. } => {
            collect_hygienic_from_domain_expr(expr, hygienic_cols);
            for val in values {
                collect_hygienic_from_domain_expr(val, hygienic_cols);
            }
        }
        DomainExpression::Exists { query, .. } => {
            hygienic_cols.extend(collect_hygienic_column_names(query));
        }
        DomainExpression::Subquery(query) => {
            hygienic_cols.extend(collect_hygienic_column_names(query));
        }
        // Leaf expressions: no column references inside.
        DomainExpression::Literal(_) | DomainExpression::Star | DomainExpression::RawSql(_) => {}
        // Window function: walk arguments (could reference hygienic columns).
        DomainExpression::WindowFunction { args, .. } => {
            for arg in args {
                collect_hygienic_from_domain_expr(arg, hygienic_cols);
            }
        }
        // IN subquery: recurse into the subquery.
        DomainExpression::InSubquery { expr, query, .. } => {
            collect_hygienic_from_domain_expr(expr, hygienic_cols);
            hygienic_cols.extend(collect_hygienic_column_names(query));
        }
    }
}

/// Collect hygienic column references from a TableExpression
#[stacksafe::stacksafe]
fn collect_hygienic_from_table(
    table: &TableExpression,
    hygienic_cols: &mut std::collections::HashSet<String>,
) {
    use crate::pipeline::sql_ast_v3::JoinCondition;

    match table {
        TableExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            collect_hygienic_from_table(left, hygienic_cols);
            collect_hygienic_from_table(right, hygienic_cols);
            if let JoinCondition::On(expr) = join_condition {
                collect_hygienic_from_domain_expr(expr, hygienic_cols);
            }
        }
        TableExpression::Subquery { query, .. } => {
            hygienic_cols.extend(collect_hygienic_column_names(query));
        }
        TableExpression::UnionTable { selects, .. } => {
            for select in selects {
                hygienic_cols.extend(collect_hygienic_column_names(select));
            }
        }
        // Leaf table references: no subqueries, no hygienic columns to collect.
        TableExpression::Table { .. }
        | TableExpression::Values { .. }
        | TableExpression::TVF { .. } => {}
    }
}

/// Collect qualified star items for tables that don't contain hygienic columns
///
/// For a JOIN, this creates SELECT u.*, orders.* but checks each subquery
/// to avoid including hygienic columns in the output.
#[stacksafe::stacksafe]
fn collect_qualified_stars_excluding_hygienic(
    table: &TableExpression,
    hygienic_cols: &std::collections::HashSet<String>,
    items: &mut Vec<SelectItem>,
) {
    match table {
        TableExpression::Table { name, alias, .. } => {
            // Regular table - use qualified star: table.*
            let qualifier = alias.as_ref().unwrap_or(name);
            items.push(SelectItem::QualifiedStar {
                qualifier: qualifier.clone(),
            });
        }
        TableExpression::Subquery { query, alias, .. } => {
            // Check if this subquery contains hygienic columns
            let subquery_hygienic = collect_hygienic_column_names(query);

            if subquery_hygienic.is_empty() {
                // No hygienic columns - use qualified star
                items.push(SelectItem::QualifiedStar {
                    qualifier: alias.clone(),
                });
            } else {
                // Has hygienic columns - expand to explicit columns from subquery, excluding hygienic ones
                if let QueryExpression::Select(stmt) = &***query {
                    for item in stmt.select_list() {
                        match item {
                            SelectItem::Expression {
                                expr,
                                alias: col_alias,
                            } => {
                                let col_name = col_alias.as_ref().or({
                                    if let DomainExpression::Column { name, .. } = expr {
                                        Some(name)
                                    } else {
                                        None
                                    }
                                });

                                if let Some(name) = col_name {
                                    if !hygienic_cols.contains(name) {
                                        // Not hygienic - include with table qualification
                                        items.push(SelectItem::Expression {
                                            expr: DomainExpression::Column {
                                                name: name.clone(),
                                                qualifier: Some(QualifierScope::structural(
                                                    alias.clone(),
                                                )),
                                            },
                                            alias: None,
                                        });
                                    }
                                }
                            }
                            SelectItem::Star | SelectItem::QualifiedStar { .. } => {
                                // SELECT * in subquery with hygienic columns
                                // Use the subquery's alias (don't recurse into internal structure)
                                items.push(SelectItem::QualifiedStar {
                                    qualifier: alias.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        TableExpression::Join { left, right, .. } => {
            // CRITICAL: Process BOTH sides of join to preserve all columns
            // This was the root cause of column loss in multi-way joins
            collect_qualified_stars_excluding_hygienic(left, hygienic_cols, items);
            collect_qualified_stars_excluding_hygienic(right, hygienic_cols, items);
        }
        TableExpression::UnionTable { alias, .. } => {
            // Union table - just use its alias
            items.push(SelectItem::QualifiedStar {
                qualifier: alias.clone(),
            });
        }
        TableExpression::TVF { alias, .. } => {
            // TVF - use alias if present
            if let Some(a) = alias {
                items.push(SelectItem::QualifiedStar {
                    qualifier: a.clone(),
                });
            }
        }
        TableExpression::Values { .. } => {
            // VALUES clause - shouldn't appear in FROM with hygienic columns
            // Just use * (edge case)
        }
    }
}
