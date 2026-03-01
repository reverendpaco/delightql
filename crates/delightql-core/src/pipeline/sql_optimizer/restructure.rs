// restructure.rs - PASS 2: Restructuring optimizations
//
// This module implements restructuring optimizations:
// - Projection flattening (collapsing nested SELECT layers)
// - Predicate pushdown (future)
// - CTE extraction (future)
//
// Refactored to use the visitor pattern from visitor.rs

use crate::error::Result;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement, TableExpression,
};

use super::visitor::{apply_transformer, QueryTransformer};

pub(super) fn pass_restructure(stmt: SqlStatement) -> Result<SqlStatement> {
    let mut transformer = RestructureTransformer;
    apply_transformer(stmt, &mut transformer)
}

/// Transformer for restructuring optimizations
struct RestructureTransformer;

impl QueryTransformer for RestructureTransformer {
    /// Try to flatten projection layers in SELECT statements
    fn transform_select(&mut self, stmt: SelectStatement) -> Result<Option<SelectStatement>> {
        // Check if this SELECT can be flattened with its subquery
        // Pattern: SELECT cols FROM (SELECT cols2 FROM ...) WHERE ...
        if let Some(from) = stmt.from() {
            if from.len() == 1 {
                if let TableExpression::Subquery { query, alias } = &from[0] {
                    // Try to flatten this layer
                    if let Some(flattened) = try_flatten_projection_layer(&stmt, query, alias)? {
                        return Ok(Some(flattened));
                    }
                }
            }
        }

        Ok(None)
    }
}

/// Try to flatten a projection layer if all columns are simple renames
fn try_flatten_projection_layer(
    outer: &SelectStatement,
    inner_query: &QueryExpression,
    _subquery_alias: &str,
) -> Result<Option<SelectStatement>> {
    // Only flatten if inner is a SELECT
    let inner_select = match inner_query {
        QueryExpression::Select(s) => s.as_ref(),
        _ => return Ok(None),
    };

    // Don't flatten if outer has GROUP BY, HAVING, DISTINCT (need to preserve semantics)
    if outer.group_by().is_some() || outer.having().is_some() || outer.is_distinct() {
        return Ok(None);
    }

    // Don't flatten if inner has GROUP BY, HAVING, DISTINCT, ORDER BY, LIMIT
    if inner_select.group_by().is_some()
        || inner_select.having().is_some()
        || inner_select.is_distinct()
        || inner_select.order_by().is_some()
        || inner_select.limit().is_some()
    {
        return Ok(None);
    }

    // Check if outer is SELECT *
    let is_select_star = outer.select_list().len() == 1
        && matches!(
            outer.select_list()[0],
            SelectItem::Star | SelectItem::QualifiedStar { .. }
        );

    // If SELECT *, we can only flatten if there's a WHERE clause to rewrite
    // Otherwise return None (no benefit to flattening)
    if is_select_star {
        if outer.where_clause().is_none() {
            // SELECT * with no WHERE - no benefit to flattening
            return Ok(None);
        }

        // SELECT * with WHERE - try to rewrite the WHERE clause
        return try_flatten_select_star_with_where(outer, inner_select, _subquery_alias);
    }

    // Check if outer has a mixed SELECT list (e.g., SELECT *, col1, col2)
    // This is common in scalar destructuring patterns
    let has_star_in_list = outer
        .select_list()
        .iter()
        .any(|item| matches!(item, SelectItem::Star | SelectItem::QualifiedStar { .. }));

    if has_star_in_list {
        // Mixed SELECT list with star - cannot safely flatten
        // We'd need to expand the star which requires schema information
        log::debug!("  Cannot flatten: outer SELECT has mixed list with SELECT *");
        return Ok(None);
    }

    // Build column mapping from outer SELECT list to inner columns
    // Only proceed if all outer columns are simple column references
    let mut column_mapping: Vec<(String, String)> = Vec::new(); // (outer_name, inner_name)

    for item in outer.select_list() {
        match item {
            SelectItem::Star | SelectItem::QualifiedStar { .. } => {
                // Should have been handled above
                log::error!("Unexpected star in SELECT list - should have been caught earlier");
                return Ok(None);
            }
            SelectItem::Expression { expr, alias } => {
                // Check if this is a simple column reference
                if let DomainExpression::Column {
                    name, qualifier, ..
                } = expr
                {
                    let outer_name = alias.as_ref().unwrap_or(name).clone();

                    // Find this column in inner SELECT list
                    if let Some(inner_col) =
                        find_column_in_select_list(name, qualifier, inner_select.select_list())
                    {
                        column_mapping.push((outer_name, inner_col));
                    } else {
                        // Column not found in inner - can't flatten
                        return Ok(None);
                    }
                } else {
                    // Complex expression - not a simple column reference
                    return Ok(None);
                }
            }
        }
    }

    // Check for column swaps: if column A maps to B and column B maps to A,
    // we cannot safely flatten because the semantic meaning of columns is not preserved
    if has_column_swap(&column_mapping) {
        log::debug!("  Cannot flatten: detected column swap (e.g., id AS age, age AS id)");
        return Ok(None);
    }

    // All checks passed - we can flatten!
    // Build new SELECT with columns traced back to inner's FROM
    let mut builder = SelectStatement::builder();

    // Build new SELECT list using inner column names
    for (outer_alias, inner_col_name) in &column_mapping {
        builder = builder.select(SelectItem::Expression {
            expr: DomainExpression::Column {
                name: inner_col_name.clone(),
                qualifier: None,
            },
            alias: Some(outer_alias.clone()),
        });
    }

    // Use inner's FROM clause
    if let Some(inner_from) = inner_select.from() {
        builder = builder.from_tables(inner_from.to_vec());
    }

    // Merge WHERE clauses
    // Rewrite outer WHERE clause if it exists
    if let Some(outer_where) = outer.where_clause() {
        let available_columns = extract_available_columns(inner_select.select_list());

        match rewrite_where_clause(outer_where, _subquery_alias, &available_columns) {
            Some(rewritten_outer_where) => {
                // Merge with inner WHERE if both exist
                let merged_where = match inner_select.where_clause() {
                    Some(inner_where) => DomainExpression::Binary {
                        left: Box::new(inner_where.clone()),
                        op: crate::pipeline::sql_ast_v3::BinaryOperator::And,
                        right: Box::new(rewritten_outer_where),
                    },
                    None => rewritten_outer_where,
                };
                builder = builder.where_clause(merged_where);
            }
            None => {
                // Failed to rewrite WHERE - can't flatten
                return Ok(None);
            }
        }
    } else if let Some(inner_where) = inner_select.where_clause() {
        // Only inner has WHERE
        builder = builder.where_clause(inner_where.clone());
    }

    // Preserve outer's ORDER BY and LIMIT
    if let Some(order_by) = outer.order_by() {
        for term in order_by {
            builder = builder.order_by(term.clone());
        }
    }

    if let Some(limit) = outer.limit() {
        if let Some(offset) = limit.offset() {
            builder = builder.limit_offset(limit.count(), offset);
        } else {
            builder = builder.limit(limit.count());
        }
    }

    match builder.build() {
        Ok(flattened) => Ok(Some(flattened)),
        Err(_) => Ok(None), // If build fails, don't flatten
    }
}

/// Check if a SELECT list contains column swaps
/// Example: SELECT id AS age, age AS id contains a swap
fn inner_select_has_swaps(select_list: &[SelectItem]) -> bool {
    // Build a mapping of (output_name, source_name) for all simple column references
    let mut mapping: Vec<(String, String)> = Vec::new();

    for item in select_list {
        if let SelectItem::Expression { expr, alias } = item {
            if let DomainExpression::Column { name, .. } = expr {
                let output_name = alias.as_ref().unwrap_or(name).clone();
                mapping.push((output_name, name.clone()));
            }
        }
    }

    has_column_swap(&mapping)
}

/// Detect if there's a column swap or permutation pattern in the mapping
/// Examples:
///   - 2-way swap: (id, age) and (age, id)
///   - 3-way rotation: (id, age), (age, first_name), (first_name, id)
///   - Any permutation where a column's value comes from a different column
fn has_column_swap(mapping: &[(String, String)]) -> bool {
    // A permutation exists if any output column receives its value from
    // a source column with a different name, AND that source column name
    // also appears as an output somewhere in the mapping.
    //
    // For example:
    //   (age, id) - age receives value from id
    //   (id, age) - id receives value from age
    // Both id and age appear as both output and source, forming a cycle.

    for (output, source) in mapping {
        if output != source {
            // This column is being renamed (output != source)
            // Check if the source name appears as an output name anywhere
            if mapping.iter().any(|(out, _src)| out == source) {
                // Found a permutation: source column is also an output
                log::debug!(
                    "  Detected column permutation: {} <- {} (and {} is also an output)",
                    output,
                    source,
                    source
                );
                return true;
            }
        }
    }
    false
}

/// Find a column in the SELECT list and return its source name
fn find_column_in_select_list(
    name: &str,
    _qualifier: &Option<crate::pipeline::sql_ast_v3::ColumnQualifier>,
    select_list: &[SelectItem],
) -> Option<String> {
    for item in select_list {
        if let SelectItem::Expression { expr, alias } = item {
            // Check if this is a simple column reference
            if let DomainExpression::Column { name: col_name, .. } = expr {
                let item_alias = alias.as_ref().unwrap_or(col_name);
                if item_alias == name {
                    // Found it - return the source column name
                    return Some(col_name.clone());
                }
            } else {
                // This is a complex expression with an alias
                if let Some(alias_name) = alias {
                    if alias_name == name {
                        // This is an expression, not a simple column - can't trace back
                        return None;
                    }
                }
            }
        }
    }
    None
}

/// Try to flatten SELECT * FROM (subquery) WHERE ... pattern
/// by rewriting the WHERE clause and merging it with the inner query
fn try_flatten_select_star_with_where(
    outer: &SelectStatement,
    inner_select: &SelectStatement,
    subquery_alias: &str,
) -> Result<Option<SelectStatement>> {
    log::debug!("try_flatten_select_star_with_where: Attempting to flatten SELECT * with WHERE");

    // Check if inner SELECT contains column swaps
    // If it does, we cannot safely merge WHERE clauses because column semantics change
    if inner_select_has_swaps(inner_select.select_list()) {
        log::debug!("  Cannot flatten: inner SELECT contains column swaps");
        return Ok(None);
    }

    // Extract outer WHERE clause (we know it exists from caller)
    let outer_where = match outer.where_clause() {
        Some(w) => w,
        None => {
            log::debug!("  No outer WHERE clause");
            return Ok(None);
        }
    };

    // Build a set of available columns from inner SELECT
    let available_columns = extract_available_columns(inner_select.select_list());

    log::debug!(
        "  Available columns from inner SELECT: {:?}",
        available_columns
    );

    // Rewrite the outer WHERE clause to remove subquery alias qualifiers
    let rewritten_where =
        match rewrite_where_clause(outer_where, subquery_alias, &available_columns) {
            Some(w) => w,
            None => {
                log::debug!("  Failed to rewrite WHERE clause");
                return Ok(None);
            }
        };

    // Build the flattened SELECT
    // Copy everything from inner, but merge WHERE clauses
    let mut builder = SelectStatement::builder();

    // Copy inner's SELECT list
    for item in inner_select.select_list() {
        builder = builder.select(item.clone());
    }

    // Copy inner's FROM clause
    if let Some(inner_from) = inner_select.from() {
        builder = builder.from_tables(inner_from.to_vec());
    }

    // Merge WHERE clauses
    let merged_where = match inner_select.where_clause() {
        Some(inner_where) => {
            // Both have WHERE - AND them together
            DomainExpression::Binary {
                left: Box::new(inner_where.clone()),
                op: crate::pipeline::sql_ast_v3::BinaryOperator::And,
                right: Box::new(rewritten_where),
            }
        }
        None => {
            // Only outer has WHERE
            rewritten_where
        }
    };

    builder = builder.where_clause(merged_where);

    // Preserve outer's ORDER BY and LIMIT
    if let Some(order_by) = outer.order_by() {
        for term in order_by {
            builder = builder.order_by(term.clone());
        }
    }

    if let Some(limit) = outer.limit() {
        if let Some(offset) = limit.offset() {
            builder = builder.limit_offset(limit.count(), offset);
        } else {
            builder = builder.limit(limit.count());
        }
    }

    match builder.build() {
        Ok(flattened) => {
            log::debug!("  Successfully flattened SELECT * with WHERE");
            Ok(Some(flattened))
        }
        Err(_) => {
            log::debug!("  Failed to build flattened SELECT");
            Ok(None)
        }
    }
}

/// Extract all available column names from a SELECT list
fn extract_available_columns(select_list: &[SelectItem]) -> std::collections::HashSet<String> {
    let mut columns = std::collections::HashSet::new();

    for item in select_list {
        match item {
            SelectItem::Expression { expr, alias } => {
                if let DomainExpression::Column { name, .. } = expr {
                    let col_name = alias.as_ref().unwrap_or(name);
                    columns.insert(col_name.clone());
                }
            }
            SelectItem::Star => {
                // Can't determine columns from SELECT *
                // This is a limitation - we'd need schema information
            }
            SelectItem::QualifiedStar { .. } => {
                // Can't determine columns from table.*
            }
        }
    }

    columns
}

/// Rewrite a WHERE clause to remove a specific subquery alias qualifier
/// Returns None if the WHERE clause references columns not in the available set
fn rewrite_where_clause(
    expr: &DomainExpression,
    subquery_alias: &str,
    available_columns: &std::collections::HashSet<String>,
) -> Option<DomainExpression> {
    match expr {
        DomainExpression::Column {
            name, qualifier, ..
        } => {
            // Check if this column references the subquery alias
            let should_rewrite = match qualifier {
                Some(q) => q.table_name() == subquery_alias,
                None => true, // Unqualified column - check if it's available
            };

            if should_rewrite {
                // Check if this column is available in the inner SELECT
                if !available_columns.contains(name) {
                    log::debug!("  Column {} not found in available columns", name);
                    return None;
                }

                // Rewrite to unqualified column
                Some(DomainExpression::Column {
                    name: name.clone(),
                    qualifier: None,
                })
            } else {
                // Keep as-is (different qualifier)
                Some(expr.clone())
            }
        }
        DomainExpression::Binary { left, op, right } => {
            let rewritten_left = rewrite_where_clause(left, subquery_alias, available_columns)?;
            let rewritten_right = rewrite_where_clause(right, subquery_alias, available_columns)?;
            Some(DomainExpression::Binary {
                left: Box::new(rewritten_left),
                op: op.clone(),
                right: Box::new(rewritten_right),
            })
        }
        DomainExpression::Unary {
            op,
            expr: inner_expr,
        } => {
            let rewritten = rewrite_where_clause(inner_expr, subquery_alias, available_columns)?;
            Some(DomainExpression::Unary {
                op: op.clone(),
                expr: Box::new(rewritten),
            })
        }
        DomainExpression::InList {
            expr: inner_expr,
            not,
            values,
        } => {
            let rewritten_expr =
                rewrite_where_clause(inner_expr, subquery_alias, available_columns)?;
            let rewritten_values: Option<Vec<_>> = values
                .iter()
                .map(|v| rewrite_where_clause(v, subquery_alias, available_columns))
                .collect();
            let rewritten_values = rewritten_values?;
            Some(DomainExpression::InList {
                expr: Box::new(rewritten_expr),
                not: *not,
                values: rewritten_values,
            })
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            // Rewrite the CASE expression (if present)
            let rewritten_expr = if let Some(case_expr) = expr {
                Some(Box::new(rewrite_where_clause(
                    case_expr,
                    subquery_alias,
                    available_columns,
                )?))
            } else {
                None
            };

            // Rewrite each WHEN clause
            let rewritten_when: Option<Vec<_>> = when_clauses
                .iter()
                .map(|when_clause| {
                    use crate::pipeline::sql_ast_v3::WhenClause;
                    let rewritten_when = rewrite_where_clause(
                        when_clause.when(),
                        subquery_alias,
                        available_columns,
                    )?;
                    let rewritten_then = rewrite_where_clause(
                        when_clause.then(),
                        subquery_alias,
                        available_columns,
                    )?;
                    Some(WhenClause::new(rewritten_when, rewritten_then))
                })
                .collect();
            let rewritten_when = rewritten_when?;

            let rewritten_else = if let Some(else_expr) = else_clause {
                Some(Box::new(rewrite_where_clause(
                    else_expr,
                    subquery_alias,
                    available_columns,
                )?))
            } else {
                None
            };

            Some(DomainExpression::Case {
                expr: rewritten_expr,
                when_clauses: rewritten_when,
                else_clause: rewritten_else,
            })
        }
        DomainExpression::InSubquery {
            expr: inner_expr,
            not,
            query,
        } => {
            let rewritten_expr =
                rewrite_where_clause(inner_expr, subquery_alias, available_columns)?;
            Some(DomainExpression::InSubquery {
                expr: Box::new(rewritten_expr),
                not: *not,
                query: query.clone(),
            })
        }
        // For other expressions, just clone them (literals, aggregates, etc.)
        other => Some(other.clone()),
    }
}
