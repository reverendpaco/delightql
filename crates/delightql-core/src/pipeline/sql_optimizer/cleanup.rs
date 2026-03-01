// cleanup.rs - PASS 1: Cleanup optimizations
//
// This module implements basic cleanup optimizations:
// - Redundant subquery elimination
// - EXISTS query optimization
//
// Refactored to use the visitor pattern from visitor.rs

use crate::error::Result;
use crate::pipeline::sql_ast_v3::{
    QueryExpression, SelectItem, SelectStatement, SqlStatement, TableExpression,
};

use super::visitor::{apply_transformer, QueryTransformer};

/// Pass 1: Cleanup optimizations
/// - Redundant subquery elimination
pub(super) fn pass_cleanup(stmt: SqlStatement) -> Result<SqlStatement> {
    let mut transformer = CleanupTransformer;
    apply_transformer(stmt, &mut transformer)
}

/// Transformer for cleanup optimizations
struct CleanupTransformer;

impl QueryTransformer for CleanupTransformer {
    /// Transform query expressions - check for redundant SELECT * wrappers
    fn transform_query(&mut self, query: QueryExpression) -> Result<Option<QueryExpression>> {
        match &query {
            QueryExpression::Select(select_stmt) => {
                // Check if this SELECT is a redundant wrapper around a subquery
                if is_redundant_subquery_wrapper(select_stmt) {
                    // Extract and return the inner subquery
                    let inner = extract_inner_subquery(select_stmt)?;
                    Ok(Some(inner))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }
}

/// Check if a SELECT statement is a redundant wrapper around a subquery
///
/// Pattern: SELECT * FROM (subquery) AS alias
/// with NO additional operations (WHERE, GROUP BY, HAVING, ORDER BY, LIMIT)
fn is_redundant_subquery_wrapper(stmt: &SelectStatement) -> bool {
    // Criterion 1: SELECT list must be exactly SELECT *
    let select_list = stmt.select_list();
    if select_list.len() != 1 {
        log::debug!("  Not redundant: select_list.len() = {}", select_list.len());
        return false;
    }
    if !matches!(select_list[0], SelectItem::Star) {
        log::debug!("  Not redundant: SELECT list is not *");
        return false;
    }

    // Criterion 2: FROM clause must have exactly one table
    let Some(from_clause) = stmt.from() else {
        log::debug!("  Not redundant: No FROM clause");
        return false;
    };
    if from_clause.len() != 1 {
        log::debug!("  Not redundant: FROM has {} tables", from_clause.len());
        return false;
    }

    // Criterion 3: That one table must be a subquery or union table
    match &from_clause[0] {
        TableExpression::Subquery { .. } => {
            log::debug!("  FROM table is a Subquery");
        }
        TableExpression::UnionTable { .. } => {
            log::debug!("  FROM table is a UnionTable");
        }
        other => {
            log::debug!(
                "  Not redundant: FROM table is {:?}",
                std::mem::discriminant(other)
            );
            return false;
        }
    }

    // Criteria 4-9: No additional operations
    let result = stmt.where_clause().is_none()
        && stmt.group_by().is_none()
        && stmt.having().is_none()
        && stmt.order_by().is_none()
        && stmt.limit().is_none()
        && !stmt.is_distinct(); // Also check DISTINCT flag

    if !result {
        log::debug!("  Not redundant: Has additional operations");
    } else {
        log::debug!("  FOUND REDUNDANT SUBQUERY!");
    }

    result
}

/// Extract the inner subquery from a redundant wrapper
fn extract_inner_subquery(stmt: &SelectStatement) -> Result<QueryExpression> {
    let from_clause = stmt.from().expect("Already checked FROM exists");

    match &from_clause[0] {
        TableExpression::Subquery { query, alias: _ } => {
            // For MVP, we just return the inner query
            // The alias is discarded - safe because we checked there's no WHERE/JOIN/etc.
            // that could reference it
            Ok((***query).clone())
        }
        TableExpression::UnionTable { selects, alias: _ } => {
            // Convert UnionTable to SetOperation QueryExpression
            // UnionTable contains Vec<QueryExpression>, we need to build nested SetOperation
            if selects.is_empty() {
                return Err(crate::error::DelightQLError::ParseError {
                    message: "Empty UnionTable".to_string(),
                    source: None,
                    subcategory: None,
                });
            }

            if selects.len() == 1 {
                // Single select - just return it
                return Ok(selects[0].clone());
            }

            // Build nested SetOperation (binary tree)
            // [A, B, C] becomes: A UNION ALL (B UNION ALL C)
            let mut result = selects[selects.len() - 1].clone();
            for i in (0..selects.len() - 1).rev() {
                result = QueryExpression::SetOperation {
                    op: crate::pipeline::sql_ast_v3::SetOperator::UnionAll,
                    left: Box::new(selects[i].clone()),
                    right: Box::new(result),
                };
            }
            Ok(result)
        }
        _ => unreachable!("Already checked this is a subquery or union table"),
    }
}
