// visitor.rs - Generic AST visitor pattern for SQL optimizer passes
//
// This module provides a reusable visitor pattern that eliminates ~450 lines
// of duplicated AST traversal code across cleanup, restructure, and advanced passes.
//
// The pattern:
// 1. Define a QueryTransformer trait with methods for each transformation
// 2. Implement apply_transformer() that walks the AST recursively
// 3. Each pass implements its own transformer that focuses on the optimization logic

use crate::error::Result;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, QueryExpression, SelectStatement, SqlStatement, TableExpression,
};

/// Trait for implementing query transformations
///
/// Each optimizer pass implements this trait to define how to transform
/// different AST nodes. The visitor framework handles the recursive traversal.
pub trait QueryTransformer {
    /// Transform a query expression (SELECT, SetOperation, VALUES, etc.)
    ///
    /// This is called after recursively processing all subqueries within the node.
    /// Return None to leave the query unchanged, or Some(new_query) to replace it.
    fn transform_query(&mut self, _query: QueryExpression) -> Result<Option<QueryExpression>> {
        // Default: no transformation
        Ok(None)
    }

    /// Transform a SELECT statement
    ///
    /// This is called after recursively processing all subqueries and expressions.
    /// Return None to leave unchanged, or Some(new_stmt) to replace it.
    fn transform_select(&mut self, _stmt: SelectStatement) -> Result<Option<SelectStatement>> {
        // Default: no transformation
        Ok(None)
    }

    /// Transform a table expression (subquery, join, etc.)
    ///
    /// This is called after recursively processing nested table expressions.
    /// Return None to leave unchanged, or Some(new_table) to replace it.
    fn transform_table(&mut self, _table: TableExpression) -> Result<Option<TableExpression>> {
        // Default: no transformation
        Ok(None)
    }

    /// Transform a domain expression (WHERE, HAVING, JOIN ON conditions, etc.)
    ///
    /// This is called after recursively processing nested expressions.
    /// Return None to leave unchanged, or Some(new_expr) to replace it.
    fn transform_domain_expr(
        &mut self,
        _expr: DomainExpression,
    ) -> Result<Option<DomainExpression>> {
        // Default: no transformation
        Ok(None)
    }
}

/// Apply a transformer to a SQL statement
///
/// This is the main entry point. It recursively walks the AST and applies
/// the transformer at each node.
pub fn apply_transformer<T: QueryTransformer>(
    stmt: SqlStatement,
    transformer: &mut T,
) -> Result<SqlStatement> {
    match stmt {
        SqlStatement::Query { with_clause, query } => {
            let transformed_query = transform_query(query, transformer)?;
            Ok(SqlStatement::Query {
                with_clause,
                query: transformed_query,
            })
        }
        // Other statement types - pass through unchanged
        other => Ok(other),
    }
}

/// Recursively transform a query expression
fn transform_query<T: QueryTransformer>(
    query: QueryExpression,
    transformer: &mut T,
) -> Result<QueryExpression> {
    // First, recursively process all sub-expressions
    let processed = match query {
        QueryExpression::Select(select_stmt) => {
            let transformed = transform_select(*select_stmt, transformer)?;
            QueryExpression::Select(Box::new(transformed))
        }
        QueryExpression::SetOperation { op, left, right } => {
            let transformed_left = Box::new(transform_query(*left, transformer)?);
            let transformed_right = Box::new(transform_query(*right, transformer)?);
            QueryExpression::SetOperation {
                op,
                left: transformed_left,
                right: transformed_right,
            }
        }
        QueryExpression::WithCte { ctes, query } => {
            // Transform each CTE's query
            let transformed_ctes = ctes
                .into_iter()
                .map(|cte| {
                    let transformed_query = transform_query(cte.query().clone(), transformer)?;
                    Ok(crate::pipeline::sql_ast_v3::Cte::new(
                        cte.name(),
                        transformed_query,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;

            let transformed_inner = Box::new(transform_query(*query, transformer)?);
            QueryExpression::WithCte {
                ctes: transformed_ctes,
                query: transformed_inner,
            }
        }
        // VALUES - no subqueries to process
        other => other,
    };

    // Then apply the transformer to this node
    match transformer.transform_query(processed.clone())? {
        Some(transformed) => Ok(transformed),
        None => Ok(processed),
    }
}

/// Recursively transform a SELECT statement
fn transform_select<T: QueryTransformer>(
    stmt: SelectStatement,
    transformer: &mut T,
) -> Result<SelectStatement> {
    // Extract all fields from the statement
    let select_list = stmt.select_list().to_vec();
    let distinct = stmt.is_distinct();
    let where_clause = stmt.where_clause().cloned();
    let group_by = stmt.group_by().map(|g| g.to_vec());
    let having = stmt.having().cloned();
    let order_by = stmt.order_by().map(|o| o.to_vec());
    let limit = stmt.limit().cloned();

    // Recursively transform FROM clause
    let transformed_from = if let Some(from_tables) = stmt.from() {
        Some(
            from_tables
                .iter()
                .map(|table| transform_table(table.clone(), transformer))
                .collect::<Result<Vec<_>>>()?,
        )
    } else {
        None
    };

    // Recursively transform WHERE clause
    let transformed_where = if let Some(expr) = where_clause {
        Some(transform_domain_expr(expr, transformer)?)
    } else {
        None
    };

    // Recursively transform HAVING clause
    let transformed_having = if let Some(expr) = having {
        Some(transform_domain_expr(expr, transformer)?)
    } else {
        None
    };

    // Rebuild the SELECT statement with transformed parts
    let mut builder = SelectStatement::builder();

    if distinct {
        builder = builder.distinct();
    }

    builder = builder.select_all(select_list);

    if let Some(from) = transformed_from {
        builder = builder.from_tables(from);
    }

    if let Some(where_expr) = transformed_where {
        builder = builder.where_clause(where_expr);
    }

    if let Some(group_by_exprs) = group_by {
        builder = builder.group_by(group_by_exprs);
    }

    if let Some(having_expr) = transformed_having {
        builder = builder.having(having_expr);
    }

    if let Some(order_by_terms) = order_by {
        for term in order_by_terms {
            builder = builder.order_by(term);
        }
    }

    if let Some(limit_clause) = limit {
        if let Some(offset_val) = limit_clause.offset() {
            builder = builder.limit_offset(limit_clause.count(), offset_val);
        } else {
            builder = builder.limit(limit_clause.count());
        }
    }

    let rebuilt = builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("Failed to rebuild SELECT: {}", e),
            source: None,
            subcategory: None,
        })?;

    // Apply the transformer to the rebuilt statement
    match transformer.transform_select(rebuilt.clone())? {
        Some(transformed) => Ok(transformed),
        None => Ok(rebuilt),
    }
}

/// Recursively transform a table expression
fn transform_table<T: QueryTransformer>(
    table: TableExpression,
    transformer: &mut T,
) -> Result<TableExpression> {
    // First, recursively process nested table expressions
    let processed = match table {
        TableExpression::Subquery { query, alias } => {
            let transformed_query = transform_query((*query).into_inner(), transformer)?;
            TableExpression::Subquery {
                query: Box::new(stacksafe::StackSafe::new(transformed_query)),
                alias,
            }
        }
        TableExpression::Join {
            left,
            join_type,
            right,
            join_condition,
        } => {
            let transformed_left = Box::new(transform_table(*left, transformer)?);
            let transformed_right = Box::new(transform_table(*right, transformer)?);

            // Transform join condition if it's an ON clause
            use crate::pipeline::sql_ast_v3::JoinCondition;
            let transformed_condition = match join_condition {
                JoinCondition::On(expr) => {
                    JoinCondition::On(transform_domain_expr(expr, transformer)?)
                }
                other => other,
            };

            TableExpression::Join {
                left: transformed_left,
                join_type,
                right: transformed_right,
                join_condition: transformed_condition,
            }
        }
        TableExpression::UnionTable { selects, alias } => {
            // Transform each query in the union table
            let transformed_selects = selects
                .into_iter()
                .map(|query| transform_query(query, transformer))
                .collect::<Result<Vec<_>>>()?;
            TableExpression::UnionTable {
                selects: transformed_selects,
                alias,
            }
        }
        // Other table types (Table, TableFunction) - no nested queries
        other => other,
    };

    // Then apply the transformer to this node
    match transformer.transform_table(processed.clone())? {
        Some(transformed) => Ok(transformed),
        None => Ok(processed),
    }
}

/// Recursively transform a domain expression
fn transform_domain_expr<T: QueryTransformer>(
    expr: DomainExpression,
    transformer: &mut T,
) -> Result<DomainExpression> {
    // First, recursively process nested expressions
    let processed = match expr {
        DomainExpression::Binary { left, op, right } => {
            let transformed_left = Box::new(transform_domain_expr(*left, transformer)?);
            let transformed_right = Box::new(transform_domain_expr(*right, transformer)?);
            DomainExpression::Binary {
                left: transformed_left,
                op,
                right: transformed_right,
            }
        }
        DomainExpression::Unary { op, expr } => {
            let transformed = Box::new(transform_domain_expr(*expr, transformer)?);
            DomainExpression::Unary {
                op,
                expr: transformed,
            }
        }
        DomainExpression::InList { expr, not, values } => {
            let transformed_expr = Box::new(transform_domain_expr(*expr, transformer)?);
            let transformed_values = values
                .into_iter()
                .map(|v| transform_domain_expr(v, transformer))
                .collect::<Result<Vec<_>>>()?;
            DomainExpression::InList {
                expr: transformed_expr,
                not,
                values: transformed_values,
            }
        }
        DomainExpression::Exists { not, query } => {
            let transformed_query = transform_query(*query, transformer)?;
            DomainExpression::Exists {
                not,
                query: Box::new(transformed_query),
            }
        }
        DomainExpression::Subquery(query) => {
            let transformed_query = transform_query(*query, transformer)?;
            DomainExpression::Subquery(Box::new(transformed_query))
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            let transformed_expr = if let Some(e) = expr {
                Some(Box::new(transform_domain_expr(*e, transformer)?))
            } else {
                None
            };

            let transformed_when = when_clauses
                .into_iter()
                .map(|when_clause| {
                    let transformed_when =
                        transform_domain_expr(when_clause.when().clone(), transformer)?;
                    let transformed_then =
                        transform_domain_expr(when_clause.then().clone(), transformer)?;
                    Ok(crate::pipeline::sql_ast_v3::WhenClause::new(
                        transformed_when,
                        transformed_then,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;

            let transformed_else = if let Some(e) = else_clause {
                Some(Box::new(transform_domain_expr(*e, transformer)?))
            } else {
                None
            };

            DomainExpression::Case {
                expr: transformed_expr,
                when_clauses: transformed_when,
                else_clause: transformed_else,
            }
        }
        // Literals, columns, aggregates, etc. - no nested expressions
        other => other,
    };

    // Then apply the transformer to this node
    match transformer.transform_domain_expr(processed.clone())? {
        Some(transformed) => Ok(transformed),
        None => Ok(processed),
    }
}
