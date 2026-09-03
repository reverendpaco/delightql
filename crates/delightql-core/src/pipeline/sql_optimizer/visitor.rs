// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// visitor.rs - Generic AST visitor pattern for SQL optimizer passes
//
// This module provides the reusable visitor pattern the optimizer passes
// (today: cleanup) share for AST traversal.
//
// The pattern:
// 1. Define a QueryTransformer trait with methods for each transformation
// 2. Implement apply_transformer() that walks the AST recursively
// 3. Each pass implements its own transformer that focuses on the optimization logic

use crate::error::Result;
use crate::pipeline::sql_ast::{
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

    /// The walker is DESCENDING into the expressions of a SELECT whose
    /// FROM exposes these table names: any subquery met inside them can
    /// correlate against these names (they are its enclosing scope).
    /// Default: ignore. A pass that must distinguish "reference to an
    /// enclosing scope" from "reference to a name I might expose" keeps
    /// a stack from these calls.
    fn enter_expr_scope(&mut self, _scopes: &[crate::names::ScopeId]) {}

    /// The walker finished the expressions of the SELECT that pushed the
    /// matching `enter_expr_scope`.
    fn exit_expr_scope(&mut self) {}

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
#[stacksafe::stacksafe]
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
                .map(|cte| cte.rewrite_parts(|part| transform_query(part, transformer)))
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

    // Recursively transform WHERE and HAVING under this statement's
    // expression scope: subqueries inside them can correlate against
    // this FROM's exposed names.
    let scopes: Vec<crate::names::ScopeId> = transformed_from
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .flat_map(exposed_scopes)
        .collect();
    transformer.enter_expr_scope(&scopes);
    let transformed_where = if let Some(expr) = where_clause {
        Some(transform_domain_expr(expr, transformer)?)
    } else {
        None
    };
    let transformed_having = if let Some(expr) = having {
        Some(transform_domain_expr(expr, transformer)?)
    } else {
        None
    };
    transformer.exit_expr_scope();

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
        builder = builder.limit_from(limit_clause.clone());
    }

    let rebuilt =
        builder
            .rebuilding(&stmt)
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
            use crate::pipeline::sql_ast::JoinCondition;
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
        DomainExpression::Cast { expr, type_name } => {
            let transformed = Box::new(transform_domain_expr(*expr, transformer)?);
            DomainExpression::Cast {
                expr: transformed,
                type_name,
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
                    Ok(crate::pipeline::sql_ast::WhenClause::new(
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

/// The table names a FROM item exposes into its statement's scope:
/// bare tables by alias-or-name, aliased subqueries by alias, join
/// trees recursively.
pub(super) fn exposed_scopes(table: &TableExpression) -> Vec<crate::names::ScopeId> {
    let mut out = Vec::new();
    fn walk(table: &TableExpression, out: &mut Vec<crate::names::ScopeId>) {
        match table {
            TableExpression::Scope(scope) | TableExpression::QualifiedScope { scope, .. } => {
                out.push(*scope)
            }
            TableExpression::Entity {
                alias: Some(scope), ..
            } => out.push(*scope),
            TableExpression::Entity { alias: None, .. } => {}
            TableExpression::Subquery { alias, .. } => out.push(*alias),
            TableExpression::Join { left, right, .. } => {
                walk(left, out);
                walk(right, out);
            }
            TableExpression::TVF { alias, .. } => out.push(*alias),
        }
    }
    walk(table, &mut out);
    out
}
