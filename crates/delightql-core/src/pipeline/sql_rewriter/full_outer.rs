// full_outer.rs — Expand FULL OUTER JOIN for dialects without native support.
//
// SELECT <proj> FROM A FULL OUTER JOIN B ON cond WHERE <filters>
// becomes:
//   SELECT <proj> FROM A LEFT JOIN B ON cond WHERE <filters>
//   UNION ALL
//   SELECT <proj> FROM B LEFT JOIN A ON cond WHERE <left_key> IS NULL AND <filters>
//
// The expansion works at the SELECT level — duplicating the entire projection
// and WHERE into both branches — because the outer query's column references
// use the join operand aliases (u.id, o.total) which must remain in scope.

use crate::error::{DelightQLError, Result};
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::{
    BinaryOperator, DomainExpression, JoinCondition, JoinType, QueryExpression, SelectStatement,
    SetOperator, SqlStatement, TableExpression,
};

/// Should we expand FULL OUTER JOIN for this dialect?
pub fn needs_expansion(dialect: SqlDialect) -> bool {
    match dialect {
        SqlDialect::SQLite | SqlDialect::MySQL => true,
        SqlDialect::PostgreSQL | SqlDialect::SqlServer => false,
    }
}

/// Walk the SQL statement and expand any FULL OUTER JOINs.
pub fn expand_full_outer_joins(stmt: SqlStatement) -> Result<SqlStatement> {
    match stmt {
        SqlStatement::Query { with_clause, query } => {
            let rewritten = rewrite_query(query)?;
            Ok(SqlStatement::Query {
                with_clause,
                query: rewritten,
            })
        }
        other => Ok(other),
    }
}

#[stacksafe::stacksafe]
fn rewrite_query(query: QueryExpression) -> Result<QueryExpression> {
    match query {
        QueryExpression::Select(select) => rewrite_select_query(*select),
        QueryExpression::SetOperation { op, left, right } => {
            let left = Box::new(rewrite_query(*left)?);
            let right = Box::new(rewrite_query(*right)?);
            Ok(QueryExpression::SetOperation { op, left, right })
        }
        QueryExpression::WithCte { ctes, query } => {
            let ctes = ctes
                .into_iter()
                .map(|cte| {
                    let rewritten = rewrite_query(cte.query().clone())?;
                    Ok(crate::pipeline::sql_ast_v3::Cte::new(cte.name(), rewritten))
                })
                .collect::<Result<Vec<_>>>()?;
            let query = Box::new(rewrite_query(*query)?);
            Ok(QueryExpression::WithCte { ctes, query })
        }
        other => Ok(other),
    }
}

/// Rewrite a SELECT. If its FROM contains a top-level FULL OUTER JOIN,
/// expand the entire SELECT into a UNION ALL of two LEFT JOINs.
/// If the FULL OUTER is nested deeper, recurse into subqueries.
fn rewrite_select_query(stmt: SelectStatement) -> Result<QueryExpression> {
    let Some(from) = stmt.from() else {
        return Ok(QueryExpression::Select(Box::new(stmt)));
    };

    // Check for a top-level FULL OUTER JOIN in FROM
    if from.len() == 1 {
        if let TableExpression::Join {
            ref left,
            join_type: JoinType::Full,
            ref right,
            ref join_condition,
        } = from[0]
        {
            // Top-level FULL OUTER — expand this SELECT
            return expand_full_outer_select(&stmt, left, right, join_condition);
        }
    }

    // No top-level FULL OUTER — recurse into subqueries within FROM
    let new_from: Vec<TableExpression> = from
        .iter()
        .map(|t| rewrite_table_subqueries(t.clone()))
        .collect::<Result<Vec<_>>>()?;

    rebuild_select_with_from(stmt, new_from).map(|s| QueryExpression::Select(Box::new(s)))
}

/// Expand a SELECT with a top-level FULL OUTER JOIN.
///
/// Given: SELECT <proj> FROM A FULL OUTER JOIN B ON cond WHERE <where> ...
/// Produce:
///   SELECT <proj> FROM A LEFT JOIN B ON cond WHERE <where>
///   UNION ALL
///   SELECT <proj> FROM B LEFT JOIN A ON cond WHERE A.key IS NULL [AND <where>]
fn expand_full_outer_select(
    stmt: &SelectStatement,
    left: &TableExpression,
    right: &TableExpression,
    condition: &JoinCondition,
) -> Result<QueryExpression> {
    // First, recursively expand any FULL OUTERs in the children
    let left = rewrite_table_subqueries(left.clone())?;
    let right = rewrite_table_subqueries(right.clone())?;

    let null_check_col = extract_null_check_column(condition)?;

    // Branch 1: A LEFT JOIN B ON cond (same projection, same WHERE)
    let branch1_from = TableExpression::Join {
        left: Box::new(left.clone()),
        join_type: JoinType::Left,
        right: Box::new(right.clone()),
        join_condition: condition.clone(),
    };
    let branch1 = rebuild_select_with_from_and_extra_where(stmt, vec![branch1_from], None)?;

    // Branch 2: B LEFT JOIN A ON cond, with extra WHERE A.key IS NULL
    let branch2_from = TableExpression::Join {
        left: Box::new(right),
        join_type: JoinType::Left,
        right: Box::new(left),
        join_condition: condition.clone(),
    };
    let null_check = DomainExpression::Binary {
        left: Box::new(null_check_col),
        op: BinaryOperator::Is,
        right: Box::new(DomainExpression::Literal(
            crate::pipeline::ast_refined::LiteralValue::Null,
        )),
    };
    let branch2 =
        rebuild_select_with_from_and_extra_where(stmt, vec![branch2_from], Some(null_check))?;

    // UNION ALL
    Ok(QueryExpression::SetOperation {
        op: SetOperator::UnionAll,
        left: Box::new(QueryExpression::Select(Box::new(branch1))),
        right: Box::new(QueryExpression::Select(Box::new(branch2))),
    })
}

/// Recurse into subqueries within table expressions (for nested FULL OUTERs)
fn rewrite_table_subqueries(table: TableExpression) -> Result<TableExpression> {
    match table {
        TableExpression::Subquery { query, alias } => {
            let rewritten = rewrite_query((*query).into_inner())?;
            Ok(TableExpression::Subquery {
                query: Box::new(stacksafe::StackSafe::new(rewritten)),
                alias,
            })
        }
        TableExpression::Join {
            left,
            join_type,
            right,
            join_condition,
        } => {
            let left = Box::new(rewrite_table_subqueries(*left)?);
            let right = Box::new(rewrite_table_subqueries(*right)?);
            Ok(TableExpression::Join {
                left,
                join_type,
                right,
                join_condition,
            })
        }
        other => Ok(other),
    }
}

/// Rebuild a SELECT with a new FROM clause, preserving all other clauses.
fn rebuild_select_with_from(
    stmt: SelectStatement,
    from: Vec<TableExpression>,
) -> Result<SelectStatement> {
    rebuild_select_with_from_and_extra_where(&stmt, from, None)
}

/// Rebuild a SELECT with a new FROM and optionally AND an extra WHERE condition.
fn rebuild_select_with_from_and_extra_where(
    stmt: &SelectStatement,
    from: Vec<TableExpression>,
    extra_where: Option<DomainExpression>,
) -> Result<SelectStatement> {
    let mut builder = SelectStatement::builder();

    if stmt.is_distinct() {
        builder = builder.distinct();
    }

    builder = builder.select_all(stmt.select_list().to_vec());
    builder = builder.from_tables(from);

    // Merge WHERE: original AND extra
    let where_clause = match (stmt.where_clause(), extra_where) {
        (Some(original), Some(extra)) => Some(DomainExpression::Binary {
            left: Box::new(extra),
            op: BinaryOperator::And,
            right: Box::new(original.clone()),
        }),
        (Some(original), None) => Some(original.clone()),
        (None, Some(extra)) => Some(extra),
        (None, None) => None,
    };
    if let Some(w) = where_clause {
        builder = builder.where_clause(w);
    }

    if let Some(gb) = stmt.group_by() {
        builder = builder.group_by(gb.to_vec());
    }
    if let Some(h) = stmt.having() {
        builder = builder.having(h.clone());
    }
    if let Some(ob) = stmt.order_by() {
        for term in ob {
            builder = builder.order_by(term.clone());
        }
    }
    if let Some(lim) = stmt.limit() {
        if let Some(off) = lim.offset() {
            builder = builder.limit_offset(lim.count(), off);
        } else {
            builder = builder.limit(lim.count());
        }
    }

    builder.build().map_err(|e| DelightQLError::ParseError {
        message: format!("sql_rewriter full_outer rebuild: {}", e),
        source: None,
        subcategory: None,
    })
}

/// Extract a qualified column from the join condition for NULL checking.
fn extract_null_check_column(condition: &JoinCondition) -> Result<DomainExpression> {
    match condition {
        JoinCondition::On(expr) => {
            find_first_qualified_column(expr).ok_or_else(|| DelightQLError::ParseError {
                message: "FULL OUTER JOIN: could not find qualified column in ON for NULL check"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }
        JoinCondition::Using(cols) => {
            if cols.is_empty() {
                Err(DelightQLError::ParseError {
                    message: "FULL OUTER JOIN with empty USING clause".to_string(),
                    source: None,
                    subcategory: None,
                })
            } else {
                Ok(DomainExpression::column(&cols[0]))
            }
        }
        JoinCondition::Natural => Err(DelightQLError::ParseError {
            message: "FULL OUTER JOIN with NATURAL is not supported".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

fn find_first_qualified_column(expr: &DomainExpression) -> Option<DomainExpression> {
    match expr {
        DomainExpression::Column {
            name,
            qualifier: Some(qual),
        } => Some(DomainExpression::Column {
            name: name.clone(),
            qualifier: Some(qual.clone()),
        }),
        DomainExpression::Binary { left, right, .. } => {
            find_first_qualified_column(left).or_else(|| find_first_qualified_column(right))
        }
        DomainExpression::Parens(inner) => find_first_qualified_column(inner),
        _ => None,
    }
}
