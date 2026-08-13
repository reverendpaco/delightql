// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Conservative subquery-boundary collapse over structural identities.
//!
//! A transparent single-FROM wrapper may be removed when every reference
//! to its heading can be substituted by `ColId`. Boundaries inside joins
//! stay in place; removing one there also changes the join's exposed scope
//! and needs a separate structural proof.

use std::collections::HashMap;

use crate::error::Result;
use crate::names::ColId;
use crate::pipeline::sql_ast::{
    DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement, TableExpression,
};

use super::visitor::{apply_transformer, QueryTransformer};

pub(super) fn pass_cleanup(stmt: SqlStatement) -> Result<SqlStatement> {
    apply_transformer(stmt, &mut CollapseTransformer)
}

struct CollapseTransformer;

impl QueryTransformer for CollapseTransformer {
    fn transform_query(&mut self, query: QueryExpression) -> Result<Option<QueryExpression>> {
        let QueryExpression::Select(outer) = &query else {
            return Ok(None);
        };
        let Some([TableExpression::Subquery { query: inner, .. }]) = outer.from() else {
            return Ok(None);
        };
        let inner = inner.as_ref().clone().into_inner();

        // A star wrapper is removable only when it publishes what its body
        // already publishes. Where the two stand at different scopes the
        // wrapper is not decoration — it is the boundary that re-publishes the
        // body's occurrences under the alias naming it — and promoting the
        // body leaves that alias claiming a heading the promoted statement
        // does not output. The collapse below keeps the outer scope for the
        // same reason; this arm must not be the one place that discards it.
        if trivial_star_wrapper(outer) {
            let carries_identity = match &inner {
                QueryExpression::Select(body) => body.at() == outer.at(),
                _ => false,
            };
            if carries_identity {
                return Ok(Some(inner));
            }
            return Ok(None);
        }

        let QueryExpression::Select(inner) = inner else {
            return Ok(None);
        };
        if has_barrier(outer) || has_barrier(&inner) {
            return Ok(None);
        }

        let Some(definitions) = output_definitions(inner.select_list()) else {
            return Ok(None);
        };
        if a_definition_would_be_written_twice(outer, &definitions) {
            return Ok(None);
        }
        let Some(select_list) = rewrite_select_list(outer.select_list(), &definitions) else {
            return Ok(None);
        };
        let outer_where = match outer.where_clause() {
            Some(expr) => {
                let Some(expr) = rewrite_expr(expr, &definitions) else {
                    return Ok(None);
                };
                Some(expr)
            }
            None => None,
        };

        let where_clause = match (inner.where_clause().cloned(), outer_where) {
            (Some(left), Some(right)) => Some(DomainExpression::and(vec![left, right])),
            (left, right) => left.or(right),
        };

        let mut builder = SelectStatement::builder().select_all(select_list);
        if let Some(from) = inner.from() {
            builder = builder.from_tables(from.to_vec());
        }
        if let Some(where_clause) = where_clause {
            builder = builder.where_clause(where_clause);
        }
        let collapsed = builder
            .rebuilding(outer)
            .map_err(crate::error::DelightQLError::parse_error)?;
        Ok(Some(QueryExpression::Select(Box::new(collapsed))))
    }
}

fn trivial_star_wrapper(select: &SelectStatement) -> bool {
    !select.is_distinct()
        && matches!(select.select_list(), [SelectItem::Star { .. }])
        && select.where_clause().is_none()
        && select.group_by().is_none()
        && select.having().is_none()
        && select.order_by().is_none()
        && select.limit().is_none()
}

fn has_barrier(select: &SelectStatement) -> bool {
    select.is_distinct()
        || select.group_by().is_some()
        || select.having().is_some()
        || select.order_by().is_some()
        || select.limit().is_some()
        || select
            .select_list()
            .iter()
            .any(select_item_contains_window_or_aggregate)
}

/// Would collapsing write some body expression into more than one place?
///
/// Substitution replaces a reference with the expression behind it, so a
/// column read twice becomes that expression twice — one evaluation where the
/// body had one, and a volatile expression is then two values rather than one.
/// A column or a literal costs nothing to repeat and is exempt; a boundary a
/// reader is leaning on to name one value stays standing.
fn a_definition_would_be_written_twice(
    outer: &SelectStatement,
    definitions: &HashMap<ColId, DomainExpression>,
) -> bool {
    let mut reads: HashMap<ColId, usize> = HashMap::new();
    for item in outer.select_list() {
        if let SelectItem::Expression { expr, .. } = item {
            count_reads(expr, &mut reads);
        }
    }
    if let Some(expr) = outer.where_clause() {
        count_reads(expr, &mut reads);
    }
    definitions.iter().any(|(output, expr)| {
        reads.get(output).copied().unwrap_or(0) > 1
            && !matches!(
                expr,
                DomainExpression::Column(_) | DomainExpression::Literal(_)
            )
    })
}

fn count_reads(expr: &DomainExpression, reads: &mut HashMap<ColId, usize>) {
    let counted = std::cell::RefCell::new(std::mem::take(reads));
    let _ = expr.clone().map_columns(&|column| {
        *counted.borrow_mut().entry(column).or_insert(0) += 1;
        column
    });
    *reads = counted.into_inner();
}

fn output_definitions(select_list: &[SelectItem]) -> Option<HashMap<ColId, DomainExpression>> {
    let mut definitions = HashMap::new();
    for item in select_list {
        let SelectItem::Expression { expr, alias } = item else {
            return None;
        };
        let output = alias.or_else(|| match expr {
            DomainExpression::Column(column) => Some(*column),
            _ => None,
        })?;
        if definitions.insert(output, expr.clone()).is_some() {
            return None;
        }
    }
    Some(definitions)
}

fn rewrite_select_list(
    items: &[SelectItem],
    definitions: &HashMap<ColId, DomainExpression>,
) -> Option<Vec<SelectItem>> {
    items
        .iter()
        .map(|item| match item {
            SelectItem::Expression { expr, alias } => Some(SelectItem::Expression {
                expr: rewrite_expr(expr, definitions)?,
                alias: *alias,
            }),
            SelectItem::Star { .. } => None,
        })
        .collect()
}

fn rewrite_expr(
    expr: &DomainExpression,
    definitions: &HashMap<ColId, DomainExpression>,
) -> Option<DomainExpression> {
    Some(match expr {
        // Promoting the body dissolves the alias that stood over it, so a
        // reference this map cannot answer is one no surviving FROM entry
        // offers. Keeping it is how a projection loses the literals it was
        // there to add: nothing downstream can tell `success` standing for a
        // computed output from `success` standing for nothing at all, because
        // both spell the same. Decline the collapse instead — a boundary left
        // in place costs a subquery, and there is no wrong answer in it.
        DomainExpression::Column(column) => match definitions.get(column)? {
            replacement @ (DomainExpression::Binary { .. } | DomainExpression::Unary { .. }) => {
                DomainExpression::Parens(Box::new(replacement.clone()))
            }
            replacement => replacement.clone(),
        },
        DomainExpression::Cast { expr, type_name } => DomainExpression::Cast {
            expr: Box::new(rewrite_expr(expr, definitions)?),
            type_name: type_name.clone(),
        },
        DomainExpression::Observation { expr, positive } => DomainExpression::Observation {
            expr: Box::new(rewrite_expr(expr, definitions)?),
            positive: *positive,
        },
        DomainExpression::Binary { left, op, right } => DomainExpression::Binary {
            left: Box::new(rewrite_expr(left, definitions)?),
            op: op.clone(),
            right: Box::new(rewrite_expr(right, definitions)?),
        },
        DomainExpression::Unary { op, expr } => DomainExpression::Unary {
            op: op.clone(),
            expr: Box::new(rewrite_expr(expr, definitions)?),
        },
        DomainExpression::Function {
            name,
            args,
            distinct,
        } => DomainExpression::Function {
            name: name.clone(),
            args: rewrite_exprs(args, definitions)?,
            distinct: *distinct,
        },
        DomainExpression::Parens(inner) => {
            DomainExpression::Parens(Box::new(rewrite_expr(inner, definitions)?))
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => DomainExpression::Case {
            expr: match expr.as_deref() {
                Some(expr) => Some(Box::new(rewrite_expr(expr, definitions)?)),
                None => None,
            },
            when_clauses: when_clauses
                .iter()
                .map(|clause| {
                    Some(crate::pipeline::sql_ast::WhenClause::new(
                        rewrite_expr(clause.when(), definitions)?,
                        rewrite_expr(clause.then(), definitions)?,
                    ))
                })
                .collect::<Option<_>>()?,
            else_clause: match else_clause.as_deref() {
                Some(expr) => Some(Box::new(rewrite_expr(expr, definitions)?)),
                None => None,
            },
        },
        DomainExpression::Exists { .. } | DomainExpression::Subquery(_) => {
            // A nested query has its own scope. Rebinding through that
            // scope belongs to the query walker, not an expression clone.
            return None;
        }
        DomainExpression::WindowFunction {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            frame,
        } => DomainExpression::WindowFunction {
            name: name.clone(),
            args: rewrite_exprs(args, definitions)?,
            distinct: *distinct,
            partition_by: rewrite_exprs(partition_by, definitions)?,
            order_by: order_by
                .iter()
                .map(|(expr, direction)| {
                    Some((rewrite_expr(expr, definitions)?, direction.clone()))
                })
                .collect::<Option<_>>()?,
            frame: frame.clone(),
        },
        DomainExpression::PredicateRewrite {
            name,
            args,
            negated,
        } => DomainExpression::PredicateRewrite {
            name: name.clone(),
            args: rewrite_exprs(args, definitions)?,
            negated: *negated,
        },
        DomainExpression::Literal(_)
        | DomainExpression::PublishedNameLiteral(_)
        | DomainExpression::PublishedJsonPathLiteral(_)
        | DomainExpression::JsonPathLiteral(_)
        | DomainExpression::ScopeNameLiteral(_)
        | DomainExpression::Star => expr.clone(),
    })
}

fn rewrite_exprs(
    expressions: &[DomainExpression],
    definitions: &HashMap<ColId, DomainExpression>,
) -> Option<Vec<DomainExpression>> {
    expressions
        .iter()
        .map(|expr| rewrite_expr(expr, definitions))
        .collect()
}

fn select_item_contains_window_or_aggregate(item: &SelectItem) -> bool {
    match item {
        SelectItem::Expression { expr, .. } => contains_window_or_aggregate(expr),
        SelectItem::Star { .. } => false,
    }
}

fn contains_window_or_aggregate(expr: &DomainExpression) -> bool {
    match expr {
        DomainExpression::WindowFunction { .. } => true,
        DomainExpression::Function { name, args, .. } => {
            name.user().is_some_and(|name| {
                matches!(
                    name.to_ascii_lowercase().as_str(),
                    "count" | "sum" | "avg" | "min" | "max" | "group_concat"
                )
            }) || args.iter().any(contains_window_or_aggregate)
        }
        DomainExpression::Cast { expr, .. }
        | DomainExpression::Unary { expr, .. }
        | DomainExpression::Observation { expr, .. }
        | DomainExpression::Parens(expr) => contains_window_or_aggregate(expr),
        DomainExpression::Binary { left, right, .. } => {
            contains_window_or_aggregate(left) || contains_window_or_aggregate(right)
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_deref().is_some_and(contains_window_or_aggregate)
                || when_clauses.iter().any(|clause| {
                    contains_window_or_aggregate(clause.when())
                        || contains_window_or_aggregate(clause.then())
                })
                || else_clause
                    .as_deref()
                    .is_some_and(contains_window_or_aggregate)
        }
        DomainExpression::PredicateRewrite { args: elements, .. } => {
            elements.iter().any(contains_window_or_aggregate)
        }
        DomainExpression::Exists { .. }
        | DomainExpression::Subquery(_)
        | DomainExpression::Column(_)
        | DomainExpression::Literal(_)
        | DomainExpression::PublishedNameLiteral(_)
        | DomainExpression::PublishedJsonPathLiteral(_)
        | DomainExpression::JsonPathLiteral(_)
        | DomainExpression::ScopeNameLiteral(_)
        | DomainExpression::Star => false,
    }
}
