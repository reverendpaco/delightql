// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

use crate::diagnostic::{Constraint, Internal};
use crate::error::{DelightQLError, Result};
use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::{
    BinaryOperator, DomainExpression, JoinCondition, JoinType, QueryExpression, SelectItem,
    SelectStatement, SetOperator, SqlStatement, TableExpression,
};
use std::collections::BTreeSet;

/// Should we expand FULL OUTER JOIN for this dialect?
pub fn needs_expansion(dialect: SqlDialect) -> bool {
    match dialect {
        SqlDialect::SQLite | SqlDialect::MySQL => true,
        SqlDialect::PostgreSQL | SqlDialect::SqlServer | SqlDialect::DuckDB => false,
    }
}

/// Walk the SQL statement and expand any FULL OUTER JOINs.
pub fn expand_full_outer_joins(
    stmt: SqlStatement,
    identities: &crate::names::Registry,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<SqlStatement> {
    match stmt {
        SqlStatement::Query { with_clause, query } => {
            let rewritten = rewrite_query(query, identities, target)?;
            Ok(SqlStatement::Query {
                with_clause,
                query: rewritten,
            })
        }
        other => Ok(other),
    }
}

#[stacksafe::stacksafe]
fn rewrite_query(
    query: QueryExpression,
    identities: &crate::names::Registry,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<QueryExpression> {
    match query {
        QueryExpression::Select(select) => rewrite_select_query(*select, identities, target),
        QueryExpression::SetOperation { op, left, right } => {
            let left = Box::new(rewrite_query(*left, identities, target)?);
            let right = Box::new(rewrite_query(*right, identities, target)?);
            Ok(QueryExpression::SetOperation { op, left, right })
        }
    }
}

/// Rewrite a SELECT. If its FROM contains a top-level FULL OUTER JOIN,
/// expand the entire SELECT into a UNION ALL of two LEFT JOINs.
/// If the FULL OUTER is nested deeper, recurse into subqueries.
fn rewrite_select_query(
    stmt: SelectStatement,
    identities: &crate::names::Registry,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<QueryExpression> {
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
            // Top-level FULL OUTER — expand this SELECT. A SELECT that
            // computes over the whole relation (aggregate, DISTINCT,
            // GROUP BY, window) must push the union UNDER that
            // computation; duplicating it per branch computes it once
            // per part and yields two rows where a scalar is promised.
            if computes_over_whole_relation(&stmt, target) {
                return expand_full_outer_select_aggregated(
                    &stmt,
                    left,
                    right,
                    join_condition,
                    identities,
                    target,
                );
            }
            return expand_full_outer_select(
                &stmt,
                left,
                right,
                join_condition,
                identities,
                target,
            );
        }
    }

    // No top-level FULL OUTER — recurse into subqueries within FROM
    let new_from: Vec<TableExpression> = from
        .iter()
        .map(|t| rewrite_table_subqueries(t.clone(), identities, target))
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
    identities: &crate::names::Registry,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<QueryExpression> {
    // First, recursively expand any FULL OUTERs in the children
    let left = rewrite_table_subqueries(left.clone(), identities, target)?;
    let right = rewrite_table_subqueries(right.clone(), identities, target)?;

    let null_check_col = extract_null_check_column(condition, operand_scope(&left), identities)?;

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
            crate::pipeline::asts::core::LiteralValue::Null,
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

/// Does this SELECT compute something over the WHOLE relation — an
/// aggregate, DISTINCT, GROUP BY/HAVING, or a window function — such
/// that computing it once per expansion branch would be wrong?
fn computes_over_whole_relation(
    stmt: &SelectStatement,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> bool {
    stmt.is_distinct()
        || stmt.group_by().is_some()
        || stmt.having().is_some()
        || stmt.select_list().iter().any(|item| {
            item.expr()
                .is_some_and(|expr| contains_whole_relation_fn(expr, target))
        })
}

fn contains_whole_relation_fn(
    expr: &DomainExpression,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> bool {
    let contains = |expr: &DomainExpression| contains_whole_relation_fn(expr, target);
    match expr {
        // THE GRADE IS THE CALL'S, judged with its arity against what the
        // target reduces: `max(v, 0)` computes per row of each part, never
        // over the whole relation.
        DomainExpression::Function { name, args, .. } => {
            name.user()
                .is_some_and(|name| target.reduces(name, args.len()))
                || args.iter().any(contains)
        }
        DomainExpression::WindowFunction { .. } => true,
        DomainExpression::Binary { left, right, .. } => contains(left) || contains(right),
        DomainExpression::Unary { expr, .. }
        | DomainExpression::Parens(expr)
        | DomainExpression::Cast { expr, .. } => contains(expr),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_deref().is_some_and(contains)
                || when_clauses
                    .iter()
                    .any(|w| contains(w.when()) || contains(w.then()))
                || else_clause.as_deref().is_some_and(contains)
        }
        DomainExpression::PredicateRewrite { args, .. } => args.iter().any(contains),
        // Subquery bodies aggregate over their OWN relation.
        _ => false,
    }
}

/// Expand a SELECT that both holds a top-level FULL OUTER JOIN and
/// computes over the whole relation. The branches are demoted to pure
/// row producers projecting every operand column the outer clauses
/// reference (mangled `qualifier__name`); the aggregate/DISTINCT/
/// grouping runs ONCE over their UNION ALL:
///
///   SELECT <proj'> FROM (
///     SELECT <refs> FROM A LEFT JOIN B ON cond WHERE <where>
///     UNION ALL
///     SELECT <refs> FROM B LEFT JOIN A ON cond WHERE A.key IS NULL AND <where>
///   ) AS __fo GROUP BY <g'> HAVING <h'> ...
///
/// where <proj'>/<g'>/<h'> are the original clauses with operand
/// references rewritten to the mangled names. References inside scalar
/// subqueries are NOT rewritten — a correlated ref to a join operand
/// there fails loudly at execution rather than silently misbinding.
fn expand_full_outer_select_aggregated(
    stmt: &SelectStatement,
    left: &TableExpression,
    right: &TableExpression,
    condition: &JoinCondition,
    identities: &crate::names::Registry,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<QueryExpression> {
    // Recursively expand any FULL OUTERs in the children first.
    let left = rewrite_table_subqueries(left.clone(), identities, target)?;
    let right = rewrite_table_subqueries(right.clone(), identities, target)?;

    let (Some(left_scope), Some(right_scope)) = (operand_scope(&left), operand_scope(&right))
    else {
        return Err(DelightQLError::from(Constraint::Join {
            message: "FULL OUTER JOIN under an aggregate: operands must carry aliases".to_string(),
        }));
    };
    let operand_scopes = [left_scope, right_scope];

    let null_check_col = extract_null_check_column(condition, Some(left_scope), identities)?;

    // Every operand column the outer clauses reference.
    let mut refs: Vec<crate::names::ColId> = Vec::new();
    for item in stmt.select_list() {
        if let Some(expr) = item.expr() {
            collect_operand_refs(expr, &operand_scopes, identities, &mut refs);
        }
    }
    if let Some(gb) = stmt.group_by() {
        for e in gb {
            collect_operand_refs(e, &operand_scopes, identities, &mut refs);
        }
    }
    if let Some(h) = stmt.having() {
        collect_operand_refs(h, &operand_scopes, identities, &mut refs);
    }
    if let Some(ob) = stmt.order_by() {
        for term in ob {
            collect_operand_refs(term.expr(), &operand_scopes, identities, &mut refs);
        }
    }

    let join_scope = identities.join_scope();
    let carrier_scope = identities.wrap_scope(join_scope, crate::names::WrapReason::SetOperation);
    let mut replacements = std::collections::HashMap::new();
    let inner_items: Vec<SelectItem> = if refs.is_empty() {
        // count(*)-style: no column refs — the branches still must
        // produce one column per row.
        let output = identities.sql_column(carrier_scope, None, crate::names::Addressing::Hygienic);
        vec![SelectItem::Publishing {
            expr: DomainExpression::Literal(crate::pipeline::asts::core::LiteralValue::integer(1)),
            slot: output,
            printed: true,
        }]
    } else {
        refs.iter()
            .map(|source| {
                let output = identities.rebind_sql_column(
                    *source,
                    carrier_scope,
                    identities.published(*source),
                );
                replacements.insert(*source, output);
                SelectItem::Publishing {
                    expr: DomainExpression::Column(*source),
                    slot: output,
                    printed: true,
                }
            })
            .collect()
    };

    // Branch 1: A LEFT JOIN B; branch 2: B LEFT JOIN A + unmatched test.
    let branch = |from_left: &TableExpression,
                  from_right: &TableExpression,
                  extra_where: Option<DomainExpression>|
     -> Result<SelectStatement> {
        let mut b = SelectStatement::builder()
            .select_all(inner_items.clone())
            .from_tables(vec![TableExpression::Join {
                left: Box::new(from_left.clone()),
                join_type: JoinType::Left,
                right: Box::new(from_right.clone()),
                join_condition: condition.clone(),
            }]);
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
            b = b.where_clause(w);
        }
        // The branch stands at the carrier the union publishes through, and
        // its items name the occurrences just minted there — a heading of its
        // own, so it goes through the authority rather than carrying evidence
        // from a statement it is not a rewrite of.
        (b).standing_at(carrier_scope)
            .map_err(|e| Internal::invariant("sql_rewriter::full_outer", e))
    };
    let null_check = DomainExpression::Binary {
        left: Box::new(null_check_col),
        op: BinaryOperator::Is,
        right: Box::new(DomainExpression::Literal(
            crate::pipeline::asts::core::LiteralValue::Null,
        )),
    };
    let branch1 = branch(&left, &right, None)?;
    let branch2 = branch(&right, &left, Some(null_check))?;

    let union = QueryExpression::SetOperation {
        op: SetOperator::UnionAll,
        left: Box::new(QueryExpression::Select(Box::new(branch1))),
        right: Box::new(QueryExpression::Select(Box::new(branch2))),
    };
    let sub = TableExpression::Subquery {
        query: Box::new(stacksafe::StackSafe::new(union)),
        alias: carrier_scope,
    };

    // The outer SELECT: original clauses over the union, operand refs
    // rewritten to the mangled subquery columns.
    let mut builder = SelectStatement::builder();
    if stmt.is_distinct() {
        builder = builder.distinct();
    }
    builder = builder.select_all(
        stmt.select_list()
            .iter()
            .map(|item| match item.expr() {
                Some(expr) => item.with_expr(rewrite_operand_refs(expr.clone(), &replacements)),
                None => item.clone(),
            })
            .collect(),
    );
    builder = builder.from_tables(vec![sub]);
    if let Some(gb) = stmt.group_by() {
        builder = builder.group_by(
            gb.iter()
                .map(|e| rewrite_operand_refs(e.clone(), &replacements))
                .collect(),
        );
    }
    if let Some(h) = stmt.having() {
        builder = builder.having(rewrite_operand_refs(h.clone(), &replacements));
    }
    if let Some(ob) = stmt.order_by() {
        for term in ob {
            builder = builder.order_by(crate::pipeline::sql_ast::OrderTerm::new(
                rewrite_operand_refs(term.expr().clone(), &replacements),
                term.direction().cloned(),
            ));
        }
    }
    if let Some(lim) = stmt.limit() {
        builder = builder.limit_from(lim.clone());
    }

    builder
        .rebuilding(stmt)
        .map(|s| QueryExpression::Select(Box::new(s)))
        .map_err(|e| {
            Internal::invariant(
                "sql_rewriter::full_outer",
                format!("sql_rewriter full_outer aggregated rebuild: {}", e),
            )
        })
}

/// Collect every column ref owned by a join operand. Does not descend
/// into subquery bodies.
fn collect_operand_refs(
    expr: &DomainExpression,
    operand_scopes: &[crate::names::ScopeId; 2],
    identities: &crate::names::Registry,
    out: &mut Vec<crate::names::ColId>,
) {
    let walk = |e: &DomainExpression, out: &mut Vec<crate::names::ColId>| {
        collect_operand_refs(e, operand_scopes, identities, out)
    };
    match expr {
        DomainExpression::Column(column) => {
            if operand_scopes.contains(&identities.scope_of(*column)) && !out.contains(column) {
                out.push(*column);
            }
        }
        DomainExpression::Binary { left, right, .. } => {
            walk(left, out);
            walk(right, out);
        }
        DomainExpression::Unary { expr, .. }
        | DomainExpression::Parens(expr)
        | DomainExpression::Cast { expr, .. } => walk(expr, out),
        DomainExpression::Function { args, .. }
        | DomainExpression::PredicateRewrite { args, .. } => {
            for a in args {
                walk(a, out);
            }
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            if let Some(e) = expr.as_deref() {
                walk(e, out);
            }
            for w in when_clauses {
                walk(w.when(), out);
                walk(w.then(), out);
            }
            if let Some(e) = else_clause.as_deref() {
                walk(e, out);
            }
        }
        DomainExpression::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for a in args.iter().chain(partition_by.iter()) {
                walk(a, out);
            }
            for (e, _) in order_by {
                walk(e, out);
            }
        }
        _ => {}
    }
}

/// Rewrite operand column refs to the carrier's republished columns.
fn rewrite_operand_refs(
    expr: DomainExpression,
    replacements: &std::collections::HashMap<crate::names::ColId, crate::names::ColId>,
) -> DomainExpression {
    let walk = |e: DomainExpression| rewrite_operand_refs(e, replacements);
    let walk_box = |e: Box<DomainExpression>| Box::new(rewrite_operand_refs(*e, replacements));
    match expr {
        DomainExpression::Column(column) => {
            DomainExpression::Column(replacements.get(&column).copied().unwrap_or(column))
        }
        DomainExpression::Binary { left, op, right } => DomainExpression::Binary {
            left: walk_box(left),
            op,
            right: walk_box(right),
        },
        DomainExpression::Unary { op, expr } => DomainExpression::Unary {
            op,
            expr: walk_box(expr),
        },
        DomainExpression::Parens(inner) => DomainExpression::Parens(walk_box(inner)),
        DomainExpression::Cast { expr, type_name } => DomainExpression::Cast {
            expr: walk_box(expr),
            type_name,
        },
        DomainExpression::Function {
            name,
            args,
            distinct,
        } => DomainExpression::Function {
            name,
            args: args.into_iter().map(walk).collect(),
            distinct,
        },
        DomainExpression::PredicateRewrite {
            name,
            namespace,
            args,
            negated,
        } => DomainExpression::PredicateRewrite {
            name,
            namespace,
            args: args.into_iter().map(walk).collect(),
            negated,
        },
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => DomainExpression::Case {
            expr: expr.map(walk_box),
            when_clauses: when_clauses
                .into_iter()
                .map(|w| {
                    let (when, then) = (w.when().clone(), w.then().clone());
                    crate::pipeline::sql_ast::WhenClause::new(walk(when), walk(then))
                })
                .collect(),
            else_clause: else_clause.map(walk_box),
        },
        DomainExpression::WindowFunction {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            frame,
        } => DomainExpression::WindowFunction {
            name,
            args: args.into_iter().map(walk).collect(),
            distinct,
            partition_by: partition_by.into_iter().map(walk).collect(),
            order_by: order_by.into_iter().map(|(e, d)| (walk(e), d)).collect(),
            frame,
        },
        other => other,
    }
}

/// Recurse into subqueries within table expressions (for nested FULL OUTERs)
fn rewrite_table_subqueries(
    table: TableExpression,
    identities: &crate::names::Registry,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<TableExpression> {
    match table {
        TableExpression::Subquery { query, alias } => {
            let rewritten = rewrite_query((*query).into_inner(), identities, target)?;
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
            let left = Box::new(rewrite_table_subqueries(*left, identities, target)?);
            let right = Box::new(rewrite_table_subqueries(*right, identities, target)?);
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
        builder = builder.limit_from(lim.clone());
    }

    builder.rebuilding(stmt).map_err(|e| {
        Internal::invariant(
            "sql_rewriter::full_outer",
            format!("sql_rewriter full_outer rebuild: {}", e),
        )
    })
}

/// The single alias under which a join operand is addressable. The
/// transformer subquery-wraps nested joins, so operands here always
/// carry one alias; None only for shapes this rewriter never receives.
fn operand_scope(table: &TableExpression) -> Option<crate::names::ScopeId> {
    match table {
        TableExpression::Scope(scope) => Some(*scope),
        TableExpression::Entity { alias, .. } => *alias,
        TableExpression::Subquery { alias, .. } => Some(*alias),
        TableExpression::TVF { alias, .. } => Some(*alias),
        _ => None,
    }
}

/// Extract a preserved-side column proven non-NULL whenever ON is TRUE.
/// In `B LEFT JOIN A`, that column is NULL on a padded A and non-NULL on
/// every matched A. An arbitrary referenced column cannot serve as this
/// witness: a null-accepting condition may match a real row with it NULL.
fn extract_null_check_column(
    condition: &JoinCondition,
    left_scope: Option<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) -> Result<DomainExpression> {
    match condition {
        JoinCondition::On(expr) => {
            let column = null_rejecting_witnesses(expr, left_scope, identities)
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DelightQLError::from(Constraint::Unsupported {
                        message: "FULL OUTER JOIN: no null-rejecting preserved-side column in ON condition for unmatched-row check"
                            .to_string(),
                    })
                })?;
            Ok(DomainExpression::Column(column))
        }
        JoinCondition::Cartesian => Err(DelightQLError::from(Constraint::Unsupported {
            message: "FULL OUTER JOIN with NATURAL is not supported".to_string(),
        })),
    }
}

/// Every returned column is non-NULL whenever this truth is TRUE. Retain all
/// candidates while composing: AND unions proofs, OR intersects them. Only
/// after the complete truth is judged may the expansion choose one sentinel.
fn null_rejecting_witnesses(
    expr: &DomainExpression,
    of_scope: Option<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) -> BTreeSet<crate::names::ColId> {
    match expr {
        DomainExpression::Binary { left, op, right } => match op {
            BinaryOperator::And => {
                let mut proven = null_rejecting_witnesses(left, of_scope, identities);
                proven.extend(null_rejecting_witnesses(right, of_scope, identities));
                proven
            }
            BinaryOperator::Or => {
                let left = null_rejecting_witnesses(left, of_scope, identities);
                let right = null_rejecting_witnesses(right, of_scope, identities);
                left.intersection(&right).copied().collect()
            }
            BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::Like
            | BinaryOperator::NotLike => {
                let mut proven = strict_operand_columns(left, of_scope, identities);
                proven.extend(strict_operand_columns(right, of_scope, identities));
                proven
            }
            BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::Concatenate
            | BinaryOperator::Is
            | BinaryOperator::IsNot
            | BinaryOperator::IsNotDistinctFrom
            | BinaryOperator::IsDistinctFrom => BTreeSet::new(),
        },
        DomainExpression::PredicateRewrite {
            name,
            namespace,
            args,
            ..
        } => {
            if let Some((left, right)) =
                crate::bin_cartridge::prelude::strict_comparison_operands(namespace, name, args)
            {
                let mut proven = strict_operand_columns(left, of_scope, identities);
                proven.extend(strict_operand_columns(right, of_scope, identities));
                proven
            } else {
                BTreeSet::new()
            }
        }
        DomainExpression::Observation {
            expr,
            positive: true,
        }
        | DomainExpression::Parens(expr) => null_rejecting_witnesses(expr, of_scope, identities),
        DomainExpression::Column(_)
        | DomainExpression::Literal(_)
        | DomainExpression::PublishedNameLiteral(_)
        | DomainExpression::PublishedJsonPathLiteral(_)
        | DomainExpression::JsonPathLiteral(_)
        | DomainExpression::ScopeNameLiteral(_)
        | DomainExpression::Cast { .. }
        | DomainExpression::Unary { .. }
        | DomainExpression::Function { .. }
        | DomainExpression::Star
        | DomainExpression::Case { .. }
        | DomainExpression::Exists { .. }
        | DomainExpression::Subquery(_)
        | DomainExpression::WindowFunction { .. }
        | DomainExpression::Observation {
            positive: false, ..
        } => BTreeSet::new(),
    }
}

/// A non-NULL value of a known NULL-propagating expression proves every
/// operand below it non-NULL. A general function makes no such promise: it
/// may replace NULL with a value.
fn strict_operand_columns(
    expr: &DomainExpression,
    of_scope: Option<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) -> BTreeSet<crate::names::ColId> {
    match expr {
        DomainExpression::Column(column)
            if of_scope.is_some_and(|scope| identities.scope_of(*column) == scope) =>
        {
            BTreeSet::from([*column])
        }
        DomainExpression::Parens(expr)
        | DomainExpression::Cast { expr, .. }
        | DomainExpression::Unary { expr, .. } => {
            strict_operand_columns(expr, of_scope, identities)
        }
        // The exact operand is its value under another comparison: NULL
        // exactly where its argument is.
        DomainExpression::Function {
            name: crate::pipeline::sql_ast::FunctionName::Intrinsic(crate::names::Intrinsic::Exact),
            args,
            ..
        } => args
            .iter()
            .flat_map(|arg| strict_operand_columns(arg, of_scope, identities))
            .collect(),
        DomainExpression::Binary { left, op, right } => match op {
            BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::Concatenate => {
                let mut proven = strict_operand_columns(left, of_scope, identities);
                proven.extend(strict_operand_columns(right, of_scope, identities));
                proven
            }
            BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Like
            | BinaryOperator::NotLike
            | BinaryOperator::Is
            | BinaryOperator::IsNot
            | BinaryOperator::IsNotDistinctFrom
            | BinaryOperator::IsDistinctFrom => BTreeSet::new(),
        },
        DomainExpression::Column(_)
        | DomainExpression::Literal(_)
        | DomainExpression::PublishedNameLiteral(_)
        | DomainExpression::PublishedJsonPathLiteral(_)
        | DomainExpression::JsonPathLiteral(_)
        | DomainExpression::ScopeNameLiteral(_)
        | DomainExpression::Function { .. }
        | DomainExpression::Star
        | DomainExpression::Case { .. }
        | DomainExpression::Exists { .. }
        | DomainExpression::Subquery(_)
        | DomainExpression::WindowFunction { .. }
        | DomainExpression::PredicateRewrite { .. }
        | DomainExpression::Observation { .. } => BTreeSet::new(),
    }
}

#[cfg(test)]
mod witness_tests {
    use super::*;
    use crate::names::{Addressing, Registry};
    use crate::pipeline::sql_ast::FunctionName;

    #[test]
    fn a_full_join_witness_is_entailed_by_every_true_match() {
        let names = Registry::new(&[]);
        let left = names.anonymous_scope(None);
        let right = names.anonymous_scope(None);
        let left_key = names.sql_column(left, None, Addressing::Published);
        let left_other = names.sql_column(left, None, Addressing::Published);
        let right_key = names.sql_column(right, None, Addressing::Published);
        let eq = |column| DomainExpression::Binary {
            left: Box::new(DomainExpression::Column(column)),
            op: BinaryOperator::Equal,
            right: Box::new(DomainExpression::Column(right_key)),
        };
        let proofs = |expr: &DomainExpression| null_rejecting_witnesses(expr, Some(left), &names);
        let proof = |expr: &DomainExpression| proofs(expr).into_iter().next();

        assert_eq!(proof(&eq(left_key)), Some(left_key));
        assert_eq!(
            proof(&DomainExpression::Observation {
                expr: Box::new(eq(left_key)),
                positive: true,
            }),
            Some(left_key)
        );
        assert_eq!(
            proof(&DomainExpression::Binary {
                left: Box::new(eq(left_key)),
                op: BinaryOperator::And,
                right: Box::new(DomainExpression::Column(left_other)),
            }),
            Some(left_key)
        );
        assert_eq!(
            proof(&DomainExpression::Binary {
                left: Box::new(eq(left_key)),
                op: BinaryOperator::Or,
                right: Box::new(eq(left_key)),
            }),
            Some(left_key)
        );
        let both = |first, second| DomainExpression::Binary {
            left: Box::new(eq(first)),
            op: BinaryOperator::And,
            right: Box::new(eq(second)),
        };
        let reordered = DomainExpression::Binary {
            left: Box::new(both(left_key, left_other)),
            op: BinaryOperator::Or,
            right: Box::new(both(left_other, left_key)),
        };
        assert_eq!(proofs(&reordered), BTreeSet::from([left_key, left_other]));
        for op in [
            BinaryOperator::Add,
            BinaryOperator::Subtract,
            BinaryOperator::Multiply,
            BinaryOperator::Divide,
            BinaryOperator::Modulo,
            BinaryOperator::Concatenate,
        ] {
            assert_eq!(
                proof(&DomainExpression::Binary {
                    left: Box::new(DomainExpression::Binary {
                        left: Box::new(DomainExpression::Column(left_key)),
                        op,
                        right: Box::new(DomainExpression::Literal(
                            crate::pipeline::asts::core::LiteralValue::integer(1),
                        )),
                    }),
                    op: BinaryOperator::Equal,
                    right: Box::new(DomainExpression::Column(right_key)),
                }),
                Some(left_key)
            );
        }
        assert_eq!(
            proof(&DomainExpression::PredicateRewrite {
                name: "sql_eq".to_string(),
                namespace: vec!["std".to_string(), "prelude".to_string()],
                args: vec![
                    DomainExpression::Column(left_key),
                    DomainExpression::Column(right_key)
                ],
                negated: false,
            }),
            Some(left_key)
        );

        assert_eq!(
            proof(&DomainExpression::Observation {
                expr: Box::new(eq(left_key)),
                positive: false,
            }),
            None
        );
        assert_eq!(
            proof(&DomainExpression::Binary {
                left: Box::new(eq(left_key)),
                op: BinaryOperator::Or,
                right: Box::new(eq(left_other)),
            }),
            None
        );
        assert_eq!(
            proof(&DomainExpression::Binary {
                left: Box::new(DomainExpression::Column(left_key)),
                op: BinaryOperator::IsNotDistinctFrom,
                right: Box::new(DomainExpression::Column(right_key)),
            }),
            None
        );
        assert_eq!(
            proof(&DomainExpression::Binary {
                left: Box::new(DomainExpression::Function {
                    name: FunctionName::from("coalesce"),
                    args: vec![
                        DomainExpression::Column(left_key),
                        DomainExpression::Literal(
                            crate::pipeline::asts::core::LiteralValue::integer(1),
                        ),
                    ],
                    distinct: false,
                }),
                op: BinaryOperator::Equal,
                right: Box::new(DomainExpression::Column(right_key)),
            }),
            None
        );
        assert_eq!(
            proof(&DomainExpression::PredicateRewrite {
                name: "sql_eq".to_string(),
                namespace: vec!["caller".to_string()],
                args: vec![
                    DomainExpression::Column(left_key),
                    DomainExpression::Column(right_key)
                ],
                negated: false,
            }),
            None
        );
    }
}
