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

use crate::error::{DelightQLError, Result};
use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::{
    BinaryOperator, DomainExpression, JoinCondition, JoinType, QueryExpression, SelectItem,
    SelectStatement, SetOperator, SqlStatement, TableExpression,
};

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
) -> Result<SqlStatement> {
    match stmt {
        SqlStatement::Query { with_clause, query } => {
            let rewritten = rewrite_query(query, identities)?;
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
) -> Result<QueryExpression> {
    match query {
        QueryExpression::Select(select) => rewrite_select_query(*select, identities),
        QueryExpression::SetOperation { op, left, right } => {
            let left = Box::new(rewrite_query(*left, identities)?);
            let right = Box::new(rewrite_query(*right, identities)?);
            Ok(QueryExpression::SetOperation { op, left, right })
        }
        QueryExpression::WithCte { ctes, query } => {
            let ctes = ctes
                .into_iter()
                .map(|cte| {
                    let rewritten = rewrite_query(cte.query().clone(), identities)?;
                    Ok(cte.with_query(rewritten))
                })
                .collect::<Result<Vec<_>>>()?;
            let query = Box::new(rewrite_query(*query, identities)?);
            Ok(QueryExpression::WithCte { ctes, query })
        }
        other => Ok(other),
    }
}

/// Rewrite a SELECT. If its FROM contains a top-level FULL OUTER JOIN,
/// expand the entire SELECT into a UNION ALL of two LEFT JOINs.
/// If the FULL OUTER is nested deeper, recurse into subqueries.
fn rewrite_select_query(
    stmt: SelectStatement,
    identities: &crate::names::Registry,
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
            if computes_over_whole_relation(&stmt) {
                return expand_full_outer_select_aggregated(
                    &stmt,
                    left,
                    right,
                    join_condition,
                    identities,
                );
            }
            return expand_full_outer_select(&stmt, left, right, join_condition, identities);
        }
    }

    // No top-level FULL OUTER — recurse into subqueries within FROM
    let new_from: Vec<TableExpression> = from
        .iter()
        .map(|t| rewrite_table_subqueries(t.clone(), identities))
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
) -> Result<QueryExpression> {
    // First, recursively expand any FULL OUTERs in the children
    let left = rewrite_table_subqueries(left.clone(), identities)?;
    let right = rewrite_table_subqueries(right.clone(), identities)?;

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

/// SQL group-aggregate function names (lowercase). Detection keys the
/// aggregated expansion; a name missing here silently reverts that
/// query to the per-branch (per-part) computation.
const AGGREGATE_FNS: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "total",
    "group_concat",
    "string_agg",
    "array_agg",
    "listagg",
    "json_group_array",
    "json_group_object",
    "jsonb_group_array",
    "jsonb_group_object",
    "every",
    "some",
    "bool_and",
    "bool_or",
    "bit_and",
    "bit_or",
    "bit_xor",
    "var_pop",
    "var_samp",
    "variance",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "median",
    "mode",
];

/// Does this SELECT compute something over the WHOLE relation — an
/// aggregate, DISTINCT, GROUP BY/HAVING, or a window function — such
/// that computing it once per expansion branch would be wrong?
fn computes_over_whole_relation(stmt: &SelectStatement) -> bool {
    stmt.is_distinct()
        || stmt.group_by().is_some()
        || stmt.having().is_some()
        || stmt.select_list().iter().any(|item| match item {
            SelectItem::Expression { expr, .. } => contains_whole_relation_fn(expr),
            _ => false,
        })
}

fn contains_whole_relation_fn(expr: &DomainExpression) -> bool {
    match expr {
        DomainExpression::Function { name, args, .. } => {
            name.user()
                .is_some_and(|name| AGGREGATE_FNS.contains(&name.to_lowercase().as_str()))
                || args.iter().any(contains_whole_relation_fn)
        }
        DomainExpression::WindowFunction { .. } => true,
        DomainExpression::Binary { left, right, .. } => {
            contains_whole_relation_fn(left) || contains_whole_relation_fn(right)
        }
        DomainExpression::Unary { expr, .. }
        | DomainExpression::Parens(expr)
        | DomainExpression::Cast { expr, .. } => contains_whole_relation_fn(expr),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_deref().is_some_and(contains_whole_relation_fn)
                || when_clauses.iter().any(|w| {
                    contains_whole_relation_fn(w.when()) || contains_whole_relation_fn(w.then())
                })
                || else_clause
                    .as_deref()
                    .is_some_and(contains_whole_relation_fn)
        }
        DomainExpression::PredicateRewrite { args, .. } => {
            args.iter().any(contains_whole_relation_fn)
        }
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
) -> Result<QueryExpression> {
    // Recursively expand any FULL OUTERs in the children first.
    let left = rewrite_table_subqueries(left.clone(), identities)?;
    let right = rewrite_table_subqueries(right.clone(), identities)?;

    let (Some(left_scope), Some(right_scope)) = (operand_scope(&left), operand_scope(&right))
    else {
        return Err(DelightQLError::ParseError {
            message: "FULL OUTER JOIN under an aggregate: operands must carry aliases".to_string(),
            source: None,
            subcategory: None,
        });
    };
    let operand_scopes = [left_scope, right_scope];

    let null_check_col = extract_null_check_column(condition, Some(left_scope), identities)?;

    // Every operand column the outer clauses reference.
    let mut refs: Vec<crate::names::ColId> = Vec::new();
    for item in stmt.select_list() {
        if let SelectItem::Expression { expr, .. } = item {
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

    let join_scope = identities.mint_scope(
        crate::names::ScopeOrigin::Join {
            left: left_scope,
            right: right_scope,
        },
        crate::names::Hint::None,
        None,
    );
    let carrier_scope = identities.mint_derived_scope(
        crate::names::ScopeOrigin::Wrap {
            input: join_scope,
            why: crate::names::WrapReason::SetOperation,
        },
        crate::names::Hint::None,
    );
    let mut replacements = std::collections::HashMap::new();
    let inner_items: Vec<SelectItem> = if refs.is_empty() {
        // count(*)-style: no column refs — the branches still must
        // produce one column per row.
        let output = identities.mint_column(
            carrier_scope,
            crate::names::ColumnOrigin::Computed {
                via: crate::names::Computation::Literal,
            },
            None,
            crate::names::Addressing::Hygienic,
            crate::names::ValueFacts::default(),
        );
        vec![SelectItem::Expression {
            expr: DomainExpression::Literal(crate::pipeline::ast_refined::LiteralValue::Number(
                "1".to_string(),
            )),
            alias: Some(output),
        }]
    } else {
        refs.iter()
            .map(|source| {
                let output = identities.mint_column(
                    carrier_scope,
                    crate::names::ColumnOrigin::Republished {
                        from: *source,
                        how: crate::names::Republish::BoundaryExport,
                    },
                    identities.published(*source),
                    crate::names::Addressing::Published,
                    identities.facts(*source),
                );
                replacements.insert(*source, output);
                SelectItem::Expression {
                    expr: DomainExpression::Column(*source),
                    alias: Some(output),
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
        crate::pipeline::transformer::builder::publish_at(
            carrier_scope,
            inner_items
                .iter()
                .filter_map(|item| match item.publishes() {
                    crate::pipeline::sql_ast::Publishes::One(column) => Some(column),
                    _ => None,
                }),
            b,
            identities,
        )
    };
    let null_check = DomainExpression::Binary {
        left: Box::new(null_check_col),
        op: BinaryOperator::Is,
        right: Box::new(DomainExpression::Literal(
            crate::pipeline::ast_refined::LiteralValue::Null,
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
            .map(|item| match item {
                SelectItem::Expression { expr, alias } => SelectItem::Expression {
                    expr: rewrite_operand_refs(expr.clone(), &replacements),
                    alias: *alias,
                },
                other => other.clone(),
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
        .map_err(|e| DelightQLError::ParseError {
            message: format!("sql_rewriter full_outer aggregated rebuild: {}", e),
            source: None,
            subcategory: None,
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
            args,
            negated,
        } => DomainExpression::PredicateRewrite {
            name,
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
) -> Result<TableExpression> {
    match table {
        TableExpression::Subquery { query, alias } => {
            let rewritten = rewrite_query((*query).into_inner(), identities)?;
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
            let left = Box::new(rewrite_table_subqueries(*left, identities)?);
            let right = Box::new(rewrite_table_subqueries(*right, identities)?);
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

    builder
        .rebuilding(stmt)
        .map_err(|e| DelightQLError::ParseError {
            message: format!("sql_rewriter full_outer rebuild: {}", e),
            source: None,
            subcategory: None,
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

/// Extract a column for branch 2's unmatched-row test. The column MUST
/// belong to the preserved (left) operand of the original join: in
/// `B LEFT JOIN A`, only A's columns are NULL exactly on the rows the
/// first branch missed. A right-side column — the USING-coalesced name
/// resolves to the right side — always has a value there, which makes
/// branch 2 return zero rows and silently drops left-side orphans.
fn extract_null_check_column(
    condition: &JoinCondition,
    left_scope: Option<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) -> Result<DomainExpression> {
    match condition {
        JoinCondition::On(expr) => {
            find_column_of(expr, left_scope, identities).ok_or_else(|| DelightQLError::ParseError {
                message:
                    "FULL OUTER JOIN: no column of the preserved side found in ON condition for NULL check"
                        .to_string(),
                source: None,
                subcategory: None,
            })
        }
        JoinCondition::Using(cols) => {
            let Some(col) = cols
                .iter()
                .copied()
                .find(|column| left_scope == Some(identities.scope_of(*column)))
            else {
                return Err(DelightQLError::ParseError {
                    message: "FULL OUTER JOIN with USING: no preserved-side column for NULL check"
                        .to_string(),
                    source: None,
                    subcategory: None,
                });
            };
            Ok(DomainExpression::Column(col))
        }
        JoinCondition::Natural => Err(DelightQLError::ParseError {
            message: "FULL OUTER JOIN with NATURAL is not supported".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

/// First qualified column whose qualifier names `of_alias` (any
/// qualified column when no alias is known).
fn find_column_of(
    expr: &DomainExpression,
    of_scope: Option<crate::names::ScopeId>,
    identities: &crate::names::Registry,
) -> Option<DomainExpression> {
    match expr {
        DomainExpression::Column(column)
            if of_scope.is_none_or(|scope| identities.scope_of(*column) == scope) =>
        {
            Some(DomainExpression::Column(*column))
        }
        DomainExpression::Binary { left, right, .. } => find_column_of(left, of_scope, identities)
            .or_else(|| find_column_of(right, of_scope, identities)),
        DomainExpression::Parens(inner) => find_column_of(inner, of_scope, identities),
        DomainExpression::Cast { expr, .. } => find_column_of(expr, of_scope, identities),
        _ => None,
    }
}
