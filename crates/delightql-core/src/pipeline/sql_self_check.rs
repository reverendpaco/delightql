// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Post-lowering binding verification over the final structural SQL AST.
//!
//! This pass independently rebuilds the scopes visible at every select.
//! A reference is valid only when its owning `ScopeId` is visible and,
//! for a derived scope with an enumerable heading, its `ColId` is one of
//! that scope's outputs.

use std::collections::HashMap;

use crate::error::{DelightQLError, Result};
use crate::names::{ColId, Registry, ScopeId};
use crate::pipeline::sql_ast::{
    Cte, DomainExpression, JoinCondition, QueryExpression, SelectItem, SelectStatement,
    SqlStatement, TableExpression, TvfArgument,
};

#[derive(Debug, Clone)]
enum ColumnSet {
    Open,
    Known(Vec<ColId>),
}

impl ColumnSet {
    fn contains(&self, column: ColId) -> bool {
        match self {
            ColumnSet::Open => true,
            ColumnSet::Known(columns) => columns.contains(&column),
        }
    }
}

type Frame = HashMap<ScopeId, ColumnSet>;
type CteEnv = HashMap<ScopeId, ColumnSet>;

pub fn check(stmt: &SqlStatement, identities: &Registry) -> Result<()> {
    let mut ctes = CteEnv::new();
    match stmt {
        // A name and nothing else: no publication to check.
        SqlStatement::DropTempTable { .. } => {}
        SqlStatement::Query { with_clause, query }
        | SqlStatement::CreateTempTable {
            with_clause, query, ..
        }
        | SqlStatement::CreateTempView {
            with_clause, query, ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes, identities)?;
            check_query(query, &[], &ctes, identities)?;
        }
        SqlStatement::Delete {
            target_scope,
            with_clause,
            where_clause,
            ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes, identities)?;
            let stack = [HashMap::from([(*target_scope, ColumnSet::Open)])];
            if let Some(expr) = where_clause {
                check_expr(expr, &stack, &ctes, identities)?;
            }
        }
        SqlStatement::Update {
            target_scope,
            with_clause,
            set_clause,
            where_clause,
            ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes, identities)?;
            let stack = [HashMap::from([(*target_scope, ColumnSet::Open)])];
            for (_, expr) in set_clause {
                check_expr(expr, &stack, &ctes, identities)?;
            }
            if let Some(expr) = where_clause {
                check_expr(expr, &stack, &ctes, identities)?;
            }
        }
        SqlStatement::Insert {
            with_clause,
            source,
            ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes, identities)?;
            check_query(source, &[], &ctes, identities)?;
        }
    }
    Ok(())
}

fn check_cte_list(
    list: Option<&[Cte]>,
    stack: &[Frame],
    ctes: &mut CteEnv,
    identities: &Registry,
) -> Result<()> {
    for cte in list.into_iter().flatten() {
        if cte.is_recursive() {
            ctes.insert(cte.scope(), ColumnSet::Open);
        }
        let output = check_query(cte.query(), stack, ctes, identities)?;
        ctes.insert(cte.scope(), output);
    }
    Ok(())
}

#[stacksafe::stacksafe]
fn check_query(
    query: &QueryExpression,
    stack: &[Frame],
    ctes: &CteEnv,
    identities: &Registry,
) -> Result<ColumnSet> {
    match query {
        QueryExpression::Select(select) => check_select(select, stack, ctes, identities),
        QueryExpression::SetOperation { left, right, .. } => {
            let output = check_query(left, stack, ctes, identities)?;
            check_query(right, stack, ctes, identities)?;
            Ok(output)
        }
        QueryExpression::Values { rows } => {
            for row in rows {
                for expr in row {
                    check_expr(expr, stack, ctes, identities)?;
                }
            }
            Ok(ColumnSet::Open)
        }
        QueryExpression::WithCte { ctes: inner, query } => {
            let mut extended = ctes.clone();
            check_cte_list(Some(inner), stack, &mut extended, identities)?;
            check_query(query, stack, &extended, identities)
        }
    }
}

#[stacksafe::stacksafe]
fn check_select(
    select: &SelectStatement,
    stack: &[Frame],
    ctes: &CteEnv,
    identities: &Registry,
) -> Result<ColumnSet> {
    let mut frame = Frame::new();
    let mut join_conditions = Vec::new();
    let mut tvf_args = Vec::new();
    for table in select.from().into_iter().flatten() {
        collect_from(
            table,
            stack,
            ctes,
            identities,
            &mut frame,
            &mut join_conditions,
            &mut tvf_args,
        )?;
    }

    // Output aliases are visible to ORDER BY on every supported target.
    // Making them visible to the other clauses keeps this self-check
    // deliberately lenient where dialects disagree.
    for item in select.select_list() {
        if let SelectItem::Expression {
            alias: Some(column),
            ..
        } = item
        {
            frame
                .entry(identities.scope_of(*column))
                .or_insert_with(|| ColumnSet::Known(Vec::new()));
            if let Some(ColumnSet::Known(columns)) = frame.get_mut(&identities.scope_of(*column)) {
                columns.push(*column);
            }
        }
    }

    let mut full_stack = stack.to_vec();
    full_stack.push(frame);

    for condition in join_conditions {
        check_expr(condition, &full_stack, ctes, identities)?;
    }
    for argument in tvf_args {
        if let TvfArgument::Column(column) = argument {
            check_reference(*column, &full_stack, identities)?;
        }
    }
    for expr in select.group_by().into_iter().flatten() {
        check_expr(expr, &full_stack, ctes, identities)?;
    }
    if let Some(expr) = select.having() {
        check_expr(expr, &full_stack, ctes, identities)?;
    }
    if let Some(expr) = select.where_clause() {
        check_expr(expr, &full_stack, ctes, identities)?;
    }
    for term in select.order_by().into_iter().flatten() {
        check_expr(term.expr(), &full_stack, ctes, identities)?;
    }

    let mut output = Vec::new();
    let mut open = false;
    for item in select.select_list() {
        match item {
            SelectItem::Star { .. } => open = true,
            SelectItem::Expression { expr, alias } => {
                check_expr(expr, &full_stack, ctes, identities)?;
                match (alias, expr) {
                    (Some(column), _) => output.push(*column),
                    (None, DomainExpression::Column(column)) => output.push(*column),
                    (None, _) => open = true,
                }
            }
        }
    }
    Ok(if open {
        ColumnSet::Open
    } else {
        ColumnSet::Known(output)
    })
}

#[stacksafe::stacksafe]
fn collect_from<'a>(
    table: &'a TableExpression,
    stack: &[Frame],
    ctes: &CteEnv,
    identities: &Registry,
    frame: &mut Frame,
    join_conditions: &mut Vec<&'a DomainExpression>,
    tvf_args: &mut Vec<&'a TvfArgument>,
) -> Result<()> {
    match table {
        TableExpression::Scope(scope) | TableExpression::QualifiedScope { scope, .. } => {
            frame.insert(*scope, ctes.get(scope).cloned().unwrap_or(ColumnSet::Open));
        }
        TableExpression::Entity {
            alias: Some(scope), ..
        } => {
            frame.insert(*scope, ColumnSet::Open);
        }
        TableExpression::Entity { alias: None, .. } => {}
        TableExpression::Subquery { query, alias } => {
            let columns = check_query(query, stack, ctes, identities)?;
            frame.insert(*alias, columns);
        }
        TableExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            collect_from(
                left,
                stack,
                ctes,
                identities,
                frame,
                join_conditions,
                tvf_args,
            )?;
            collect_from(
                right,
                stack,
                ctes,
                identities,
                frame,
                join_conditions,
                tvf_args,
            )?;
            if let JoinCondition::On(expr) = join_condition {
                join_conditions.push(expr);
            }
        }
        TableExpression::TVF {
            arguments, alias, ..
        } => {
            frame.insert(*alias, ColumnSet::Open);
            tvf_args.extend(arguments);
        }
    }
    Ok(())
}

#[stacksafe::stacksafe]
fn check_expr(
    expr: &DomainExpression,
    stack: &[Frame],
    ctes: &CteEnv,
    identities: &Registry,
) -> Result<()> {
    match expr {
        DomainExpression::Column(column) => check_reference(*column, stack, identities),
        DomainExpression::Literal(_)
        | DomainExpression::PublishedNameLiteral(_)
        | DomainExpression::PublishedJsonPathLiteral(_)
        | DomainExpression::JsonPathLiteral(_)
        | DomainExpression::ScopeNameLiteral(_)
        | DomainExpression::Star => Ok(()),
        DomainExpression::Cast { expr, .. }
        | DomainExpression::Unary { expr, .. }
        | DomainExpression::Observation { expr, .. }
        | DomainExpression::Parens(expr) => check_expr(expr, stack, ctes, identities),
        DomainExpression::Binary { left, right, .. } => {
            check_expr(left, stack, ctes, identities)?;
            check_expr(right, stack, ctes, identities)
        }
        DomainExpression::Function { args, .. }
        | DomainExpression::PredicateRewrite { args, .. } => {
            check_exprs(args, stack, ctes, identities)
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            if let Some(expr) = expr {
                check_expr(expr, stack, ctes, identities)?;
            }
            for clause in when_clauses {
                check_expr(clause.when(), stack, ctes, identities)?;
                check_expr(clause.then(), stack, ctes, identities)?;
            }
            if let Some(expr) = else_clause {
                check_expr(expr, stack, ctes, identities)?;
            }
            Ok(())
        }
        DomainExpression::Exists { query, .. } | DomainExpression::Subquery(query) => {
            check_query(query, stack, ctes, identities).map(|_| ())
        }
        DomainExpression::WindowFunction {
            args,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            check_exprs(args, stack, ctes, identities)?;
            check_exprs(partition_by, stack, ctes, identities)?;
            for (expr, _) in order_by {
                check_expr(expr, stack, ctes, identities)?;
            }
            if let Some(frame) = frame {
                use crate::pipeline::sql_ast::SqlFrameBound;
                for bound in [&frame.start, &frame.end] {
                    if let SqlFrameBound::Preceding(expr) | SqlFrameBound::Following(expr) = bound {
                        check_expr(expr, stack, ctes, identities)?;
                    }
                }
            }
            Ok(())
        }
    }
}

fn check_exprs(
    expressions: &[DomainExpression],
    stack: &[Frame],
    ctes: &CteEnv,
    identities: &Registry,
) -> Result<()> {
    for expr in expressions {
        check_expr(expr, stack, ctes, identities)?;
    }
    Ok(())
}

fn check_reference(column: ColId, stack: &[Frame], identities: &Registry) -> Result<()> {
    let scope = identities.scope_of(column);
    let Some(columns) = resolve_scope(scope, stack) else {
        crate::probe::probing!(selfcheck, {
            crate::probe::probe!(
                selfcheck,
                "dangling {:?} owner {scope:?} {:?}",
                crate::probe::chain(identities, column),
                identities.origin_of(scope)
            );
            for frame in stack {
                for visible in frame.keys() {
                    crate::probe::probe!(
                        selfcheck,
                        "  visible {visible:?} {:?}",
                        identities.origin_of(*visible)
                    );
                }
            }
        });
        return Err(dangling(scope, Some(column), stack, identities));
    };
    if columns.contains(column) {
        return Ok(());
    }
    crate::probe::probing!(selfcheck, {
        crate::probe::probe!(
            selfcheck,
            "unpublished {:?} owner {scope:?} {:?}",
            crate::probe::chain(identities, column),
            identities.origin_of(scope)
        );
        if let ColumnSet::Known(outputs) = columns {
            for output in outputs {
                crate::probe::probe!(
                    selfcheck,
                    "  owner outputs {:?}",
                    crate::probe::chain(identities, *output)
                );
            }
        }
        crate::probe::probe!(
            selfcheck,
            "  owner heading {:?}",
            identities.known_heading(scope)?
        );
    });
    Err(DelightQLError::validation_error_categorized(
        "transform/self_check/unknown_column",
        format!(
            "SQL self-check: {} is not an output of its visible owner {}",
            tell_column(identities, column),
            tell_scope(identities, scope)
        ),
        "internal invariant violation: the transpiler referenced a column its own \
         derived scope does not output; please report the query that produced this",
    ))
}

fn resolve_scope(scope: ScopeId, stack: &[Frame]) -> Option<&ColumnSet> {
    stack.iter().rev().find_map(|frame| frame.get(&scope))
}

/// Spell a column for whoever reads the refusal.
///
/// The index stays, because it is what a bug report needs and what the
/// probes print. It is not what anyone wrote, so the published name goes
/// beside it — a reference reported only as `col#3` tells a reader nothing
/// they can look for in their own query.
fn tell_column(identities: &Registry, column: ColId) -> String {
    match identities.published(column) {
        Some(spelling) => {
            let mut text = String::new();
            identities.write(spelling, &mut crate::names::Teaching(&mut text));
            format!("{column:?} `{text}`")
        }
        None => format!("{column:?} (unnamed)"),
    }
}

/// Spell a scope the same way. `describe` is the only teaching road a
/// compiler-minted scope has — it has no name until baptism — and saying
/// "a compiler wrap" is what distinguishes a boundary the transpiler
/// inserted from one the query asked for.
fn tell_scope(identities: &Registry, scope: ScopeId) -> String {
    let mut text = String::new();
    identities.describe(scope, &mut crate::names::Teaching(&mut text));
    format!("{scope:?} ({text})")
}

fn dangling(
    scope: ScopeId,
    column: Option<ColId>,
    stack: &[Frame],
    identities: &Registry,
) -> DelightQLError {
    let mut visible: Vec<_> = stack
        .iter()
        .flat_map(|frame| frame.keys().copied())
        .collect();
    visible.sort();
    visible.dedup();
    let visible: Vec<_> = visible
        .into_iter()
        .map(|scope| tell_scope(identities, scope))
        .collect();
    let what = match column {
        Some(column) => format!("reference {}", tell_column(identities, column)),
        None => "a qualified star".to_string(),
    };
    DelightQLError::validation_error_categorized(
        "transform/self_check/dangling_qualifier",
        format!(
            "SQL self-check: {what} is owned by {}, which is not visible on its \
             path (visible scopes: {})",
            tell_scope(identities, scope),
            visible.join(", ")
        ),
        "internal invariant violation: the transpiler emitted a dangling scope; \
         please report the query that produced this",
    )
}
