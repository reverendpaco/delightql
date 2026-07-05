// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// walk.rs — mutating traversal over the SQL AST.
//
// One total traversal engine, hook points via `SqlVisitorMut`: every
// `TableExpression`, `QueryExpression`, and `DomainExpression` in a
// statement, post-order (children first), descending through
// statement-level CTEs, nested WITH clauses, set operations, and
// expression-level subqueries (EXISTS / IN (SELECT …) / scalar
// subqueries / window arguments).
//
// Written for the lowering pass (`sql_rewriter`): legalizations must see
// every node, no matter how deeply an expression buries it. The visitor
// is `&mut dyn` (not generic) so the recursive fns monomorphize once.

use super::expressions::DomainExpression;
use super::query::{QueryExpression, SelectStatement};
use super::select_items::SelectItem;
use super::statements::{Cte, SqlStatement};
use super::table::{JoinCondition, TableExpression};

/// Post-order hooks over the SQL AST. Default impls are no-ops; override
/// only what you need.
pub trait SqlVisitorMut {
    fn table(&mut self, _t: &mut TableExpression) {}
    fn query(&mut self, _q: &mut QueryExpression) {}
    fn expr(&mut self, _e: &mut DomainExpression) {}
}

/// Apply the visitor to every node in the statement, post-order.
pub fn visit_mut(stmt: &mut SqlStatement, v: &mut dyn SqlVisitorMut) {
    match stmt {
        SqlStatement::Query { with_clause, query } => {
            visit_ctes(with_clause, v);
            visit_query(query, v);
        }
        SqlStatement::CreateTempTable {
            with_clause, query, ..
        }
        | SqlStatement::CreateTempView {
            with_clause, query, ..
        } => {
            visit_ctes(with_clause, v);
            visit_query(query, v);
        }
        SqlStatement::Delete {
            with_clause,
            where_clause,
            ..
        } => {
            visit_ctes(with_clause, v);
            if let Some(w) = where_clause {
                visit_expr(w, v);
            }
        }
        SqlStatement::Update {
            with_clause,
            set_clause,
            where_clause,
            ..
        } => {
            visit_ctes(with_clause, v);
            for (_, e) in set_clause {
                visit_expr(e, v);
            }
            if let Some(w) = where_clause {
                visit_expr(w, v);
            }
        }
        SqlStatement::Insert {
            with_clause, source, ..
        } => {
            visit_ctes(with_clause, v);
            visit_query(source, v);
        }
    }
}

/// Apply `f` to every `TableExpression` in the statement, post-order.
pub fn visit_tables_mut(stmt: &mut SqlStatement, f: &mut dyn FnMut(&mut TableExpression)) {
    struct Tables<'a>(&'a mut dyn FnMut(&mut TableExpression));
    impl SqlVisitorMut for Tables<'_> {
        fn table(&mut self, t: &mut TableExpression) {
            (self.0)(t)
        }
    }
    visit_mut(stmt, &mut Tables(f));
}

fn visit_ctes(ctes: &mut Option<Vec<Cte>>, v: &mut dyn SqlVisitorMut) {
    if let Some(ctes) = ctes {
        for cte in ctes {
            visit_query(cte.query_mut(), v);
        }
    }
}

/// Apply the visitor to every node under (and including) this query
/// expression, post-order. Public so passes can scan a single CTE body.
#[stacksafe::stacksafe]
pub fn visit_query(query: &mut QueryExpression, v: &mut dyn SqlVisitorMut) {
    match query {
        QueryExpression::Select(select) => visit_select(select, v),
        QueryExpression::SetOperation { left, right, .. } => {
            visit_query(left, v);
            visit_query(right, v);
        }
        QueryExpression::Values { rows } => {
            for row in rows {
                for e in row {
                    visit_expr(e, v);
                }
            }
        }
        QueryExpression::WithCte { ctes, query } => {
            for cte in ctes {
                visit_query(cte.query_mut(), v);
            }
            visit_query(query, v);
        }
    }
    v.query(query);
}

fn visit_select(select: &mut SelectStatement, v: &mut dyn SqlVisitorMut) {
    for item in &mut select.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            visit_expr(expr, v);
        }
    }
    if let Some(from) = &mut select.from {
        for t in from {
            visit_table(t, v);
        }
    }
    if let Some(w) = &mut select.where_clause {
        visit_expr(w, v);
    }
    if let Some(gb) = &mut select.group_by {
        for e in gb {
            visit_expr(e, v);
        }
    }
    if let Some(h) = &mut select.having {
        visit_expr(h, v);
    }
    if let Some(ob) = &mut select.order_by {
        for term in ob {
            visit_expr(term.expr_mut(), v);
        }
    }
    // LIMIT holds plain integers — nothing to descend into.
}

#[stacksafe::stacksafe]
fn visit_table(table: &mut TableExpression, v: &mut dyn SqlVisitorMut) {
    match table {
        TableExpression::Table { .. } | TableExpression::TVF { .. } => {}
        TableExpression::Subquery { query, .. } => visit_query(query, v),
        TableExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            visit_table(left, v);
            visit_table(right, v);
            match join_condition {
                JoinCondition::On(e) => visit_expr(e, v),
                JoinCondition::Using(_) | JoinCondition::Natural => {}
            }
        }
        TableExpression::Values { rows, .. } => {
            for row in rows {
                for e in row {
                    visit_expr(e, v);
                }
            }
        }
        TableExpression::UnionTable { selects, .. } => {
            for q in selects {
                visit_query(q, v);
            }
        }
    }
    v.table(table);
}

#[stacksafe::stacksafe]
fn visit_expr(expr: &mut DomainExpression, v: &mut dyn SqlVisitorMut) {
    match expr {
        DomainExpression::Column { .. }
        | DomainExpression::Literal(_)
        | DomainExpression::Star
        | DomainExpression::RawSql(_) => {}
        DomainExpression::Cast { expr, .. } => visit_expr(expr, v),
        DomainExpression::Binary { left, right, .. } => {
            visit_expr(left, v);
            visit_expr(right, v);
        }
        DomainExpression::Unary { expr, .. } => visit_expr(expr, v),
        DomainExpression::Function { args, .. } => {
            for a in args {
                visit_expr(a, v);
            }
        }
        DomainExpression::Parens(inner) => visit_expr(inner, v),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            if let Some(e) = expr {
                visit_expr(e, v);
            }
            for wc in when_clauses {
                visit_expr(wc.when_mut(), v);
                visit_expr(wc.then_mut(), v);
            }
            if let Some(e) = else_clause {
                visit_expr(e, v);
            }
        }
        DomainExpression::InList { expr, values, .. } => {
            visit_expr(expr, v);
            for val in values {
                visit_expr(val, v);
            }
        }
        DomainExpression::InSubquery { expr, query, .. } => {
            visit_expr(expr, v);
            visit_query(query, v);
        }
        DomainExpression::Exists { query, .. } => visit_query(query, v),
        DomainExpression::Subquery(query) => visit_query(query, v),
        DomainExpression::WindowFunction {
            args,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            for a in args {
                visit_expr(a, v);
            }
            for p in partition_by {
                visit_expr(p, v);
            }
            for (e, _) in order_by {
                visit_expr(e, v);
            }
            if let Some(fr) = frame {
                for bound in [&mut fr.start, &mut fr.end] {
                    match bound {
                        super::expressions::SqlFrameBound::Preceding(e)
                        | super::expressions::SqlFrameBound::Following(e) => visit_expr(e, v),
                        super::expressions::SqlFrameBound::Unbounded
                        | super::expressions::SqlFrameBound::CurrentRow => {}
                    }
                }
            }
        }
        DomainExpression::Tuple(items) => {
            for i in items {
                visit_expr(i, v);
            }
        }
        DomainExpression::PredicateRewrite { args, .. } => {
            for a in args {
                visit_expr(a, v);
            }
        }
    }
    v.expr(expr);
}
