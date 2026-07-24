// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Post-lowering self-check: name-binding verification of the final SQL
//! AST, after legalization and immediately before rendering — the
//! transpiler's analogue of the formatter's L1 (verify the artifact we
//! actually ship, not an intermediate).
//!
//! Two structural guarantees, no catalog required:
//!
//! - **Qualifier visibility**: every qualified column reference names a
//!   scope visible at its position — its own SELECT's FROM, an enclosing
//!   query's FROM (correlation), or a sibling FROM item for TVF
//!   arguments (SQLite binds those laterally). A violation means the
//!   qualifier exists nowhere on the reference's path: a dangling alias.
//!
//! - **Column existence**: when the named scope is derived (subquery,
//!   CTE, union table) with a fully enumerable output list, the
//!   referenced column must be in that list. Base tables and open lists
//!   (stars, unnamed expression items) are skipped — absence is
//!   inconclusive without a catalog.
//!
//! A failure is a compiler invariant violation, never a user error: the
//! transpiler emitted a reference SQL could not resolve, or could only
//! resolve to the wrong thing. Loud here beats a backend "no such
//! column" (best case) or a silently wrong binding (worst case).
//!
//! Scope rules are deliberately one notch LENIENT where SQL dialects
//! disagree (outer scopes stay visible inside FROM-subqueries): a miss
//! here is a weaker check, never a false refusal of working SQL.

use std::collections::HashMap;

use crate::error::{DelightQLError, Result};
use crate::pipeline::sql_ast_v3::{
    Cte, DomainExpression, JoinCondition, OrderTerm, QueryExpression, SelectItem, SelectStatement,
    SqlStatement, TableExpression, TvfArgument,
};

/// What we know about a scope's output columns.
#[derive(Debug, Clone)]
enum ColumnSet {
    /// Not enumerable (base table, star projection, unnamed expression
    /// item, VALUES, TVF). Column existence is not checked.
    Open,
    /// Fully enumerable output list (lowercased names).
    Known(Vec<String>),
}

impl ColumnSet {
    fn contains(&self, lowered: &str) -> bool {
        match self {
            ColumnSet::Open => true,
            ColumnSet::Known(cols) => cols.iter().any(|c| c == lowered),
        }
    }
}

/// One SELECT's FROM clause: qualifier (lowercased) → its columns.
type Frame = HashMap<String, ColumnSet>;

/// CTE names visible at a point (statement-level WITH plus any nested
/// WITH clauses on the path), name (lowercased) → output columns.
type CteEnv = HashMap<String, ColumnSet>;

fn lower(s: &str) -> String {
    s.to_ascii_lowercase()
}

/// Verify every name binding in the statement. Called once per compiled
/// statement, on the exact AST handed to the generator.
pub fn check(stmt: &SqlStatement) -> Result<()> {
    let mut ctes = CteEnv::new();
    match stmt {
        SqlStatement::Query { with_clause, query }
        | SqlStatement::CreateTempTable {
            with_clause, query, ..
        }
        | SqlStatement::CreateTempView {
            with_clause, query, ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes)?;
            check_query(query, &[], &ctes)?;
        }
        SqlStatement::Delete {
            target_table,
            with_clause,
            where_clause,
            ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes)?;
            let mut frame = Frame::new();
            frame.insert(lower(target_table), ColumnSet::Open);
            let stack = [frame];
            if let Some(w) = where_clause {
                check_expr(w, &stack, &ctes)?;
            }
        }
        SqlStatement::Update {
            target_table,
            with_clause,
            set_clause,
            where_clause,
            ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes)?;
            let mut frame = Frame::new();
            frame.insert(lower(target_table), ColumnSet::Open);
            let stack = [frame];
            for (_, expr) in set_clause {
                check_expr(expr, &stack, &ctes)?;
            }
            if let Some(w) = where_clause {
                check_expr(w, &stack, &ctes)?;
            }
        }
        SqlStatement::Insert {
            with_clause,
            source,
            ..
        } => {
            check_cte_list(with_clause.as_deref(), &[], &mut ctes)?;
            check_query(source, &[], &ctes)?;
        }
    }
    Ok(())
}

/// Check CTE bodies in declaration order; each sees the ones before it
/// (and itself when recursive). Extends `ctes` with every output set.
///
/// CTE bodies receive the ENCLOSING scope stack: standard SQL has no
/// correlated CTE, but SQLite inlines CTEs and resolves outer references
/// through them, and the HO-pipe lowering ships exactly that shape (a
/// WITH clause inside a correlated scalar subquery whose body reads the
/// outer row). Leniency rule: never refuse SQL the target accepts.
fn check_cte_list(list: Option<&[Cte]>, stack: &[Frame], ctes: &mut CteEnv) -> Result<()> {
    for cte in list.into_iter().flatten() {
        let name = lower(cte.name());
        if cte.is_recursive() {
            ctes.insert(name.clone(), ColumnSet::Open);
        }
        let out = check_query(cte.query(), stack, ctes)?;
        ctes.insert(name, out);
    }
    Ok(())
}

/// Check a query expression; returns its output columns.
#[stacksafe::stacksafe]
fn check_query(query: &QueryExpression, stack: &[Frame], ctes: &CteEnv) -> Result<ColumnSet> {
    match query {
        QueryExpression::Select(select) => check_select(select, stack, ctes),
        QueryExpression::SetOperation { left, right, .. } => {
            // Column names of a compound come from the left arm.
            let out = check_query(left, stack, ctes)?;
            check_query(right, stack, ctes)?;
            Ok(out)
        }
        QueryExpression::Values { rows } => {
            for row in rows {
                for expr in row {
                    check_expr(expr, stack, ctes)?;
                }
            }
            Ok(ColumnSet::Open)
        }
        QueryExpression::WithCte { ctes: inner, query } => {
            let mut extended = ctes.clone();
            check_cte_list(Some(inner), stack, &mut extended)?;
            check_query(query, stack, &extended)
        }
    }
}

/// Check one SELECT: build its FROM frame, then verify every expression
/// against the frame stack. Returns the SELECT's output columns.
#[stacksafe::stacksafe]
fn check_select(select: &SelectStatement, stack: &[Frame], ctes: &CteEnv) -> Result<ColumnSet> {
    let mut frame = Frame::new();
    // Join ON conditions and TVF arguments bind against the COMPLETE
    // frame (both join sides; TVF lateral siblings), so they are
    // collected during the build and checked after it.
    let mut join_conditions: Vec<&DomainExpression> = Vec::new();
    let mut tvf_args: Vec<&TvfArgument> = Vec::new();

    for te in select.from().into_iter().flatten() {
        collect_from(te, stack, ctes, &mut frame, &mut join_conditions, &mut tvf_args)?;
    }

    let mut full_stack: Vec<Frame> = stack.to_vec();
    full_stack.push(frame);

    for cond in join_conditions {
        check_expr(cond, &full_stack, ctes)?;
    }
    for arg in tvf_args {
        match arg {
            TvfArgument::QualifiedRef { qualifier, column } => {
                check_reference(qualifier, column, &full_stack)?;
            }
            TvfArgument::ColumnRef { qualifier, column } => {
                check_reference(qualifier.table_name(), column, &full_stack)?;
            }
            TvfArgument::StringLiteral(_)
            | TvfArgument::NumberLiteral(_)
            | TvfArgument::Identifier(_) => {}
        }
    }

    for expr in select.group_by().into_iter().flatten() {
        check_expr(expr, &full_stack, ctes)?;
    }
    if let Some(h) = select.having() {
        check_expr(h, &full_stack, ctes)?;
    }
    if let Some(w) = select.where_clause() {
        check_expr(w, &full_stack, ctes)?;
    }
    for term in select.order_by().into_iter().flatten() {
        let term: &OrderTerm = term;
        check_expr(term.expr(), &full_stack, ctes)?;
    }

    let mut out: Vec<String> = Vec::new();
    let mut open = false;
    for item in select.select_list() {
        match item {
            SelectItem::Star => open = true,
            SelectItem::QualifiedStar { qualifier } => {
                let scope = resolve_scope(qualifier.table_name(), &full_stack)
                    .ok_or_else(|| dangling(qualifier.table_name(), "*", &full_stack))?;
                match scope {
                    ColumnSet::Known(cols) => out.extend(cols.iter().cloned()),
                    ColumnSet::Open => open = true,
                }
            }
            SelectItem::Expression { expr, alias } => {
                check_expr(expr, &full_stack, ctes)?;
                match (alias, expr) {
                    (Some(a), _) => out.push(lower(a)),
                    (None, DomainExpression::Column { name, .. }) => out.push(lower(name)),
                    // Unnamed non-column item: the backend derives a name
                    // we do not model, so the list is not enumerable.
                    (None, _) => open = true,
                }
            }
        }
    }
    Ok(if open {
        ColumnSet::Open
    } else {
        ColumnSet::Known(out)
    })
}

/// Add one FROM item's scopes to the frame, checking nested queries.
#[stacksafe::stacksafe]
fn collect_from<'a>(
    te: &'a TableExpression,
    stack: &[Frame],
    ctes: &CteEnv,
    frame: &mut Frame,
    join_conditions: &mut Vec<&'a DomainExpression>,
    tvf_args: &mut Vec<&'a TvfArgument>,
) -> Result<()> {
    match te {
        TableExpression::Table { name, alias, .. } => {
            let cols = ctes.get(&lower(name)).cloned().unwrap_or(ColumnSet::Open);
            let qual = alias.as_deref().unwrap_or(name);
            frame.insert(lower(qual), cols);
        }
        TableExpression::Subquery { query, alias } => {
            // A FROM-subquery body sees enclosing scopes but not its
            // siblings (only TVF arguments bind laterally).
            let cols = check_query(query, stack, ctes)?;
            frame.insert(lower(alias), cols);
        }
        TableExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            collect_from(left, stack, ctes, frame, join_conditions, tvf_args)?;
            collect_from(right, stack, ctes, frame, join_conditions, tvf_args)?;
            if let JoinCondition::On(expr) = join_condition {
                join_conditions.push(expr);
            }
        }
        TableExpression::Values { rows, alias } => {
            for row in rows {
                for expr in row {
                    check_expr(expr, stack, ctes)?;
                }
            }
            frame.insert(lower(alias), ColumnSet::Open);
        }
        TableExpression::UnionTable { selects, alias } => {
            let mut first: Option<ColumnSet> = None;
            for q in selects {
                let out = check_query(q, stack, ctes)?;
                if first.is_none() {
                    first = Some(out);
                }
            }
            frame.insert(lower(alias), first.unwrap_or(ColumnSet::Open));
        }
        TableExpression::TVF {
            function,
            arguments,
            alias,
            ..
        } => {
            let qual = alias.as_deref().unwrap_or(function);
            frame.insert(lower(qual), ColumnSet::Open);
            tvf_args.extend(arguments.iter());
        }
    }
    Ok(())
}

/// Walk an expression, verifying every qualified column reference and
/// descending into subqueries with the current stack (correlation).
#[stacksafe::stacksafe]
fn check_expr(expr: &DomainExpression, stack: &[Frame], ctes: &CteEnv) -> Result<()> {
    match expr {
        DomainExpression::Column {
            name,
            qualifier: Some(q),
        } => check_reference(q.table_name(), name, stack),
        DomainExpression::Column { qualifier: None, .. }
        | DomainExpression::Literal(_)
        | DomainExpression::Star
        // Opaque by definition; its references are not modeled.
        | DomainExpression::RawSql(_) => Ok(()),
        DomainExpression::Cast { expr, .. }
        | DomainExpression::Unary { expr, .. }
        | DomainExpression::Parens(expr) => check_expr(expr, stack, ctes),
        DomainExpression::Binary { left, right, .. } => {
            check_expr(left, stack, ctes)?;
            check_expr(right, stack, ctes)
        }
        DomainExpression::Function { args, .. }
        | DomainExpression::PredicateRewrite { args, .. }
        | DomainExpression::Tuple(args) => {
            for a in args {
                check_expr(a, stack, ctes)?;
            }
            Ok(())
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            if let Some(e) = expr {
                check_expr(e, stack, ctes)?;
            }
            for wc in when_clauses {
                check_expr(wc.when(), stack, ctes)?;
                check_expr(wc.then(), stack, ctes)?;
            }
            if let Some(e) = else_clause {
                check_expr(e, stack, ctes)?;
            }
            Ok(())
        }
        DomainExpression::InList { expr, values, .. } => {
            check_expr(expr, stack, ctes)?;
            for v in values {
                check_expr(v, stack, ctes)?;
            }
            Ok(())
        }
        DomainExpression::InSubquery { expr, query, .. } => {
            check_expr(expr, stack, ctes)?;
            check_query(query, stack, ctes).map(|_| ())
        }
        DomainExpression::Exists { query, .. } | DomainExpression::Subquery(query) => {
            check_query(query, stack, ctes).map(|_| ())
        }
        DomainExpression::WindowFunction {
            args,
            partition_by,
            order_by,
            frame,
            ..
        } => {
            for a in args.iter().chain(partition_by.iter()) {
                check_expr(a, stack, ctes)?;
            }
            for (e, _) in order_by {
                check_expr(e, stack, ctes)?;
            }
            if let Some(f) = frame {
                use crate::pipeline::sql_ast_v3::SqlFrameBound;
                for bound in [&f.start, &f.end] {
                    if let SqlFrameBound::Preceding(e) | SqlFrameBound::Following(e) = bound {
                        check_expr(e, stack, ctes)?;
                    }
                }
            }
            Ok(())
        }
    }
}

/// Verify one qualified reference `qualifier.column`.
fn check_reference(qualifier: &str, column: &str, stack: &[Frame]) -> Result<()> {
    let Some(scope) = resolve_scope(qualifier, stack) else {
        return Err(dangling(qualifier, column, stack));
    };
    if !scope.contains(&lower(column)) {
        let ColumnSet::Known(cols) = scope else {
            unreachable!("Open contains everything")
        };
        return Err(DelightQLError::validation_error_categorized(
            "transform/self_check/unknown_column",
            format!(
                "SQL self-check: '{}.{}' does not exist — scope '{}' produces only [{}]",
                qualifier,
                column,
                qualifier,
                cols.join(", ")
            ),
            "internal invariant violation: the transpiler referenced a column its own \
             derived scope does not output; please report the query that produced this",
        ));
    }
    Ok(())
}

/// Innermost frame that knows the qualifier wins (SQL shadowing order).
/// CTE names are deliberately NOT a fallback: a CTE is referable as a
/// qualifier only via FROM membership, and every FROM road already put
/// it in a frame — falling back here would mask a dangling reference.
fn resolve_scope<'a>(qualifier: &str, stack: &'a [Frame]) -> Option<&'a ColumnSet> {
    let key = lower(qualifier);
    stack.iter().rev().find_map(|frame| frame.get(&key))
}

fn dangling(qualifier: &str, column: &str, stack: &[Frame]) -> DelightQLError {
    let mut visible: Vec<&String> = stack.iter().flat_map(|f| f.keys()).collect();
    visible.sort();
    visible.dedup();
    let visible = visible
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    DelightQLError::validation_error_categorized(
        "transform/self_check/dangling_qualifier",
        format!(
            "SQL self-check: reference '{}.{}' names a scope that is not visible \
             anywhere on its path (visible scopes: [{}])",
            qualifier, column, visible
        ),
        "internal invariant violation: the transpiler emitted a dangling qualifier; \
         please report the query that produced this",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::sql_ast_v3::{ColumnQualifier, SelectStatement};

    fn col(q: &str, name: &str) -> DomainExpression {
        DomainExpression::with_qualifier(ColumnQualifier::table(q), name)
    }

    fn select_from(items: Vec<SelectItem>, from: TableExpression) -> SelectStatement {
        SelectStatement::builder()
            .select_all(items)
            .from_tables(vec![from])
            .build()
            .unwrap()
    }

    fn stmt(query: QueryExpression) -> SqlStatement {
        SqlStatement::Query {
            with_clause: None,
            query,
        }
    }

    /// The HO-pipe lowering ships a WITH clause inside a correlated
    /// scalar subquery whose CTE body reads the outer row (SQLite
    /// inlines CTEs, so this resolves and runs). The check must not
    /// refuse it: CTE bodies see enclosing scopes.
    #[test]
    fn correlated_cte_body_sees_outer_scope() {
        // SELECT (WITH c AS (SELECT users.id AS v) SELECT c.v FROM c)
        // FROM users
        let cte_body = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(col("users", "id"), "v"))
            .build()
            .unwrap();
        let inner_main = select_from(
            vec![SelectItem::expression(col("c", "v"))],
            TableExpression::table("c"),
        );
        let scalar = QueryExpression::WithCte {
            ctes: vec![Cte::new("c", QueryExpression::Select(Box::new(cte_body)))],
            query: Box::new(QueryExpression::Select(Box::new(inner_main))),
        };
        let outer = select_from(
            vec![SelectItem::expression(DomainExpression::Subquery(Box::new(
                scalar,
            )))],
            TableExpression::table("users"),
        );
        assert!(check(&stmt(QueryExpression::Select(Box::new(outer)))).is_ok());
    }

    #[test]
    fn base_table_reference_passes() {
        let s = select_from(
            vec![SelectItem::expression(col("users", "id"))],
            TableExpression::table("users"),
        );
        assert!(check(&stmt(QueryExpression::Select(Box::new(s)))).is_ok());
    }

    #[test]
    fn dangling_qualifier_is_caught() {
        let s = select_from(
            vec![SelectItem::expression(col("nowhere", "id"))],
            TableExpression::table("users"),
        );
        let err = check(&stmt(QueryExpression::Select(Box::new(s)))).unwrap_err();
        assert!(err.to_string().contains("not visible"), "got: {}", err);
    }

    #[test]
    fn derived_scope_missing_column_is_caught() {
        // SELECT t.gone FROM (SELECT users.id AS id FROM users) AS t
        let inner = select_from(
            vec![SelectItem::expression_with_alias(col("users", "id"), "id")],
            TableExpression::table("users"),
        );
        let outer = select_from(
            vec![SelectItem::expression(col("t", "gone"))],
            TableExpression::subquery(QueryExpression::Select(Box::new(inner)), "t"),
        );
        let err = check(&stmt(QueryExpression::Select(Box::new(outer)))).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {}", err);
    }

    #[test]
    fn derived_scope_present_column_passes() {
        let inner = select_from(
            vec![SelectItem::expression_with_alias(col("users", "id"), "id")],
            TableExpression::table("users"),
        );
        let outer = select_from(
            vec![SelectItem::expression(col("t", "id"))],
            TableExpression::subquery(QueryExpression::Select(Box::new(inner)), "t"),
        );
        assert!(check(&stmt(QueryExpression::Select(Box::new(outer)))).is_ok());
    }

    #[test]
    fn correlated_exists_sees_outer_scope() {
        // SELECT u.id FROM users AS u WHERE EXISTS
        //   (SELECT o.uid FROM orders AS o WHERE o.uid = u.id)
        let inner = SelectStatement::builder()
            .select(SelectItem::expression(col("o", "uid")))
            .from_tables(vec![TableExpression::table_with_alias("orders", "o")])
            .and_where(DomainExpression::eq(col("o", "uid"), col("u", "id")))
            .build()
            .unwrap();
        let outer = SelectStatement::builder()
            .select(SelectItem::expression(col("u", "id")))
            .from_tables(vec![TableExpression::table_with_alias("users", "u")])
            .and_where(DomainExpression::exists(QueryExpression::Select(Box::new(
                inner,
            ))))
            .build()
            .unwrap();
        assert!(check(&stmt(QueryExpression::Select(Box::new(outer)))).is_ok());
    }

    #[test]
    fn from_subquery_does_not_see_siblings() {
        // SELECT * FROM users AS u, (SELECT u.id FROM t2) AS x — u is a
        // sibling, invisible inside the derived table.
        let inner = select_from(
            vec![SelectItem::expression(col("u", "id"))],
            TableExpression::table("t2"),
        );
        let outer = SelectStatement::builder()
            .select(SelectItem::star())
            .from_tables(vec![
                TableExpression::table_with_alias("users", "u"),
                TableExpression::subquery(QueryExpression::Select(Box::new(inner)), "x"),
            ])
            .build()
            .unwrap();
        let err = check(&stmt(QueryExpression::Select(Box::new(outer)))).unwrap_err();
        assert!(err.to_string().contains("not visible"), "got: {}", err);
    }

    #[test]
    fn cte_columns_are_enumerable() {
        // WITH c AS (SELECT users.id AS id FROM users) SELECT c.gone FROM c
        let body = select_from(
            vec![SelectItem::expression_with_alias(col("users", "id"), "id")],
            TableExpression::table("users"),
        );
        let main = select_from(
            vec![SelectItem::expression(col("c", "gone"))],
            TableExpression::table("c"),
        );
        let s = SqlStatement::Query {
            with_clause: Some(vec![Cte::new(
                "c",
                QueryExpression::Select(Box::new(body)),
            )]),
            query: QueryExpression::Select(Box::new(main)),
        };
        let err = check(&s).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {}", err);
    }

    #[test]
    fn tvf_argument_sees_siblings() {
        // SELECT je.value FROM (SELECT users.j AS j FROM users) AS t,
        //   json_each(t.j) AS je — lateral sibling binding.
        let inner = select_from(
            vec![SelectItem::expression_with_alias(col("users", "j"), "j")],
            TableExpression::table("users"),
        );
        let outer = SelectStatement::builder()
            .select(SelectItem::expression(col("je", "value")))
            .from_tables(vec![
                TableExpression::subquery(QueryExpression::Select(Box::new(inner)), "t"),
                TableExpression::TVF {
                    schema: None,
                    function: "json_each".to_string(),
                    arguments: vec![TvfArgument::QualifiedRef {
                        qualifier: "t".to_string(),
                        column: "j".to_string(),
                    }],
                    alias: Some("je".to_string()),
                },
            ])
            .build()
            .unwrap();
        assert!(check(&stmt(QueryExpression::Select(Box::new(outer)))).is_ok());
    }
}
