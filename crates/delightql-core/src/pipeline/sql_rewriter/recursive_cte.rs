// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// recursive_cte.rs — recursive-CTE legalizations (final-word pass).
//
// Two legalizations, both keyed on structural self-reference (a CTE body
// referencing its own name), independent of which upstream path built the
// Cte:
//
// 1. `mark_recursive_ctes` — set `is_recursive` so the generator spells
//    `WITH RECURSIVE`. Canonical on ALL targets (ratified: we control the
//    SQL, we emit proper SQL). SQLite treats the keyword as optional;
//    postgres/duckdb/mysql refuse the self-reference without it.
//    Detection is SHADOWING-AWARE: references under a nested WITH clause
//    that redefines the name belong to the inner CTE (nested HO pipes
//    reuse internal names like `_ho_pipe_src`). This matters because the
//    validator below consumes the flag — over-marking is harmless for the
//    keyword but poisons validation (stress/391 false N4).
//
// 2. `legalize_recursive_limits` — `#<N` inside a recursive rule.
//    DQL semantics (ratified): a TOTAL-ROW CAP on the fixpoint unfold —
//    a demand bound, the co-recursive dual of a filter condition.
//    The transformer lowers it as a `__dql_limit_wrap` subquery, which
//    buries the self-reference — illegal on sqlite ("circular
//    reference") even though sqlite's native spelling (a trailing LIMIT
//    on the recursive member, applying to the whole compound) implements
//    exactly the total-cap semantics. So: on targets with the native
//    spelling (sqlite, mysql) the wrapper is inlined and the LIMIT
//    hoisted to the member tail; on targets without one (postgres,
//    duckdb, sqlserver) there is no legal single-statement equivalent
//    and the pass diagnoses — never emits illegal-or-wrong SQL. (The
//    buried form happens to PARSE on postgres, but means per-iteration
//    limit there: non-terminating. Worse than refusing.)

use crate::error::{DelightQLError, Result};
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::{
    walk, Cte, DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement,
    TableExpression,
};

// ---------------------------------------------------------------------------
// Marking: structural self-reference → is_recursive
// ---------------------------------------------------------------------------

/// Mark every CTE whose body references its own name as recursive.
pub fn mark_recursive_ctes(stmt: &mut SqlStatement) {
    if let Some(ctes) = statement_with_clause_mut(stmt) {
        mark_list(ctes);
    }
    struct Marker;
    impl walk::SqlVisitorMut for Marker {
        fn query(&mut self, q: &mut QueryExpression) {
            if let QueryExpression::WithCte { ctes, .. } = q {
                mark_list(ctes);
            }
        }
    }
    walk::visit_mut(stmt, &mut Marker);
}

fn mark_list(ctes: &mut [Cte]) {
    for cte in ctes.iter_mut() {
        if !cte.is_recursive() {
            let name = cte.name().to_string();
            if query_body_references(cte.query_mut(), &name) {
                cte.set_recursive(true);
            }
        }
    }
}

/// Does this query body reference `name` as a table anywhere (including
/// inside expression subqueries)? SCOPE-AWARE: references under a nested
/// WITH clause that redefines `name` belong to the inner CTE, not this
/// one — nested higher-order pipes legitimately reuse internal CTE names
/// (`_ho_pipe_src`), and counting the shadowed reference here produced a
/// false N4 refusal (stress/391).
fn query_body_references(query: &mut QueryExpression, name: &str) -> bool {
    scoped_reference_count(query, name) > 0
}

/// Count references to `name` in this query, excluding any subtree under
/// a nested WITH clause that redefines `name` (shadowing). Works on a
/// stripped clone so the real tree is untouched.
fn scoped_reference_count(query: &QueryExpression, name: &str) -> usize {
    let mut clone = query.clone();
    struct Strip<'a> {
        name: &'a str,
    }
    impl walk::SqlVisitorMut for Strip<'_> {
        fn query(&mut self, q: &mut QueryExpression) {
            if let QueryExpression::WithCte { ctes, .. } = q {
                if ctes.iter().any(|c| c.name() == self.name) {
                    *q = QueryExpression::Values { rows: vec![] };
                }
            }
        }
    }
    walk::visit_query(&mut clone, &mut Strip { name });

    struct Counter<'a> {
        name: &'a str,
        count: usize,
    }
    impl walk::SqlVisitorMut for Counter<'_> {
        fn table(&mut self, t: &mut TableExpression) {
            if let TableExpression::Table { name, .. } = t {
                if name == self.name {
                    self.count += 1;
                }
            }
        }
    }
    let mut c = Counter { name, count: 0 };
    walk::visit_query(&mut clone, &mut c);
    c.count
}

// ---------------------------------------------------------------------------
// LIMIT-in-recursive-member legalization
// ---------------------------------------------------------------------------

/// Can this dialect spell a total-row cap natively (trailing LIMIT on the
/// recursive member, applying to the whole compound)?
fn allows_recursive_limit(dialect: SqlDialect) -> bool {
    match dialect {
        SqlDialect::SQLite | SqlDialect::MySQL => true,
        SqlDialect::PostgreSQL | SqlDialect::DuckDB | SqlDialect::SqlServer => false,
    }
}

/// Legalize `#<N` bounds inside recursive members. Must run AFTER
/// `mark_recursive_ctes` (it only inspects CTEs already marked).
pub fn legalize_recursive_limits(stmt: &mut SqlStatement, dialect: SqlDialect) -> Result<()> {
    if let Some(ctes) = statement_with_clause_mut(stmt) {
        for cte in ctes.iter_mut() {
            legalize_cte(cte, dialect)?;
        }
    }
    struct Legalizer {
        dialect: SqlDialect,
        err: Option<DelightQLError>,
    }
    impl walk::SqlVisitorMut for Legalizer {
        fn query(&mut self, q: &mut QueryExpression) {
            if self.err.is_some() {
                return;
            }
            if let QueryExpression::WithCte { ctes, .. } = q {
                for cte in ctes.iter_mut() {
                    if let Err(e) = legalize_cte(cte, self.dialect) {
                        self.err = Some(e);
                        return;
                    }
                }
            }
        }
    }
    let mut l = Legalizer {
        dialect,
        err: None,
    };
    walk::visit_mut(stmt, &mut l);
    match l.err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn legalize_cte(cte: &mut Cte, dialect: SqlDialect) -> Result<()> {
    if !cte.is_recursive() {
        return Ok(());
    }
    let name = cte.name().to_string();
    legalize_branches(cte.query_mut(), &name, dialect)
}

/// Walk the set-op tree of a recursive CTE body; legalize each SELECT
/// branch that references the CTE (= each recursive member).
#[stacksafe::stacksafe]
fn legalize_branches(q: &mut QueryExpression, cte_name: &str, dialect: SqlDialect) -> Result<()> {
    match q {
        QueryExpression::SetOperation { left, right, .. } => {
            legalize_branches(left, cte_name, dialect)?;
            legalize_branches(right, cte_name, dialect)
        }
        QueryExpression::Select(_) => legalize_member(q, cte_name, dialect),
        _ => Ok(()),
    }
}

fn legalize_member(q: &mut QueryExpression, cte_name: &str, dialect: SqlDialect) -> Result<()> {
    // Member classification is shadowing-aware: a reference under a nested
    // WITH that redefines the name belongs to the inner CTE.
    if scoped_reference_count(q, cte_name) == 0 {
        return Ok(()); // base member
    }
    let QueryExpression::Select(select) = &*q else {
        return Ok(());
    };

    // Direct LIMIT on the recursive member: already the legal total-cap
    // spelling where supported; no single-statement equivalent elsewhere.
    if select.limit().is_some() {
        if allows_recursive_limit(dialect) {
            return Ok(());
        }
        return Err(limit_bound_error(cte_name, dialect));
    }

    // The transformer's buried form: FROM (SELECT … FROM <cte> … LIMIT n) AS w
    let Some(wrapper) = find_limit_wrapper(select, cte_name) else {
        return Ok(());
    };
    if !allows_recursive_limit(dialect) {
        return Err(limit_bound_error(cte_name, dialect));
    }
    let inlined = inline_limit_wrapper(select, &wrapper)?;
    *q = QueryExpression::Select(Box::new(inlined));
    Ok(())
}

/// The wrapper's alias, if this member has the buried-limit shape.
#[stacksafe::stacksafe]
fn find_limit_wrapper(select: &SelectStatement, cte_name: &str) -> Option<String> {
    let from = select.from()?;
    if from.len() != 1 {
        return None;
    }
    let TableExpression::Subquery { query, alias } = &from[0] else {
        return None;
    };
    let QueryExpression::Select(inner) = &***query else {
        return None;
    };
    if inner.limit().is_some() && select_from_references(inner, cte_name) {
        Some(alias.clone())
    } else {
        None
    }
}

/// Inline the limit wrapper: substitute wrapper-column references in the
/// outer select list with the inner select's expressions, adopt the
/// inner FROM/WHERE/ORDER BY, and hoist the LIMIT to the member tail
/// (where sqlite reads it as the compound-level total cap).
///
/// The shape is strict — the transformer's `__dql_limit_wrap` lowering is
/// the only known producer — and anything unrecognized diagnoses rather
/// than risking illegal or wrong SQL.
#[stacksafe::stacksafe]
fn inline_limit_wrapper(outer: &SelectStatement, wrapper_alias: &str) -> Result<SelectStatement> {
    let Some([TableExpression::Subquery { query, .. }]) = outer.from() else {
        return Err(shape_error("wrapper FROM vanished"));
    };
    let QueryExpression::Select(inner) = &***query else {
        return Err(shape_error("wrapper body is not a plain SELECT"));
    };

    // The outer member carries no clauses of its own in the known shape.
    if outer.where_clause().is_some()
        || outer.group_by().is_some()
        || outer.having().is_some()
        || outer.order_by().is_some()
        || outer.is_distinct()
    {
        return Err(shape_error("recursive member has clauses outside the limit wrapper"));
    }
    if inner.group_by().is_some() || inner.having().is_some() || inner.is_distinct() {
        return Err(shape_error("limit wrapper carries aggregation"));
    }

    // Map: wrapper column name -> inner expression.
    let mut map = std::collections::HashMap::new();
    for item in inner.select_list() {
        let SelectItem::Expression {
            expr,
            alias: Some(alias),
        } = item
        else {
            return Err(shape_error("limit wrapper has unaliased or star items"));
        };
        map.insert(alias.clone(), expr.clone());
    }

    // Rebuild the outer select list with wrapper references substituted.
    let mut items = Vec::with_capacity(outer.select_list().len());
    for item in outer.select_list() {
        let SelectItem::Expression { expr, alias } = item else {
            return Err(shape_error("recursive member selects a star"));
        };
        let new_expr = match expr {
            DomainExpression::Column {
                name,
                qualifier: Some(q),
            } if q.table_name() == wrapper_alias => map
                .get(name)
                .cloned()
                .ok_or_else(|| shape_error("wrapper reference names an unknown column"))?,
            DomainExpression::Literal(_) => expr.clone(),
            DomainExpression::Column { qualifier: None, .. } => expr.clone(),
            DomainExpression::Column {
                qualifier: Some(q), ..
            } if q.table_name() != wrapper_alias => expr.clone(),
            _ => return Err(shape_error("recursive member computes over the wrapper")),
        };
        items.push(match alias {
            Some(a) => SelectItem::expression_with_alias(new_expr, a.clone()),
            None => SelectItem::expression(new_expr),
        });
    }

    let mut builder = SelectStatement::builder()
        .select_all(items)
        .from_tables(inner.from().unwrap_or(&[]).to_vec());
    if let Some(w) = inner.where_clause() {
        builder = builder.where_clause(w.clone());
    }
    if let Some(ob) = inner.order_by() {
        for term in ob {
            builder = builder.order_by(term.clone());
        }
    }
    if let Some(lim) = inner.limit() {
        builder = match lim.offset() {
            Some(off) => builder.limit_offset(lim.count(), off),
            None => builder.limit(lim.count()),
        };
    }
    builder.build().map_err(|e| DelightQLError::ValidationError {
        message: format!("recursive limit legalization rebuild: {}", e),
        context: "sql_rewriter::recursive_cte".to_string(),
        subcategory: Some(crate::uri_registry::subcat::RECURSION_LIMIT_BOUND),
    })
}

/// Does this SELECT's FROM tree (including nested subqueries) reference
/// the table `name`?
fn select_from_references(select: &SelectStatement, name: &str) -> bool {
    select
        .from()
        .is_some_and(|from| from.iter().any(|t| table_references(t, name)))
}

#[stacksafe::stacksafe]
fn table_references(table: &TableExpression, name: &str) -> bool {
    match table {
        TableExpression::Table { name: n, .. } => n == name,
        TableExpression::Subquery { query, .. } => query_references(query, name),
        TableExpression::Join { left, right, .. } => {
            table_references(left, name) || table_references(right, name)
        }
        TableExpression::Values { .. } | TableExpression::TVF { .. } => false,
        TableExpression::UnionTable { selects, .. } => {
            selects.iter().any(|q| query_references(q, name))
        }
    }
}

#[stacksafe::stacksafe]
fn query_references(query: &QueryExpression, name: &str) -> bool {
    match query {
        QueryExpression::Select(s) => select_from_references(s, name),
        QueryExpression::SetOperation { left, right, .. } => {
            query_references(left, name) || query_references(right, name)
        }
        QueryExpression::Values { .. } => false,
        QueryExpression::WithCte { ctes, query } => {
            // A nested CTE of the same name shadows; conservatively still
            // count it (over-detection only strengthens the keyword).
            ctes.iter().any(|c| query_references(c.query(), name))
                || query_references(query, name)
        }
    }
}

fn statement_with_clause_mut(stmt: &mut SqlStatement) -> Option<&mut Vec<Cte>> {
    match stmt {
        SqlStatement::Query { with_clause, .. }
        | SqlStatement::CreateTempTable { with_clause, .. }
        | SqlStatement::CreateTempView { with_clause, .. }
        | SqlStatement::Delete { with_clause, .. }
        | SqlStatement::Update { with_clause, .. }
        | SqlStatement::Insert { with_clause, .. } => with_clause.as_mut(),
    }
}

fn limit_bound_error(cte_name: &str, dialect: SqlDialect) -> DelightQLError {
    DelightQLError::ValidationError {
        message: format!(
            "'{cte_name}' bounds its recursion with a row limit (#<N). DelightQL \
             defines this as a total-row cap on the fixpoint, but {dialect:?} has \
             no single-statement spelling for it — this target only supports \
             filter-based bounds. Rewrite the bound as a filter condition on the \
             recursive rule (e.g. a depth or value predicate)."
        ),
        context: "sql_rewriter::recursive_cte".to_string(),
        subcategory: Some(crate::uri_registry::subcat::RECURSION_LIMIT_BOUND),
    }
}

fn shape_error(detail: &str) -> DelightQLError {
    DelightQLError::ValidationError {
        message: format!(
            "cannot legalize the row limit inside this recursive rule ({detail}). \
             Rewrite the bound as a filter condition on the recursive rule."
        ),
        context: "sql_rewriter::recursive_cte".to_string(),
        subcategory: Some(crate::uri_registry::subcat::RECURSION_LIMIT_BOUND),
    }
}

// ---------------------------------------------------------------------------
// The recursion validator (RECURSION-CONTRACT.md N1/N3/N4)
//
// Refusals of recursive forms the language does not permit, checked
// structurally per recursive member. These shapes were previously refused
// by the TARGET's validator (sqlite prepare-time errors: "multiple
// references to recursive table", "recursive aggregate queries not
// supported", "circular reference") — unbadged, target-worded, and absent
// on targets with different validators. The contract owns them now:
// language-level rules, refused before any target sees SQL, each with its
// badge and rewrite path.
// ---------------------------------------------------------------------------

/// Validate every recursive CTE's members. Must run AFTER
/// `legalize_recursive_limits` (the limit unwrap turns the one legal
/// buried shape into a direct reference before this pass judges burial).
pub fn validate_recursive_members(stmt: &mut SqlStatement) -> Result<()> {
    if let Some(ctes) = statement_with_clause_mut(stmt) {
        for cte in ctes.iter_mut() {
            validate_cte(cte)?;
        }
    }
    struct Validator {
        err: Option<DelightQLError>,
    }
    impl walk::SqlVisitorMut for Validator {
        fn query(&mut self, q: &mut QueryExpression) {
            if self.err.is_some() {
                return;
            }
            if let QueryExpression::WithCte { ctes, .. } = q {
                for cte in ctes.iter_mut() {
                    if let Err(e) = validate_cte(cte) {
                        self.err = Some(e);
                        return;
                    }
                }
            }
        }
    }
    let mut v = Validator { err: None };
    walk::visit_mut(stmt, &mut v);
    match v.err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

fn validate_cte(cte: &mut Cte) -> Result<()> {
    if !cte.is_recursive() {
        return Ok(());
    }
    let name = cte.name().to_string();
    validate_branches(cte.query_mut(), &name)
}

#[stacksafe::stacksafe]
fn validate_branches(q: &mut QueryExpression, cte_name: &str) -> Result<()> {
    match q {
        QueryExpression::SetOperation { left, right, .. } => {
            validate_branches(left, cte_name)?;
            validate_branches(right, cte_name)
        }
        QueryExpression::Select(_) => validate_member(q, cte_name),
        _ => Ok(()),
    }
}

fn validate_member(q: &mut QueryExpression, cte_name: &str) -> Result<()> {
    // Total references anywhere in the member (including expression
    // subqueries), via the total walker.
    let total = count_references(q, cte_name);
    if total == 0 {
        return Ok(()); // base member
    }

    let QueryExpression::Select(select) = &*q else {
        return Ok(());
    };

    // Direct references: reachable through FROM join nesting only.
    let direct: usize = select
        .from()
        .map(|from| from.iter().map(|t| count_direct(t, cte_name)).sum())
        .unwrap_or(0);

    // N1 — non-linear recursion: the frontier cannot join with itself.
    if direct > 1 {
        return Err(recursion_error(
            "nonlinear",
            format!(
                "the recursive rule references '{cte_name}' {direct} times — the \
                 frontier cannot join with itself. Carry the values you need as \
                 columns of one frontier row instead (tupling: fib-style \
                 `(a, b) -> (b, a+b)`). RECURSION-CONTRACT.md N1."
            ),
        ));
    }

    // N4 — self-reference buried in a subquery (semi/anti-join, IN, scalar,
    // or a derived table): the rule would need the accumulated set.
    if total > direct {
        return Err(recursion_error(
            "self_subquery",
            format!(
                "'{cte_name}' is referenced inside a subquery of its own recursive \
                 rule — a recursive rule sees only the previous iteration's rows, \
                 as a direct source. Track visited state in the frontier row, or \
                 deduplicate/filter after the fixpoint. RECURSION-CONTRACT.md N4."
            ),
        ));
    }

    // N3 — aggregation over the frontier.
    if member_has_aggregation(select) {
        return Err(recursion_error(
            "aggregate",
            format!(
                "aggregation inside the recursive rule of '{cte_name}' would need \
                 the accumulated set. Aggregate after the fixpoint (a later pipe \
                 stage — strata are textual), or carry a running value in the \
                 frontier row. RECURSION-CONTRACT.md N3."
            ),
        ));
    }

    Ok(())
}

/// Count references to `name` anywhere under this query, shadowing-aware
/// (see `scoped_reference_count`).
fn count_references(q: &mut QueryExpression, name: &str) -> usize {
    scoped_reference_count(q, name)
}

/// Count references reachable through join nesting only — the positions a
/// recursive reference is allowed to occupy.
fn count_direct(table: &TableExpression, name: &str) -> usize {
    match table {
        TableExpression::Table { name: n, .. } => usize::from(n == name),
        TableExpression::Join { left, right, .. } => {
            count_direct(left, name) + count_direct(right, name)
        }
        _ => 0,
    }
}

fn member_has_aggregation(select: &SelectStatement) -> bool {
    if select.group_by().is_some() || select.having().is_some() {
        return true;
    }
    select.select_list().iter().any(|item| {
        if let SelectItem::Expression { expr, .. } = item {
            expr_has_aggregate(expr)
        } else {
            false
        }
    })
}

fn expr_has_aggregate(expr: &DomainExpression) -> bool {
    match expr {
        DomainExpression::Function { name, args, .. } => {
            is_aggregate_fn(name) || args.iter().any(expr_has_aggregate)
        }
        DomainExpression::Binary { left, right, .. } => {
            expr_has_aggregate(left) || expr_has_aggregate(right)
        }
        DomainExpression::Unary { expr, .. } | DomainExpression::Parens(expr) => {
            expr_has_aggregate(expr)
        }
        DomainExpression::Cast { expr, .. } => expr_has_aggregate(expr),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_deref().is_some_and(expr_has_aggregate)
                || when_clauses
                    .iter()
                    .any(|wc| expr_has_aggregate(wc.when()) || expr_has_aggregate(wc.then()))
                || else_clause.as_deref().is_some_and(expr_has_aggregate)
        }
        _ => false,
    }
}

fn is_aggregate_fn(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "AVG"
            | "MIN"
            | "MAX"
            | "TOTAL"
            | "GROUP_CONCAT"
            | "STRING_AGG"
            | "JSON_GROUP_ARRAY"
            | "JSON_GROUP_OBJECT"
    )
}

fn recursion_error(leaf: &str, message: String) -> DelightQLError {
    let subcategory = match leaf {
        "nonlinear" => "recursion/nonlinear",
        "aggregate" => "recursion/aggregate",
        "self_subquery" => "recursion/self_subquery",
        other => unreachable!("unknown recursion refusal leaf: {other}"),
    };
    DelightQLError::ValidationError {
        message,
        context: "sql_rewriter::recursive_cte".to_string(),
        subcategory: Some(subcategory),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::ast_refined::LiteralValue;
    use crate::pipeline::sql_ast_v3::{ColumnQualifier, SetOperator};

    fn table(name: &str) -> TableExpression {
        TableExpression::Table {
            schema: None,
            name: name.to_string(),
            alias: None,
        }
    }

    fn qualified(table: &str, column: &str) -> DomainExpression {
        DomainExpression::Column {
            name: column.to_string(),
            qualifier: Some(ColumnQualifier::table(table)),
        }
    }

    fn base_member() -> SelectStatement {
        SelectStatement::builder()
            .select(SelectItem::expression_with_alias(
                DomainExpression::Literal(LiteralValue::Number("1".to_string())),
                "n",
            ))
            .build()
            .unwrap()
    }

    /// The transformer's buried shape:
    /// SELECT w.n AS n FROM (SELECT x.n + 1 AS n FROM x LIMIT 2) AS w
    fn buried_limit_member(cte: &str) -> SelectStatement {
        let inner = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified(cte, "n"), "n"))
            .from_tables(vec![table(cte)])
            .limit(2)
            .build()
            .unwrap();
        SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified("w", "n"), "n"))
            .from_tables(vec![TableExpression::Subquery {
                query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(
                    Box::new(inner),
                ))),
                alias: "w".to_string(),
            }])
            .build()
            .unwrap()
    }

    fn recursive_cte_stmt(member: SelectStatement) -> SqlStatement {
        let body = QueryExpression::SetOperation {
            op: SetOperator::UnionAll,
            left: Box::new(QueryExpression::Select(Box::new(base_member()))),
            right: Box::new(QueryExpression::Select(Box::new(member))),
        };
        SqlStatement::Query {
            with_clause: Some(vec![Cte::new("x", body)]),
            query: QueryExpression::Select(Box::new(
                SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_tables(vec![table("x")])
                    .build()
                    .unwrap(),
            )),
        }
    }

    fn first_cte(stmt: &SqlStatement) -> &Cte {
        let SqlStatement::Query {
            with_clause: Some(ctes),
            ..
        } = stmt
        else {
            panic!("expected WITH clause");
        };
        &ctes[0]
    }

    #[test]
    fn self_referencing_cte_is_marked() {
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified("x", "n"), "n"))
            .from_tables(vec![table("x")])
            .build()
            .unwrap();
        let mut stmt = recursive_cte_stmt(member);
        assert!(!first_cte(&stmt).is_recursive());
        mark_recursive_ctes(&mut stmt);
        assert!(first_cte(&stmt).is_recursive());
    }

    #[test]
    fn non_recursive_cte_stays_unmarked() {
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified("t", "n"), "n"))
            .from_tables(vec![table("t")])
            .build()
            .unwrap();
        let mut stmt = recursive_cte_stmt(member);
        mark_recursive_ctes(&mut stmt);
        assert!(!first_cte(&stmt).is_recursive());
    }

    #[test]
    fn buried_limit_unwraps_on_sqlite() {
        let mut stmt = recursive_cte_stmt(buried_limit_member("x"));
        mark_recursive_ctes(&mut stmt);
        legalize_recursive_limits(&mut stmt, SqlDialect::SQLite).unwrap();

        let QueryExpression::SetOperation { right, .. } = first_cte(&stmt).query() else {
            panic!("expected set operation in CTE body");
        };
        let QueryExpression::Select(member) = &**right else {
            panic!("expected SELECT recursive member");
        };
        // Direct FROM x, LIMIT hoisted to the member tail.
        assert!(matches!(
            member.from().unwrap()[0],
            TableExpression::Table { ref name, .. } if name == "x"
        ));
        assert_eq!(member.limit().unwrap().count(), 2);
        // Wrapper reference substituted with the inner expression.
        let SelectItem::Expression { expr, .. } = &member.select_list()[0] else {
            panic!("expected expression item");
        };
        assert_eq!(expr, &qualified("x", "n"));
    }

    #[test]
    fn buried_limit_diagnoses_on_postgres() {
        let mut stmt = recursive_cte_stmt(buried_limit_member("x"));
        mark_recursive_ctes(&mut stmt);
        let err = legalize_recursive_limits(&mut stmt, SqlDialect::PostgreSQL).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("total-row cap"), "unexpected message: {msg}");
    }

    #[test]
    fn plain_recursive_member_untouched() {
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified("x", "n"), "n"))
            .from_tables(vec![table("x")])
            .build()
            .unwrap();
        let mut stmt = recursive_cte_stmt(member.clone());
        mark_recursive_ctes(&mut stmt);
        legalize_recursive_limits(&mut stmt, SqlDialect::PostgreSQL).unwrap();
        let QueryExpression::SetOperation { right, .. } = first_cte(&stmt).query() else {
            panic!("expected set operation");
        };
        assert_eq!(**right, QueryExpression::Select(Box::new(member)));
    }
}

#[cfg(test)]
mod validator_tests {
    use super::*;
    use crate::pipeline::ast_refined::LiteralValue;
    use crate::pipeline::sql_ast_v3::{ColumnQualifier, SetOperator};

    fn table(name: &str) -> TableExpression {
        TableExpression::Table {
            schema: None,
            name: name.to_string(),
            alias: None,
        }
    }

    fn qualified(table: &str, column: &str) -> DomainExpression {
        DomainExpression::Column {
            name: column.to_string(),
            qualifier: Some(ColumnQualifier::table(table)),
        }
    }

    fn base_member() -> SelectStatement {
        SelectStatement::builder()
            .select(SelectItem::expression_with_alias(
                DomainExpression::Literal(LiteralValue::Number("1".to_string())),
                "n",
            ))
            .build()
            .unwrap()
    }

    fn stmt_with_member(member: SelectStatement) -> SqlStatement {
        let body = QueryExpression::SetOperation {
            op: SetOperator::UnionAll,
            left: Box::new(QueryExpression::Select(Box::new(base_member()))),
            right: Box::new(QueryExpression::Select(Box::new(member))),
        };
        let mut cte = Cte::new("x", body);
        cte.set_recursive(true);
        SqlStatement::Query {
            with_clause: Some(vec![cte]),
            query: QueryExpression::Select(Box::new(
                SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_tables(vec![table("x")])
                    .build()
                    .unwrap(),
            )),
        }
    }

    fn expect_badge(stmt: &mut SqlStatement, leaf: &str) {
        let err = validate_recursive_members(stmt).unwrap_err();
        let uri = err.error_uri();
        assert!(
            uri.ends_with(&format!("semantic/recursion/{leaf}")),
            "expected {leaf}, got {uri}"
        );
    }

    #[test]
    fn nonlinear_two_direct_refs_refused() {
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified("a", "n"), "n"))
            .from_tables(vec![TableExpression::Join {
                left: Box::new(table("x")),
                right: Box::new(table("x")),
                join_type: crate::pipeline::sql_ast_v3::JoinType::Inner,
                join_condition: crate::pipeline::sql_ast_v3::JoinCondition::On(
                    DomainExpression::column("n"),
                ),
            }])
            .build()
            .unwrap();
        expect_badge(&mut stmt_with_member(member), "nonlinear");
    }

    #[test]
    fn self_ref_inside_exists_refused() {
        let inner = SelectStatement::builder()
            .select(SelectItem::star())
            .from_tables(vec![table("x")])
            .build()
            .unwrap();
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified("x", "n"), "n"))
            .from_tables(vec![table("x")])
            .where_clause(DomainExpression::Exists {
                not: true,
                query: Box::new(QueryExpression::Select(Box::new(inner))),
            })
            .build()
            .unwrap();
        expect_badge(&mut stmt_with_member(member), "self_subquery");
    }

    #[test]
    fn aggregate_in_member_refused() {
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(
                DomainExpression::Function {
                    name: "max".to_string(),
                    args: vec![qualified("x", "n")],
                    distinct: false,
                },
                "n",
            ))
            .from_tables(vec![table("x")])
            .build()
            .unwrap();
        expect_badge(&mut stmt_with_member(member), "aggregate");
    }

    #[test]
    fn linear_member_passes() {
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(qualified("x", "n"), "n"))
            .from_tables(vec![table("x")])
            .build()
            .unwrap();
        validate_recursive_members(&mut stmt_with_member(member)).unwrap();
    }


    #[test]
    fn shadowed_inner_cte_does_not_mark_outer() {
        // Nested HO pipes reuse internal CTE names: an outer CTE whose body
        // contains a nested WITH defining the SAME name is not recursive —
        // the inner reference belongs to the inner CTE (stress/391).
        let inner_body = QueryExpression::Select(Box::new(
            SelectStatement::builder()
                .select(SelectItem::star())
                .from_tables(vec![table("x")]) // refers to the INNER x
                .build()
                .unwrap(),
        ));
        let nested = QueryExpression::WithCte {
            ctes: vec![Cte::new("x", inner_body)],
            query: Box::new(QueryExpression::Select(Box::new(
                SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_tables(vec![table("x")])
                    .build()
                    .unwrap(),
            ))),
        };
        let mut stmt = SqlStatement::Query {
            with_clause: Some(vec![Cte::new("x", nested)]),
            query: QueryExpression::Select(Box::new(
                SelectStatement::builder()
                    .select(SelectItem::star())
                    .from_tables(vec![table("x")])
                    .build()
                    .unwrap(),
            )),
        };
        mark_recursive_ctes(&mut stmt);
        let SqlStatement::Query { with_clause: Some(ctes), .. } = &stmt else {
            panic!("expected WITH");
        };
        assert!(!ctes[0].is_recursive(), "shadowed reference over-marked");
        validate_recursive_members(&mut stmt).unwrap();
    }

    #[test]
    fn base_member_with_aggregate_passes() {
        // Aggregation is only refused in RECURSIVE members; a base member
        // (no self-reference) may aggregate freely.
        let member = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(
                DomainExpression::Function {
                    name: "max".to_string(),
                    args: vec![qualified("t", "n")],
                    distinct: false,
                },
                "n",
            ))
            .from_tables(vec![table("t")])
            .build()
            .unwrap();
        validate_recursive_members(&mut stmt_with_member(member)).unwrap();
    }
}
