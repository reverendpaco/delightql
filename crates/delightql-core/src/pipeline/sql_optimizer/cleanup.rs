// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// cleanup.rs - Subquery boundary collapse
//
// One rule: if the child SELECT has no epistemological barrier
// (no GROUP BY, DISTINCT, LIMIT, set ops, window functions, aggregates),
// collapse the parent-child boundary.
//
// Post-order walker means children are already optimized by the
// time the parent is visited — so we only inspect one level down.
//
// The collapse handles all four cases uniformly:
//   SELECT *    / no WHERE  → trivial (discard wrapper)
//   SELECT *    / WHERE     → merge WHERE into child
//   SELECT cols / no WHERE  → substitute column defs from child
//   SELECT cols / WHERE     → substitute + merge
//
// NOTE: JOIN inlining (collapsing subqueries inside JOINs) is future work.
// The current pass only handles single-FROM collapse.

use crate::error::Result;
use crate::pipeline::sql_ast_v3::{
    BinaryOperator, DomainExpression, OrderTerm, QualifierParts, QueryExpression, SelectItem,
    SelectStatement, SqlStatement, TableExpression,
};

use super::visitor::{apply_transformer, QueryTransformer};

pub(super) fn pass_cleanup(stmt: SqlStatement) -> Result<SqlStatement> {
    let mut transformer = CollapseTransformer {
        enclosing_scopes: Vec::new(),
    };
    apply_transformer(stmt, &mut transformer)
}

struct CollapseTransformer {
    /// FROM-exposed names of the statements the walker is currently
    /// inside the EXPRESSIONS of — the enclosing scopes a subquery met
    /// here can correlate against. Drives the capture barrier.
    enclosing_scopes: Vec<Vec<String>>,
}

impl QueryTransformer for CollapseTransformer {
    fn enter_expr_scope(&mut self, names: &[String]) {
        self.enclosing_scopes.push(names.to_vec());
    }

    fn exit_expr_scope(&mut self) {
        self.enclosing_scopes.pop();
    }

    fn transform_query(&mut self, query: QueryExpression) -> Result<Option<QueryExpression>> {
        let QueryExpression::Select(ref select_stmt) = query else {
            return Ok(None);
        };

        let Some(from) = select_stmt.from() else {
            return Ok(None);
        };

        // Single-FROM: full collapse (merge outer into inner)
        if from.len() == 1 {
            match &from[0] {
                TableExpression::Subquery {
                    query: inner_q,
                    alias,
                } => {
                    let inner_query = (**inner_q).clone().into_inner();
                    let enclosing: Vec<String> = self
                        .enclosing_scopes
                        .iter()
                        .flatten()
                        .cloned()
                        .collect();
                    if let Some(collapsed) =
                        try_collapse(select_stmt, &inner_query, alias, &enclosing)?
                    {
                        return Ok(Some(collapsed));
                    }
                }
                TableExpression::UnionTable { selects, alias: _ } => {
                    if is_trivial_star_wrapper(select_stmt) {
                        return Ok(Some(union_table_to_set_op(selects)?));
                    }
                }
                _ => {}
            }
        }

        Ok(None)
    }

    /// Inline transparent subqueries inside JOIN trees.
    /// Scans the FROM tree, finds subqueries with no barriers whose inner
    /// FROM is a single bare table, replaces them, and rewrites all
    /// column references in the owning SELECT.
    fn transform_select(&mut self, stmt: SelectStatement) -> Result<Option<SelectStatement>> {
        let Some(from) = stmt.from() else {
            return Ok(None);
        };

        // Scan FROM tree for inlineable subqueries
        let mut rewrites: Vec<(String, std::collections::HashMap<String, ColDef>)> = Vec::new();
        let mut extra_wheres: Vec<DomainExpression> = Vec::new();
        let mut new_from = Vec::new();
        let mut any_inlined = false;
        let mut claimed: Vec<String> = Vec::new();

        for table in from {
            let (new_table, did_inline) =
                scan_and_inline(table, &stmt, &mut rewrites, &mut extra_wheres, &mut claimed)?;
            new_from.push(new_table);
            if did_inline {
                any_inlined = true;
            }
        }

        if !any_inlined {
            return Ok(None);
        }

        // Rebuild SELECT with all references rewritten
        let mut builder = SelectStatement::builder();
        if stmt.is_distinct() {
            builder = builder.distinct();
        }

        // Rewrite SELECT list
        let mut items = stmt.select_list().to_vec();
        for (alias, col_map) in &rewrites {
            let r: Option<Vec<_>> = items
                .into_iter()
                .map(|item| match &item {
                    SelectItem::Expression {
                        expr,
                        alias: item_alias,
                    } => {
                        let rewritten = rewrite_expr(expr, alias, col_map)?;
                        let final_alias = if item_alias.is_some() {
                            item_alias.clone()
                        } else {
                            let orig = expr_natural_name(expr);
                            let new_name = expr_natural_name(&rewritten);
                            if orig != new_name {
                                orig
                            } else {
                                None
                            }
                        };
                        Some(SelectItem::Expression {
                            expr: rewritten,
                            alias: final_alias,
                        })
                    }
                    _ => Some(item),
                })
                .collect();
            items = match r {
                Some(list) => list,
                None => return Ok(None),
            };
        }
        builder = builder.select_all(items);

        // Rewrite JOIN ON conditions in the FROM tree
        let mut final_from = new_from;
        for (alias, col_map) in &rewrites {
            final_from = final_from
                .into_iter()
                .map(|t| rewrite_table_refs(&t, alias, col_map))
                .collect::<Option<Vec<_>>>()
                .unwrap_or_else(|| {
                    // Bail — but we can't undo. This shouldn't happen
                    // if our barrier checks are correct.
                    vec![]
                });
        }
        builder = builder.from_tables(final_from);

        // Merge WHERE: rewrite outer WHERE + add inner WHEREs
        let mut where_parts: Vec<DomainExpression> = Vec::new();
        if let Some(w) = stmt.where_clause() {
            let mut expr = w.clone();
            for (alias, col_map) in &rewrites {
                expr = match rewrite_expr(&expr, alias, col_map) {
                    Some(r) => r,
                    None => return Ok(None),
                };
            }
            where_parts.push(expr);
        }
        where_parts.extend(extra_wheres);
        if !where_parts.is_empty() {
            let merged = where_parts
                .into_iter()
                .reduce(|a, b| DomainExpression::Binary {
                    left: Box::new(a),
                    op: BinaryOperator::And,
                    right: Box::new(b),
                })
                .unwrap();
            builder = builder.where_clause(merged);
        }

        // Rewrite GROUP BY. A bare literal must not land in GROUP BY
        // position — SQL reads a bare integer there as a column ORDINAL,
        // so a key that inlines to a constant would silently regroup (or
        // error out of range). Keep the boundary instead.
        if let Some(gb) = stmt.group_by() {
            let mut exprs = gb.to_vec();
            for (alias, col_map) in &rewrites {
                let r: Option<Vec<_>> = exprs
                    .into_iter()
                    .map(|e| rewrite_expr(&e, alias, col_map))
                    .collect();
                exprs = r.unwrap_or_default();
            }
            if exprs.iter().any(lands_as_ordinal) {
                return Ok(None);
            }
            if !exprs.is_empty() {
                builder = builder.group_by(exprs);
            }
        }

        // Rewrite HAVING
        if let Some(having) = stmt.having() {
            let mut h = having.clone();
            for (alias, col_map) in &rewrites {
                h = match rewrite_expr(&h, alias, col_map) {
                    Some(r) => r,
                    None => return Ok(None),
                };
            }
            builder = builder.having(h);
        }

        // Rewrite ORDER BY. Same ordinal hazard as GROUP BY: `ORDER BY 42`
        // is a position, not a constant.
        if let Some(order_by) = stmt.order_by() {
            for term in order_by {
                let mut e = term.expr().clone();
                for (alias, col_map) in &rewrites {
                    e = match rewrite_expr(&e, alias, col_map) {
                        Some(r) => r,
                        None => return Ok(None),
                    };
                }
                if lands_as_ordinal(&e) {
                    return Ok(None);
                }
                builder = builder.order_by(OrderTerm::new(e, term.direction().cloned()));
            }
        }

        if let Some(limit) = stmt.limit() {
            if let Some(offset) = limit.offset() {
                builder = builder.limit_offset(limit.count(), offset);
            } else {
                builder = builder.limit(limit.count());
            }
        }

        match builder.build() {
            Ok(s) => Ok(Some(s)),
            Err(_) => Ok(None),
        }
    }
}

/// Walk a FROM table tree, replacing inlineable subqueries with bare tables.
fn scan_and_inline(
    table: &TableExpression,
    outer: &SelectStatement,
    rewrites: &mut Vec<(String, std::collections::HashMap<String, ColDef>)>,
    extra_wheres: &mut Vec<DomainExpression>,
    claimed: &mut Vec<String>,
) -> Result<(TableExpression, bool)> {
    scan_and_inline_inner(table, outer, rewrites, extra_wheres, claimed, false)
}

fn scan_and_inline_inner(
    table: &TableExpression,
    outer: &SelectStatement,
    rewrites: &mut Vec<(String, std::collections::HashMap<String, ColDef>)>,
    extra_wheres: &mut Vec<DomainExpression>,
    claimed: &mut Vec<String>,
    inside_join: bool,
) -> Result<(TableExpression, bool)> {
    match table {
        // Only inline Subqueries that are INSIDE a Join (not top-level FROM items).
        // Top-level single Subqueries are handled by transform_query with its own guards.
        // We detect "inside a Join" by only matching Subquery when called recursively
        // from the Join branch below — top-level calls come from transform_select's
        // FROM iteration, which we skip.
        TableExpression::Subquery { .. } if !inside_join => {
            return Ok((table.clone(), false));
        }
        TableExpression::Subquery {
            query: inner_q,
            alias,
        } => {
            let inner_query = (**inner_q).clone().into_inner();
            let QueryExpression::Select(ref inner_box) = inner_query else {
                return Ok((table.clone(), false));
            };
            let inner = inner_box.as_ref();

            // Epistemological barriers
            if inner.group_by().is_some()
                || inner.having().is_some()
                || inner.is_distinct()
                || inner.limit().is_some()
                || inner.order_by().is_some()
            {
                return Ok((table.clone(), false));
            }
            if inner_has_aggregates(inner) || inner_select_has_window_functions(inner) {
                return Ok((table.clone(), false));
            }

            // Inner FROM must be exactly one bare table
            let Some(inner_from) = inner.from() else {
                return Ok((table.clone(), false));
            };
            if inner_from.len() != 1 {
                return Ok((table.clone(), false));
            }
            let inner_table_name = match &inner_from[0] {
                TableExpression::Table {
                    name,
                    alias: table_alias,
                    ..
                } => table_alias.as_ref().unwrap_or(name).clone(),
                _ => {
                    return Ok((table.clone(), false));
                }
            };

            // Build column map. For SELECT *, columns pass through with a
            // qualifier swap: subquery_alias.col → inner_table.col
            let is_star = inner
                .select_list()
                .iter()
                .any(|i| matches!(i, SelectItem::Star | SelectItem::QualifiedStar { .. }));

            let col_map = if is_star {
                // Empty map signals "qualifier swap only" mode in rewrite_expr
                std::collections::HashMap::new()
            } else {
                let map = build_inner_column_map(inner.select_list());
                if map.is_empty() {
                    return Ok((table.clone(), false));
                }
                map
            };

            // Bail if outer has correlated refs for this alias
            if outer_has_correlated_refs_to(outer, alias) {
                return Ok((table.clone(), false));
            }

            // For non-star, check for unresolvable columns
            if !is_star && !col_map.is_empty() {
                for item in outer.select_list() {
                    if let SelectItem::Expression { expr, .. } = item {
                        if has_unresolvable_column(expr, alias, &col_map) {
                            return Ok((table.clone(), false));
                        }
                    }
                }
                if let Some(w) = outer.where_clause() {
                    if has_unresolvable_column(w, alias, &col_map) {
                        return Ok((table.clone(), false));
                    }
                }
                // A TVF argument referencing this alias can only be
                // rewritten by qualifier swap when its mapped definition
                // is a plain column. `json_each(alias._extracted)` over
                // an EXPRESSION definition has no legal rewrite
                // (TvfArgument carries no expressions); inlining anyway
                // leaves the argument naming a table that no longer
                // exists.
                if let Some(from) = outer.from() {
                    for t in from {
                        if tvf_refs_block_inline(t, alias, &col_map) {
                            return Ok((table.clone(), false));
                        }
                    }
                }
            }

            // For star-passthrough, build a qualifier-swap map:
            // any column qualified with the subquery alias gets re-qualified
            // with the inner table name.
            // Only inline with explicit column lists — star passthrough
            // can't be safely rewritten without schema info (destructured
            // columns, TVF arguments referencing synthetic columns, etc.)
            if is_star {
                return Ok((table.clone(), false));
            }
            let effective_map = col_map;

            // ── Collision barrier ──
            // Inlining exposes the inner table's name into the owning
            // statement's scope. If a SIBLING FROM item (or an earlier
            // inline in this same pass) already exposes that name — the
            // classic shape is a view of `employees` joined next to
            // `employees` itself — the collapse would put two operands
            // under one SQL qualifier (`FROM employees INNER JOIN
            // employees`) and every reference goes ambiguous. Keep the
            // aliased subquery; the boundary is load-bearing.
            let mut sibling_names: Vec<String> = Vec::new();
            if let Some(from) = outer.from() {
                for t in from {
                    for n in super::visitor::exposed_table_names(t) {
                        if n != *alias {
                            sibling_names.push(n);
                        }
                    }
                }
            }
            if sibling_names
                .iter()
                .chain(claimed.iter())
                .any(|n| n == &inner_table_name)
            {
                return Ok((table.clone(), false));
            }

            // Inline: record rewrite, capture inner WHERE, return bare table
            if let Some(w) = inner.where_clause() {
                extra_wheres.push(w.clone());
            }
            claimed.push(inner_table_name.clone());
            rewrites.push((alias.clone(), effective_map));
            Ok((inner_from[0].clone(), true))
        }
        TableExpression::Join {
            left,
            join_type,
            right,
            join_condition,
        } => {
            // Don't inline through USING — column renames break USING joins.
            // Natural in this AST means "no explicit condition" (predicate is in WHERE),
            // not SQL NATURAL JOIN — it's safe to inline through.
            if matches!(
                join_condition,
                crate::pipeline::sql_ast_v3::JoinCondition::Using(_)
            ) {
                return Ok((table.clone(), false));
            }

            let (new_left, l) = scan_and_inline_inner(left, outer, rewrites, extra_wheres, claimed, true)?;
            let (new_right, r) = scan_and_inline_inner(right, outer, rewrites, extra_wheres, claimed, true)?;

            if l || r {
                Ok((
                    TableExpression::Join {
                        left: Box::new(new_left),
                        join_type: join_type.clone(),
                        right: Box::new(new_right),
                        join_condition: join_condition.clone(), // ON rewritten by transform_select
                    },
                    true,
                ))
            } else {
                Ok((table.clone(), false))
            }
        }
        _ => Ok((table.clone(), false)),
    }
}

/// Does any TVF in this table tree reference `alias` with a column whose
/// mapped definition is NOT a plain column (or is unmapped)? Such a
/// reference blocks inlining — see the guard at the decision site.
fn tvf_refs_block_inline(
    table: &TableExpression,
    alias: &str,
    col_map: &std::collections::HashMap<String, ColDef>,
) -> bool {
    use crate::pipeline::sql_ast_v3::TvfArgument;
    match table {
        TableExpression::Join { left, right, .. } => {
            tvf_refs_block_inline(left, alias, col_map)
                || tvf_refs_block_inline(right, alias, col_map)
        }
        TableExpression::TVF { arguments, .. } => arguments.iter().any(|arg| {
            let column = match arg {
                TvfArgument::QualifiedRef { qualifier, column } if qualifier == alias => {
                    Some(column)
                }
                TvfArgument::ColumnRef { qualifier, column }
                    if qualifier.table_name() == alias =>
                {
                    Some(column)
                }
                _ => None,
            };
            match column {
                Some(c) => !matches!(
                    col_map.get(c).map(|d| &d.expr),
                    Some(DomainExpression::Column { .. })
                ),
                None => false,
            }
        }),
        _ => false,
    }
}

/// Rewrite ON conditions in a table expression tree through a single alias→col_map.
/// Rewrite alias references inside a table expression tree:
/// JOIN ON conditions and TVF (table-valued function) arguments.
fn rewrite_table_refs(
    table: &TableExpression,
    alias: &str,
    col_map: &std::collections::HashMap<String, ColDef>,
) -> Option<TableExpression> {
    match table {
        TableExpression::Join {
            left,
            join_type,
            right,
            join_condition,
        } => {
            let new_left = rewrite_table_refs(left, alias, col_map)?;
            let new_right = rewrite_table_refs(right, alias, col_map)?;
            let new_cond = match join_condition {
                crate::pipeline::sql_ast_v3::JoinCondition::On(expr) => {
                    crate::pipeline::sql_ast_v3::JoinCondition::On(rewrite_expr(
                        expr, alias, col_map,
                    )?)
                }
                other => other.clone(),
            };
            Some(TableExpression::Join {
                left: Box::new(new_left),
                join_type: join_type.clone(),
                right: Box::new(new_right),
                join_condition: new_cond,
            })
        }
        TableExpression::TVF {
            schema,
            function,
            arguments,
            alias: tvf_alias,
        } => {
            use crate::pipeline::sql_ast_v3::TvfArgument;
            let new_args: Vec<_> = arguments
                .iter()
                .map(|arg| match arg {
                    TvfArgument::QualifiedRef { qualifier, column } if qualifier == alias => {
                        // Look up the column in the map to find the real table
                        if col_map.is_empty() {
                            // Star passthrough — we don't know the real table name.
                            // But the Subquery was replaced with a bare Table, so
                            // the qualifier should become that table's name.
                            // We can't resolve it here — leave as-is and bail.
                            // Actually: for star passthrough, the inner FROM was a
                            // bare table. Its name is what we need. But we don't
                            // have it here. For now, strip qualifier.
                            TvfArgument::Identifier(column.clone())
                        } else if let Some(def) = col_map.get(column) {
                            // Substitute with the inner expression's qualifier
                            match &def.expr {
                                DomainExpression::Column {
                                    name,
                                    qualifier: Some(q),
                                } => TvfArgument::QualifiedRef {
                                    qualifier: q.table_name().to_string(),
                                    column: name.clone(),
                                },
                                DomainExpression::Column {
                                    name,
                                    qualifier: None,
                                } => TvfArgument::Identifier(name.clone()),
                                _ => arg.clone(),
                            }
                        } else {
                            arg.clone()
                        }
                    }
                    TvfArgument::ColumnRef { qualifier, column }
                        if qualifier.table_name() == alias =>
                    {
                        if let Some(def) = col_map.get(column) {
                            match &def.expr {
                                DomainExpression::Column {
                                    name,
                                    qualifier: Some(q),
                                } => TvfArgument::ColumnRef {
                                    qualifier: q.clone(),
                                    column: name.clone(),
                                },
                                _ => arg.clone(),
                            }
                        } else if col_map.is_empty() {
                            // Star passthrough — just strip (same issue)
                            TvfArgument::Identifier(column.clone())
                        } else {
                            arg.clone()
                        }
                    }
                    other => other.clone(),
                })
                .collect();
            Some(TableExpression::TVF {
                schema: schema.clone(),
                function: function.clone(),
                arguments: new_args,
                alias: tvf_alias.clone(),
            })
        }
        other => Some(other.clone()),
    }
}

/// Check if a SELECT is a trivial `SELECT * ... ` with no other clauses
/// True when SQL would read the expression in GROUP BY / ORDER BY
/// position as a column ORDINAL rather than a constant. Parentheses are
/// transparent to that reading — SQLite treats `GROUP BY (42)` exactly
/// like `GROUP BY 42` — so the test strips them before matching. Any
/// literal bails conservatively: bailing the collapse is always
/// semantics-preserving, in every dialect.
fn lands_as_ordinal(expr: &DomainExpression) -> bool {
    match expr {
        DomainExpression::Literal(_) => true,
        DomainExpression::Parens(inner) => lands_as_ordinal(inner),
        _ => false,
    }
}

fn is_trivial_star_wrapper(stmt: &SelectStatement) -> bool {
    let list = stmt.select_list();
    list.len() == 1
        && matches!(list[0], SelectItem::Star)
        && stmt.where_clause().is_none()
        && stmt.group_by().is_none()
        && stmt.having().is_none()
        && stmt.order_by().is_none()
        && stmt.limit().is_none()
        && !stmt.is_distinct()
}

/// Convert a UnionTable's selects into nested SetOperation
fn union_table_to_set_op(selects: &[QueryExpression]) -> Result<QueryExpression> {
    if selects.is_empty() {
        return Err(crate::error::DelightQLError::ParseError {
            message: "Empty UnionTable".to_string(),
            source: None,
            subcategory: None,
        });
    }
    if selects.len() == 1 {
        return Ok(selects[0].clone());
    }
    let mut result = selects[selects.len() - 1].clone();
    for i in (0..selects.len() - 1).rev() {
        result = QueryExpression::SetOperation {
            op: crate::pipeline::sql_ast_v3::SetOperator::UnionAll,
            left: Box::new(selects[i].clone()),
            right: Box::new(result),
        };
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Core collapse logic
// ---------------------------------------------------------------------------

/// Try to collapse an outer SELECT into its inner query.
/// Returns None if the boundary cannot be removed (epistemological barrier,
/// or a pattern we can't safely rewrite).
fn try_collapse(
    outer: &SelectStatement,
    inner_query: &QueryExpression,
    subquery_alias: &str,
    enclosing_scopes: &[String],
) -> Result<Option<QueryExpression>> {
    // Inner must be a plain SELECT (not set-op, not CTE, not VALUES)
    let QueryExpression::Select(ref inner_box) = inner_query else {
        return Ok(None);
    };
    let inner = inner_box.as_ref();

    // ── Epistemological barriers: cannot push through ──
    if inner.group_by().is_some()
        || inner.having().is_some()
        || inner.is_distinct()
        || inner.limit().is_some()
        || inner.order_by().is_some()
    {
        return Ok(None);
    }

    // ── Implicit aggregation barrier ──
    // If the inner has aggregate functions in its SELECT list but no GROUP BY,
    // it's performing a full-table aggregation — that's an epistemological barrier.
    if inner_has_aggregates(inner) {
        return Ok(None);
    }

    // ── Window function barrier ──
    if inner_select_has_window_functions(inner) {
        return Ok(None);
    }

    // ── Build the column substitution map ──
    // If the inner has SELECT * (possibly mixed with explicit columns), we can't
    // build a complete column map. Only proceed if the outer is a trivial
    // SELECT * wrapper — otherwise we'd lose track of column names.
    let inner_has_star = inner
        .select_list()
        .iter()
        .any(|item| matches!(item, SelectItem::Star | SelectItem::QualifiedStar { .. }));
    if inner_has_star && !is_trivial_star_wrapper(outer) {
        return Ok(None);
    }
    let inner_col_map = build_inner_column_map(inner.select_list());

    // ── Bail if correlated subqueries reference the alias we're removing ──
    // EXISTS/IN/scalar subqueries inside the outer's WHERE/HAVING may contain
    // correlated references to the subquery alias. We can't rewrite inside
    // those scopes, so bail.
    if outer_has_correlated_refs_to(outer, subquery_alias) {
        return Ok(None);
    }

    // ── Capture barrier ──
    // Collapsing exposes the inner FROM's table names into the outer's
    // scope. If one of those names ALSO names an ENCLOSING statement's
    // scope and the outer references it, that reference is correlated
    // OUT today and would be captured by the exposure — the classic
    // shape is a rule inlined over `employees` inside a scalar subquery
    // whose enclosing query also reads `employees`. Only enclosing-
    // scope names can be captured: an outer reference to a name no
    // enclosing scope carries is an anticipatory reference to the very
    // exposure this collapse performs (the delegate lowering does
    // this), and collapsing is what makes it valid. Bail only on the
    // former.
    if let Some(inner_from) = inner.from() {
        let mut exposed: Vec<String> = Vec::new();
        for t in inner_from {
            collect_exposed_table_names(t, &mut exposed);
        }
        for name in &exposed {
            if name == subquery_alias {
                continue;
            }
            if !enclosing_scopes.iter().any(|s| s == name) {
                continue;
            }
            let referenced = outer
                .select_list()
                .iter()
                .any(|item| match item {
                    SelectItem::Expression { expr, .. } => expr_references_alias(expr, name),
                    _ => false,
                })
                || outer
                    .where_clause()
                    .is_some_and(|w| expr_references_alias(w, name))
                || outer.having().is_some_and(|h| expr_references_alias(h, name));
            if referenced {
                return Ok(None);
            }
        }
    }

    // ── Bail if the outer SELECT list has unqualified columns not in the map ──
    // These could be synthetic columns (e.g. from JSON destructuring) that only
    // exist in the subquery boundary's output scope, not in the inner query.
    if !inner_col_map.is_empty() {
        for item in outer.select_list() {
            if let SelectItem::Expression { expr, .. } = item {
                if has_unresolvable_column(expr, subquery_alias, &inner_col_map) {
                    return Ok(None);
                }
            }
        }
    }

    // ── Determine what the outer is doing ──
    let is_star = outer.select_list().len() == 1
        && matches!(
            outer.select_list()[0],
            SelectItem::Star | SelectItem::QualifiedStar { .. }
        );

    // ── Rewrite outer's SELECT list ──
    let new_select_list = if is_star {
        inner.select_list().to_vec()
    } else {
        match rewrite_select_list(outer.select_list(), subquery_alias, &inner_col_map) {
            Some(list) => list,
            None => return Ok(None),
        }
    };

    // ── Bail if outer WHERE has unresolvable columns ──
    if !inner_col_map.is_empty() {
        if let Some(outer_where) = outer.where_clause() {
            if has_unresolvable_column(outer_where, subquery_alias, &inner_col_map) {
                return Ok(None);
            }
        }
    }

    // ── Rewrite outer's WHERE clause ──
    let outer_where_rewritten = if let Some(outer_where) = outer.where_clause() {
        match rewrite_expr(outer_where, subquery_alias, &inner_col_map) {
            Some(expr) => Some(expr),
            None => return Ok(None),
        }
    } else {
        None
    };

    // ── Merge WHERE clauses ──
    let merged_where = match (inner.where_clause(), outer_where_rewritten) {
        (Some(iw), Some(ow)) => Some(DomainExpression::Binary {
            left: Box::new(iw.clone()),
            op: BinaryOperator::And,
            right: Box::new(ow),
        }),
        (Some(iw), None) => Some(iw.clone()),
        (None, Some(ow)) => Some(ow),
        (None, None) => None,
    };

    // ── Rebuild ──
    let mut builder = SelectStatement::builder();

    if outer.is_distinct() {
        builder = builder.distinct();
    }

    builder = builder.select_all(new_select_list);

    if let Some(inner_from) = inner.from() {
        builder = builder.from_tables(inner_from.to_vec());
    }

    if let Some(w) = merged_where {
        builder = builder.where_clause(w);
    }

    // Rewrite GROUP BY through column map. A bare literal must not land
    // in GROUP BY position — SQL reads a bare integer there as a column
    // ORDINAL, so a key that folds to a constant would silently regroup
    // (or error out of range). Keep the boundary instead.
    if let Some(gb) = outer.group_by() {
        let rewritten_gb: Option<Vec<_>> = gb
            .iter()
            .map(|expr| rewrite_expr(expr, subquery_alias, &inner_col_map))
            .collect();
        match rewritten_gb {
            Some(exprs) => {
                if exprs.iter().any(lands_as_ordinal) {
                    return Ok(None);
                }
                builder = builder.group_by(exprs)
            }
            None => return Ok(None),
        }
    }

    // Rewrite HAVING through column map
    if let Some(having) = outer.having() {
        match rewrite_expr(having, subquery_alias, &inner_col_map) {
            Some(expr) => builder = builder.having(expr),
            None => return Ok(None),
        }
    }

    // Rewrite ORDER BY through column map. Same ordinal hazard as
    // GROUP BY: `ORDER BY 42` is a position, not a constant — a column
    // whose value folds to a literal must keep the boundary.
    if let Some(order_by) = outer.order_by() {
        for term in order_by {
            match rewrite_expr(term.expr(), subquery_alias, &inner_col_map) {
                Some(rewritten_expr) => {
                    if lands_as_ordinal(&rewritten_expr) {
                        return Ok(None);
                    }
                    builder =
                        builder.order_by(OrderTerm::new(rewritten_expr, term.direction().cloned()));
                }
                None => return Ok(None),
            }
        }
    }

    if let Some(limit) = outer.limit() {
        if let Some(offset) = limit.offset() {
            builder = builder.limit_offset(limit.count(), offset);
        } else {
            builder = builder.limit(limit.count());
        }
    }

    match builder.build() {
        Ok(stmt) => Ok(Some(QueryExpression::Select(Box::new(stmt)))),
        Err(_) => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Barrier detection
// ---------------------------------------------------------------------------

/// Check if an expression contains an unqualified column reference that is
/// NOT in the inner column map. Such columns might be synthetic (e.g. from
/// JSON destructuring) and would break if the subquery boundary is removed.
fn has_unresolvable_column(
    expr: &DomainExpression,
    subquery_alias: &str,
    col_map: &std::collections::HashMap<String, ColDef>,
) -> bool {
    match expr {
        DomainExpression::Column { name, qualifier } => {
            match qualifier {
                Some(q) => match q.parts() {
                    // Qualified with subquery alias but not in map → unresolvable
                    QualifierParts::Table(t) if t == subquery_alias => !col_map.contains_key(name),
                    _ => false,
                },
                // Unqualified and not in map → might be synthetic
                None => !col_map.contains_key(name),
            }
        }
        DomainExpression::Binary { left, right, .. } => {
            has_unresolvable_column(left, subquery_alias, col_map)
                || has_unresolvable_column(right, subquery_alias, col_map)
        }
        DomainExpression::Unary { expr, .. } => {
            has_unresolvable_column(expr, subquery_alias, col_map)
        }
        DomainExpression::Cast { expr, .. } => {
            has_unresolvable_column(expr, subquery_alias, col_map)
        }
        DomainExpression::Function { args, .. } => args
            .iter()
            .any(|a| has_unresolvable_column(a, subquery_alias, col_map)),
        DomainExpression::Parens(inner) => has_unresolvable_column(inner, subquery_alias, col_map),
        DomainExpression::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .any(|a| has_unresolvable_column(a, subquery_alias, col_map))
                || partition_by
                    .iter()
                    .any(|a| has_unresolvable_column(a, subquery_alias, col_map))
                || order_by
                    .iter()
                    .any(|(a, _)| has_unresolvable_column(a, subquery_alias, col_map))
        }
        _ => false,
    }
}

/// Check if inner SELECT list contains aggregate functions (implicit aggregation)
fn inner_has_aggregates(inner: &SelectStatement) -> bool {
    inner.select_list().iter().any(|item| {
        if let SelectItem::Expression { expr, .. } = item {
            expr_contains_aggregate(expr)
        } else {
            false
        }
    })
}

/// Common SQL aggregate function names
fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "GROUP_CONCAT" | "TOTAL"
    )
}

fn expr_contains_aggregate(expr: &DomainExpression) -> bool {
    match expr {
        DomainExpression::Function { name, args, .. } => {
            if is_aggregate_name(name) {
                return true;
            }
            args.iter().any(expr_contains_aggregate)
        }
        DomainExpression::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        DomainExpression::Unary { expr, .. } => expr_contains_aggregate(expr),
        DomainExpression::Cast { expr, .. } => expr_contains_aggregate(expr),
        DomainExpression::Parens(inner) => expr_contains_aggregate(inner),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_ref().map_or(false, |e| expr_contains_aggregate(e))
                || when_clauses.iter().any(|wc| {
                    expr_contains_aggregate(wc.when()) || expr_contains_aggregate(wc.then())
                })
                || else_clause
                    .as_ref()
                    .map_or(false, |e| expr_contains_aggregate(e))
        }
        // Window functions contain aggregates but are scoped — don't count them
        // (we already bail on window functions separately)
        _ => false,
    }
}

/// Check if inner SELECT list contains window functions
fn inner_select_has_window_functions(inner: &SelectStatement) -> bool {
    inner.select_list().iter().any(|item| {
        if let SelectItem::Expression { expr, .. } = item {
            expr_contains_window(expr)
        } else {
            false
        }
    })
}

fn expr_contains_window(expr: &DomainExpression) -> bool {
    match expr {
        DomainExpression::WindowFunction { .. } => true,
        DomainExpression::Binary { left, right, .. } => {
            expr_contains_window(left) || expr_contains_window(right)
        }
        DomainExpression::Unary { expr, .. } => expr_contains_window(expr),
        DomainExpression::Cast { expr, .. } => expr_contains_window(expr),
        DomainExpression::Parens(inner) => expr_contains_window(inner),
        DomainExpression::Function { args, .. } => args.iter().any(expr_contains_window),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_ref().map_or(false, |e| expr_contains_window(e))
                || when_clauses
                    .iter()
                    .any(|wc| expr_contains_window(wc.when()) || expr_contains_window(wc.then()))
                || else_clause
                    .as_ref()
                    .map_or(false, |e| expr_contains_window(e))
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Correlated subquery reference detection
// ---------------------------------------------------------------------------

/// Check if the outer SELECT's WHERE, HAVING, or SELECT list contains
/// subqueries (EXISTS, IN subquery, scalar subquery) that reference
/// the subquery alias we're about to remove.
/// The table names a FROM item would expose into the enclosing scope if
/// its subquery boundary were removed: bare tables by alias-or-name,
/// aliased subqueries by alias, join trees recursively.
fn collect_exposed_table_names(table: &TableExpression, out: &mut Vec<String>) {
    match table {
        TableExpression::Table { name, alias, .. } => {
            out.push(alias.as_deref().unwrap_or(name).to_string());
        }
        TableExpression::Subquery { alias, .. } => out.push(alias.clone()),
        TableExpression::Join { left, right, .. } => {
            collect_exposed_table_names(left, out);
            collect_exposed_table_names(right, out);
        }
        _ => {}
    }
}

fn outer_has_correlated_refs_to(outer: &SelectStatement, alias: &str) -> bool {
    // Check SELECT list
    for item in outer.select_list() {
        if let SelectItem::Expression { expr, .. } = item {
            if expr_subqueries_reference_alias(expr, alias) {
                return true;
            }
        }
    }
    // Check WHERE
    if let Some(w) = outer.where_clause() {
        if expr_subqueries_reference_alias(w, alias) {
            return true;
        }
    }
    // Check HAVING
    if let Some(h) = outer.having() {
        if expr_subqueries_reference_alias(h, alias) {
            return true;
        }
    }
    false
}

/// Check if any subquery expression (EXISTS, IN subquery, scalar subquery)
/// within this expression tree contains a reference to the given alias.
fn expr_subqueries_reference_alias(expr: &DomainExpression, alias: &str) -> bool {
    match expr {
        DomainExpression::Exists { query, .. } => query_references_alias(query, alias),
        DomainExpression::InSubquery { expr, query, .. } => {
            expr_subqueries_reference_alias(expr, alias) || query_references_alias(query, alias)
        }
        DomainExpression::Subquery(query) => query_references_alias(query, alias),

        // Recurse into non-subquery compound expressions
        DomainExpression::Binary { left, right, .. } => {
            expr_subqueries_reference_alias(left, alias)
                || expr_subqueries_reference_alias(right, alias)
        }
        DomainExpression::Unary { expr, .. } => expr_subqueries_reference_alias(expr, alias),
        DomainExpression::Cast { expr, .. } => expr_subqueries_reference_alias(expr, alias),
        DomainExpression::Parens(inner) => expr_subqueries_reference_alias(inner, alias),
        DomainExpression::Function { args, .. } => args
            .iter()
            .any(|a| expr_subqueries_reference_alias(a, alias)),
        DomainExpression::InList { expr, values, .. } => {
            expr_subqueries_reference_alias(expr, alias)
                || values
                    .iter()
                    .any(|v| expr_subqueries_reference_alias(v, alias))
        }
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_ref()
                .map_or(false, |e| expr_subqueries_reference_alias(e, alias))
                || when_clauses.iter().any(|wc| {
                    expr_subqueries_reference_alias(wc.when(), alias)
                        || expr_subqueries_reference_alias(wc.then(), alias)
                })
                || else_clause
                    .as_ref()
                    .map_or(false, |e| expr_subqueries_reference_alias(e, alias))
        }
        DomainExpression::PredicateRewrite { args, .. } => args
            .iter()
            .any(|a| expr_subqueries_reference_alias(a, alias)),
        _ => false,
    }
}

/// Check if a query expression tree contains any column reference to the given alias.
fn query_references_alias(query: &QueryExpression, alias: &str) -> bool {
    match query {
        QueryExpression::Select(stmt) => select_references_alias(stmt, alias),
        QueryExpression::SetOperation { left, right, .. } => {
            query_references_alias(left, alias) || query_references_alias(right, alias)
        }
        QueryExpression::WithCte { ctes, query } => {
            ctes.iter()
                .any(|cte| query_references_alias(cte.query(), alias))
                || query_references_alias(query, alias)
        }
        _ => false,
    }
}

fn select_references_alias(stmt: &SelectStatement, alias: &str) -> bool {
    // Check SELECT list
    for item in stmt.select_list() {
        if let SelectItem::Expression { expr, .. } = item {
            if expr_references_alias(expr, alias) {
                return true;
            }
        }
    }
    // Check FROM (subqueries within FROM)
    if let Some(from) = stmt.from() {
        for table in from {
            if table_references_alias(table, alias) {
                return true;
            }
        }
    }
    // Check WHERE
    if let Some(w) = stmt.where_clause() {
        if expr_references_alias(w, alias) {
            return true;
        }
    }
    // Check HAVING
    if let Some(h) = stmt.having() {
        if expr_references_alias(h, alias) {
            return true;
        }
    }
    false
}

fn table_references_alias(table: &TableExpression, alias: &str) -> bool {
    match table {
        TableExpression::Subquery { query, .. } => {
            query_references_alias(&(**query).clone().into_inner(), alias)
        }
        TableExpression::Join {
            left,
            right,
            join_condition,
            ..
        } => {
            table_references_alias(left, alias)
                || table_references_alias(right, alias)
                || match join_condition {
                    crate::pipeline::sql_ast_v3::JoinCondition::On(expr) => {
                        expr_references_alias(expr, alias)
                    }
                    _ => false,
                }
        }
        TableExpression::UnionTable { selects, .. } => {
            selects.iter().any(|q| query_references_alias(q, alias))
        }
        _ => false,
    }
}

/// Check if a domain expression contains any column reference qualified with the given alias.
fn expr_references_alias(expr: &DomainExpression, alias: &str) -> bool {
    match expr {
        DomainExpression::Column { qualifier, .. } => match qualifier {
            Some(q) => match q.parts() {
                QualifierParts::Table(t) => t == alias,
                _ => false,
            },
            None => false,
        },
        DomainExpression::Binary { left, right, .. } => {
            expr_references_alias(left, alias) || expr_references_alias(right, alias)
        }
        DomainExpression::Unary { expr, .. } => expr_references_alias(expr, alias),
        DomainExpression::Cast { expr, .. } => expr_references_alias(expr, alias),
        DomainExpression::Parens(inner) => expr_references_alias(inner, alias),
        DomainExpression::Function { args, .. } => {
            args.iter().any(|a| expr_references_alias(a, alias))
        }
        DomainExpression::InList { expr, values, .. } => {
            expr_references_alias(expr, alias)
                || values.iter().any(|v| expr_references_alias(v, alias))
        }
        DomainExpression::Exists { query, .. } => query_references_alias(query, alias),
        DomainExpression::InSubquery { expr, query, .. } => {
            expr_references_alias(expr, alias) || query_references_alias(query, alias)
        }
        DomainExpression::Subquery(query) => query_references_alias(query, alias),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_ref()
                .map_or(false, |e| expr_references_alias(e, alias))
                || when_clauses.iter().any(|wc| {
                    expr_references_alias(wc.when(), alias)
                        || expr_references_alias(wc.then(), alias)
                })
                || else_clause
                    .as_ref()
                    .map_or(false, |e| expr_references_alias(e, alias))
        }
        DomainExpression::PredicateRewrite { args, .. } => {
            args.iter().any(|a| expr_references_alias(a, alias))
        }
        DomainExpression::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(|a| expr_references_alias(a, alias))
                || partition_by.iter().any(|a| expr_references_alias(a, alias))
                || order_by
                    .iter()
                    .any(|(a, _)| expr_references_alias(a, alias))
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Inner column map: the substitution table
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct ColDef {
    expr: DomainExpression,
}

/// Build a map from output column name → inner expression.
/// Returns empty map for SELECT * (we can't resolve individual names).
fn build_inner_column_map(select_list: &[SelectItem]) -> std::collections::HashMap<String, ColDef> {
    let mut map = std::collections::HashMap::new();
    for item in select_list {
        match item {
            SelectItem::Expression { expr, alias } => {
                let output_name = match alias {
                    Some(a) => a.clone(),
                    None => match expr {
                        DomainExpression::Column { name, .. } => name.clone(),
                        _ => continue,
                    },
                };
                map.insert(output_name, ColDef { expr: expr.clone() });
            }
            SelectItem::Star | SelectItem::QualifiedStar { .. } => {
                return std::collections::HashMap::new();
            }
        }
    }
    map
}

// ---------------------------------------------------------------------------
// Expression rewriting: substitute column references through the inner map
// ---------------------------------------------------------------------------

/// Rewrite a SELECT list by substituting each column reference through
/// the inner column map. Injects explicit aliases when substitution would
/// change the output column name.
fn rewrite_select_list(
    outer_list: &[SelectItem],
    subquery_alias: &str,
    col_map: &std::collections::HashMap<String, ColDef>,
) -> Option<Vec<SelectItem>> {
    let mut result = Vec::with_capacity(outer_list.len());
    for item in outer_list {
        match item {
            SelectItem::Star | SelectItem::QualifiedStar { .. } => {
                return None;
            }
            SelectItem::Expression { expr, alias } => {
                let rewritten = rewrite_expr(expr, subquery_alias, col_map)?;

                // Determine the correct output alias.
                // If the outer already has an explicit alias, keep it.
                // Otherwise, the output name is the column reference name.
                // After substitution the "natural name" of the expression
                // may differ — inject an alias to preserve the original name.
                let final_alias = if alias.is_some() {
                    alias.clone()
                } else {
                    let original_name = expr_natural_name(expr);
                    let rewritten_name = expr_natural_name(&rewritten);
                    if original_name != rewritten_name {
                        original_name
                    } else {
                        None
                    }
                };

                result.push(SelectItem::Expression {
                    expr: rewritten,
                    alias: final_alias,
                });
            }
        }
    }
    Some(result)
}

/// Get the "natural name" of an expression — the column name it would
/// produce if used without an explicit alias.
fn expr_natural_name(expr: &DomainExpression) -> Option<String> {
    match expr {
        DomainExpression::Column { name, .. } => Some(name.clone()),
        _ => None,
    }
}

/// Rewrite a domain expression, substituting column references that point
/// at the subquery alias with their definitions from the inner column map.
///
/// Returns None if we encounter a reference we can't resolve.
fn rewrite_expr(
    expr: &DomainExpression,
    subquery_alias: &str,
    col_map: &std::collections::HashMap<String, ColDef>,
) -> Option<DomainExpression> {
    match expr {
        DomainExpression::Column { name, qualifier } => {
            let references_subquery = match qualifier {
                Some(q) => match q.parts() {
                    QualifierParts::Table(t) => t == subquery_alias,
                    _ => false,
                },
                None => col_map.contains_key(name),
            };

            if references_subquery {
                if col_map.is_empty() {
                    // Inner is SELECT * — strip qualifier, names pass through
                    Some(DomainExpression::Column {
                        name: name.clone(),
                        qualifier: None,
                    })
                } else if let Some(def) = col_map.get(name) {
                    // A substituted definition lands in an arbitrary
                    // syntactic context; a bare operator expression must
                    // keep its grouping (`age := x + 1` inside `age * 2`
                    // is `(x + 1) * 2`, never `x + 1 * 2`). Self-
                    // delimiting forms (columns, literals, calls, CASE)
                    // need no wrap.
                    Some(match &def.expr {
                        e @ (DomainExpression::Binary { .. }
                        | DomainExpression::Unary { .. }) => {
                            DomainExpression::Parens(Box::new(e.clone()))
                        }
                        e => e.clone(),
                    })
                } else {
                    None
                }
            } else {
                Some(expr.clone())
            }
        }

        DomainExpression::Binary { left, op, right } => {
            let l = rewrite_expr(left, subquery_alias, col_map)?;
            let r = rewrite_expr(right, subquery_alias, col_map)?;
            Some(DomainExpression::Binary {
                left: Box::new(l),
                op: op.clone(),
                right: Box::new(r),
            })
        }

        DomainExpression::Unary { op, expr: inner } => {
            let rewritten = rewrite_expr(inner, subquery_alias, col_map)?;
            Some(DomainExpression::Unary {
                op: op.clone(),
                expr: Box::new(rewritten),
            })
        }

        DomainExpression::Cast {
            expr: inner,
            type_name,
        } => {
            let rewritten = rewrite_expr(inner, subquery_alias, col_map)?;
            Some(DomainExpression::Cast {
                expr: Box::new(rewritten),
                type_name: type_name.clone(),
            })
        }

        DomainExpression::Function {
            name,
            args,
            distinct,
        } => {
            let new_args: Option<Vec<_>> = args
                .iter()
                .map(|a| rewrite_expr(a, subquery_alias, col_map))
                .collect();
            Some(DomainExpression::Function {
                name: name.clone(),
                args: new_args?,
                distinct: *distinct,
            })
        }

        DomainExpression::Parens(inner) => {
            let rewritten = rewrite_expr(inner, subquery_alias, col_map)?;
            Some(DomainExpression::Parens(Box::new(rewritten)))
        }

        DomainExpression::InList {
            expr: inner,
            not,
            values,
        } => {
            let rewritten_expr = rewrite_expr(inner, subquery_alias, col_map)?;
            let rewritten_values: Option<Vec<_>> = values
                .iter()
                .map(|v| rewrite_expr(v, subquery_alias, col_map))
                .collect();
            Some(DomainExpression::InList {
                expr: Box::new(rewritten_expr),
                not: *not,
                values: rewritten_values?,
            })
        }

        DomainExpression::Case {
            expr: case_expr,
            when_clauses,
            else_clause,
        } => {
            let new_case_expr = match case_expr {
                Some(e) => Some(Box::new(rewrite_expr(e, subquery_alias, col_map)?)),
                None => None,
            };
            let new_whens: Option<Vec<_>> = when_clauses
                .iter()
                .map(|wc| {
                    let w = rewrite_expr(wc.when(), subquery_alias, col_map)?;
                    let t = rewrite_expr(wc.then(), subquery_alias, col_map)?;
                    Some(crate::pipeline::sql_ast_v3::WhenClause::new(w, t))
                })
                .collect();
            let new_else = match else_clause {
                Some(e) => Some(Box::new(rewrite_expr(e, subquery_alias, col_map)?)),
                None => None,
            };
            Some(DomainExpression::Case {
                expr: new_case_expr,
                when_clauses: new_whens?,
                else_clause: new_else,
            })
        }

        // Subqueries — the subquery body is a separate scope (don't rewrite
        // inside it), but the expressions OUTSIDE the subquery (e.g. the LHS
        // of IN) still need rewriting.
        DomainExpression::Exists { .. } => Some(expr.clone()),
        DomainExpression::Subquery(_) => Some(expr.clone()),
        DomainExpression::InSubquery {
            expr: inner_expr,
            not,
            query,
        } => {
            let rewritten_expr = rewrite_expr(inner_expr, subquery_alias, col_map)?;
            Some(DomainExpression::InSubquery {
                expr: Box::new(rewritten_expr),
                not: *not,
                query: query.clone(),
            })
        }

        // Leaf expressions
        DomainExpression::Literal(_) | DomainExpression::Star | DomainExpression::RawSql(_) => {
            Some(expr.clone())
        }

        DomainExpression::WindowFunction {
            name,
            args,
            partition_by,
            order_by,
            frame,
        } => {
            let new_args: Option<Vec<_>> = args
                .iter()
                .map(|a| rewrite_expr(a, subquery_alias, col_map))
                .collect();
            let new_partition: Option<Vec<_>> = partition_by
                .iter()
                .map(|a| rewrite_expr(a, subquery_alias, col_map))
                .collect();
            let new_order: Option<Vec<_>> = order_by
                .iter()
                .map(|(expr, dir)| {
                    rewrite_expr(expr, subquery_alias, col_map).map(|e| (e, dir.clone()))
                })
                .collect();
            Some(DomainExpression::WindowFunction {
                name: name.clone(),
                args: new_args?,
                partition_by: new_partition?,
                order_by: new_order?,
                frame: frame.clone(),
            })
        }

        DomainExpression::PredicateRewrite {
            name,
            args,
            negated,
        } => {
            let new_args: Option<Vec<_>> = args
                .iter()
                .map(|a| rewrite_expr(a, subquery_alias, col_map))
                .collect();
            Some(DomainExpression::PredicateRewrite {
                name: name.clone(),
                args: new_args?,
                negated: *negated,
            })
        }

        DomainExpression::Tuple(elements) => {
            let new_elements: Option<Vec<_>> = elements
                .iter()
                .map(|e| rewrite_expr(e, subquery_alias, col_map))
                .collect();
            Some(DomainExpression::Tuple(new_elements?))
        }
    }
}
