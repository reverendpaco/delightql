// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// recursive_cte.rs — recursive-CTE legalizations (final-word pass).
//
// Whether a CTE is recursive is NOT decided here. The resolver decides it
// where the self-reference binds and stores the decision on the binding;
// this pass reads the BODY it was built into — a fixpoint body is a
// fixpoint, not a query with a flag beside it. A second structural
// detector over the SQL AST is a second opinion, and two opinions about
// one fact are free to differ.
//
// `legalize_recursive_limits` — `#<N` inside a recursive rule.
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

use crate::diagnostic::Recursion;
use crate::error::{DelightQLError, Result};
use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::{
    walk, Cte, DomainExpression, QueryExpression, SelectItem, SelectStatement, SqlStatement,
    TableExpression,
};

/// Count references to `name` in this query. Works on a clone so the real
/// tree is untouched.
fn scoped_reference_count(query: &QueryExpression, scope: crate::names::ScopeId) -> usize {
    let mut clone = query.clone();

    struct Counter {
        scope: crate::names::ScopeId,
        count: usize,
    }
    impl walk::SqlVisitorMut for Counter {
        fn table(&mut self, t: &mut TableExpression) {
            if matches!(t, TableExpression::Scope(found) if *found == self.scope) {
                self.count += 1;
            }
        }
    }
    let mut c = Counter { scope, count: 0 };
    walk::visit_query(&mut clone, &mut c);
    c.count
}

// ---------------------------------------------------------------------------
// LIMIT-in-recursive-member legalization
// ---------------------------------------------------------------------------

/// Can this dialect spell a total-row cap natively (trailing LIMIT on the
/// recursive member, applying to the whole compound)?
fn allows_recursive_limit(dialect: SqlDialect) -> bool {
    dialect.spells_recursive_total_cap()
}

/// Legalize `#<N` bounds inside recursive members. It reaches only the
/// bindings the resolver's decision built as fixpoints.
pub fn legalize_recursive_limits(stmt: &mut SqlStatement, dialect: SqlDialect) -> Result<()> {
    if let Some(ctes) = statement_with_clause_mut(stmt) {
        for cte in ctes.iter_mut() {
            legalize_cte(cte, dialect)?;
        }
    }
    Ok(())
}

fn legalize_cte(cte: &mut Cte, dialect: SqlDialect) -> Result<()> {
    if !cte.is_recursive() {
        return Ok(());
    }
    let scope = cte.scope();
    // SQLite's native total cap belongs to the final recursive compound
    // member. Multiple member-local caps, or a cap on a nonfinal member,
    // cannot be represented by moving one LIMIT to the compound tail: that
    // would change which member's rows the cap governs. Keep this unresolved
    // multi-member policy typed until its owner chooses a realization.
    if dialect == SqlDialect::SQLite {
        let member_caps: Vec<bool> = cte
            .body()
            .parts()
            .into_iter()
            .skip(1)
            .map(|part| recursive_part_has_limit(part, scope))
            .collect();
        let cap_count = member_caps.iter().filter(|has_cap| **has_cap).count();
        let nonfinal_cap = member_caps
            .iter()
            .take(member_caps.len().saturating_sub(1))
            .any(|has_cap| *has_cap);
        if cap_count > 1 || nonfinal_cap {
            return Err(multi_member_limit_error());
        }
    }
    // A FIXPOINT'S PARTS ARE ITS BRANCHES. The anchor and each member come
    // out of the body as themselves; the descent inside one is for an arm's
    // OWN union, not for rediscovering the accumulation.
    for part in cte.parts_mut() {
        legalize_branches(part, scope, dialect)?;
    }
    Ok(())
}

#[stacksafe::stacksafe]
fn recursive_part_has_limit(query: &QueryExpression, cte_scope: crate::names::ScopeId) -> bool {
    match query {
        QueryExpression::Select(select) => {
            select.limit().is_some() && scoped_reference_count(query, cte_scope) > 0
        }
        QueryExpression::SetOperation { left, right, .. } => {
            recursive_part_has_limit(left, cte_scope) || recursive_part_has_limit(right, cte_scope)
        }
    }
}

/// Walk the set-op tree of a recursive CTE body; legalize each SELECT
/// branch that references the CTE (= each recursive member).
#[stacksafe::stacksafe]
fn legalize_branches(
    q: &mut QueryExpression,
    cte_scope: crate::names::ScopeId,
    dialect: SqlDialect,
) -> Result<()> {
    match q {
        QueryExpression::SetOperation { left, right, .. } => {
            legalize_branches(left, cte_scope, dialect)?;
            legalize_branches(right, cte_scope, dialect)
        }
        QueryExpression::Select(_) => legalize_member(q, cte_scope, dialect),
    }
}

fn legalize_member(
    q: &mut QueryExpression,
    cte_scope: crate::names::ScopeId,
    dialect: SqlDialect,
) -> Result<()> {
    // Member classification is shadowing-aware: a reference under a nested
    // WITH that redefines the name belongs to the inner CTE.
    if scoped_reference_count(q, cte_scope) == 0 {
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
        return Err(limit_bound_error(dialect));
    }

    // The transformer's buried form: FROM (SELECT … FROM <cte> … LIMIT n) AS w
    let Some(wrapper) = find_limit_wrapper(select, cte_scope) else {
        return Ok(());
    };
    if !allows_recursive_limit(dialect) {
        return Err(limit_bound_error(dialect));
    }
    let inlined = inline_limit_wrapper(select, &wrapper)?;
    *q = QueryExpression::Select(Box::new(inlined));
    Ok(())
}

/// The wrapper's alias, if this member has the buried-limit shape.
#[stacksafe::stacksafe]
fn find_limit_wrapper(
    select: &SelectStatement,
    cte_scope: crate::names::ScopeId,
) -> Option<crate::names::ScopeId> {
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
    if inner.limit().is_some() && select_from_references(inner, cte_scope) {
        Some(*alias)
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
fn inline_limit_wrapper(
    outer: &SelectStatement,
    _wrapper_scope: &crate::names::ScopeId,
) -> Result<SelectStatement> {
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
        return Err(shape_error(
            "recursive member has clauses outside the limit wrapper",
        ));
    }
    if inner.group_by().is_some() || inner.having().is_some() || inner.is_distinct() {
        return Err(shape_error("limit wrapper carries aggregation"));
    }

    // Map: wrapper column name -> inner expression.
    let mut map = std::collections::HashMap::new();
    for item in inner.select_list() {
        let SelectItem::Publishing {
            expr,
            slot: alias,
            printed: true,
        } = item
        else {
            return Err(shape_error("limit wrapper has unaliased or star items"));
        };
        map.insert(*alias, expr.clone());
    }

    // Rebuild the outer select list with wrapper references substituted.
    let mut items = Vec::with_capacity(outer.select_list().len());
    for item in outer.select_list() {
        let SelectItem::Publishing {
            expr,
            slot,
            printed,
        } = item
        else {
            return Err(shape_error("recursive member selects a star"));
        };
        let (alias, printed) = (slot, *printed);
        let new_expr = match expr {
            DomainExpression::Column(column) if map.contains_key(column) => map
                .get(column)
                .cloned()
                .ok_or_else(|| shape_error("wrapper reference names an unknown column"))?,
            DomainExpression::Literal(_) | DomainExpression::Column(_) => expr.clone(),
            _ => return Err(shape_error("recursive member computes over the wrapper")),
        };
        items.push(if printed {
            SelectItem::expression_with_alias(new_expr, *alias)
        } else {
            SelectItem::Publishing {
                expr: new_expr,
                slot: *alias,
                printed: false,
            }
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
        builder = builder.limit_from(lim.clone());
    }
    builder.restructuring(inner.at(), outer).map_err(|e| {
        DelightQLError::from(Recursion::LimitBound {
            message: format!("recursive limit legalization rebuild: {}", e),
        })
    })
}

/// Does this SELECT's FROM tree (including nested subqueries) reference
/// the table `name`?
fn select_from_references(select: &SelectStatement, scope: crate::names::ScopeId) -> bool {
    select
        .from()
        .is_some_and(|from| from.iter().any(|t| table_references(t, scope)))
}

#[stacksafe::stacksafe]
fn table_references(table: &TableExpression, scope: crate::names::ScopeId) -> bool {
    match table {
        TableExpression::Scope(found) => {
            *found == scope
        }
        TableExpression::Entity { .. } => false,
        TableExpression::Subquery { query, .. } => query_references(query, scope),
        TableExpression::Join { left, right, .. } => {
            table_references(left, scope) || table_references(right, scope)
        }
        TableExpression::TVF { .. } => false,
    }
}

#[stacksafe::stacksafe]
fn query_references(query: &QueryExpression, scope: crate::names::ScopeId) -> bool {
    match query {
        QueryExpression::Select(s) => select_from_references(s, scope),
        QueryExpression::SetOperation { left, right, .. } => {
            query_references(left, scope) || query_references(right, scope)
        }
    }
}

fn statement_with_clause_mut(stmt: &mut SqlStatement) -> Option<&mut Vec<Cte>> {
    match stmt {
        SqlStatement::Query { with_clause, .. }
        | SqlStatement::CreateTempTable { with_clause, .. }
        | SqlStatement::Delete { with_clause, .. }
        | SqlStatement::Update { with_clause, .. }
        | SqlStatement::Insert { with_clause, .. } => with_clause.as_mut(),
        SqlStatement::DropTempTable { .. } => None,
    }
}

fn limit_bound_error(dialect: SqlDialect) -> DelightQLError {
    DelightQLError::from(Recursion::LimitBound {
        message: format!(
            "this recursive rule bounds its recursion with a row limit (#<N). DelightQL \
             defines this as a total-row cap on the fixpoint, but {dialect:?} has \
             no single-statement spelling for it — this target only supports \
             filter-based bounds. Rewrite the bound as a filter condition on the \
             recursive rule (e.g. a depth or value predicate)."
        ),
    })
}

fn multi_member_limit_error() -> DelightQLError {
    DelightQLError::from(Recursion::LimitBound {
        message: "SQLite can place a recursive total-row cap only on the final recursive member; multiple or nonfinal member caps have no settled single-statement realization. Rewrite the rule with filter-based bounds until the multi-member cap law is decided"
            .to_string(),
    })
}

fn shape_error(detail: &str) -> DelightQLError {
    DelightQLError::from(Recursion::LimitBound {
        message: format!(
            "cannot legalize the row limit inside this recursive rule ({detail}). \
             Rewrite the bound as a filter condition on the recursive rule."
        ),
    })
}

// ---------------------------------------------------------------------------
// The recursion validator (N1/N3/N4)
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
pub fn validate_recursive_members(
    stmt: &mut SqlStatement,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<()> {
    if let Some(ctes) = statement_with_clause_mut(stmt) {
        for cte in ctes.iter_mut() {
            validate_cte(cte, target)?;
        }
    }
    Ok(())
}

fn validate_cte(
    cte: &mut Cte,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<()> {
    if !cte.is_recursive() {
        return Ok(());
    }
    let scope = cte.scope();
    for part in cte.parts_mut() {
        unwrap_transparent_members(part, scope);
        inline_ordered_stages(part, scope);
        inline_aliased_self_references(part, scope);
        validate_branches(part, scope, target)?;
    }
    Ok(())
}

/// AN ORDERED STAGE OVER THE FRONTIER (ORDERING IS ADMITTED): a member that
/// reads its frontier through derived tables which only order it —
/// `SELECT … FROM (SELECT … FROM <cte> … ORDER BY …) AS w WHERE …`, or a chain
/// of such stages — reads the frontier with each ordering lowered at its own
/// stage. An engine resolves a recursive reference only as a direct source, so
/// the chain collapses innermost first: each stage is inlined into the stage
/// that reads it, its outputs becoming the inner expressions and its FROM and
/// condition standing under the reader's. A plain stage between two orderings
/// (a projection an ordered stage reads) inlines the same way.
///
/// No target spells a stage's ordering there. SQLite reads an ORDER BY on
/// a recursive select as the queue's priority over the rows the member
/// produces, not as an order of the frontier rows it reads, and PostgreSQL
/// admits none in a recursive term. The inlined stages therefore keep no
/// ordering: no row the fixpoint accumulates depends on it, and no
/// preservation is promised. A derived table that does more than order (a
/// bound, a grouping, DISTINCT), or a member that orders, groups or bounds
/// itself, is left as it is; so is a window a stage computes beneath its
/// reader's condition, which inlining would move under that condition.
#[stacksafe::stacksafe]
fn inline_ordered_stages(q: &mut QueryExpression, cte_scope: crate::names::ScopeId) {
    match q {
        QueryExpression::SetOperation { left, right, .. } => {
            inline_ordered_stages(left, cte_scope);
            inline_ordered_stages(right, cte_scope);
        }
        QueryExpression::Select(_) => collapse_stages(q, cte_scope, true),
    }
}

/// Collapse the stages beneath `q`, innermost first, then inline the stage
/// `q` reads when it may be. `member` marks the recursive member itself.
#[stacksafe::stacksafe]
fn collapse_stages(q: &mut QueryExpression, cte_scope: crate::names::ScopeId, member: bool) {
    let QueryExpression::Select(select) = q else {
        return;
    };
    if let Some([TableExpression::Subquery { query, .. }]) = select.from_mut() {
        collapse_stages(&mut ***query, cte_scope, false);
    }
    inline_stage(q, cte_scope, member);
}

/// Inline the one derived table `q` reads, when it reads the frontier
/// directly and either it orders or `q` is an ordered stage reading it.
fn inline_stage(q: &mut QueryExpression, cte_scope: crate::names::ScopeId, member: bool) {
    let QueryExpression::Select(select) = q else {
        return;
    };
    if select.group_by().is_some()
        || select.having().is_some()
        || select.is_distinct()
        || select.limit().is_some()
        || (member && select.order_by().is_some())
    {
        return;
    }
    let Some([TableExpression::Subquery { query, .. }]) = select.from() else {
        return;
    };
    let QueryExpression::Select(inner) = &(**query).clone().into_inner() else {
        return;
    };
    let inner = inner.clone();
    if inner.limit().is_some()
        || inner.group_by().is_some()
        || inner.having().is_some()
        || inner.is_distinct()
        || (inner.order_by().is_none() && select.order_by().is_none())
    {
        return;
    }
    let direct: usize = inner
        .from()
        .map(|from| from.iter().map(|t| count_direct(t, cte_scope)).sum())
        .unwrap_or(0);
    if direct != 1 {
        return;
    }
    let mut map = std::collections::HashMap::new();
    for item in inner.select_list() {
        let SelectItem::Publishing {
            expr,
            slot,
            printed: true,
        } = item
        else {
            return;
        };
        map.insert(*slot, expr.clone());
    }
    if select.where_clause().is_some() && holds_window(&inner) {
        return;
    }
    let mut builder = SelectStatement::builder()
        .select_all(select.select_list().to_vec())
        .from_tables(inner.from().unwrap_or(&[]).to_vec());
    let condition: Vec<DomainExpression> =
        inner.where_clause().into_iter().chain(select.where_clause()).cloned().collect();
    if !condition.is_empty() {
        builder = builder.where_clause(DomainExpression::and(condition));
    }
    for term in select.order_by().into_iter().flatten() {
        builder = builder.order_by(term.clone());
    }
    let Ok(merged) = builder.restructuring(select.at(), select) else {
        return;
    };
    struct Substitute {
        map: std::collections::HashMap<crate::names::ColId, DomainExpression>,
    }
    impl walk::SqlVisitorMut for Substitute {
        fn expr(&mut self, e: &mut DomainExpression) {
            if let DomainExpression::Column(column) = e {
                if let Some(inner) = self.map.get(column) {
                    *e = inner.clone();
                }
            }
        }
    }
    let mut merged = QueryExpression::Select(Box::new(merged));
    walk::visit_query(&mut merged, &mut Substitute { map });
    *q = merged;
}

/// Whether a statement computes a window anywhere in it.
fn holds_window(select: &SelectStatement) -> bool {
    struct Find(bool);
    impl walk::SqlVisitorMut for Find {
        fn expr(&mut self, e: &mut DomainExpression) {
            if matches!(e, DomainExpression::WindowFunction { .. }) {
                self.0 = true;
            }
        }
    }
    let mut probe = QueryExpression::Select(Box::new(select.clone()));
    let mut find = Find(false);
    walk::visit_query(&mut probe, &mut find);
    find.0
}

/// A self-reference wearing a relation alias — `FROM (SELECT c.a AS a, c.b AS b
/// FROM c) AS m` — is `FROM c` under another name. No `TableExpression` spells
/// a CTE under an alias, so a reference that needs its own qualifier reaches
/// for a derived table; inside a recursive member that derived table buries the
/// self-reference where no engine will resolve it.
///
/// Removing the boundary and re-anchoring the member's references to the CTE's
/// own occurrences are ONE act — the alias's columns stop existing the moment
/// the wrapper does, so a pass that dropped the wrapper alone would leave every
/// `m.a` naming nothing. Only a lone wrapper is inlined: two would collide on
/// one name, and a member that references itself twice is refused regardless.
#[stacksafe::stacksafe]
fn inline_aliased_self_references(q: &mut QueryExpression, cte_scope: crate::names::ScopeId) {
    match q {
        QueryExpression::SetOperation { left, right, .. } => {
            inline_aliased_self_references(left, cte_scope);
            inline_aliased_self_references(right, cte_scope);
        }
        QueryExpression::Select(select) => {
            let mut wrappers = Vec::new();
            for entry in select.from_mut().into_iter().flatten() {
                each_from_entry(entry, &mut |entry| {
                    if let Some(pairs) = aliased_self_reference(entry, cte_scope) {
                        wrappers.push(pairs);
                    }
                });
            }
            let [pairs] = wrappers.as_slice() else { return };
            let pairs: Vec<_> = pairs.clone();

            for entry in select.from_mut().into_iter().flatten() {
                each_from_entry(entry, &mut |entry| {
                    if aliased_self_reference(entry, cte_scope).is_some() {
                        *entry = TableExpression::Scope(cte_scope);
                    }
                });
            }

            struct Reanchor {
                pairs: Vec<(crate::names::ColId, crate::names::ColId)>,
            }
            impl walk::SqlVisitorMut for Reanchor {
                fn expr(&mut self, e: &mut DomainExpression) {
                    if let DomainExpression::Column(column) = e {
                        if let Some((_, source)) =
                            self.pairs.iter().find(|(output, _)| output == column)
                        {
                            *column = *source;
                        }
                    }
                }
            }
            let mut reanchor = Reanchor { pairs };
            walk::visit_query(q, &mut reanchor);
        }
    }
}

/// Every leaf of a FROM entry's join tree — the entries a member actually
/// stands on, as opposed to anything nested inside a subquery beneath them.
fn each_from_entry(entry: &mut TableExpression, f: &mut impl FnMut(&mut TableExpression)) {
    match entry {
        TableExpression::Join { left, right, .. } => {
            each_from_entry(left, f);
            each_from_entry(right, f);
        }
        other => f(other),
    }
}

/// `(SELECT c.a AS a, c.b AS b FROM c) AS m` — nothing but a rename of `c`.
/// Returns each output paired with the CTE occurrence it stands for.
#[stacksafe::stacksafe]
fn aliased_self_reference(
    entry: &TableExpression,
    cte_scope: crate::names::ScopeId,
) -> Option<Vec<(crate::names::ColId, crate::names::ColId)>> {
    let TableExpression::Subquery { query, .. } = entry else {
        return None;
    };
    let inner = (**query).clone().into_inner();
    let QueryExpression::Select(select) = &inner else {
        return None;
    };
    if select.where_clause().is_some()
        || select.group_by().is_some()
        || select.having().is_some()
        || select.is_distinct()
        || select.limit().is_some()
        || select.order_by().is_some()
    {
        return None;
    }
    let [TableExpression::Scope(found)] = select.from()? else {
        return None;
    };
    if *found != cte_scope {
        return None;
    }
    select
        .select_list()
        .iter()
        .map(|item| match item {
            SelectItem::Publishing {
                expr: DomainExpression::Column(source),
                slot: output,
                printed: true,
            } => Some((*output, *source)),
            _ => None,
        })
        .collect()
}

/// A member of the shape `SELECT t.b AS x, t.a AS y FROM (inner) AS t` —
/// a reprojection that reads each of the inner's own outputs exactly once,
/// in any order, and nothing else — is the inner member wearing a wrapper.
/// The effect road's assembly also renames those outputs to the recursive
/// CTE heading. Removing the wrapper therefore rebuilds the inner body at the
/// outer result scope, in the wrapper's order: the inner expressions keep
/// their source identities while the outer aliases remain owned by the scope
/// that publishes them. Judging the uncollapsed shape as N4 burial would
/// refuse legal linear recursion.
///
/// Correspondence is by column identity. Pairing by position misreads, as
/// burial, a wrapper that restores the heading's order over an inner that
/// publishes in another order; pairing by width alone admits a wrapper that
/// repeats one output and drops another, which changes the rows.
#[stacksafe::stacksafe]
fn unwrap_transparent_members(q: &mut QueryExpression, cte_scope: crate::names::ScopeId) {
    match q {
        QueryExpression::SetOperation { left, right, .. } => {
            unwrap_transparent_members(left, cte_scope);
            unwrap_transparent_members(right, cte_scope);
        }
        QueryExpression::Select(select) => {
            let Some(from) = select.from() else { return };
            let [TableExpression::Subquery { query: inner, .. }] = from else {
                return;
            };
            // An outer LIMIT is what `#<N` inside a recursive rule lowers
            // to: a TOTAL-ROW CAP on the fixpoint (ratified). It rides down
            // onto the member tail, which is
            // SQLite's own spelling for it. Bailing on it left the
            // self-reference buried in the wrapper and the member read as N4
            // burial, so the cap refused the very shape it exists to express.
            let hoisted_limit = select.limit().cloned();
            if select.where_clause().is_some()
                || select.group_by().is_some()
                || select.having().is_some()
                || select.is_distinct()
                || select.order_by().is_some()
            {
                return;
            }
            let QueryExpression::Select(inner_select) = &(**inner).clone().into_inner() else {
                return;
            };
            if inner_select.is_distinct()
                || inner_select.group_by().is_some()
                || inner_select.having().is_some()
                || inner_select.limit().is_some()
                || inner_select.order_by().is_some()
            {
                return;
            }
            // Only unwrap when the wrapper actually hides a self-reference.
            if scoped_reference_count(&QueryExpression::Select(inner_select.clone()), cte_scope)
                == 0
            {
                return;
            }
            // Outer must read the complete inner select list, each output
            // once. Its aliases are the heading the wrapper exists to restore.
            let inner_columns: Vec<crate::names::ColId> = inner_select
                .select_list()
                .iter()
                .filter_map(|item| match item {
                    SelectItem::Publishing {
                        slot: a,
                        printed: true,
                        ..
                    } => Some(*a),
                    _ => None,
                })
                .collect();
            if inner_columns.len() != inner_select.select_list().len()
                || select.select_list().len() != inner_columns.len()
            {
                return;
            }
            // Per outer item, in the outer's order: the inner position it
            // reads, its alias, and whether the alias is printed. Equal widths
            // with no position read twice make the reads a permutation.
            let mut read = vec![false; inner_columns.len()];
            let mut outer_heading = Vec::with_capacity(inner_columns.len());
            for item in select.select_list() {
                let SelectItem::Publishing {
                    expr: DomainExpression::Column(column),
                    slot,
                    printed,
                } = item
                else {
                    return;
                };
                let Some(position) = inner_columns.iter().position(|want| want == column) else {
                    return;
                };
                if std::mem::replace(&mut read[position], true) {
                    return;
                }
                outer_heading.push((position, *slot, *printed));
            }
            let unchanged = outer_heading.iter().enumerate().all(|(index, read)| {
                let (position, output, _) = *read;
                position == index && output == inner_columns[index]
            });
            if unchanged {
                let mut inner = inner_select.clone();
                if let Some(limit) = hoisted_limit {
                    inner.set_limit(limit);
                }
                *q = QueryExpression::Select(inner);
                return;
            }
            if outer_heading.iter().any(|(_, _, explicit)| !explicit) {
                return;
            }
            let projected = outer_heading
                .iter()
                .map(|(position, output, _)| {
                    let Some(expr) = inner_select.select_list()[*position].expr() else {
                        unreachable!("inner_columns covers the complete select list")
                    };
                    SelectItem::expression_with_alias(expr.clone(), *output)
                })
                .collect();
            let mut builder = SelectStatement::builder().select_all(projected);
            if let Some(from) = inner_select.from() {
                builder = builder.from_tables(from.to_vec());
            }
            if let Some(predicate) = inner_select.where_clause() {
                builder = builder.where_clause(predicate.clone());
            }
            if let Some(limit) = hoisted_limit {
                builder = builder.limit_from(limit);
            }
            // The member stands at the CTE. Its outputs ARE the CTE's
            // columns — that is what makes it a member — so a statement
            // left standing at the wrapper scope publishes columns that
            // do not belong to it, and the generator says so. `rebuilding`
            // keeps the wrapper's proven fact, which is the fact that
            // stops being true the moment the wrapper goes.
            let Ok(unwrapped) = builder.restructuring(cte_scope, select) else {
                return;
            };
            *q = QueryExpression::Select(Box::new(unwrapped));
        }
    }
}

#[stacksafe::stacksafe]
fn validate_branches(
    q: &mut QueryExpression,
    cte_scope: crate::names::ScopeId,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<()> {
    match q {
        QueryExpression::SetOperation { left, right, .. } => {
            validate_branches(left, cte_scope, target)?;
            validate_branches(right, cte_scope, target)
        }
        QueryExpression::Select(_) => validate_member(q, cte_scope, target),
    }
}

fn validate_member(
    q: &mut QueryExpression,
    cte_scope: crate::names::ScopeId,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> Result<()> {
    // Total references anywhere in the member (including expression
    // subqueries), via the total walker.
    let total = count_references(q, cte_scope);
    if total == 0 {
        return Ok(()); // base member
    }

    let QueryExpression::Select(select) = &*q else {
        return Ok(());
    };

    // Direct references: reachable through FROM join nesting only.
    let direct: usize = select
        .from()
        .map(|from| from.iter().map(|t| count_direct(t, cte_scope)).sum())
        .unwrap_or(0);

    // N1 — non-linear recursion: the frontier cannot join with itself.
    if direct > 1 {
        return Err(DelightQLError::from(Recursion::Nonlinear {
            message: format!(
                "the recursive rule references its frontier {direct} times — the \
                 frontier cannot join with itself. Carry the values you need as \
                 columns of one frontier row instead (tupling: fib-style \
                 `(a, b) -> (b, a+b)`). SEMANTICS/recursion-contract-law.md N1."
            ),
        }));
    }

    // N4 — self-reference buried in a subquery (semi/anti-join, IN, scalar,
    // or a derived table): the rule would need the accumulated set.
    if total > direct {
        log::debug!(
            "N4 firing: cte={cte_scope:?} total={total} direct={direct} member={:#?}",
            q
        );
        return Err(DelightQLError::from(Recursion::SelfSubquery {
            message: format!(
                "the recursive relation is referenced inside a subquery of its own recursive \
                 rule — a recursive rule sees only the previous iteration's rows, \
                 as a direct source. Track visited state in the frontier row, or \
                 deduplicate/filter after the fixpoint. SEMANTICS/recursion-contract-law.md N4."
            ),
        }));
    }

    // N3 — aggregation over the frontier.
    if member_has_aggregation(select, target) {
        return Err(DelightQLError::from(Recursion::Aggregate {
            message: format!(
                "aggregation inside the recursive rule would need \
                 the accumulated set. Aggregate after the fixpoint (a later pipe \
                 stage — strata are textual), or carry a running value in the \
                 frontier row. SEMANTICS/recursion-contract-law.md N3."
            ),
        }));
    }

    Ok(())
}

/// Count references to `name` anywhere under this query, shadowing-aware
/// (see `scoped_reference_count`).
fn count_references(q: &mut QueryExpression, scope: crate::names::ScopeId) -> usize {
    scoped_reference_count(q, scope)
}

/// Count references reachable through join nesting only — the positions a
/// recursive reference is allowed to occupy.
///
/// A reference wearing a RENAME WRAPPER (`(SELECT c.a AS a FROM c) AS m`)
/// occupies one of those positions too: no `TableExpression` spells a CTE
/// under an alias, so `c(*) as a` has nowhere else to go. Counting it as
/// buried made a frontier self-JOIN — two such wrappers, which
/// `inline_aliased_self_references` deliberately leaves alone — report N4
/// burial instead of the N1 non-linearity it actually is.
fn count_direct(table: &TableExpression, scope: crate::names::ScopeId) -> usize {
    match table {
        TableExpression::Scope(found) => usize::from(*found == scope),
        TableExpression::Join { left, right, .. } => {
            count_direct(left, scope) + count_direct(right, scope)
        }
        other => usize::from(aliased_self_reference(other, scope).is_some()),
    }
}

fn member_has_aggregation(
    select: &SelectStatement,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> bool {
    if select.group_by().is_some() || select.having().is_some() {
        return true;
    }
    select.select_list().iter().any(|item| {
        item.expr()
            .is_some_and(|expr| expr_has_aggregate(expr, target))
    })
}

fn expr_has_aggregate(
    expr: &DomainExpression,
    target: &crate::pipeline::aggregate_catalog::TargetAggregates,
) -> bool {
    let has = |expr: &DomainExpression| expr_has_aggregate(expr, target);
    match expr {
        // THE GRADE IS THE CALL'S, judged with its arity against what the
        // target reduces: `max(n + 1, 0)` computes per row of the member and
        // accumulates nothing.
        DomainExpression::Function { name, args, .. } => {
            name.user()
                .is_some_and(|name| target.reduces(name, args.len()))
                || args.iter().any(has)
        }
        DomainExpression::Binary { left, right, .. } => has(left) || has(right),
        DomainExpression::Unary { expr, .. } | DomainExpression::Parens(expr) => has(expr),
        DomainExpression::Cast { expr, .. } => has(expr),
        DomainExpression::Case {
            expr,
            when_clauses,
            else_clause,
        } => {
            expr.as_deref().is_some_and(has)
                || when_clauses
                    .iter()
                    .any(|wc| has(wc.when()) || has(wc.then()))
                || else_clause.as_deref().is_some_and(has)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::{Addressing, Registry};
    use crate::pipeline::asts::core::LiteralValue;
    use crate::pipeline::bindings::SqlFixpoint;
    use crate::pipeline::sql_ast::{CteBody, SelectItem};

    struct Fixture {
        x: crate::names::ScopeId,
        t: crate::names::ScopeId,
        a: crate::names::ScopeId,
        w: crate::names::ScopeId,
        xn: crate::names::ColId,
        tn: crate::names::ColId,
        an: crate::names::ColId,
        wn: crate::names::ColId,
        xm: crate::names::ColId,
        wm: crate::names::ColId,
    }

    impl Fixture {
        fn new() -> Self {
            let identities = Registry::new(&[]);
            let make_scope = |name: &str| {
                let spelling = identities.intern(name, false);
                let entity = identities.mint_entity(spelling);
                identities.resolved_access_scope(entity, spelling)
            };
            let x = make_scope("x");
            let t = make_scope("t");
            let a = make_scope("a");
            let w = make_scope("w");
            let make_column = |scope| {
                let spelling = identities.intern("n", false);
                identities.sql_column(scope, Some(spelling), Addressing::Published)
            };
            let xn = make_column(x);
            let tn = make_column(t);
            let an = make_column(a);
            let wn = make_column(w);
            let second_column = |scope| {
                let spelling = identities.intern("m", false);
                identities.sql_column(scope, Some(spelling), Addressing::Published)
            };
            let xm = second_column(x);
            let wm = second_column(w);
            Self {
                x,
                t,
                a,
                w,
                xn,
                tn,
                an,
                wn,
                xm,
                wm,
            }
        }

        /// A fixture's statements go through the same door production's do.
        fn publish(
            &self,
            at: crate::names::ScopeId,
            select: crate::pipeline::sql_ast::SelectBuilder,
        ) -> SelectStatement {
            (select)
                .standing_at(at)
                .map_err(|e| {
                    crate::diagnostic::Internal::invariant("sql_rewriter::recursive_cte", e)
                })
                .expect("a fixture publishes exactly what it names")
        }

        fn table(scope: crate::names::ScopeId) -> TableExpression {
            TableExpression::Scope(scope)
        }

        fn column(column: crate::names::ColId) -> DomainExpression {
            DomainExpression::Column(column)
        }

        fn base_member(&self) -> SelectStatement {
            self.publish(
                self.x,
                SelectStatement::builder().select(SelectItem::expression_with_alias(
                    DomainExpression::Literal(LiteralValue::integer(1)),
                    self.xn,
                )),
            )
        }

        /// The statement as the transformer builds it for a binding the
        /// resolver decided is a fixpoint: an anchor and one member, kept
        /// apart, joined by the accumulation the decision named.
        fn fixpoint_stmt(&self, member: SelectStatement) -> SqlStatement {
            self.fixpoint_query_stmt(QueryExpression::Select(Box::new(member)))
        }

        fn fixpoint_stmt_with_members(&self, members: Vec<SelectStatement>) -> SqlStatement {
            let cte = Cte::fixpoint(SqlFixpoint::bag_fixture(
                self.x,
                QueryExpression::Select(Box::new(self.base_member())),
                members
                    .into_iter()
                    .map(|member| QueryExpression::Select(Box::new(member)))
                    .collect(),
            ));
            SqlStatement::Query {
                with_clause: Some(vec![cte]),
                query: QueryExpression::Select(Box::new(
                    self.publish(
                        self.x,
                        SelectStatement::builder()
                            .select(SelectItem::star_over_nothing())
                            .from_tables(vec![Self::table(self.x)]),
                    ),
                )),
            }
        }

        fn fixpoint_query_stmt(&self, member: QueryExpression) -> SqlStatement {
            self.anchored_fixpoint(self.base_member(), member)
        }

        fn anchored_fixpoint(
            &self,
            anchor: SelectStatement,
            member: QueryExpression,
        ) -> SqlStatement {
            let cte = Cte::fixpoint(SqlFixpoint::bag_fixture(
                self.x,
                QueryExpression::Select(Box::new(anchor)),
                vec![member],
            ));
            SqlStatement::Query {
                with_clause: Some(vec![cte]),
                query: QueryExpression::Select(Box::new(
                    self.publish(
                        self.x,
                        SelectStatement::builder()
                            .select(SelectItem::star_over_nothing())
                            .from_tables(vec![Self::table(self.x)]),
                    ),
                )),
            }
        }

        fn recursive_member(
            &self,
            scope: crate::names::ScopeId,
            column: crate::names::ColId,
        ) -> SelectStatement {
            self.publish(
                self.x,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(column),
                        self.xn,
                    ))
                    .from_tables(vec![Self::table(scope)]),
            )
        }

        fn capped_recursive_member(&self) -> SelectStatement {
            self.publish(
                self.x,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(self.xn),
                        self.xn,
                    ))
                    .from_tables(vec![Self::table(self.x)])
                    .limit_from(crate::pipeline::sql_ast::ordering::Limit::new(2)),
            )
        }

        fn buried_limit_member(&self) -> SelectStatement {
            let inner = self.publish(
                self.w,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(self.xn),
                        self.wn,
                    ))
                    .from_tables(vec![Self::table(self.x)])
                    .limit_from(crate::pipeline::sql_ast::ordering::Limit::new(2)),
            );
            self.publish(
                self.x,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(self.wn),
                        self.xn,
                    ))
                    .from_tables(vec![TableExpression::Subquery {
                        query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(
                            Box::new(inner),
                        ))),
                        alias: self.w,
                    }]),
            )
        }

        fn transparently_renamed_member(
            &self,
            inner_predicate: Option<DomainExpression>,
            inner_distinct: bool,
        ) -> SelectStatement {
            let mut inner = SelectStatement::builder()
                .select(SelectItem::expression_with_alias(
                    Self::column(self.xn),
                    self.wn,
                ))
                .from_tables(vec![Self::table(self.x)]);
            if let Some(predicate) = inner_predicate {
                inner = inner.where_clause(predicate);
            }
            if inner_distinct {
                inner = inner.distinct();
            }
            let inner = self.publish(self.w, inner);
            self.publish(
                self.x,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(self.wn),
                        self.xn,
                    ))
                    .from_tables(vec![TableExpression::Subquery {
                        query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(
                            Box::new(inner),
                        ))),
                        alias: self.w,
                    }]),
            )
        }

        /// The realizer's ordered stage under a member: `SELECT w.n + 1 AS n
        /// FROM (SELECT x.n AS n FROM x ORDER BY x.n DESC) AS w`, with the
        /// member's condition `w.n + 1 < 4` when `condition`, the inner
        /// publishing a window over `x.n` instead of `x.n` when `window`.
        fn ordered_stage_member(&self, condition: bool, window: bool) -> SelectStatement {
            use crate::pipeline::sql_ast::{BinaryOperator, OrderDirection, OrderTerm};
            let published = if window {
                DomainExpression::WindowFunction {
                    name: "row_number".to_string(),
                    args: Vec::new(),
                    distinct: false,
                    partition_by: Vec::new(),
                    order_by: vec![(Self::column(self.xn), OrderDirection::Asc)],
                    frame: None,
                }
            } else {
                Self::column(self.xn)
            };
            let inner = self.publish(
                self.w,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(published, self.wn))
                    .from_tables(vec![Self::table(self.x)])
                    .order_by(OrderTerm::new(Self::column(self.xn), Some(OrderDirection::Desc))),
            );
            let plus_one = |over: crate::names::ColId| DomainExpression::Binary {
                left: Box::new(Self::column(over)),
                op: BinaryOperator::Add,
                right: Box::new(DomainExpression::Literal(LiteralValue::integer(1))),
            };
            let mut outer = SelectStatement::builder()
                .select(SelectItem::expression_with_alias(plus_one(self.wn), self.xn))
                .from_tables(vec![TableExpression::Subquery {
                    query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(Box::new(inner)))),
                    alias: self.w,
                }]);
            if condition {
                outer = outer.where_clause(DomainExpression::Binary {
                    left: Box::new(plus_one(self.wn)),
                    op: BinaryOperator::LessThan,
                    right: Box::new(DomainExpression::Literal(LiteralValue::integer(4))),
                });
            }
            self.publish(self.x, outer)
        }

        /// A stage standing at `at`, publishing `expr` as `col` from
        /// `source` (the frontier, or another stage standing at its scope),
        /// optionally restricted by `col > 0` over what it reads and ordered
        /// by what it reads.
        fn stage(
            &self,
            at: crate::names::ScopeId,
            col: crate::names::ColId,
            source: (crate::names::ScopeId, crate::names::ColId, Option<SelectStatement>),
            plus_zero: bool,
            positive: bool,
            ordered: Option<crate::pipeline::sql_ast::OrderDirection>,
        ) -> SelectStatement {
            use crate::pipeline::sql_ast::{BinaryOperator, OrderTerm};
            let (scope, read, body) = source;
            let from = match body {
                None => Self::table(scope),
                Some(body) => TableExpression::Subquery {
                    query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(Box::new(body)))),
                    alias: scope,
                },
            };
            let published = if plus_zero {
                DomainExpression::Binary {
                    left: Box::new(Self::column(read)),
                    op: BinaryOperator::Add,
                    right: Box::new(DomainExpression::Literal(LiteralValue::integer(0))),
                }
            } else {
                Self::column(read)
            };
            let mut stage = SelectStatement::builder()
                .select(SelectItem::expression_with_alias(published, col))
                .from_tables(vec![from]);
            if positive {
                stage = stage.where_clause(DomainExpression::Binary {
                    left: Box::new(Self::column(read)),
                    op: BinaryOperator::GreaterThan,
                    right: Box::new(DomainExpression::Literal(LiteralValue::integer(0))),
                });
            }
            if let Some(direction) = ordered {
                stage = stage.order_by(OrderTerm::new(Self::column(read), Some(direction)));
            }
            self.publish(at, stage)
        }

        /// The member `SELECT top.n + 1 AS n FROM (top) AS top WHERE
        /// top.n + 1 < 4` over a stage standing at `scope`.
        fn member_reading(&self, scope: crate::names::ScopeId, col: crate::names::ColId, top: SelectStatement) -> SelectStatement {
            use crate::pipeline::sql_ast::BinaryOperator;
            let plus_one = DomainExpression::Binary {
                left: Box::new(Self::column(col)),
                op: BinaryOperator::Add,
                right: Box::new(DomainExpression::Literal(LiteralValue::integer(1))),
            };
            let outer = SelectStatement::builder()
                .select(SelectItem::expression_with_alias(plus_one.clone(), self.xn))
                .from_tables(vec![TableExpression::Subquery {
                    query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(Box::new(top)))),
                    alias: scope,
                }])
                .where_clause(DomainExpression::Binary {
                    left: Box::new(plus_one),
                    op: BinaryOperator::LessThan,
                    right: Box::new(DomainExpression::Literal(LiteralValue::integer(4))),
                });
            self.publish(self.x, outer)
        }

        /// The fixpoint over `x(n, m)`: the anchor `SELECT 1 AS n, 2 AS m`
        /// and one member.
        fn wide_fixpoint_stmt(&self, member: SelectStatement) -> SqlStatement {
            let anchor = self.publish(
                self.x,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        DomainExpression::Literal(LiteralValue::integer(1)),
                        self.xn,
                    ))
                    .select(SelectItem::expression_with_alias(
                        DomainExpression::Literal(LiteralValue::integer(2)),
                        self.xm,
                    )),
            );
            self.anchored_fixpoint(anchor, QueryExpression::Select(Box::new(member)))
        }

        /// `SELECT w.? AS n, w.? AS m FROM (SELECT x.n AS n, x.m AS m FROM x
        /// WHERE x.n) AS w`, standing at `x`, with the outer items reading
        /// `reads` in order. The inner filters, so it is a member body rather
        /// than a rename of `x`.
        fn wide_wrapped_member(&self, reads: [crate::names::ColId; 2]) -> SelectStatement {
            let inner = self.publish(
                self.w,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(self.xn),
                        self.wn,
                    ))
                    .select(SelectItem::expression_with_alias(
                        Self::column(self.xm),
                        self.wm,
                    ))
                    .from_tables(vec![Self::table(self.x)])
                    .where_clause(Self::column(self.xn)),
            );
            self.publish(
                self.x,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(reads[0]),
                        self.xn,
                    ))
                    .select(SelectItem::expression_with_alias(
                        Self::column(reads[1]),
                        self.xm,
                    ))
                    .from_tables(vec![TableExpression::Subquery {
                        query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(
                            Box::new(inner),
                        ))),
                        alias: self.w,
                    }]),
            )
        }

        /// `(SELECT x.n AS a.n FROM x) AS a` — the wrapper a reference builds
        /// when it needs a qualifier of its own.
        fn rename_of_self(
            &self,
            alias: crate::names::ScopeId,
            output: crate::names::ColId,
        ) -> TableExpression {
            let inner = self.publish(
                alias,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        Self::column(self.xn),
                        output,
                    ))
                    .from_tables(vec![Self::table(self.x)]),
            );
            TableExpression::Subquery {
                query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(
                    Box::new(inner),
                ))),
                alias,
            }
        }

        fn member_over(
            &self,
            from: Vec<TableExpression>,
            read: crate::names::ColId,
        ) -> QueryExpression {
            QueryExpression::Select(Box::new(
                self.publish(
                    self.x,
                    SelectStatement::builder()
                        .select(SelectItem::expression_with_alias(
                            Self::column(read),
                            self.xn,
                        ))
                        .from_tables(from),
                ),
            ))
        }
    }

    /// The one recursive member of the fixture's fixpoint.
    fn sole_member(stmt: &SqlStatement) -> &QueryExpression {
        let CteBody::Fixpoint(fixpoint) = first_cte(stmt).body() else {
            panic!("expected a fixpoint body");
        };
        let [member] = fixpoint.members() else {
            panic!("the fixture has exactly one member");
        };
        member
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

    fn sqlite_target() -> crate::pipeline::aggregate_catalog::TargetAggregates {
        crate::pipeline::aggregate_catalog::TargetAggregates::of(
            crate::pipeline::aggregate_catalog::seeded_catalog(),
            SqlDialect::SQLite,
        )
    }

    fn expect_badge(stmt: &mut SqlStatement, leaf: &str) {
        let err = validate_recursive_members(stmt, &sqlite_target()).unwrap_err();
        assert!(err
            .error_uri()
            .ends_with(&format!("semantic/recursion/{leaf}")));
    }

    #[test]
    fn an_aliased_self_reference_becomes_a_direct_one() {
        let f = Fixture::new();
        let mut member = f.member_over(vec![f.rename_of_self(f.a, f.an)], f.an);
        inline_aliased_self_references(&mut member, f.x);
        let QueryExpression::Select(select) = &member else {
            panic!("expected a SELECT member");
        };
        assert_eq!(select.from().unwrap(), [Fixture::table(f.x)]);
        // Dropping the wrapper without re-anchoring would leave the item
        // reading a column of a scope that no longer stands anywhere.
        assert_eq!(
            select.select_list(),
            [SelectItem::expression_with_alias(
                Fixture::column(f.xn),
                f.xn
            )]
        );
    }

    #[test]
    fn two_aliased_self_references_are_left_alone() {
        let f = Fixture::new();
        let from = vec![f.rename_of_self(f.a, f.an), f.rename_of_self(f.w, f.wn)];
        let mut member = f.member_over(from.clone(), f.an);
        inline_aliased_self_references(&mut member, f.x);
        let QueryExpression::Select(select) = &member else {
            panic!("expected a SELECT member");
        };
        // Both would inline to the same bare name and collide.
        assert_eq!(select.from().unwrap(), from.as_slice());
    }

    /// Two renamed self-references are a frontier self-JOIN — N1 — and the
    /// refusal must say so. They are the pair `inline_aliased_self_references`
    /// deliberately leaves alone (both would inline to one name), so if a
    /// rename wrapper reads as burial the member reports N4 instead and the
    /// teaching sends the reader to the wrong rewrite. No outside observer
    /// can separate the two refusals from the query alone: both are refusals
    /// of the same source text.
    #[test]
    fn two_renamed_self_references_are_nonlinear_not_buried() {
        let f = Fixture::new();
        let from = vec![f.rename_of_self(f.a, f.an), f.rename_of_self(f.w, f.wn)];
        let QueryExpression::Select(member) = f.member_over(from, f.an) else {
            unreachable!("member_over builds a SELECT")
        };
        expect_badge(&mut f.fixpoint_stmt(*member), "nonlinear");
    }

    #[test]
    fn a_wrapper_that_selects_rows_is_not_a_rename() {
        let f = Fixture::new();
        let inner = f.publish(
            f.a,
            SelectStatement::builder()
                .select(SelectItem::expression_with_alias(
                    Fixture::column(f.xn),
                    f.an,
                ))
                .from_tables(vec![Fixture::table(f.x)])
                .where_clause(Fixture::column(f.xn)),
        );
        let from = vec![TableExpression::Subquery {
            query: Box::new(stacksafe::StackSafe::new(QueryExpression::Select(
                Box::new(inner),
            ))),
            alias: f.a,
        }];
        let mut member = f.member_over(from.clone(), f.an);
        inline_aliased_self_references(&mut member, f.x);
        let QueryExpression::Select(select) = &member else {
            panic!("expected a SELECT member");
        };
        assert_eq!(select.from().unwrap(), from.as_slice());
    }

    #[test]
    fn buried_limit_unwraps_only_where_supported() {
        let f = Fixture::new();
        // The fixpoint body arrives from the resolver's decision; this
        // pass reads it.
        let mut sqlite = f.fixpoint_stmt(f.buried_limit_member());
        legalize_recursive_limits(&mut sqlite, SqlDialect::SQLite).unwrap();
        let QueryExpression::Select(member) = sole_member(&sqlite) else {
            panic!("expected recursive SELECT");
        };
        assert_eq!(member.from().unwrap()[0], Fixture::table(f.x));
        assert_eq!(member.limit().unwrap().count(), Some(2));
        let Some(expr) = member.select_list()[0].expr() else {
            panic!("expected expression");
        };
        assert_eq!(expr, &Fixture::column(f.xn));

        let mut postgres = f.fixpoint_stmt(f.buried_limit_member());
        let err = legalize_recursive_limits(&mut postgres, SqlDialect::PostgreSQL).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/recursion/limit_bound"
        );
        assert!(err.to_string().contains("total-row cap"));
    }

    #[test]
    fn sqlite_multiple_recursive_member_caps_have_the_limit_bound_identity() {
        let f = Fixture::new();
        let mut stmt = f.fixpoint_stmt_with_members(vec![
            f.capped_recursive_member(),
            f.capped_recursive_member(),
        ]);
        let err = legalize_recursive_limits(&mut stmt, SqlDialect::SQLite).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/recursion/limit_bound"
        );
    }

    #[test]
    fn sqlite_nonfinal_recursive_member_cap_has_the_limit_bound_identity() {
        let f = Fixture::new();
        let mut stmt = f.fixpoint_stmt_with_members(vec![
            f.capped_recursive_member(),
            f.recursive_member(f.x, f.xn),
        ]);
        let err = legalize_recursive_limits(&mut stmt, SqlDialect::SQLite).unwrap_err();
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/recursion/limit_bound"
        );
    }

    #[test]
    fn plain_recursive_member_is_unchanged() {
        let f = Fixture::new();
        let member = f.recursive_member(f.x, f.xn);
        let mut stmt = f.fixpoint_stmt(member.clone());
        legalize_recursive_limits(&mut stmt, SqlDialect::PostgreSQL).unwrap();
        assert_eq!(
            *sole_member(&stmt),
            QueryExpression::Select(Box::new(member))
        );
    }

    #[test]
    fn transparent_member_rebuilds_at_the_outer_scope_before_validation() {
        let f = Fixture::new();
        let mut stmt =
            f.fixpoint_stmt(f.transparently_renamed_member(Some(Fixture::column(f.xn)), false));
        validate_recursive_members(&mut stmt, &sqlite_target()).unwrap();
        let right = sole_member(&stmt);
        let QueryExpression::Select(member) = right else {
            panic!("expected recursive SELECT");
        };
        assert_eq!(member.from().unwrap(), [Fixture::table(f.x)]);
        assert_eq!(
            member.select_list(),
            [SelectItem::expression_with_alias(
                Fixture::column(f.xn),
                f.xn
            )]
        );
        assert_eq!(member.where_clause(), Some(&Fixture::column(f.xn)));
    }

    /// ORDERING IS ADMITTED: an ordered stage over the frontier inlines, its
    /// reads re-expressed over the frontier, and keeps no ordering, which no
    /// target spells on a recursive member.
    #[test]
    fn an_ordered_stage_over_the_frontier_inlines_without_its_ordering() {
        use crate::pipeline::sql_ast::BinaryOperator;
        let f = Fixture::new();
        let mut stmt = f.fixpoint_stmt(f.ordered_stage_member(true, false));
        validate_recursive_members(&mut stmt, &sqlite_target()).unwrap();
        let QueryExpression::Select(member) = sole_member(&stmt) else {
            panic!("expected recursive SELECT");
        };
        let plus_one = DomainExpression::Binary {
            left: Box::new(Fixture::column(f.xn)),
            op: BinaryOperator::Add,
            right: Box::new(DomainExpression::Literal(LiteralValue::integer(1))),
        };
        assert_eq!(member.from().unwrap(), [Fixture::table(f.x)]);
        assert_eq!(member.select_list(), [SelectItem::expression_with_alias(plus_one.clone(), f.xn)]);
        assert_eq!(
            member.where_clause(),
            Some(&DomainExpression::Binary {
                left: Box::new(plus_one),
                op: BinaryOperator::LessThan,
                right: Box::new(DomainExpression::Literal(LiteralValue::integer(4))),
            })
        );
        assert!(member.order_by().is_none());
    }

    /// A window the inner computes would move under the member's condition
    /// if the stage were inlined: the self-reference stays buried.
    #[test]
    fn an_ordered_stage_with_a_window_under_a_condition_remains_buried() {
        let f = Fixture::new();
        expect_badge(&mut f.fixpoint_stmt(f.ordered_stage_member(true, true)), "self_subquery");
    }

    /// Without a member condition the window's rows are unchanged, so the
    /// stage inlines.
    #[test]
    fn an_ordered_stage_with_a_window_and_no_condition_inlines() {
        let f = Fixture::new();
        let mut stmt = f.fixpoint_stmt(f.ordered_stage_member(false, true));
        validate_recursive_members(&mut stmt, &sqlite_target()).unwrap();
        let QueryExpression::Select(member) = sole_member(&stmt) else {
            panic!("expected recursive SELECT");
        };
        assert_eq!(member.from().unwrap(), [Fixture::table(f.x)]);
        assert!(member.order_by().is_none());
    }

    /// A collapsed member reads the frontier directly and orders nothing.
    fn collapsed(stmt: &mut SqlStatement) -> SelectStatement {
        validate_recursive_members(stmt, &sqlite_target()).unwrap();
        let QueryExpression::Select(member) = sole_member(stmt) else {
            panic!("expected recursive SELECT");
        };
        assert!(matches!(member.from().unwrap(), [TableExpression::Scope(_)]), "{member:?}");
        assert!(member.order_by().is_none());
        (**member).clone()
    }

    fn plus(column: crate::names::ColId, n: i64) -> DomainExpression {
        DomainExpression::Binary {
            left: Box::new(Fixture::column(column)),
            op: crate::pipeline::sql_ast::BinaryOperator::Add,
            right: Box::new(DomainExpression::Literal(LiteralValue::integer(n))),
        }
    }

    /// Two consecutive orderings: `c |> #(n desc) |> #(n) |> (n + 1), n < 4`.
    #[test]
    fn consecutive_ordered_stages_collapse_onto_the_frontier() {
        use crate::pipeline::sql_ast::OrderDirection::{Asc, Desc};
        let f = Fixture::new();
        let first = f.stage(f.w, f.wn, (f.x, f.xn, None), false, false, Some(Desc));
        let second = f.stage(f.a, f.an, (f.w, f.wn, Some(first)), false, false, Some(Asc));
        let member = collapsed(&mut f.fixpoint_stmt(f.member_reading(f.a, f.an, second)));
        assert_eq!(member.from().unwrap(), [Fixture::table(f.x)]);
        assert_eq!(member.select_list(), [SelectItem::expression_with_alias(plus(f.xn, 1), f.xn)]);
    }

    /// A restriction the second ordered stage carries stands under the
    /// member's own condition.
    #[test]
    fn a_restriction_between_orderings_stands_under_the_member() {
        use crate::pipeline::sql_ast::OrderDirection::{Asc, Desc};
        let f = Fixture::new();
        let first = f.stage(f.w, f.wn, (f.x, f.xn, None), false, false, Some(Desc));
        let second = f.stage(f.a, f.an, (f.w, f.wn, Some(first)), false, true, Some(Asc));
        let member = collapsed(&mut f.fixpoint_stmt(f.member_reading(f.a, f.an, second)));
        let condition = format!("{:?}", member.where_clause().expect("both conditions"));
        assert!(condition.contains("GreaterThan") && condition.contains("LessThan"), "{condition}");
    }

    /// A projection between two orderings inlines into the ordered stage
    /// that reads it.
    #[test]
    fn a_projection_between_orderings_collapses() {
        use crate::pipeline::sql_ast::OrderDirection::{Asc, Desc};
        let f = Fixture::new();
        let first = f.stage(f.w, f.wn, (f.x, f.xn, None), false, false, Some(Desc));
        let projection = f.stage(f.t, f.tn, (f.w, f.wn, Some(first)), true, false, None);
        let second = f.stage(f.a, f.an, (f.t, f.tn, Some(projection)), false, false, Some(Asc));
        let member = collapsed(&mut f.fixpoint_stmt(f.member_reading(f.a, f.an, second)));
        let expected = DomainExpression::Binary {
            left: Box::new(plus(f.xn, 0)),
            op: crate::pipeline::sql_ast::BinaryOperator::Add,
            right: Box::new(DomainExpression::Literal(LiteralValue::integer(1))),
        };
        assert_eq!(member.select_list(), [SelectItem::expression_with_alias(expected, f.xn)]);
    }

    /// Three consecutive orderings collapse as two do.
    #[test]
    fn three_ordered_stages_collapse() {
        use crate::pipeline::sql_ast::OrderDirection::{Asc, Desc};
        let f = Fixture::new();
        let first = f.stage(f.w, f.wn, (f.x, f.xn, None), false, false, Some(Desc));
        let second = f.stage(f.t, f.tn, (f.w, f.wn, Some(first)), false, false, Some(Asc));
        let third = f.stage(f.a, f.an, (f.t, f.tn, Some(second)), false, false, Some(Desc));
        let member = collapsed(&mut f.fixpoint_stmt(f.member_reading(f.a, f.an, third)));
        assert_eq!(member.select_list(), [SelectItem::expression_with_alias(plus(f.xn, 1), f.xn)]);
    }

    /// A plain stage that no ordered stage reads is not this legalization's:
    /// a bounded stage beneath an ordering stays buried, as before.
    #[test]
    fn a_bounded_stage_beneath_an_ordering_remains_buried() {
        use crate::pipeline::sql_ast::OrderDirection::{Asc, Desc};
        let f = Fixture::new();
        let first = f.stage(f.w, f.wn, (f.x, f.xn, None), false, false, Some(Desc));
        let mut bounded = f.stage(f.t, f.tn, (f.w, f.wn, Some(first)), false, false, Some(Asc));
        bounded.set_limit(crate::pipeline::sql_ast::ordering::Limit::new(1));
        let second = f.stage(f.a, f.an, (f.t, f.tn, Some(bounded)), false, false, Some(Asc));
        expect_badge(&mut f.fixpoint_stmt(f.member_reading(f.a, f.an, second)), "self_subquery");
    }

    #[test]
    fn a_distinct_self_wrapper_remains_buried() {
        let f = Fixture::new();
        let member = f.transparently_renamed_member(None, true);
        expect_badge(&mut f.fixpoint_stmt(member), "self_subquery");
    }

    /// A wrapper that reads every inner output exactly once is a rename in
    /// any order. The member rebuilds at the CTE in the WRAPPER's order, so
    /// the rows it unions are the rows the wrapper published.
    #[test]
    fn a_reordering_wrapper_unwraps_by_identity() {
        let f = Fixture::new();
        let mut stmt = f.wide_fixpoint_stmt(f.wide_wrapped_member([f.wm, f.wn]));
        validate_recursive_members(&mut stmt, &sqlite_target()).unwrap();
        let QueryExpression::Select(member) = sole_member(&stmt) else {
            panic!("expected recursive SELECT");
        };
        assert_eq!(member.at(), f.x);
        assert_eq!(member.from().unwrap(), [Fixture::table(f.x)]);
        assert_eq!(
            member.select_list(),
            [
                SelectItem::expression_with_alias(Fixture::column(f.xm), f.xn),
                SelectItem::expression_with_alias(Fixture::column(f.xn), f.xm),
            ]
        );
        assert_eq!(member.where_clause(), Some(&Fixture::column(f.xn)));
    }

    /// Same width is not a rename: reading one inner output twice drops the
    /// other, so the wrapper changes the rows and the self-reference stays
    /// buried.
    #[test]
    fn a_same_width_wrapper_that_repeats_an_output_remains_buried() {
        let f = Fixture::new();
        let member = f.wide_wrapped_member([f.wn, f.wn]);
        expect_badge(&mut f.wide_fixpoint_stmt(member), "self_subquery");
    }

    /// An item reading anything but the inner's own outputs — here the CTE
    /// column the inner reads rather than the one it publishes — is not an
    /// inner output under another name.
    #[test]
    fn a_wrapper_reading_what_the_inner_does_not_publish_remains_buried() {
        let f = Fixture::new();
        let member = f.wide_wrapped_member([f.wn, f.xm]);
        expect_badge(&mut f.wide_fixpoint_stmt(member), "self_subquery");
    }

    #[test]
    fn nonlinear_and_subquery_self_references_are_refused() {
        let f = Fixture::new();
        let nonlinear = SelectStatement::builder()
            .select(SelectItem::expression_with_alias(
                Fixture::column(f.an),
                f.xn,
            ))
            .from_tables(vec![TableExpression::Join {
                left: Box::new(Fixture::table(f.x)),
                right: Box::new(Fixture::table(f.x)),
                join_type: crate::pipeline::sql_ast::JoinType::Inner,
                join_condition: crate::pipeline::sql_ast::JoinCondition::On(Fixture::column(f.xn)),
            }]);
        let nonlinear = f.publish(f.x, nonlinear);
        expect_badge(&mut f.fixpoint_stmt(nonlinear), "nonlinear");

        let inner = f.publish(
            f.x,
            SelectStatement::builder()
                .select(SelectItem::star_over_nothing())
                .from_tables(vec![Fixture::table(f.x)]),
        );
        let subquery = f.publish(
            f.x,
            SelectStatement::builder()
                .select(SelectItem::expression_with_alias(
                    Fixture::column(f.xn),
                    f.xn,
                ))
                .from_tables(vec![Fixture::table(f.x)])
                .where_clause(DomainExpression::Exists {
                    not: true,
                    query: Box::new(QueryExpression::Select(Box::new(inner))),
                }),
        );
        expect_badge(&mut f.fixpoint_stmt(subquery), "self_subquery");
    }

    #[test]
    fn aggregate_is_refused_only_in_recursive_member() {
        let f = Fixture::new();
        let aggregate = |scope, column| {
            f.publish(
                f.x,
                SelectStatement::builder()
                    .select(SelectItem::expression_with_alias(
                        DomainExpression::Function {
                            name: "max".into(),
                            args: vec![Fixture::column(column)],
                            distinct: false,
                        },
                        f.xn,
                    ))
                    .from_tables(vec![Fixture::table(scope)]),
            )
        };
        expect_badge(&mut f.fixpoint_stmt(aggregate(f.x, f.xn)), "aggregate");
        validate_recursive_members(&mut f.fixpoint_stmt(aggregate(f.t, f.tn)), &sqlite_target())
            .unwrap();
    }
}
