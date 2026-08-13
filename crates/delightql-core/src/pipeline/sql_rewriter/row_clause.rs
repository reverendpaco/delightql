// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// row_clause.rs — Put a row bound where Transact-SQL keeps one.
//
// Four targets take a trailing `LIMIT`/`OFFSET` clause that stands on its
// own, so the lowering's habit of ordering in a derived table and bounding
// outside it costs them nothing. T-SQL has no such clause: `OFFSET` is part
// of `ORDER BY`, and an `ORDER BY` in a derived table is a syntax error
// unless something in that same block consumes it. Split across two blocks,
// the bound then skips rows of a relation whose order the outer block never
// promised — legal-looking SQL that answers a different question.
//
// Two moves put them back together. A bound standing on a passthrough over
// an ordered body moves INTO that body, which is exact because a passthrough
// neither drops rows nor makes them. An ordering that no bound ever reaches
// is consumed where it stands by a skip of nothing, which is what the other
// targets already get from engine courtesy.

use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::ordering::Limit;
use crate::pipeline::sql_ast::{
    walk, QueryExpression, SelectItem, SelectStatement, SqlStatement, TableExpression,
};

/// Does this target keep its bound inside the ordering's block?
pub fn needs_legalization(dialect: SqlDialect) -> bool {
    match dialect {
        SqlDialect::SqlServer => true,
        SqlDialect::SQLite | SqlDialect::PostgreSQL | SqlDialect::MySQL | SqlDialect::DuckDB => {
            false
        }
    }
}

/// Rewrite every row bound into one T-SQL will accept and answer.
pub fn legalize_row_clauses(stmt: &mut SqlStatement) {
    // Reuniting comes first, and in its own pass: a body given a consumer
    // for its ordering already has a bound, and the bound above it would
    // then have nowhere to land.
    struct Reunite;
    impl walk::SqlVisitorMut for Reunite {
        fn query(&mut self, q: &mut QueryExpression) {
            if let QueryExpression::Select(select) = q {
                cap_of_zero_needs_no_skip(select);
                sink_bound_into_ordered_body(select);
            }
        }
    }
    struct Consume;
    impl walk::SqlVisitorMut for Consume {
        fn query(&mut self, q: &mut QueryExpression) {
            if let QueryExpression::Select(select) = q {
                consume_a_stranded_ordering(select);
            }
        }
    }
    walk::visit_mut(stmt, &mut Reunite);
    walk::visit_mut(stmt, &mut Consume);

    // The statement's own final ordering answers to no one, so it needs no
    // consumer and gets none. Releasing a skip of nothing is always sound —
    // it is the identity on any relation — so this needs no record of which
    // skips the pass above put there.
    if let SqlStatement::Query {
        query: QueryExpression::Select(select),
        ..
    } = stmt
    {
        if select.limit().is_some_and(is_skip_of_nothing) {
            select.clear_limit();
        }
    }
}

/// A cap of zero admits no skip. `FETCH NEXT` takes no count below one, and
/// `TOP 0` needs no ordering — and skipping rows before taking none of them
/// takes none either way, so the skip is dropped rather than spelled.
fn cap_of_zero_needs_no_skip(select: &mut SelectStatement) {
    if select.limit().is_some_and(|l| l.count() == Some(0)) {
        select.set_limit(Limit::new(0));
    }
}

/// Move a bound standing on a passthrough down onto the ordered body it
/// reads, so the two arrive in one block.
///
/// The move is exact only over a body whose rows leave unchanged and
/// unmultiplied: any predicate, grouping, distinct, join or bound of the
/// outer block's own makes "bound the body" and "bound the result" two
/// different relations, and then the bound stays where the author put it.
fn sink_bound_into_ordered_body(select: &mut SelectStatement) {
    let Some(bound) = select.limit().cloned() else {
        return;
    };
    if select.order_by().is_some()
        || select.where_clause().is_some()
        || select.group_by().is_some()
        || select.having().is_some()
        || select.is_distinct()
        || !select
            .select_list()
            .iter()
            .all(|item| matches!(item, SelectItem::Expression { .. }))
    {
        return;
    }
    let Some([TableExpression::Subquery { query, .. }]) = select.from_mut() else {
        return;
    };
    let QueryExpression::Select(body) = &mut ***query else {
        return;
    };
    if body.order_by().is_none() || body.limit().is_some() {
        return;
    }
    body.set_limit(bound);
    select.clear_limit();
}

/// Give an ordering that no bound reaches something to belong to.
///
/// A skip of nothing is the identity, so this adds no meaning; it says only
/// that the ordering ends here, which is what T-SQL requires a derived
/// table's ordering to say.
fn consume_a_stranded_ordering(select: &mut SelectStatement) {
    if select.order_by().is_some() && select.limit().is_none() {
        select.set_limit(Limit::offset_only(0));
    }
}

fn is_skip_of_nothing(limit: &Limit) -> bool {
    limit.count().is_none() && limit.offset() == Some(0)
}
