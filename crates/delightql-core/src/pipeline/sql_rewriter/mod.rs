// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// sql_rewriter/mod.rs — Dialect-aware SQL AST rewriting
//
// Runs AFTER lowering and BEFORE the optimizer.
// Rewrites SQL patterns that the target dialect cannot express natively.
//
// Two entry points, sandwiching the optimizer (the lowering sandwich):
//
//   rewrite()  — expansions, BEFORE the optimizer (their verbose output
//                benefits from cleanup):
//                - FULL OUTER JOIN → LEFT JOIN UNION ALL (SQLite, MySQL)
//   legalize() — mandatory legalizations, AFTER the optimizer — the
//                final word; nothing may rewrite the tree afterwards, so
//                "never illegal SQL" holds by construction:
//                - literal ORDER BY / GROUP BY key, which SQL reads as a
//                  position → dropped, a literal-only grouping kept as one
//                  group of an inhabited input in the target's own form
//                  (all targets) — FIRST
//                - recursive-CTE marking → WITH RECURSIVE (all targets)
//                - bare JOIN (no ON) → CROSS JOIN / ON TRUE (postgres,
//                  duckdb, sqlserver)
//                - #<N in recursive members: total-cap LIMIT hoist
//                  (sqlite, mysql) or diagnostic (postgres, duckdb,
//                  sqlserver)
//                - row bound → TOP / ORDER BY … OFFSET … FETCH (sqlserver),
//                  after the recursive passes
//                - compound columns whose arms carry unlike affinity →
//                  every arm affinity-free (sqlite) — LAST, over the
//                  final shape

mod bare_join;
mod compound_affinity;
mod document_numbers;
mod full_outer;
mod recursive_cte;
mod row_clause;
mod value_keys;

use crate::error::Result;
use crate::pipeline::generator::SqlDialect;
use crate::pipeline::sql_ast::SqlStatement;

/// Expand SQL patterns the target cannot express natively. Runs BEFORE
/// the optimizer so cleanup can tidy the expansion's output.
pub fn rewrite(
    statement: SqlStatement,
    dialect: SqlDialect,
    identities: &crate::names::Registry,
) -> Result<SqlStatement> {
    let stmt = if full_outer::needs_expansion(dialect) {
        let target =
            crate::pipeline::aggregate_catalog::TargetAggregates::of_arena(identities, dialect);
        full_outer::expand_full_outer_joins(statement, identities, &target)?
    } else {
        statement
    };

    Ok(stmt)
}

/// The final legalization word. Runs AFTER the optimizer; nothing may
/// rewrite the statement after this returns.
pub fn legalize(
    statement: SqlStatement,
    dialect: SqlDialect,
    identities: &crate::names::Registry,
) -> Result<SqlStatement> {
    let mut stmt = statement;

    // First, on every target: no key SQL would read as a position reaches
    // the text, and the row clause legalization below reads whether a block
    // still orders.
    value_keys::legalize_value_keys(&mut stmt, dialect);

    let mut stmt = if bare_join::needs_legalization(dialect) {
        bare_join::legalize_bare_joins(stmt)
    } else {
        stmt
    };

    recursive_cte::legalize_recursive_limits(&mut stmt, dialect)?;

    // The recursion validator (LINEARITY, STRATA ARE TEXTUAL, NO SUBQUERY
    // AGAINST THE TARGET) — after the limit legalization, so the one legal
    // buried shape has already been unwrapped before burial is judged.
    recursive_cte::validate_recursive_members(
        &mut stmt,
        &crate::pipeline::aggregate_catalog::TargetAggregates::of_arena(identities, dialect),
    )?;

    // After both recursive passes: the skip of nothing that consumes a
    // stranded ordering is a bound to them, and a member's ordered stage
    // would read as a buried cap.
    if row_clause::needs_legalization(dialect) {
        row_clause::legalize_row_clauses(&mut stmt);
    }

    // A judgment, not a rewrite: on a target whose document read states no
    // category, every consumer that needs one refuses here, before any SQL
    // is written, unless an authored cast has established it.
    if document_numbers::needs_judgment(dialect) {
        document_numbers::refuse_unproved_document_numbers(&stmt, dialect, identities)?;
    }

    // THE LAST WORD. The affinity judgment reads the shape SQL will carry,
    // so it runs after every rewrite that can change a compound arm: the
    // recursive legalizations above unwrap a member's transparent limit
    // wrapper and inline an aliased self-reference, and a `+` written
    // before that would hide the direct column their proofs require.
    if compound_affinity::needs_legalization(dialect) {
        compound_affinity::legalize_compound_affinity(&mut stmt, identities)?;
    }

    Ok(stmt)
}
