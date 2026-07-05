// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// sql_rewriter/mod.rs — Dialect-aware SQL AST rewriting
//
// Runs AFTER the transformer and BEFORE the optimizer.
// Rewrites SQL patterns that the target dialect cannot express natively.
//
// Two entry points, sandwiching the optimizer (the lowering sandwich,
// ALL-SQL-TARGETING-DESIGN.md §2.D):
//
//   rewrite()  — expansions, BEFORE the optimizer (their verbose output
//                benefits from cleanup):
//                - FULL OUTER JOIN → LEFT JOIN UNION ALL (SQLite, MySQL)
//   legalize() — mandatory legalizations, AFTER the optimizer — the
//                final word; nothing may rewrite the tree afterwards, so
//                "never illegal SQL" holds by construction:
//                - recursive-CTE marking → WITH RECURSIVE (all targets)
//                - bare JOIN (no ON) → CROSS JOIN / ON TRUE (postgres,
//                  duckdb, sqlserver)
//                - #<N in recursive members: total-cap LIMIT hoist
//                  (sqlite, mysql) or diagnostic (postgres, duckdb,
//                  sqlserver)

mod bare_join;
mod full_outer;
mod recursive_cte;

use crate::error::Result;
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::SqlStatement;

/// Expand SQL patterns the target cannot express natively. Runs BEFORE
/// the optimizer so cleanup can tidy the expansion's output.
pub fn rewrite(statement: SqlStatement, dialect: SqlDialect) -> Result<SqlStatement> {
    let stmt = if full_outer::needs_expansion(dialect) {
        full_outer::expand_full_outer_joins(statement)?
    } else {
        statement
    };

    Ok(stmt)
}

/// The final legalization word. Runs AFTER the optimizer; nothing may
/// rewrite the statement after this returns.
pub fn legalize(statement: SqlStatement, dialect: SqlDialect) -> Result<SqlStatement> {
    let mut stmt = statement;

    // Structural recursion detection first — the limit legalization only
    // inspects CTEs this pass has marked.
    recursive_cte::mark_recursive_ctes(&mut stmt);

    let mut stmt = if bare_join::needs_legalization(dialect) {
        bare_join::legalize_bare_joins(stmt)
    } else {
        stmt
    };

    recursive_cte::legalize_recursive_limits(&mut stmt, dialect)?;

    // The recursion validator (RECURSION-CONTRACT.md N1/N3/N4) — after the
    // limit legalization, so the one legal buried shape has already been
    // unwrapped before burial is judged.
    recursive_cte::validate_recursive_members(&mut stmt)?;

    Ok(stmt)
}
