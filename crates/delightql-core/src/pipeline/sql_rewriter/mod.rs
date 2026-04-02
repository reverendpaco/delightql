// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// sql_rewriter/mod.rs — Dialect-aware SQL AST rewriting
//
// Runs AFTER the transformer and BEFORE the optimizer.
// Rewrites SQL patterns that the target dialect cannot express natively.
//
// Current rewrites:
// - FULL OUTER JOIN → LEFT JOIN UNION ALL (for SQLite, MySQL)

mod full_outer;

use crate::error::Result;
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::SqlStatement;

/// Rewrite a SQL statement for the target dialect.
pub fn rewrite(statement: SqlStatement, dialect: SqlDialect) -> Result<SqlStatement> {
    let stmt = if full_outer::needs_expansion(dialect) {
        full_outer::expand_full_outer_joins(statement)?
    } else {
        statement
    };

    Ok(stmt)
}
