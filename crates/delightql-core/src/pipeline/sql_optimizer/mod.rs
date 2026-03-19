// sql_optimizer/mod.rs - SQL-level optimization pass
//
// Post-order (bottom-up) walker over SQL AST v3.
// Each rewrite rule only inspects its immediate child — deeper
// nodes are already optimized by the time the parent is visited.
//
// Current passes:
// - Cleanup (Basic): redundant subquery elimination
//
// See OPTIMIZER.md for the theory (epistemological barrier / grammar barrier).

mod cleanup;
mod visitor;

use crate::error::Result;
use crate::pipeline::sql_ast_v3::SqlStatement;

use cleanup::pass_cleanup;

/// Optimization level controls which passes are applied
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum OptimizationLevel {
    /// No optimization - identity pass
    #[default]
    None,
    /// Basic cleanup - redundant subquery elimination only
    Basic,
}

/// Read the optimization level from the DQL_SOPTIMIZE environment variable.
/// Falls back to `Basic` if unset or unparseable.
pub fn level_from_env() -> OptimizationLevel {
    match std::env::var("DQL_SOPTIMIZE").as_deref() {
        Ok("0") => OptimizationLevel::None,
        Ok("1") => OptimizationLevel::Basic,
        _ => OptimizationLevel::Basic,
    }
}

/// Main entry point for SQL optimization
/// Takes a SQL AST v3 and returns an optimized version
pub fn optimize(statement: SqlStatement, level: OptimizationLevel) -> Result<SqlStatement> {
    log::debug!("SQL Optimizer: Starting with level {:?}", level);

    if matches!(level, OptimizationLevel::None) {
        log::debug!("SQL Optimizer: No optimization requested, returning unchanged");
        return Ok(statement);
    }

    // PASS 1: Cleanup (Level >= Basic)
    let stmt = if level >= OptimizationLevel::Basic {
        log::debug!("SQL Optimizer: Running cleanup pass");
        pass_cleanup(statement)?
    } else {
        statement
    };

    log::debug!("SQL Optimizer: Complete");
    Ok(stmt)
}
