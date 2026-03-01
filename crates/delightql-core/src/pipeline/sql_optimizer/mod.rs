// sql_optimizer/mod.rs - SQL-level optimization pass
//
// This module takes SQL AST v3 and applies various optimizations:
// - Redundant subquery elimination (PASS 1 - BASIC)
// - CTE extraction from deeply nested subqueries (PASS 2 - MODERATE)
// - Predicate pushdown (PASS 2 - MODERATE)
// - Boolean algebra simplification (PASS 3 - AGGRESSIVE)
//
// See DESIGN.md for full architecture and implementation details

mod advanced;
mod boolean_simplification;
mod cleanup;
mod restructure;
mod visitor;

use crate::error::Result;
use crate::pipeline::sql_ast_v3::SqlStatement;

// Re-export the pass functions for internal use
use advanced::pass_advanced;
use cleanup::pass_cleanup;
use restructure::pass_restructure;

/// Optimization level controls which passes are applied
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum OptimizationLevel {
    /// No optimization - identity pass
    #[default]
    None,
    /// Basic cleanup - redundant subquery elimination only
    Basic,
    /// Moderate - Basic + projection flattening (restructuring pass)
    /// Note: Partially implemented. CTE extraction and predicate pushdown are reserved for future use.
    Moderate,
    /// Aggressive - Moderate + boolean simplification and advanced optimizations
    /// Note: Currently available but not used in production. Reserved for future use.
    Aggressive,
}

/// Main entry point for SQL optimization
/// Takes a SQL AST v3 and returns an optimized version
pub fn optimize(statement: SqlStatement, level: OptimizationLevel) -> Result<SqlStatement> {
    log::debug!("SQL Optimizer: Starting with level {:?}", level);

    // Level 0: No optimization
    if matches!(level, OptimizationLevel::None) {
        log::debug!("SQL Optimizer: No optimization requested, returning unchanged");
        return Ok(statement);
    }

    let stmt = statement;

    // PASS 1: Cleanup (Level >= Basic)
    let stmt = if level >= OptimizationLevel::Basic {
        log::debug!("SQL Optimizer: Running PASS 1 (Cleanup)");
        let result = pass_cleanup(stmt)?;
        log::debug!("SQL Optimizer: PASS 1 complete");
        result
    } else {
        stmt
    };

    // PASS 2: Restructuring (Level >= Moderate)
    let stmt = if level >= OptimizationLevel::Moderate {
        log::debug!("SQL Optimizer: Running PASS 2 (Restructuring)");
        pass_restructure(stmt)?
    } else {
        stmt
    };

    // PASS 3: Advanced (Level >= Aggressive)
    let stmt = if level >= OptimizationLevel::Aggressive {
        log::debug!("SQL Optimizer: Running PASS 3 (Advanced)");
        pass_advanced(stmt)?
    } else {
        stmt
    };

    log::debug!("SQL Optimizer: Complete");
    Ok(stmt)
}
