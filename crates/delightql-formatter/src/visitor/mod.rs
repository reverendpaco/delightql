/// Tree visitor for formatting DelightQL queries
///
/// This module is organized into logical submodules:
/// - core: Main Formatter struct and basic formatting logic
/// - helpers: Utility methods for finding nodes and text manipulation
/// - base: Base expressions (tables, TVF calls, anonymous tables)
/// - domain: Domain expressions and arithmetic operations
/// - pipes: Pipe operators and relational continuations
/// - operations: Pipe operations (projections, filters, grouping, etc.)
/// - continuations: Binary operators and comma continuations
/// - case: CASE expression formatting
/// - cte: CTE binding formatting
mod base;
mod case;
mod continuations;
mod core;
mod cte;
mod domain;
mod helpers;
mod hooks;
mod operations;
mod pipes;

// Re-export the main Formatter struct
pub use core::Formatter;
