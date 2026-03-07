//! Command handlers for DelightQL CLI
//!
//! This module organizes the CLI into distinct subcommands:
//! - query: Execute queries (string/file/stdin/REPL)
//! - format: Format/prettify DelightQL code
//! - jstruct: JSON destructuring from stdin (dql tools jstruct)

pub mod csvstruct;
pub mod delegate;
pub mod filemunge;
pub mod format;
pub mod jstruct;
pub mod query;
