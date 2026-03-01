//! Command handlers for DelightQL CLI
//!
//! This module organizes the CLI into distinct subcommands:
//! - query: Execute queries (string/file/stdin/REPL)
//! - from: Ingest data from stdin and query it
//! - format: Format/prettify DelightQL code
//! - jstruct: JSON destructuring from stdin (dql tools jstruct)

pub mod delegate;
pub mod format;
pub mod from;
pub mod jstruct;
pub mod query;
