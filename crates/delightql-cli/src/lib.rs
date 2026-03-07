/// DelightQL CLI library
///
/// Exposes shared modules for use by binary targets
// Core modules (always available)
pub mod args;
pub mod attach;
pub mod bug_report;
pub mod commands;
pub mod connection;
pub mod connection_factory;
pub mod exec;
pub mod exec_ng;
pub mod file_inputs; // File input tracking with auto-numbering
pub mod modifiers; // Modifier parsing for CSV/TSV files
pub mod output_format;
pub mod pipe_exec;
pub mod sanitize;
pub mod server;
pub mod theme;
pub mod util;
pub mod version_info;

// Formatter is now in the delightql-formatter crate

// REPL module (only available with 'repl' feature)
#[cfg(feature = "repl")]
pub mod repl;
