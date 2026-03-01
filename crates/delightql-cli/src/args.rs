use crate::output_format::OutputFormat;
/// Command-line argument parsing for DelightQL CLI
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "delightql",
    version,
    about = "DelightQL - Query language transpiler",
    long_about = None
)]
pub struct CliArgs {
    /// Subcommand to execute (if omitted and no flags, starts REPL)
    #[command(subcommand)]
    pub command: Option<Command>,

    /// SQLite database file to use (global option)
    #[arg(long = "db", value_name = "DATABASE", global = true)]
    pub database: Option<PathBuf>,

    /// Prefix for structured error lines on stderr (default: ASCII RS \x1E).
    /// Machine-parseable error records are emitted as: <prefix>[uri] message.
    /// Set to any string for custom scripting (e.g. --error-prefix '@error ').
    #[arg(long, global = true, default_value = "\x1E")]
    pub error_prefix: String,
}

/// Subcommands for DelightQL CLI
#[derive(Subcommand)]
pub enum Command {
    /// Execute a query (from string, file, or stdin)
    #[command(visible_alias = "q")]
    Query {
        /// Query string to execute (if omitted, reads from stdin or starts REPL)
        query: Option<String>,

        /// Read query from file
        #[arg(long, conflicts_with = "query")]
        file: Option<PathBuf>,

        /// Stop at intermediate stage for inspection
        #[arg(long, value_enum)]
        to: Option<Stage>,

        /// Output format (table, json, csv, tsv, list)
        #[arg(short = 'f', long, value_parser = parse_output_format)]
        format: Option<OutputFormat>,

        /// Assertion queries to run after main query
        #[arg(long = "assert")]
        assert_queries: Vec<String>,

        /// Format errors with DelightQL query
        #[arg(long = "if-errors")]
        if_errors_query: Option<String>,

        /// Debug options (comma-separated)
        #[arg(long)]
        debug: Option<String>,

        /// SQL optimization level (0-3)
        #[arg(long = "soptimize", default_value = "0")]
        sql_optimize: u8,

        /// Inline CTEs as subqueries
        #[arg(long)]
        inline_ctes: bool,

        /// Suppress headers in results
        #[arg(long, short = 'n')]
        no_headers: bool,

        /// Disable output sanitization (allows raw terminal control sequences)
        #[arg(long)]
        no_sanitize: bool,

        /// Strict validation mode
        #[arg(long)]
        strict: bool,

        /// Quiet mode
        #[arg(long, short = 'q')]
        quiet: bool,

        /// Create new database if missing
        #[arg(long = "make-new-db-if-missing")]
        make_new_db_if_missing: bool,

        /// Consult DDL file(s)
        #[arg(long = "consult")]
        consult_files: Vec<PathBuf>,

        /// Attach external database
        #[arg(long = "attach")]
        attach: Vec<String>,

        /// Interactive mode - allow dot commands
        #[cfg(feature = "repl")]
        #[arg(long, short = 'i')]
        interactive: bool,

        /// Verbose mode
        #[arg(long)]
        verbose: bool,

        /// Path to highlights.scm file
        #[cfg(feature = "repl")]
        #[arg(long)]
        highlights: Option<PathBuf>,

        /// Path to theme file
        #[cfg(feature = "repl")]
        #[arg(long)]
        theme: Option<PathBuf>,

        /// Execute multiple queries sequentially (for files with multiple queries)
        #[arg(long)]
        sequential: bool,

        /// Bind emit streams to sink destinations (format: name=path)
        ///
        /// Routes named emit streams to file sinks. Unbound streams
        /// fall back to RS-prefixed records on stderr.
        /// Example: --sink young=./young.jsonl --sink old=./old.jsonl
        #[arg(long = "sink")]
        sinks: Vec<String>,

        /// Open danger gates for this session (format: uri=STATE)
        ///
        /// Override default danger gate states. STATE is ON, OFF, ALLOW, or 1-9.
        /// Example: --danger dql/cardinality/nulljoin=ON
        #[arg(long = "danger")]
        dangers: Vec<String>,

        /// Set options for this session (format: uri=STATE)
        ///
        /// Override default option states. STATE is ON, OFF, ALLOW, or 1-9.
        /// Example: --option generation/rule/inlining/view=ON
        #[arg(long = "option")]
        options: Vec<String>,
    },

    /// Read data FROM stdin and query it (JSON, CSV, etc.)
    From {
        /// Format of stdin data (json, jsonl, csv, etc.)
        format: String,

        /// Query to run on ingested data (accesses special.input table)
        query: Option<String>,

        /// Read query from file
        #[arg(long, conflicts_with = "query")]
        file: Option<PathBuf>,

        /// Output format
        #[arg(short = 'f', long, value_parser = parse_output_format)]
        format_out: Option<OutputFormat>,

        /// Stop at intermediate stage
        #[arg(long, value_enum)]
        to: Option<Stage>,

        /// Assertion queries
        #[arg(long = "assert")]
        assert_queries: Vec<String>,

        /// Format errors with DelightQL query
        #[arg(long = "if-errors")]
        if_errors_query: Option<String>,

        /// Debug options
        #[arg(long)]
        debug: Option<String>,

        /// SQL optimization level
        #[arg(long = "soptimize", default_value = "0")]
        sql_optimize: u8,

        /// Inline CTEs
        #[arg(long)]
        inline_ctes: bool,

        /// Suppress headers
        #[arg(long, short = 'n')]
        no_headers: bool,

        /// Disable output sanitization (allows raw terminal control sequences)
        #[arg(long)]
        no_sanitize: bool,

        /// Strict mode
        #[arg(long)]
        strict: bool,

        /// Quiet mode
        #[arg(long, short = 'q')]
        quiet: bool,

        /// Create new database if missing
        #[arg(long = "make-new-db-if-missing")]
        make_new_db_if_missing: bool,

        /// Consult DDL file(s)
        #[arg(long = "consult")]
        consult_files: Vec<PathBuf>,

        /// Attach external database
        #[arg(long = "attach")]
        attach: Vec<String>,
    },

    /// Format/prettify DelightQL code
    Format {
        /// Source code or file path (if omitted, reads from stdin)
        source: Option<String>,

        /// Use colored output (always, auto, never)
        #[arg(long, default_value = "auto")]
        color: ColorMode,

        /// Exit 1 if input is not already formatted (for CI enforcement)
        #[arg(long)]
        fail_if_not_formatted: bool,

        /// Path to highlights.scm file
        #[cfg(feature = "repl")]
        #[arg(long)]
        highlights: Option<PathBuf>,

        /// Path to theme file
        #[cfg(feature = "repl")]
        #[arg(long)]
        theme: Option<PathBuf>,
    },

    /// Tools for ad-hoc data munging and manipulation
    #[command(visible_alias = "t")]
    Tools {
        #[command(subcommand)]
        tool: ToolCommand,
    },

    /// Start relay protocol server on a Unix socket
    Server {
        /// Unix socket path (default: /tmp/dql-{pid}.sock)
        #[arg(long)]
        socket: Option<std::path::PathBuf>,

        /// Number of worker threads (default: available CPUs)
        #[arg(long, default_value = "0")]
        workers: usize,

        /// Shut down after N seconds of no messages (0 = disabled)
        #[arg(long, default_value = "0")]
        idle_timeout: u64,
    },
}

/// Subcommands under `dql tools`
#[derive(Subcommand)]
pub enum ToolCommand {
    /// JSON destructuring from stdin
    #[command(visible_alias = "j")]
    Jstruct {
        /// DQL query to run against j(j TEXT)
        query: String,

        /// Output format (table, json, csv, tsv)
        #[arg(short = 'f', long, value_parser = parse_output_format)]
        format: Option<OutputFormat>,

        /// Stop at intermediate stage for inspection
        #[arg(long, value_enum)]
        to: Option<Stage>,
    },

    /// Multi-source relational munging (not yet implemented)
    #[allow(dead_code)]
    Munge {
        // Placeholder for future implementation
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ColorMode {
    /// Always use colors
    Always,
    /// Auto-detect based on terminal (default)
    Auto,
    /// Never use colors
    Never,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Stage {
    /// Show CST (Concrete Syntax Tree)
    Cst,
    /// Show Unresolved AST
    #[value(name = "ast-unresolved")]
    AstUnresolved,
    /// Show Resolved AST
    #[value(name = "ast-resolved")]
    AstResolved,
    /// Show Refined AST
    #[value(name = "ast-refined")]
    AstRefined,
    /// Show SQL AST
    #[value(name = "ast-sql")]
    AstSql,
    /// Show generated SQL
    Sql,
    /// Execute query and show results (default)
    Results,
    /// Show result fingerprint JSON for semantic comparison
    Fingerprint,
    /// Show just the data hash as plain text (column-name independent)
    Hash,
    /// Show just the total hash as plain text (includes column names)
    #[value(name = "totalhash")]
    TotalHash,
    /// Deprecated: use sys::execution.stack(*) instead
    #[value(name = "recursion-depth")]
    RecursionDepth,
}

fn parse_output_format(s: &str) -> Result<OutputFormat, String> {
    OutputFormat::from_str(s).ok_or_else(|| {
        format!(
            "Invalid format '{}'. Available formats: {}",
            s,
            OutputFormat::all_formats().join(", ")
        )
    })
}

/// Debug options parsed from --debug flag
#[derive(Debug, Clone, Default)]
pub struct DebugOptions {
    pub features: bool,
    pub timing: bool,
}

impl DebugOptions {
    /// Parse debug options from a comma-separated string like "+features,+timing"
    pub fn from_str(s: &str) -> Self {
        let mut opts = DebugOptions::default();

        for part in s.split(',') {
            let part = part.trim();
            match part {
                "+features" => opts.features = true,
                "+timing" => opts.timing = true,
                _ if part.starts_with('+') => {
                    eprintln!("Warning: Unknown debug option: {}", part);
                }
                _ => {}
            }
        }

        opts
    }
}

