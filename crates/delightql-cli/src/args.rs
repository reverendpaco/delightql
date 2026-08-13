// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::output_format::OutputFormat;
/// Command-line argument parsing for DelightQL CLI
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "delightql",
    version = delightql_buildinfo::human_static(),
    about = "DelightQL - Query language transpiler",
    long_about = None,
    disable_help_subcommand = true,
    after_help = "EXAMPLES:\n  \
dql query --db app.db 'users(*), age > 30'\n  \
dql query --db app.db 'users(_, name, age)'          # positional pattern: one slot per column, _ discards\n  \
dql query --db app.db 'users(uid, name, _), orders(_, uid, total) |> %(name ~> sum:(total) as spent)'\n  \
echo 'users(*) ~> count:(*)' | dql query --db app.db\n  \
dql query --db app.db --to sql -f raw 'users(*)'\n  \
dql explain semantic/cast\n\n\
The pipe is |> (feeds a relation into %()/#()/+() operators); ~> is the\n\
aggregate separator, not the pipe. A shared bare variable (uid above) IS\n\
the join. Depth lives in the man pages: man dql, man dql-query."
)]
pub struct CliArgs {
    /// Subcommand to execute (if omitted and no flags, starts REPL)
    #[command(subcommand)]
    pub command: Option<Command>,

    // Consumer lists in the doc one-liners below mirror the refusal
    // matrix in main.rs: clap propagates global flags into every
    // subcommand's --help, including subcommands that refuse them —
    // so each flag's own text must say where it works.
    /// SQLite database file (query, server, book; others refuse it)
    #[arg(long = "db", value_name = "DATABASE", global = true)]
    pub database: Option<PathBuf>,

    /// With --db: create the database file if missing (query only)
    #[arg(long = "make-new-db-if-missing", global = true, requires = "database")]
    pub make_new_db_if_missing: bool,

    /// Prefix for structured error records on stderr (default: ASCII RS; see dql(1))
    #[arg(long, global = true, default_value = "\x1E")]
    pub error_prefix: String,

    /// Error record body format on stderr: text or json
    #[arg(long, global = true, value_enum, default_value = "text")]
    pub error_format: ErrorFormat,

    /// Target SQL dialect: sqlite (default), postgres, mysql, sqlserver, duckdb (query, tools, server)
    #[arg(long, global = true, value_name = "DIALECT", value_parser = parse_dialect)]
    pub dialect: Option<String>,

    /// Mechanism for reaching a postgres resource: fatboy (default) or siso (query only)
    #[arg(long, global = true, value_name = "MECHANISM")]
    pub via: Option<String>,
}

/// Subcommands for DelightQL CLI
#[derive(Subcommand)]
pub enum Command {
    /// Print build identity (machine-readable with --json)
    Version {
        /// Emit as a single-line JSON object
        #[arg(long)]
        json: bool,
    },

    /// Execute a query (from string, file, or stdin)
    #[command(visible_alias = "q")]
    Query {
        /// Query string (if omitted: reads stdin, or starts the REPL on a TTY)
        query: Option<String>,

        /// Read the query from FILE ('-' means stdin)
        #[arg(long, conflicts_with = "query")]
        file: Option<PathBuf>,

        /// Stop at intermediate stage for inspection
        #[arg(long, value_enum)]
        to: Option<Stage>,

        /// Output format (table, box, json, jsonl, csv, tsv, list, raw)
        #[arg(short = 'f', long, value_parser = parse_output_format)]
        format: Option<OutputFormat>,

        /// Debug options (comma-separated)
        #[arg(long)]
        debug: Option<String>,

        /// Suppress headers in results
        #[arg(long, short = 'n')]
        no_headers: bool,

        /// Disable output sanitization (allows raw terminal control sequences)
        #[arg(long)]
        no_sanitize: bool,

        /// Suppress REPL banner and meta output (interactive mode only)
        #[arg(long, short = 'q')]
        quiet: bool,

        /// Attach external database
        #[arg(long = "attach")]
        attach: Vec<String>,

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

        /// Open a danger gate (hierarchy=STATE, e.g. cardinality/cartesian=ON); repeatable
        #[arg(long = "danger")]
        dangers: Vec<String>,

        /// Set a config for this session (hierarchy=STATE); repeatable
        #[arg(long = "config")]
        options: Vec<String>,
    },

    /// Format/prettify DelightQL code
    Format {
        /// Source code or file path (if omitted, reads from stdin)
        source: Option<String>,

        /// Style bundle from sys::format.bundle (e.g. "book"); a
        /// .dql-format file still overrides individual knobs
        #[arg(long)]
        style: Option<String>,

        /// Use colored output (always, auto, never)
        #[arg(long, default_value = "auto")]
        color: ColorMode,

        /// Print nothing; exit 0 if already formatted, 1 if not,
        /// 2 if the formatter cannot determine (parse error, unhandled
        /// construct). For CI enforcement.
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

    /// Show a manual page (tokens hyphen-join: `dql man dql target` = dql-target(1))
    Man {
        /// Optional leading section number, then name tokens
        name: Vec<String>,

        /// Write every embedded troff page into DIR (release staging)
        #[arg(long, value_name = "DIR")]
        dump: Option<PathBuf>,
    },

    /// Emit an embedded documentation book as Markdown (bare: list books)
    Book {
        /// Book name (currently: reference)
        name: Option<String>,
        /// Also write the bundle's images into DIR (default: images/) so
        /// the emitted markdown's relative references resolve for pandoc
        #[arg(long, value_name = "DIR", num_args = 0..=1, default_missing_value = "images")]
        export_images: Option<PathBuf>,
    },

    /// Editor support: export the embedded grammar, highlight queries, and compiled parser
    Editor {
        #[command(subcommand)]
        action: EditorCommand,
    },

    /// Show a command's manual page (`dql help query` = `dql man query`; bare = usage)
    Help {
        /// Command name tokens (same grammar as `dql man`)
        name: Vec<String>,
    },

    /// Generate shell completions to stdout
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },

    /// Run self-diagnostics (checks dql's own health)
    Selftest {
        /// Emit findings as a JSON array (machine-readable, for CI)
        #[arg(long)]
        json: bool,

        /// Treat warnings as failures (exit nonzero on any WARN)
        #[arg(long)]
        strict: bool,
    },

    /// Explain a DelightQL identifier (error, danger gate, or config)
    Explain {
        /// The identifier to explain
        identifier: String,
    },

    /// Manage target adapters (the per-engine fatboy binaries)
    Target {
        #[command(subcommand)]
        action: TargetCommand,
    },

    /// Start the internal relay-protocol server on a Unix socket (no compatibility promise; see man dql-server)
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

        /// Shut down after N seconds with zero active connections (0 = disabled)
        #[arg(long, default_value = "0")]
        socket_idle_timeout: u64,
    },
}

/// Subcommands under `dql editor`
#[derive(Subcommand)]
pub enum EditorCommand {
    /// Write the editor-support artifacts into DIR: parser/delightql.so +
    /// queries/delightql/*.scm (an editor runtimepath) and grammar/
    /// (a rebuildable tree-sitter project)
    ExportArtifacts {
        /// Output directory (created if absent; existing files overwritten)
        #[arg(long, value_name = "DIR", default_value = ".")]
        dir: PathBuf,
    },
}

/// Subcommands under `dql target`
///
/// `install` and `verify` wait on the release pipeline: nothing
/// published to fetch, no digests to check against yet.
#[derive(Subcommand)]
pub enum TargetCommand {
    /// Show each known adapter and where it resolves from
    #[command(visible_alias = "ls")]
    List,

    /// Install an adapter into the adapter store (digest-verified)
    Install {
        /// Adapter profile (e.g. postgres, duckdb)
        profile: String,

        /// Local directory holding the adapter binary (see dql-target(1))
        #[arg(long)]
        from: Option<PathBuf>,
    },

    /// Re-hash installed adapters against this dql's burned digests
    Verify,
}

/// Subcommands under `dql tools`
#[derive(Subcommand)]
pub enum ToolCommand {
    /// JSON destructuring from stdin
    #[command(visible_alias = "j")]
    Jstruct {
        /// DQL query to run against j(j TEXT)
        query: String,

        /// Output format (table, box, json, jsonl, csv, tsv, list, raw)
        #[arg(short = 'f', long, value_parser = parse_output_format)]
        format: Option<OutputFormat>,

        /// Stop at intermediate stage for inspection
        #[arg(long, value_enum)]
        to: Option<Stage>,
    },

    /// CSV destructuring from stdin
    #[command(visible_alias = "c")]
    Csvstruct {
        /// DQL query to run against c(...)
        query: String,

        /// Output format (table, box, json, jsonl, csv, tsv, list, raw)
        #[arg(short = 'f', long, value_parser = parse_output_format)]
        format: Option<OutputFormat>,

        /// Stop at intermediate stage for inspection
        #[arg(long, value_enum)]
        to: Option<Stage>,

        /// First row is column headers
        #[arg(long)]
        has_headers: bool,

        /// Field delimiter (default: comma)
        #[arg(long, default_value = ",")]
        delimiter: String,
    },

    /// Multi-source file munging (load tables from files, then query)
    #[command(visible_alias = "m")]
    Filemunge {
        /// DQL query to run against loaded tables
        query: String,

        /// Table spec: name:format[:noheader] path
        /// Formats: csv, tsv, json-singleton
        /// csv/tsv default to header; add :noheader to override
        #[arg(long = "table", num_args = 2, value_names = ["SPEC", "PATH"])]
        tables: Vec<String>,

        /// Output format (table, box, json, jsonl, csv, tsv, list, raw)
        #[arg(short = 'f', long, value_parser = parse_output_format)]
        format: Option<OutputFormat>,

        /// Stop at intermediate stage for inspection
        #[arg(long, value_enum)]
        to: Option<Stage>,
    },
}

/// Body format for structured error records. With the default RS
/// prefix, json mode is exactly RFC 7464 (JSON text sequences —
/// what `jq --seq` reads): RS + JSON + LF.
#[derive(Debug, Clone, Copy, PartialEq, ValueEnum)]
pub enum ErrorFormat {
    /// `[uri] message` / `Error: message`
    Text,
    /// `{"uri": ..., "message": ...}` (uri null when unbadged)
    Json,
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
    /// Show byte-level data hash (type-preserving, platform-independent)
    #[value(name = "bhash")]
    ByteHash,
    /// Show just the total hash as plain text (includes column names)
    #[value(name = "totalhash")]
    TotalHash,
}

/// Eager --dialect validation, mirroring --format's contract: a bogus
/// value is a hard usage error before anything runs, never a lazy
/// warning downstream. Accepts
/// exactly what `SqlDialect::from_family_name` accepts, aliases included.
fn parse_dialect(s: &str) -> Result<String, String> {
    if delightql_core::is_known_dialect_family(s.trim()) {
        Ok(s.trim().to_string())
    } else {
        Err(format!(
            "unknown dialect '{}'. Valid dialects: sqlite, postgres (alias: postgresql), \
             mysql, sqlserver, duckdb",
            s
        ))
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consult_flag_is_removed() {
        // --consult does not exist: consult!() in DQL source is the one
        // spelling. The flag must not quietly return as an alias.
        let parsed = CliArgs::try_parse_from(["dql", "query", "--consult", "x.dql", "users(*)"]);
        assert!(parsed.is_err(), "--consult must be an unknown flag");
    }

    #[test]
    fn sink_flag_is_removed() {
        // Emit is a silent reserve: no CLI surface may advertise or
        // accept a sink binding. Row fan-out belongs to the effect
        // algebra (stdout!, sink!) when it lands.
        let parsed = CliArgs::try_parse_from(["dql", "query", "--sink", "a=b.csv", "users(*)"]);
        assert!(parsed.is_err(), "--sink must be an unknown flag");
    }
}
