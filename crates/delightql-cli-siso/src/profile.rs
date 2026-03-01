use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

/// Output format produced by the coprocess CLI.
#[derive(Debug, Clone)]
pub enum OutputFormat {
    Csv,
    Tsv,
}

/// How to discover tables and their columns from this backend.
#[derive(Debug, Clone)]
pub enum IntrospectionMode {
    /// No introspection available.
    None,
    /// Single query returns (table_name, table_type, cid, col_name, col_type, notnull).
    /// Uses pragma_table_info() join — one round-trip for all tables + columns.
    SingleQuery(String),
    /// Two-phase: discovery SQL lists table names (+ optional type column),
    /// then per-table `PRAGMA table_info(name)` for columns.
    TwoPhase {
        discovery_sql: String,
        has_type_column: bool,
    },
}

/// How to look up columns for a single table.
#[derive(Debug, Clone)]
pub enum SchemaMode {
    /// Use `PRAGMA table_info(table_name)` — works for SQLite, osquery.
    Pragma,
    /// Use a parameterized SQL query with `{table}` placeholder.
    /// Must return rows with columns: name, notnull (0/1), cid (position).
    Query(String),
}

/// How the target (e.g. database file path) is passed to the binary.
#[derive(Debug, Clone)]
pub enum TargetMode {
    /// No target argument (e.g. osqueryi).
    None,
    /// Target passed as positional argument.
    Positional,
    /// Target passed via a named flag.
    Flag(String),
}

/// Describes how to launch and communicate with a CLI database tool.
#[derive(Debug, Clone)]
pub struct PipeProfile {
    pub name: String,
    pub binary: String,
    pub target_mode: TargetMode,
    pub setup_commands: Vec<String>,
    pub output_format: OutputFormat,
    pub headers: bool,
    pub null_value: String,
    pub cli_flags: Vec<String>,
    pub introspection: IntrospectionMode,
    pub schema_mode: SchemaMode,
    pub env_vars: HashMap<String, String>,
}

/// Built-in osqueryi profile.
pub fn osqueryi_profile() -> PipeProfile {
    PipeProfile {
        name: "osqueryi".into(),
        binary: "osqueryi".into(),
        target_mode: TargetMode::None,
        setup_commands: vec![],
        output_format: OutputFormat::Csv,
        headers: true,
        null_value: String::new(),
        cli_flags: vec![
            "--csv".into(),
            "--separator".into(),
            ",".into(),
            "--header=true".into(),
        ],
        introspection: IntrospectionMode::TwoPhase {
            discovery_sql: "SELECT name FROM osquery_registry WHERE registry = 'table' AND active = 1".into(),
            has_type_column: false,
        },
        schema_mode: SchemaMode::Pragma,
        env_vars: HashMap::new(),
    }
}

/// Built-in sqlite3 profile.
pub fn sqlite3_profile() -> PipeProfile {
    PipeProfile {
        name: "sqlite3".into(),
        binary: "sqlite3".into(),
        target_mode: TargetMode::Positional,
        setup_commands: vec![
            ".mode csv".into(),
            ".headers on".into(),
            ".nullvalue NULL".into(),
        ],
        output_format: OutputFormat::Csv,
        headers: true,
        null_value: "NULL".into(),
        cli_flags: vec![],
        introspection: IntrospectionMode::SingleQuery(
            "SELECT s.name, s.type, p.cid, p.name, p.type, p.\"notnull\" \
             FROM sqlite_master s, pragma_table_info(s.name) p \
             WHERE s.type IN ('table','view') AND s.name NOT LIKE 'sqlite_%' \
             ORDER BY s.name, p.cid".into()
        ),
        schema_mode: SchemaMode::Pragma,
        env_vars: HashMap::new(),
    }
}

/// Built-in duckdb profile.
pub fn duckdb_profile() -> PipeProfile {
    PipeProfile {
        name: "duckdb".into(),
        binary: "duckdb".into(),
        target_mode: TargetMode::Positional,
        setup_commands: vec![
            ".nullvalue NULL".into(),
        ],
        output_format: OutputFormat::Csv,
        headers: true,
        null_value: "NULL".into(),
        cli_flags: vec!["-csv".into(), "-interactive".into()],
        introspection: IntrospectionMode::SingleQuery(
            "SELECT t.table_name, t.table_type, c.ordinal_position - 1 AS cid, \
             c.column_name, c.data_type, \
             CASE WHEN c.is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull \
             FROM information_schema.tables t \
             JOIN information_schema.columns c \
               ON t.table_catalog = c.table_catalog \
              AND t.table_schema = c.table_schema \
              AND t.table_name = c.table_name \
             WHERE t.table_schema = 'main' \
             ORDER BY t.table_name, c.ordinal_position".into()
        ),
        schema_mode: SchemaMode::Query(
            "SELECT c.column_name AS name, \
             CASE WHEN c.is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull, \
             c.ordinal_position - 1 AS cid \
             FROM information_schema.columns c \
             WHERE c.table_schema = 'main' AND c.table_name = '{table}' \
             ORDER BY c.ordinal_position".into()
        ),
        env_vars: HashMap::new(),
    }
}

/// Built-in snowflake profile (via dql-snowflake-bridge Python coprocess).
pub fn snowflake_profile() -> PipeProfile {
    PipeProfile {
        name: "snowflake".into(),
        binary: "dql-snowflake-bridge".into(),
        target_mode: TargetMode::None,
        setup_commands: vec![],
        output_format: OutputFormat::Csv,
        headers: true,
        null_value: "NULL".into(),
        cli_flags: vec![],
        introspection: IntrospectionMode::SingleQuery(
            "SELECT t.table_name, t.table_type, \
             c.ordinal_position - 1 AS cid, \
             c.column_name, c.data_type, \
             CASE WHEN c.is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull \
             FROM information_schema.tables t \
             JOIN information_schema.columns c \
               ON t.table_catalog = c.table_catalog \
              AND t.table_schema = c.table_schema \
              AND t.table_name = c.table_name \
             WHERE t.table_schema = CURRENT_SCHEMA() \
             ORDER BY t.table_name, c.ordinal_position".into()
        ),
        schema_mode: SchemaMode::Query(
            "SELECT c.column_name AS name, \
             CASE WHEN c.is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull, \
             c.ordinal_position - 1 AS cid \
             FROM information_schema.columns c \
             WHERE c.table_schema = CURRENT_SCHEMA() \
               AND c.table_name = '{table}' \
             ORDER BY c.ordinal_position".into()
        ),
        env_vars: HashMap::new(),
    }
}

/// Look up a built-in profile by name.
pub fn builtin_profile(name: &str) -> Option<PipeProfile> {
    match name {
        "osqueryi" => Some(osqueryi_profile()),
        "sqlite3" => Some(sqlite3_profile()),
        "duckdb" => Some(duckdb_profile()),
        "snowflake" => Some(snowflake_profile()),
        _ => None,
    }
}

// --- TOML profile loading ---

#[derive(Deserialize)]
struct TomlProfileFile {
    pipe: Option<TomlPipe>,
}

#[derive(Deserialize)]
struct TomlPipe {
    binary: Option<String>,
    target_mode: Option<String>,
    target_flag: Option<String>,
    cli_flags: Option<Vec<String>>,
    setup: Option<TomlSetup>,
    output: Option<TomlOutput>,
    env: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
struct TomlSetup {
    commands: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct TomlOutput {
    format: Option<String>,
    headers: Option<bool>,
    null_value: Option<String>,
}

/// Config directory for user pipe profiles: `~/.config/delightql/pipes/`
fn config_profile_path(name: &str) -> Option<PathBuf> {
    let proj = directories::ProjectDirs::from("", "", "delightql")?;
    Some(proj.config_dir().join("pipes").join(format!("{}.toml", name)))
}

/// Expand a leading `~/` to the user's home directory.
fn expand_tilde(value: &str) -> String {
    if let Some(rest) = value.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest).to_string_lossy().into_owned();
        }
    }
    value.to_string()
}

fn expand_env_values(env: HashMap<String, String>) -> HashMap<String, String> {
    env.into_iter()
        .map(|(k, v)| (k, expand_tilde(&v)))
        .collect()
}

/// Build a `PipeProfile` from a parsed TOML file.
fn profile_from_toml(name: &str, file: TomlProfileFile) -> Option<PipeProfile> {
    let pipe = file.pipe?;
    let binary = pipe.binary?;

    let target_mode = match pipe.target_mode.as_deref() {
        Some("positional") => TargetMode::Positional,
        Some("flag") => TargetMode::Flag(pipe.target_flag.unwrap_or_default()),
        _ => TargetMode::None,
    };

    let output = pipe.output.unwrap_or(TomlOutput {
        format: None,
        headers: None,
        null_value: None,
    });

    let output_format = match output.format.as_deref() {
        Some("tsv") => OutputFormat::Tsv,
        _ => OutputFormat::Csv,
    };

    Some(PipeProfile {
        name: name.to_string(),
        binary,
        target_mode,
        setup_commands: pipe
            .setup
            .and_then(|s| s.commands)
            .unwrap_or_default(),
        output_format,
        headers: output.headers.unwrap_or(true),
        null_value: output.null_value.unwrap_or_else(|| "NULL".into()),
        cli_flags: pipe.cli_flags.unwrap_or_default(),
        introspection: IntrospectionMode::None,
        schema_mode: SchemaMode::Pragma,
        env_vars: expand_env_values(pipe.env.unwrap_or_default()),
    })
}

/// Resolve a pipe profile by name.
///
/// Resolution order:
/// 1. Built-in profile (hardcoded Rust)
/// 2. User config file: `~/.config/delightql/pipes/{name}.toml`
///
/// If both exist, the TOML `[env]` section is merged onto the built-in profile.
/// If only the TOML exists, a profile is built from it (no introspection support).
pub fn resolve_profile(name: &str) -> Option<PipeProfile> {
    let builtin = builtin_profile(name);
    let toml_file = config_profile_path(name).and_then(|p| {
        let contents = std::fs::read_to_string(&p).ok()?;
        let parsed: TomlProfileFile = toml::from_str(&contents).ok()?;
        Some(parsed)
    });

    match (builtin, toml_file) {
        (Some(mut profile), Some(file)) => {
            // Merge env vars from TOML onto the built-in profile
            if let Some(env) = file.pipe.and_then(|p| p.env) {
                profile.env_vars = expand_env_values(env);
            }
            Some(profile)
        }
        (Some(profile), None) => Some(profile),
        (None, Some(file)) => profile_from_toml(name, file),
        (None, None) => None,
    }
}
