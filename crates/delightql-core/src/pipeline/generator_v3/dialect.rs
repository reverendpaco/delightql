// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[allow(dead_code)]
pub enum SqlDialect {
    #[default]
    SQLite,
    PostgreSQL,
    MySQL,
    SqlServer,
    DuckDB,
}

impl SqlDialect {
    /// Dialect family key as spelled in the targeting tables and
    /// `language.dialect` (`dialect_render.dialect` etc.).
    pub fn family_name(self) -> &'static str {
        match self {
            SqlDialect::SQLite => "sqlite",
            SqlDialect::PostgreSQL => "postgres",
            SqlDialect::MySQL => "mysql",
            SqlDialect::SqlServer => "sqlserver",
            SqlDialect::DuckDB => "duckdb",
        }
    }

    /// Parse a dialect family key (the `family_name` spelling).
    pub fn from_family_name(name: &str) -> Option<Self> {
        match name {
            "sqlite" => Some(SqlDialect::SQLite),
            "postgres" | "postgresql" => Some(SqlDialect::PostgreSQL),
            "mysql" => Some(SqlDialect::MySQL),
            "sqlserver" => Some(SqlDialect::SqlServer),
            "duckdb" => Some(SqlDialect::DuckDB),
            _ => None,
        }
    }

    /// Explicit dialect OVERRIDE from the `DQL_DIALECT` env var (the CLI's
    /// global `--dialect` flag sets it). `None` = no override: the pipeline
    /// derives the dialect from the connection the query routes to
    /// (`DelightQLSystem::dialect_for_connection`), defaulting to SQLite.
    pub fn override_from_env() -> Option<Self> {
        match std::env::var("DQL_DIALECT") {
            Ok(v) => match Self::from_family_name(v.trim()) {
                Some(d) => Some(d),
                None => {
                    // The dql CLI refuses an unknown DQL_DIALECT at startup
                    // (delightql-cli main.rs), so this lenient path serves
                    // embedded/library contexts only. Once: this is consulted
                    // per compile stage and used to warn three times for one
                    // query (bugs/cli-surface-2026-07-05/PLAN.md #4).
                    static WARNED: std::sync::Once = std::sync::Once::new();
                    WARNED.call_once(|| {
                        eprintln!(
                            "warning: unknown DQL_DIALECT '{}' (expected sqlite|postgres|mysql|sqlserver|duckdb); ignoring",
                            v
                        );
                    });
                    None
                }
            },
            Err(_) => None,
        }
    }
}
