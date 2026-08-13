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

/// WHERE A TARGET PUTS ITS ROW BOUND.
///
/// Four of the five write a trailing `LIMIT`/`OFFSET` clause that stands on
/// its own. Transact-SQL has no such clause: a cap is a `TOP` between
/// `SELECT` and the list, and a skip belongs to `ORDER BY`, which must
/// therefore be present in the same query block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RowClauseStyle {
    /// `LIMIT <count|uncapped> [OFFSET n]`, where `uncapped` is how this
    /// target spells "no maximum".
    Trailing { uncapped: &'static str },
    /// `SELECT TOP n` for a bare cap, `ORDER BY … OFFSET n ROWS [FETCH NEXT
    /// m ROWS ONLY]` once a skip is involved.
    TopAndFetch,
}

impl SqlDialect {
    /// HOW THIS TARGET SPELLS A ROW BOUND.
    ///
    /// `#>n` skips rows and names no cap, and no two targets agree on how to
    /// say so: two write a sentinel count beside `OFFSET`, two write the
    /// keyword the standard gives them, and one has no `LIMIT` at all.
    pub fn row_clause_style(self) -> RowClauseStyle {
        match self {
            // SQLite documents a negative limit as "no upper bound".
            SqlDialect::SQLite => RowClauseStyle::Trailing { uncapped: "-1" },
            // MySQL has no keyword; its manual prescribes the largest
            // unsigned value, which does not fit the AST's signed count and
            // so is written here rather than carried as one.
            SqlDialect::MySQL => RowClauseStyle::Trailing {
                uncapped: "18446744073709551615",
            },
            SqlDialect::PostgreSQL | SqlDialect::DuckDB => {
                RowClauseStyle::Trailing { uncapped: "ALL" }
            }
            SqlDialect::SqlServer => RowClauseStyle::TopAndFetch,
        }
    }

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
                    // per compile stage, so an unguarded warn fires three
                    // times for one query.
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
