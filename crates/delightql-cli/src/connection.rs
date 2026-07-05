// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Multi-database connection wrapper
///
/// Provides a unified interface for SQLite, pipe-based, and fatboy connections
use anyhow::Result;
use delightql_backends::SqliteConnectionManager;
use delightql_types::DatabaseConnection;
use std::sync::{Arc, Mutex};

/// True when the string is URI-shaped (`scheme://...`) rather than a file
/// path. One shared test so no caller can fall through to file handling.
pub fn looks_like_uri(path: &str) -> bool {
    match path.find("://") {
        Some(end) if end > 0 => path[..end]
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')),
        _ => false,
    }
}

/// One classified route for a `--db` / `mount!()` input — THE single
/// scheme-dispatch point (URI-DESIGN.md §4: users speak in resources,
/// DelightQL chooses mechanisms). An unknown scheme teaches; it can never
/// fall through to file-path handling.
#[derive(Debug, Clone, PartialEq)]
pub enum Route {
    /// A SQLite file — in-process.
    Sqlite(String),
    /// A DuckDB file (magic bytes / extension) — the duckdb fatboy.
    DuckdbFatboy(String),
    /// A worldly postgres resource — the postgres fatboy, with the URL
    /// handed to libpq verbatim as conninfo (worldly syntax, worldly
    /// semantics; `postgres:///db` is libpq's own env-completed form).
    PostgresFatboy { url: String, display_db: String },
    /// `delightql-siso://<profile>[/<target>]` — the pipe-coprocess
    /// residue for resources with no worldly URI (osqueryi and kin).
    Siso { rest: String },
}

/// Classify a `--db` / mount input. `via` is the mechanism override
/// (`--via`); it applies to postgres resources (fatboy | siso).
pub fn classify(input: &str, via: Option<&str>) -> Result<Route> {
    if let Some(v) = via {
        if !matches!(v, "fatboy" | "siso") {
            anyhow::bail!("unknown --via '{v}' (known mechanisms: fatboy, siso)");
        }
    }

    if let Some(rest) = input.strip_prefix("delightql-siso://") {
        if rest.is_empty() {
            anyhow::bail!("delightql-siso:// needs a profile: delightql-siso://<profile>[/<target>]");
        }
        return Ok(Route::Siso { rest: rest.to_string() });
    }

    if looks_like_uri(input) {
        let scheme_end = input.find("://").expect("looks_like_uri checked");
        let scheme = input[..scheme_end].to_ascii_lowercase();
        return match scheme.as_str() {
            "postgres" | "postgresql" => {
                let url = url::Url::parse(input).map_err(|e| {
                    anyhow::anyhow!("'{input}': not a valid postgres URL: {e}")
                })?;
                if url.password().is_some() {
                    anyhow::bail!(
                        "'{input}': passwords are never accepted in connection URLs \
                         (they would persist into session metadata). Set PGPASSWORD \
                         in the environment instead."
                    );
                }
                let display_db = url.path().trim_start_matches('/').to_string();
                match via {
                    None | Some("fatboy") => Ok(Route::PostgresFatboy {
                        url: input.to_string(),
                        display_db,
                    }),
                    Some("siso") => Ok(Route::Siso {
                        rest: if display_db.is_empty() {
                            "postgres".to_string()
                        } else {
                            format!("postgres/{display_db}")
                        },
                    }),
                    Some(_) => unreachable!("via validated above"),
                }
            }
            "file" => {
                // RFC 8089: file:///path (empty authority) or file://localhost/path.
                let url = url::Url::parse(input)
                    .map_err(|e| anyhow::anyhow!("'{input}': not a valid file URL: {e}"))?;
                match url.host_str() {
                    None | Some("") | Some("localhost") => {}
                    Some(h) => anyhow::bail!(
                        "'{input}': file URLs with a remote host ('{h}') are not \
                         supported — file:///absolute/path only."
                    ),
                }
                classify_file_path(url.path(), via)
            }
            "fatboy" => anyhow::bail!(
                "'{input}': fatboy:// is retired. Name the resource instead: \
                 postgres:///<dbname> (or postgres://host:port/db) for Postgres, \
                 or the file path for DuckDB — the right adapter is chosen \
                 automatically."
            ),
            "pipe" => anyhow::bail!(
                "'{input}': pipe:// is now delightql-siso:// (same \
                 profile/target syntax)."
            ),
            other => anyhow::bail!(
                "'{input}': unsupported URI scheme '{other}://'. Known: \
                 postgres://, file://, delightql-siso://, or a plain file path."
            ),
        };
    }

    classify_file_path(input, via)
}

/// Classify a filesystem path by magic bytes / extension.
fn classify_file_path(path: &str, via: Option<&str>) -> Result<Route> {
    use std::io::Read;

    if let Some(v) = via {
        if v != "fatboy" {
            anyhow::bail!("--via {v} does not apply to file-backed databases");
        }
    }

    // DuckDB magic: "DUCK" at offset 8 of the 16-byte header.
    if let Ok(mut file) = std::fs::File::open(path) {
        let mut header = [0u8; 16];
        if file.read_exact(&mut header).is_ok() && &header[8..12] == b"DUCK" {
            return Ok(Route::DuckdbFatboy(path.to_string()));
        }
    }
    if path.ends_with(".duckdb") || path.ends_with(".ddb") {
        return Ok(Route::DuckdbFatboy(path.to_string()));
    }

    // SQLite for .db/.sqlite/anything else (including files to create).
    Ok(Route::Sqlite(path.to_string()))
}

/// Connection information structure (unified across all database types)
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionInfo {
    pub database_type: String,
    pub path: Option<String>,
    pub is_memory: bool,
    pub is_connected: bool,
}

/// Unified connection manager supporting multiple database backends
#[derive(Clone)]
pub enum ConnectionManager {
    SQLite(SqliteConnectionManager),
    Pipe(Arc<delightql_cli_siso::PipeConnectionManager>),
    /// A fatboy process: relay protocol over a Unix socket, foreign
    /// engine behind it (`fatboy://postgres/<db>`).
    Fatboy(Arc<crate::fatboy_exec::FatboyManager>),
}

impl ConnectionManager {
    /// Open a classified route (see [`classify`]).
    pub fn open_route(route: Route) -> Result<Self> {
        match route {
            Route::Sqlite(path) => Ok(ConnectionManager::SQLite(
                SqliteConnectionManager::new_file(&path)?,
            )),
            Route::DuckdbFatboy(path) => {
                let mgr = crate::fatboy_exec::FatboyManager::connect("duckdb", &path)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ConnectionManager::Fatboy(Arc::new(mgr)))
            }
            Route::PostgresFatboy { url, display_db } => {
                let mgr =
                    crate::fatboy_exec::FatboyManager::connect_postgres_url(&url, &display_db)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ConnectionManager::Fatboy(Arc::new(mgr)))
            }
            Route::Siso { rest } => {
                let mgr = delightql_cli_siso::PipeConnectionManager::from_uri(&format!(
                    "delightql-siso://{rest}"
                ))
                .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ConnectionManager::Pipe(Arc::new(mgr)))
            }
        }
    }

    /// Open from a `--db` / mount input string with a mechanism override.
    pub fn open(input: &str, via: Option<&str>) -> Result<Self> {
        Self::open_route(classify(input, via)?)
    }

    /// Create a new connection from a resource string (path or worldly
    /// URI), default mechanisms. Kept as the factory-facing entry point.
    pub fn new_file(path: &str) -> Result<Self> {
        Self::open(path, None)
    }

    /// Create a new in-memory connection (defaults to SQLite)
    pub fn new_memory() -> Result<Self> {
        Ok(ConnectionManager::SQLite(
            SqliteConnectionManager::new_memory()?,
        ))
    }

    /// Test the connection
    #[allow(dead_code)]
    pub fn test_connection(&self) -> Result<()> {
        match self {
            ConnectionManager::SQLite(conn) => Ok(conn.test_connection()?),
            ConnectionManager::Pipe(mgr) => {
                let _conn = mgr.connect().map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(())
            }
            // Connected eagerly at construction (fail-closed).
            ConnectionManager::Fatboy(_) => Ok(()),
        }
    }

    /// Get the database type name
    pub fn database_type(&self) -> &str {
        match self {
            ConnectionManager::SQLite(_) => "SQLite",
            ConnectionManager::Pipe(mgr) => mgr.profile_name(),
            ConnectionManager::Fatboy(mgr) => &mgr.profile,
        }
    }

    #[allow(dead_code)]
    pub fn as_sqlite(&self) -> Option<&SqliteConnectionManager> {
        match self {
            ConnectionManager::SQLite(conn) => Some(conn),
            _ => None,
        }
    }

    /// Get connection Arc (for SQLite - backward compatibility)
    /// TODO: Remove this once all code uses database-agnostic APIs
    pub fn get_connection_arc(&self) -> std::sync::Arc<std::sync::Mutex<rusqlite::Connection>> {
        match self {
            ConnectionManager::SQLite(conn) => conn.get_connection_arc(),
            ConnectionManager::Pipe(_) => {
                panic!("Cannot get SQLite connection from Pipe - use database-agnostic APIs")
            }
            ConnectionManager::Fatboy(_) => {
                panic!("Cannot get SQLite connection from Fatboy - use database-agnostic APIs")
            }
        }
    }

    /// Get database connection as a trait object (database-agnostic)
    pub fn get_database_connection(&self) -> Arc<Mutex<dyn DatabaseConnection>> {
        match self {
            ConnectionManager::SQLite(conn) => {
                let adapter =
                    delightql_backends::sqlite::SqliteConnection::new(conn.get_connection_arc());
                Arc::new(Mutex::new(adapter))
            }
            ConnectionManager::Pipe(mgr) => {
                let conn = mgr.connect().expect("Failed to spawn pipe connection");
                Arc::new(Mutex::new(conn))
            }
            ConnectionManager::Fatboy(mgr) => Arc::new(Mutex::new(
                crate::fatboy_exec::FatboyConnection::new(mgr.clone()),
            )),
        }
    }

    /// Get connection information
    pub fn connection_info(&self) -> Result<ConnectionInfo> {
        match self {
            ConnectionManager::SQLite(conn) => {
                let info = conn.connection_info()?;
                Ok(ConnectionInfo {
                    database_type: info.database_type,
                    path: info.path,
                    is_memory: info.is_memory,
                    is_connected: info.is_connected,
                })
            }
            ConnectionManager::Pipe(mgr) => Ok(ConnectionInfo {
                database_type: format!("Pipe({})", mgr.profile_name()),
                path: mgr.target().map(|s| s.to_string()),
                is_memory: false,
                is_connected: true,
            }),
            ConnectionManager::Fatboy(mgr) => Ok(ConnectionInfo {
                database_type: format!("Fatboy({})", mgr.profile),
                path: Some(format!("{} [stdio]", mgr.db)),
                is_memory: false,
                is_connected: true,
            }),
        }
    }

    /// Attach another database file with a schema name (SQLite only for now)
    pub fn attach_database(&self, db_path: &str, schema_name: &str) -> Result<()> {
        match self {
            ConnectionManager::SQLite(conn) => conn
                .attach_database_file(db_path, schema_name)
                .map_err(|e| anyhow::anyhow!("Failed to attach database: {}", e)),
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("Database attachment not supported for pipe connections")
            }
            ConnectionManager::Fatboy(_) => {
                anyhow::bail!("Database attachment not supported for fatboy connections")
            }
        }
    }

    /// Get raw SQLite connection for import operations
    /// Returns the underlying Arc<Mutex<rusqlite::Connection>> for SQLite connections
    ///
    /// This is used by import operations that need direct access to the connection
    /// to work with _bootstrap.* tables.
    pub fn get_raw_sqlite_connection(&self) -> Result<Arc<Mutex<rusqlite::Connection>>> {
        match self {
            ConnectionManager::SQLite(conn) => Ok(conn.get_connection_arc()),
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("Import operations not supported for pipe connections")
            }
            ConnectionManager::Fatboy(_) => {
                anyhow::bail!("Import operations not supported for fatboy connections")
            }
        }
    }

    /// Execute a SQL query against the underlying database connection.
    ///
    /// Dispatches to the appropriate backend (SQLite, Pipe, or Fatboy).
    /// The `db_label` is used for error messages in SQLite; Pipe ignores it.
    pub fn execute_query(
        &self,
        sql: &str,
        db_label: &str,
    ) -> Result<delightql_backends::QueryResults> {
        match self {
            ConnectionManager::SQLite(conn) => delightql_backends::execute_sql_with_connection(
                sql.to_string(),
                conn,
                std::path::Path::new(db_label),
            )
            .map_err(|e| anyhow::anyhow!("{}", e)),
            ConnectionManager::Pipe(mgr) => crate::pipe_exec::execute_sql_with_pipe(sql, mgr)
                .map_err(|e| anyhow::anyhow!("{}", e)),
            ConnectionManager::Fatboy(mgr) => {
                crate::fatboy_exec::execute_sql_with_fatboy(sql, mgr)
                    .map_err(|e| anyhow::anyhow!("{}", e))
            }
        }
    }

    /// Execute a SQL query with NULL fidelity preserved.
    ///
    /// Returns rows as `Vec<Vec<Option<String>>>` where `None` = SQL NULL.
    /// Used by the relay adapter to produce honest `Cell = Option<ByteSeq>`.
    pub fn execute_query_typed(
        &self,
        sql: &str,
        db_label: &str,
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        match self {
            ConnectionManager::SQLite(conn) => {
                let mut executor = delightql_backends::SqliteExecutorImpl::new(conn);
                let typed = executor
                    .execute_query_typed(sql)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                let rows = typed
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|val| match val {
                                delightql_backends::SqlValue::Null => None,
                                other => Some(other.to_display_string()),
                            })
                            .collect()
                    })
                    .collect();
                Ok((typed.columns, rows))
            }
            ConnectionManager::Pipe(_) => {
                let results = self.execute_query(sql, db_label)?;
                let rows = results
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(Some).collect())
                    .collect();
                Ok((results.columns, rows))
            }
            // NULL fidelity is native here: relay Cells are Option already.
            ConnectionManager::Fatboy(mgr) => mgr
                .relay()
                .query_nullable(sql)
                .map_err(|e| anyhow::anyhow!("{}", e)),
        }
    }

    /// Execute a DML statement with NULL fidelity preserved.
    ///
    /// Returns (columns, rows) where affected_rows is the first column.
    pub fn execute_dml_typed(
        &self,
        sql: &str,
        _db_label: &str,
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        match self {
            ConnectionManager::SQLite(conn) => {
                use delightql_backends::SqliteExecutor;
                let mut executor = delightql_backends::SqliteExecutorImpl::new(conn);
                let affected = executor
                    .execute_statement(sql)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok((
                    vec!["affected_rows".to_string()],
                    vec![vec![Some(affected.to_string())]],
                ))
            }
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("DML not supported on pipe connections")
            }
            ConnectionManager::Fatboy(_) => {
                anyhow::bail!("DML not yet supported on fatboy connections")
            }
        }
    }

    /// Execute a DML statement (DELETE, UPDATE, INSERT) and return affected row count.
    pub fn execute_dml(
        &self,
        sql: &str,
        _db_label: &str,
    ) -> Result<delightql_backends::QueryResults> {
        match self {
            ConnectionManager::SQLite(conn) => {
                use delightql_backends::SqliteExecutor;
                let mut executor = delightql_backends::SqliteExecutorImpl::new(conn);
                let affected = executor
                    .execute_statement(sql)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(delightql_backends::QueryResults {
                    columns: vec!["affected_rows".to_string()],
                    rows: vec![vec![affected.to_string()]],
                    row_count: 1,
                })
            }
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("DML not supported on pipe connections")
            }
            ConnectionManager::Fatboy(_) => {
                anyhow::bail!("DML not yet supported on fatboy connections")
            }
        }
    }

    /// Create ConnectionComponents for `open()`.
    ///
    /// The CLI never touches the individual components — it passes the
    /// opaque struct straight to `delightql_core::api::open()`.
    pub fn create_system_components(&self) -> Result<delightql_types::ConnectionComponents> {
        match self {
            ConnectionManager::SQLite(sqlite_conn) => {
                let raw_conn_arc = sqlite_conn.get_connection_arc();
                let schema = Box::new(delightql_backends::DynamicSqliteSchema::new(
                    raw_conn_arc.clone(),
                ));
                let introspector = Box::new(delightql_backends::sqlite::SqliteIntrospector::new(
                    raw_conn_arc.clone(),
                ));
                let adapter =
                    delightql_backends::sqlite::SqliteConnection::new(raw_conn_arc.clone());
                let conn_arc: Arc<Mutex<dyn DatabaseConnection>> = Arc::new(Mutex::new(adapter));
                let identity = raw_conn_arc
                    .lock()
                    .ok()
                    .and_then(|c| c.path().map(|p| p.to_string()))
                    .filter(|p| !p.is_empty())
                    .and_then(|p| std::fs::canonicalize(&p).ok())
                    .map(|abs| format!("realpath:{}", abs.display()));
                Ok(delightql_types::ConnectionComponents {
                    schema,
                    connection: conn_arc,
                    introspector,
                    db_type: "sqlite".to_string(),
                    mechanism: "in-process".to_string(),
                    identity,
                })
            }
            ConnectionManager::Pipe(mgr) => crate::pipe_exec::create_pipe_system_components(mgr),
            ConnectionManager::Fatboy(mgr) => {
                crate::fatboy_exec::create_fatboy_system_components(mgr)
            }
        }
    }

    /// Open a DqlHandle using the factory-only API.
    ///
    /// Returns `Box<dyn DqlHandle>` — the compiler-enforced API boundary.
    /// The handle starts with an empty "main" namespace. The CLI must send
    /// `mount!("path", "main")` to populate it.
    pub fn open_handle(&self) -> Result<Box<dyn delightql_core::api::DqlHandle>> {
        let factory = Box::new(crate::connection_factory::CliConnectionFactory);
        // Second factory (types-level) powers mount!/import! of URI-scheme
        // databases (pipe://, etc.). Same unit struct, both trait impls.
        let mount_factory = Box::new(crate::connection_factory::CliConnectionFactory);
        delightql_core::api::open(factory, Some(mount_factory))
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_urls_route_to_the_fatboy() {
        let r = classify("postgres://alice@db.example:5432/prod", None).unwrap();
        assert_eq!(
            r,
            Route::PostgresFatboy {
                url: "postgres://alice@db.example:5432/prod".into(),
                display_db: "prod".into()
            }
        );
        // libpq's env-completed form
        let r = classify("postgres:///dql_core", None).unwrap();
        assert_eq!(
            r,
            Route::PostgresFatboy {
                url: "postgres:///dql_core".into(),
                display_db: "dql_core".into()
            }
        );
        // postgresql:// spelling too
        assert!(matches!(
            classify("postgresql://h/d", None).unwrap(),
            Route::PostgresFatboy { .. }
        ));
        // --via siso reroutes to the pipe coprocess
        assert_eq!(
            classify("postgres:///dql_core", Some("siso")).unwrap(),
            Route::Siso { rest: "postgres/dql_core".into() }
        );
    }

    #[test]
    fn secrets_never_enter_connection_urls() {
        let err = classify("postgres://alice:hunter2@h/d", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("passwords are never accepted"), "{err}");
        assert!(err.contains("PGPASSWORD"), "{err}");
    }

    #[test]
    fn file_urls_and_paths_classify_by_content() {
        assert_eq!(
            classify("some/dir/data.db", None).unwrap(),
            Route::Sqlite("some/dir/data.db".into())
        );
        assert_eq!(
            classify("weird:name.db", None).unwrap(),
            Route::Sqlite("weird:name.db".into())
        );
        // duckdb by extension routes to the fatboy — no teaching error
        assert_eq!(
            classify("analytics.duckdb", None).unwrap(),
            Route::DuckdbFatboy("analytics.duckdb".into())
        );
        assert_eq!(
            classify("file:///data/x.duckdb", None).unwrap(),
            Route::DuckdbFatboy("/data/x.duckdb".into())
        );
        let err = classify("file://remotehost/x.db", None).unwrap_err().to_string();
        assert!(err.contains("remote host"), "{err}");
    }

    #[test]
    fn retired_and_unknown_schemes_teach() {
        let err = classify("fatboy://postgres/db", None).unwrap_err().to_string();
        assert!(err.contains("retired"), "{err}");
        assert!(err.contains("postgres:///"), "{err}");
        let err = classify("pipe://psql", None).unwrap_err().to_string();
        assert!(err.contains("delightql-siso://"), "{err}");
        let err = classify("mysql://host/db", None).unwrap_err().to_string();
        assert!(err.contains("unsupported URI scheme 'mysql://'"), "{err}");
    }

    #[test]
    fn siso_scheme_routes() {
        assert_eq!(
            classify("delightql-siso://osqueryi", None).unwrap(),
            Route::Siso { rest: "osqueryi".into() }
        );
        assert_eq!(
            classify("delightql-siso://postgres/dql_core", None).unwrap(),
            Route::Siso { rest: "postgres/dql_core".into() }
        );
    }
}
