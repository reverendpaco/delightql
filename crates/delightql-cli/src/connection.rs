/// Multi-database connection wrapper
///
/// Provides a unified interface for SQLite, DuckDB, and pipe-based connections
use anyhow::Result;
#[cfg(feature = "duckdb")]
use delightql_backends::DuckDBConnectionManager;
use delightql_backends::SqliteConnectionManager;
use delightql_types::DatabaseConnection;
use std::sync::{Arc, Mutex};

/// Database type detected from file magic numbers
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DatabaseType {
    SQLite,
    #[cfg(feature = "duckdb")]
    DuckDB,
}

impl DatabaseType {
    /// Detect database type from file path by checking magic numbers
    ///
    /// SQLite files start with: "SQLite format 3\0" (16 bytes)
    /// DuckDB files have "DUCK" at offset 8 (4 bytes)
    ///
    /// Falls back to extension-based detection if file doesn't exist or can't be read.
    pub fn from_path(path: &str) -> Self {
        use std::io::Read;

        // Try to detect by reading file magic numbers
        if let Ok(mut file) = std::fs::File::open(path) {
            let mut header = [0u8; 16];
            if file.read_exact(&mut header).is_ok() {
                // Check for SQLite magic: "SQLite format 3\0"
                if &header[0..16] == b"SQLite format 3\0" {
                    return DatabaseType::SQLite;
                }

                // Check for DuckDB magic: "DUCK" at offset 8
                #[cfg(feature = "duckdb")]
                if &header[8..12] == b"DUCK" {
                    return DatabaseType::DuckDB;
                }
            }
        }

        // Fall back to extension-based detection
        #[cfg(feature = "duckdb")]
        if path.ends_with(".duckdb") || path.ends_with(".ddb") {
            return DatabaseType::DuckDB;
        }

        // Default to SQLite for .db, .sqlite, .sqlite3, or no extension
        DatabaseType::SQLite
    }
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
    #[cfg(feature = "duckdb")]
    DuckDB(DuckDBConnectionManager),
    Pipe(Arc<delightql_cli_siso::PipeConnectionManager>),
}

impl ConnectionManager {
    /// Create a connection from a URI string.
    ///
    /// Supports `pipe://profile` and `pipe://profile/target` URIs.
    pub fn from_uri(uri: &str) -> Result<Self> {
        if uri.starts_with("pipe://") {
            let mgr = delightql_cli_siso::PipeConnectionManager::from_uri(uri)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            Ok(ConnectionManager::Pipe(Arc::new(mgr)))
        } else {
            anyhow::bail!("Unsupported URI scheme: {}", uri)
        }
    }

    /// Create a new connection from a file path, auto-detecting database type.
    ///
    /// Also accepts `pipe://` URIs for pipe-based connections.
    pub fn new_file(path: &str) -> Result<Self> {
        // Check for URI schemes before treating as a file path
        if path.starts_with("pipe://") {
            return Self::from_uri(path);
        }

        let db_type = DatabaseType::from_path(path);

        match db_type {
            DatabaseType::SQLite => Ok(ConnectionManager::SQLite(
                SqliteConnectionManager::new_file(path)?,
            )),
            #[cfg(feature = "duckdb")]
            DatabaseType::DuckDB => Ok(ConnectionManager::DuckDB(
                DuckDBConnectionManager::new_file(path)?,
            )),
        }
    }

    /// Create a new in-memory connection (defaults to SQLite)
    pub fn new_memory() -> Result<Self> {
        Ok(ConnectionManager::SQLite(
            SqliteConnectionManager::new_memory()?,
        ))
    }

    /// Create a new in-memory DuckDB connection
    #[cfg(feature = "duckdb")]
    #[allow(dead_code)]
    pub fn new_memory_duckdb() -> Result<Self> {
        Ok(ConnectionManager::DuckDB(
            DuckDBConnectionManager::new_memory()?,
        ))
    }

    /// Test the connection
    #[allow(dead_code)]
    pub fn test_connection(&self) -> Result<()> {
        match self {
            ConnectionManager::SQLite(conn) => Ok(conn.test_connection()?),
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(conn) => Ok(conn.test_connection()?),
            ConnectionManager::Pipe(mgr) => {
                let _conn = mgr.connect().map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(())
            }
        }
    }

    /// Get the database type name
    pub fn database_type(&self) -> &str {
        match self {
            ConnectionManager::SQLite(_) => "SQLite",
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(_) => "DuckDB",
            ConnectionManager::Pipe(mgr) => mgr.profile_name(),
        }
    }

    #[allow(dead_code)]
    pub fn as_sqlite(&self) -> Option<&SqliteConnectionManager> {
        match self {
            ConnectionManager::SQLite(conn) => Some(conn),
            _ => None,
        }
    }

    /// Get the underlying DuckDB connection
    #[cfg(feature = "duckdb")]
    #[allow(dead_code)]
    pub fn as_duckdb(&self) -> Option<&DuckDBConnectionManager> {
        match self {
            ConnectionManager::DuckDB(conn) => Some(conn),
            _ => None,
        }
    }

    /// Get connection Arc (for SQLite - backward compatibility)
    /// TODO: Remove this once all code uses database-agnostic APIs
    pub fn get_connection_arc(&self) -> std::sync::Arc<std::sync::Mutex<rusqlite::Connection>> {
        match self {
            ConnectionManager::SQLite(conn) => conn.get_connection_arc(),
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(_) => {
                panic!("Cannot get SQLite connection from DuckDB - use database-agnostic APIs")
            }
            ConnectionManager::Pipe(_) => {
                panic!("Cannot get SQLite connection from Pipe - use database-agnostic APIs")
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
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(conn) => {
                let adapter =
                    delightql_backends::duckdb::DuckDBConnection::new(conn.get_connection_arc());
                Arc::new(Mutex::new(adapter))
            }
            ConnectionManager::Pipe(mgr) => {
                let conn = mgr.connect().expect("Failed to spawn pipe connection");
                Arc::new(Mutex::new(conn))
            }
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
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(conn) => {
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
        }
    }

    /// Attach another database file with a schema name (SQLite only for now)
    pub fn attach_database(&self, db_path: &str, schema_name: &str) -> Result<()> {
        match self {
            ConnectionManager::SQLite(conn) => conn
                .attach_database_file(db_path, schema_name)
                .map_err(|e| anyhow::anyhow!("Failed to attach database: {}", e)),
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(_) => {
                anyhow::bail!("Database attachment not yet supported for DuckDB")
            }
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("Database attachment not supported for pipe connections")
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
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(_) => {
                anyhow::bail!("Import operations not yet supported for DuckDB")
            }
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("Import operations not supported for pipe connections")
            }
        }
    }

    /// Execute a SQL query against the underlying database connection.
    ///
    /// Dispatches to the appropriate backend (SQLite, DuckDB, or Pipe).
    /// The `db_label` is used for error messages in SQLite/DuckDB; Pipe ignores it.
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
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(conn) => {
                delightql_backends::execute_sql_with_duckdb_connection(
                    sql.to_string(),
                    conn,
                    std::path::Path::new(db_label),
                )
                .map_err(|e| anyhow::anyhow!("{}", e))
            }
            ConnectionManager::Pipe(mgr) => crate::pipe_exec::execute_sql_with_pipe(sql, mgr)
                .map_err(|e| anyhow::anyhow!("{}", e)),
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
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(_) => {
                // Fall back to string-based execution for now
                let results = self.execute_query(sql, db_label)?;
                let rows = results
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(Some).collect())
                    .collect();
                Ok((results.columns, rows))
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
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(_) => {
                anyhow::bail!("DML not yet supported on DuckDB connections")
            }
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("DML not supported on pipe connections")
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
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(_conn) => {
                anyhow::bail!("DML not yet supported on DuckDB connections")
            }
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("DML not supported on pipe connections")
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
                Ok(delightql_types::ConnectionComponents {
                    schema,
                    connection: conn_arc,
                    introspector,
                    db_type: "sqlite".to_string(),
                })
            }
            #[cfg(feature = "duckdb")]
            ConnectionManager::DuckDB(duckdb_conn) => {
                let duckdb_arc = duckdb_conn.get_connection_arc();
                let schema = Box::new(delightql_backends::DynamicDuckDBSchema::new(
                    duckdb_arc.clone(),
                ));
                let introspector = Box::new(delightql_backends::duckdb::DuckDBIntrospector::new(
                    duckdb_arc.clone(),
                ));
                let adapter = delightql_backends::duckdb::DuckDBConnection::new(duckdb_arc.clone());
                let conn_arc: Arc<Mutex<dyn DatabaseConnection>> = Arc::new(Mutex::new(adapter));
                Ok(delightql_types::ConnectionComponents {
                    schema,
                    connection: conn_arc,
                    introspector,
                    db_type: "duckdb".to_string(),
                })
            }
            ConnectionManager::Pipe(mgr) => crate::pipe_exec::create_pipe_system_components(mgr),
        }
    }

    /// Open a DqlHandle using the factory-only API.
    ///
    /// Returns `Box<dyn DqlHandle>` — the compiler-enforced API boundary.
    /// The handle starts with an empty "main" namespace. The CLI must send
    /// `mount!("path", "main")` to populate it.
    pub fn open_handle(&self) -> Result<Box<dyn delightql_core::api::DqlHandle>> {
        let factory = Box::new(crate::connection_factory::CliConnectionFactory);
        delightql_core::api::open(factory).map_err(|e| anyhow::anyhow!("{}", e))
    }
}
