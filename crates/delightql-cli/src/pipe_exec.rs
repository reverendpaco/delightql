//! Pipe execution helpers for SISO (Standard In / Standard Out) connections.
//!
//! Provides a bridge between the CLI execution engine and pipe-based backends.

use delightql_backends::QueryResults;
use delightql_cli_siso::PipeConnectionManager;
use std::sync::Arc;

/// Execute SQL through a pipe connection manager and return QueryResults.
///
/// Uses the manager's shared coprocess — no additional process is spawned.
pub(crate) fn execute_sql_with_pipe(
    sql: &str,
    mgr: &Arc<PipeConnectionManager>,
) -> std::result::Result<QueryResults, delightql_core::error::DelightQLError> {
    let (columns, rows) = mgr.execute_query_raw(sql).map_err(|e| {
        delightql_core::error::DelightQLError::database_error(
            format!("Pipe query failed: {}", e),
            e.to_string(),
        )
    })?;

    let row_count = rows.len();
    Ok(QueryResults {
        columns,
        rows,
        row_count,
    })
}

/// Create an introspector for a Pipe connection.
pub(crate) fn create_pipe_introspector(
    mgr: &Arc<PipeConnectionManager>,
) -> Result<
    Box<dyn delightql_types::introspect::DatabaseIntrospector>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let introspector =
        mgr.introspector()
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to create pipe introspector: {}", e),
                ))
            })?;
    Ok(Box::new(introspector))
}

/// Create ConnectionComponents for a Pipe connection.
///
/// The schema comes from the pipe (via PRAGMA table_info), but the connection
/// is a local in-memory SQLite database used for bootstrap session tables.
/// Actual query execution goes through the pipe via `execute_sql_with_pipe`.
pub(crate) fn create_pipe_system_components(
    mgr: &Arc<PipeConnectionManager>,
) -> anyhow::Result<delightql_types::ConnectionComponents> {
    let schema = mgr.schema().map_err(|e| anyhow::anyhow!("{}", e))?;

    // Session connection: a local in-memory SQLite DB for bootstrap session tables.
    let session_conn = delightql_backends::SqliteConnectionManager::new_memory()
        .map_err(|e| anyhow::anyhow!("Failed to create session database for pipe: {}", e))?;
    let raw_conn_arc = session_conn.get_connection_arc();
    let adapter = delightql_backends::sqlite::SqliteConnection::new(raw_conn_arc.clone());
    let conn_arc: std::sync::Arc<std::sync::Mutex<dyn delightql_types::DatabaseConnection>> =
        std::sync::Arc::new(std::sync::Mutex::new(adapter));

    let introspector = mgr
        .introspector()
        .map_err(|e| anyhow::anyhow!("Failed to create pipe introspector: {}", e))?;

    Ok(delightql_types::ConnectionComponents {
        schema: Box::new(schema),
        connection: conn_arc,
        introspector: Box::new(introspector),
        db_type: "sqlite".to_string(),
    })
}
