use crate::{QueryResult, SqliteConnectionManager, SqliteExecutor};
use delightql_types::{DelightQLError, Result};
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResults {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub row_count: usize,
}

impl QueryResults {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        let row_count = rows.len();
        Self {
            columns,
            rows,
            row_count,
        }
    }
}

impl From<QueryResult> for QueryResults {
    fn from(result: QueryResult) -> Self {
        QueryResults::new(result.columns, result.rows)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("SQL execution failed on database '{database_path}' with query: {sql}")]
    SqlExecutionError {
        sql: String,
        database_path: std::path::PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("SQL syntax error: {message}")]
    SqlSyntaxError {
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Database connection failed: {path}")]
    DatabaseConnectionError {
        path: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    #[error("Row count mismatch: expected {expected}, got {actual}")]
    RowCountMismatch { expected: usize, actual: usize },

    #[error("Column mismatch: expected {expected:?}, got {actual:?}")]
    ColumnMismatch {
        expected: Vec<String>,
        actual: Vec<String>,
    },

    #[error("Data mismatch at row {row}: expected {expected:?}, got {actual:?}")]
    DataMismatch {
        row: usize,
        expected: Vec<String>,
        actual: Vec<String>,
    },
}

fn validate_test_database_path(database_path: &Path) -> Result<()> {
    if !database_path.exists() {
        return Err(DelightQLError::validation_error(
            "Test database does not exist",
            format!("Expected test database at: {}", database_path.display()),
        ));
    }

    if let Some(file_name) = database_path.file_name() {
        let name = file_name.to_string_lossy();
        let is_valid = name.ends_with(".db")
            || name.ends_with(".sqlite")
            || name.ends_with(".sqlite3");
        #[cfg(feature = "duckdb")]
        {
            is_valid = is_valid || name.ends_with(".duckdb") || name.ends_with(".ddb");
        }

        if !is_valid {
            return Err(DelightQLError::validation_error(
                "Invalid database file extension",
                format!(
                    "Database file should have a supported extension: {}",
                    database_path.display()
                ),
            ));
        }
    }

    Ok(())
}

/// Detect database type from file extension
fn detect_database_type(_database_path: &Path) -> DatabaseType {
    #[cfg(feature = "duckdb")]
    if let Some(file_name) = database_path.file_name() {
        let name = file_name.to_string_lossy();
        if name.ends_with(".duckdb") || name.ends_with(".ddb") {
            return DatabaseType::DuckDB;
        }
    }
    DatabaseType::SQLite // Default to SQLite
}

/// Database type enum
enum DatabaseType {
    SQLite,
    #[cfg(feature = "duckdb")]
    DuckDB,
}

pub fn execute_sql(sql: String, database_path: &Path) -> Result<QueryResults> {
    validate_test_database_path(database_path)?;

    let database_path_str = database_path.to_str().ok_or_else(|| {
        DelightQLError::validation_error(
            "Invalid database path encoding",
            format!(
                "Database path '{}' must be valid UTF-8",
                database_path.display()
            ),
        )
    })?;

    // Detect database type and execute accordingly
    match detect_database_type(database_path) {
        DatabaseType::SQLite => {
            let connection_manager =
                SqliteConnectionManager::new_file(database_path_str).map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to create database connection manager",
                        format!("Database: {}, SQL: {}", database_path.display(), sql),
                        Box::new(e),
                    )
                })?;
            execute_sql_with_connection(sql, &connection_manager, database_path)
        }
        #[cfg(feature = "duckdb")]
        DatabaseType::DuckDB => {
            use crate::DuckDBConnectionManager;

            let connection_manager =
                DuckDBConnectionManager::new_file(database_path_str).map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to create DuckDB connection manager",
                        format!("Database: {}, SQL: {}", database_path.display(), sql),
                        Box::new(e),
                    )
                })?;

            execute_sql_with_duckdb_connection(sql, &connection_manager, database_path)
        }
    }
}

/// Execute SQL using an existing connection manager
pub fn execute_sql_with_connection(
    sql: String,
    connection_manager: &SqliteConnectionManager,
    database_path: &Path,
) -> Result<QueryResults> {
    let mut executor =
        crate::SqliteExecutorImpl::new(connection_manager);

    let result = executor.execute_query(&sql).map_err(|e| {
        DelightQLError::database_error_with_source(
            "SQL execution failed",
            format!("Database: {}, SQL: {}", database_path.display(), sql),
            Box::new(e),
        )
    })?;

    Ok(QueryResults::new(result.columns, result.rows))
}

/// Execute SQL using an existing DuckDB connection manager
#[cfg(feature = "duckdb")]
pub fn execute_sql_with_duckdb_connection(
    sql: String,
    connection_manager: &crate::DuckDBConnectionManager,
    database_path: &Path,
) -> Result<QueryResults> {
    use crate::DuckDBExecutor;

    let mut executor =
        crate::DuckDBExecutorImpl::new(connection_manager);

    let result = executor.execute_query(&sql).map_err(|e| {
        DelightQLError::database_error_with_source(
            "SQL execution failed",
            format!("Database: {}, SQL: {}", database_path.display(), sql),
            Box::new(e),
        )
    })?;

    // Convert DuckDB QueryResult to the common QueryResults type
    let row_count = result.rows.len();
    Ok(QueryResults {
        columns: result.columns,
        rows: result.rows,
        row_count,
    })
}

pub fn validate_execution_results(
    delightql_results: QueryResults,
    sql_results: QueryResults,
) -> std::result::Result<(), ExecutionError> {
    if delightql_results.row_count != sql_results.row_count {
        return Err(ExecutionError::RowCountMismatch {
            expected: delightql_results.row_count,
            actual: sql_results.row_count,
        });
    }

    if delightql_results.columns != sql_results.columns {
        return Err(ExecutionError::ColumnMismatch {
            expected: delightql_results.columns,
            actual: sql_results.columns,
        });
    }

    for (i, (expected_row, actual_row)) in delightql_results
        .rows
        .iter()
        .zip(sql_results.rows.iter())
        .enumerate()
    {
        if expected_row != actual_row {
            return Err(ExecutionError::DataMismatch {
                row: i,
                expected: expected_row.clone(),
                actual: actual_row.clone(),
            });
        }
    }

    Ok(())
}

pub fn execute_and_validate(
    sql: String,
    delightql_results: QueryResults,
    database_path: &Path,
) -> std::result::Result<(), ExecutionError> {
    let sql_results =
        execute_sql(sql.clone(), database_path).map_err(|e| ExecutionError::SqlExecutionError {
            sql: sql.clone(),
            database_path: database_path.to_path_buf(),
            source: Box::new(e),
        })?;

    validate_execution_results(delightql_results, sql_results)?;

    Ok(())
}
