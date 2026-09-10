// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::{SqliteConnectionManager, SqliteExecutor};
use delightql_types::diagnostic::Runtime;
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

fn validate_test_database_path(database_path: &Path) -> Result<()> {
    if !database_path.exists() {
        return Err(Runtime::Io {
            message: format!(
                "Test database does not exist: expected it at {}",
                database_path.display()
            ),
        }
        .into());
    }

    if let Some(file_name) = database_path.file_name() {
        let name = file_name.to_string_lossy();
        #[allow(unused_mut)] // mutated only under the duckdb feature
        let mut is_valid =
            name.ends_with(".db") || name.ends_with(".sqlite") || name.ends_with(".sqlite3");
        #[cfg(feature = "duckdb")]
        {
            is_valid = is_valid || name.ends_with(".duckdb") || name.ends_with(".ddb");
        }

        if !is_valid {
            return Err(Runtime::Unsupported {
                message: format!(
                    "Invalid database file extension: the database file should have a \
                     supported extension: {}",
                    database_path.display()
                ),
            }
            .into());
        }
    }

    Ok(())
}

/// Detect database type from file extension
fn detect_database_type(database_path: &Path) -> DatabaseType {
    let _ = database_path; // used only under the duckdb feature
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
        DelightQLError::from(Runtime::Io {
            message: format!(
                "Invalid database path encoding: '{}' must be valid UTF-8",
                database_path.display()
            ),
        })
    })?;

    // Detect database type and execute accordingly
    match detect_database_type(database_path) {
        DatabaseType::SQLite => {
            let connection_manager = SqliteConnectionManager::new_file(database_path_str)?;
            execute_sql_with_connection(sql, &connection_manager)
        }
        #[cfg(feature = "duckdb")]
        DatabaseType::DuckDB => {
            use crate::DuckDBConnectionManager;

            let connection_manager = DuckDBConnectionManager::new_file(database_path_str)?;

            execute_sql_with_duckdb_connection(sql, &connection_manager)
        }
    }
}

/// Execute SQL using an existing connection manager
pub fn execute_sql_with_connection(
    sql: String,
    connection_manager: &SqliteConnectionManager,
) -> Result<QueryResults> {
    let mut executor = crate::SqliteExecutorImpl::new(connection_manager);

    let result = executor.execute_query(&sql)?;

    Ok(QueryResults::new(result.columns, result.rows))
}

/// Execute SQL using an existing DuckDB connection manager
#[cfg(feature = "duckdb")]
pub fn execute_sql_with_duckdb_connection(
    sql: String,
    connection_manager: &crate::DuckDBConnectionManager,
) -> Result<QueryResults> {
    use crate::DuckDBExecutor;

    let mut executor = crate::DuckDBExecutorImpl::new(connection_manager);

    let result = executor.execute_query(&sql)?;

    // Convert DuckDB QueryResult to the common QueryResults type
    let row_count = result.rows.len();
    Ok(QueryResults {
        columns: result.columns,
        rows: result.rows,
        row_count,
    })
}
