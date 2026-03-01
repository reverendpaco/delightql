use crate::duckdb::connection::DuckDBConnectionManager;
/// DuckDB SQL Execution Interface
///
/// Provides SQL execution capabilities for DuckDB databases with support for
/// queries, statements, transactions, and prepared statements.
use delightql_types::{DelightQLError, Result};
use duckdb::Connection;
use std::sync::{Arc, Mutex};

/// Query result structure that the DuckDB executor provides
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub affected_rows: Option<usize>,
}

impl QueryResult {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self {
            columns,
            rows,
            affected_rows: None,
        }
    }

    pub fn with_affected_rows(mut self, affected: usize) -> Self {
        self.affected_rows = Some(affected);
        self
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_value(&self, row: usize, column: &str) -> Option<&String> {
        let col_index = self.columns.iter().position(|c| c == column)?;
        self.rows.get(row)?.get(col_index)
    }
}

/// Table schema information
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
}

/// Prepared statement interface
pub trait PreparedStatement {
    /// Execute the prepared statement with parameters
    fn execute(&mut self, params: &[&dyn std::fmt::Display]) -> Result<QueryResult>;

    /// Execute and return affected rows for statements
    fn execute_statement(&mut self, params: &[&dyn std::fmt::Display]) -> Result<usize>;
}

/// DuckDB prepared statement implementation
pub struct DuckDBPreparedStatement {
    connection: Arc<Mutex<Connection>>,
    sql: String,
}

impl DuckDBPreparedStatement {
    pub fn new(connection: Arc<Mutex<Connection>>, sql: String) -> Self {
        Self { connection, sql }
    }
}

impl PreparedStatement for DuckDBPreparedStatement {
    fn execute(&mut self, params: &[&dyn std::fmt::Display]) -> Result<QueryResult> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!(
                    "Previous operation panicked. Consider restarting the connection. Error: {}",
                    poison_err
                ),
            )
        })?;

        let mut stmt = conn
            .prepare(&self.sql)
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        // Convert parameters to rusqlite format
        let param_values: Vec<String> = params.iter().map(|p| p.to_string()).collect();
        let param_refs: Vec<&dyn duckdb::ToSql> = param_values
            .iter()
            .map(|s| s as &dyn duckdb::ToSql)
            .collect();

        // Get column names
        let columns: Vec<String> = stmt
            .column_names()
            .iter()
            .map(|name| name.to_string())
            .collect();

        // Execute and collect results with type information preserved
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                let mut row_values = Vec::new();
                for i in 0..columns.len() {
                    // Use SqlValue to preserve type information
                    let value = match super::value::SqlValue::from_duckdb_value(row, i) {
                        Ok(sql_val) => sql_val.to_display_string(),
                        Err(_) => "ERROR".to_string(),
                    };
                    row_values.push(value);
                }
                Ok(row_values)
            })
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        let mut result_rows = Vec::new();
        for row in rows {
            result_rows.push(row.map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?);
        }

        Ok(QueryResult::new(columns, result_rows))
    }

    fn execute_statement(&mut self, params: &[&dyn std::fmt::Display]) -> Result<usize> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!(
                    "Previous operation panicked. Consider restarting the connection. Error: {}",
                    poison_err
                ),
            )
        })?;

        let mut stmt = conn
            .prepare(&self.sql)
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        // Convert parameters to rusqlite format
        let param_values: Vec<String> = params.iter().map(|p| p.to_string()).collect();
        let param_refs: Vec<&dyn duckdb::ToSql> = param_values
            .iter()
            .map(|s| s as &dyn duckdb::ToSql)
            .collect();

        let affected = stmt
            .execute(param_refs.as_slice())
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        Ok(affected)
    }
}

/// SQL execution interface for SQLite
pub trait DuckDBExecutor {
    /// Execute a query and return results
    fn execute_query(&mut self, sql: &str) -> Result<QueryResult>;

    /// Execute a statement (INSERT, UPDATE, DELETE) and return affected rows
    fn execute_statement(&mut self, sql: &str) -> Result<usize>;

    /// Execute multiple statements in a transaction
    fn execute_transaction(&mut self, statements: &[&str]) -> Result<Vec<usize>>;

    /// Prepare a statement for repeated execution (optional optimization)
    fn prepare_statement(&mut self, sql: &str) -> Result<Box<dyn PreparedStatement>>;

    /// Check if a table exists
    fn table_exists(&self, table_name: &str) -> Result<bool>;

    /// Get table schema information
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema>;
}

/// SQLite executor implementation
pub struct DuckDBExecutorImpl {
    connection: Arc<Mutex<Connection>>,
}

impl DuckDBExecutorImpl {
    pub fn new(connection_manager: &DuckDBConnectionManager) -> Self {
        Self {
            connection: connection_manager.get_connection(),
        }
    }
}

impl DuckDBExecutor for DuckDBExecutorImpl {
    fn execute_query(&mut self, sql: &str) -> Result<QueryResult> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!(
                    "Previous operation panicked. Consider restarting the connection. Error: {}",
                    poison_err
                ),
            )
        })?;

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| DelightQLError::DatabaseOperationError {
                message: "Failed to prepare SQL statement".to_string(),
                details: format!("DuckDB error: {}", e),
                source: Some(Box::new(e)),
                subcategory: None,
            })?;

        // Execute the query - this makes column info available
        let rows = stmt
            .query_map([], |row| {
                // Get column count from the row
                let column_count = row.as_ref().column_count();
                let mut values = Vec::new();
                for i in 0..column_count {
                    // Use SqlValue to preserve type information
                    let value = match super::value::SqlValue::from_duckdb_value(row, i) {
                        Ok(sql_val) => sql_val.to_display_string(),
                        Err(_) => "ERROR".to_string(),
                    };
                    values.push(value);
                }
                Ok(values)
            })
            .map_err(|e| DelightQLError::DatabaseOperationError {
                message: "Failed to execute query".to_string(),
                details: format!("DuckDB error: {}", e),
                source: Some(Box::new(e)),
                subcategory: None,
            })?;

        // Collect rows first
        let mut result_rows = Vec::new();
        for row_result in rows {
            let row = row_result.map_err(|e| DelightQLError::DatabaseOperationError {
                message: "Failed to fetch row".to_string(),
                details: format!("DuckDB error: {}", e),
                source: Some(Box::new(e)),
                subcategory: None,
            })?;
            result_rows.push(row);
        }

        // After query_map has been called and consumed, we can get column info
        // Get column names from stmt (which is now executed)
        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| {
                stmt.column_name(i)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|_| format!("column_{}", i))
            })
            .collect();

        Ok(QueryResult::new(columns, result_rows))
    }

    fn execute_statement(&mut self, sql: &str) -> Result<usize> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!(
                    "Previous operation panicked. Consider restarting the connection. Error: {}",
                    poison_err
                ),
            )
        })?;

        let affected = conn.execute(sql, []).map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        Ok(affected)
    }

    fn execute_transaction(&mut self, statements: &[&str]) -> Result<Vec<usize>> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!(
                    "Previous operation panicked. Consider restarting the connection. Error: {}",
                    poison_err
                ),
            )
        })?;

        // Begin transaction
        conn.execute("BEGIN TRANSACTION", [])
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        let mut results = Vec::new();

        // Execute each statement
        for statement in statements {
            match conn.execute(statement, []) {
                Ok(affected) => results.push(affected),
                Err(e) => {
                    // Rollback on error
                    let _ = conn.execute("ROLLBACK", []);
                    return Err(DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()));
                }
            }
        }

        // Commit transaction
        conn.execute("COMMIT", [])
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        Ok(results)
    }

    fn prepare_statement(&mut self, sql: &str) -> Result<Box<dyn PreparedStatement>> {
        Ok(Box::new(DuckDBPreparedStatement::new(
            Arc::clone(&self.connection),
            sql.to_string(),
        )))
    }

    fn table_exists(&self, table_name: &str) -> Result<bool> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!(
                    "Previous operation panicked. Consider restarting the connection. Error: {}",
                    poison_err
                ),
            )
        })?;

        let sql = "SELECT COUNT(*) FROM sqlite_master WHERE name=?";
        let count: i64 = conn
            .query_row(sql, [table_name], |row| row.get(0))
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        Ok(count > 0)
    }

    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!(
                    "Previous operation panicked. Consider restarting the connection. Error: {}",
                    poison_err
                ),
            )
        })?;

        // First check if table exists
        let sql = "SELECT COUNT(*) FROM sqlite_master WHERE name=?";
        let count: i64 = conn
            .query_row(sql, [table_name], |row| row.get(0))
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        if count == 0 {
            return Err(DelightQLError::validation_error(
                format!("Table '{}' does not exist", table_name),
                "Schema introspection",
            ));
        }

        // Get column information using PRAGMA table_info
        let sql = format!("PRAGMA table_info({})", table_name);
        let mut stmt = conn.prepare(&sql).map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        let column_rows = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let data_type: String = row.get(2)?;
                let not_null: i32 = row.get(3)?;
                let pk: i32 = row.get(5)?;

                Ok(ColumnInfo {
                    name,
                    data_type,
                    nullable: not_null == 0,
                    primary_key: pk > 0,
                })
            })
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        let mut columns = Vec::new();
        for column in column_rows {
            columns.push(column.map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?);
        }

        Ok(TableSchema {
            name: table_name.to_string(),
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sqlite::connection::DuckDBConnectionManager;

    #[test]
    fn test_query_execution() {
        let manager =
            DuckDBConnectionManager::new_memory().expect("Failed to create connection manager");
        let mut executor = DuckDBExecutorImpl::new(&manager);

        // Create a test table
        executor
            .execute_statement("CREATE TABLE test (id INTEGER, name TEXT)")
            .expect("Failed to create table");
        executor
            .execute_statement("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
            .expect("Failed to insert data");

        let result = executor
            .execute_query("SELECT * FROM test ORDER BY id")
            .expect("Failed to execute query");

        assert_eq!(result.columns, vec!["id", "name"]);
        assert_eq!(result.row_count(), 2);
        assert_eq!(result.get_value(0, "name"), Some(&"Alice".to_string()));
        assert_eq!(result.get_value(1, "name"), Some(&"Bob".to_string()));
    }

    #[test]
    fn test_table_schema() {
        let manager =
            DuckDBConnectionManager::new_memory().expect("Failed to create connection manager");
        let executor = DuckDBExecutorImpl::new(&manager);

        // Create a test table with various column types
        {
            let conn = executor.connection.lock().unwrap();
            conn.execute(
                "CREATE TABLE test_schema (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    age INTEGER,
                    score REAL
                )",
                [],
            )
            .expect("Failed to create table");
        }

        let schema = executor
            .get_table_schema("test_schema")
            .expect("Failed to get schema");

        assert_eq!(schema.name, "test_schema");
        assert_eq!(schema.columns.len(), 4);

        // Check primary key column
        let id_col = schema.columns.iter().find(|c| c.name == "id").unwrap();
        assert!(id_col.primary_key);
        assert_eq!(id_col.data_type, "INTEGER");

        // Check NOT NULL column
        let name_col = schema.columns.iter().find(|c| c.name == "name").unwrap();
        assert!(!name_col.nullable);
        assert_eq!(name_col.data_type, "TEXT");
    }
}
