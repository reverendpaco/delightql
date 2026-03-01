//! Mock implementations for testing
//!
//! Provides mock database connections and schema providers that can be used
//! in tests without requiring a real database. This is useful for:
//! - Testing core logic in isolation
//! - Faster test execution
//! - Testing error conditions
//! - Serving as a "dry run" for the WASM bridge implementation
//!
//! # Examples
//!
//! ## Testing with Mock Database Connection
//!
//! ```rust,ignore
//! use delightql_types::test_utils::{MockDatabaseConnection, MockRow};
//! use delightql_types::DbValue;
//!
//! let conn = MockDatabaseConnection::new();
//!
//! // Configure expected query results
//! conn.expect_query(
//!     "SELECT * FROM users WHERE id = ?",
//!     vec![MockRow::new(
//!         vec!["id".to_string(), "name".to_string()],
//!         vec![DbValue::Integer(1), DbValue::Text("Alice".to_string())],
//!     )],
//! );
//!
//! // Execute query - returns mocked data
//! let result = conn.query_row_values("SELECT * FROM users WHERE id = ?", &[DbValue::Integer(1)]);
//! assert!(result.is_ok());
//!
//! // Verify the query was executed
//! assert!(conn.assert_executed("SELECT * FROM users"));
//! ```
//!
//! ## Testing with Mock Schema Provider
//!
//! ```rust,ignore
//! use delightql_types::test_utils::MockSchemaProvider;
//! use delightql_types::schema::{DatabaseSchema, ColumnInfo};
//!
//! let schema = MockSchemaProvider::new();
//!
//! // Add tables programmatically
//! schema.add_table(
//!     None,
//!     "users",
//!     vec![
//!         ColumnInfo { name: "id".to_string(), nullable: false, position: 0 },
//!         ColumnInfo { name: "name".to_string(), nullable: true, position: 1 },
//!     ],
//! );
//!
//! // Use in resolution/validation logic
//! assert!(schema.table_exists(None, "users"));
//! let columns = schema.get_table_columns(None, "users").unwrap();
//! assert_eq!(columns.len(), 2);
//! ```
//!
//! ## Testing Error Conditions
//!
//! ```rust,ignore
//! use delightql_types::test_utils::MockDatabaseConnection;
//! use delightql_types::DbValue;
//!
//! let conn = MockDatabaseConnection::new();
//!
//! // Configure error for specific query
//! conn.expect_error("SELECT * FROM missing_table", "Table not found");
//!
//! // Query will fail with configured error
//! let result = conn.query_row_values("SELECT * FROM missing_table", &[]);
//! assert!(result.is_err());
//! ```
//!
//! ## Combined Example: Testing Query Compiler
//!
//! This shows how mocks enable testing without a real database:
//!
//! ```ignore
//! // In your test:
//! let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
//! let schema = Box::new(MockSchemaProvider::new());
//!
//! // Configure mock schema
//! schema.add_table(None, "users", vec![
//!     ColumnInfo { name: "id".to_string(), nullable: false, position: 0 },
//!     ColumnInfo { name: "email".to_string(), nullable: true, position: 1 },
//! ]);
//!
//! // Create system with mocks (no real database!)
//! let system = DelightQLSystem::new_with_schema(conn, schema)?;
//!
//! // Test compilation
//! let sql = system.compile("users(id, email)")?;
//! assert_eq!(sql, "SELECT id, email FROM users");
//! ```

use crate::db_traits::{DatabaseConnection, DbValue, Row};
use crate::error::{DelightQLError, Result};
use crate::schema::{ColumnInfo as SchemaColumnInfo, DatabaseSchema};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Mock row implementation that stores column data
#[derive(Debug, Clone)]
pub struct MockRow {
    columns: Vec<String>,
    values: Vec<DbValue>,
}

impl MockRow {
    /// Create a new mock row from column names and values
    pub fn new(columns: Vec<String>, values: Vec<DbValue>) -> Self {
        assert_eq!(
            columns.len(),
            values.len(),
            "Column count must match value count"
        );
        Self { columns, values }
    }

    /// Create a mock row from a map of column name -> value
    pub fn from_map(data: HashMap<String, DbValue>) -> Self {
        let mut columns = Vec::new();
        let mut values = Vec::new();

        for (col, val) in data {
            columns.push(col);
            values.push(val);
        }

        Self { columns, values }
    }
}

impl Row for MockRow {
    fn get_value(&self, idx: usize) -> Result<DbValue> {
        self.values.get(idx).cloned().ok_or_else(|| {
            DelightQLError::validation_error(
                "Column index out of bounds",
                format!("Index {} exceeds column count {}", idx, self.values.len()),
            )
        })
    }

    fn get_value_by_name(&self, name: &str) -> Result<DbValue> {
        self.columns
            .iter()
            .position(|col| col == name)
            .and_then(|idx| self.values.get(idx).cloned())
            .ok_or_else(|| {
                DelightQLError::validation_error(
                    "Column not found",
                    format!("Column '{}' not found in row", name),
                )
            })
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }

    fn column_name(&self, idx: usize) -> Result<&str> {
        self.columns.get(idx).map(|s| s.as_str()).ok_or_else(|| {
            DelightQLError::validation_error(
                "Column index out of bounds",
                format!("Index {} exceeds column count {}", idx, self.columns.len()),
            )
        })
    }
}

/// Recorded query execution for verification
#[derive(Debug, Clone)]
pub struct ExecutedQuery {
    pub sql: String,
    pub params: Vec<DbValue>,
}

/// Mock database connection that records queries and returns pre-configured results
pub struct MockDatabaseConnection {
    /// Queries that have been executed
    executed_queries: Arc<Mutex<Vec<ExecutedQuery>>>,

    /// Pre-configured query results: SQL pattern -> rows
    query_results: Arc<Mutex<HashMap<String, Vec<MockRow>>>>,

    /// Pre-configured errors: SQL pattern -> error message
    query_errors: Arc<Mutex<HashMap<String, String>>>,

    /// Last inserted row ID (for auto-increment simulation)
    last_insert_rowid: Arc<Mutex<i64>>,
}

impl MockDatabaseConnection {
    /// Create a new mock database connection
    pub fn new() -> Self {
        Self {
            executed_queries: Arc::new(Mutex::new(Vec::new())),
            query_results: Arc::new(Mutex::new(HashMap::new())),
            query_errors: Arc::new(Mutex::new(HashMap::new())),
            last_insert_rowid: Arc::new(Mutex::new(0)),
        }
    }

    /// Configure expected query results
    /// The SQL pattern can be an exact match or a prefix
    pub fn expect_query(&self, sql_pattern: impl Into<String>, rows: Vec<MockRow>) {
        let mut results = self.query_results.lock().unwrap();
        results.insert(sql_pattern.into(), rows);
    }

    /// Configure a query to return an error
    pub fn expect_error(&self, sql_pattern: impl Into<String>, error_msg: impl Into<String>) {
        let mut errors = self.query_errors.lock().unwrap();
        errors.insert(sql_pattern.into(), error_msg.into());
    }

    /// Set the last insert row ID (for simulating auto-increment)
    pub fn set_last_insert_rowid(&self, rowid: i64) {
        *self.last_insert_rowid.lock().unwrap() = rowid;
    }

    /// Verify that a query was executed
    pub fn assert_executed(&self, expected_sql: &str) -> bool {
        let queries = self.executed_queries.lock().unwrap();
        queries.iter().any(|q| q.sql.contains(expected_sql))
    }

    /// Get all executed queries (for inspection)
    pub fn get_executed_queries(&self) -> Vec<ExecutedQuery> {
        self.executed_queries.lock().unwrap().clone()
    }

    /// Clear all recorded queries and configured results
    pub fn reset(&self) {
        self.executed_queries.lock().unwrap().clear();
        self.query_results.lock().unwrap().clear();
        self.query_errors.lock().unwrap().clear();
        *self.last_insert_rowid.lock().unwrap() = 0;
    }

    /// Find matching query result (exact match or prefix match)
    fn find_query_result(&self, sql: &str) -> Option<Vec<MockRow>> {
        let results = self.query_results.lock().unwrap();

        // Try exact match first
        if let Some(rows) = results.get(sql) {
            return Some(rows.clone());
        }

        // Try prefix match
        for (pattern, rows) in results.iter() {
            if sql.starts_with(pattern) || sql.contains(pattern) {
                return Some(rows.clone());
            }
        }

        None
    }

    /// Find matching error configuration
    fn find_query_error(&self, sql: &str) -> Option<String> {
        let errors = self.query_errors.lock().unwrap();

        // Try exact match first
        if let Some(err) = errors.get(sql) {
            return Some(err.clone());
        }

        // Try prefix match
        for (pattern, err) in errors.iter() {
            if sql.starts_with(pattern) || sql.contains(pattern) {
                return Some(err.clone());
            }
        }

        None
    }
}

impl Default for MockDatabaseConnection {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseConnection for MockDatabaseConnection {
    fn execute(&self, sql: &str, params: &[DbValue]) -> Result<usize> {
        // Record the query
        {
            let mut queries = self.executed_queries.lock().unwrap();
            queries.push(ExecutedQuery {
                sql: sql.to_string(),
                params: params.to_vec(),
            });
        }

        // Check for configured error
        if let Some(err_msg) = self.find_query_error(sql) {
            return Err(DelightQLError::database_error(
                "Mock database error",
                err_msg,
            ));
        }

        // Simulate affected rows (just return 1 for success)
        Ok(1)
    }

    fn last_insert_rowid(&self) -> Result<i64> {
        Ok(*self.last_insert_rowid.lock().unwrap())
    }

    fn query_row_values(&self, sql: &str, params: &[DbValue]) -> Result<Option<Vec<DbValue>>> {
        // Record the query
        {
            let mut queries = self.executed_queries.lock().unwrap();
            queries.push(ExecutedQuery {
                sql: sql.to_string(),
                params: params.to_vec(),
            });
        }

        // Check for configured error
        if let Some(err_msg) = self.find_query_error(sql) {
            return Err(DelightQLError::database_error(
                "Mock database error",
                err_msg,
            ));
        }

        // Return configured result
        if let Some(rows) = self.find_query_result(sql) {
            if let Some(row) = rows.first() {
                return Ok(Some(row.values.clone()));
            }
        }

        Ok(None)
    }
}

/// Mock schema provider for testing without a real database
///
/// Allows programmatic definition of tables and columns for testing
/// schema-dependent logic (like resolution, validation, etc.)
pub struct MockSchemaProvider {
    /// Tables: (schema, table_name) -> columns
    tables: Arc<Mutex<HashMap<(Option<String>, String), Vec<SchemaColumnInfo>>>>,

    /// Namespace mappings: namespace_path -> backend_schema_name
    namespaces: Arc<Mutex<HashMap<String, String>>>,
}

impl MockSchemaProvider {
    /// Create a new empty mock schema provider
    pub fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
            namespaces: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Add a table with columns to the mock schema
    ///
    /// # Example
    /// ```ignore
    /// let mut schema = MockSchemaProvider::new();
    /// schema.add_table(None, "users", vec![
    ///     SchemaColumnInfo { name: "id".to_string(), nullable: false, position: 0 },
    ///     SchemaColumnInfo { name: "name".to_string(), nullable: true, position: 1 },
    /// ]);
    /// ```
    pub fn add_table(
        &self,
        schema: Option<&str>,
        table_name: &str,
        columns: Vec<SchemaColumnInfo>,
    ) {
        let key = (schema.map(|s| s.to_string()), table_name.to_string());
        self.tables.lock().unwrap().insert(key, columns);
    }

    /// Add a namespace mapping (for testing namespace resolution)
    ///
    /// # Example
    /// ```ignore
    /// schema.add_namespace("nba::players", "nba_schema");
    /// ```
    pub fn add_namespace(&self, path: &str, backend_schema: &str) {
        self.namespaces
            .lock()
            .unwrap()
            .insert(path.to_string(), backend_schema.to_string());
    }

    /// Remove all tables and namespaces (for test cleanup)
    pub fn clear(&self) {
        self.tables.lock().unwrap().clear();
        self.namespaces.lock().unwrap().clear();
    }

    /// Get all table names (for inspection)
    pub fn list_tables(&self) -> Vec<(Option<String>, String)> {
        self.tables
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

impl Default for MockSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseSchema for MockSchemaProvider {
    fn get_table_columns(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Option<Vec<SchemaColumnInfo>> {
        let key = (schema.map(|s| s.to_string()), table_name.to_string());
        self.tables.lock().unwrap().get(&key).cloned()
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> bool {
        let key = (schema.map(|s| s.to_string()), table_name.to_string());
        self.tables.lock().unwrap().contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_row() {
        let row = MockRow::new(
            vec!["id".to_string(), "name".to_string()],
            vec![DbValue::Integer(1), DbValue::Text("Alice".to_string())],
        );

        assert_eq!(row.column_count(), 2);
        assert_eq!(row.column_name(0).unwrap(), "id");
        assert_eq!(row.get_value(0).unwrap().as_integer(), Some(1));
        assert_eq!(
            row.get_value_by_name("name").unwrap().as_text(),
            Some("Alice")
        );
    }

    #[test]
    fn test_mock_connection() {
        let conn = MockDatabaseConnection::new();

        // Configure expected results
        conn.expect_query(
            "SELECT * FROM users",
            vec![MockRow::new(
                vec!["id".to_string(), "name".to_string()],
                vec![DbValue::Integer(1), DbValue::Text("Alice".to_string())],
            )],
        );

        // Execute query
        let result = conn.query_row_values("SELECT * FROM users", &[]).unwrap();
        assert!(result.is_some());

        // Verify execution
        assert!(conn.assert_executed("SELECT * FROM users"));
    }

    #[test]
    fn test_mock_error() {
        let conn = MockDatabaseConnection::new();

        // Configure error
        conn.expect_error("SELECT * FROM missing", "Table not found");

        // Execute query - should fail
        let result = conn.query_row_values("SELECT * FROM missing", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_schema() {
        let schema = MockSchemaProvider::new();

        // Add a table
        schema.add_table(
            None,
            "users",
            vec![
                SchemaColumnInfo {
                    name: "id".to_string(),
                    nullable: false,
                    position: 0,
                },
                SchemaColumnInfo {
                    name: "name".to_string(),
                    nullable: true,
                    position: 1,
                },
            ],
        );

        // Test table exists
        assert!(schema.table_exists(None, "users"));
        assert!(!schema.table_exists(None, "missing"));

        // Test get columns
        let columns = schema.get_table_columns(None, "users").unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
    }

    #[test]
    fn test_mock_namespace() {
        let schema = MockSchemaProvider::new();

        // Add namespace mapping
        schema.add_namespace("nba::players", "nba_schema");

        // Test namespace resolution
        let path = NamespacePath::from_parts(vec!["nba".to_string(), "players".to_string()]);
        let resolved = schema.resolve_namespace_path(&path).unwrap();
        assert_eq!(resolved, Some("nba_schema".to_string()));
    }
}
