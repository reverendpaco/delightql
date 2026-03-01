/// DuckDB Schema Provider
///
/// Implements schema introspection for DuckDB databases using information_schema.
use super::{ColumnInfo, Schema, TableInfo};
use delightql_types::{DelightQLError, Result};
use duckdb::{params, Connection};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// SQLite schema provider
#[derive(Debug)]
pub struct DuckDBSchema {
    conn: Arc<Mutex<Connection>>,
}

impl DuckDBSchema {
    /// Create a new SQLite schema provider
    pub fn new(conn: Arc<Mutex<Connection>>) -> Self {
        Self { conn }
    }

    /// Parse SQLite type affinity to a simplified type name
    fn parse_type_affinity(type_name: &str) -> String {
        let upper = type_name.to_uppercase();

        // SQLite type affinity rules
        if upper.contains("INT") {
            "INTEGER".to_string()
        } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
            "TEXT".to_string()
        } else if upper.contains("BLOB") || upper.is_empty() {
            "BLOB".to_string()
        } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
            "REAL".to_string()
        } else {
            "NUMERIC".to_string()
        }
    }
}

impl Schema for DuckDBSchema {
    fn table_exists(&self, _schema: Option<&str>, table: &str) -> Result<bool> {
        // Handle SQLite system tables specially since they don't appear in sqlite_master
        if table.starts_with("sqlite_") {
            let conn = self.conn.lock().map_err(|poison_err| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire database lock for schema operations",
                    format!("Connection was poisoned. Error: {}", poison_err),
                )
            })?;

            // Try PRAGMA table_info to see if the table exists
            let stmt_result = conn.prepare(&format!("PRAGMA table_info({})", table));
            return Ok(stmt_result.is_ok());
        }

        // For regular tables, use the default implementation
        self.get_table_info(_schema, table)
            .map(|_| true)
            .or_else(|e| match e {
                DelightQLError::TableNotFoundError { .. } => Ok(false),
                _ => Err(e),
            })
    }

    fn get_table_info(&self, _schema: Option<&str>, table: &str) -> Result<TableInfo> {
        // SQLite doesn't use schemas in the same way as other databases
        // The schema parameter is ignored

        let conn = self.conn.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Failed to acquire database lock for schema operations",
                format!("Connection was poisoned. Error: {}", poison_err),
            )
        })?;

        // First check if table exists (handle system tables specially)
        let exists: bool = if table.starts_with("sqlite_") {
            // For system tables, try PRAGMA table_info to check existence
            conn.prepare(&format!("PRAGMA table_info({})", table))
                .is_ok()
        } else {
            // For regular tables/views, check sqlite_master
            conn.query_row(
                "SELECT 1 FROM sqlite_master WHERE name = ?1",
                params![table],
                |_| Ok(true),
            )
            .unwrap_or(false)
        };

        if !exists {
            return Err(DelightQLError::table_not_found_error(
                table,
                "Table does not exist in database",
            ));
        }

        // Use PRAGMA table_info to get column information
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({})", table))
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to prepare PRAGMA statement",
                    e.to_string(),
                    Box::new(e),
                )
            })?;

        let columns = stmt
            .query_map([], |row| {
                let is_primary_key = row.get::<_, i32>(5)? != 0;
                let notnull = row.get::<_, i32>(3)? == 1;
                // In SQLite, PRIMARY KEY columns are implicitly NOT NULL
                let nullable = !notnull && !is_primary_key;

                Ok(ColumnInfo {
                    name: row.get(1)?,
                    data_type: Self::parse_type_affinity(&row.get::<_, String>(2)?),
                    nullable,
                    is_primary_key,
                    default_value: row.get(4)?,
                })
            })
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to query table info",
                    e.to_string(),
                    Box::new(e),
                )
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to collect column info",
                    e.to_string(),
                    Box::new(e),
                )
            })?;

        Ok(TableInfo {
            name: table.to_string(),
            schema: None,
            columns,
        })
    }

    fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>> {
        let conn = self.conn.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Failed to acquire database lock for schema operations",
                format!("Connection was poisoned. Error: {}", poison_err),
            )
        })?;

        let mut stmt = conn
            .prepare("SELECT DISTINCT name FROM sqlite_master ORDER BY name")
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to prepare table list query",
                    e.to_string(),
                    Box::new(e),
                )
            })?;

        let tables = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to query table list",
                    e.to_string(),
                    Box::new(e),
                )
            })?
            .collect::<std::result::Result<Vec<String>, _>>()
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to collect table names",
                    e.to_string(),
                    Box::new(e),
                )
            })?;

        Ok(tables)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;

    fn create_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();

        // Create test tables
        conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )
        .unwrap();

        conn.execute(
            "CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                price REAL,
                description TEXT,
                in_stock BOOLEAN DEFAULT 1
            )",
            [],
        )
        .unwrap();

        conn
    }

    #[test]
    fn test_sqlite_schema_get_table_info() {
        let conn = Arc::new(Mutex::new(create_test_db()));
        let schema = DuckDBSchema::new(conn);

        // Test users table
        let users = schema.get_table_info(None, "users").unwrap();
        assert_eq!(users.name, "users");
        assert_eq!(users.columns.len(), 5);

        // Check specific columns
        let id_col = users.get_column("id").unwrap();
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.data_type, "INTEGER");
        assert!(id_col.is_primary_key);
        assert!(!id_col.nullable);

        let name_col = users.get_column("name").unwrap();
        assert_eq!(name_col.name, "name");
        assert_eq!(name_col.data_type, "TEXT");
        assert!(!name_col.is_primary_key);
        assert!(!name_col.nullable);

        let age_col = users.get_column("age").unwrap();
        assert_eq!(age_col.data_type, "INTEGER");
        assert!(age_col.nullable);

        let created_col = users.get_column("created_at").unwrap();
        assert_eq!(
            created_col.default_value.as_deref(),
            Some("CURRENT_TIMESTAMP")
        );
    }

    #[test]
    fn test_sqlite_schema_list_tables() {
        let conn = Arc::new(Mutex::new(create_test_db()));
        let schema = DuckDBSchema::new(conn);

        let tables = schema.list_tables(None).unwrap();
        // Should include user tables
        assert!(tables.len() >= 2); // At least users and products
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"products".to_string()));

        // Should also include system tables like sqlite_sequence
        assert!(tables.contains(&"sqlite_sequence".to_string()));
    }

    #[test]
    fn test_sqlite_schema_table_exists() {
        let conn = Arc::new(Mutex::new(create_test_db()));
        let schema = DuckDBSchema::new(conn);

        assert!(schema.table_exists(None, "users").unwrap());
        assert!(schema.table_exists(None, "products").unwrap());
        assert!(!schema.table_exists(None, "nonexistent").unwrap());
    }

    #[test]
    fn test_sqlite_system_table_access() {
        let conn = Arc::new(Mutex::new(create_test_db()));
        let schema = DuckDBSchema::new(conn);

        // Test that system tables are accessible
        assert!(schema.table_exists(None, "sqlite_master").unwrap());

        // Test that we can get table info for sqlite_master
        let table_info = schema.get_table_info(None, "sqlite_master").unwrap();
        assert_eq!(table_info.name, "sqlite_master");

        // sqlite_master should have these standard columns
        assert!(table_info.get_column("type").is_some());
        assert!(table_info.get_column("name").is_some());
        assert!(table_info.get_column("tbl_name").is_some());
        assert!(table_info.get_column("rootpage").is_some());
        assert!(table_info.get_column("sql").is_some());
    }

    #[test]
    fn test_sqlite_type_parsing() {
        // Test various SQLite type names
        assert_eq!(DuckDBSchema::parse_type_affinity("INTEGER"), "INTEGER");
        assert_eq!(DuckDBSchema::parse_type_affinity("INT"), "INTEGER");
        assert_eq!(DuckDBSchema::parse_type_affinity("BIGINT"), "INTEGER");

        assert_eq!(DuckDBSchema::parse_type_affinity("TEXT"), "TEXT");
        assert_eq!(DuckDBSchema::parse_type_affinity("VARCHAR(255)"), "TEXT");
        assert_eq!(DuckDBSchema::parse_type_affinity("CHAR(10)"), "TEXT");

        assert_eq!(DuckDBSchema::parse_type_affinity("REAL"), "REAL");
        assert_eq!(DuckDBSchema::parse_type_affinity("FLOAT"), "REAL");
        assert_eq!(DuckDBSchema::parse_type_affinity("DOUBLE"), "REAL");

        assert_eq!(DuckDBSchema::parse_type_affinity("BLOB"), "BLOB");
        assert_eq!(DuckDBSchema::parse_type_affinity(""), "BLOB");

        assert_eq!(DuckDBSchema::parse_type_affinity("NUMERIC"), "NUMERIC");
        assert_eq!(DuckDBSchema::parse_type_affinity("DECIMAL"), "NUMERIC");
        assert_eq!(DuckDBSchema::parse_type_affinity("BOOLEAN"), "NUMERIC");
    }

    #[test]
    fn test_nonexistent_table() {
        let conn = Arc::new(Mutex::new(create_test_db()));
        let schema = DuckDBSchema::new(conn);

        let result = schema.get_table_info(None, "nonexistent");
        assert!(result.is_err());

        if let Err(DelightQLError::TranspilationError { message, .. }) = result {
            assert!(message.contains("not found"));
        } else {
            panic!("Expected TranspilationError");
        }
    }
}

// NOTE: ng_ng_ast module no longer exists after consolidation to single CLI
// This implementation can be removed if no longer needed
/*
// Implementation of the simpler ng_ng_ast::Schema trait
impl crate::ng_ng_ast::Schema for DuckDBSchema {
    fn table_exists(&self, name: &str) -> bool {
        // Use the existing Schema trait implementation
        Schema::table_exists(self, None, name).unwrap_or(false)
    }

    fn get_columns(&self, table: &str) -> Vec<String> {
        // Use the existing Schema trait implementation
        match self.get_table_info(None, table) {
            Ok(info) => info.columns.into_iter().map(|col| col.name).collect(),
            Err(_) => Vec::new(),
        }
    }
}
*/
