/// DuckDB Schema Provider Implementation
///
/// This module implements the SchemaProvider trait for DuckDB databases,
/// providing schema introspection capabilities without exposing DuckDB-specific
/// details to the rest of the system.
use crate::schema_base::{ColumnInfo, DatabaseSchema, SchemaProvider, TableInfo};
use delightql_types::{DelightQLError, Result};
use duckdb::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// SQLite-specific implementation of SchemaProvider
pub struct DuckDBSchemaProvider {
    connection: Arc<Mutex<Connection>>,
}

impl DuckDBSchemaProvider {
    /// Create a new SQLite schema provider from a database path
    pub fn new(database_path: &Path) -> Result<Self> {
        let connection = Connection::open(database_path).map_err(|e| {
            DelightQLError::parse_error(format!("Failed to open SQLite database: {}", e))
        })?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Create a new SQLite schema provider from an existing connection
    pub fn from_connection(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }

    /// Load columns for a specific table using information_schema (requires connection lock)
    fn load_table_columns(&self, conn: &Connection, table_name: &str) -> Result<Vec<ColumnInfo>> {
        let mut stmt = conn
            .prepare(
                "SELECT column_name, data_type, is_nullable,
                        CASE WHEN column_name IN (
                            SELECT column_name FROM information_schema.key_column_usage
                            WHERE table_name = ? AND constraint_name LIKE '%_pkey'
                        ) THEN 1 ELSE 0 END as is_pk
                 FROM information_schema.columns
                 WHERE table_name = ?
                 ORDER BY ordinal_position",
            )
            .map_err(|e| {
                DelightQLError::parse_error(format!(
                    "Failed to query columns for table '{}': {}",
                    table_name, e
                ))
            })?;

        let columns = stmt
            .query_map([table_name, table_name], |row| {
                Ok(ColumnInfo {
                    name: row.get(0)?,
                    data_type: row.get(1)?,
                    is_nullable: row.get::<_, String>(2)? == "YES",
                    is_primary_key: row.get::<_, i32>(3)? == 1,
                })
            })
            .map_err(|e| {
                DelightQLError::parse_error(format!(
                    "Failed to read columns for table '{}': {}",
                    table_name, e
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                DelightQLError::parse_error(format!(
                    "Failed to collect columns for table '{}': {}",
                    table_name, e
                ))
            })?;

        Ok(columns)
    }
}

impl SchemaProvider for DuckDBSchemaProvider {
    fn get_schema(&self) -> Result<DatabaseSchema> {
        // Query all tables from information_schema
        let conn = self
            .connection
            .lock()
            .map_err(|e| DelightQLError::parse_error(format!("Failed to acquire lock: {}", e)))?;
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT table_name
                 FROM information_schema.tables
                 WHERE table_schema = 'main'
                 ORDER BY table_name",
            )
            .map_err(|e| DelightQLError::parse_error(format!("Failed to query tables: {}", e)))?;

        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| DelightQLError::parse_error(format!("Failed to read table names: {}", e)))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to collect table names: {}", e))
            })?;

        let mut schema = DatabaseSchema::new();

        // Add all tables from information_schema
        for table_name in table_names {
            let columns = self.load_table_columns(&conn, &table_name)?;
            schema.add_table(TableInfo {
                name: table_name,
                columns,
            });
        }

        log::debug!("Loaded schema with {} tables", schema.tables.len());
        Ok(schema)
    }

    fn get_table_info(&self, table_name: &str) -> Result<TableInfo> {
        // First check if table exists
        if !self.table_exists(table_name)? {
            return Err(DelightQLError::parse_error(format!(
                "Table '{}' does not exist",
                table_name
            )));
        }

        let conn = self
            .connection
            .lock()
            .map_err(|e| DelightQLError::parse_error(format!("Failed to acquire lock: {}", e)))?;
        let columns = self.load_table_columns(&conn, table_name)?;

        Ok(TableInfo {
            name: table_name.to_string(),
            columns,
        })
    }

    fn table_exists(&self, table_name: &str) -> Result<bool> {
        let conn = self
            .connection
            .lock()
            .map_err(|e| DelightQLError::parse_error(format!("Failed to acquire lock: {}", e)))?;

        let mut stmt = conn
            .prepare(
                "SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_name = ?1 AND table_schema = 'main'",
            )
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to check table existence: {}", e))
            })?;

        let count: i32 = stmt
            .query_row([table_name], |row| row.get(0))
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to query table existence: {}", e))
            })?;

        Ok(count > 0)
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self
            .connection
            .lock()
            .map_err(|e| DelightQLError::parse_error(format!("Failed to acquire lock: {}", e)))?;
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT table_name
                 FROM information_schema.tables
                 WHERE table_schema = 'main'
                 ORDER BY table_name",
            )
            .map_err(|e| DelightQLError::parse_error(format!("Failed to list tables: {}", e)))?;

        let tables = stmt
            .query_map([], |row| row.get(0))
            .map_err(|e| DelightQLError::parse_error(format!("Failed to read table list: {}", e)))?
            .collect::<std::result::Result<Vec<String>, _>>()
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to collect table list: {}", e))
            })?;

        Ok(tables)
    }
}
