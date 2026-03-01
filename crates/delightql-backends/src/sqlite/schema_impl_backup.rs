/// SQLite Schema Provider Implementation
///
/// This module implements the SchemaProvider trait for SQLite databases,
/// providing schema introspection capabilities without exposing SQLite-specific
/// details to the rest of the system.
use crate::schema::{ColumnInfo, DatabaseSchema, SchemaProvider, TableInfo};
use delightql_types::{DelightQLError, Result};
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// SQLite-specific implementation of SchemaProvider
pub struct SqliteSchemaProvider {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteSchemaProvider {
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

    /// Load columns for a specific table using PRAGMA (requires connection lock)
    fn load_table_columns(&self, conn: &Connection, table_name: &str) -> Result<Vec<ColumnInfo>> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info('{}')", table_name))
            .map_err(|e| {
                DelightQLError::parse_error(format!(
                    "Failed to query columns for table '{}': {}",
                    table_name, e
                ))
            })?;

        let columns = stmt
            .query_map([], |row| {
                Ok(ColumnInfo {
                    name: row.get(1)?,
                    data_type: row.get(2)?,
                    is_nullable: row.get::<_, i32>(3)? == 0,
                    is_primary_key: row.get::<_, i32>(5)? == 1,
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

impl SchemaProvider for SqliteSchemaProvider {
    fn get_schema(&self) -> Result<DatabaseSchema> {
        // Query all tables from both sqlite_master and sqlite_temp_master
        let conn = self
            .connection
            .lock()
            .map_err(|e| DelightQLError::parse_error(format!("Failed to acquire lock: {}", e)))?;
        let mut stmt = conn
            .prepare(
                "
            SELECT DISTINCT name FROM sqlite_master
            UNION
            SELECT DISTINCT name FROM sqlite_temp_master
            ORDER BY name
        ",
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

        // Add user tables from both sqlite_master and sqlite_temp_master
        for table_name in table_names {
            let columns = self.load_table_columns(&conn, &table_name)?;
            schema.add_table(TableInfo {
                name: table_name,
                columns,
            });
        }

        // Also add system tables that don't appear in sqlite_master
        // Try to add sqlite_master itself using PRAGMA
        if let Ok(columns) = self.load_table_columns(&conn, "sqlite_master") {
            if !columns.is_empty() {
                schema.add_table(TableInfo {
                    name: "sqlite_master".to_string(),
                    columns,
                });
            }
        }

        // Try other common system tables
        for system_table in &["sqlite_schema", "sqlite_temp_master", "sqlite_temp_schema"] {
            if let Ok(columns) = self.load_table_columns(&conn, system_table) {
                if !columns.is_empty() {
                    schema.add_table(TableInfo {
                        name: system_table.to_string(),
                        columns,
                    });
                }
            }
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
        // For system tables starting with sqlite_, check if PRAGMA works
        if table_name.starts_with("sqlite_") {
            let conn = self.connection.lock().map_err(|e| {
                DelightQLError::parse_error(format!("Failed to acquire lock: {}", e))
            })?;
            // Try PRAGMA table_info to see if the table exists
            return match conn.prepare(&format!("PRAGMA table_info('{}')", table_name)) {
                Ok(mut stmt) => {
                    // If we can prepare the statement and it returns at least one row, table exists
                    match stmt.query_map([], |_| Ok(())) {
                        Ok(mut rows) => Ok(rows.next().is_some()),
                        Err(_) => Ok(false),
                    }
                }
                Err(_) => Ok(false),
            };
        }

        // For regular tables, check both sqlite_master and sqlite_temp_master
        let conn = self
            .connection
            .lock()
            .map_err(|e| DelightQLError::parse_error(format!("Failed to acquire lock: {}", e)))?;

        // First check sqlite_master for permanent tables
        let mut stmt = conn
            .prepare(
                "
            SELECT COUNT(*) FROM sqlite_master 
            WHERE name = ?1
        ",
            )
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to check table existence: {}", e))
            })?;

        let count: i32 = stmt
            .query_row([table_name], |row| row.get(0))
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to query table existence: {}", e))
            })?;

        if count > 0 {
            return Ok(true);
        }

        // If not found in sqlite_master, check sqlite_temp_master for temporary tables
        let mut temp_stmt = conn
            .prepare(
                "
            SELECT COUNT(*) FROM sqlite_temp_master 
            WHERE name = ?1
        ",
            )
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to check temp table existence: {}", e))
            })?;

        let temp_count: i32 = temp_stmt
            .query_row([table_name], |row| row.get(0))
            .map_err(|e| {
                DelightQLError::parse_error(format!("Failed to query temp table existence: {}", e))
            })?;

        Ok(temp_count > 0)
    }

    fn list_tables(&self) -> Result<Vec<String>> {
        let conn = self
            .connection
            .lock()
            .map_err(|e| DelightQLError::parse_error(format!("Failed to acquire lock: {}", e)))?;
        let mut stmt = conn
            .prepare(
                "
            SELECT DISTINCT name FROM sqlite_master 
            ORDER BY name
        ",
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
