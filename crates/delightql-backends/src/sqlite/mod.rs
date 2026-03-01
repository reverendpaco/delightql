//! SQLite backend implementation for DelightQL

pub mod connection;
pub mod db_adapter;
pub mod executor;
pub mod introspect;
pub mod introspection;
pub mod value;

pub use db_adapter::SqliteConnection;
pub use introspect::introspect_sqlite_database;
pub use introspection::SqliteIntrospector;

// Re-export the schema from the parent module (it was already here as the original mod.rs)
use delightql_types::schema::{ColumnInfo, DatabaseSchema};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// Dynamic schema provider that queries SQLite directly
pub struct DynamicSqliteSchema {
    /// User database connection
    connection: Arc<Mutex<Connection>>,
}

impl DynamicSqliteSchema {
    /// Create from an existing user database connection
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self {
            connection,
        }
    }
}

impl DatabaseSchema for DynamicSqliteSchema {
    fn get_table_columns(&self, schema: Option<&str>, table_name: &str) -> Option<Vec<ColumnInfo>> {
        let conn = self.connection.lock().ok()?;

        // For SQLite, schema refers to attached databases (main, temp, or attached name)
        // Use table_xinfo instead of table_info to include generated columns
        let query = if let Some(schema_name) = schema {
            format!("PRAGMA {}.table_xinfo('{}')", schema_name, table_name)
        } else {
            format!("PRAGMA table_xinfo('{}')", table_name)
        };

        let mut stmt = conn.prepare(&query).ok()?;
        let columns = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;  // Column name is at index 1
                let notnull: i32 = row.get(3)?;  // NOT NULL flag is at index 3
                let cid: i32 = row.get(0)?;      // Column ID is at index 0

                Ok(ColumnInfo {
                    name: name.into(),
                    nullable: notnull == 0,  // notnull=0 means nullable
                    position: (cid + 1) as usize,  // Convert 0-based to 1-based
                })
            })
            .ok()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .ok()?;

        if columns.is_empty() {
            None
        } else {
            Some(columns)
        }
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> bool {
        let conn = match self.connection.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        // For SQLite, check if we can get table_xinfo successfully
        let query = if let Some(schema_name) = schema {
            format!("PRAGMA {}.table_xinfo('{}')", schema_name, table_name)
        } else {
            format!("PRAGMA table_xinfo('{}')", table_name)
        };

        let result = match conn.prepare(&query) {
            Ok(mut stmt) => {
                // If we can query and get at least one row, table exists
                match stmt.query_map([], |_| Ok(())) {
                    Ok(mut rows) => rows.next().is_some(),
                    Err(_) => false,
                }
            }
            Err(_) => false,
        };
        result
    }
}
