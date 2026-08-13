// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
use delightql_types::{DelightQLError, Result};
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
    fn get_table_columns(&self, schema: Option<&str>, table_name: &str) -> Result<Option<Vec<ColumnInfo>>> {
        let conn = self.connection.lock().map_err(|error| {
            DelightQLError::connection_poison_error(
                "Failed to acquire SQLite schema connection",
                error.to_string(),
            )
        })?;

        // For SQLite, schema refers to attached databases (main, temp, or attached name)
        // Use table_xinfo instead of table_info to include generated columns
        let query = if let Some(schema_name) = schema {
            format!("PRAGMA {}.table_xinfo('{}')", schema_name, table_name)
        } else {
            format!("PRAGMA table_xinfo('{}')", table_name)
        };

        let mut stmt = conn.prepare(&query).map_err(|error| {
            DelightQLError::database_error("Failed to prepare SQLite schema query", error.to_string())
        })?;
        let columns = stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;  // Column name is at index 1
                let decltype: String = row.get(2)?;  // Declared type at index 2 ('' if none)
                let notnull: i32 = row.get(3)?;  // NOT NULL flag is at index 3
                let cid: i32 = row.get(0)?;      // Column ID is at index 0

                Ok(ColumnInfo {
                    name: name.into(),
                    nullable: notnull == 0,  // notnull=0 means nullable
                    position: (cid + 1) as usize,  // Convert 0-based to 1-based
                    declared_type: (!decltype.is_empty()).then_some(decltype),
                })
            })
            .map_err(|error| {
                DelightQLError::database_error("Failed to query SQLite schema", error.to_string())
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                DelightQLError::database_error("Failed to read SQLite schema", error.to_string())
            })?;

        if columns.is_empty() {
            Ok(None)
        } else {
            Ok(Some(columns))
        }
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> Result<bool> {
        let conn = self.connection.lock().map_err(|error| {
            DelightQLError::connection_poison_error(
                "Failed to acquire SQLite schema connection",
                error.to_string(),
            )
        })?;

        // For SQLite, check if we can get table_xinfo successfully
        let query = if let Some(schema_name) = schema {
            format!("PRAGMA {}.table_xinfo('{}')", schema_name, table_name)
        } else {
            format!("PRAGMA table_xinfo('{}')", table_name)
        };

        let mut stmt = conn.prepare(&query).map_err(|error| {
            DelightQLError::database_error("Failed to prepare SQLite schema query", error.to_string())
        })?;
        let mut rows = stmt.query_map([], |_| Ok(())).map_err(|error| {
            DelightQLError::database_error("Failed to query SQLite schema", error.to_string())
        })?;
        match rows.next() {
            None => Ok(false),
            Some(Ok(())) => Ok(true),
            Some(Err(error)) => Err(DelightQLError::database_error(
                "Failed to read SQLite schema",
                error.to_string(),
            )),
        }
    }
}
