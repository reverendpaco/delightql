// SQLite Database Introspection Implementation
//
// Implements DatabaseIntrospector trait for user-facing SQLite databases.
// This is for transpilation TARGETS, not runtime infrastructure.

use super::introspect::introspect_sqlite_database;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::{DelightQLError, Result};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// SQLite introspector for user databases (transpilation targets)
///
/// This implementation queries SQLite's system catalogs to discover tables and views.
/// - Uses `sqlite_master` to find entities
/// - Uses `PRAGMA table_info` to discover columns
///
/// NOTE: This is for user-facing databases, not the runtime _bootstrap database.
pub struct SqliteIntrospector {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteIntrospector {
    /// Create a new SQLite introspector
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }
}

impl DatabaseIntrospector for SqliteIntrospector {
    fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
        let conn = self.connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Call the local introspect_sqlite_database() function
        // Schema is None because we're introspecting the main user database
        introspect_sqlite_database(&*conn, None).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to introspect SQLite database: {}", e),
                e.to_string(),
            )
        })
    }

    fn introspect_entities_in_schema(&self, schema: &str) -> Result<Vec<DiscoveredEntity>> {
        let conn = self.connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Call the local introspect_sqlite_database() function with schema parameter
        introspect_sqlite_database(&*conn, Some(schema)).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to introspect SQLite schema '{}': {}", schema, e),
                e.to_string(),
            )
        })
    }
}
