/// SQLite Connection Management
///
/// Provides thread-safe connection management for SQLite databases,
/// supporting both in-memory and file-based databases.
use delightql_types::{DelightQLError, Result};
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Helper to convert rusqlite errors to DelightQL errors
fn rusqlite_to_dql_error(e: rusqlite::Error, operation: &str) -> DelightQLError {
    DelightQLError::DatabaseOperationError {
        message: format!("{} failed", operation),
        details: format!("SQLite error: {}", e),
        source: Some(Box::new(e)),
        subcategory: None,
    }
}

/// Connection information structure for SQLite databases
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionInfo {
    pub database_type: String,
    pub path: Option<String>,
    pub is_memory: bool,
    pub is_connected: bool,
}

/// Thread-safe SQLite connection manager
#[derive(Clone)]
pub struct SqliteConnectionManager {
    connection: Arc<Mutex<Connection>>,
    info: ConnectionInfo,
}

impl SqliteConnectionManager {
    /// Create a new connection to an in-memory SQLite database
    pub fn new_memory() -> Result<Self> {
        let connection = Connection::open_in_memory().map_err(|e| rusqlite_to_dql_error(e, "Open in-memory database"))?;

        // Attach system schemas (SQLite-specific operation)
        // These are session-specific temporary schemas for the user database
        // NOTE: _bootstrap is NO LONGER attached here - it's a separate internal
        // SQLite connection managed by delightql-core as an engine implementation detail
        connection
            .execute_batch(
                "ATTACH DATABASE ':memory:' AS 'sys';",
            )
            .map_err(|e| rusqlite_to_dql_error(e, "Attach system schemas"))?;

        let connection = Arc::new(Mutex::new(connection));

        let info = ConnectionInfo {
            database_type: "SQLite".to_string(),
            path: None,
            is_memory: true,
            is_connected: true,
        };

        Ok(SqliteConnectionManager { connection, info })
    }

    /// Create a new connection to a file-based SQLite database
    pub fn new_file(path: &str) -> Result<Self> {
        // Ensure the parent directory exists if creating a new database
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(DelightQLError::IoError)?;
            }
        }

        let connection = Connection::open(path).map_err(|e| rusqlite_to_dql_error(e, "Open database file"))?;

        // Attach system schemas (SQLite-specific operation)
        connection
            .execute_batch(
                "ATTACH DATABASE ':memory:' AS 'sys';",
            )
            .map_err(|e| rusqlite_to_dql_error(e, "Attach system schemas"))?;

        let connection = Arc::new(Mutex::new(connection));

        let info = ConnectionInfo {
            database_type: "SQLite".to_string(),
            path: Some(path.to_string()),
            is_memory: false,
            is_connected: true,
        };

        Ok(SqliteConnectionManager { connection, info })
    }

    /// Create a new connection to an existing file-based SQLite database
    /// Returns an error if the database file doesn't exist
    pub fn new_file_existing(path: &str) -> Result<Self> {
        // Check if database file exists
        if !Path::new(path).exists() {
            return Err(DelightQLError::ParseError {
                message: format!(
                    "Database file '{}' does not exist. Use --make-new-db-if-missing to create it.",
                    path
                ),
                source: None,
                subcategory: None,
            });
        }

        let connection = Connection::open(path).map_err(|e| rusqlite_to_dql_error(e, "Open database file"))?;

        // Attach system schemas (SQLite-specific operation)
        connection
            .execute_batch(
                "ATTACH DATABASE ':memory:' AS 'sys';",
            )
            .map_err(|e| rusqlite_to_dql_error(e, "Attach system schemas"))?;

        let connection = Arc::new(Mutex::new(connection));

        let info = ConnectionInfo {
            database_type: "SQLite".to_string(),
            path: Some(path.to_string()),
            is_memory: false,
            is_connected: true,
        };

        Ok(SqliteConnectionManager { connection, info })
    }

    /// Create a connection manager from an existing Arc<Mutex<Connection>>
    /// This is useful when you want to share an existing connection (like from DelightQLSystem)
    pub fn from_arc(connection: Arc<Mutex<Connection>>) -> Self {
        // Try to determine if this is a memory database by querying
        let (path, is_memory) = if let Ok(conn) = connection.lock() {
            match conn.query_row(
                "SELECT file FROM pragma_database_list WHERE name = 'main'",
                [],
                |row| row.get::<_, String>(0),
            ) {
                Ok(file) if file.is_empty() || file == ":memory:" => (None, true),
                Ok(file) => (Some(file), false),
                Err(_) => (None, true), // Assume memory if we can't determine
            }
        } else {
            (None, true)
        };

        let info = ConnectionInfo {
            database_type: "SQLite".to_string(),
            path,
            is_memory,
            is_connected: true,
        };

        SqliteConnectionManager { connection, info }
    }

    /// Get the underlying Arc<Mutex<Connection>> for sharing
    pub fn get_connection_arc(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }

    /// Get a reference to the Arc<Mutex<Connection>> for backwards compatibility
    pub fn get_connection(&self) -> &Arc<Mutex<Connection>> {
        &self.connection
    }

    /// Test if the connection is working
    pub fn test_connection(&self) -> Result<()> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!("Previous operation panicked. Error: {}", poison_err),
            )
        })?;

        conn.query_row("SELECT 1", [], |_| Ok(()))
            .map_err(|e| rusqlite_to_dql_error(e, "Test connection"))?;

        Ok(())
    }

    /// Get connection information
    pub fn connection_info(&self) -> Result<ConnectionInfo> {
        Ok(self.info.clone())
    }

    /// Attach another SQLite database file with a schema name
    pub fn attach_database_file(&self, db_path: &str, schema_name: &str) -> Result<()> {
        let conn = self.connection.lock().map_err(|poison_err| {
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!("Previous operation panicked. Error: {}", poison_err),
            )
        })?;

        conn.execute(
            &format!("ATTACH DATABASE '{}' AS {}", db_path, schema_name),
            [],
        )
        .map_err(|e| rusqlite_to_dql_error(e, "Attach database"))?;

        Ok(())
    }
}

// The SqliteConnectionManager struct implements the trait interface defined in tests
// The methods are already implemented above with the same signatures

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_memory_connection() {
        let manager =
            SqliteConnectionManager::new_memory().expect("Failed to create memory connection");

        assert!(manager.is_connected());
        assert!(manager.test_connection().is_ok());

        let info = manager.connection_info().unwrap();
        assert_eq!(info.database_type, "SQLite");
        assert!(info.is_memory);
        assert!(info.is_connected);
        assert!(info.path.is_none());
    }

    #[test]
    fn test_file_connection() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path().to_str().unwrap();
        let manager =
            SqliteConnectionManager::new_file(temp_path).expect("Failed to create file connection");

        assert!(manager.is_connected());
        assert!(manager.test_connection().is_ok());

        let info = manager.connection_info().unwrap();
        assert_eq!(info.database_type, "SQLite");
        assert!(!info.is_memory);
        assert!(info.is_connected);
        assert_eq!(info.path, Some(temp_path.to_string()));

        // Clean up
        let _ = std::fs::remove_file(temp_path);
    }
}
