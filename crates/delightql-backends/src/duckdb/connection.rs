/// DuckDB Connection Management
///
/// Provides thread-safe connection management for DuckDB databases,
/// supporting both in-memory and file-based databases.
use delightql_types::{DelightQLError, Result};
use duckdb::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Connection information structure for DuckDB databases
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionInfo {
    pub database_type: String,
    pub path: Option<String>,
    pub is_memory: bool,
    pub is_connected: bool,
}

/// Thread-safe DuckDB connection manager
#[derive(Clone)]
pub struct DuckDBConnectionManager {
    connection: Arc<Mutex<Connection>>,
    info: ConnectionInfo,
}

impl DuckDBConnectionManager {
    /// Create a new connection to an in-memory DuckDB database
    pub fn new_memory() -> Result<Self> {
        let connection = Connection::open_in_memory().map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        let connection = Arc::new(Mutex::new(connection));
        let info = ConnectionInfo {
            database_type: "DuckDB".to_string(),
            path: None,
            is_memory: true,
            is_connected: true,
        };

        Ok(DuckDBConnectionManager { connection, info })
    }

    /// Create a new connection to a file-based DuckDB database
    pub fn new_file(path: &str) -> Result<Self> {
        // Ensure the parent directory exists if creating a new database
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(DelightQLError::IoError)?;
            }
        }

        let connection = Connection::open(path).map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        let connection = Arc::new(Mutex::new(connection));
        let info = ConnectionInfo {
            database_type: "DuckDB".to_string(),
            path: Some(path.to_string()),
            is_memory: false,
            is_connected: true,
        };

        Ok(DuckDBConnectionManager { connection, info })
    }

    /// Create a new connection to an existing file-based DuckDB database
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

        let connection = Connection::open(path).map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        let connection = Arc::new(Mutex::new(connection));
        let info = ConnectionInfo {
            database_type: "DuckDB".to_string(),
            path: Some(path.to_string()),
            is_memory: false,
            is_connected: true,
        };

        Ok(DuckDBConnectionManager { connection, info })
    }

    /// Test if the connection is still alive and responsive
    pub fn is_connected(&self) -> bool {
        if let Ok(conn) = self.connection.lock() {
            // Try a simple query to test connectivity
            conn.execute_batch("").is_ok()
        } else {
            false
        }
    }

    /// Get the raw connection Arc for schema introspection
    /// This is needed for DuckDBSchema to access database metadata
    pub fn get_connection_arc(&self) -> Arc<Mutex<Connection>> {
        self.connection.clone()
    }

    /// Interrupt any ongoing database operations
    /// This can be safely called from another thread to cancel a running query
    ///
    /// NOTE: DuckDB doesn't currently support interrupt handles like SQLite
    pub fn interrupt(&self) {
        // DuckDB doesn't support interrupt handles yet
        // This is a no-op for now
        // TODO: Implement when DuckDB adds interrupt support
    }

    /// Execute a simple test query to verify connectivity
    pub fn test_connection(&self) -> Result<()> {
        let conn = self.connection.lock().map_err(|poison_err| {
            // Attempt to recover from poison by using the data anyway
            // In production, you might want to reinitialize the connection instead
            DelightQLError::connection_poison_error(
                "Database connection lock was poisoned",
                format!("Previous operation panicked. Error: {}", poison_err),
            )
        })?;

        conn.query_row("SELECT 1", [], |_| Ok(()))
            .map_err(|e| DelightQLError::database_error(format!("DuckDB error: {}", e), String::new()))?;

        Ok(())
    }

    /// Close the connection explicitly
    pub fn close(self) -> Result<()> {
        // The connection will be dropped when the Arc goes out of scope
        // DuckDB handles cleanup automatically
        Ok(())
    }

    /// Get connection information/metadata
    pub fn connection_info(&self) -> Result<ConnectionInfo> {
        let mut info = self.info.clone();
        info.is_connected = self.is_connected();
        Ok(info)
    }

    /// Get a reference to the underlying connection (for internal use)
    pub(crate) fn get_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }
}

// The DuckDBConnectionManager struct implements the trait interface defined in tests
// The methods are already implemented above with the same signatures

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_memory_connection() {
        let manager =
            DuckDBConnectionManager::new_memory().expect("Failed to create memory connection");

        assert!(manager.is_connected());
        assert!(manager.test_connection().is_ok());

        let info = manager.connection_info().unwrap();
        assert_eq!(info.database_type, "DuckDB");
        assert!(info.is_memory);
        assert!(info.is_connected);
        assert!(info.path.is_none());
    }

    #[test]
    fn test_file_connection() {
        let temp_file = NamedTempFile::new().unwrap();
        let temp_path = temp_file.path().to_str().unwrap();
        let manager =
            DuckDBConnectionManager::new_file(temp_path).expect("Failed to create file connection");

        assert!(manager.is_connected());
        assert!(manager.test_connection().is_ok());

        let info = manager.connection_info().unwrap();
        assert_eq!(info.database_type, "DuckDB");
        assert!(!info.is_memory);
        assert!(info.is_connected);
        assert_eq!(info.path, Some(temp_path.to_string()));

        // Clean up
        let _ = std::fs::remove_file(temp_path);
    }
}
