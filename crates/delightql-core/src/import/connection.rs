// Connection Registration
//
// This module handles registering database connections in the bootstrap metadata.
// Connections represent physical database connections that cartridges can reference.
//
// See: documentation/design/ddl/SYS-NS-CARTRIDGE-CONNECTION.md

use anyhow::Result;
use rusqlite::Connection;

/// Register a new connection in the bootstrap database
///
/// This is a reusable method following the pattern of install_cartridge(),
/// create_namespace(), activate_entity(), etc.
///
/// # Arguments
/// * `conn` - Bootstrap database connection
/// * `connection_uri` - URI identifying the connection (e.g., "user://main")
/// * `connection_type` - Type ID from connection_type_enum (1=sqlite-file, 2=sqlite-memory, etc.)
/// * `description` - Human-readable description
///
/// # Returns
/// The connection_id of the newly registered connection
///
/// # Example
/// ```
/// use delightql_core::import::connection::register_connection;
/// use rusqlite::Connection;
///
/// let conn = Connection::open_in_memory().unwrap();
/// // ... initialize bootstrap schema ...
///
/// // Register user SQLite file connection
/// let conn_id = register_connection(
///     &conn,
///     "user://main",
///     1,  // sqlite-file
///     "User target database"
/// ).unwrap();
/// ```
pub fn register_connection(
    conn: &Connection,
    connection_uri: &str,
    connection_type: i32,
    description: &str,
) -> Result<i32> {
    // If a connection with this URI already exists, return its ID.
    // SQLite allows ATTACH of the same file multiple times, so
    // mounting the same database under different namespaces is valid.
    if let Ok(existing_id) = conn.query_row(
        "SELECT id FROM connection WHERE connection_uri = ?1",
        [connection_uri],
        |row| row.get::<_, i32>(0),
    ) {
        return Ok(existing_id);
    }

    conn.execute(
        "INSERT INTO connection (connection_uri, connection_type, description)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![connection_uri, connection_type, description],
    )?;

    Ok(conn.last_insert_rowid() as i32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap;

    #[test]
    fn test_register_connection() {
        let conn = Connection::open_in_memory().unwrap();
        bootstrap::initialize_bootstrap_db(&conn).unwrap();

        // Register a SQLite file connection
        let conn_id = register_connection(
            &conn,
            "user://test",
            1, // sqlite-file
            "Test database",
        )
        .unwrap();

        // Verify it was created
        let (uri, conn_type, desc): (String, i32, String) = conn
            .query_row(
                "SELECT connection_uri, connection_type, description FROM connection WHERE id = ?1",
                [conn_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(uri, "user://test");
        assert_eq!(conn_type, 1);
        assert_eq!(desc, "Test database");
    }

    #[test]
    fn test_register_multiple_connections() {
        let conn = Connection::open_in_memory().unwrap();
        bootstrap::initialize_bootstrap_db(&conn).unwrap();

        // Register SQLite connection
        let sqlite_id = register_connection(&conn, "user://sqlite", 1, "SQLite").unwrap();

        // Register DuckDB connection
        let duckdb_id = register_connection(&conn, "user://duckdb", 4, "DuckDB").unwrap();

        // Verify both exist and have different IDs
        assert_ne!(sqlite_id, duckdb_id);

        let count: i32 = conn
            .query_row(
                "SELECT COUNT(*) FROM connection WHERE id IN (?1, ?2)",
                rusqlite::params![sqlite_id, duckdb_id],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(count, 2);
    }
}
