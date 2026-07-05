// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
/// * `resource_uri` - what the user named (worldly spelling; the label
///   "session:primary" for the pre-mount placeholder)
/// * `mechanism` - how DelightQL reaches it (in-process|fatboy|siso|attach)
/// * `identity` - what the resource asserts about itself (method-prefixed)
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
///     "data/users.db",
///     "in-process",
///     Some("realpath:/abs/data/users.db"),
///     1,  // sqlite-file
///     "User target database"
/// ).unwrap();
/// ```
pub fn register_connection(
    conn: &Connection,
    resource_uri: &str,
    mechanism: &str,
    identity: Option<&str>,
    connection_type: i32,
    description: &str,
) -> Result<i32> {
    // Dedupe keys on IDENTITY when the resource asserted one — two
    // spellings of the same server (postgres://localhost:5433/db vs
    // postgres:///db), or two paths to the same file (the symlink trap),
    // fold into one connection row. Without identity, fall back to
    // (resource_uri, mechanism) string equality.
    if let Some(id) = identity {
        if let Ok(existing_id) = conn.query_row(
            "SELECT id FROM connection WHERE identity = ?1",
            [id],
            |row| row.get::<_, i32>(0),
        ) {
            return Ok(existing_id);
        }
    }
    if let Ok(existing_id) = conn.query_row(
        "SELECT id FROM connection WHERE resource_uri = ?1 AND mechanism = ?2 \
         AND identity IS NULL",
        rusqlite::params![resource_uri, mechanism],
        |row| row.get::<_, i32>(0),
    ) {
        return Ok(existing_id);
    }

    conn.execute(
        "INSERT INTO connection (resource_uri, mechanism, identity, connection_type, description)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![resource_uri, mechanism, identity, connection_type, description],
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
            "some/test.db",
            "in-process",
            Some("realpath:/abs/some/test.db"),
            1, // sqlite-file
            "Test database",
        )
        .unwrap();

        // Verify it was created
        let (uri, mech, ident, conn_type, desc): (String, String, String, i32, String) = conn
            .query_row(
                "SELECT resource_uri, mechanism, identity, connection_type, description \
                 FROM connection WHERE id = ?1",
                [conn_id],
                |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
                },
            )
            .unwrap();

        assert_eq!(uri, "some/test.db");
        assert_eq!(mech, "in-process");
        assert_eq!(ident, "realpath:/abs/some/test.db");
        assert_eq!(conn_type, 1);
        assert_eq!(desc, "Test database");

        // Identity-keyed dedupe: a different SPELLING of the same resource
        // folds into the same row.
        let again = register_connection(
            &conn,
            "./some/../some/test.db",
            "in-process",
            Some("realpath:/abs/some/test.db"),
            1,
            "same file, different spelling",
        )
        .unwrap();
        assert_eq!(again, conn_id);
    }

    #[test]
    fn test_register_multiple_connections() {
        let conn = Connection::open_in_memory().unwrap();
        bootstrap::initialize_bootstrap_db(&conn).unwrap();

        // Register SQLite connection
        let sqlite_id =
            register_connection(&conn, "a.db", "in-process", None, 1, "SQLite").unwrap();

        // Register DuckDB connection
        let duckdb_id =
            register_connection(&conn, "b.duckdb", "fatboy", None, 4, "DuckDB").unwrap();

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
