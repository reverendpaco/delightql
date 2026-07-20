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

/// True if a `resource_uri` embeds credentials — a password in the URL
/// userinfo (`scheme://user:pass@host`).
///
/// Credentials must never enter the connection catalog: `resource_uri` is
/// stored, deduped on, and DQL-exposed via `sys::connections.connection(*)`,
/// so a URL-embedded password would persist into session metadata and every
/// dedup query — the worst possible residency. Credentials are sourced from
/// the environment instead (e.g. `PGPASSWORD`), never typed into the URI.
///
/// This is the CORE-side chokepoint for that invariant: every host (CLI,
/// wasm, cabi, adapters) funnels connection registration through
/// [`register_connection`], so the guarantee holds by construction, not by
/// each frontend remembering to check. Non-URL resource_uris (`session:primary`,
/// `:memory:`, file paths, `catalog://sys::meta`) have no authority/userinfo
/// and are never rejected. Username-only (`scheme://user@host`, no password)
/// is allowed — a username is not a secret.
fn resource_uri_embeds_credentials(uri: &str) -> bool {
    // Only URL-form (has an authority component) can carry userinfo.
    let Some((_scheme, rest)) = uri.split_once("://") else {
        return false;
    };
    // Authority is up to the first '/', '?', or '#'.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    // Userinfo is everything before the first '@'; a ':' in it is a password.
    match authority.split_once('@') {
        Some((userinfo, _host)) => userinfo.contains(':'),
        None => false,
    }
}

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
/// ```ignore
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
    // Structural guarantee (SYS-NAMESPACE-TAXONOMY.md credential-sourcing
    // policy): credentials never enter the connection catalog. Enforced here,
    // at the single core sink, so every host inherits it — not at a frontend
    // that a new host could bypass. The message deliberately does NOT echo the
    // offending URI (that would re-leak the password into logs/errors).
    if resource_uri_embeds_credentials(resource_uri) {
        anyhow::bail!(
            "connection resource_uri must not embed credentials — a password in \
             the URL would persist into the connection catalog and every dedup \
             query. Source it from the environment (e.g. PGPASSWORD) instead."
        );
    }

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
    fn credential_detection_rejects_only_passwords() {
        // Rejected: a password in the userinfo.
        assert!(resource_uri_embeds_credentials("postgres://alice:hunter2@host/db"));
        assert!(resource_uri_embeds_credentials("postgres://:secret@host:5432/db"));
        assert!(resource_uri_embeds_credentials("mysql://u:p@h/d?x=1"));
        // Allowed: username-only (not a secret), port colons, and every
        // non-URL resource_uri form that legitimately flows in.
        assert!(!resource_uri_embeds_credentials("postgres://alice@host/db"));
        assert!(!resource_uri_embeds_credentials("postgres://host:5432/db"));
        assert!(!resource_uri_embeds_credentials("postgres:///db"));
        assert!(!resource_uri_embeds_credentials("session:primary"));
        assert!(!resource_uri_embeds_credentials(":memory:"));
        assert!(!resource_uri_embeds_credentials("data/users.db"));
        assert!(!resource_uri_embeds_credentials("catalog://sys::meta"));
        assert!(!resource_uri_embeds_credentials("realpath:/abs/db.db"));
    }

    #[test]
    fn register_connection_refuses_embedded_credentials() {
        // The guard fires before any DB access, so no schema is needed.
        let conn = Connection::open_in_memory().unwrap();
        let err = register_connection(
            &conn,
            "postgres://alice:hunter2@host/db",
            "fatboy",
            None,
            2,
            "leaky",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("must not embed credentials"), "{msg}");
        // And it does NOT echo the password back into the error.
        assert!(!msg.contains("hunter2"), "error re-leaked the password: {msg}");
    }

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
