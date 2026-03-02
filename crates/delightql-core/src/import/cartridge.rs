// Cartridge Installation and Management
//
// This module handles installing cartridges from various sources:
// - Database connections (SQLite, PostgreSQL, etc.)
// - DQL/SQL files
// - Binary/built-in entities
//
// See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md

use anyhow::Result;
use rusqlite::Connection;

/// Source type for cartridges
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum SourceType {
    /// Text files containing DQL/SQL source code
    File = 1,
    /// Binary files (compiled/serialized definitions)
    FileBin = 2,
    /// Database connection (introspected tables/views)
    Db = 3,
    /// Built-in entities defined in the engine code
    Bin = 4,
}

impl SourceType {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Install a cartridge from a given source
///
/// This is Step 3 of the bootstrap process (REUSABLE).
/// Works for any cartridge type: bootstrap://sys, file://app.dql, postgres://prod-db
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database (where metadata is stored)
/// * `source_uri` - URI identifying the cartridge (e.g., "bootstrap://sys")
/// * `source_type` - Type of source (file, db, bin, etc.)
/// * `language_id` - Language ID from language table (1=DQL, 2=SQL/postgres, 3=SQL/sqlite)
/// * `source_ns` - Optional SQLite database name or namespace hint
/// * `connection_id` - Optional connection ID (None for universal cartridges)
/// * `is_universal` - Whether this cartridge works on all connections
///
/// # Returns
/// * `Ok(cartridge_id)` - The ID of the newly installed cartridge
/// * `Err(anyhow::Error)` - If installation fails
///
/// # Implementation Notes
/// For `SourceType::Db`:
/// - Connects to the database specified by source_uri
/// - Uses introspection to discover tables/views
/// - Populates entity and entity_attribute tables
/// - All discovered entities are automatically "grounded" (no external dependencies)
///
/// For other types (File, Bin):
/// - Not yet implemented (will parse source, analyze references, etc.)
///
/// # Example
/// ```
/// use delightql_core::import::cartridge::{install_cartridge, SourceType};
/// use rusqlite::Connection;
///
/// let conn = Connection::open_in_memory().unwrap();
/// // ... initialize bootstrap schema ...
///
/// // Bootstrap cartridge on bootstrap connection
/// let cartridge_id = install_cartridge(
///     &conn,
///     "bootstrap://sys",
///     SourceType::Db,
///     3,  // SQLite
///     Some("_bootstrap"),
///     Some(1),  // connection_id=1 (bootstrap connection)
///     false     // not universal
/// ).unwrap();
///
/// // Universal cartridge (std library)
/// let std_id = install_cartridge(
///     &conn,
///     "bootstrap://std/predicates",
///     SourceType::Bin,
///     1,   // DQL
///     None,
///     None, // connection_id=NULL
///     true  // is_universal
/// ).unwrap();
/// ```
pub fn install_cartridge(
    conn: &Connection,
    source_uri: &str,
    source_type: SourceType,
    language_id: i32,
    source_ns: Option<&str>,
    connection_id: Option<i32>,
    is_universal: bool,
) -> Result<i32> {
    conn.execute(
        "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
         VALUES (?1, ?2, ?3, ?4, 1, ?5, ?6)",
        rusqlite::params![
            language_id,
            source_type.as_i32(),
            source_uri,
            source_ns,
            connection_id,
            if is_universal { 1 } else { 0 }
        ],
    )?;

    let cartridge_id = conn.last_insert_rowid() as i32;

    match source_type {
        SourceType::Db => {
            // For database connections, introspect to discover tables/views
            introspect_database_cartridge(conn, cartridge_id, source_ns)?;
        }
        SourceType::File | SourceType::FileBin | SourceType::Bin => {}
    }

    Ok(cartridge_id)
}

/// Introspect a database connection to discover entities
///
/// Internal helper for database-type cartridges.
/// Uses the reusable introspection logic from bootstrap::introspect module.
///
/// Works for ANY attached SQLite schema (not just _bootstrap).
fn introspect_database_cartridge(
    conn: &Connection,
    cartridge_id: i32,
    source_ns: Option<&str>,
) -> Result<()> {
    // Introspect the specified schema (or main database if None)
    // For bootstrap://sys: source_ns=Some("_bootstrap")
    // For attached databases: source_ns=Some("_attached_1"), etc.
    let entities = crate::bootstrap::introspect::introspect_sqlite_database(conn, source_ns)?;

    // Insert discovered entities into metadata tables
    crate::bootstrap::introspect::insert_discovered_entities(conn, cartridge_id, &entities)?;

    Ok(())
}
