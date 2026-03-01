// DelightQL Bootstrap Module
//
// This module implements the bootstrap initialization system for the DDL-LIGHT metadata
// infrastructure. It handles Steps 1-2 of the bootstrap process (NON-REUSABLE):
// - Creating the _bootstrap SQLite database schema
// - Inserting seed data for reference tables
//
// See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md

pub mod bin_sync;
pub mod introspect;

// Re-export enums from standalone module for backward compatibility
pub(crate) use crate::enums;
pub(crate) use crate::enums::{ConnectionType, EntityType, Language, SourceType};

// Re-export bin sync function for convenience
pub use bin_sync::sync_bin_cartridges_to_bootstrap;

use anyhow::Result;
use rusqlite::{params, Connection};

/// Embedded SQL schema DDL
///
/// Contains all CREATE TABLE and CREATE VIEW statements for the bootstrap metadata system.
/// Includes:
/// - Reference tables: entity_type_enum, source_type_enum, language
/// - Cartridge tables: cartridge
/// - Entity tables: entity, referenced_entity, entity_attribute, entity_resolution
/// - Namespace tables: namespace, activated_entity, enlisted_entity, enlisted_namespace
/// - Views: GroundedEntity, ExternalNamespaces
pub const BOOTSTRAP_SCHEMA: &str = include_str!("../../bootstrap/schema.sql");

/// Seed enum tables from Rust enum definitions
///
/// This is the SINGLE SOURCE OF TRUTH for enum values.
/// All enum IDs and variants are defined in src/bootstrap/enums.rs
/// and inserted programmatically here.
///
/// Benefits:
/// - Type-safe: Impossible to use wrong enum value
/// - Single source of truth: No sync issues between Rust and SQL
/// - Self-documenting: Enum names are clear
/// - Refactor-safe: Compiler catches all usages
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
///
/// # Returns
/// * `Ok(())` if seeding succeeds
/// * `Err(anyhow::Error)` if any INSERT fails
fn seed_enum_tables(conn: &Connection) -> Result<()> {
    // Seed source_type_enum table
    for source_type in SourceType::ALL {
        conn.execute(
            "INSERT INTO source_type_enum (id, variant, explanation) VALUES (?1, ?2, ?3)",
            params![
                source_type.as_i32(),
                source_type.variant_name(),
                source_type.explanation()
            ],
        )?;
    }

    // Seed language table
    for language in Language::ALL {
        conn.execute(
            "INSERT INTO language (id, language, dialect, version) VALUES (?1, ?2, ?3, ?4)",
            params![
                language.as_i32(),
                language.language(),
                language.dialect(),
                language.version()
            ],
        )?;
    }

    // Seed entity_type_enum table
    for entity_type in EntityType::ALL {
        conn.execute(
            "INSERT INTO entity_type_enum (id, variant, is_ho, is_fn) VALUES (?1, ?2, ?3, ?4)",
            params![
                entity_type.as_i32(),
                entity_type.variant_name(),
                entity_type.is_ho(),
                entity_type.is_fn(),
            ],
        )?;
    }

    // Seed connection_type_enum table
    for connection_type in ConnectionType::ALL {
        conn.execute(
            "INSERT INTO connection_type_enum (id, variant, explanation) VALUES (?1, ?2, ?3)",
            params![
                connection_type.as_i32(),
                connection_type.variant_name(),
                connection_type.explanation()
            ],
        )?;
    }

    Ok(())
}

/// Initialize the _bootstrap in-memory database
///
/// This function implements Steps 1-2 of the bootstrap process:
/// 1. Create all metadata tables and views (BOOTSTRAP_SCHEMA)
/// 2. Insert seed data for reference tables (programmatically from Rust enums)
///
/// These steps are NON-REUSABLE (run once per session).
/// Steps 3-5 (cartridge installation, namespace creation, entity activation)
/// use the reusable cartridge/namespace logic and are NOT implemented here.
///
/// # Arguments
/// * `conn` - SQLite connection to the _bootstrap database
///
/// # Returns
/// * `Ok(())` if initialization succeeds
/// * `Err(anyhow::Error)` if SQL execution fails
///
/// # Example
/// ```
/// use rusqlite::Connection;
/// use delightql_core::bootstrap::initialize_bootstrap_db;
///
/// let conn = Connection::open_in_memory().unwrap();
/// initialize_bootstrap_db(&conn).unwrap();
/// ```
pub fn initialize_bootstrap_db(conn: &Connection) -> Result<()> {
    // Step 1: Execute schema DDL
    conn.execute_batch(BOOTSTRAP_SCHEMA)?;

    // Step 2: Seed enum tables from Rust definitions (SINGLE SOURCE OF TRUTH)
    seed_enum_tables(conn)?;

    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enum_seeding_integration() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_bootstrap_db(&conn).unwrap();

        // Verify source_type_enum table
        for source_type in SourceType::ALL {
            let (variant, explanation): (String, String) = conn
                .query_row(
                    "SELECT variant, explanation FROM source_type_enum WHERE id = ?1",
                    [source_type.as_i32()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();

            assert_eq!(variant, source_type.variant_name());
            assert_eq!(explanation, source_type.explanation());
        }

        // Verify language table
        for language in Language::ALL {
            let (lang, dialect, version): (String, String, String) = conn
                .query_row(
                    "SELECT language, dialect, version FROM language WHERE id = ?1",
                    [language.as_i32()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert_eq!(lang, language.language());
            assert_eq!(dialect, language.dialect());
            assert_eq!(version, language.version());
        }

        // Verify entity_type_enum table
        for entity_type in EntityType::ALL {
            let (variant, is_ho, is_fn): (String, bool, bool) = conn
                .query_row(
                    "SELECT variant, is_ho, is_fn FROM entity_type_enum WHERE id = ?1",
                    [entity_type.as_i32()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();

            assert_eq!(variant, entity_type.variant_name());
            assert_eq!(is_ho, entity_type.is_ho());
            assert_eq!(is_fn, entity_type.is_fn());
        }

        // Verify connection_type_enum table
        for connection_type in ConnectionType::ALL {
            let (variant, explanation): (String, String) = conn
                .query_row(
                    "SELECT variant, explanation FROM connection_type_enum WHERE id = ?1",
                    [connection_type.as_i32()],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();

            assert_eq!(variant, connection_type.variant_name());
            assert_eq!(explanation, connection_type.explanation());
        }
    }

    #[test]
    fn test_enum_counts_match() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_bootstrap_db(&conn).unwrap();

        // Verify table counts match Rust enum counts
        let source_type_count: i32 = conn
            .query_row("SELECT COUNT(*) FROM source_type_enum", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(source_type_count, SourceType::ALL.len() as i32);

        let language_count: i32 = conn
            .query_row("SELECT COUNT(*) FROM language", [], |row| row.get(0))
            .unwrap();
        assert_eq!(language_count, Language::ALL.len() as i32);

        let entity_type_count: i32 = conn
            .query_row("SELECT COUNT(*) FROM entity_type_enum", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(entity_type_count, EntityType::ALL.len() as i32);

        let connection_type_count: i32 = conn
            .query_row("SELECT COUNT(*) FROM connection_type_enum", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(connection_type_count, ConnectionType::ALL.len() as i32);
    }
}
