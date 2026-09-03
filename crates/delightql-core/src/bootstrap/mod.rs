// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// DelightQL Bootstrap Module
//
// This module implements the bootstrap initialization system for the DDL-LIGHT metadata
// infrastructure (NON-REUSABLE: runs once per session):
// - Creating the _bootstrap SQLite database schema
// - Inserting seed data for reference tables

pub mod bin_sync;
pub(crate) mod guard;
pub mod introspect;

pub(crate) use crate::enums;
pub(crate) use crate::enums::{ConnectionType, EntityType, Language, SourceType};

// Re-export bin sync function for convenience
pub use bin_sync::sync_bin_cartridges_to_bootstrap;

use crate::error::DelightQLError;
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
/// All enum IDs and variants are defined in src/enums.rs (re-exported
/// here as `bootstrap::enums`) and inserted programmatically here.
/// (Do not recreate src/bootstrap/enums.rs: a file there is never
/// declared as a module, never compiled, and silently absorbs edits
/// meant for the live enum.)
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
/// This function:
/// 1. Creates all metadata tables and views (BOOTSTRAP_SCHEMA)
/// 2. Inserts seed data for reference tables (programmatically from Rust enums)
///
/// These steps are NON-REUSABLE (run once per session).
/// Cartridge installation, namespace creation, and entity activation
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
/// ```ignore
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

    /// A catalog with one lib namespace and one consult cartridge, for the
    /// definition-store law tests.
    fn definition_store() -> (Connection, i32, i64) {
        let conn = Connection::open_in_memory().unwrap();
        initialize_bootstrap_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO connection (id, resource_uri, mechanism, connection_type) VALUES (1, 'test://', 'in-process', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO namespace (name, fq_name, kind, provenance) VALUES ('t', 'lib::t', 'lib', 'file')",
            [],
        )
        .unwrap();
        let ns = conn.last_insert_rowid() as i32;
        conn.execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (1, 1, 'file://t.dql', 'lib::t', 1, 1, 0)",
            [],
        )
        .unwrap();
        let cart = conn.last_insert_rowid();
        (conn, ns, cart)
    }

    /// Register an entity row with one clause and activate it in the
    /// namespace — the consult writer's exact sequence.
    fn activate(
        conn: &Connection,
        ns: i32,
        cart: i64,
        name: &str,
        stropped: i32,
        kind: i32,
        with_clause: bool,
    ) -> rusqlite::Result<usize> {
        conn.execute(
            "INSERT INTO entity (name, name_stropped, type, cartridge_id) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![name, stropped, kind, cart],
        )?;
        let entity = conn.last_insert_rowid();
        if with_clause {
            conn.execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, 'body')",
                [entity],
            )?;
        }
        conn.execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity, ns, cart],
        )
    }

    /// ONE CANONICAL NAME, ONE FAMILY, per namespace: the store refuses a
    /// second authored family under a name the namespace already answers,
    /// under the identifier law's folding.
    #[test]
    fn a_second_family_under_one_canonical_name_refuses_at_the_store() {
        let (conn, ns, cart) = definition_store();
        activate(&conn, ns, cart, "Pick", 0, 4, true).unwrap();
        // Unstropped names fold: `pick` and `Pick` are one canonical name,
        // and so is a stropped lowercase `pick` — same canonical bytes.
        assert!(activate(&conn, ns, cart, "pick", 0, 4, true).is_err());
        assert!(activate(&conn, ns, cart, "pick", 1, 4, true).is_err());
        // A stropped case survivor keeps its bytes and IS a different name.
        activate(&conn, ns, cart, "Pick", 1, 4, true).unwrap();
        // Category never selects among same-named definitions: a function
        // under the relation's name refuses too.
        assert!(activate(&conn, ns, cart, "pick", 0, 1, true).is_err());
    }

    /// The two activation triggers apply to exactly the authored kinds the
    /// enum names: a clauseless authored family refuses to activate, a
    /// clauseless served row activates.
    #[test]
    fn the_store_triggers_and_the_enum_agree_on_authored_kinds() {
        use crate::enums::EntityType;
        let (conn, ns, cart) = definition_store();
        for raw in 1..=21 {
            let Ok(kind) = EntityType::from_i32(raw) else {
                continue;
            };
            let activated = activate(&conn, ns, cart, &format!("probe_{raw}"), 0, raw, false);
            assert_eq!(
                activated.is_err(),
                kind.is_authored_definition(),
                "trigger and EntityType::is_authored_definition disagree on {kind:?} ({raw})"
            );
        }
    }

    /// The store keeps no revision history: no table, column, or trigger
    /// names one.
    #[test]
    fn the_store_holds_no_definition_revision() {
        let conn = Connection::open_in_memory().unwrap();
        initialize_bootstrap_db(&conn).unwrap();
        let mut statement = conn
            .prepare("SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL")
            .unwrap();
        let objects: Vec<(String, String)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        for (name, sql) in objects {
            assert!(
                !sql.to_ascii_lowercase().contains("revision"),
                "{name} carries revision vocabulary: {sql}"
            );
        }
    }

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

// ---------------------------------------------------------------------------
// Session-scoped tables
//
// The verdict tables a compilation writes into: assertions, dangers and
// errors. They are session state, recreated on every `reinit_bootstrap`, and
// have nothing to do with reading source.
// ---------------------------------------------------------------------------

#[cfg(not(target_arch = "wasm32"))]
/// Create the assertions table on the bootstrap connection.
///
/// This table records assertion verdicts for querying via sys.assertions(*).
pub fn setup_assertions_table_on_bootstrap(
    conn: &rusqlite::Connection,
) -> crate::error::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS assertions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            source_file TEXT,
            source_line INTEGER,
            body TEXT NOT NULL,
            outcome TEXT NOT NULL,
            detail TEXT,
            run_id TEXT NOT NULL
        )",
        [],
    )
    .map_err(|e| {
        DelightQLError::database_error(
            "Failed to create assertions table on bootstrap",
            e.to_string(),
        )
    })?;

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
/// Create the danger gates table on the bootstrap connection.
///
/// This table records the current state of each danger gate for querying via sys.danger(*).
/// Seeded with known defaults (all OFF) at session start.
pub fn setup_danger_table_on_bootstrap(conn: &rusqlite::Connection) -> crate::error::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS danger (
            uri TEXT PRIMARY KEY,
            state TEXT NOT NULL,
            cli_overridable INTEGER NOT NULL DEFAULT 1,
            description TEXT
        )",
        [],
    )
    .map_err(|e| {
        DelightQLError::database_error("Failed to create danger table on bootstrap", e.to_string())
    })?;

    // Seed default rows for all known danger URIs
    let defaults = [
        (
            "delightql-danger://cardinality/cartesian",
            "OFF",
            true,
            "Unrestricted cartesian product",
        ),
        (
            "delightql-danger://termination/unbounded",
            "OFF",
            true,
            "Unbounded recursive query",
        ),
        (
            "delightql-danger://semantics/min_multiplicity",
            "OFF",
            false,
            "True INTERSECT ALL via ROW_NUMBER (min-multiplicity)",
        ),
    ];
    for (uri, state, cli_overridable, description) in &defaults {
        conn.execute(
            "INSERT OR IGNORE INTO danger (uri, state, cli_overridable, description) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![uri, state, *cli_overridable as i32, description],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to seed danger row '{}': {}", uri, e),
                e.to_string(),
            )
        })?;
    }

    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
/// Create the session's finding table on the bootstrap connection,
/// published as `sys::diagnostics.finding`.
///
/// One row per refusal the compiler raised and per selftest finding:
/// `kind` shares its domain with the client's incident table so the two
/// project together; `uri` is the identity; `input` is the submission
/// when there was one; `provider` says which road wrote it.
pub fn setup_finding_table_on_bootstrap(conn: &rusqlite::Connection) -> crate::error::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS finding (
            id          INTEGER PRIMARY KEY,
            occurred_at TEXT NOT NULL,
            kind        TEXT NOT NULL CHECK (kind IN ('error', 'warning', 'info')),
            uri         TEXT NOT NULL,
            message     TEXT NOT NULL,
            input       TEXT,
            provider    TEXT NOT NULL
        )",
        [],
    )
    .map_err(|e| {
        DelightQLError::database_error("Failed to create finding table on bootstrap", e.to_string())
    })?;

    Ok(())
}
