// DelightQL Database Introspection Module (Runtime)
//
// This module implements database introspection logic for the RUNTIME/BOOTSTRAP database.
// This is specifically for introspecting the _bootstrap SQLite database that stores
// namespaces, entities, cartridges, and other runtime metadata.
//
// NOTE: This is NOT for user-facing database targets. User database introspection
// should use delightql-backends, which has its own independent implementation.
// This separation ensures that runtime infrastructure and transpilation targets
// remain distinct dependencies.
//
// See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md
// See: documentation/design/ddl/db-introspection-sequence.d2

use anyhow::Result;
use rusqlite::Connection;

// Re-export types from delightql-types for backward compatibility
pub use delightql_types::introspect::{
    DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity,
};

/// Introspect a SQLite database and discover tables/views (RUNTIME)
///
/// This is used for introspecting the _bootstrap database during system initialization.
/// It queries `sqlite_master` to discover all tables and views, then uses `PRAGMA table_info`
/// to discover columns for each entity.
///
/// NOTE: For user-facing SQLite databases (transpilation targets), use
/// `delightql_backends::sqlite::introspect::introspect_sqlite_database` instead.
///
/// # Arguments
/// * `conn` - SQLite connection to introspect
/// * `schema` - Optional schema name (e.g., "_bootstrap", "_attached_1"). If None, introspects main database.
///
/// # Returns
/// * `Ok(Vec<DiscoveredEntity>)` - List of discovered tables and views with their columns
/// * `Err(anyhow::Error)` - If introspection queries fail
///
/// # Example
/// ```
/// use rusqlite::Connection;
/// use delightql_core::bootstrap::introspect::introspect_sqlite_database;
///
/// let conn = Connection::open_in_memory().unwrap();
/// conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", []).unwrap();
///
/// let entities = introspect_sqlite_database(&conn, None).unwrap();
/// assert_eq!(entities.len(), 1);
/// assert_eq!(entities[0].name, "users");
/// ```
pub fn introspect_sqlite_database(
    conn: &Connection,
    schema: Option<&str>,
) -> Result<Vec<DiscoveredEntity>> {
    let mut entities = Vec::new();

    // Build query for the appropriate schema
    let schema_prefix = schema.map(|s| format!("{}.", s)).unwrap_or_default();
    let query = format!(
        "SELECT name, type FROM {}sqlite_master
         WHERE type IN ('table', 'view')
         AND name NOT LIKE 'sqlite_%'
         ORDER BY name",
        schema_prefix
    );

    let mut stmt = conn.prepare(&query)?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?, // name
            row.get::<_, String>(1)?, // type
        ))
    })?;

    for result in rows {
        let (table_name, table_type) = result?;

        // Determine entity type: 10=DBPermanentTable, 11=DBPermanentView
        let entity_type_id = if table_type == "table" { 10 } else { 11 };

        // Introspect columns using PRAGMA table_info
        let attributes = introspect_table_columns(conn, schema, &table_name)?;

        entities.push(DiscoveredEntity {
            name: table_name.into(),
            entity_type_id,
            attributes,
        });
    }

    Ok(entities)
}

/// Introspect columns for a specific table using PRAGMA table_xinfo
///
/// Internal helper function that queries SQLite's PRAGMA table_xinfo to discover
/// column metadata for a given table or view. Uses table_xinfo instead of
/// table_info to include generated columns.
///
/// # Arguments
/// * `conn` - SQLite connection
/// * `schema` - Optional schema name (e.g., "_bootstrap", "_attached_1")
/// * `table_name` - Name of table/view to introspect
///
/// # Returns
/// * `Ok(Vec<DiscoveredAttribute>)` - List of columns with metadata
/// * `Err(anyhow::Error)` - If PRAGMA query fails
fn introspect_table_columns(
    conn: &Connection,
    schema: Option<&str>,
    table_name: &str,
) -> Result<Vec<DiscoveredAttribute>> {
    let mut attributes = Vec::new();

    // PRAGMA table_xinfo returns: cid, name, type, notnull, dflt_value, pk, hidden
    // For attached databases: PRAGMA schema_name.table_xinfo(table_name)
    let query = if let Some(s) = schema {
        format!("PRAGMA {}.table_xinfo({})", s, table_name)
    } else {
        format!("PRAGMA table_xinfo({})", table_name)
    };
    let mut stmt = conn.prepare(&query)?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i32>(0)?,    // cid (column id)
            row.get::<_, String>(1)?, // name
            row.get::<_, String>(2)?, // type
            row.get::<_, i32>(3)?,    // notnull (1=NOT NULL, 0=nullable)
        ))
    })?;

    for result in rows {
        let (position, name, data_type, notnull) = result?;

        attributes.push(DiscoveredAttribute {
            name: name.into(),
            data_type,
            position,
            is_nullable: notnull == 0, // notnull=0 means nullable
        });
    }

    Ok(attributes)
}

/// Insert discovered entities into the entity and entity_attribute tables
///
/// This completes Step 3 (ANALYZE) by populating the metadata tables with discovered entities.
/// For each discovered entity:
/// 1. Insert into `entity` table
/// 2. Insert all columns into `entity_attribute` table as 'output_column' type
///
/// This is RUNTIME code that writes to the _bootstrap database.
///
/// # Arguments
/// * `conn` - Connection to the _bootstrap database (where metadata is stored)
/// * `cartridge_id` - ID of the cartridge being analyzed
/// * `entities` - List of discovered entities from introspection
///
/// # Returns
/// * `Ok(())` - If all entities and attributes inserted successfully
/// * `Err(anyhow::Error)` - If INSERT operations fail
///
/// # Example
/// ```
/// use rusqlite::Connection;
/// use delightql_core::bootstrap::{initialize_bootstrap_db, introspect::*};
///
/// // Setup bootstrap database
/// let conn = Connection::open_in_memory().unwrap();
/// initialize_bootstrap_db(&conn).unwrap();
///
/// // Create a cartridge
/// conn.execute(
///     "INSERT INTO cartridge (id, language, source_type_enum, source_uri, connected)
///      VALUES (1, 3, 3, 'bootstrap://sys', 1)",
///     [],
/// ).unwrap();
///
/// // Introspect the bootstrap database itself
/// let entities = introspect_sqlite_database(&conn, None).unwrap();
///
/// // Insert discovered entities into metadata
/// insert_discovered_entities(&conn, 1, &entities).unwrap();
/// ```
pub fn insert_discovered_entities(
    conn: &Connection,
    cartridge_id: i32,
    entities: &[DiscoveredEntity],
) -> Result<()> {
    for entity in entities {
        // Insert into entity table
        conn.execute(
            "INSERT INTO entity (name, type, cartridge_id)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![entity.name.as_str(), entity.entity_type_id, cartridge_id,],
        )?;

        let entity_id = conn.last_insert_rowid() as i32;

        // Insert entity clause (introspected entities have a single placeholder clause)
        conn.execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, ?2)",
            rusqlite::params![entity_id, format!("-- Introspected from database"),],
        )?;

        // Insert attributes
        for attr in &entity.attributes {
            conn.execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![
                    entity_id,
                    attr.name.as_str(),
                    attr.data_type,
                    attr.position,
                    attr.is_nullable,
                ],
            )?;
        }
    }

    Ok(())
}
