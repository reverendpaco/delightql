// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// SQLite Database Introspection (User Target)
//
// This module implements database introspection for user-facing SQLite databases
// that are transpilation TARGETS. This is separate from the runtime introspection
// in delightql-core, which is used for the _bootstrap database.
//
// The separation ensures that runtime infrastructure and transpilation targets
// remain distinct dependencies, even though the implementation is similar.

use anyhow::Result;
use delightql_types::introspect::{DiscoveredAttribute, DiscoveredEntity};
use rusqlite::Connection;

/// Introspect a user's SQLite database and discover tables/views
///
/// This is used for introspecting user-facing SQLite databases that serve as
/// transpilation targets. It queries `sqlite_master` to discover all tables
/// and views, then uses `PRAGMA table_info` to discover columns for each entity.
///
/// NOTE: For runtime/bootstrap introspection, use
/// `delightql_core::bootstrap::introspect::introspect_sqlite_database` instead.
///
/// # Arguments
/// * `conn` - SQLite connection to introspect
/// * `schema` - Optional schema name (e.g., for attached databases). If None, introspects main database.
///
/// # Returns
/// * `Ok(Vec<DiscoveredEntity>)` - List of discovered tables and views with their columns
/// * `Err(anyhow::Error)` - If introspection queries fail
///
/// # Example
/// ```
/// use rusqlite::Connection;
/// use delightql_backends::sqlite::introspect::introspect_sqlite_database;
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

/// Introspect columns for a specific table using PRAGMA table_info
///
/// Internal helper function that queries SQLite's PRAGMA table_info to discover
/// column metadata for a given table or view.
///
/// # Arguments
/// * `conn` - SQLite connection
/// * `schema` - Optional schema name (for attached databases)
/// * `table_name` - Name of table/view to introspect
///
/// # Returns
/// * `Ok(Vec<DiscoveredAttribute>)` - List of columns with metadata
/// * `Err(anyhow::Error)` - If PRAGMA query fails
pub(super) fn introspect_table_columns(
    conn: &Connection,
    schema: Option<&str>,
    table_name: &str,
) -> Result<Vec<DiscoveredAttribute>> {
    let mut attributes = Vec::new();

    // rusqlite's pragma helper quotes both the schema identifier and table
    // argument. This function is also used for an authored passthrough name,
    // so constructing PRAGMA text from either string is not safe.
    conn.pragma(schema, "table_info", table_name, |row| {
        let position = row.get::<_, i32>(0)?;
        let name = row.get::<_, String>(1)?;
        let data_type = row.get::<_, String>(2)?;
        let notnull = row.get::<_, i32>(3)?;
        attributes.push(DiscoveredAttribute {
            name: name.into(),
            data_type,
            position,
            is_nullable: notnull == 0, // notnull=0 means nullable
        });
        Ok(())
    })?;

    Ok(attributes)
}
