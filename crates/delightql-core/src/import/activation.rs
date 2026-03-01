// Entity Activation and Deactivation
//
// This module handles activating entities within namespaces.
// Activation makes entities available for querying via namespace.entity(*) syntax.
//
// See: documentation/design/ddl/SYS-NS-CARTRIDGE-ER-DESIGN.md

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

/// Activate a single entity in a namespace
///
/// This is Step 5 of the bootstrap process (REUSABLE).
/// Makes an entity available for querying in a specific namespace.
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
/// * `entity_id` - ID of the entity to activate
/// * `namespace_id` - ID of the namespace to activate in
/// * `cartridge_id` - ID of the source cartridge
///
/// # Returns
/// * `Ok(())` - If activation succeeds
/// * `Err(anyhow::Error)` - If activation fails
///
/// # Example
/// ```
/// use delightql_core::import::activation::activate_entity;
/// use rusqlite::Connection;
///
/// let conn = Connection::open_in_memory().unwrap();
/// // ... setup bootstrap, cartridge, namespaces, entities ...
///
/// // Activate cartridge entity in sys::cartridges namespace
/// activate_entity(&conn, entity_id, 3, cartridge_id).unwrap();
/// ```
pub fn activate_entity(
    conn: &Connection,
    entity_id: i32,
    namespace_id: i32,
    cartridge_id: i32,
) -> Result<()> {
    conn.execute(
        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id)
         VALUES (?1, ?2, ?3)",
        rusqlite::params![entity_id, namespace_id, cartridge_id],
    )?;

    Ok(())
}

/// Activate entities from a cartridge into specific namespaces
///
/// Activates entities based on their names and the target namespace.
/// This is used for bootstrap to organize system tables into logical namespaces.
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
/// * `cartridge_id` - ID of the cartridge whose entities to activate
/// * `mappings` - List of (entity_name, namespace_id) pairs
///
/// # Returns
/// * `Ok(count)` - Number of entities activated
/// * `Err(anyhow::Error)` - If activation fails
///
/// # Example
/// ```
/// use delightql_core::import::activation::activate_entities_by_name;
/// use rusqlite::Connection;
///
/// let conn = Connection::open_in_memory().unwrap();
/// // ... setup ...
///
/// let mappings = vec![
///     ("cartridge", 3),  // Activate in sys::cartridges
///     ("entity", 4),     // Activate in sys::entities
/// ];
///
/// activate_entities_by_name(&conn, cartridge_id, &mappings).unwrap();
/// ```
pub fn activate_entities_by_name(
    conn: &Connection,
    cartridge_id: i32,
    mappings: &[(&str, i32)],
) -> Result<usize> {
    let mut count = 0;

    for (entity_name, namespace_id) in mappings {
        let entity_id: Option<i32> = conn
            .query_row(
                "SELECT id FROM entity WHERE name = ?1 AND cartridge_id = ?2",
                rusqlite::params![entity_name, cartridge_id],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(entity_id) = entity_id {
            activate_entity(conn, entity_id, *namespace_id, cartridge_id)?;
            count += 1;
        }
    }

    Ok(count)
}

/// Activate all entities from a cartridge in a single namespace
///
/// Convenience function for activating all entities from a cartridge
/// into the same namespace (common for user imports).
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
/// * `cartridge_id` - ID of the cartridge
/// * `namespace_id` - ID of the namespace
///
/// # Returns
/// * `Ok(count)` - Number of entities activated
/// * `Err(anyhow::Error)` - If activation fails
pub fn activate_entities_from_cartridge(
    conn: &Connection,
    cartridge_id: i32,
    namespace_id: i32,
) -> Result<usize> {
    let mut stmt = conn.prepare("SELECT id FROM entity WHERE cartridge_id = ?1")?;

    let entity_ids: Vec<i32> = stmt
        .query_map([cartridge_id], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;

    for entity_id in &entity_ids {
        activate_entity(conn, *entity_id, namespace_id, cartridge_id)?;
    }

    Ok(entity_ids.len())
}

/// Activate bootstrap system entities in their respective namespaces
///
/// Organizes bootstrap entities into:
/// - sys::cartridges: cartridge, language, source_type_enum
/// - sys::entities: entity, referenced_entity, entity_attribute, entity_resolution,
///                  entity_type_enum, GroundedEntity, ExternalNamespaces
/// - sys::ns: namespace, activated_entity, enlisted_entity, enlisted_namespace
///
/// # Arguments
/// * `conn` - Connection to _bootstrap database
/// * `cartridge_id` - ID of the bootstrap://sys cartridge
///
/// # Returns
/// * `Ok(count)` - Total number of entities activated
/// * `Err(anyhow::Error)` - If activation fails
pub fn activate_bootstrap_entities(conn: &Connection, cartridge_id: i32) -> Result<usize> {
    let mappings = vec![
        // sys::cartridges (namespace_id = 3)
        ("cartridge", 3),
        ("language", 3),
        ("source_type_enum", 3),
        // sys::entities (namespace_id = 4)
        ("entity", 4),
        ("referenced_entity", 4),
        ("entity_attribute", 4),
        ("entity_resolution", 4),
        ("entity_type_enum", 4),
        ("GroundedEntity", 4),
        ("ExternalNamespaces", 4),
        // sys::ns (namespace_id = 5)
        ("namespace", 5),
        ("activated_entity", 5),
        ("enlisted_entity", 5),
        ("enlisted_namespace", 5),
        // sys::execution (namespace_id = 10)
        ("compilation", 10),
        ("stack", 10),
    ];

    activate_entities_by_name(conn, cartridge_id, &mappings)
}
