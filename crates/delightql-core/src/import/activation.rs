// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Entity Activation and Deactivation
//
// This module handles activating entities within namespaces.
// Activation makes entities available for querying via namespace.entity(*) syntax.

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
/// ```ignore
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
/// ```ignore
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
///                  entity_type_enum, GroundedEntity, ExternalNamespaces,
///                  entity_clause, er_rule
/// - sys::entities::ho: ho_param, ho_param_column
/// - sys::entities::interior: interior_entity, interior_entity_attribute
/// - sys::ns: activated_entity, enlisted_entity, enlisted_namespace,
///            namespace_alias, namespace_local_alias, namespace_local_enlist,
///            exposed_namespace, grounding, liminal_receipt (the curated
///            `namespace` entity is registered separately in system.rs,
///            public columns only — it carries an internal mount link)
/// - sys::execution: compilation, stack, compiler_limit, effect_plan,
///                    effect_guard, effect_requirement, effect_run
/// - sys::targeting: dialect_render, dialect_form_rule, dialect_capability
/// - sys::connections: connection_type_enum (the curated `connection` entity is
///                     registered separately in system.rs, safe columns only)
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
        // sys::ns (namespace_id = 5). The `namespace` table itself is NOT
        // activated raw: its public face is the
        // curated explicit-column entity registered separately in system.rs
        // (register_sys_ns_namespace_table) — the sys::connections
        // precedent: a column added to the physical table is default-deny.
        ("activated_entity", 5),
        ("enlisted_entity", 5),
        ("enlisted_namespace", 5),
        // sys::execution (namespace_id = 10)
        ("compilation", 10),
        ("stack", 10),
        // sys::execution — the resource policies a compilation runs under.
        // Engine-written: the effective value is published at compilation
        // entry, the rest is burned with the schema.
        ("compiler_limit", 10),
        // sys::execution — the typed effect plan's observational
        // projection (engine-written, cleared at next compile).
        ("effect_plan", 10),
        ("effect_guard", 10),
        ("effect_requirement", 10),
        ("effect_run", 10),
        // sys::targeting (namespace_id = 12) — the data-driven multi-target
        // rule tables. Introspection already registers them as cartridge-1
        // entities; this is the missing activation that gives them a DQL
        // address.
        ("dialect_render", 12),
        ("dialect_form_rule", 12),
        ("dialect_capability", 12),
        // sys::entities (namespace_id = 4) — entity-detail that isn't ho/interior
        ("entity_clause", 4),
        ("join_edge", 4),
        ("functional_dependency", 4),
        // sys::entities::ho (namespace_id = 15)
        ("ho_param", 15),
        ("ho_param_column", 15),
        // sys::entities::interior (namespace_id = 16)
        ("interior_entity", 16),
        ("interior_entity_attribute", 16),
        // sys::ns (namespace_id = 5) — namespace wiring
        ("namespace_alias", 5),
        ("namespace_local_alias", 5),
        ("namespace_local_enlist", 5),
        ("exposed_namespace", 5),
        ("grounding", 5),
        // sys::ns — the liminal ledger storage (THE LIMINAL RELATION); read by
        // the catalog functor's synthesized `liminal` drill expansion
        // (resolver_fold::r_resolve_pipe). Pinned by effects/liminal--43/45.
        ("liminal_receipt", 5),
        // sys::connections (namespace_id = 13) — reference enum (safe raw).
        // The `connection` table itself carries secret columns; its curated
        // safe-subset entity is registered separately in system.rs.
        ("connection_type_enum", 13),
    ];

    activate_entities_by_name(conn, cartridge_id, &mappings)
}
