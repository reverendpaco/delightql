// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Sync bin cartridges to bootstrap database
//!
//! This module provides functionality to register bin cartridges (built-in entities)
//! in the bootstrap metadata system during system initialization.

use super::enums::SourceType;
use crate::bin_cartridge::registry::BinCartridgeRegistry;
use crate::bin_cartridge::OutputSchema;
use anyhow::Result;
use rusqlite::{params, Connection};

/// Sync all bin cartridges to the bootstrap database
///
/// This function is called during system initialization (after schema creation)
/// to register bin cartridges and their entities in the bootstrap metadata.
///
/// For each cartridge:
/// 1. Insert cartridge record (source_type=4 for Bin)
/// 2. Create namespace hierarchy
/// 3. Register each entity
/// 4. Activate entities in the namespace
///
/// # Arguments
///
/// * `conn` - Connection to the _bootstrap database
/// * `registry` - The bin cartridge registry containing all registered cartridges
///
/// # Returns
///
/// * `Ok(())` - If all cartridges synced successfully
/// * `Err(...)` - If any registration fails
pub fn sync_bin_cartridges_to_bootstrap(
    conn: &Connection,
    registry: &BinCartridgeRegistry,
) -> Result<Vec<String>> {
    let mut universal_namespaces = Vec::new();
    for cartridge in registry.cartridges() {
        let metadata = cartridge.metadata();

        // Step 1: Insert cartridge record
        conn.execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, connected, is_universal)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                metadata.language.as_i32(),
                SourceType::Bin.as_i32(),
                metadata.source_uri,
                true, // Bin cartridges are always "connected" (they're compiled in)
                metadata.is_universal,
            ],
        )?;
        let cartridge_id = conn.last_insert_rowid() as i32;

        // Step 2: Create namespace hierarchy from path string
        let namespace_id =
            crate::import::create_namespace_from_path(conn, &metadata.namespace_path)?;

        // Step 3: Register each entity under its IDENTITY namespace — the
        // entity's namespace_override if declared, else the cartridge's
        // namespace (deliberate catalog
        // identities; compile activates under sys::execution, not
        // std::prelude).
        for entity in cartridge.entities() {
            let signature = entity.signature();
            let entity_namespace_id = match entity.namespace_override() {
                Some(ns_path) => crate::import::create_namespace_from_path(conn, ns_path)?,
                None => namespace_id,
            };

            // Insert entity record
            conn.execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES (?1, ?2, ?3)",
                params![entity.name(), entity.entity_type().as_i32(), cartridge_id,],
            )?;
            let entity_id = conn.last_insert_rowid() as i32;

            crate::import::activate_entity(conn, entity_id, entity_namespace_id, cartridge_id)?;

            // Insert parameter attributes
            for (param_index, param) in signature.parameters.iter().enumerate() {
                conn.execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        entity_id,
                        param.name,
                        "input_param",
                        param.data_type,
                        param_index as i32,
                    ],
                )?;
            }

            // Insert output schema attributes
            match &signature.output_schema {
                OutputSchema::Relation(columns) => {
                    for (column_index, (column_name, data_type)) in columns.iter().enumerate() {
                        conn.execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            params![
                                entity_id,
                                column_name,
                                "output_column",
                                data_type,
                                column_index as i32,
                            ],
                        )?;
                    }
                }
                OutputSchema::Void => {
                    // Sigma predicates with EXISTS semantics don't contribute output columns
                    // No attributes to insert
                }
            }
        }

        // Step 4: activation happened per entity above, in each entity's
        // identity namespace.
        //
        // EVERY DECLARED DIRECTIVE IDENTITY PUBLISHES ITSELF. Identity and
        // execution capability are separate: a directive realized as a
        // syntax terminal or a liminal-only form has no bin entity to
        // register, and its identity row is published HERE, from the one
        // declaration — so a built-in cannot be implemented while
        // forgetting to publish. An Entity realization must have arrived
        // through the cartridge above; a missing registration is an error
        // that propagates, never an absence.
        publish_declared_directive_identities(
            conn,
            cartridge_id,
            &metadata.namespace_path,
            namespace_id,
        )?;
        log::debug!(
            "Synced bin cartridge '{}' to bootstrap (namespace '{}')",
            metadata.source_uri,
            metadata.namespace_path
        );

        // Step 5: Auto-enlist universal namespaces into `home` — the
        // interactive session's own resolution scope (edges
        // are owned by the namespace whose environment they extend,
        // never by the `main` data namespace)
        if metadata.is_universal {
            let main_ns_id: i32 = conn.query_row(
                "SELECT id FROM namespace WHERE fq_name = 'home'",
                [],
                |row| row.get(0),
            )?;
            conn.execute(
                "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
                 VALUES (?1, ?2)",
                params![namespace_id, main_ns_id],
            )?;
            universal_namespaces.push(metadata.namespace_path.clone());
            log::debug!(
                "Auto-enlisted universal namespace '{}' into 'home'",
                metadata.namespace_path
            );
        }
    }

    // Seed per-functor dialect_form_rule rows now that bin entities have
    // entity ids (session-local, resolved by name in the seed SQL).
    // Idempotent; covers both init and reinit_bootstrap.
    conn.execute_batch(FORM_RULES_SEED)?;

    Ok(universal_namespaces)
}

/// Publish the identity of every DECLARED directive whose catalog namespace
/// is this cartridge's namespace.
///
/// For an Entity realization the cartridge registration above is the
/// publication, and its absence is an ERROR: the declaration says the entity
/// exists, so a missing row means the realization dispatch forgot it — the
/// error propagates rather than masquerading as absence. For a syntax
/// terminal or a liminal-only form the identity row is inserted here with
/// [`EntityType::SyntaxDirective`], so reflection enumerates the COMPLETE
/// declared universe without pretending those identities are ordinary
/// executable bin entities.
fn publish_declared_directive_identities(
    conn: &Connection,
    cartridge_id: i32,
    namespace_path: &str,
    namespace_id: i32,
) -> Result<()> {
    use crate::pipeline::asts::effects::{DirectiveKind, DirectiveRealization};
    for kind in DirectiveKind::ALL {
        let descriptor = kind.descriptor();
        if descriptor.namespace != namespace_path {
            continue;
        }
        let bang = kind.bang_name();
        match descriptor.realization {
            DirectiveRealization::Entity => {
                let registered: bool = conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM entity
                      WHERE name = ?1 AND cartridge_id = ?2)",
                    params![bang, cartridge_id],
                    |row| row.get(0),
                )?;
                anyhow::ensure!(
                    registered,
                    "directive '{bang}' declares Entity realization but the \
                     cartridge registered no entity for it: its identity \
                     cannot be published"
                );
            }
            DirectiveRealization::SyntaxPipeTerminal | DirectiveRealization::LiminalOnly => {
                conn.execute(
                    "INSERT INTO entity (name, type, cartridge_id)
                     VALUES (?1, ?2, ?3)",
                    params![
                        bang,
                        crate::enums::EntityType::SyntaxDirective.as_i32(),
                        cartridge_id,
                    ],
                )?;
                let entity_id = conn.last_insert_rowid() as i32;
                crate::import::activate_entity(conn, entity_id, namespace_id, cartridge_id)?;
            }
        }
    }
    Ok(())
}

/// Embedded seed for `dialect_form_rule` — per-functor targeting rules for
/// bin entities. Runs after entity sync
/// because the rows resolve `entity_id` by (name, type) subselect.
const FORM_RULES_SEED: &str = include_str!("../../bootstrap/form_rules.sql");
