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
) -> Result<()> {
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

        // Step 3: Register each entity
        for entity in cartridge.entities() {
            let signature = entity.signature();

            // Insert entity record
            conn.execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES (?1, ?2, ?3)",
                params![entity.name(), entity.entity_type().as_i32(), cartridge_id,],
            )?;
            let entity_id = conn.last_insert_rowid() as i32;

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

        // Step 4: Activate all entities from this cartridge in the namespace
        let activated_count =
            crate::import::activate_entities_from_cartridge(conn, cartridge_id, namespace_id)?;

        log::debug!(
            "Synced bin cartridge '{}' to bootstrap: {} entities activated in namespace '{}'",
            metadata.source_uri,
            activated_count,
            metadata.namespace_path
        );
    }

    Ok(())
}
