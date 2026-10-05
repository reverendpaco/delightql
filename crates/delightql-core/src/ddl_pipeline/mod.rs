// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
pub mod assemble_manifest;
pub mod asts;
pub mod builder;
pub mod generator;
pub mod sql_ast;

use crate::ddl::manifest;
use crate::Result;

/// Result of reading manifest data and producing CREATE TEMP TABLE SQL.
pub struct ManifestCreateResult {
    pub create_sql: String,
    /// The schema rows in the created table's physical order.
    pub schema_rows: Vec<manifest::SchemaRow>,
}

/// Produce CREATE TEMP TABLE SQL for one entity of a judged manifest, its
/// cells compiled by the new middle where the companion rules stand.
///
/// Returns `Ok(Some(result))` if the entity has schema rows, `Ok(None)` if not.
pub(crate) fn create_temp_table_from_manifest(
    system: &crate::system::DelightQLSystem,
    companions: &manifest::ManifestRows,
    entity_name: &str,
) -> Result<Option<ManifestCreateResult>> {
    let declared = companions.schema(entity_name);
    if declared.is_empty() {
        return Ok(None);
    }
    let schema_rows =
        crate::ddl::correspondence::Correspondence::decide(entity_name, declared, None)?
            .physical()
            .to_vec();
    let constraint_rows = companions.constraints(entity_name);
    let default_rows = companions.defaults(entity_name);
    let table = assemble_manifest::assemble_from_manifest(
        entity_name,
        true,
        &schema_rows,
        &constraint_rows,
        &default_rows,
    )?;
    Ok(Some(ManifestCreateResult {
        create_sql: crate::pipeline::middle::api::declared_table(system, companions.namespace(), &table)?,
        schema_rows,
    }))
}
