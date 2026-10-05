// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// DatabaseSchema implementation backed by bootstrap metadata.
///
/// Instead of querying the live database connection (via PRAGMA table_xinfo),
/// this reads column information from the bootstrap's `entity` + `entity_attribute`
/// tables. This is the single source of truth after mount! introspects a database.
///
/// Advantages:
/// - No coupling to backend-specific types (no DynamicSqliteSchema dependency)
/// - Works identically for SQLite, DuckDB, and future backends
/// - Schema information arrives via mount!, not at open() time
use crate::diagnostic::Runtime;
use delightql_types::schema::{ColumnInfo, DatabaseSchema};
use delightql_types::Result;
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// Schema provider that reads from bootstrap metadata tables.
///
/// Queries the `entity_attribute` table joined through `activated_entity`
/// and `namespace` to find columns for a given table name.
pub struct BootstrapBackedSchema {
    bootstrap_conn: Arc<Mutex<Connection>>,
}

impl BootstrapBackedSchema {
    pub fn new(bootstrap_conn: Arc<Mutex<Connection>>) -> Self {
        Self { bootstrap_conn }
    }
}

// Safety: Arc<Mutex<Connection>> is Send+Sync when Connection is Send.
// rusqlite::Connection is Send but not Sync; the Mutex provides Sync.
unsafe impl Sync for BootstrapBackedSchema {}

impl DatabaseSchema for BootstrapBackedSchema {
    fn get_table_columns(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<Vec<ColumnInfo>>> {
        let conn = self.bootstrap_conn.lock().map_err(|error| {
            Runtime::poisoned(
                "Failed to acquire bootstrap schema connection",
                error.to_string(),
            )
        })?;

        // The schema qualifier can be either:
        // 1. A namespace fq_name (e.g., "main", "zot") — used by direct user queries
        // 2. A mount qualification (an ATTACH alias or engine schema) returned
        //    by resolve_namespace_path from the authoritative `mount` row.
        //
        // We try namespace fq_name first, then mount qualification, then the
        // source namespace of cartridges that are not mount bindings.
        let qualifier = schema.unwrap_or("main");

        // Primary path: look up by namespace fq_name.
        //
        // ONE entity's columns, never a merge. A session object is
        // published under its connection's shadow (`sys::shadow::<root>`),
        // which holds one entity per name because the connection's temp
        // schema is one name pool, so no preference between cartridges is
        // needed; the newest row answers should a registration ever overlap
        // its predecessor.
        let sql_by_namespace = r#"
            SELECT ea.attribute_name, ea.position, ea.is_nullable, ea.data_type,
                   EXISTS (SELECT 1 FROM interior_entity ie
                            WHERE ie.parent_entity_id = ea.entity_id
                              AND ie.column_name = ea.attribute_name) AS interior
            FROM entity_attribute ea
            WHERE ea.entity_id = (
                SELECT e.id
                FROM entity e
                JOIN activated_entity ae ON ae.entity_id = e.id
                JOIN namespace n ON n.id = ae.namespace_id
                WHERE n.fq_name = ?1
                  AND e.name = ?2
                ORDER BY e.id DESC
                LIMIT 1
            )
              AND ea.attribute_type = 'output_column'
            ORDER BY ea.position
        "#;

        let columns = Self::query_columns(&conn, sql_by_namespace, qualifier, table_name)?;
        if let Some(cols) = columns {
            if !cols.is_empty() {
                return Ok(Some(cols));
            }
        }

        // Mounted-data fallback: qualification policy belongs to `mount`, not
        // to cartridge.source_ns. In particular an unqualified main mount may
        // still record its physical ATTACH alias on the cartridge without
        // making generated reads qualified.
        //
        // ONE entity's columns, never a merge — the same law the namespace
        // path states, and it binds here for a second reason: a physical
        // schema may back more than one mount (one file named by two
        // namespaces), so this qualifier can reach several equally valid
        // entities. Their attribute rows interleave by position into a
        // heading that describes no relation, and every column in it loses
        // its name. Pick one; they are the same table.
        let sql_by_mount_qualification = r#"
            SELECT ea.attribute_name, ea.position, ea.is_nullable, ea.data_type,
                   EXISTS (SELECT 1 FROM interior_entity ie
                            WHERE ie.parent_entity_id = ea.entity_id
                              AND ie.column_name = ea.attribute_name) AS interior
            FROM entity_attribute ea
            WHERE ea.entity_id = (
                SELECT e.id
                FROM entity e
                JOIN activated_entity ae ON ae.entity_id = e.id
                JOIN cartridge c ON ae.cartridge_id = c.id
                JOIN mount m ON m.namespace_id = ae.namespace_id
                            AND m.cartridge_id = c.id
                WHERE CASE
                        WHEN m.qualification = 'aliased' THEN m.attach_alias
                        WHEN m.qualification = 'engine_schema' THEN m.engine_schema
                        ELSE NULL
                      END = ?1
                  AND e.name = ?2
                ORDER BY e.id DESC
                LIMIT 1
            )
              AND ea.attribute_type = 'output_column'
            ORDER BY ea.position
        "#;

        let columns =
            Self::query_columns(&conn, sql_by_mount_qualification, qualifier, table_name)?;
        if let Some(cols) = columns {
            if !cols.is_empty() {
                return Ok(Some(cols));
            }
        }

        // Non-mount cartridges (consulted libraries and other definition
        // sources) retain source_ns as source metadata. Excluding cartridges
        // participating in `mount` keeps that metadata from becoming a second
        // writable copy of mount qualification policy.
        let sql_by_non_mount_source_ns = r#"
            SELECT ea.attribute_name, ea.position, ea.is_nullable, ea.data_type,
                   EXISTS (SELECT 1 FROM interior_entity ie
                            WHERE ie.parent_entity_id = ea.entity_id
                              AND ie.column_name = ea.attribute_name) AS interior
            FROM entity_attribute ea
            JOIN entity e ON e.id = ea.entity_id
            JOIN activated_entity ae ON ae.entity_id = e.id
            JOIN cartridge c ON ae.cartridge_id = c.id
            WHERE c.source_ns = ?1
              AND NOT EXISTS (
                    SELECT 1 FROM mount m WHERE m.cartridge_id = c.id
              )
              AND e.name = ?2
              AND ea.attribute_type = 'output_column'
            ORDER BY ea.position
        "#;

        let columns =
            Self::query_columns(&conn, sql_by_non_mount_source_ns, qualifier, table_name)?;
        if let Some(cols) = columns {
            if !cols.is_empty() {
                return Ok(Some(cols));
            }
        }

        Ok(None)
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> Result<bool> {
        Ok(self.get_table_columns(schema, table_name)?.is_some())
    }
}

impl BootstrapBackedSchema {
    fn query_columns(
        conn: &Connection,
        sql: &str,
        qualifier: &str,
        table_name: &str,
    ) -> Result<Option<Vec<ColumnInfo>>> {
        let mut stmt = conn.prepare(sql).map_err(|error| {
            Runtime::catalog(
                "Failed to prepare bootstrap schema query",
                error.to_string(),
            )
        })?;
        let columns: Vec<ColumnInfo> = stmt
            .query_map(rusqlite::params![qualifier, table_name], |row| {
                let name: String = row.get(0)?;
                let position: i32 = row.get(1)?;
                let is_nullable: Option<i32> = row.get(2)?;
                let data_type: Option<String> = row.get(3)?;
                let interior: i32 = row.get(4)?;

                Ok(ColumnInfo {
                    name: name.into(),
                    nullable: is_nullable.unwrap_or(1) != 0,
                    position: (position + 1) as usize, // 0-based to 1-based
                    declared_type: data_type.filter(|t| !t.is_empty()),
                    interior: interior != 0,
                })
            })
            .map_err(|error| {
                Runtime::catalog("Failed to query bootstrap schema", error.to_string())
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                Runtime::catalog("Failed to read bootstrap schema", error.to_string())
            })?;

        Ok(Some(columns))
    }
}
