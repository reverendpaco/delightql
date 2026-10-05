// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Which connection, dialect and physical schema a statement uses, and the
//! catalog reads the compiler host serves for that decision.

use super::{DelightQLSystem, PhysicalRead};
use crate::ddl::lifecycle::refuse_if_blueprint;
use crate::diagnostic::{Internal, Runtime};
use crate::error::{DelightQLError, Result};
use delightql_types::DatabaseConnection;
use log::debug;
use rusqlite::OptionalExtension;
use std::sync::{Arc, Mutex};

impl DelightQLSystem {
    /// The SQL dialect of the connection a query routes to — the
    /// dialect-from-connection inference (ALL-SQL-TARGETING). `None` or the
    /// user connection (id 2) resolve to the PRIMARY's db_type (so a
    /// `--db postgres:///...` primary compiles postgres-spelled SQL);
    /// mounted connections resolve via their `connection` row:
    /// connection_type 3 = postgres, 4 = duckdb, siso (6) parses the
    /// `delightql-siso://<profile>/...` profile. Anything unknown is
    /// canonical SQLite.
    pub fn dialect_for_connection(
        &self,
        connection_id: Option<i64>,
    ) -> crate::pipeline::generator::SqlDialect {
        use crate::pipeline::generator::SqlDialect;
        let primary = || {
            SqlDialect::from_family_name(&self.db_type.to_lowercase()).unwrap_or(SqlDialect::SQLite)
        };
        let id = match connection_id {
            Some(id) if id != 2 => id,
            _ => return primary(),
        };
        let Ok(conn) = self.bootstrap_connection.lock() else {
            return primary();
        };
        let row: Option<(i32, String)> = conn
            .query_row(
                "SELECT connection_type, resource_uri FROM connection WHERE id = ?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        match row {
            Some((3, _)) => SqlDialect::PostgreSQL,
            Some((4, _)) => SqlDialect::DuckDB,
            Some((6, uri)) => uri
                .strip_prefix("delightql-siso://")
                .and_then(|rest| rest.split('/').next())
                .and_then(SqlDialect::from_family_name)
                .unwrap_or(SqlDialect::SQLite),
            _ => SqlDialect::SQLite,
        }
    }

    /// Whether the catalog records a connection's transport as siso
    /// (connection_type 6). A catalog that cannot be read, or a connection
    /// it does not record, is an error, never "not siso".
    pub(crate) fn connection_is_siso(&self, id: i64) -> Result<bool> {
        let conn = self
            .bootstrap_connection
            .lock()
            .map_err(|_| Internal::invariant("connection transport", "the bootstrap catalog's lock is poisoned"))?;
        let connection_type = conn
            .query_row("SELECT connection_type FROM connection WHERE id = ?1", [id], |r| r.get::<_, i64>(0))
            .map_err(|e| {
                Internal::invariant("connection transport", &format!("the catalog records no transport for connection {id}: {e}"))
            })?;
        Ok(connection_type == 6)
    }

    /// Get the appropriate connection for executing a query based on connection_id
    ///
    /// Routes query execution to the correct physical connection:
    /// - connection_id=1 → Bootstrap connection (internal metadata)
    /// - connection_id=2 → User connection (target database)
    ///
    /// # Arguments
    /// * `connection_id` - The connection ID from cartridge metadata
    ///
    /// # Returns
    /// * `Ok(Arc<Mutex<dyn DatabaseConnection>>)` - Arc reference to the appropriate connection
    /// * `Err(...)` - If connection_id is invalid/unknown
    pub fn get_connection(&self, connection_id: i64) -> Result<Arc<Mutex<dyn DatabaseConnection>>> {
        self.connection_map
            .get(&connection_id)
            .cloned()
            .ok_or_else(|| {
                DelightQLError::from(Runtime::General {
                    message: "Unknown connection ID".to_string(),
                    details: format!(
                        "Connection ID {} is not recognized. Valid IDs: 1 (bootstrap), 2 (user)",
                        connection_id
                    ),
                })
            })
    }

    /// Resolve a namespace path to its backend schema name and connection ID
    ///
    /// This is an engine implementation detail that queries the internal _bootstrap
    /// metadata to map namespace paths to backend schema names and connection routing info.
    /// This method encapsulates all bootstrap access, keeping it internal to the engine.
    ///
    /// # Arguments
    /// * `path` - The namespace path to resolve
    ///
    /// # Returns
    /// * `Ok(Some((schema_name, connection_id)))` - Namespace resolved to backend schema and connection
    /// * `Ok(None)` - Namespace not found or has no activated entities
    /// * `Err(...)` - Database error during resolution
    pub fn resolve_namespace_path(
        &self,
        path: &delightql_types::namespace::NamespacePath,
    ) -> Result<Option<(Option<String>, i64)>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap database lock for namespace resolution",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // META-CIRCULAR IMPLEMENTATION: Use bootstrap.namespace for namespace resolution
        // Build the fully-qualified namespace path (e.g., "main" or "sys::cartridges")
        // DEFAULT: Empty namespace path → "main" namespace
        let fq_name = if path.is_empty() {
            "main".to_string()
        } else {
            let path_parts: Vec<String> = path
                .iter()
                .map(|segment| segment.name.to_string())
                .collect();
            path_parts.join("::")
        };

        // Step 1: Look up namespace in bootstrap.namespace by fq_name
        // NOTE: _bootstrap is a separate connection, NOT attached, so no schema prefix needed
        debug!("resolve_namespace_path: Looking up fq_name={}", fq_name);
        let namespace_id = match conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [&fq_name],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(id) => {
                debug!("resolve_namespace_path: Found namespace_id={}", id);
                // Blueprint inertness: refuse to resolve any entity through
                // an archived blueprint namespace (or a descendant of one). This
                // is the sole namespace-qualified resolution chokepoint — bare
                // table lookups use `lookup_table` and never reach here, so the
                // scan stays off the hot path. The catalog functor
                // (`{blueprint}::(*)`) resolves through `sys::meta`, not this
                // path, so it stays visible (pinned by companion_linear--61).
                refuse_if_blueprint(&conn, &fq_name)?;
                id
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // THE ONLY OTHER ROUTE — an `alias!` shorthand, itself an
                // exact route. A real namespace always beats a same-named
                // alias (registration refuses the collision); the target
                // re-enters the blueprint guard under its canonical name.
                // Nothing searches the children of `home` or of an enlisted
                // namespace for a plain spelling.
                match conn.query_row(
                    "SELECT n.id, n.fq_name FROM namespace_alias a \
                     JOIN namespace n ON n.id = a.target_namespace_id \
                     WHERE a.alias = ?1",
                    [&fq_name],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                ) {
                    Ok((id, target_fq)) => {
                        refuse_if_blueprint(&conn, &target_fq)?;
                        id
                    }
                    Err(rusqlite::Error::QueryReturnedNoRows) => {
                        debug!("resolve_namespace_path: Namespace '{}' not found", fq_name);
                        return Ok(None);
                    }
                    Err(e) => {
                        return Err(Runtime::catalog(
                            "Failed to resolve namespace alias",
                            e.to_string(),
                        ));
                    }
                }
            }
            Err(e) => {
                if e.to_string().contains("no such table") {
                    // Bootstrap table doesn't exist - system not initialized
                    return Ok(None);
                }
                return Err(Runtime::catalog(
                    "Failed to query bootstrap.namespace",
                    e.to_string(),
                ));
            }
        };

        // Step 2: mounted namespaces resolve routing and qualification from
        // their authoritative binding, including valid empty mounts.
        let mounted = conn.query_row(
            "SELECT CASE
                        WHEN m.qualification = 'aliased' THEN m.attach_alias
                        WHEN m.qualification = 'engine_schema' THEN m.engine_schema
                        ELSE NULL
                    END,
                    c.connection_id
             FROM mount m
             JOIN cartridge c ON c.id = m.cartridge_id
             WHERE m.namespace_id = ?1",
            [namespace_id],
            |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?)),
        );
        match mounted {
            Ok(binding) => return Ok(Some(binding)),
            Err(rusqlite::Error::QueryReturnedNoRows) => {}
            Err(e) => {
                return Err(Runtime::catalog(
                    "Failed to resolve namespace mount binding",
                    e.to_string(),
                ));
            }
        }

        // Non-mount namespaces retain the cartridge source namespace model.
        let result = conn.query_row(
            "SELECT DISTINCT c.source_ns, c.connection_id
             FROM activated_entity ae
             JOIN cartridge c ON ae.cartridge_id = c.id
             WHERE ae.namespace_id = ?1
               -- Pure-DQL cartridges have no external connection.  They are
               -- consult definitions, not a backend route; leaving their
               -- NULL connection_id in this routing query turns an otherwise
               -- ordinary namespace miss into a row-decoding failure.
               AND c.connection_id IS NOT NULL
               AND NOT EXISTS (
                    SELECT 1 FROM mount m WHERE m.cartridge_id = c.id
               )
             LIMIT 1",
            [namespace_id],
            |row| {
                let source_ns = row.get::<_, Option<String>>(0)?;
                let connection_id = row.get::<_, i64>(1)?;
                Ok((source_ns, connection_id))
            },
        );

        match result {
            Ok((source_ns, connection_id)) => Ok(Some((source_ns, connection_id))),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Namespace exists but has no activated entities
                Ok(None)
            }
            Err(e) => Err(Runtime::catalog(
                "Failed to resolve backend schema and connection from bootstrap",
                e.to_string(),
            )),
        }
    }

    /// [`Self::physical_read`] for an identity given by its catalog row (when
    /// the catalog rows it), its namespace and its name.
    pub(crate) fn physical_read_of(
        &self,
        entity_id: Option<i64>,
        namespace_fq: &str,
        entity_name: &str,
    ) -> Result<PhysicalRead> {
        if let Some(entity_id) = entity_id {
            let recorded: Option<i64> = {
                let conn = self.bootstrap_connection.lock().map_err(|e| {
                    Runtime::poisoned(
                        "Failed to acquire bootstrap lock for session placement",
                        format!("Connection was poisoned: {}", e),
                    )
                })?;
                conn.query_row(
                    "SELECT c.connection_id FROM session_overlay so
                     JOIN entity e ON e.id = so.entity_id
                     JOIN cartridge c ON c.id = e.cartridge_id
                     WHERE so.entity_id = ?1",
                    [entity_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| Runtime::catalog("session placement", e.to_string()))?
            };
            if let Some(connection_id) = recorded {
                return Ok(PhysicalRead {
                    connection_id,
                    backend_schema: None,
                });
            }
        }
        let Some((backend_schema, connection_id)) = self.resolve_namespace_path(
            &delightql_types::namespace::NamespacePath::from_fq_string(namespace_fq),
        )?
        else {
            return Err(Internal::invariant(
                "system::physical_read",
                format!("served entity '{entity_name}' names no namespace '{namespace_fq}'"),
            ));
        };
        if connection_id == 1 {
            return Ok(PhysicalRead {
                connection_id,
                backend_schema,
            });
        }
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for shadow judgment",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let facts: &dyn crate::definition_catalog::DefinitionCatalog = &*conn;
        if facts
            .session_holders(connection_id, entity_name)?
            .is_empty()
        {
            return Ok(PhysicalRead {
                connection_id,
                backend_schema,
            });
        }
        // Only a data namespace has a durable placement to spell; any other
        // namespace keeps its read qualification.
        let durable = match crate::creation_target::DataTarget::read(
            facts,
            crate::definition_catalog::NamespaceKey::Fq(namespace_fq),
            "a durable read",
        ) {
            Ok(data) => Some(data.durable().clone()),
            Err(_) => None,
        };
        drop(conn);
        let Some(durable) = durable else {
            return Ok(PhysicalRead {
                connection_id,
                backend_schema,
            });
        };
        match durable.address_past_session(self.dialect_for_connection(Some(connection_id))) {
            Some(schema) => Ok(PhysicalRead {
                connection_id,
                backend_schema: Some(schema),
            }),
            // A creation refuses this pair before execution, so the durable
            // object entered the catalog after the session one, by a later
            // mount or refresh; every spelling of it would answer with the
            // session object.
            None => Err(DelightQLError::from(Runtime::Unsupported {
                message: format!(
                    "{namespace_fq}.{entity_name} cannot be read exactly: a session object on its \
                     connection holds the same name, and this engine resolves every \
                     spelling of the durable object's schema into the session pool first"
                ),
            })),
        }
    }

    /// The registered `output_column` attributes of one entity, in position
    /// order — the per-entity form of what `BootstrapBackedSchema` answers
    /// by name.
    pub fn output_columns_for_entity(
        &self,
        entity_id: i64,
    ) -> Result<Vec<delightql_types::schema::ColumnInfo>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for entity columns",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let mut stmt = conn
            .prepare(
                "SELECT ea.attribute_name, ea.position, ea.is_nullable, ea.data_type,
                        EXISTS (SELECT 1 FROM interior_entity ie
                                 WHERE ie.parent_entity_id = ea.entity_id
                                   AND ie.column_name = ea.attribute_name) AS interior
                 FROM entity_attribute ea
                 WHERE ea.entity_id = ?1 AND ea.attribute_type = 'output_column'
                 ORDER BY ea.position",
            )
            .map_err(|e| Runtime::catalog("entity column query", e.to_string()))?;
        let cols = stmt
            .query_map([entity_id], |row| {
                let name: String = row.get(0)?;
                let position: i32 = row.get(1)?;
                let is_nullable: Option<i32> = row.get(2)?;
                let data_type: Option<String> = row.get(3)?;
                let interior: i32 = row.get(4)?;
                Ok(delightql_types::schema::ColumnInfo {
                    name: name.into(),
                    nullable: is_nullable.unwrap_or(1) != 0,
                    position: (position + 1) as usize, // 0-based to 1-based
                    declared_type: data_type.filter(|t| !t.is_empty()),
                    interior: interior != 0,
                })
            })
            .map_err(|e| Runtime::catalog("entity column query", e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(cols)
    }
}
