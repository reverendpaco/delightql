// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// DuckDB Database Introspection Implementation
//
// Implements DatabaseIntrospector trait for user-facing DuckDB databases.
// This is for transpilation TARGETS, not runtime infrastructure.

use delightql_types::diagnostic::{DuckDb, Runtime};
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
use delightql_types::{DelightQLError, Result};
use duckdb::Connection;
use std::sync::{Arc, Mutex};

/// DuckDB introspector for user databases (transpilation targets)
///
/// This implementation queries DuckDB's information_schema to discover tables and views.
/// - Uses `information_schema.tables` to find entities
/// - Uses `information_schema.columns` to discover columns
pub struct DuckDBIntrospector {
    connection: Arc<Mutex<Connection>>,
}

impl DuckDBIntrospector {
    /// Create a new DuckDB introspector
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }
}

impl DatabaseIntrospector for DuckDBIntrospector {
    fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
        self.introspect_entities_in_schema("main")
    }

    fn introspect_entities_in_schema(&self, schema: &str) -> Result<Vec<DiscoveredEntity>> {
        let conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire lock on DuckDB connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut entities = Vec::new();

        // Query information_schema.tables to discover tables and views
        let table_query = format!(
            "
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = '{}'
              AND table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY table_name
        ",
            schema
        );

        let mut stmt = conn.prepare(&table_query).map_err(|e| {
            DelightQLError::from(DuckDb::Engine {
                message: format!("Failed to prepare DuckDB introspection query: {}", e),
            })
        })?;

        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?, // table_name
                    row.get::<_, String>(1)?, // table_type
                ))
            })
            .map_err(|e| {
                DelightQLError::from(DuckDb::Engine {
                    message: format!("Failed to execute DuckDB introspection query: {}", e),
                })
            })?;

        for result in rows {
            let (table_name, table_type) = result.map_err(|e| {
                DelightQLError::from(DuckDb::Engine {
                    message: format!("Failed to read DuckDB table row: {}", e),
                })
            })?;

            // Determine entity type: 10=DBPermanentTable, 11=DBPermanentView
            let entity_type_id = if table_type == "BASE TABLE" { 10 } else { 11 };

            // Introspect columns using information_schema.columns
            let attributes = introspect_table_columns(&*conn, schema, &table_name)?;

            entities.push(DiscoveredEntity {
                name: table_name.to_string().into(),
                entity_type_id,
                attributes,
            });
        }

        Ok(entities)
    }

    /// A DuckDB base table enforces its columns' declared types; a view
    /// computes its rows.
    fn storage_guarantees_declared_types(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<bool>> {
        let conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire lock on DuckDB connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let mut stmt = conn
            .prepare(
                "SELECT table_type FROM information_schema.tables \
                 WHERE table_schema = ? AND table_name = ?",
            )
            .map_err(|e| {
                DelightQLError::from(DuckDb::Engine {
                    message: format!("Failed to prepare DuckDB storage query: {}", e),
                })
            })?;
        let kinds: Vec<String> = stmt
            .query_map([schema.unwrap_or("main"), relation_name], |row| row.get::<_, String>(0))
            .and_then(|rows| rows.collect())
            .map_err(|e| {
                DelightQLError::from(DuckDb::Engine {
                    message: format!("Failed to read DuckDB storage: {}", e),
                })
            })?;
        Ok(match kinds.as_slice() {
            [kind] if kind == "BASE TABLE" => Some(true),
            [kind] if kind == "VIEW" => Some(false),
            _ => None,
        })
    }
}

/// Introspect columns for a specific table using information_schema.columns
///
/// # Arguments
/// * `conn` - DuckDB connection
/// * `schema` - Schema name
/// * `table_name` - Name of table/view to introspect
///
/// # Returns
/// * `Ok(Vec<DiscoveredAttribute>)` - List of columns with metadata
/// * `Err(DelightQLError)` - If query fails
fn introspect_table_columns(
    conn: &Connection,
    schema: &str,
    table_name: &str,
) -> Result<Vec<DiscoveredAttribute>> {
    let mut attributes = Vec::new();

    let query = format!(
        "
        SELECT column_name, data_type, ordinal_position, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{}'
          AND table_name = ?1
        ORDER BY ordinal_position
    ",
        schema
    );

    let mut stmt = conn.prepare(&query).map_err(|e| {
        DelightQLError::from(DuckDb::Engine {
            message: format!("Failed to prepare DuckDB column introspection query: {}", e),
        })
    })?;

    let rows = stmt
        .query_map([table_name], |row| {
            Ok((
                row.get::<_, String>(0)?, // column_name
                row.get::<_, String>(1)?, // data_type
                row.get::<_, i32>(2)?,    // ordinal_position
                row.get::<_, String>(3)?, // is_nullable ('YES' or 'NO')
            ))
        })
        .map_err(|e| {
            DelightQLError::from(DuckDb::Engine {
                message: format!("Failed to execute DuckDB column introspection query: {}", e),
            })
        })?;

    for result in rows {
        let (name, data_type, position, is_nullable_str) = result.map_err(|e| {
            DelightQLError::from(DuckDb::Engine {
                message: format!("Failed to read DuckDB column row: {}", e),
            })
        })?;

        attributes.push(DiscoveredAttribute {
            name: name.into(),
            data_type,
            position: position - 1, // Convert 1-based to 0-based
            is_nullable: is_nullable_str == "YES",
        });
    }

    Ok(attributes)
}
