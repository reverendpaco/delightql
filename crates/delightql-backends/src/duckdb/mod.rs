// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// DuckDB Integration Module for DelightQL
///
/// This module provides complete DuckDB integration for DelightQL, including:
/// - Connection management
/// - SQL execution
/// - DelightQL-to-SQL transpilation and execution
/// - Schema introspection
/// - Thread-safe operations

pub mod connection;
pub mod db_adapter;
pub mod executor;
pub mod introspection;
pub mod schema_provider;
pub mod value;

// Re-export public types and traits
pub use connection::{ConnectionInfo, DuckDBConnectionManager};
pub use db_adapter::DuckDBConnection;
pub use executor::{ColumnInfo, DuckDBExecutor, PreparedStatement, QueryResult, TableSchema};
pub use introspection::DuckDBIntrospector;
pub use schema_provider::DuckDBSchemaProvider;
pub use value::SqlValue;

// DynamicDuckDBSchema implementation for DatabaseSchema trait
use delightql_types::{DelightQLError, Result};
use delightql_types::schema::{ColumnInfo as ResolverColumnInfo, DatabaseSchema};
use delightql_types::namespace::NamespacePath;
use duckdb::Connection;
use std::sync::{Arc, Mutex};

/// Dynamic schema provider that queries DuckDB's information_schema directly
pub struct DynamicDuckDBSchema {
    connection: Arc<Mutex<Connection>>,
}

impl DynamicDuckDBSchema {
    /// Create from an existing connection
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }
}

impl DatabaseSchema for DynamicDuckDBSchema {
    fn get_table_columns(&self, schema: Option<&str>, table_name: &str) -> Result<Option<Vec<ResolverColumnInfo>>> {
        let conn = self.connection.lock().map_err(|error| {
            DelightQLError::connection_poison_error(
                "Failed to acquire DuckDB schema connection",
                error.to_string(),
            )
        })?;

        // Build the query based on whether schema is specified
        let query = if let Some(schema_name) = schema {
            format!(
                "SELECT column_name, is_nullable, ordinal_position, data_type
                 FROM information_schema.columns
                 WHERE table_schema = '{}' AND table_name = '{}'
                 ORDER BY ordinal_position",
                schema_name, table_name
            )
        } else {
            format!(
                "SELECT column_name, is_nullable, ordinal_position, data_type
                 FROM information_schema.columns
                 WHERE table_name = '{}'
                 ORDER BY ordinal_position",
                table_name
            )
        };

        let mut stmt = conn.prepare(&query).map_err(|error| {
            DelightQLError::database_error("Failed to prepare DuckDB schema query", error.to_string())
        })?;
        let columns = stmt
            .query_map([], |row| {
                let name: String = row.get(0)?;
                let is_nullable: String = row.get(1)?;
                let position: i32 = row.get(2)?;
                let data_type: Option<String> = row.get(3)?;

                Ok(ResolverColumnInfo {
                    name: name.into(),
                    nullable: is_nullable == "YES",
                    position: position as usize,
                    declared_type: data_type.filter(|t| !t.is_empty()),
                })
            })
            .map_err(|error| {
                DelightQLError::database_error("Failed to query DuckDB schema", error.to_string())
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                DelightQLError::database_error("Failed to read DuckDB schema", error.to_string())
            })?;

        if columns.is_empty() {
            Ok(None)
        } else {
            Ok(Some(columns))
        }
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> Result<bool> {
        let conn = self.connection.lock().map_err(|error| {
            DelightQLError::connection_poison_error(
                "Failed to acquire DuckDB schema connection",
                error.to_string(),
            )
        })?;

        let query = if let Some(schema_name) = schema {
            format!(
                "SELECT 1 FROM information_schema.tables
                 WHERE table_schema = '{}' AND table_name = '{}'
                 LIMIT 1",
                schema_name, table_name
            )
        } else {
            format!(
                "SELECT 1 FROM information_schema.tables
                 WHERE table_name = '{}'
                 LIMIT 1",
                table_name
            )
        };

        let mut stmt = conn.prepare(&query).map_err(|error| {
            DelightQLError::database_error("Failed to prepare DuckDB schema query", error.to_string())
        })?;
        let mut rows = stmt.query_map([], |_| Ok(())).map_err(|error| {
            DelightQLError::database_error("Failed to query DuckDB schema", error.to_string())
        })?;
        match rows.next() {
            None => Ok(false),
            Some(Ok(())) => Ok(true),
            Some(Err(error)) => Err(DelightQLError::database_error(
                "Failed to read DuckDB schema",
                error.to_string(),
            )),
        }
    }
}
