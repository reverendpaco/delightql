// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// SQLite Database Introspection Implementation
//
// Implements DatabaseIntrospector trait for user-facing SQLite databases.
// This is for transpilation TARGETS, not runtime infrastructure.

use super::introspect::introspect_sqlite_database;
use delightql_types::introspect::{
    DatabaseIntrospector, DiscoveredEntity, DiscoveredRelation,
};
use delightql_types::{DelightQLError, Result};
use rusqlite::Connection;
use std::sync::{Arc, Mutex};

/// SQLite introspector for user databases (transpilation targets)
///
/// This implementation queries SQLite's system catalogs to discover tables and views.
/// - Uses `sqlite_master` to find entities
/// - Uses `PRAGMA table_info` to discover columns
///
/// NOTE: This is for user-facing databases, not the runtime _bootstrap database.
pub struct SqliteIntrospector {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteIntrospector {
    /// Create a new SQLite introspector
    pub fn new(connection: Arc<Mutex<Connection>>) -> Self {
        Self { connection }
    }
}

impl DatabaseIntrospector for SqliteIntrospector {
    fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
        let conn = self.connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Call the local introspect_sqlite_database() function
        // Schema is None because we're introspecting the main user database
        introspect_sqlite_database(&*conn, None).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to introspect SQLite database: {}", e),
                e.to_string(),
            )
        })
    }

    fn introspect_entities_in_schema(&self, schema: &str) -> Result<Vec<DiscoveredEntity>> {
        let conn = self.connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Call the local introspect_sqlite_database() function with schema parameter
        introspect_sqlite_database(&*conn, Some(schema)).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to introspect SQLite schema '{}': {}", schema, e),
                e.to_string(),
            )
        })
    }

    fn introspect_relation(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<DiscoveredRelation>> {
        let conn = self.connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        // `sqlite_temp_master` belongs to the connection-wide TEMP schema,
        // not to whichever attached schema routed the DelightQL namespace.
        // SQLite accepts its bare name from every routed namespace, while
        // `<attached>.sqlite_temp_master` does not exist.
        let introspection_schema = (relation_name != "sqlite_temp_master")
            .then_some(schema)
            .flatten();
        let columns =
            super::introspect::introspect_table_columns(&conn, introspection_schema, relation_name)
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to introspect SQLite relation '{}': {}",
                            relation_name, e
                        ),
                        e.to_string(),
                    )
                })?;
        if columns.is_empty() {
            return Ok(None);
        }

        Ok(Some(DiscoveredRelation {
            entity: DiscoveredEntity {
                name: relation_name.into(),
                entity_type_id: 10,
                attributes: columns,
            },
            backend_schema: introspection_schema.map(str::to_owned),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_introspection_reaches_sqlite_master_without_enumerating_it() {
        let connection = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        connection
            .lock()
            .unwrap()
            .execute("CREATE TABLE users(id INTEGER)", [])
            .unwrap();
        let introspector = SqliteIntrospector::new(connection);

        assert!(introspector
            .introspect_entities()
            .unwrap()
            .iter()
            .all(|entity| entity.name.as_str() != "sqlite_master"));

        let system_relation = introspector
            .introspect_relation(None, "sqlite_master")
            .unwrap()
            .expect("sqlite_master is directly addressable");
        let names: Vec<_> = system_relation
            .entity
            .attributes
            .iter()
            .map(|attribute| attribute.name.as_str())
            .collect();
        assert_eq!(names, ["type", "name", "tbl_name", "rootpage", "sql"]);
        assert_eq!(system_relation.backend_schema, None);

        assert!(introspector
            .introspect_relation(None, "does_not_exist")
            .unwrap()
            .is_none());
    }

    #[test]
    fn named_introspection_reaches_sqlite_temp_master() {
        let introspector =
            SqliteIntrospector::new(Arc::new(Mutex::new(Connection::open_in_memory().unwrap())));

        let system_relation = introspector
            .introspect_relation(Some("main"), "sqlite_temp_master")
            .unwrap()
            .expect("sqlite_temp_master is directly addressable");
        let names: Vec<_> = system_relation
            .entity
            .attributes
            .iter()
            .map(|attribute| attribute.name.as_str())
            .collect();
        assert_eq!(names, ["type", "name", "tbl_name", "rootpage", "sql"]);
        assert_eq!(system_relation.backend_schema, None);
    }

    #[test]
    fn named_introspection_keeps_an_attached_sqlite_master_schema() {
        let connection = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        connection
            .lock()
            .unwrap()
            .execute_batch(
                "ATTACH DATABASE ':memory:' AS mounted;
                 CREATE TABLE mounted.users(id INTEGER);",
            )
            .unwrap();
        let introspector = SqliteIntrospector::new(connection);

        let system_relation = introspector
            .introspect_relation(Some("mounted"), "sqlite_master")
            .unwrap()
            .expect("the attached persistent catalog is directly addressable");

        assert_eq!(system_relation.backend_schema.as_deref(), Some("mounted"));
    }
}
