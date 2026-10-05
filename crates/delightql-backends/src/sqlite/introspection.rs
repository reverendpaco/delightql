// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// SQLite Database Introspection Implementation
//
// Implements DatabaseIntrospector trait for user-facing SQLite databases.
// This is for transpilation TARGETS, not runtime infrastructure.

use super::introspect::introspect_sqlite_database;
use delightql_types::diagnostic::Runtime;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity, DiscoveredRelation, StoredRowIdentity};
use delightql_types::Result;
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
            Runtime::poisoned(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Call the local introspect_sqlite_database() function
        // Schema is None because we're introspecting the main user database
        introspect_sqlite_database(&*conn, None)
            .map_err(|e| super::engine_error("Failed to introspect SQLite database", e))
    }

    fn introspect_entities_in_schema(&self, schema: &str) -> Result<Vec<DiscoveredEntity>> {
        let conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Call the local introspect_sqlite_database() function with schema parameter
        introspect_sqlite_database(&*conn, Some(schema)).map_err(|e| {
            super::engine_error(
                &format!("Failed to introspect SQLite schema '{}'", schema),
                e,
            )
        })
    }

    fn introspect_relation(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<DiscoveredRelation>> {
        let conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
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
        // A schema the connection does not hold holds no relation. TEMP
        // belongs to every connection, listed or not.
        if let Some(schema) = introspection_schema {
            let held = schema.eq_ignore_ascii_case("temp")
                || conn
                    .prepare("SELECT 1 FROM pragma_database_list WHERE name = ?1 COLLATE NOCASE")
                    .and_then(|mut statement| statement.exists([schema]))
                    .map_err(|e| {
                        super::engine_error(&format!("Failed to list the schemas holding '{}'", relation_name), e)
                    })?;
            if !held {
                return Ok(None);
            }
        }
        let columns =
            super::introspect::introspect_table_columns(&conn, introspection_schema, relation_name)
                .map_err(|e| {
                    super::engine_error(
                        &format!("Failed to introspect SQLite relation '{}'", relation_name),
                        e,
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

    /// An ordinary table guarantees its columns' declared types by affinity,
    /// a STRICT table by its type rules. A virtual table's module decides
    /// what it holds (an R*Tree keeps `-0.0` in a REAL coordinate), and a
    /// view computes its rows: neither guarantees them.
    fn storage_guarantees_declared_types(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<bool>> {
        let conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let mut kinds: Vec<String> = Vec::new();
        conn.pragma(schema, "table_list", relation_name, |row| {
            kinds.push(row.get::<_, String>(2)?);
            Ok(())
        })
        .map_err(|e| {
            super::engine_error(
                &format!("Failed to read the storage of SQLite relation '{}'", relation_name),
                e,
            )
        })?;
        Ok(match kinds.as_slice() {
            [kind] => match kind.as_str() {
                "table" | "shadow" => Some(true),
                "virtual" | "view" => Some(false),
                _ => None,
            },
            _ => None,
        })
    }

    /// A stored table keeps its rowid unless it is declared `WITHOUT ROWID`,
    /// where its rows are told apart by their primary key alone (`pk` in
    /// `table_xinfo` is a column's place in that key, from 1).
    /// A column is the rowid itself when it is the table's one primary key
    /// column, declared exactly `INTEGER`, and SQLite keeps no index for that
    /// key: every other primary key (`INTEGER PRIMARY KEY DESC` among them)
    /// has its own `pk` index. The names the table's columns occupy are
    /// `table_xinfo`'s, which lists the generated and hidden columns
    /// `table_info` omits. A view and a virtual table answer nothing.
    fn stored_row_identity(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<StoredRowIdentity>> {
        let conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let failed = |e: rusqlite::Error| {
            super::engine_error(
                &format!("Failed to read the row identity of SQLite relation '{}'", relation_name),
                e,
            )
        };
        let Some((owner, without_rowid)) = stored_table(&conn, schema, relation_name).map_err(failed)? else {
            return Ok(None);
        };
        let mut keys: Vec<(i64, String, String)> = Vec::new();
        let mut occupied: Vec<delightql_types::SqlIdentifier> = Vec::new();
        conn.pragma(Some(owner.as_str()), "table_xinfo", relation_name, |row| {
            let name = row.get::<_, String>(1)?;
            let place = row.get::<_, i64>(5)?;
            if place > 0 {
                keys.push((place, name.clone(), row.get::<_, String>(2)?));
            }
            occupied.push(name.as_str().into());
            Ok(())
        })
        .map_err(failed)?;
        keys.sort_by_key(|(place, _, _)| *place);
        if without_rowid {
            return Ok(Some(StoredRowIdentity::Unlocated {
                key: keys.iter().map(|(_, name, _)| name.as_str().into()).collect(),
            }));
        }
        let mut key_indexed = false;
        conn.pragma(Some(owner.as_str()), "index_list", relation_name, |row| {
            key_indexed |= row.get::<_, String>(3)? == "pk";
            Ok(())
        })
        .map_err(failed)?;
        let alias = match keys.as_slice() {
            [(_, name, declared)] if declared.eq_ignore_ascii_case("INTEGER") && !key_indexed => {
                Some(name.as_str().into())
            }
            _ => None,
        };
        Ok(Some(StoredRowIdentity::Located { alias, occupied }))
    }

    /// A stored table's generated columns: `table_xinfo`'s `hidden` is 2 for
    /// a virtual generated column and 3 for a stored one. A view and a
    /// virtual table answer nothing.
    fn computed_columns(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<Vec<delightql_types::SqlIdentifier>>> {
        let conn = self.connection.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire lock on SQLite connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let failed = |e: rusqlite::Error| {
            super::engine_error(
                &format!("Failed to read the generated columns of SQLite relation '{}'", relation_name),
                e,
            )
        };
        let Some((owner, _)) = stored_table(&conn, schema, relation_name).map_err(failed)? else {
            return Ok(None);
        };
        let mut computed = Vec::new();
        conn.pragma(Some(owner.as_str()), "table_xinfo", relation_name, |row| {
            if matches!(row.get::<_, i64>(6)?, 2 | 3) {
                computed.push(row.get::<_, String>(1)?.as_str().into());
            }
            Ok(())
        })
        .map_err(failed)?;
        Ok(Some(computed))
    }
}

/// The stored table an unqualified name reads (the temp schema's, then
/// main's, then an attached one's): its schema, and whether it is declared
/// `WITHOUT ROWID`. A view and a virtual table are none.
fn stored_table(
    conn: &Connection,
    schema: Option<&str>,
    relation_name: &str,
) -> rusqlite::Result<Option<(String, bool)>> {
    let mut listed: Vec<(String, String, bool)> = Vec::new();
    conn.pragma(schema, "table_list", relation_name, |row| {
        listed.push((row.get::<_, String>(0)?, row.get::<_, String>(2)?, row.get::<_, i64>(4)? != 0));
        Ok(())
    })?;
    let rank = |s: &str| match s {
        "temp" => 0,
        "main" => 1,
        _ => 2,
    };
    Ok(listed
        .into_iter()
        .min_by_key(|(s, _, _)| rank(s))
        .filter(|(_, kind, _)| matches!(kind.as_str(), "table" | "shadow"))
        .map(|(owner, _, without_rowid)| (owner, without_rowid)))
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

    #[test]
    fn storage_guarantees_declared_types_only_for_stored_tables() {
        let connection = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
        connection
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TABLE plain(x REAL);
                 CREATE TABLE strict_one(x REAL) STRICT;
                 CREATE VIRTUAL TABLE rt USING rtree(id, x0, x1);
                 CREATE VIEW v AS SELECT x FROM plain;
                 ATTACH DATABASE ':memory:' AS mounted;
                 CREATE TABLE mounted.far(x REAL);",
            )
            .unwrap();
        let introspector = SqliteIntrospector::new(connection);
        let fact = |schema: Option<&str>, name: &str| introspector.storage_guarantees_declared_types(schema, name).unwrap();

        assert_eq!(fact(None, "plain"), Some(true));
        assert_eq!(fact(None, "strict_one"), Some(true));
        assert_eq!(fact(None, "rt"), Some(false));
        assert_eq!(fact(None, "v"), Some(false));
        assert_eq!(fact(Some("mounted"), "far"), Some(true));
        assert_eq!(fact(None, "absent"), None);
    }
}
