// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! WHO OPENED a mounted schema, and who may therefore close it.
//!
//! One physical file may be named by more than one namespace, and naming it
//! twice must not OPEN it twice — one connection holding two handles on one
//! file cannot write through either while the other reads. So a mount binds
//! a schema the connection already holds rather than attaching a second one.
//!
//! That makes teardown two questions, not one. REFCOUNTING says whether any
//! binding still names the schema; OWNERSHIP says whether closing it was
//! ever this binding's to do. Neither substitutes for the other, and the
//! case that tells them apart is a schema whose opener is not a mount at
//! all: a session whose connection opens its database directly holds it as
//! SQLite's own `main`, which cannot be detached by anybody.
//!
//! The world here is that session — a real file-backed rusqlite connection,
//! the shape `ConnectionManager::open` builds for `--db <path>`.

use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
use delightql_types::{DatabaseConnection, DbValue};
use std::sync::{Arc, Mutex};

struct RealSqliteConnection {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

fn to_rusqlite(value: &DbValue) -> rusqlite::types::Value {
    match value {
        DbValue::Null => rusqlite::types::Value::Null,
        DbValue::Integer(i) => rusqlite::types::Value::Integer(*i),
        DbValue::Real(f) => rusqlite::types::Value::Real(*f),
        DbValue::Text(s) => rusqlite::types::Value::Text(s.clone()),
        DbValue::Blob(b) => rusqlite::types::Value::Blob(b.clone()),
    }
}

impl DatabaseConnection for RealSqliteConnection {
    fn execute(&self, sql: &str, params: &[DbValue]) -> delightql_types::Result<usize> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error("poisoned", e.to_string())
        })?;
        let vals: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
        let refs: Vec<&dyn rusqlite::ToSql> =
            vals.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        conn.execute(sql, refs.as_slice()).map_err(|e| {
            delightql_types::DelightQLError::database_error("Execute failed", e.to_string())
        })
    }

    fn last_insert_rowid(&self) -> delightql_types::Result<i64> {
        Ok(0)
    }

    fn query_row_values(
        &self,
        _sql: &str,
        _params: &[DbValue],
    ) -> delightql_types::Result<Option<Vec<DbValue>>> {
        Ok(None)
    }

    fn query_all_rows(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> delightql_types::Result<(Vec<String>, Vec<Vec<DbValue>>)> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error("poisoned", e.to_string())
        })?;
        let vals: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
        let refs: Vec<&dyn rusqlite::ToSql> =
            vals.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        let mut stmt = conn.prepare(sql).map_err(|e| {
            delightql_types::DelightQLError::database_error("Prepare failed", e.to_string())
        })?;
        let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let width = cols.len();
        let rows: std::result::Result<Vec<Vec<DbValue>>, _> = stmt
            .query_map(refs.as_slice(), |row| {
                let mut out = Vec::with_capacity(width);
                for index in 0..width {
                    out.push(match row.get_ref(index)? {
                        rusqlite::types::ValueRef::Null => DbValue::Null,
                        rusqlite::types::ValueRef::Integer(i) => DbValue::Integer(i),
                        rusqlite::types::ValueRef::Real(f) => DbValue::Real(f),
                        rusqlite::types::ValueRef::Text(s) => {
                            DbValue::Text(String::from_utf8_lossy(s).to_string())
                        }
                        rusqlite::types::ValueRef::Blob(b) => DbValue::Blob(b.to_vec()),
                    });
                }
                Ok(out)
            })
            .map_err(|e| {
                delightql_types::DelightQLError::database_error("Query failed", e.to_string())
            })?
            .collect();
        Ok((
            cols,
            rows.map_err(|e| {
                delightql_types::DelightQLError::database_error("Row read failed", e.to_string())
            })?,
        ))
    }
}

struct OneTable;
impl DatabaseIntrospector for OneTable {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![DiscoveredEntity {
            name: "t".into(),
            entity_type_id: 10,
            attributes: vec![DiscoveredAttribute {
                name: "k".into(),
                data_type: "INTEGER".to_string(),
                position: 0,
                is_nullable: true,
            }],
        }])
    }
}

/// Every schema the connection currently holds.
fn open_schemas(raw: &Arc<Mutex<rusqlite::Connection>>) -> Vec<String> {
    let conn = raw.lock().unwrap();
    let mut stmt = conn
        .prepare("SELECT name FROM pragma_database_list")
        .unwrap();
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    names
}

/// A session whose user connection opens the database file DIRECTLY, so the
/// file is SQLite's own `main` on that connection.
fn session_over_its_own_file() -> (
    DelightQLSystem,
    Arc<Mutex<rusqlite::Connection>>,
    String,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("w.sqlite");
    {
        let seed = rusqlite::Connection::open(&db_path).expect("seed open");
        seed.execute_batch("CREATE TABLE t (k INTEGER); INSERT INTO t VALUES (7);")
            .expect("seed schema");
    }
    let raw = Arc::new(Mutex::new(
        rusqlite::Connection::open(&db_path).expect("user open"),
    ));
    let adapter: Arc<Mutex<dyn DatabaseConnection>> =
        Arc::new(Mutex::new(RealSqliteConnection { conn: raw.clone() }));
    let system = DelightQLSystem::new(adapter, Box::new(OneTable), "sqlite").expect("system");
    let path = db_path.to_str().expect("utf-8 path").to_string();
    (system, raw, path, dir)
}

/// A session mounting its OWN database binds SQLite's `main`, and unmounting
/// that binding does not try to close it.
///
/// RED-BEFORE: teardown read the binding's alias with no record of who
/// opened it, so the last name to go issued `DETACH DATABASE 'main'` —
/// which SQLite refuses, rolling the catalog deletion back with it.
#[test]
fn unmounting_a_borrowed_main_neither_detaches_nor_fails() {
    let (mut system, raw, path, _dir) = session_over_its_own_file();
    system
        .mount_database(&path, "twin")
        .expect("a session may name its own database");

    // Nothing was attached: the file is the connection's `main`, and the
    // binding named that. A second handle would show up here.
    assert_eq!(
        open_schemas(&raw),
        ["main"],
        "naming the file must not have opened it again"
    );

    system
        .unmount_database("twin")
        .expect("unmounting a borrowed schema must not try to close it");

    // `main` is still there and still readable.
    assert_eq!(open_schemas(&raw), ["main"]);
    let rows: i64 = raw
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM main.t", [], |row| row.get(0))
        .expect("main survives the unmount");
    assert_eq!(rows, 1);
}

/// The `main` NAMESPACE empties back to its fixture face instead of being
/// destroyed, which is a second teardown road — and it must reach the same
/// answer about what it may close. A session naming its own database `main`
/// borrows SQLite's `main`, and emptying that namespace must not try to
/// detach it.
///
/// RED-BEFORE: `empty_main_namespace` built its cleanup identity straight
/// from the binding's alias, so this sequence issued `DETACH DATABASE 'main'`
/// while the ordinary road beside it was already refusing to.
#[test]
fn emptying_the_main_namespace_does_not_detach_a_borrowed_schema() {
    let (mut system, raw, path, _dir) = session_over_its_own_file();
    system
        .mount_database(&path, "main")
        .expect("a session may name its own database `main`");
    assert_eq!(open_schemas(&raw), ["main"]);

    system
        .unmount_database("main")
        .expect("emptying main must not try to close SQLite's own main");

    assert_eq!(open_schemas(&raw), ["main"]);
    let rows: i64 = raw
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM main.t", [], |row| row.get(0))
        .expect("main survives the unmount");
    assert_eq!(rows, 1);
}

/// Two names for one borrowed schema: the first out hands nothing over that
/// it did not have, and the last out still does not close what it borrowed.
#[test]
fn two_borrowed_names_never_close_the_schema_between_them() {
    let (mut system, raw, path, _dir) = session_over_its_own_file();
    system.mount_database(&path, "aa").expect("first name");
    system.mount_database(&path, "bb").expect("second name");

    system.unmount_database("aa").expect("first name goes");
    system
        .unmount_database("bb")
        .expect("last name goes without closing a schema it never opened");

    assert_eq!(open_schemas(&raw), ["main"]);
    let rows: i64 = raw
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM main.t", [], |row| row.get(0))
        .expect("main survives both unmounts");
    assert_eq!(rows, 1);
}

/// The OWNED half, proved on the connection rather than on a later mount.
///
/// A second file is attached because nothing already held it, so the first
/// binding owns it. Removing one of two names leaves it attached; removing
/// the last one CLOSES it — and the only way to see that is to ask the
/// connection which schemas it holds. Mounting the file again would succeed
/// either way, because a leaked attachment is exactly what the reuse road
/// looks for.
///
/// RED-BEFORE: with the final detach removed the side schema is still listed
/// after the last name goes.
#[test]
fn the_last_owner_out_closes_the_attachment_it_opened() {
    let (mut system, raw, _path, dir) = session_over_its_own_file();
    let side = dir.path().join("side.sqlite");
    {
        let seed = rusqlite::Connection::open(&side).expect("seed open");
        seed.execute_batch("CREATE TABLE t (k INTEGER); INSERT INTO t VALUES (9);")
            .expect("seed schema");
    }
    let side = side.to_str().expect("utf-8 path").to_string();

    system.mount_database(&side, "aa").expect("first name");
    let attached = open_schemas(&raw);
    assert_eq!(
        attached.len(),
        2,
        "a file the connection did not hold is attached: {attached:?}"
    );
    let schema = attached
        .iter()
        .find(|name| *name != "main")
        .expect("the attached schema")
        .clone();

    system.mount_database(&side, "bb").expect("second name");
    assert_eq!(
        open_schemas(&raw).len(),
        2,
        "the second name must borrow, not open the file again"
    );

    system.unmount_database("aa").expect("first name goes");
    assert!(
        open_schemas(&raw).contains(&schema),
        "removing one binding must not close the schema the other stands on"
    );

    system.unmount_database("bb").expect("last name goes");
    assert!(
        !open_schemas(&raw).contains(&schema),
        "the last binding on an OWNED attachment closes it"
    );
}
