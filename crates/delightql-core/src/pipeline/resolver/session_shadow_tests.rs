// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Temp shadows main — SQLite's
//! semantics, UNQUALIFIED names only. The shadow is a resolution
//! PREFERENCE, not a catalog edit:
//!
//! - created-object registration retires ONLY the run's own prior
//!   registration (the `session://materialized` cartridge), never a
//!   mount-introspected physical entity;
//! - a bare `staged(*)` prefers the session-materialized temp;
//! - a qualified `main.staged(*)` reaches the physical table.
//!
//! The world is REAL: a file-backed SQLite database holding a physical
//! `staged` (one row, a = 999) and `orders`, wired as the system's user
//! connection through a minimal rusqlite adapter, so `temp_table!`'s
//! registration tail (PRAGMA read-back on the user connection) and the
//! compiled SQL both run against genuine engine state. The effects ball's
//! scratch--52_qualified_read_reaches_physical pins the same contract
//! end-to-end through the dql binary.

use super::{resolve_query_with, ResolutionConfig};
use crate::pipeline::{ast_unresolved, danger_gates, generator, refiner, transformer};
use crate::resolution::ResolverCore;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
use delightql_types::{DatabaseConnection, DbValue};
use std::sync::{Arc, Mutex};

// ------------------------------------------------------------------
// A real user connection: minimal rusqlite adapter (the shape of
// delightql-backends' SqliteConnection, which core cannot depend on).
// ------------------------------------------------------------------

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
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error("poisoned", e.to_string())
        })?;
        Ok(conn.last_insert_rowid())
    }

    fn query_row_values(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> delightql_types::Result<Option<Vec<DbValue>>> {
        let conn = self.conn.lock().map_err(|e| {
            delightql_types::DelightQLError::connection_poison_error("poisoned", e.to_string())
        })?;
        let vals: Vec<rusqlite::types::Value> = params.iter().map(to_rusqlite).collect();
        let refs: Vec<&dyn rusqlite::ToSql> =
            vals.iter().map(|v| v as &dyn rusqlite::ToSql).collect();
        match conn.query_row(sql, refs.as_slice(), |row| {
            let n = row.as_ref().column_count();
            let mut out = Vec::with_capacity(n);
            for i in 0..n {
                let v = match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => DbValue::Null,
                    rusqlite::types::ValueRef::Integer(i) => DbValue::Integer(i),
                    rusqlite::types::ValueRef::Real(f) => DbValue::Real(f),
                    rusqlite::types::ValueRef::Text(s) => {
                        DbValue::Text(String::from_utf8_lossy(s).to_string())
                    }
                    rusqlite::types::ValueRef::Blob(b) => DbValue::Blob(b.to_vec()),
                };
                out.push(v);
            }
            Ok(out)
        }) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(delightql_types::DelightQLError::database_error(
                "Query failed",
                e.to_string(),
            )),
        }
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
        let n = cols.len();
        let rows: Result<Vec<Vec<DbValue>>, _> = stmt
            .query_map(refs.as_slice(), |row| {
                let mut out = Vec::with_capacity(n);
                for i in 0..n {
                    out.push(match row.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => DbValue::Null,
                        rusqlite::types::ValueRef::Integer(v) => DbValue::Integer(v),
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
        let rows = rows.map_err(|e| {
            delightql_types::DelightQLError::database_error("Row read failed", e.to_string())
        })?;
        Ok((cols, rows))
    }
}

// ------------------------------------------------------------------
// The world: physical staged(a = 999) + orders, mount-introspected
// into namespace main (the queue-introspector pattern of
// effect_transformer/tests.rs, but over a REAL connection).
// ------------------------------------------------------------------

fn entity(name: &str, cols: &[&str]) -> DiscoveredEntity {
    DiscoveredEntity {
        name: name.into(),
        entity_type_id: 10,
        attributes: cols
            .iter()
            .enumerate()
            .map(|(i, c)| DiscoveredAttribute {
                name: (*c).into(),
                data_type: "TEXT".to_string(),
                position: i as i32,
                is_nullable: true,
            })
            .collect(),
    }
}

const ORDER_COLS: &[&str] = &["order_id", "customer_id", "region", "amount"];

struct MainIntrospector;
impl DatabaseIntrospector for MainIntrospector {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![entity("staged", &["a"]), entity("orders", ORDER_COLS)])
    }
}

/// (system, raw user connection, tempdir guard). The raw handle IS the
/// system's user connection — temp tables created on it live in the temp
/// schema the created-object PRAGMA read-back sees.
fn world() -> (
    DelightQLSystem,
    Arc<Mutex<rusqlite::Connection>>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("w.sqlite");
    {
        let seed = rusqlite::Connection::open(&db_path).expect("seed open");
        seed.execute_batch(
            "CREATE TABLE staged (a INTEGER);
             INSERT INTO staged VALUES (999);
             CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, region TEXT, amount INTEGER);
             INSERT INTO orders VALUES (101, 1, 'EU', 250), (102, 2, 'US', 80), (103, 3, 'EU', 0);",
        )
        .expect("seed schema");
    }
    let raw = Arc::new(Mutex::new(
        rusqlite::Connection::open(&db_path).expect("user open"),
    ));
    let adapter: Arc<Mutex<dyn DatabaseConnection>> =
        Arc::new(Mutex::new(RealSqliteConnection { conn: raw.clone() }));
    let mut system = DelightQLSystem::new(adapter, Box::new(MainIntrospector), "sqlite")
        .expect("system should build");
    system
        .mount_database(db_path.to_str().unwrap(), "main")
        .expect("mount main");
    (system, raw, dir)
}

/// Create the temp table a `temp_table!(staged(*))(*)` run would have created,
/// then run the post-run registration tail exactly as relay/entry.rs's
/// `play_plan` does.
fn create_and_register_temp_staged(
    system: &mut DelightQLSystem,
    raw: &Arc<Mutex<rusqlite::Connection>>,
) {
    raw.lock()
        .unwrap()
        .execute_batch(
            "CREATE TEMP TABLE staged AS SELECT order_id, customer_id, region, amount \
             FROM orders WHERE region = 'EU';",
        )
        .expect("create temp staged");
    let registered = system
        .register_run_created_objects_with(
            &[crate::pipeline::compiled_query::PlanCreatedObject {
                name: "staged".to_string(),
                is_view: false,
                connection_id: Some(2),
            }],
            &crate::system::RealCreatedObjectCatalog,
        )
        .expect("registration should not error");
    assert!(
        matches!(
            registered.as_slice(),
            [crate::external_effects::RegistrationOutcome::Registered]
        ),
        "temp staged should read back and register"
    );
}

// ------------------------------------------------------------------
// Compile + execute helpers.
// ------------------------------------------------------------------

fn compile_plain(source: &str, system: &DelightQLSystem) -> crate::error::Result<String> {
    let tree = crate::pipeline::parse::query_sequence(source).expect("source should parse");
    let normalized =
        crate::pipeline::parse::normalize_sequence(&tree).expect("source should normalize");
    let mut queries = normalized.into_queries();
    assert_eq!(queries.len(), 1, "one statement expected");
    let query: ast_unresolved::Query = queries.remove(0).query;
    let schema = system.get_schema()?;
    let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let mut registry = ResolverCore::new_with_system(schema, system, &identities);
    let mut env = crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::session(&registry.consult, "home")?,
    );
    let mut fold = super::resolver_fold::ResolverFold::new(
        &mut registry,
        &mut env,
        ResolutionConfig::default(),
    );
    let resolved = resolve_query_with(&mut fold, query)?.into_query();
    drop(fold);
    let gates = danger_gates::DangerGateMap::with_defaults();
    let refined = refiner::refine_query_with_gates(resolved, gates.clone(), &identities)?;
    let names_handle = identities.names();
    let ctx = transformer::TransformCtx {
        relations: identities.seal(),
        identities: std::rc::Rc::clone(&names_handle),
        outer_sites: Vec::new(),
        names: transformer::builder::NameGenerator::new(std::rc::Rc::clone(&names_handle)),
        danger_gates: gates,
    };
    let sql_ast = transformer::transform(refined, &ctx)?.without_obligations()?;
    let names = generator::baptise_statements(&names_handle, &[&sql_ast])
        .map_err(|e| e.into_delightql_error("session-shadow SQL naming failed"))?;
    generator::SqlGenerator::new(&names)
        .generate_statement(&sql_ast)
        .map_err(|e| {
            crate::error::DelightQLError::validation_error(
                format!("SQL generation failed: {e}"),
                "session-shadow test chain",
            )
        })
}

/// Execute compiled SQL on the raw user connection (the engine the CLI's
/// relay would route to) and return string rows.
fn run_sql(raw: &Arc<Mutex<rusqlite::Connection>>, sql: &str) -> Vec<Vec<String>> {
    let conn = raw.lock().unwrap();
    let mut stmt = conn
        .prepare(sql)
        .unwrap_or_else(|e| panic!("SQL failed to prepare: {e}\n{sql}"));
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let v: rusqlite::types::Value = row.get(i)?;
            out.push(match v {
                rusqlite::types::Value::Null => "NULL".to_string(),
                rusqlite::types::Value::Integer(i) => i.to_string(),
                rusqlite::types::Value::Real(f) => f.to_string(),
                rusqlite::types::Value::Text(s) => s,
                rusqlite::types::Value::Blob(_) => "<blob>".to_string(),
            });
        }
        Ok(out)
    })
    .unwrap_or_else(|e| panic!("SQL failed to run: {e}\n{sql}"))
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

/// Count entities named `name` activated in `main`, split into
/// (session-materialized, other-cartridge) — the catalog-shape probe.
fn staged_entity_split(system: &DelightQLSystem, name: &str) -> (i64, i64) {
    let conn = system.get_bootstrap_connection();
    let guard = conn.lock().unwrap();
    guard
        .query_row(
            "SELECT
                 COALESCE(SUM(CASE WHEN c.source_uri = 'session://materialized' THEN 1 ELSE 0 END), 0),
                 COALESCE(SUM(CASE WHEN c.source_uri <> 'session://materialized' THEN 1 ELSE 0 END), 0)
             FROM activated_entity ae
             JOIN entity e ON e.id = ae.entity_id
             JOIN cartridge c ON c.id = e.cartridge_id
             JOIN namespace n ON n.id = ae.namespace_id
             WHERE e.name = ?1 AND n.fq_name = 'main'",
            [name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("catalog probe")
}

// ------------------------------------------------------------------
// Control: the world compiles and answers correctly BEFORE any temp
// exists — if this fails, the fixture is broken and the other tests
// prove nothing.
// ------------------------------------------------------------------

#[test]
fn control_physical_staged_answers_before_any_temp() {
    let (system, raw, _dir) = world();
    let sql = compile_plain("main.staged(*)", &system).expect("qualified compile");
    assert_eq!(run_sql(&raw, &sql), vec![vec!["999".to_string()]]);
    let sql = compile_plain("staged(*)", &system).expect("bare compile");
    assert_eq!(run_sql(&raw, &sql), vec![vec!["999".to_string()]]);
}

// ------------------------------------------------------------------
// (a) QUALIFIED reads reach the physical table (the F2 fix proper).
// ------------------------------------------------------------------

#[test]
fn qualified_read_reaches_physical_after_same_name_temp() {
    let (mut system, raw, _dir) = world();
    create_and_register_temp_staged(&mut system, &raw);

    let sql =
        compile_plain("main.staged(*)", &system).expect("qualified read should still compile");
    let rows = run_sql(&raw, &sql);
    assert_eq!(
        rows,
        vec![vec!["999".to_string()]],
        "main.staged(*) must answer the PHYSICAL row (a = 999), not the temp's \
         order rows (materialize-pipe §6: shadow is unqualified-only). SQL was:\n{sql}"
    );
}

// ------------------------------------------------------------------
// (b) BARE reads prefer the session-materialized temp (SQLite's
// temp-shadows-main semantics).
// ------------------------------------------------------------------

#[test]
fn bare_read_prefers_session_materialized_temp() {
    let (mut system, raw, _dir) = world();
    create_and_register_temp_staged(&mut system, &raw);

    let sql = compile_plain("staged(*)", &system).expect("bare read should compile");
    let rows = run_sql(&raw, &sql);
    // The temp's EU order rows — 2 of them, 4 columns.
    assert_eq!(
        rows.len(),
        2,
        "bare staged(*) must answer the temp's 2 EU rows. SQL was:\n{sql}"
    );
    assert!(
        rows.iter().all(|r| r.len() == 4 && r[2] == "EU"),
        "bare staged(*) must carry the temp's shape/rows, got {rows:?}. SQL:\n{sql}"
    );
}

// ------------------------------------------------------------------
// (c) A re-run's re-registration retires ITS OWN prior entry — the
// fresh-scratch contract survives the scoping.
// ------------------------------------------------------------------

#[test]
fn reregistration_retires_prior_session_entry_only() {
    let (mut system, raw, _dir) = world();
    create_and_register_temp_staged(&mut system, &raw);
    // A second run re-creates and re-registers (the F7 replace ruling makes
    // the CREATE side legal; here we exercise only the registration tail).
    let registered = system
        .register_run_created_objects_with(
            &[crate::pipeline::compiled_query::PlanCreatedObject {
                name: "staged".to_string(),
                is_view: false,
                connection_id: Some(2),
            }],
            &crate::system::RealCreatedObjectCatalog,
        )
        .expect("re-registration should not error");
    assert!(matches!(
        registered.as_slice(),
        [crate::external_effects::RegistrationOutcome::Registered]
    ));

    let (session, physical) = staged_entity_split(&system, "staged");
    assert_eq!(
        session, 1,
        "re-registration must retire the run's own prior session entry (fresh scratch)"
    );
    assert_eq!(
        physical, 1,
        "re-registration must never take the mount-introspected physical entity"
    );
}

// ------------------------------------------------------------------
// (d) The physical entity's registration SURVIVES — the shadow is a
// preference, not a delete (materialize-pipe §6).
// ------------------------------------------------------------------

#[test]
fn physical_registration_survives_temp_registration() {
    let (mut system, raw, _dir) = world();
    let before = staged_entity_split(&system, "staged");
    assert_eq!(
        before,
        (0, 1),
        "fixture: exactly the mount-introspected entity"
    );

    create_and_register_temp_staged(&mut system, &raw);

    let (session, physical) = staged_entity_split(&system, "staged");
    assert_eq!(session, 1, "the temp registered on the session cartridge");
    assert_eq!(
        physical, 1,
        "registering a same-name temp must not retire the physical entity \
         (the retirement is scoped to the session cartridge)"
    );
}
