// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The bootstrap catalog's structural backstop.
//!
//! Bootstrap catalog tables are engine-owned structure, not ordinary DDL
//! targets. The language's resolved-ownership check refuses first; this
//! SQLite authorizer is the runtime backstop behind it: a compiler defect or
//! an unanticipated SQL road must still be unable to drop or alter a system
//! table (`SEMANTICS/namespace-law.md`, ENGINE-OWNED STRUCTURE).
//!
//! Protection is CLOSED BY CONSTRUCTION: the protected inventory is a
//! snapshot of `sqlite_master` taken when installation seals the connection
//! — whatever the canonical bootstrap schema authority created IS protected,
//! so a newly declared system table needs no second registration. Only the
//! narrowly scoped installation/migration capability crosses the guard: a
//! connection is structurally open exactly while it is being installed
//! (construction and reinitialization build on an unsealed connection and
//! seal it before any query executes), and [`MigrationWindow`] reopens it
//! for an in-place migration and re-seals over the migrated inventory.
//! Ordinary query execution holds neither and cannot toggle the guard.
//!
//! This is a contractual-runtime defense, not protection against a process
//! that opens the backing database outside DelightQL. Row DML against
//! catalog tables is untouched — the catalog's own writes are ordinary
//! INSERT/UPDATE/DELETE. For objects the guard did not inventory (a query's
//! scratch on the bootstrap connection), creation, drop, and row DML stay
//! under ordinary policy; sealed `ALTER TABLE` alone is denied even for
//! them, because SQLite's authorizer exposes no rename destination to judge.

use rusqlite::config::DbConfig;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::Connection;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use delightql_types::error::DelightQLError;

#[derive(Debug, Default)]
struct GuardState {
    /// Names of every object the canonical installation created, from the
    /// `sqlite_master` snapshot taken at seal time — held under SQLite's
    /// own identifier law ([`canonical`]), so a case-variant spelling is
    /// the same name here exactly as it is to the engine.
    protected: HashSet<String>,
    /// Open installation/migration windows. While any is open the guard
    /// stands aside; sealing snapshots and closes them all.
    open_windows: u32,
    /// Open definition-catalog write windows. Row DML against the
    /// definition-family tables is denied unless one is open: the
    /// publication capability and the sanctioned lifecycle writers open
    /// one for the duration of their act, and compilation never does —
    /// which is what makes "compilation cannot write the catalog" a store
    /// fact rather than a discipline.
    catalog_windows: u32,
}

/// The definition-family store: every table whose rows publish, link, or
/// activate a definition family. Writes to these are denied outside a
/// catalog window; the namespace row itself stays under its own writers'
/// policies.
const DEFINITION_TABLES: &[&str] = &[
    "entity",
    "entity_clause",
    "entity_attribute",
    "referenced_entity",
    "entity_resolution",
    "ho_param",
    "ho_param_column",
    "join_edge",
    "functional_dependency",
    "interior_entity",
    "interior_entity_attribute",
    "activated_entity",
];

/// SQLite compares object names ASCII-case-insensitively; the guard must
/// compare under the same law or `CREATE TEMP TABLE ENTITY` shadows a
/// protected `entity` through a spelling the engine treats as identical.
fn canonical(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// The installed guard: a handle to the authorizer's shared state. Dropping
/// the handle does not uninstall the authorizer — the connection keeps its
/// protection for as long as it lives.
#[derive(Debug, Clone)]
pub(crate) struct BootstrapGuard {
    state: Arc<Mutex<GuardState>>,
}

impl BootstrapGuard {
    /// Install the authorizer on a freshly installed bootstrap connection
    /// and SEAL it: snapshot `sqlite_master` as the protected inventory.
    /// Called at the end of installation, after the canonical schema
    /// authority and every system-table registration has run.
    pub(crate) fn seal(conn: &Connection) -> Result<BootstrapGuard> {
        let guard = BootstrapGuard {
            state: Arc::new(Mutex::new(GuardState::default())),
        };
        guard.snapshot(conn)?;
        let state = Arc::clone(&guard.state);
        conn.authorizer(Some(move |context: AuthContext<'_>| {
            let Ok(state) = state.lock() else {
                // A poisoned guard fails CLOSED: structural actions are
                // denied rather than silently admitted.
                return match classify(&context.action) {
                    Some(_) => Authorization::Deny,
                    None => Authorization::Allow,
                };
            };
            if state.open_windows > 0 {
                return Authorization::Allow;
            }
            if state.catalog_windows == 0 && fenced_catalog_write(&context.action) {
                return Authorization::Deny;
            }
            match classify(&context.action) {
                Some(Structural::Target(name)) if state.protected.contains(&canonical(name)) => {
                    Authorization::Deny
                }
                Some(Structural::Sealed) => Authorization::Deny,
                _ => Authorization::Allow,
            }
        }));
        // Defense in depth beneath the authorizer: defensive mode makes the
        // engine itself refuse direct writes to sqlite_master and the other
        // schema-corrupting roads, so a bypass of the callback still meets
        // the engine's own refusal. Arming it is part of the seal contract:
        // a connection whose backstop cannot be armed is not sealed.
        let armed = conn
            .set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)
            .map_err(|e| {
                DelightQLError::database_error("bootstrap guard defensive mode", e.to_string())
            })?;
        if !armed {
            return Err(DelightQLError::database_error(
                "bootstrap guard defensive mode",
                "the engine reported defensive mode disarmed after arming".to_string(),
            ));
        }
        Ok(guard)
    }

    /// Open the scoped migration capability: the guard stands aside until
    /// the returned window is closed, and closing RE-SEALS — the migrated
    /// inventory becomes the protected set. An abandoned (dropped) window
    /// closes without re-sealing, leaving the previous inventory protected.
    #[allow(dead_code)]
    pub(crate) fn migration_window(&self) -> MigrationWindow {
        if let Ok(mut state) = self.state.lock() {
            state.open_windows += 1;
        }
        MigrationWindow {
            state: Arc::clone(&self.state),
            closed: false,
        }
    }

    /// Open the definition-catalog write capability: row DML against the
    /// definition-family tables (and the namespace active pointer) is
    /// admitted exactly while the returned window lives. The publication
    /// capability and the sanctioned lifecycle writers hold one for the
    /// duration of their act; compilation never opens one.
    pub(crate) fn catalog_window(&self) -> CatalogWindow {
        if let Ok(mut state) = self.state.lock() {
            state.catalog_windows += 1;
        }
        CatalogWindow {
            state: Arc::clone(&self.state),
        }
    }

    fn snapshot(&self, conn: &Connection) -> Result<()> {
        let mut protected = HashSet::new();
        let mut statement = conn
            .prepare("SELECT name FROM sqlite_master")
            .map_err(|e| {
                DelightQLError::database_error("bootstrap guard inventory", e.to_string())
            })?;
        let names = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| {
                DelightQLError::database_error("bootstrap guard inventory", e.to_string())
            })?;
        for name in names {
            protected.insert(canonical(&name.map_err(|e| {
                DelightQLError::database_error("bootstrap guard inventory", e.to_string())
            })?));
        }
        let mut state = self.state.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "bootstrap guard state",
                format!("guard state was poisoned: {e}"),
            )
        })?;
        state.protected = protected;
        state.open_windows = 0;
        state.catalog_windows = 0;
        Ok(())
    }
}

/// An open definition-catalog write window. Dropping it closes the
/// capability; every exit path (errors included) re-fences the store.
pub(crate) struct CatalogWindow {
    state: Arc<Mutex<GuardState>>,
}

impl Drop for CatalogWindow {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.catalog_windows = state.catalog_windows.saturating_sub(1);
        }
    }
}

/// An open installation/migration window. Close it with the connection to
/// re-seal over the migrated inventory.
#[allow(dead_code)]
pub(crate) struct MigrationWindow {
    state: Arc<Mutex<GuardState>>,
    closed: bool,
}

impl MigrationWindow {
    /// Re-seal: snapshot the migrated inventory and close the window.
    #[allow(dead_code)]
    pub(crate) fn close(mut self, conn: &Connection) -> Result<()> {
        let guard = BootstrapGuard {
            state: Arc::clone(&self.state),
        };
        guard.snapshot(conn)?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for MigrationWindow {
    fn drop(&mut self) {
        if !self.closed {
            if let Ok(mut state) = self.state.lock() {
                state.open_windows = state.open_windows.saturating_sub(1);
            }
        }
    }
}

/// What a structural action puts before the guard. `None` for every
/// non-structural action (reads, row DML, transactions, ordinary pragmas),
/// which the guard never touches.
enum Structural<'c> {
    /// Judged by the target's name against the protected inventory.
    Target(&'c str),
    /// Denied outright while sealed: either the authorizer does not expose
    /// enough to judge the result, or the action would defeat the seal
    /// itself. The migration window is the one crossing.
    Sealed,
}

/// Whether one action is a row write the catalog fence denies outside a
/// window: INSERT/UPDATE/DELETE on a definition-family table.
fn fenced_catalog_write(action: &AuthAction<'_>) -> bool {
    use AuthAction::*;
    match action {
        Insert { table_name } | Delete { table_name } | Update { table_name, .. } => {
            DEFINITION_TABLES.contains(&canonical(table_name).as_str())
        }
        _ => false,
    }
}

/// Classify one authorizer action.
///
/// Creations are judged by the NEW name (a protected name may not be
/// shadowed or replaced — temp schema and virtual-table modules included);
/// drops by the target; index/trigger creation and drops ALSO by the table
/// they hang on, because SQLite drops a table's triggers and indexes with
/// the table and a trigger on a catalog table is a mutation vector of its
/// own.
///
/// Two actions are `Sealed` rather than name-judged:
/// - `ALTER TABLE`: SQLite's authorizer event reports only the OLD name, so
///   `ALTER TABLE scratch RENAME TO entity` looks ordinary at the only
///   judgment the guard is offered while its result shadows the catalog.
///   With no destination to judge, the closed answer is to deny sealed
///   ALTER wholesale; catalog migration crosses through the window.
/// - `PRAGMA writable_schema = …`: assigning it turns catalog destruction
///   into row DML on `sqlite_master`. Reading the pragma stays ordinary.
fn classify<'c>(action: &'c AuthAction<'c>) -> Option<Structural<'c>> {
    use AuthAction::*;
    match action {
        DropTable { table_name } | DropTempTable { table_name } => {
            Some(Structural::Target(table_name))
        }
        DropView { view_name } | DropTempView { view_name } => Some(Structural::Target(view_name)),
        AlterTable { .. } => Some(Structural::Sealed),
        Pragma {
            pragma_name,
            pragma_value: Some(_),
        } if pragma_name.eq_ignore_ascii_case("writable_schema") => Some(Structural::Sealed),
        DropIndex { table_name, .. }
        | DropTempIndex { table_name, .. }
        | DropTrigger { table_name, .. }
        | DropTempTrigger { table_name, .. }
        | CreateIndex { table_name, .. }
        | CreateTempIndex { table_name, .. }
        | CreateTrigger { table_name, .. }
        | CreateTempTrigger { table_name, .. } => Some(Structural::Target(table_name)),
        CreateTable { table_name }
        | CreateTempTable { table_name }
        | CreateVtable { table_name, .. }
        | DropVtable { table_name, .. } => Some(Structural::Target(table_name)),
        CreateView { view_name } | CreateTempView { view_name } => {
            Some(Structural::Target(view_name))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealed() -> (Connection, BootstrapGuard) {
        let conn = Connection::open_in_memory().expect("in-memory");
        conn.execute_batch(
            "CREATE TABLE entity (id INTEGER); \
             CREATE VIEW catalog_view AS SELECT id FROM entity; \
             CREATE INDEX entity_id ON entity(id);",
        )
        .expect("install");
        let guard = BootstrapGuard::seal(&conn).expect("seal");
        (conn, guard)
    }

    /// EVERY object the installation created is covered — the inventory is
    /// the snapshot, not a hand list.
    #[test]
    fn every_installed_object_is_protected() {
        let (conn, _guard) = sealed();
        for (sql, teaching) in [
            ("DROP TABLE entity", "drop table"),
            ("DROP VIEW catalog_view", "drop view"),
            ("DROP INDEX entity_id", "drop index"),
            ("ALTER TABLE entity ADD COLUMN extra TEXT", "alter table"),
            ("ALTER TABLE entity RENAME TO entity2", "rename table"),
            (
                "CREATE TRIGGER t AFTER INSERT ON entity BEGIN SELECT 1; END",
                "trigger on",
            ),
            ("CREATE INDEX second ON entity(id)", "index on"),
            ("CREATE TEMP TABLE entity (x INTEGER)", "temp shadow"),
        ] {
            assert!(
                conn.execute_batch(sql).is_err(),
                "{teaching} must be denied: {sql}"
            );
        }
    }

    /// DEFINITION-TABLE row DML is the catalog fence's: denied outside a
    /// catalog window, admitted inside one, and closed again when the
    /// window drops — compilation, which never opens one, cannot write the
    /// definition catalog.
    #[test]
    fn definition_writes_need_the_catalog_window() {
        let (conn, guard) = sealed();
        assert!(
            conn.execute("INSERT INTO entity (id) VALUES (1)", [])
                .is_err(),
            "a definition-table write outside the window must be denied"
        );
        {
            let _window = guard.catalog_window();
            conn.execute("INSERT INTO entity (id) VALUES (1)", [])
                .expect("the publication window admits the write");
            conn.execute("DELETE FROM entity", [])
                .expect("and the delete");
        }
        assert!(
            conn.execute("INSERT INTO entity (id) VALUES (2)", [])
                .is_err(),
            "dropping the window re-fences the store"
        );
        // Non-definition tables stay under ordinary row policy.
        conn.execute_batch("CREATE TABLE ordinary (x INTEGER)")
            .expect("scratch stays ordinary");
        conn.execute("INSERT INTO ordinary (x) VALUES (1)", [])
            .expect("ordinary row DML is untouched");
    }

    /// Objects the installation did not create stay under ordinary policy:
    /// a query's scratch on the same connection is creatable and droppable.
    #[test]
    fn uninventoried_objects_stay_ordinary() {
        let (conn, _guard) = sealed();
        conn.execute_batch("CREATE TABLE scratch (x INTEGER)")
            .expect("a new name is ordinary");
        conn.execute_batch("DROP TABLE scratch")
            .expect("and stays ordinary");
    }

    /// Only the scoped capability crosses: a migration window admits the
    /// structural change, and closing re-seals over the migrated inventory.
    #[test]
    fn the_migration_window_is_the_one_crossing() {
        let (conn, guard) = sealed();
        assert!(conn
            .execute_batch("ALTER TABLE entity ADD COLUMN extra TEXT")
            .is_err());

        let window = guard.migration_window();
        conn.execute_batch(
            "ALTER TABLE entity ADD COLUMN extra TEXT; \
             CREATE TABLE new_catalog (id INTEGER);",
        )
        .expect("the capability admits the migration");
        window.close(&conn).expect("re-seal");

        // The migrated inventory is protected — the NEW table included,
        // with no second registration.
        assert!(conn.execute_batch("DROP TABLE new_catalog").is_err());
        assert!(conn.execute_batch("DROP TABLE entity").is_err());
    }

    /// An abandoned window closes without re-sealing: the previous
    /// inventory stays protected.
    #[test]
    fn an_abandoned_window_keeps_the_previous_seal() {
        let (conn, guard) = sealed();
        drop(guard.migration_window());
        assert!(conn.execute_batch("DROP TABLE entity").is_err());
    }
}

/// Bypass-road discrimination pins: the name/action roads that could reach
/// protected structure without spelling a protected name at the judged
/// position — case-variant spellings, shadowing creates, renames, and the
/// `writable_schema` road — each denied AT the road it used.
#[cfg(test)]
mod seal_bypass_pins {
    use super::*;

    fn sealed() -> (Connection, BootstrapGuard) {
        let conn = Connection::open_in_memory().expect("in-memory");
        conn.execute_batch("CREATE TABLE entity (id INTEGER);")
            .expect("install");
        let guard = BootstrapGuard::seal(&conn).expect("seal");
        (conn, guard)
    }

    fn denied(conn: &Connection, sql: &str) {
        let error = conn
            .execute_batch(sql)
            .expect_err(&format!("'{sql}' must be denied"));
        assert!(
            error.to_string().contains("not authorized"),
            "'{sql}' must be denied by the AUTHORIZER, got: {error}"
        );
    }

    /// Sealing arms the engine's defensive mode as its own observable
    /// setting — the second defense exists independently of the authorizer,
    /// not merely as an inference from a refused mutation.
    #[test]
    fn sealing_arms_defensive_mode() {
        let (conn, _guard) = sealed();
        let armed = conn
            .db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)
            .expect("read defensive mode");
        assert!(armed, "seal() must leave defensive mode armed");
    }

    /// SQLite's identifier law: a case-variant spelling is the same name to
    /// the engine, so it is the same name to the seal.
    #[test]
    fn a_case_variant_spelling_is_the_same_protected_name() {
        let (conn, _guard) = sealed();
        denied(&conn, "CREATE TEMP TABLE ENTITY (x INTEGER)");
        denied(&conn, "DROP TABLE Entity");
        denied(&conn, "CREATE TABLE \"Entity\" (x INTEGER)");
    }

    /// A virtual table is a creation like any other: it may not shadow or
    /// replace a protected name, temp schema included.
    #[test]
    fn a_virtual_table_cannot_shadow_a_protected_name() {
        let (conn, _guard) = sealed();
        denied(&conn, "CREATE VIRTUAL TABLE temp.entity USING fts5(x)");
        denied(&conn, "CREATE VIRTUAL TABLE temp.ENTITY USING fts5(x)");
    }

    /// The authorizer's ALTER event reports only the OLD name, so a rename
    /// into a protected name looks ordinary at the only judgment offered.
    /// Sealed ALTER is therefore denied wholesale; the migration window is
    /// the crossing (pinned beside the window tests above).
    #[test]
    fn a_rename_cannot_arrive_at_a_protected_name() {
        let (conn, guard) = sealed();
        {
            let window = guard.migration_window();
            conn.execute_batch("CREATE TABLE scratch (x INTEGER)")
                .expect("scratch");
            window.close(&conn).expect("re-seal");
        }
        // `scratch` is now inventoried; but even an UNINVENTORIED table's
        // rename is denied while sealed — the destination is unjudgeable.
        // The TEMP schema is the live bypass road: without the seal the
        // rename succeeds there (no main-schema collision) and the result
        // shadows protected `entity`.
        conn.execute_batch("CREATE TEMP TABLE scratch2 (x INTEGER)")
            .expect("ordinary create");
        denied(&conn, "ALTER TABLE scratch2 RENAME TO entity");
        denied(&conn, "ALTER TABLE scratch2 ADD COLUMN extra TEXT");
    }

    /// `writable_schema` turns catalog destruction into row DML on
    /// `sqlite_master`. The assignment is denied while sealed; the direct
    /// mutation meets the engine's defensive mode beneath it; and the
    /// catalog row survives a REOPEN of the backing file — the pin does not
    /// rely on the schema cache of the connection that attempted it.
    #[test]
    fn writable_schema_cannot_reach_the_catalog() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("guard_pin.sqlite");
        {
            let conn = Connection::open(&path).expect("file-backed");
            conn.execute_batch("CREATE TABLE entity (id INTEGER);")
                .expect("install");
            let _guard = BootstrapGuard::seal(&conn).expect("seal");
            denied(&conn, "PRAGMA writable_schema = ON");
            denied(&conn, "PRAGMA writable_schema = 1");
            // Reading the pragma stays ordinary.
            conn.query_row("PRAGMA writable_schema", [], |row| row.get::<_, bool>(0))
                .expect("reading the pragma is not structural");
            // The direct mutation fails without the pragma (the engine's own
            // refusal, defensive mode beneath the authorizer).
            assert!(conn
                .execute("DELETE FROM sqlite_master WHERE name = 'entity'", [])
                .is_err());
        }
        // The catalog row is judged after REOPEN, not from a schema cache.
        let reopened = Connection::open(&path).expect("reopen");
        let present: bool = reopened
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'entity')",
                [],
                |row| row.get(0),
            )
            .expect("read catalog");
        assert!(present, "the protected catalog row survived");
    }
}
