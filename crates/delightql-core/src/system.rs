// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DelightQL System Management
//!
//! This module provides the `DelightQLSystem` struct which encapsulates
//! the user database connection and the internal _bootstrap metadata store.

use crate::diagnostic::Runtime;
use crate::error::{DelightQLError, Result};
use crate::external_effects::SessionHealth;
use delightql_types::{schema::DatabaseSchema, ConnectionFactory, DatabaseConnection};
use log::debug;
use rusqlite::{Connection, OptionalExtension};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// The catalog rows an entity owns: their retirement and their copy.
pub(crate) mod entity_rows;
/// The pristine world: constructed once, frozen, instantiated per reset.
/// A child so it may lay fields into `DelightQLSystem`.
mod world;
pub(crate) use crate::system_vocabulary::{
    builtin_registry, validate_producer_target, validate_user_namespace_target, ImprintMode, embedded_module,
    LiminalReceipt, Producer,
    LiminalRow, LoadPhase, PhysicalRead, StdlibLoad, BOOTSTRAP_CONNECTION_ID, PRIMARY_CONNECTION_ID,
};
pub(crate) use world::ReadySystem;

mod compiler_host;
mod created;
mod grounding;
mod ledgers;
mod liminal;
mod load;
mod mount;
mod population;
mod registration;
mod removal;
mod routing;
mod session_act;
pub(crate) use session_act::run_namespace_of;

pub(crate) use created::{CreatedObjectRegistration, Placement, RealCreatedObjectCatalog};
pub(crate) use liminal::LiminalProgramKind;
use liminal::ProgramContext;
pub(crate) use load::{PreparedLoad, PublishedLoad};
use mount::ByteBinding;

/// DelightQL system state with user database and internal metadata store
///
/// This struct manages:
/// 1. User database connection (can be any backend: SQLite, Postgres, DuckDB)
/// 2. Internal _bootstrap SQLite database (always SQLite, engine implementation detail)
/// 3. System schema (sys) attached to user database
/// 4. Connection routing map for query execution
///
/// The _bootstrap database is NOT attached to the user database - it's a completely
/// separate SQLite connection used internally by the engine for metadata storage.
///
/// THE HOST IS UNSIZED. Its last field is a zero-length slice, so a
/// `DelightQLSystem` is never a value: safe code cannot move one, replace
/// one, or swap two — `std::mem::swap`, `replace` and `take` all require
/// `Sized`. A `&mut DelightQLSystem` can only OPERATE on the host where it
/// stands, under the carrier that owns it beside its image
/// (`world::ReadySystem`, and the construction carriers before it), and
/// nothing can pair that host with another image. The sized twin exists
/// only for the one instant of construction in `system::world`, where it
/// is boxed and unsized in the same expression.
pub(crate) struct DelightQLSystem<Standing: ?Sized = InPlace> {
    /// Optional services supplied by this concrete host instance.
    capabilities: crate::host::HostCapabilities,

    /// User database connection (target backend)
    pub connection: Arc<Mutex<dyn DatabaseConnection>>,

    /// Internal _bootstrap metadata store (always SQLite)
    /// This is an engine implementation detail, not part of the user's database
    bootstrap_connection: Arc<Mutex<Connection>>,

    /// Database schema provider (injected by CLI)
    /// Stores trait object to avoid coupling to concrete backend implementations
    schema: Option<Box<dyn DatabaseSchema>>,

    /// Connection routing map: connection_id → DatabaseConnection
    /// This maps logical connection IDs to physical database connections for query execution.
    /// - connection_id=1 → Bootstrap connection (internal metadata)
    /// - connection_id=2 → User connection (target database)
    /// Additional connections can be added for attached databases, federation, etc.
    connection_map: HashMap<i64, Arc<Mutex<dyn DatabaseConnection>>>,

    /// Database introspector for discovering schema metadata
    introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,

    /// Bin cartridge registry for built-in entities (pseudo-predicates, functions, etc.)
    /// Wrapped in Arc so it can be shared without cloning
    bin_registry: Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>,

    /// Host-bound static database images for `delightql-bytes://` mounts.
    /// Names are bound once by the host via `bind_static_bytes` and are
    /// immutable for the life of the handle; the locator resolves ONLY
    /// names in this table (no ambient authority).
    byte_bindings: HashMap<String, ByteBinding>,

    /// Factory for creating connections from URIs (injected by CLI).
    /// Enables import! to handle delightql-siso:// and other URI schemes.
    connection_factory: Option<Box<dyn ConnectionFactory>>,

    /// Schema map: connection_id → DatabaseSchema for imported connections.
    /// The primary connection schema is in `self.schema`; this holds schemas
    /// for connections created via import!/ConnectionFactory.
    schema_map: HashMap<i64, Box<dyn DatabaseSchema>>,

    /// The introspector of each connection created through the factory, kept
    /// beside its schema: a storage question about a relation such a
    /// connection serves is answered by that connection's own backend.
    introspector_map: HashMap<i64, Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>>,

    /// Cartridge ID for catalog wrapper views in sys::meta.
    /// Lazily initialized on first access to catalog features.
    catalog_cartridge_id: Cell<Option<i32>>,

    /// Database type string ("sqlite", "duckdb", "postgres"); selects the
    /// primary connection's dialect.
    db_type: String,

    /// Monotonic count of directive executions (`EffectExecutable::execute`
    /// calls) in this session.
    effects_executed: Cell<u64>,

    /// The one OUTERMOST consultation/reconsultation context. Its catalog
    /// mutations live under a SQLite savepoint; effects outside that catalog
    /// are recorded in the typed journal. Nested loads inherit this context
    /// rather than opening competing transaction/rollback mechanisms.
    active_liminal_program: RefCell<Option<ProgramContext>>,

    /// A failed external-effect recovery quarantines this session until a
    /// successful reset. Healthy is the zero-cost steady state.
    session_health: SessionHealth,

    /// The sealed structural guard on the bootstrap connection. Held so the
    /// narrowly scoped migration capability exists at all; nothing on any
    /// query road reaches it.
    bootstrap_guard: crate::bootstrap::guard::BootstrapGuard,

    /// The unsizing tail: `[()]` in every type position the crate names.
    /// Last, so the struct's other fields are laid out before it and the
    /// sized twin coerces to it. Never read — it is a fact of the type,
    /// not a datum.
    #[allow(dead_code)]
    standing: Standing,
}

/// The standing of a published host: the zero-length slice type that makes
/// `DelightQLSystem` unsized, and so unmovable, everywhere it is named.
pub(crate) type InPlace = [()];

/// A nestable catalog transaction. SQLite SAVEPOINT works both at the top
/// level and inside the outer liminal-program savepoint, unlike `BEGIN`.
/// Uncommitted instances roll back on drop, so early `?`/`return Err` paths
/// cannot forget cleanup.
struct CatalogSavepoint<'a> {
    conn: &'a Connection,
    name: &'static str,
    active: bool,
}

impl<'a> CatalogSavepoint<'a> {
    fn begin(conn: &'a Connection, name: &'static str, context: &str) -> Result<Self> {
        conn.execute_batch(&format!("SAVEPOINT {name}"))
            .map_err(|e| Runtime::catalog(context, e.to_string()))?;
        Ok(Self {
            conn,
            name,
            active: true,
        })
    }

    fn commit(mut self, context: &str) -> Result<()> {
        self.conn
            .execute_batch(&format!("RELEASE SAVEPOINT {}", self.name))
            .map_err(|e| Runtime::catalog(context, e.to_string()))?;
        self.active = false;
        Ok(())
    }
}

impl Drop for CatalogSavepoint<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .conn
                .execute_batch(&format!("ROLLBACK TO SAVEPOINT {}", self.name));
            let _ = self
                .conn
                .execute_batch(&format!("RELEASE SAVEPOINT {}", self.name));
        }
    }
}

/// RAII savepoint bracket over the bootstrap connection: constructed
/// before a multi-statement registration/cascade, it
/// ROLLS BACK on drop unless `commit()` ran — so every scattered `?`
/// early-return inside the bracket restores the catalog whole (the mount
/// link included). SAVEPOINT rather than BEGIN so nesting inside any
/// caller-held transaction stays legal.
struct BootstrapTxn<'a> {
    conn: &'a Connection,
    name: &'static str,
    armed: bool,
}

impl<'a> BootstrapTxn<'a> {
    fn begin(conn: &'a Connection, name: &'static str) -> Result<Self> {
        conn.execute_batch(&format!("SAVEPOINT {}", name))
            .map_err(|e| Runtime::catalog("Failed to open bootstrap savepoint", e.to_string()))?;
        Ok(Self {
            conn,
            name,
            armed: true,
        })
    }

    fn commit(mut self) -> Result<()> {
        // RELEASE first, disarm only on success: if the
        // release fails (e.g. a deferred constraint), the guard stays armed
        // and Drop rolls the still-open savepoint back — disarming early
        // would leave the partial transaction dangling.
        self.conn
            .execute_batch(&format!("RELEASE {}", self.name))
            .map_err(|e| {
                Runtime::catalog("Failed to release bootstrap savepoint", e.to_string())
            })?;
        self.armed = false;
        Ok(())
    }
}

impl Drop for BootstrapTxn<'_> {
    fn drop(&mut self) {
        if self.armed {
            if let Err(e) = self.conn.execute_batch(&format!(
                "ROLLBACK TO {name}; RELEASE {name}",
                name = self.name
            )) {
                debug!("bootstrap savepoint rollback failed: {}", e);
            }
        }
    }
}

/// Check that a namespace fq_name is not already registered in bootstrap.
/// Returns Ok(()) if available, Err if already taken.
/// ONE NAME POOL PER NODE: a name in a namespace is an entity or a child
/// namespace, never both. `fq_name` is a namespace about to be created; its
/// parent may not hold an entity of its last segment's name.
fn refuse_child_named_like_entity(conn: &rusqlite::Connection, fq_name: &str) -> Result<()> {
    let Some((parent, leaf)) = fq_name.rsplit_once("::") else {
        return Ok(());
    };
    let collides: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM entity e
             JOIN activated_entity ae ON ae.entity_id = e.id
             JOIN namespace n ON n.id = ae.namespace_id
             WHERE n.fq_name = ?1 AND e.name = ?2)",
            rusqlite::params![parent, leaf],
            |row| row.get(0),
        )
        .map_err(|e| Runtime::catalog("Failed to check the namespace's name pool", e.to_string()))?;
    if collides {
        return Err(DelightQLError::from(crate::diagnostic::Constraint::General {
            message: format!(
                "cannot create namespace '{fq_name}': '{parent}' holds an entity named '{leaf}', and a name in a \
                 namespace is an entity or a child namespace, never both"
            ),
        }));
    }
    Ok(())
}

/// ONE NAME POOL PER NODE, from the entity's side: `namespace` may not
/// hold an entity named like one of its child namespaces.
fn refuse_entity_named_like_child(conn: &rusqlite::Connection, namespace: &str, entity: &str) -> Result<()> {
    let child = format!("{namespace}::{entity}");
    let collides: bool = conn
        .query_row("SELECT EXISTS(SELECT 1 FROM namespace WHERE fq_name = ?1)", [&child], |row| row.get(0))
        .map_err(|e| Runtime::catalog("Failed to check the namespace's name pool", e.to_string()))?;
    if collides {
        return Err(DelightQLError::from(crate::diagnostic::Constraint::General {
            message: format!(
                "cannot define '{entity}' in '{namespace}': '{child}' is a child namespace, and a name in a \
                 namespace is an entity or a child namespace, never both"
            ),
        }));
    }
    Ok(())
}

/// Creating a path creates every missing prefix as a structural node: no
/// kind, no entities, no backing (ONE NAME POOL PER NODE). Each new node is
/// judged by the name pool of its parent.
fn create_structural_prefixes(conn: &rusqlite::Connection, fq_name: &str) -> Result<()> {
    let segments: Vec<&str> = fq_name.split("::").collect();
    for end in 1..segments.len() {
        let prefix = segments[..end].join("::");
        let exists: bool = conn
            .query_row("SELECT EXISTS(SELECT 1 FROM namespace WHERE fq_name = ?1)", [&prefix], |row| row.get(0))
            .map_err(|e| Runtime::catalog("Failed to check namespace existence", e.to_string()))?;
        if exists {
            continue;
        }
        refuse_child_named_like_entity(conn, &prefix)?;
        ensure_namespace_available(conn, &prefix)?;
        conn.execute(
            "INSERT INTO namespace (name, pid, fq_name, writable) VALUES (?1, NULL, ?2, 0)",
            rusqlite::params![segments[end - 1], prefix],
        )
        .map_err(|e| Runtime::catalog("Failed to create a structural namespace", e.to_string()))?;
    }
    refuse_child_named_like_entity(conn, fq_name)
}

fn ensure_namespace_available(conn: &rusqlite::Connection, fq_name: &str) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM namespace WHERE fq_name = ?1)",
            [fq_name],
            |row| row.get(0),
        )
        .map_err(|e| Runtime::catalog("Failed to check namespace existence", e.to_string()))?;

    if exists {
        return Err(DelightQLError::from(Runtime::General {
            message: format!(
                "Namespace '{}' already exists. Cannot register the same namespace twice.",
                fq_name
            ),
            details: "Duplicate namespace".to_string(),
        }));
    }

    // The inverse of register_namespace_alias's guard — the exclusivity
    // invariant (an alias shorthand and an exact namespace name never
    // coexist) must hold from BOTH creation orders. A namespace shadowing
    // an alias makes every lookup for the name two-headed: the entity
    // resolution query ORs the exact-fq and alias branches with no
    // ordering, so which entity answers is scan order — silent
    // wrong-entity resolution, not an error.
    let alias_target: Option<String> = conn
        .query_row(
            "SELECT n.fq_name FROM namespace_alias a
             JOIN namespace n ON n.id = a.target_namespace_id
             WHERE a.alias = ?1",
            [fq_name],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| Runtime::catalog("Failed to check alias collision", e.to_string()))?;

    if let Some(target) = alias_target {
        return Err(Runtime::catalog(
            format!(
                "'{}' is already an alias for namespace '{}'. A namespace may not \
                take an alias's name — every reference to '{}' would resolve to \
                whichever entity the scan found first. Pick a different name, or \
                drop the alias first.",
                fq_name, target, fq_name
            ),
            "Alias collision",
        ));
    }
    Ok(())
}

impl DelightQLSystem {

    /// The introspector of the backend serving `connection_id`: a
    /// factory-created connection's own; the session's for the primary
    /// connection and the schemas attached to it. Any other connection has
    /// none, and its storage questions answer `None`. Asking the session's
    /// introspector about a factory connection's relation misreads it: the
    /// two are different engines, or different databases of one engine.
    fn serving_introspector(
        &self,
        connection_id: i64,
        schema: Option<&str>,
    ) -> Option<&dyn crate::bootstrap::introspect::DatabaseIntrospector> {
        if let Some(introspector) = self.introspector_map.get(&connection_id) {
            return Some(introspector.as_ref());
        }
        (connection_id == PRIMARY_CONNECTION_ID || schema.is_some()).then_some(self.introspector.as_ref())
    }

    /// Whether a relation's storage guarantees its declared column types, as
    /// the backend serving it answers ([`Self::serving_introspector`]). A
    /// connection without one answers `None`, and so does an error.
    pub(crate) fn storage_guarantees_declared_types(
        &self,
        connection_id: i64,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Option<bool> {
        self.serving_introspector(connection_id, schema)?
            .storage_guarantees_declared_types(schema, relation_name)
            .ok()
            .flatten()
    }

    /// How a stored table's rows are identified, as the backend serving it
    /// answers ([`Self::serving_introspector`]). A connection without one
    /// answers `None`. An introspection failure is the error it is.
    pub(crate) fn stored_row_identity(
        &self,
        connection_id: i64,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<delightql_types::introspect::StoredRowIdentity>> {
        match self.serving_introspector(connection_id, schema) {
            Some(introspector) => introspector.stored_row_identity(schema, relation_name),
            None => Ok(None),
        }
    }

    /// The columns of a stored table its storage computes, as the backend
    /// serving it answers: the runtime database's own introspection for the
    /// tables it holds (a grounded library's tables), otherwise
    /// [`Self::serving_introspector`]. A connection without one answers
    /// `None`.
    pub(crate) fn stored_computed_columns(
        &self,
        connection_id: i64,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<Vec<delightql_types::SqlIdentifier>>> {
        if connection_id == BOOTSTRAP_CONNECTION_ID {
            let conn = self.bootstrap_connection.lock().map_err(|e| {
                Runtime::poisoned(
                    "Failed to acquire the runtime database's lock to read its generated columns",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            let computed = crate::bootstrap::introspect::generated_columns(&conn, schema, relation_name).map_err(|e| {
                Runtime::catalog(
                    &format!("Failed to read the generated columns of runtime relation '{relation_name}'"),
                    e.to_string(),
                )
            })?;
            return Ok(computed.map(|names| names.into_iter().map(delightql_types::SqlIdentifier::new).collect()));
        }
        match self.serving_introspector(connection_id, schema) {
            Some(introspector) => introspector.computed_columns(schema, relation_name),
            None => Ok(None),
        }
    }

    /// One relation the primary connection's engine serves under `schema`,
    /// as its introspector answers it: a system table, a table the engine
    /// made, or a table function, none of which the catalog records. `None`
    /// when the engine answers no relation of that name.
    pub(crate) fn introspect_passthrough_relation(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<delightql_types::introspect::DiscoveredRelation>> {
        self.introspector.introspect_relation(schema, relation_name)
    }

    /// Get a reference to the bootstrap connection (for session tables: assertions, danger, errors).
    #[cfg(test)]
    pub fn bootstrap_connection(&self) -> &Arc<Mutex<Connection>> {
        &self.bootstrap_connection
    }

    /// Get the internal _bootstrap metadata connection
    ///
    /// Returns a reference to the internal SQLite connection used for metadata storage.
    /// This connection is independent of the user's database and is always SQLite.
    ///
    /// Used by:
    /// - Resolver for namespace lookups (_bootstrap.namespace)
    /// - Import operations (.attach, .borrow, etc.)
    /// - Metadata queries (sys::* namespaces)
    pub fn get_bootstrap_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.bootstrap_connection)
    }

    /// The bootstrap connection locked for one question, borrowing the
    /// system: the definition-use authority's catalog read reaches the
    /// store through this and nothing else.
    pub(crate) fn lock_bootstrap(
        &self,
        context: &str,
    ) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.bootstrap_connection
            .lock()
            .map_err(|e| Runtime::poisoned(context, format!("Connection was poisoned: {e}")))
    }

    /// Resolve a file path the program wrote against the base directory in
    /// force: the session's, else the one the host stated at boot.
    pub(crate) fn resolve_path(&self, raw: &str) -> Result<std::path::PathBuf> {
        let conn = self.lock_bootstrap("Failed to acquire bootstrap lock to resolve a path")?;
        crate::settings::resolve_path(&conn, raw)
    }

    /// Set `key` for the running session, over the host's boot value.
    pub(crate) fn set_session_setting(&self, key: &str, value: Option<&str>) -> Result<()> {
        let conn =
            self.lock_bootstrap("Failed to acquire bootstrap lock to set a session setting")?;
        crate::settings::set_session(&conn, key, value)
    }

    /// Return every setting to the host's boot value.
    pub(crate) fn clear_session_settings(&self) -> Result<()> {
        let conn =
            self.lock_bootstrap("Failed to acquire bootstrap lock to clear session settings")?;
        crate::settings::clear_session(&conn)
    }

    /// Get the bin cartridge registry
    ///
    /// Returns a reference to the registry containing all registered bin cartridges
    /// and their entities.
    pub fn bin_registry(&self) -> Arc<crate::bin_cartridge::registry::BinCartridgeRegistry> {
        Arc::clone(&self.bin_registry)
    }

    /// TEST-HARNESS ONLY: the definition-catalog write capability, for
    /// tests that seed catalog state directly. Production writers reach
    /// the window through the PRIVATE `bootstrap_guard` field — possession
    /// of the guard handle is the capability, and no production accessor
    /// hands it out, so compiler code cannot open the fence.
    #[cfg(test)]
    pub(crate) fn catalog_window(&self) -> crate::bootstrap::guard::CatalogWindow {
        self.bootstrap_guard.catalog_window()
    }

    /// Record that one directive (an `EffectExecutable::execute` call)
    /// actually executed. See `effects_executed`.
    pub(crate) fn note_effect_executed(&self) {
        self.effects_executed.set(self.effects_executed.get() + 1);
    }

}

#[cfg(test)]
mod name_guard_tests {
    //! The system name guard.
    //! Each prong, the home relaxation, the main exemption, and
    //! case-insensitivity. The guard is a pure string function, so these are
    //! the authoritative behavior pins; the balls prove the wiring.
    use super::validate_user_namespace_target as guard;

    // Assert a target is refused with the given subcategory.
    fn assert_refused(fq: &str, expect_sub: &str) {
        match guard(fq) {
            Ok(()) => panic!("'{fq}' should be refused ({expect_sub})"),
            Err(e) => {
                let uri = e.error_uri();
                assert!(
                    uri.contains(expect_sub),
                    "'{fq}': expected subcategory '{expect_sub}', got uri '{uri}'"
                );
            }
        }
    }

    fn assert_ok(fq: &str) {
        assert!(guard(fq).is_ok(), "'{fq}' should be allowed");
    }

    #[test]
    fn prong_a_bare_system_names_refused() {
        assert_refused("sys", "namespace/name/reserved");
        assert_refused("std", "namespace/name/reserved");
        assert_refused("home", "namespace/name/reserved");
    }

    #[test]
    fn main_is_exempt_bare_and_subtree() {
        // main is the primary DATA namespace, not the name
        // guard's business — and the CLI binds every session with
        // mount!("<db>","main")(*).
        assert_ok("main");
        assert_ok("main::orders");
    }

    #[test]
    fn prong_b_sys_std_prefix_refused_case_insensitive() {
        assert_refused("sysinfo", "namespace/name/reserved");
        assert_refused("stdlib", "namespace/name/reserved");
        assert_refused("std2", "namespace/name/reserved");
        assert_refused("sys_tools", "namespace/name/reserved");
        assert_refused("Sys_tools", "namespace/name/reserved");
        assert_refused("STDx", "namespace/name/reserved");
        assert_refused("SYS_foo", "namespace/name/reserved");
        // prefix applies to the top-level segment even when a subtree follows
        assert_refused("sysinfo::x", "namespace/name/reserved");
    }

    #[test]
    fn exact_main_home_are_not_prefix_rules() {
        // main/home are exact-only — maintenance/homework are ordinary names.
        assert_ok("maintenance");
        assert_ok("homework");
    }

    #[test]
    fn prong_c_underscore_refused_everywhere() {
        assert_refused("_internal", "namespace/name/reserved");
        assert_refused("_N_blueprint", "namespace/name/reserved");
        assert_refused("home::_y", "namespace/name/reserved");
        assert_refused("lib::_x", "namespace/name/reserved");
        assert_refused("myns::sub::_deep", "namespace/name/reserved");
        // even under main (machinery names reserved despite main's exemption)
        assert_refused("main::_x", "namespace/name/reserved");
    }

    #[test]
    fn prong_d_system_subtree_refused() {
        assert_refused("sys::evil", "namespace/name/system_subtree");
        assert_refused("std::x", "namespace/name/system_subtree");
        assert_refused("SYS::x", "namespace/name/system_subtree");
    }

    #[test]
    fn home_relaxes_prefix_but_not_underscore() {
        // Under home the sys*/std* prefix relaxes...
        assert_ok("home::sysinfo");
        assert_ok("home::stdlib");
        assert_ok("home::sys");
        assert_ok("home::chutzpah");
        // ...but the `_` reservation stays strict (checked in prong_c above).
    }

    #[test]
    fn ordinary_user_names_allowed() {
        assert_ok("lib::math");
        assert_ok("mfg");
        assert_ok("models::sales::q3");
        assert_ok("data::production");
    }
}

#[cfg(test)]
mod mount_link_tests {
    //! The stored mount fact. Written
    //! by the spine, re-pointed by refresh, cleared by unmount; UNIQUE
    //! encodes the 1:1 claim; identity consumers read it (an EMPTY image is
    //! a full citizen); a failed refresh rolls back link and cartridge
    //! TOGETHER; the bootstrap catalog stays FK-consistent.

    use super::{DelightQLSystem, LiminalProgramKind, ReadySystem};
    use delightql_types::introspect::{
        DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity,
    };
    use delightql_types::namespace::NamespacePath;
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::{DatabaseConnection, Result};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(&self, _s: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    struct OneTableIntrospector;
    impl DatabaseIntrospector for OneTableIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }

        fn introspect_entities_in_schema(&self, _s: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![DiscoveredEntity {
                name: "mounted_t".into(),
                entity_type_id: 10,
                attributes: vec![DiscoveredAttribute {
                    name: "id".into(),
                    data_type: "INTEGER".to_string(),
                    position: 0,
                    is_nullable: false,
                }],
            }])
        }
    }

    /// Succeeds `ok_calls` times, then fails — induces a mid-refresh
    /// introspection failure AFTER the transaction has cleared contents.
    struct FlakyIntrospector {
        calls: AtomicUsize,
        ok_calls: usize,
    }
    impl DatabaseIntrospector for FlakyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(&self, _s: &str) -> Result<Vec<DiscoveredEntity>> {
            if self.calls.fetch_add(1, Ordering::SeqCst) < self.ok_calls {
                Ok(vec![])
            } else {
                Err(crate::diagnostic::Runtime::catalog(
                    "induced introspection failure",
                    "mount_link_tests",
                ))
            }
        }
    }

    fn system_with(introspector: Box<dyn DatabaseIntrospector>) -> ReadySystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(conn, introspector, "sqlite").expect("system should build")
    }

    /// A VALID SQLite file with zero tables — the empty-image case.
    fn empty_db(dir: &tempfile::TempDir, name: &str) -> String {
        let path = dir.path().join(name);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE t(x); DROP TABLE t;")
            .unwrap();
        path.to_str().unwrap().to_string()
    }

    fn link_of(system: &DelightQLSystem, ns: &str) -> Option<i64> {
        let conn = system.bootstrap_connection.lock().unwrap();
        conn.query_row(
            "SELECT m.cartridge_id
             FROM mount m JOIN namespace n ON n.id = m.namespace_id
             WHERE n.fq_name = ?1",
            [ns],
            |r| r.get(0),
        )
        .ok()
        .flatten()
    }

    fn cartridge_exists(system: &DelightQLSystem, id: i64) -> bool {
        let conn = system.bootstrap_connection.lock().unwrap();
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM cartridge WHERE id = ?1)",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn mount_of(system: &DelightQLSystem, ns: &str) -> Option<(i64, String, String)> {
        let conn = system.bootstrap_connection.lock().unwrap();
        conn.query_row(
            "SELECT m.cartridge_id, m.class, m.qualification
             FROM mount m JOIN namespace n ON n.id = m.namespace_id
             WHERE n.fq_name = ?1",
            [ns],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .ok()
    }

    #[test]
    fn link_is_set_repointed_by_refresh_and_dies_with_unmount() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "lk.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));

        system.mount_database(&db, "lk").expect("empty mount");
        let _c1 = link_of(&system, "lk").expect("link set at mount");
        let (m1, class, qualification) = mount_of(&system, "lk").expect("mount row set");
        assert_eq!(class, "attach");
        assert_eq!(qualification, "aliased");
        assert_eq!(m1, _c1);

        system.refresh_namespace("lk").expect("empty refresh");
        let c2 = link_of(&system, "lk").expect("link survives refresh");
        assert_eq!(mount_of(&system, "lk").map(|m| m.0), Some(c2));
        // SQLite reuses the freed max rowid, so the NUMERIC id may coincide
        // with the old one — the invariant is single ownership: exactly one
        // cartridge exists for this source, and the link points at it.
        let (count, only_id): (i64, i64) = {
            let conn = system.bootstrap_connection.lock().unwrap();
            conn.query_row(
                "SELECT count(*), max(id) FROM cartridge WHERE source_uri = 'file://' || ?1",
                [&db],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap()
        };
        assert_eq!(count, 1, "refresh must not duplicate the cartridge");
        assert_eq!(
            c2, only_id,
            "the link must point at the surviving cartridge"
        );

        system.unmount_database("lk").expect("unmount");
        assert_eq!(
            link_of(&system, "lk"),
            None,
            "namespace row gone with unmount"
        );
        assert_eq!(mount_of(&system, "lk"), None, "mount row gone with unmount");
        assert!(
            !cartridge_exists(&system, c2),
            "cartridge gone with unmount"
        );
    }

    #[test]
    fn unique_constraint_refuses_shared_cartridge() {
        let dir = tempfile::tempdir().unwrap();
        let db_a = empty_db(&dir, "a.sqlite");
        let db_b = empty_db(&dir, "b.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));
        system.mount_database(&db_a, "ua").expect("mount a");
        system.mount_database(&db_b, "ub").expect("mount b");
        let ca = link_of(&system, "ua").unwrap();

        let conn = system.bootstrap_connection.lock().unwrap();
        let err = conn
            .execute(
                "UPDATE mount SET cartridge_id = ?1
                 WHERE namespace_id = (SELECT id FROM namespace WHERE fq_name = 'ub')",
                [ca],
            )
            .expect_err("two namespaces must never share a mount cartridge");
        assert!(
            err.to_string().to_lowercase().contains("unique"),
            "got: {err}"
        );
    }

    #[test]
    fn bootstrap_stays_fk_consistent_across_the_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "fk.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));
        system.mount_database(&db, "fkns").expect("mount");
        system.refresh_namespace("fkns").expect("refresh");
        {
            let conn = system.bootstrap_connection.lock().unwrap();
            let mismatches: i64 = conn
                .query_row(
                    "SELECT count(*)
                     FROM mount m
                     LEFT JOIN namespace n ON n.id = m.namespace_id
                     WHERE n.id IS NULL OR n.kind != 'data'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(mismatches, 0, "mount rows must bind data namespaces");
        }
        system.unmount_database("fkns").expect("unmount");

        let conn = system.bootstrap_connection.lock().unwrap();
        let violations: i64 = conn
            .query_row(
                "SELECT count(*) FROM pragma_foreign_key_check('namespace')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(violations, 0, "namespace FK references must all resolve");
    }

    #[test]
    fn refresh_rollback_preserves_old_link_and_cartridge_together() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "rb.sqlite");
        // One successful introspection (the mount), then failure (the refresh).
        let mut system = system_with(Box::new(FlakyIntrospector {
            calls: AtomicUsize::new(0),
            ok_calls: 1,
        }));
        system.mount_database(&db, "rb").expect("mount");
        let c1 = link_of(&system, "rb").expect("link set");

        system
            .refresh_namespace("rb")
            .expect_err("induced refresh failure");
        assert_eq!(
            link_of(&system, "rb"),
            Some(c1),
            "rollback must restore the OLD link"
        );
        assert!(
            cartridge_exists(&system, c1),
            "rollback must restore the old cartridge WITH its link"
        );
    }

    #[test]
    fn ordinary_refresh_failure_keeps_the_enclosing_program_open() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "ordinary.sqlite");
        let mut system = system_with(Box::new(FlakyIntrospector {
            calls: AtomicUsize::new(0),
            ok_calls: 1,
        }));
        let mark = system.max_namespace_id().unwrap();
        system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("outer program begins");
        system.mount_database(&db, "ordinary").expect("mount");
        system
            .refresh_namespace("ordinary")
            .expect_err("induced introspection failure");
        system.rollback_liminal_external_effects();
        system
            .end_liminal_program(false)
            .expect("the inner failure must leave the outer savepoint to roll back");
    }

    #[test]
    fn failed_mount_binding_refresh_keeps_the_enclosing_program_open() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "binding_failure.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));
        system
            .mount_database(&db, "binding_failure")
            .expect("mount");
        let mark = system.max_namespace_id().unwrap();
        system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("outer program begins");
        {
            let conn = system.bootstrap_connection.lock().unwrap();
            let window = system.bootstrap_guard.migration_window();
            conn.execute_batch(
                "CREATE TRIGGER reject_refresh_mount
                 BEFORE INSERT ON mount
                 BEGIN
                     SELECT RAISE(FAIL, 'injected refresh mount failure');
                 END;",
            )
            .expect("inject a failure at the mount-binding writer");
            window.close(&conn).expect("re-seal the catalog");
        }
        let error = system
            .refresh_namespace("binding_failure")
            .expect_err("the replacement binding must fail");
        assert!(
            error.to_string().contains("Failed to record mount binding"),
            "{error}"
        );
        system
            .end_liminal_program(false)
            .expect("refresh must not roll back the enclosing savepoint");
    }

    /// Physical cleanup reads its (connection, alias)
    /// identity from the LINK, not from an arbitrary `.first()` of the
    /// deletion set — an entity-bearing auxiliary cartridge on the same
    /// namespace must not steal the DETACH from the real mount alias.
    #[test]
    fn unmount_detaches_the_link_alias_despite_auxiliary_cartridges() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "aux.sqlite");
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();
        let mut system = ReadySystem::new(conn, Box::new(EmptyIntrospector), "sqlite").unwrap();

        // The auxiliary cartridge is created BEFORE the mount: its LOWER id
        // precedes the mount cartridge in the
        // deletion-set query, so an implementation using `.first()` of that
        // set would hand physical cleanup the auxiliary's identity instead
        // of the mount's — this assertion would not catch that bug if the
        // auxiliary were created after the mount.
        let (aux_cart, ent) = {
            let _window = system.catalog_window();
            let c = system.bootstrap_connection.lock().unwrap();
            c.execute_batch(
                "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (3, 2, 'aux://side', NULL, 1, 2, 0);",
            )
            .unwrap();
            let aux_cart = c.last_insert_rowid();
            c.execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES ('side_t', 10, ?1)",
                [aux_cart],
            )
            .unwrap();
            (aux_cart, c.last_insert_rowid())
        };

        system.mount_database(&db, "auxns").expect("mount");
        let link = link_of(&system, "auxns").unwrap();
        assert!(
            aux_cart < link,
            "the auxiliary must precede the mount cartridge for the pin to bite"
        );

        // Activate the pre-existing auxiliary in the mounted namespace.
        let mount_alias: String = {
            let c = system.bootstrap_connection.lock().unwrap();
            let alias: String = c
                .query_row(
                    "SELECT source_ns FROM cartridge WHERE id = ?1",
                    [link],
                    |r| r.get(0),
                )
                .unwrap();
            let _window = system.catalog_window();
            c.execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id)
                 SELECT ?1, id, ?2 FROM namespace WHERE fq_name = 'auxns'",
                [ent, aux_cart],
            )
            .unwrap();
            alias
        };

        system.unmount_database("auxns").expect("unmount");
        assert!(
            mock.lock()
                .unwrap()
                .assert_executed(&format!("DETACH DATABASE '{mount_alias}'")),
            "physical cleanup must DETACH the LINK's alias, not an auxiliary's identity"
        );
        assert_eq!(link_of(&system, "auxns"), None, "namespace gone");
    }

    /// The lazily-cached catalog-cartridge id self-heals
    /// when the transaction that initialized it rolled back — the Cell is
    /// validated against the catalog before reuse.
    #[test]
    fn catalog_cache_survives_a_rolled_back_initialization() {
        let system = system_with(Box::new(EmptyIntrospector));
        let first = {
            let _window = system.catalog_window();
            let conn = system.bootstrap_connection.lock().unwrap();
            let id =
                super::population::ensure_catalog_initialized(&system.catalog_cartridge_id, &conn)
                    .expect("initialized (or cached from construction)");
            // Simulate the rolled-back initialization: the catalog
            // cartridge row vanishes while the Cell keeps its id — and
            // SQLite may then REUSE the freed rowid for an UNRELATED
            // cartridge. An existence-only validation
            // accepts this impostor as the catalog cartridge; the
            // identity-marker validation must reject it.
            // (FKs are enforced on bootstrap — clear the cartridge's
            // dependents the same way the production paths do.)
            super::entity_rows::retire_load(&conn, id as i64).unwrap();
            conn.execute(
                "INSERT INTO cartridge (id, language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (?1, 3, 2, 'impostor://not-the-catalog', NULL, 1, 2, 0)",
                [id],
            )
            .unwrap();
            id
        };
        // The Cell now caches an id held by an unrelated cartridge. The
        // property under test: ensure must NEVER adopt the impostor as the
        // catalog cartridge — an existence-only validation would wrongly
        // return Ok(first) here. Whether re-initialization then succeeds depends
        // on how much of the original initialization this simulation
        // removed — a REAL rollback removes all of it together — so both a
        // fresh id and a refusal are acceptable; returning the impostor is
        // not.
        let result = {
            let conn = system.bootstrap_connection.lock().unwrap();
            super::population::ensure_catalog_initialized(&system.catalog_cartridge_id, &conn)
        };
        match result {
            Ok(second) => assert_ne!(
                first, second,
                "the impostor's id must not be adopted as the catalog cartridge"
            ),
            Err(_) => assert_ne!(
                system.catalog_cartridge_id.get(),
                Some(first),
                "on refusal the poisoned cache entry must have been dropped"
            ),
        }
    }

    /// A failed DETACH must not split-brain the
    /// lifecycle — the catalog cascade rolls back, the mount identity is
    /// retained, and the operation reports failure. Once the obstruction
    /// clears, unmount succeeds normally.
    #[test]
    fn failed_detach_preserves_catalog_identity() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "dt.sqlite");
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();
        let mut system = ReadySystem::new(conn, Box::new(EmptyIntrospector), "sqlite").unwrap();
        system.mount_database(&db, "dtns").expect("mount");
        let link = link_of(&system, "dtns").expect("link set");

        mock.lock()
            .unwrap()
            .expect_error("DETACH DATABASE '_imported_", "database is locked");
        let err = system
            .unmount_database("dtns")
            .expect_err("unmount must FAIL when DETACH fails");
        assert!(
            err.to_string().contains("retained"),
            "the error must state the mount is retained: {err}"
        );
        assert_eq!(
            link_of(&system, "dtns"),
            Some(link),
            "the catalog identity must survive a failed DETACH"
        );

        // Obstruction cleared: unmount completes.
        mock.lock().unwrap().reset();
        system
            .unmount_database("dtns")
            .expect("unmount succeeds once DETACH can run");
        assert_eq!(link_of(&system, "dtns"), None);
    }

    /// For an attach-class mount the recorded alias is
    /// REQUIRED identity — refresh refuses loudly on a missing alias
    /// instead of silently falling back toward hub introspection.
    #[test]
    fn refresh_refuses_when_attach_alias_missing() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "na.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));
        system.mount_database(&db, "nans").expect("mount");
        {
            let conn = system.bootstrap_connection.lock().unwrap();
            let err = conn
                .execute(
                    "UPDATE mount SET attach_alias = NULL
                 WHERE namespace_id = (SELECT id FROM namespace WHERE fq_name = 'nans')",
                    [],
                )
                .expect_err("attach mount aliases must be unrepresentable when absent");
            assert!(err.to_string().contains("CHECK"), "got: {err}");
        }
    }

    #[test]
    fn empty_mount_identity_consumers_read_the_link() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "id.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));
        system.mount_database(&db, "idns").expect("empty mount");

        // The creation target's placement resolves for a ZERO-entity mount
        // (an entity-only derivation would fail here, since an empty mount
        // has none).
        let placement = {
            let conn = system.bootstrap_connection.lock().unwrap();
            crate::creation_target::DataTarget::read(
                &*conn,
                crate::definition_catalog::NamespaceKey::Fq("idns"),
                "a placement probe",
            )
            .expect("an empty mount is data-backed")
            .durable()
            .clone()
        };
        assert!(
            matches!(
                &placement,
                crate::creation_target::DurablePlacement::Schema(alias)
                    if alias.starts_with("_imported_")
            ),
            "empty mount must resolve its placement via the link, got {placement:?}"
        );
    }

    #[test]
    fn qualification_ignores_cartridge_source_ns_policy_conventions() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "qualification.sqlite");
        let mut system = system_with(Box::new(OneTableIntrospector));
        system.mount_database(&db, "qualified").expect("mount");

        let recorded_alias: String = {
            let conn = system.bootstrap_connection.lock().unwrap();
            let alias: String = conn
                .query_row(
                    "SELECT m.attach_alias
                     FROM mount m JOIN namespace n ON n.id = m.namespace_id
                     WHERE n.fq_name = 'qualified'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            conn.execute(
                "UPDATE cartridge SET source_ns = 'deliberately-wrong'
                 WHERE id = (SELECT m.cartridge_id FROM mount m
                             JOIN namespace n ON n.id = m.namespace_id
                             WHERE n.fq_name = 'qualified')",
                [],
            )
            .unwrap();
            alias
        };

        let resolved = system
            .resolve_namespace_path(&NamespacePath::single("qualified"))
            .expect("resolution")
            .expect("mounted namespace resolves");
        assert_eq!(resolved.0.as_deref(), Some(recorded_alias.as_str()));

        let columns = system
            .schema
            .as_ref()
            .expect("bootstrap-backed schema")
            .get_table_columns(Some(&recorded_alias), "mounted_t")
            .expect("mounted relation columns query succeeds")
            .expect("mounted relation columns resolve by mount qualification");
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name.as_str(), "id");
    }

    #[test]
    fn main_mount_records_physical_alias_but_resolves_unqualified() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "main.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));
        system.mount_database(&db, "main").expect("mount main");

        let (attach_alias, source_ns, qualification): (String, String, String) = {
            let conn = system.bootstrap_connection.lock().unwrap();
            conn.query_row(
                "SELECT m.attach_alias, c.source_ns, m.qualification
                 FROM mount m
                 JOIN namespace n ON n.id = m.namespace_id
                 JOIN cartridge c ON c.id = m.cartridge_id
                 WHERE n.fq_name = 'main'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
        };
        assert_eq!(
            source_ns, attach_alias,
            "source metadata is not NULL policy"
        );
        assert_eq!(qualification, "unqualified");
        assert_eq!(
            system
                .resolve_namespace_path(&NamespacePath::single("main"))
                .unwrap(),
            Some((None, 2)),
            "mount.qualification alone controls generated qualification"
        );
    }

    #[test]
    fn reset_rebuilds_catalog_and_live_indexes_without_mount_fragments() {
        let dir = tempfile::tempdir().unwrap();
        let db = empty_db(&dir, "reset.sqlite");
        let mut system = system_with(Box::new(EmptyIntrospector));
        system.mount_database(&db, "resetns").expect("mount");
        assert!(link_of(&system, "resetns").is_some());

        system.reinit_bootstrap().expect("reset");
        assert_eq!(link_of(&system, "resetns"), None);
        assert!(
            system.get_connection(2).is_ok(),
            "primary live route is rebuilt"
        );
        let mount_rows: i64 = system
            .bootstrap_connection
            .lock()
            .unwrap()
            .query_row("SELECT count(*) FROM mount", [], |row| row.get(0))
            .unwrap();
        assert_eq!(mount_rows, 0);
    }
}

/// The bootstrap structural backstop (RULINGS 2026-08-12), pinned on a REAL
/// system: the language's resolved-ownership refusal remains first (pinned
/// by directive_contract 42/45 and effects r03_dml_road_b_engine_owned);
/// what is pinned HERE is the layer beneath it — a deliberately lower-level
/// raw SQL road against the sealed catalog is denied by the authorizer.
#[cfg(test)]
mod bootstrap_guard_pins {
    use super::{DelightQLSystem, ReadySystem};
    use delightql_types::introspect::DatabaseIntrospector;
    use delightql_types::test_utils::MockDatabaseConnection;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(
            &self,
        ) -> delightql_types::Result<Vec<delightql_types::introspect::DiscoveredEntity>> {
            Ok(Vec::new())
        }
        fn introspect_entities_in_schema(
            &self,
            _schema: &str,
        ) -> delightql_types::Result<Vec<delightql_types::introspect::DiscoveredEntity>> {
            Ok(Vec::new())
        }
    }

    fn fresh_system() -> ReadySystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    /// EVERY canonical catalog object is covered — the inventory is derived
    /// from what installation created, so this loop cannot go stale when a
    /// system table is added to the schema authority.
    fn assert_catalog_is_sealed(system: &DelightQLSystem) {
        let bootstrap = system.bootstrap_connection();
        let conn = bootstrap.lock().expect("bootstrap lock");
        let mut statement = conn
            .prepare("SELECT name, type FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")
            .expect("inventory");
        let objects: Vec<(String, String)> = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("inventory")
            .collect::<std::result::Result<_, _>>()
            .expect("inventory");
        drop(statement);
        assert!(
            objects.iter().filter(|(_, kind)| kind == "table").count() > 20,
            "the canonical catalog is dozens of tables; found {}",
            objects.len()
        );
        for (name, kind) in &objects {
            let quoted = format!("\"{}\"", name.replace('"', "\"\""));
            let drop_sql = match kind.as_str() {
                "table" => format!("DROP TABLE {quoted}"),
                "view" => format!("DROP VIEW {quoted}"),
                "index" => format!("DROP INDEX {quoted}"),
                "trigger" => format!("DROP TRIGGER {quoted}"),
                other => panic!("unexpected sqlite_master type {other}"),
            };
            assert!(
                conn.execute_batch(&drop_sql).is_err(),
                "'{drop_sql}' must be denied by the structural backstop"
            );
            if kind == "table" {
                assert!(
                    conn.execute_batch(&format!(
                        "ALTER TABLE {quoted} ADD COLUMN __backstop_probe TEXT"
                    ))
                    .is_err(),
                    "ALTER on catalog table '{name}' must be denied"
                );
            }
        }
    }

    /// A raw structural attempt against every canonical object is denied,
    /// while the connection's ordinary work — catalog row DML and scratch
    /// objects the installation did not create — stays ordinary. (Ordinary
    /// USER database tables live on the user connection, which carries no
    /// authorizer at all; the whole suite is that pin.)
    #[test]
    fn every_canonical_catalog_object_is_protected_and_ordinary_work_is_not() {
        let system = fresh_system();
        assert_catalog_is_sealed(&system);

        let bootstrap = system.bootstrap_connection();
        let conn = bootstrap.lock().expect("bootstrap lock");
        conn.execute("INSERT INTO compilation (id) VALUES (999999)", [])
            .ok(); // row DML may fail on constraints, never on the guard
        conn.execute_batch("CREATE TABLE __scratch_probe (x INTEGER)")
            .expect("an uninventoried name is ordinary");
        conn.execute_batch("DROP TABLE __scratch_probe")
            .expect("and stays ordinary");
    }

    /// Reinitialization is the sanctioned installation capability: it
    /// rebuilds the catalog and the REBUILT catalog is sealed again, with no
    /// second registration step.
    #[test]
    fn reinitialization_still_works_and_reseals() {
        let mut system = fresh_system();
        system
            .reinit_bootstrap()
            .expect("reset works under the guard");
        assert_catalog_is_sealed(&system);
    }
}
