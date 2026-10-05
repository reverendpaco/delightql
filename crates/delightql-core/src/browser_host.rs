// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Browser-owned capabilities behind the shared compiler's system contract.
//!
//! This is a host adapter, not a catalog or a second compiler. Database and
//! schema operations delegate to capabilities injected by `delightql-wasm`;
//! catalog, filesystem, and external-effect entrances refuse through
//! [`crate::host`] when the browser did not supply them.

use crate::bin_cartridge::registry::BinCartridgeRegistry;
use crate::diagnostic::{Runtime, SessionHealth};
use crate::error::{DelightQLError, Result};
use crate::host::CompilerHost;
use delightql_types::{
    ColumnInfo, ConnectionFactory, DatabaseConnection, DatabaseIntrospector, DatabaseSchema,
    NamespacePath,
};
use std::cell::Cell;
use std::sync::{Arc, Mutex};

pub(crate) use crate::system_vocabulary::{
    builtin_registry, validate_user_namespace_target, ImprintMode, LiminalReceipt, LiminalRow,
    LoadPhase, PhysicalRead, StdlibLoad, PRIMARY_CONNECTION_ID,
};

/// A load request assembled by shared directive code before it reaches the
/// browser's catalog boundary. It preserves every authored item; publication
/// is the single operation that refuses when no session catalog is supplied.
pub(crate) struct PreparedLoad {
    namespace: String,
    definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    rows: Vec<crate::bin_cartridge::prelude::consult::PreparedRow>,
    deferred_docs: Vec<(String, String)>,
}

impl PreparedLoad {
    pub(crate) fn from_file(
        namespace: &str,
        _path: &str,
        _mode: crate::bin_cartridge::prelude::consult::LiminalDirectiveMode,
    ) -> Self {
        PreparedLoad {
            namespace: namespace.to_string(),
            definitions: Vec::new(),
            rows: Vec::new(),
            deferred_docs: Vec::new(),
        }
    }

    pub(crate) fn inline(
        namespace: &str,
        definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    ) -> Self {
        PreparedLoad {
            namespace: namespace.to_string(),
            definitions,
            rows: Vec::new(),
            deferred_docs: Vec::new(),
        }
    }

    pub(crate) fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn define(&mut self, clause: crate::pipeline::asts::ddl::ClauseDecl) {
        self.definitions.push(clause);
    }

    pub(crate) fn settle(&mut self, row: crate::bin_cartridge::prelude::consult::PreparedRow) {
        self.rows.push(row);
    }

    pub(crate) fn doc(&mut self, target: String, doc: String) {
        self.deferred_docs.push((target, doc));
    }

    pub(crate) fn enlist(&mut self, system: &mut DelightQLSystem, target: &str) -> Result<()> {
        system.enlist_namespace(target)
    }

    pub(crate) fn alias(
        &mut self,
        system: &mut DelightQLSystem,
        shorthand: &str,
        target: &str,
    ) -> Result<()> {
        system.register_namespace_alias(shorthand, target)
    }

    pub(crate) fn expose(&mut self, system: &DelightQLSystem, _child_fq: &str) -> Result<()> {
        system.require(crate::host::Capability::SessionCatalog, "expose!()")
    }
}

/// Proof of a successful browser publication. The browser supplies no session
/// catalog, so safe code cannot construct this type.
pub(crate) enum PublishedLoad {}

impl PublishedLoad {
    pub(crate) fn definitions_loaded(&self) -> usize {
        match *self {}
    }

    pub(crate) fn replaced_entities(&self) -> &[String] {
        match *self {}
    }

    pub(crate) fn into_ledger(self) -> Vec<crate::bin_cartridge::prelude::consult::PreparedRow> {
        match self {}
    }
}

/// Transaction kind requested by shared consultation orchestration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiminalProgramKind {
    Consult,
    Reconsult,
}

/// Connection-backed database schema for WASM.
///
/// Implements DatabaseSchema by routing PRAGMA and sqlite_master queries
/// through the stored connection (which calls bridge_sql on the JS side).
struct ConnectionBackedSchema {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
}

fn sqlite_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sqlite_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn malformed_schema(field: &str) -> DelightQLError {
    Runtime::General {
        message: "browser SQLite schema metadata is malformed".to_string(),
        details: format!("invalid or missing required column '{field}'"),
    }
    .into()
}

impl DatabaseSchema for ConnectionBackedSchema {
    fn get_table_columns(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<Vec<ColumnInfo>>> {
        let sql = match schema {
            Some(schema) => format!(
                "PRAGMA {}.table_xinfo({})",
                sqlite_identifier(schema),
                sqlite_literal(table_name)
            ),
            None => format!("PRAGMA table_xinfo({})", sqlite_literal(table_name)),
        };
        let conn = self.connection.lock().map_err(|error| {
            Runtime::poisoned(
                "Failed to acquire browser schema connection",
                error.to_string(),
            )
        })?;
        let (columns, rows) = conn.query_all_rows(&sql, &[])?;
        if rows.is_empty() {
            return Ok(None);
        }

        let index = |name: &str| {
            columns
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| malformed_schema(name))
        };
        let cid_idx = index("cid")?;
        let name_idx = index("name")?;
        let type_idx = index("type")?;
        let notnull_idx = index("notnull")?;

        let cols = rows
            .iter()
            .map(|row| {
                let cid = row
                    .get(cid_idx)
                    .and_then(|value| value.as_wire_text())
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|value| *value >= 0)
                    .ok_or_else(|| malformed_schema("cid"))?;
                let name = row
                    .get(name_idx)
                    .and_then(|value| value.as_wire_text())
                    .ok_or_else(|| malformed_schema("name"))?;
                let declared_type = row
                    .get(type_idx)
                    .and_then(|value| value.as_wire_text())
                    .ok_or_else(|| malformed_schema("type"))?;
                let notnull = row
                    .get(notnull_idx)
                    .and_then(|value| value.as_wire_text())
                    .and_then(|value| value.parse::<i64>().ok())
                    .ok_or_else(|| malformed_schema("notnull"))?;
                Ok(ColumnInfo {
                    name: name.into(),
                    nullable: notnull == 0,
                    position: (cid + 1) as usize,
                    declared_type: (!declared_type.is_empty()).then_some(declared_type),
                    interior: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Some(cols))
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> Result<bool> {
        Ok(self.get_table_columns(schema, table_name)?.is_some())
    }
}

/// The database the browser keeps its targeting catalog in: an in-memory
/// database attached to the session's own SQLite connection. The user's
/// database is never written, and the catalog is read by the same direct
/// SQL the native bootstrap catalog is.
const TARGETING_SCHEMA: &str = "dql_targeting";

fn aggregates_table() -> String {
    format!("{TARGETING_SCHEMA}.aggregates")
}

fn type_classes_table() -> String {
    format!("{TARGETING_SCHEMA}.type_classes")
}

/// Attach a fresh targeting database and seed its `aggregates` and
/// `type_classes` tables from the seed scripts both hosts use. A session
/// reset over the same connection replaces the database it attached before.
fn seed_targeting_catalog(connection: &Arc<Mutex<dyn DatabaseConnection>>) -> Result<()> {
    let conn = connection.lock().map_err(|error| {
        Runtime::poisoned(
            "Failed to acquire the browser connection to seed the targeting catalog",
            error.to_string(),
        )
    })?;
    let (columns, rows) = conn.query_all_rows("PRAGMA database_list", &[])?;
    let name = columns
        .iter()
        .position(|column| column == "name")
        .ok_or_else(|| malformed_schema("name"))?;
    let attached = rows.iter().any(|row| {
        row.get(name)
            .and_then(|value| value.as_wire_text())
            .is_some_and(|schema| schema == TARGETING_SCHEMA)
    });
    if attached {
        conn.execute(&format!("DETACH DATABASE {TARGETING_SCHEMA}"), &[])?;
    }
    conn.execute(
        &format!("ATTACH DATABASE ':memory:' AS {TARGETING_SCHEMA}"),
        &[],
    )?;
    conn.execute(
        &crate::pipeline::aggregate_catalog::seed_script(&aggregates_table()),
        &[],
    )?;
    conn.execute(
        &crate::pipeline::type_classes::seed_script(&type_classes_table()),
        &[],
    )?;
    Ok(())
}

/// Browser host state for the shared compiler.
///
/// Database operations go through injected JavaScript-backed traits. No
/// bootstrap catalog is constructed here.
pub(crate) struct DelightQLSystem {
    /// Optional services supplied by this concrete host instance.
    capabilities: crate::host::HostCapabilities,

    /// User database connection (JavaScript bridge)
    pub connection: Arc<Mutex<dyn DatabaseConnection>>,

    /// Database schema provider (injected)
    schema: Box<dyn DatabaseSchema>,

    introspector: Box<dyn DatabaseIntrospector>,

    db_type: String,

    /// The dialect the host stated at boot. Held here because this host
    /// keeps no catalog to hold it.
    stated_dialect: Option<crate::pipeline::generator::SqlDialect>,

    connection_factory: Option<Box<dyn ConnectionFactory>>,

    /// Bin cartridge registry for built-in entities
    bin_registry: Arc<BinCartridgeRegistry>,

    /// Shared relay health API. The browser has no liminal effects today, but
    /// it still carries the latch so protocol behavior does not diverge.
    /// `None` = healthy; `Some((operation, message))` = quarantined, with the
    /// incident retained for the typed host report.
    session_incident: Option<(String, String)>,

    effects_executed: Cell<u64>,

    findings: Mutex<Vec<(String, String, String)>>,
}

/// The system a host is given. On native this is the reset-capable owner of
/// the pristine image; the wasm system owns no bootstrap catalog and no
/// image, so the host is given the system itself and `reinit_bootstrap`
/// refuses. No image abstraction is pretended here.
pub(crate) type ReadySystem = DelightQLSystem;

impl DelightQLSystem {
    pub(crate) fn compiler_host_mut(&mut self) -> &mut DelightQLSystem {
        self
    }

    /// Create a new WASM DelightQL system
    pub fn new(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn delightql_types::DatabaseIntrospector>,
        db_type: &str,
    ) -> Result<Self> {
        // Initialize bin cartridge registry
        let mut bin_registry = BinCartridgeRegistry::new();

        // Register the prelude cartridge
        bin_registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());

        // Register the predicates cartridge
        bin_registry
            .register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());

        let schema = Box::new(ConnectionBackedSchema {
            connection: connection.clone(),
        });

        seed_targeting_catalog(&connection)?;

        Ok(DelightQLSystem {
            capabilities: crate::host::HostCapabilities::browser(),
            connection,
            schema,
            introspector,
            db_type: db_type.to_string(),
            stated_dialect: None,
            connection_factory: None,
            bin_registry: Arc::new(bin_registry),
            session_incident: None,
            effects_executed: Cell::new(0),
            findings: Mutex::new(Vec::new()),
        })
    }

    /// The browser system keeps no catalog, so it keeps no boot rows: the
    /// host's settings were admitted at `open()`, and a browser has no
    /// filesystem for a base directory to name.
    pub fn booted(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn delightql_types::DatabaseIntrospector>,
        db_type: &str,
        boot: &crate::settings::Admitted,
    ) -> Result<Self> {
        let mut system = Self::new(connection, introspector, db_type)?;
        system.stated_dialect = boot.dialect();
        Ok(system)
    }

    /// Only an absolute path names a file here; there is no base directory.
    pub(crate) fn resolve_path(&self, raw: &str) -> Result<std::path::PathBuf> {
        let path = std::path::Path::new(raw);
        if path.is_absolute() {
            return Ok(path.to_path_buf());
        }
        Err(crate::diagnostic::Runtime::Io {
            message: format!(
                "'{raw}' is a relative path, and this host has no base directory \
                 to resolve it against; use an absolute path"
            ),
        }
        .into())
    }

    /// No session layer is kept, so there is nothing to clear.
    pub(crate) fn clear_session_settings(&self) -> Result<()> {
        Ok(())
    }

    /// No session layer is kept here.
    pub(crate) fn set_session_setting(&self, key: &str, _value: Option<&str>) -> Result<()> {
        Err(crate::diagnostic::Configuration::BootTable {
            problems: format!("'{key}' cannot be set per session on this host"),
        }
        .into())
    }

    /// Get the database schema
    pub fn get_schema(&self) -> Result<&dyn DatabaseSchema> {
        Ok(self.schema.as_ref())
    }

    /// Get the bin cartridge registry
    pub fn bin_registry(&self) -> Arc<BinCartridgeRegistry> {
        Arc::clone(&self.bin_registry)
    }

    /// Names a plan must not reuse for compiler-owned scratch relations.
    /// The browser receives this vocabulary from its injected SQLite schema
    /// discovery capability; it does not invent an empty session catalog.
    pub(crate) fn effect_plan_reserved_names(&self) -> Result<Vec<String>> {
        let mut names = self
            .introspector
            .introspect_entities()?
            .into_iter()
            .map(|entity| entity.name.as_str().to_string())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// Get connection for a given connection_id (WASM always returns user connection)
    pub fn get_connection(&self, connection_id: i64) -> Result<Arc<Mutex<dyn DatabaseConnection>>> {
        if connection_id == PRIMARY_CONNECTION_ID {
            Ok(Arc::clone(&self.connection))
        } else {
            Err(DelightQLError::from(Runtime::Unsupported {
                message: format!(
                    "connection {connection_id} is unavailable: the browser host supplies only the primary SQLite connection"
                ),
            }))
        }
    }

    pub fn set_connection_factory(&mut self, factory: Box<dyn ConnectionFactory>) {
        self.connection_factory = Some(factory);
    }

    pub(crate) fn introspect_passthrough_relation(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<delightql_types::introspect::DiscoveredRelation>> {
        self.introspector.introspect_relation(schema, relation_name)
    }

    pub fn dialect_for_connection(
        &self,
        connection_id: Option<i64>,
    ) -> crate::pipeline::generator::SqlDialect {
        use crate::pipeline::generator::SqlDialect;
        match connection_id {
            None | Some(PRIMARY_CONNECTION_ID) => {
                SqlDialect::from_family_name(&self.db_type.to_lowercase())
                    .unwrap_or(SqlDialect::SQLite)
            }
            Some(_) => SqlDialect::SQLite,
        }
    }

    pub fn fatboy_main_connection_for_effect_plan(&self) -> Option<i64> {
        None
    }

    pub fn siso_connection_for_effect_plan(&self, _connection_id: Option<i64>) -> bool {
        false
    }

    /// Mounting is a filesystem-owned host operation.
    pub fn mount_database(&mut self, _db_path: &str, _namespace: &str) -> Result<()> {
        self.require(crate::host::Capability::Filesystem, "mount!()")
    }

    pub fn mount_new_database(&mut self, _db_path: &str, _namespace: &str) -> Result<()> {
        self.require(crate::host::Capability::Filesystem, "mount_new!()")
    }

    pub fn mount_database_tree(&mut self, _uri: &str, _namespace: &str) -> Result<Vec<String>> {
        self.require(crate::host::Capability::Filesystem, "mount_tree!()")?;
        unreachable!("a supplied filesystem host has a mount-tree implementation")
    }

    /// Byte bindings (`delightql-bytes://`, documentation/archived/2026-08-05/BYTES-SCHEME-DESIGN.md) — not
    /// supported in WASM: it cannot attach deserialized native SQLite
    /// schemas. The documented refusal, actually implemented.
    pub fn bind_static_bytes(&mut self, _name: &str, _bytes: &'static [u8]) -> Result<()> {
        self.require(
            crate::host::Capability::NativeSqlite,
            "static SQLite image binding",
        )
    }

    /// Owned-buffer sibling — same WASM refusal.
    pub fn bind_owned_bytes(&mut self, _name: &str, _bytes: Vec<u8>) -> Result<()> {
        self.require(
            crate::host::Capability::NativeSqlite,
            "owned SQLite image binding",
        )
    }

    /// Enlistment requires a session catalog.
    pub fn enlist_namespace(&mut self, _namespace: &str) -> Result<()> {
        self.require(crate::host::Capability::SessionCatalog, "enlist!()")
    }

    /// Delistment requires a session catalog.
    pub fn delist_namespace(&mut self, _namespace: &str) -> Result<()> {
        self.require(crate::host::Capability::SessionCatalog, "delist!()")
    }

    /// Unmounting requires a filesystem host.
    pub fn unmount_database(&mut self, _namespace: &str) -> Result<()> {
        self.require(crate::host::Capability::Filesystem, "unmount!()")
    }

    /// Removing a consultation requires a session catalog.
    pub fn unconsult_namespace(&mut self, _namespace: &str) -> Result<()> {
        self.require(crate::host::Capability::SessionCatalog, "unconsult!()")
    }

    /// Refreshing a namespace requires a session catalog.
    pub fn refresh_namespace(&mut self, _namespace: &str) -> Result<usize> {
        self.require(crate::host::Capability::SessionCatalog, "refresh!()")?;
        unreachable!("a supplied session catalog has a refresh implementation")
    }

    /// Reconsulting a namespace requires a session catalog.
    pub fn reconsult_namespace(
        &mut self,
        _namespace: &str,
        _new_file: Option<&str>,
    ) -> Result<usize> {
        self.require(crate::host::Capability::SessionCatalog, "reconsult!()")?;
        unreachable!("a supplied session catalog has a reconsult implementation")
    }

    /// Alias registration requires a session catalog.
    pub fn register_namespace_alias(&mut self, _alias: &str, _namespace: &str) -> Result<()> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "namespace alias registration",
        )
    }

    /// Publish a complete load through the catalog capability.
    pub(crate) fn publish(&mut self, load: PreparedLoad) -> Result<PublishedLoad> {
        let operation = format!("publish load into '{}'", load.namespace());
        self.require(crate::host::Capability::SessionCatalog, &operation)?;
        unreachable!("a supplied session catalog has a publication implementation")
    }

    /// Liminal-program machinery (native atomic boundary): this host has
    /// no consultation, so there is never an active program.
    pub(crate) fn begin_liminal_program(
        &self,
        _mark: i64,
        _kind: LiminalProgramKind,
    ) -> Result<bool> {
        self.require(
            crate::host::Capability::ExternalEffects,
            "liminal program transaction",
        )?;
        unreachable!("a supplied external-effects host has a liminal implementation")
    }

    pub(crate) fn end_liminal_program(&self, _commit: bool) -> Result<()> {
        self.require(
            crate::host::Capability::ExternalEffects,
            "liminal program transaction",
        )
    }

    pub(crate) fn rollback_liminal_external_effects(&mut self) {}

    pub(crate) fn require_healthy(&self) -> Result<()> {
        if self.session_incident.is_none() {
            Ok(())
        } else {
            Err(DelightQLError::from(SessionHealth::ExternalEffect {
                message:
                    "the session is quarantined; reset or reconnect before issuing another query"
                        .to_string(),
            }))
        }
    }

    pub(crate) fn quarantine_session(
        &mut self,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) {
        if self.session_incident.is_none() {
            self.session_incident = Some((operation.into(), message.into()));
        }
    }

    /// The quarantine incident, if one is latched: (operation, message).
    pub(crate) fn health_incident(&self) -> Option<(&str, &str)> {
        self.session_incident
            .as_ref()
            .map(|(operation, message)| (operation.as_str(), message.as_str()))
    }

    /// Namespace snapshot helpers require the session catalog.
    pub fn max_namespace_id(&self) -> Result<i64> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "namespace enumeration",
        )?;
        unreachable!("a supplied session catalog has a namespace implementation")
    }

    pub fn namespace_exists(&self, _fq: &str) -> Result<bool> {
        self.require(crate::host::Capability::SessionCatalog, "namespace lookup")?;
        unreachable!("a supplied session catalog has a namespace implementation")
    }

    pub(crate) fn set_entity_docs_atomic(
        &mut self,
        _entries: &[(String, String)],
    ) -> Result<Vec<(String, String)>> {
        self.require(crate::host::Capability::SessionCatalog, "doc!()")?;
        unreachable!("a supplied session catalog has a documentation implementation")
    }

    /// Imprinting is catalog-owned.
    pub fn imprint_namespace(
        &mut self,
        _source_ns: &str,
        _target_ns: &str,
        _mode: ImprintMode,
    ) -> Result<Vec<(String, String, String)>> {
        self.require(crate::host::Capability::SessionCatalog, "imprint!()")?;
        unreachable!("a supplied session catalog has an imprint implementation")
    }

    /// Namespace path resolution requires the session catalog.
    pub fn resolve_namespace_path(
        &self,
        _path: &NamespacePath,
    ) -> Result<Option<(Option<String>, i64)>> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "namespace path resolution",
        )?;
        unreachable!("a supplied session catalog has a namespace implementation")
    }

    /// Publish the compilation's armed limits — nothing to publish into.
    ///
    /// This host has no bootstrap catalog, so there is no `compiler_limit` row that
    /// could go stale and nothing here to keep true. The guards are unaffected
    /// either way: a compilation is bounded by the limits its own registry
    /// armed, and neither guard reads a database.
    pub(crate) fn publish_compiler_limits(&self, _armed: &crate::compiler_limits::ArmedLimits) {}

    /// Bootstrap reinitialization requires native SQLite.
    pub fn reinit_bootstrap(&mut self) -> Result<()> {
        self.require(
            crate::host::Capability::NativeSqlite,
            "bootstrap catalog reinitialization",
        )
    }

    /// Physical schema placement belongs to the session catalog.
    pub fn physical_schema_of(&self, _namespace_fq: &str) -> Result<Option<String>> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "physical schema lookup",
        )?;
        unreachable!("a supplied session catalog has a physical-schema implementation")
    }

    /// Served-identity placement requires the session catalog.
    pub(crate) fn physical_read(
        &self,
        _served: &crate::defuse::ServedEntity,
    ) -> Result<crate::system::PhysicalRead> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "served identity placement",
        )?;
        unreachable!("a supplied session catalog has a placement implementation")
    }

    /// Registered entity headings require the session catalog.
    pub fn output_columns_for_entity(
        &self,
        _entity_id: i64,
    ) -> Result<Vec<delightql_types::schema::ColumnInfo>> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "served identity heading",
        )?;
        unreachable!("a supplied session catalog has a heading implementation")
    }

    /// Retraction is catalog-owned.
    pub(crate) fn retract(
        &mut self,
        _world: &crate::defuse::standing::StandingWorld,
        _target: &delightql_types::SqlIdentifier,
        _qualifier: Option<&crate::pipeline::asts::vocabulary::Qualifier>,
    ) -> Result<(String, String, Option<String>)> {
        self.require(crate::host::Capability::SessionCatalog, "retract!()")?;
        unreachable!("a supplied session catalog has a retraction implementation")
    }

    /// Enlistment snapshots require the session catalog.
    pub fn save_enlisted_state(&self) -> Result<Vec<(i32, i32)>> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "enlistment snapshot",
        )?;
        unreachable!("a supplied session catalog has an enlistment implementation")
    }

    /// Alias snapshots require the session catalog.
    pub fn save_alias_state(&self) -> Result<Vec<(String, i32)>> {
        self.require(crate::host::Capability::SessionCatalog, "alias snapshot")?;
        unreachable!("a supplied session catalog has an alias implementation")
    }

    /// Enlistment restore requires the session catalog.
    pub fn restore_enlisted_state(&mut self, _saved: &[(i32, i32)]) -> Result<()> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "enlistment restore",
        )
    }

    /// Alias restore requires the session catalog.
    pub fn restore_alias_state(&mut self, _saved: &[(String, i32)]) -> Result<()> {
        self.require(crate::host::Capability::SessionCatalog, "alias restore")
    }

    /// Grounding is catalog-owned.
    pub fn ground_namespace(
        &mut self,
        _data_ns: &str,
        _lib_ns: &str,
        _new_ns_name: &str,
    ) -> Result<usize> {
        self.require(crate::host::Capability::SessionCatalog, "ground!()")?;
        unreachable!("a supplied session catalog has a grounding implementation")
    }

    pub(crate) fn record_liminal_ledger(
        &self,
        _namespace: &str,
        _rows: &[LiminalRow],
    ) -> Result<()> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "liminal ledger recording",
        )
    }

    pub fn ensure_stdlib_loaded(&self, namespace_fq: &str) -> StdlibLoad {
        if !crate::stdlib_manifest::STDLIB_MODULES
            .iter()
            .any(|(namespace, _)| *namespace == namespace_fq)
        {
            return StdlibLoad::NotAModule;
        }
        StdlibLoad::Failed {
            phase: LoadPhase::Consult,
            error: self
                .require(
                    crate::host::Capability::SessionCatalog,
                    "standard-library consultation",
                )
                .expect_err("the browser host does not supply a session catalog"),
        }
    }

    pub(crate) fn note_effect_executed(&self) {
        self.effects_executed.set(self.effects_executed.get() + 1);
    }

    pub fn materialize_effect_plan(
        &self,
        _typed: &crate::pipeline::compiled_query::TypedEffectPlan,
    ) -> Result<()> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "effect-plan observation",
        )
    }

    pub fn materialize_effect_run(
        &self,
        _outcomes: &[(&'static str, Option<String>)],
    ) -> Result<()> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "effect-run observation",
        )
    }

    pub fn record_assertion_verdict(
        &self,
        _verdict: &crate::pipeline::verdict::Verdict,
        _run_id: &str,
    ) -> Result<()> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "assertion observation",
        )
    }

    pub fn record_finding(
        &self,
        _kind: crate::diagnostics::Severity,
        uri: &str,
        message: &str,
        _input: Option<&str>,
        provider: &str,
    ) {
        if let Ok(mut findings) = self.findings.lock() {
            findings.push((uri.to_string(), message.to_string(), provider.to_string()));
        }
    }
}

impl crate::host::CompilerHost for DelightQLSystem {
    fn capabilities(&self) -> crate::host::HostCapabilities {
        self.capabilities
    }

    fn get_schema(&self) -> Result<&dyn DatabaseSchema> {
        DelightQLSystem::get_schema(self)
    }

    fn bin_registry(&self) -> Arc<BinCartridgeRegistry> {
        DelightQLSystem::bin_registry(self)
    }

    fn publish_compiler_limits(&self, armed: &crate::compiler_limits::ArmedLimits) {
        DelightQLSystem::publish_compiler_limits(self, armed);
    }

    fn introspect_passthrough_relation(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<delightql_types::introspect::DiscoveredRelation>> {
        DelightQLSystem::introspect_passthrough_relation(self, schema, relation_name)
    }

    fn dialect_for_connection(
        &self,
        connection_id: Option<i64>,
    ) -> crate::pipeline::generator::SqlDialect {
        DelightQLSystem::dialect_for_connection(self, connection_id)
    }

    fn stated_dialect(&self) -> Result<Option<crate::pipeline::generator::SqlDialect>> {
        Ok(self.stated_dialect)
    }

    fn connection_dialects(&self) -> Result<Vec<crate::pipeline::generator::SqlDialect>> {
        Ok(vec![DelightQLSystem::dialect_for_connection(self, None)])
    }

    fn dialect_pack(&self) -> Result<Arc<crate::pipeline::dialect_pack::DialectPack>> {
        Ok(Arc::new(crate::pipeline::dialect_pack::DialectPack::empty()))
    }

    fn aggregate_catalog(
        &self,
    ) -> Result<Arc<crate::pipeline::aggregate_catalog::AggregateCatalog>> {
        use crate::pipeline::aggregate_catalog::{select_rows, AggregateCatalog, AggregateRow};
        let conn = self.connection.lock().map_err(|error| {
            Runtime::poisoned(
                "Failed to acquire the browser connection for the aggregate catalog",
                error.to_string(),
            )
        })?;
        let (_, rows) = conn.query_all_rows(&select_rows(&aggregates_table()), &[])?;
        let malformed = |field: &str| -> DelightQLError {
            Runtime::catalog(
                "read the browser aggregate catalog",
                format!("invalid or missing column '{field}'"),
            )
        };
        let text = |row: &[delightql_types::DbValue], index: usize, field: &str| {
            row.get(index)
                .and_then(|value| value.as_wire_text())
                .ok_or_else(|| malformed(field))
        };
        let integer = |row: &[delightql_types::DbValue], index: usize, field: &str| {
            text(row, index, field)?
                .parse::<i64>()
                .map_err(|_| malformed(field))
        };
        let rows = rows
            .iter()
            .map(|row| {
                Ok(AggregateRow {
                    dialect: text(row, 0, "dialect")?,
                    functor_name: text(row, 1, "functor_name")?,
                    arity: integer(row, 2, "arity")?,
                    can_be_globbed: integer(row, 3, "can_be_globbed")?,
                    can_be_windowed: integer(row, 4, "can_be_windowed")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(AggregateCatalog::from_rows(rows)?))
    }

    fn type_classes(&self) -> Result<Arc<crate::pipeline::type_classes::TypeClasses>> {
        use crate::pipeline::type_classes::{select_rows, TypeClassRow, TypeClasses};
        let conn = self.connection.lock().map_err(|error| {
            Runtime::poisoned(
                "Failed to acquire the browser connection for the type classes",
                error.to_string(),
            )
        })?;
        let (_, rows) = conn.query_all_rows(&select_rows(&type_classes_table()), &[])?;
        let text = |row: &[delightql_types::DbValue], index: usize, field: &str| {
            row.get(index).and_then(|value| value.as_wire_text()).ok_or_else(|| {
                Runtime::catalog(
                    "read the browser type classes",
                    format!("invalid or missing column '{field}'"),
                )
            })
        };
        let rows = rows
            .iter()
            .map(|row| {
                Ok(TypeClassRow {
                    dialect: text(row, 0, "dialect")?,
                    reads: text(row, 1, "reads")?,
                    type_name: text(row, 2, "type_name")?,
                    class: text(row, 3, "class")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Arc::new(TypeClasses::from_rows(rows)?))
    }

    fn effect_plan_reserved_names(&self) -> Result<Vec<String>> {
        DelightQLSystem::effect_plan_reserved_names(self)
    }

    fn bootstrap_table_columns(&self, _table: &str) -> Result<Vec<ColumnInfo>> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "bootstrap table heading",
        )?;
        unreachable!("a supplied session catalog has a heading implementation")
    }

    fn bootstrap_relation_rows(
        &self,
        _schema: Option<&str>,
        _table: &str,
        _columns: &[String],
    ) -> Result<Vec<Vec<delightql_types::DbValue>>> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "bootstrap relation materialization",
        )?;
        unreachable!("a supplied session catalog has a materialization implementation")
    }

    fn definition_catalog(
        &self,
        operation: &str,
    ) -> Result<Box<dyn crate::definition_catalog::DefinitionCatalog + '_>> {
        self.require(crate::host::Capability::SessionCatalog, operation)?;
        unreachable!("a supplied session catalog has a fact implementation")
    }

    fn fatboy_main_connection_for_effect_plan(&self) -> Option<i64> {
        DelightQLSystem::fatboy_main_connection_for_effect_plan(self)
    }

    fn siso_connection_for_effect_plan(&self, connection_id: Option<i64>) -> bool {
        DelightQLSystem::siso_connection_for_effect_plan(self, connection_id)
    }

    fn resolve_namespace_path(
        &self,
        path: &NamespacePath,
    ) -> Result<Option<(Option<String>, i64)>> {
        DelightQLSystem::resolve_namespace_path(self, path)
    }

    fn physical_schema_of(&self, namespace_fq: &str) -> Result<Option<String>> {
        DelightQLSystem::physical_schema_of(self, namespace_fq)
    }

    fn physical_read(&self, entity: &crate::defuse::ServedEntity) -> Result<PhysicalRead> {
        DelightQLSystem::physical_read(self, entity)
    }

    fn output_columns_for_entity(&self, entity_id: i64) -> Result<Vec<ColumnInfo>> {
        DelightQLSystem::output_columns_for_entity(self, entity_id)
    }

    fn populate_namespace(
        &self,
        _namespace: &crate::defuse::environment::population::LazyNamespace,
    ) -> Result<()> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "namespace population",
        )
    }

    fn publish_danger_overrides(
        &self,
        overrides: &[crate::pipeline::ast_unresolved::DangerSpec],
    ) -> Result<()> {
        if overrides.is_empty() {
            Ok(())
        } else {
            self.require(
                crate::host::Capability::SessionCatalog,
                "CLI danger override publication",
            )
        }
    }

    fn namespace_kind(&self, _namespace: &str) -> Result<Option<crate::namespace::NamespaceKind>> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "run! namespace lookup",
        )?;
        unreachable!("a supplied session catalog has a namespace implementation")
    }

    fn query_session_catalog(&self, _sql: &str) -> Result<crate::host::HostQueryResult> {
        self.require(
            crate::host::Capability::SessionCatalog,
            "session catalog query",
        )?;
        unreachable!("a supplied session catalog has a query implementation")
    }
}

impl crate::host::CompilerExecutionHost for DelightQLSystem {
    fn register_prompt_blocks(
        &mut self,
        blocks: Vec<crate::pipeline::ast_unresolved::InlineDdlSpec>,
    ) -> Result<()> {
        crate::pipeline::inline_ddl::register_prompt_blocks(blocks, self)
    }

    fn execute_effects(
        &mut self,
        query: crate::pipeline::ast_unresolved::Query,
        scope: &str,
        identities: &std::rc::Rc<crate::names::Registry>,
    ) -> Result<crate::pipeline::ast_unresolved::Query> {
        crate::pipeline::effect_executor::execute_effects(query, scope, self, identities)
    }

    fn set_entity_docs_atomic(
        &mut self,
        entries: &[(String, String)],
    ) -> Result<Vec<(String, String)>> {
        DelightQLSystem::set_entity_docs_atomic(self, entries)
    }

    fn observe_effect_plan(
        &mut self,
        _plan: &crate::pipeline::compiled_query::TypedEffectPlan,
    ) -> Result<()> {
        self.require(
            crate::host::Capability::ExternalEffects,
            "effect-plan catalog observation",
        )
    }

    fn reconcile_created_objects(
        &mut self,
        _objects: &[crate::pipeline::compiled_query::PlanCreatedObject],
    ) -> Result<crate::host::CreatedObjectReconciliation> {
        self.require(
            crate::host::Capability::ExternalEffects,
            "created-object catalog reconciliation",
        )?;
        unreachable!("a supplied external-effects host has a reconciliation implementation")
    }
}
