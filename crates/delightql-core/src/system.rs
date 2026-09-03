// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DelightQL System Management
//!
//! This module provides the `DelightQLSystem` struct which encapsulates
//! the user database connection and the internal _bootstrap metadata store.

use crate::bootstrap::SourceType;
use crate::bootstrap::{
    setup_assertions_table_on_bootstrap, setup_danger_table_on_bootstrap,
    setup_finding_table_on_bootstrap,
};
use crate::enums::EntityType;
use crate::error::{DelightQLError, Result};
use crate::external_effects::{
    CompensationFailure, CreatedFilePriorState, CreatedObjectCatalog, CreatedObjectReadback,
    CreatedObjectRegistration, ExternalEffect, HealthIncident, LiminalCatalogBoundary,
    LiminalClose, LiminalFileOps, ObjectExistence, RealLiminalCatalogBoundary, RealLiminalFileOps,
    RegistrationOutcome, SessionHealth,
};
use delightql_types::{
    schema::DatabaseSchema, ConnectionComponents, ConnectionFactory, DatabaseConnection,
};
use log::debug;
use rusqlite::{Connection, OptionalExtension};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// The user-selected target connection. Imported and bootstrap connections
/// receive distinct identities; an absent route also defaults here.
pub(crate) const PRIMARY_CONNECTION_ID: i64 = 2;

/// EVIDENCE OF ONE LEXICAL-EDGE ACT, WHOLE. Minted only by the act that
/// performed it — [`PreparedLoad::enlist`], [`PreparedLoad::alias`],
/// [`PreparedLoad::expose`], through this module's private acts — carrying
/// the KIND the act performed, the shorthand it registered (an alias), and
/// the namespace it selected. The value never leaves this module: an act
/// records it in the load it was performed for, so a holder can neither
/// reclassify an enlistment as an exposure, pair an alias target with a
/// shorthand its act did not register, drop it, nor move it to another
/// load — what a load declares is exactly what its acts performed.
#[derive(Debug)]
struct DeclaredEdge(LexicalAct);

#[derive(Debug)]
enum LexicalAct {
    Enlist { target: i64 },
    Alias { shorthand: String, target: i64 },
    Expose { target: i64 },
}

/// THE LOAD ONE LIMINAL PROGRAM EXECUTION CONSTRUCTS: its destination
/// namespace, its definitions, its ledger rows, its deferred `doc!`s, and
/// the edges its directive acts answered with, in authored order. The
/// walk builds it through the mutators below; publication SPENDS it —
/// [`DelightQLSystem::publish`] takes it by value and answers with the
/// [`PublishedLoad`] — so a load is published exactly once, for the
/// destination and under the publication semantics it owns.
/// WHERE A LOAD COMES FROM — fixed when the load is begun, never chosen at
/// publication. A file load's path names its cartridge; an inline block (a
/// scratch namespace's `(~~ddl ~~)`) has no file, and it alone receives the
/// session's ambient data world, as the scratch law grants.
enum LoadSource {
    File { path: String },
    Inline,
}

/// HOW A LOAD LANDS — fixed when the load is begun. A fresh consultation
/// registers into its namespace as it stands; a replacement first deletes
/// the namespace's current load whole, then rebuilds every derived world
/// that depends on it, inside the same transaction.
enum LoadMode {
    Fresh,
    Replacement,
}

pub(crate) struct PreparedLoad {
    namespace: String,
    source: LoadSource,
    mode: LoadMode,
    rows: Vec<crate::bin_cartridge::prelude::consult::PreparedRow>,
    definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    deferred_docs: Vec<(String, String)>,
    edges: Vec<DeclaredEdge>,
}

impl PreparedLoad {
    fn empty(namespace: &str, source: LoadSource, mode: LoadMode) -> Self {
        PreparedLoad {
            namespace: namespace.to_string(),
            source,
            mode,
            rows: Vec::new(),
            definitions: Vec::new(),
            deferred_docs: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// An empty load bound for `namespace`, from the file at `path`, for
    /// the liminal walk to fill — fresh, or the replacement of the
    /// namespace's current load, as the walk's own mode says.
    pub(crate) fn from_file(
        namespace: &str,
        path: &str,
        mode: crate::bin_cartridge::prelude::consult::LiminalDirectiveMode,
    ) -> Self {
        use crate::bin_cartridge::prelude::consult::LiminalDirectiveMode;
        let mode = match mode {
            LiminalDirectiveMode::Fresh => LoadMode::Fresh,
            LiminalDirectiveMode::Replay => LoadMode::Replacement,
        };
        Self::empty(
            namespace,
            LoadSource::File {
                path: path.to_string(),
            },
            mode,
        )
    }

    /// A load with no liminal space — an inline DDL block: definitions
    /// only, no ledger, no docs, no edges; fresh into its scratch
    /// namespace.
    pub(crate) fn inline(
        namespace: &str,
        definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
    ) -> Self {
        let mut load = Self::empty(namespace, LoadSource::Inline, LoadMode::Fresh);
        load.definitions = definitions;
        load
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

    /// THE LEXICAL-EDGE ACTS ARE THE LOAD'S. Each performs the session
    /// effect the directive means and records the edge it performed in
    /// THIS load, in one step: the act cannot succeed without changing the
    /// load under construction, and its evidence exists nowhere else to be
    /// dropped or attributed to another load.
    pub(crate) fn enlist(&mut self, system: &mut DelightQLSystem, target: &str) -> Result<()> {
        self.edges.push(system.perform_enlist(target)?);
        Ok(())
    }

    pub(crate) fn alias(
        &mut self,
        system: &mut DelightQLSystem,
        shorthand: &str,
        target: &str,
    ) -> Result<()> {
        self.edges.push(system.perform_alias(shorthand, target)?);
        Ok(())
    }

    pub(crate) fn expose(&mut self, system: &DelightQLSystem, child_fq: &str) -> Result<()> {
        let edge = system.perform_expose(&self.namespace, child_fq)?;
        self.edges.push(edge);
        Ok(())
    }

    /// SPEND THE LOAD into the catalog on `conn`, inside the publication
    /// transaction: register its definitions under the cartridge its source
    /// names, apply its `doc!`s, and record its declared edges — checking
    /// that each selected target still stands and that an exposure names a
    /// child (the facade law), selecting nothing again. Consumes the load;
    /// the answer is the proof that the complete load — families AND
    /// lexical graph — stands together.
    fn spend_on(self, conn: &Connection, default_data_ns: Option<&str>) -> Result<PublishedLoad> {
        let PreparedLoad {
            namespace,
            source,
            mode,
            rows,
            definitions,
            deferred_docs,
            edges,
        } = self;
        let path = match &source {
            LoadSource::File { path } => path.as_str(),
            LoadSource::Inline => "(inline)",
        };
        let replacing = matches!(mode, LoadMode::Replacement);
        let count = definitions.len();
        let registered = DelightQLSystem::consult_file_inner(
            conn,
            path,
            &namespace,
            definitions,
            count,
            default_data_ns,
            replacing,
        )?;
        for (target, doc) in &deferred_docs {
            let candidates = [
                format!("{}.{}", namespace, target),
                format!("{}.{}!", namespace, target),
                target.clone(),
            ];
            let mut last_err = None;
            let mut done = false;
            for candidate in &candidates {
                match DelightQLSystem::set_entity_doc_on(conn, candidate, doc) {
                    Ok(()) => {
                        done = true;
                        break;
                    }
                    Err(e) => last_err = Some(e),
                }
            }
            if !done {
                return Err(last_err.expect("candidates is non-empty"));
            }
        }
        let namespace_id: i64 = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [&namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error("namespace lookup for declared graph", e.to_string())
            })?;
        record_declared_edges_on(conn, namespace_id, &namespace, edges)?;
        Ok(PublishedLoad {
            namespace_id,
            definitions_loaded: registered.definitions_loaded,
            replaced_entities: registered.replaced_entities,
            rows,
        })
    }
}

/// Record a spent load's declared edges as namespace-local edges. Every
/// edge is an act's whole answer; this checks only that its selected
/// target still stands (a load that destroyed what it enlisted refuses)
/// and, for an exposure, the facade law — then writes by identity.
fn record_declared_edges_on(
    conn: &Connection,
    namespace_id: i64,
    namespace: &str,
    edges: Vec<DeclaredEdge>,
) -> Result<()> {
    let standing = |target: i64, edge: &str| -> Result<String> {
        conn.query_row(
            "SELECT fq_name FROM namespace WHERE id = ?1",
            [target],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| DelightQLError::database_error("selected target lookup", e.to_string()))?
        .ok_or_else(|| {
            DelightQLError::database_error(
                format!(
                    "'{namespace}' declares {edge} of a namespace its own load no longer \
                     holds — the target the directive selected was destroyed before \
                     publication"
                ),
                "declared target destroyed",
            )
        })
    };
    for DeclaredEdge(act) in edges {
        match act {
            LexicalAct::Enlist { target } => {
                standing(target, "an enlistment")?;
                conn.execute(
                    "INSERT OR IGNORE INTO namespace_local_enlist \
                     (namespace_id, enlisted_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![namespace_id, target],
                )
                .map_err(|e| {
                    DelightQLError::database_error("record namespace_local_enlist", e.to_string())
                })?;
            }
            LexicalAct::Alias { shorthand, target } => {
                standing(target, "an alias")?;
                conn.execute(
                    "INSERT OR IGNORE INTO namespace_local_alias \
                     (namespace_id, alias, target_namespace_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![namespace_id, shorthand, target],
                )
                .map_err(|e| {
                    DelightQLError::database_error("record namespace_local_alias", e.to_string())
                })?;
            }
            LexicalAct::Expose { target } => {
                let target_fq = standing(target, "an exposure")?;
                if !target_fq.starts_with(&format!("{namespace}::")) {
                    return Err(DelightQLError::database_error(
                        format!(
                            "Cannot expose '{target_fq}' through '{namespace}': not a child \
                             namespace"
                        ),
                        "Invalid expose target",
                    ));
                }
                conn.execute(
                    "INSERT OR IGNORE INTO exposed_namespace \
                     (exposing_namespace_id, exposed_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![namespace_id, target],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to expose namespace '{target_fq}': {e}"),
                        e.to_string(),
                    )
                })?;
            }
        }
    }
    Ok(())
}

pub(crate) struct ConsultResult {
    /// Number of definitions loaded.
    pub definitions_loaded: usize,
    /// Entity names that were replaced (non-empty only for inline DDL drop-and-replace).
    pub replaced_entities: Vec<String>,
}

/// One liminal-directive receipt row. The row schema is `success` (always
/// 1 — session directives never answer NO), `operation` (the directive's
/// name as written, with `!`), then the named echoes per the §8 table; echo
/// VALUES are the arguments as written in the file (receipts echo
/// parameters — compile-time constants, §3).
/// Pinned by `liminal_receipt_columns_follow_the_ruled_table` and the
/// effects-ball liminal--45 baseline.
#[derive(Debug, Clone)]
pub(crate) struct LiminalReceipt {
    /// Directive name as written, with the `!` (e.g. `"enlist!"`).
    pub operation: String,
    /// Echo columns in receipt order: (column name per the §8 table, value
    /// as written — `None` renders as SQL NULL, e.g. `enlist!`'s plain-form
    /// `into` or `reconsult!` re-reading the same file).
    pub echoes: Vec<(String, Option<String>)>,
}

/// One row of a consulted file's liminal ledger — THE LIMINAL RELATION, the
/// account of the load.
///
/// THE LEDGER IS A TAGGED SUM: the `operation` column is the tag and it
/// licenses the row's declared additions, so the three families are three
/// members here rather than one receipt shape with optional columns. Rows
/// are collected by the liminal executor
/// (bin_cartridge/prelude/consult.rs) in file-appearance order — one per
/// TOP-LEVEL FORM — and persisted inside the consultation, so an aborted
/// load rolls the ledger away with the namespace.
#[derive(Debug, Clone)]
pub(crate) enum LiminalRow {
    /// A session directive's receipt.
    Directive(LiminalReceipt),
    /// THE DEFINE ROW: `operation = "DEFINE"` — a FORM tag, the one
    /// non-directive tag — and the declared addition `entity`, the defined
    /// functor's canonical spelling. One row per defined entity, however
    /// many clauses spelled it, at its first clause's position.
    Define { entity: String },
    /// A relational goal's WITNESS: `operation = "GOAL"`, YES/NO in `met`,
    /// and `goal` — the body's spelling, so a ledger scan knows which goal
    /// was which.
    Goal { met: bool, goal: String },
}

impl LiminalRow {
    /// The tag: what family this row belongs to.
    pub fn operation(&self) -> &str {
        match self {
            LiminalRow::Directive(receipt) => &receipt.operation,
            LiminalRow::Define { .. } => "DEFINE",
            LiminalRow::Goal { .. } => "GOAL",
        }
    }

    /// The ordered declared-addition names (drives the ledger's
    /// corresponding-union presentation schema at drill time).
    fn addition_names(&self) -> Vec<&str> {
        match self {
            LiminalRow::Directive(receipt) => {
                receipt.echoes.iter().map(|(k, _)| k.as_str()).collect()
            }
            LiminalRow::Define { .. } => vec!["entity"],
            LiminalRow::Goal { .. } => vec!["met", "goal"],
        }
    }

    /// The ordered declared-addition names as a JSON array.
    pub fn echoes_json(&self) -> String {
        serde_json::to_string(&self.addition_names()).expect("addition names serialize")
    }

    /// The row as a JSON object: success, operation, then the declared
    /// additions the tag licenses. `met` is written as an INTEGER, so a
    /// ledger scan comparing `met = 1` reads a number and not its spelling.
    pub fn receipt_json(&self) -> String {
        let mut obj = String::from("{\"success\":1,\"operation\":");
        obj.push_str(&serde_json::to_string(self.operation()).expect("operation serializes"));
        let mut member = |name: &str, value: &str| {
            obj.push(',');
            obj.push_str(&serde_json::to_string(name).expect("addition name serializes"));
            obj.push(':');
            obj.push_str(value);
        };
        match self {
            LiminalRow::Directive(receipt) => {
                for (name, value) in &receipt.echoes {
                    let rendered = match value {
                        Some(v) => serde_json::to_string(v).expect("echo value serializes"),
                        None => "null".to_string(),
                    };
                    member(name, &rendered);
                }
            }
            LiminalRow::Define { entity } => {
                let rendered = serde_json::to_string(entity).expect("entity serializes");
                member("entity", &rendered);
            }
            LiminalRow::Goal { met, goal } => {
                member("met", if *met { "1" } else { "0" });
                let rendered = serde_json::to_string(goal).expect("goal serializes");
                member("goal", &rendered);
            }
        }
        obj.push('}');
        obj
    }
}

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
pub(crate) struct DelightQLSystem {
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
    /// Wrapped in Arc so it can be shared with transformer without cloning
    bin_registry: Arc<crate::bin_cartridge::registry::BinCartridgeRegistry>,

    /// Host-bound static database images for `delightql-bytes://` mounts.
    /// Names are bound once by the host via `bind_static_bytes` and are
    /// immutable for the life of the handle; the locator resolves ONLY
    /// names in this table (no ambient authority).
    byte_bindings: HashMap<String, ByteBinding>,

    /// When true, the namespace resolver is authoritative: `Ok(None)` from
    /// `resolve_unqualified_entity` means the entity genuinely isn't enlisted.
    /// When false (pipe/SISO connections), namespace resolution is a stub and
    /// raw database lookup should be used as a fallback.
    pub namespace_authoritative: bool,

    /// Factory for creating connections from URIs (injected by CLI).
    /// Enables import! to handle delightql-siso:// and other URI schemes.
    connection_factory: Option<Box<dyn ConnectionFactory>>,

    /// Schema map: connection_id → DatabaseSchema for imported connections.
    /// The primary connection schema is in `self.schema`; this holds schemas
    /// for connections created via import!/ConnectionFactory.
    schema_map: HashMap<i64, Box<dyn DatabaseSchema>>,

    /// Cartridge ID for catalog wrapper views in sys::meta.
    /// Lazily initialized on first access to catalog features.
    catalog_cartridge_id: Cell<Option<i32>>,

    /// Database type string ("sqlite", "duckdb", "postgres").
    /// Stored for reinit_bootstrap() to re-register the user connection.
    db_type: String,

    /// Monotonic count of Effect-Executor effects (pseudo-predicates /
    /// directive terminals) that have actually executed. `run_seed_program`
    /// snapshots this around each seed statement to detect no-op statements:
    /// a seed statement that produces zero effects is a typo by definition
    /// (a mistyped directive parses as a plain table read and is silently
    /// discarded — a quiet failure invisible without this counter). Pinned
    /// by the RED unit test `seed_no_effect_statement_is_refused`.
    effects_executed: Cell<u64>,

    /// True once `register_run_created_objects_with` has registered anything on a
    /// `session://materialized` cartridge. Gates the shadow-split probe in
    /// qualified resolution (`session_shadow_split`) so sessions that never
    /// ran a DDL directive pay zero extra bootstrap queries. Never reset:
    /// a stale `true` only costs the probe query, never correctness.
    session_materialized_names: Cell<bool>,

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
}

/// What kind of liminal program owns the current atomic boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiminalProgramKind {
    /// consult! / consult_tree! — a load;
    /// pre-program namespaces are strictly read-only for it.
    Consult,
    /// reconsult! — a reload; nested reloads of pre-existing CHILDREN are
    /// the tree-reload semantics and stay allowed (documented residue:
    /// they are reload-from-source, not compensable state).
    Reconsult,
}

/// State owned by the outermost liminal program. `namespace_mark` remains a
/// policy boundary (for operations deliberately forbidden against namespaces
/// that predate the program), not a substitute for transactional rollback.
#[derive(Debug)]
struct ProgramContext {
    namespace_mark: i64,
    kind: LiminalProgramKind,
    external_effects: Vec<ExternalEffect>,
}

/// A nestable catalog transaction. SQLite SAVEPOINT works both at the top
/// level and inside the outer liminal-program savepoint, unlike `BEGIN`.
/// Uncommitted instances roll back on drop, so early `?`/`return Err` paths
/// cannot forget cleanup.
struct CatalogSavepoint<'a> {
    conn: &'a Connection,
    name: &'static str,
    active: bool,
}

/// THE PROOF THAT A LOAD IS PUBLISHED COMPLETE: its families AND its
/// declared lexical graph (local enlistments, aliases, exposures, docs)
/// stand in the catalog together, inside the load's transaction. Minted
/// only by [`DelightQLSystem::publish`], which spent the load; the
/// derived-world rebuild accepts nothing else, so a dependent world can
/// never derive from a source whose edges are still to come. It carries
/// the ledger rows the load prepared, for the witnesses that run after
/// publication.
pub(crate) struct PublishedLoad {
    namespace_id: i64,
    definitions_loaded: usize,
    replaced_entities: Vec<String>,
    rows: Vec<crate::bin_cartridge::prelude::consult::PreparedRow>,
}

impl PublishedLoad {
    pub(crate) fn namespace_id(&self) -> i64 {
        self.namespace_id
    }

    pub(crate) fn definitions_loaded(&self) -> usize {
        self.definitions_loaded
    }

    /// Entity names an inline block replaced (drop-and-replace).
    pub(crate) fn replaced_entities(&self) -> &[String] {
        &self.replaced_entities
    }

    /// The ledger rows the load prepared, for the witnesses that run once
    /// the load stands.
    pub(crate) fn into_ledger(self) -> Vec<crate::bin_cartridge::prelude::consult::PreparedRow> {
        self.rows
    }
}

impl<'a> CatalogSavepoint<'a> {
    fn begin(conn: &'a Connection, name: &'static str, context: &str) -> Result<Self> {
        conn.execute_batch(&format!("SAVEPOINT {name}"))
            .map_err(|e| DelightQLError::database_error(context, e.to_string()))?;
        Ok(Self {
            conn,
            name,
            active: true,
        })
    }

    fn commit(mut self, context: &str) -> Result<()> {
        self.conn
            .execute_batch(&format!("RELEASE SAVEPOINT {}", self.name))
            .map_err(|e| DelightQLError::database_error(context, e.to_string()))?;
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

/// Embedded DQL source for the sys::meta generator HO view.
/// This is the sole definition of the catalog functor join logic.
const SYS_META_SOURCE: &str = include_str!("../autoload/sys/meta.dql");

/// Register the session's finding table as `sys::diagnostics.finding`.
/// Rows are written by [`DelightQLSystem::record_finding`]; the relation
/// is read-only from DQL like every bootstrap relation.
fn register_sys_diagnostics_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://diagnostics', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create sys::diagnostics cartridge: {}", e),
                e.to_string(),
            )
        })?;
    let cartridge_id = bootstrap_conn.last_insert_rowid() as i32;
    let ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::diagnostics'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::diagnostics namespace: {}", e),
                e.to_string(),
            )
        })?;
    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES ('finding', 10, ?1)",
            rusqlite::params![cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::diagnostics.finding entity: {}", e),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;
    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, '-- sys::diagnostics.finding: the session''s refusals and findings')",
            rusqlite::params![entity_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::diagnostics.finding clause: {}", e),
                e.to_string(),
            )
        })?;
    let columns: &[(&str, &str, i32, bool)] = &[
        ("id", "INTEGER", 1, false),
        ("occurred_at", "TEXT", 2, false),
        ("kind", "TEXT", 3, false),
        ("uri", "TEXT", 4, false),
        ("message", "TEXT", 5, false),
        ("input", "TEXT", 6, true),
        ("provider", "TEXT", 7, false),
    ];
    for (name, data_type, position, nullable) in columns {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![entity_id, name, data_type, position, nullable],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys::diagnostics.finding column '{name}': {e}"),
                    e.to_string(),
                )
            })?;
    }
    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, ns_id, cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys::diagnostics.finding: {}", e),
                e.to_string(),
            )
        })?;
    Ok(())
}

/// Register a thin catalog wrapper view for a namespace in sys::meta.
///
/// Creates an entity like `main::` with definition `sys::meta.generator("main")(*)`
/// so that `main::(*)` resolves through normal HO view expansion.
/// Register the engine-owned identifier registry as
/// `sys::identifiers.identifier`. The burned rows live in
/// bootstrap/schema.sql; CLI-shaped facts belong to the host instead.
fn register_sys_identifier_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://identifiers', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create sys::identifiers cartridge: {}", e),
                e.to_string(),
            )
        })?;
    let identifiers_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    let identifiers_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::identifiers'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::identifiers namespace: {}", e),
                e.to_string(),
            )
        })?;

    // (table/entity name, columns as (name, sqlite type, nullable))
    type Col = (&'static str, &'static str, bool);
    let tables: &[(&str, &[Col])] = &[(
        "identifier",
        &[
            ("kind", "TEXT", false),
            ("hierarchy", "TEXT", false),
            ("summary", "TEXT", false),
            ("explanation", "TEXT", false),
        ],
    )];

    for (table, columns) in tables {
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 10, ?2)",
                rusqlite::params![table, identifiers_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys::identifiers.{} entity: {}", table, e),
                    e.to_string(),
                )
            })?;
        let entity_id = bootstrap_conn.last_insert_rowid() as i32;

        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- engine-owned identifier registry')",
                rusqlite::params![entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys::identifiers.{} clause: {}", table, e),
                    e.to_string(),
                )
            })?;

        for (position, (col_name, data_type, nullable)) in columns.iter().enumerate() {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![
                        entity_id,
                        col_name,
                        data_type,
                        (position + 1) as i32,
                        *nullable
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to insert sys::identifiers.{} column '{}': {}",
                            table, col_name, e
                        ),
                        e.to_string(),
                    )
                })?;
        }

        bootstrap_conn
            .execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                rusqlite::params![entity_id, identifiers_ns_id, identifiers_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to activate sys::identifiers.{}: {}", table, e),
                    e.to_string(),
                )
            })?;
    }

    Ok(())
}

/// Register the burned formatter style-bundle table as
/// `sys::format.bundle`. The physical table and its 'book' row live in
/// bootstrap/schema.sql; the column list here mirrors the formatter's
/// knob registry plus the leading `bundle` key. Its own cartridge so
/// bulk activation cannot leak it into bare `sys`.
fn register_sys_format_table(bootstrap_conn: &Connection, bootstrap_conn_id: i64) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://format', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create sys::format cartridge: {}", e),
                e.to_string(),
            )
        })?;
    let format_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    let format_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::format'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::format namespace: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES ('bundle', 10, ?1)",
            rusqlite::params![format_cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::format.bundle entity: {}", e),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;

    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, '-- formatter style bundles (book row = frozen defaults)')",
            rusqlite::params![entity_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::format.bundle clause: {}", e),
                e.to_string(),
            )
        })?;

    let columns: &[(&str, &str, bool)] = &[
        ("bundle", "TEXT", false),
        ("projection_length", "INTEGER", true),
        ("continuation_length", "INTEGER", true),
        ("pipe_indent", "INTEGER", true),
        ("continuation_indent", "INTEGER", true),
        ("map_cover_extra_indent", "INTEGER", true),
        ("aggregation_arrow_indent", "INTEGER", true),
        ("cte_indent", "INTEGER", true),
        ("cte_columnar_padding", "INTEGER", true),
        ("curly_member_indent", "INTEGER", true),
        ("curly_inducer_indent", "INTEGER", true),
        ("case_arm_indent", "INTEGER", true),
        ("pipe_break_width", "INTEGER", true),
        ("member_landing_pad", "INTEGER", true),
        ("pipe_break", "TEXT", true),
        ("comma_clause_break", "TEXT", true),
        ("comma_join_args", "TEXT", true),
        ("brace_padding", "TEXT", true),
        ("member_landing", "TEXT", true),
        ("closer_placement", "TEXT", true),
        ("tree_inducer_break", "TEXT", true),
        ("member_value_break", "TEXT", true),
        ("annotation_placement", "TEXT", true),
        ("blank_lines", "TEXT", true),
        ("cte_style", "TEXT", true),
        ("curly_opening_brace_inline", "INTEGER", true),
    ];
    for (position, (col_name, data_type, nullable)) in columns.iter().enumerate() {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![
                    entity_id,
                    col_name,
                    data_type,
                    (position + 1) as i32,
                    *nullable
                ],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to insert sys::format.bundle column '{}': {}",
                        col_name, e
                    ),
                    e.to_string(),
                )
            })?;
    }

    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, format_ns_id, format_cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys::format.bundle: {}", e),
                e.to_string(),
            )
        })?;

    Ok(())
}

/// Register the CURATED `connection` entity in sys::connections.
///
/// Register the `connection` entity in sys::connections as an explicit column
/// ALLOWLIST. Under the credential-sourcing policy (credentials come from the
/// environment, never embedded in a URI) no column
/// of `connection` carries a secret: `resource_uri` is guaranteed
/// credential-free, and `identity` is a resource fingerprint (what the
/// resource asserts about itself, for idempotent-mount / conflict detection),
/// not a credential. So every column is exposed and answers "what am I
/// connected to?".
///
/// It stays a curated, explicitly-enumerated entity (not the raw introspected
/// twin) so the exposure is DEFAULT-DENY: a column added to the physical table
/// later is NOT surfaced unless deliberately added here — the structural belt
/// to the policy's suspenders. The resolver guard in registry.rs makes these
/// registered attributes authoritative for bootstrap (connection_id==1)
/// tables. The raw introspected `connection` entity (cartridge 1) is left
/// orphaned; the `catalog` diagnostic dedups by name, so this activation
/// clears its warning. Own cartridge so no bulk activation sweeps it into bare
/// `sys`.
fn register_sys_connection_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://connections', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create sys::connections cartridge: {}", e),
                e.to_string(),
            )
        })?;
    let conn_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    let conn_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::connections'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::connections namespace: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES ('connection', 10, ?1)",
            rusqlite::params![conn_cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::connections.connection entity: {}", e),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;

    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, '-- sys::connections curated safe subset')",
            rusqlite::params![entity_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::connections.connection clause: {}", e),
                e.to_string(),
            )
        })?;

    // Explicit allowlist of all current columns (none is a secret under the
    // credential-sourcing policy). Enumerated, not raw, so a future column is
    // default-deny. Order + nullability mirror the physical `connection` table.
    let connection_columns = [
        ("id", "INTEGER", 1, false),
        ("resource_uri", "TEXT", 2, false),
        ("mechanism", "TEXT", 3, false),
        ("identity", "TEXT", 4, true),
        ("connection_type", "INTEGER", 5, false),
        ("description", "TEXT", 6, true),
    ];
    for (col_name, data_type, position, nullable) in &connection_columns {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![entity_id, col_name, data_type, position, nullable],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to insert sys::connections.connection column '{}': {}",
                        col_name, e
                    ),
                    e.to_string(),
                )
            })?;
    }

    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, conn_ns_id, conn_cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys::connections.connection: {}", e),
                e.to_string(),
            )
        })?;

    Ok(())
}

/// Register ONE curated sys::ns catalog relation (the sys::connections
/// precedent): an explicit column
/// ALLOWLIST entity over a physical bootstrap table, so the public shape is
/// deliberate and a column added to the physical table later is
/// default-deny. The raw introspected entity stays orphaned by design —
/// bootstrap tables are not bulk-activated — and the shared implementation
/// exists so every curated relation gets identical registration mechanics.
fn register_curated_sys_ns_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
    table_name: &str,
    clause_comment: &str,
    columns: &[(&str, &str, i32, bool)],
) -> Result<()> {
    // ONE sys://ns cartridge shared by every curated relation — created on
    // the first registration, reused after.
    let existing: Option<i32> = bootstrap_conn
        .query_row(
            "SELECT id FROM cartridge WHERE source_uri = 'sys://ns' AND connection_id = ?1",
            [bootstrap_conn_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::ns cartridge for '{table_name}': {e}"),
                e.to_string(),
            )
        })?;
    let ns_cartridge_id = match existing {
        Some(id) => id,
        None => {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, 'sys://ns', NULL, 1, ?3, 0)",
                    rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to create sys::ns cartridge for '{table_name}': {e}"),
                        e.to_string(),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        }
    };

    let ns_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::ns'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::ns namespace: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 10, ?2)",
            rusqlite::params![table_name, ns_cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::ns.{table_name} entity: {e}"),
                e.to_string(),
            )
        })?;
    let entity_id = bootstrap_conn.last_insert_rowid() as i32;

    bootstrap_conn
        .execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition)
             VALUES (?1, 1, ?2)",
            rusqlite::params![entity_id, clause_comment],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert sys::ns.{table_name} clause: {e}"),
                e.to_string(),
            )
        })?;

    for (col_name, data_type, position, nullable) in columns {
        bootstrap_conn
            .execute(
                "INSERT INTO entity_attribute
                 (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                 VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                rusqlite::params![entity_id, col_name, data_type, position, nullable],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys::ns.{table_name} column '{col_name}': {e}"),
                    e.to_string(),
                )
            })?;
    }

    bootstrap_conn
        .execute(
            "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
            rusqlite::params![entity_id, ns_ns_id, ns_cartridge_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys::ns.{table_name}: {e}"),
                e.to_string(),
            )
        })?;

    Ok(())
}

/// Register the CURATED sys::ns relations: `namespace` (the ratified public
/// shape) and `mount` (mount identity, queryable deliberately). A
/// consulted namespace's provenance is its `source_path`; the catalog
/// keeps no load history to expose.
fn register_sys_ns_namespace_table(
    bootstrap_conn: &Connection,
    bootstrap_conn_id: i64,
) -> Result<()> {
    // Exactly the columns namespace(*) has always shown. Mount identity is
    // deliberately absent HERE — it lives in its own curated relation below,
    // not as columns grafted onto namespace.
    register_curated_sys_ns_table(
        bootstrap_conn,
        bootstrap_conn_id,
        "namespace",
        "-- sys::ns curated public columns",
        &[
            ("id", "INTEGER", 1, false),
            ("name", "TEXT", 2, false),
            ("pid", "INTEGER", 3, true),
            ("fq_name", "TEXT", 4, true),
            ("default_data_ns", "TEXT", 5, true),
            ("kind", "TEXT", 6, false),
            ("provenance", "TEXT", 7, true),
            ("source_path", "TEXT", 8, true),
            ("writable", "INTEGER", 9, false),
        ],
    )?;
    register_curated_sys_ns_table(
        bootstrap_conn,
        bootstrap_conn_id,
        "mount",
        "-- sys::ns.mount curated public columns",
        &[
            ("namespace_id", "INTEGER", 1, false),
            ("cartridge_id", "INTEGER", 2, false),
            ("attach_alias", "TEXT", 3, true),
            ("attachment", "TEXT", 4, true),
            ("qualification", "TEXT", 5, false),
            ("engine_schema", "TEXT", 6, true),
            ("class", "TEXT", 7, false),
        ],
    )?;
    Ok(())
}

fn register_catalog_wrapper(
    conn: &Connection,
    ns_fq: &str,
    sys_meta_ns_id: i32,
    cartridge_id: i32,
) -> Result<()> {
    let entity_name = format!("{}::", ns_fq);
    // The wrapper is addressed by its entity name (`ns::`); the clause
    // subject is never how it is reached. Exact `_` is reserved deixis and
    // refuses at definition admission, so the stored subject is an ordinary
    // longer-underscore compiler spelling.
    let definition = format!(r#"_wrapper(*) :- sys::meta.generator("{}")(*)"#, ns_fq);

    // Catalog initialization registers wrappers for every namespace already
    // present. Mount paths also call this function after lazy initialization
    // so that later namespaces receive a wrapper. Make that overlap explicitly
    // idempotent: a namespace has exactly one wrapper in the catalog cartridge.
    let already_registered: bool = conn
        .query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE e.name = ?1
                   AND e.cartridge_id = ?2
                   AND ae.namespace_id = ?3
             )",
            rusqlite::params![&entity_name, cartridge_id, sys_meta_ns_id],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to check catalog wrapper '{}': {}", entity_name, e),
                e.to_string(),
            )
        })?;
    if already_registered {
        return Ok(());
    }

    // The wrapper is a compiler-synthesized definition family: an authored
    // KIND (the resolver opens its body), activated in sys::meta like any
    // other family.
    conn.execute(
        "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, ?2, ?3)",
        rusqlite::params![&entity_name, 4, cartridge_id], // type 4 = DqlTemporaryViewExpression
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Failed to insert catalog wrapper entity '{}': {}",
                entity_name, e
            ),
            e.to_string(),
        )
    })?;
    let entity_id = conn.last_insert_rowid() as i32;

    conn.execute(
        "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, ?2)",
        rusqlite::params![entity_id, &definition],
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Failed to insert catalog wrapper clause for '{}': {}",
                entity_name, e
            ),
            e.to_string(),
        )
    })?;

    conn.execute(
        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
        rusqlite::params![entity_id, sys_meta_ns_id, cartridge_id],
    )
    .map_err(|e| {
        DelightQLError::database_error(
            format!(
                "Failed to activate catalog wrapper '{}': {}",
                entity_name, e
            ),
            e.to_string(),
        )
    })?;

    debug!(
        "register_catalog_wrapper: Registered '{}' in sys::meta",
        entity_name
    );
    Ok(())
}

/// Materialize the namespace tree required by a mounted data leaf.
///
/// A qualified mount such as `cli::surface` is two catalog facts, not one flat
/// row whose `name` happens to contain `::`: `cli` is a structural container
/// parented to `_`, and `surface` is the data namespace parented to `cli`.
/// Existing ancestors are reused without changing their ownership kind (a data
/// mount may legitimately live below a consulted library namespace).
#[derive(Clone, Debug, PartialEq, Eq)]
struct MountBinding {
    namespace_id: i64,
    cartridge_id: i64,
    connection_id: i64,
    attach_alias: Option<String>,
    /// Who opened the schema this binding names — `Some("owned")` when this
    /// mount attached it, `Some("borrowed")` when it was already open.
    /// `None` for an external mount, which holds no attachment handle.
    attachment: Option<String>,
    qualification: String,
    engine_schema: Option<String>,
    class: String,
}

/// Who opened the physical schema a mount names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Attachment {
    /// This mount attached it, and teardown may detach it.
    Owned,
    /// It was already open. This mount named it and nothing more; closing it
    /// was never this binding's to do.
    Borrowed,
}

impl Attachment {
    fn as_str(self) -> &'static str {
        match self {
            Attachment::Owned => "owned",
            Attachment::Borrowed => "borrowed",
        }
    }
}

impl MountBinding {
    fn attach(
        namespace_id: i64,
        cartridge_id: i64,
        connection_id: i64,
        alias: impl Into<String>,
        attachment: Attachment,
        qualification: impl Into<String>,
    ) -> Self {
        Self {
            namespace_id,
            cartridge_id,
            connection_id,
            attach_alias: Some(alias.into()),
            attachment: Some(attachment.as_str().to_string()),
            qualification: qualification.into(),
            engine_schema: None,
            class: "attach".to_string(),
        }
    }

    /// The schema this binding's teardown may DETACH.
    ///
    /// Not the schema it NAMES: a binding that BORROWED an already-open
    /// schema never acquired the right to close it — the schema may be
    /// SQLite's own `main`, which cannot be detached at all, or another
    /// owner's attachment that is still being read.
    ///
    /// One answer, here, because there is more than one teardown road: the
    /// ordinary destroy and `main`'s empty-back-to-fixture. A road that
    /// decided this for itself would be a second authority, and the second
    /// one is the one that gets it wrong.
    fn detachable_alias(&self) -> Option<String> {
        match self.attachment.as_deref() {
            Some("borrowed") => None,
            _ => self.attach_alias.clone(),
        }
    }

    fn external(
        namespace_id: i64,
        cartridge_id: i64,
        connection_id: i64,
        engine_schema: Option<String>,
    ) -> Self {
        Self {
            namespace_id,
            cartridge_id,
            connection_id,
            attach_alias: None,
            attachment: None,
            qualification: if engine_schema.is_some() {
                "engine_schema".to_string()
            } else {
                "unqualified".to_string()
            },
            engine_schema,
            class: "external".to_string(),
        }
    }
}

/// Parameters for one attach-class mount, consumed by the shared spine
/// (`mount_attach_class`). The path-specific halves —
/// how to attach, and what counts as the same source — travel as closures.
struct AttachClassMount<'a> {
    /// Target namespace fq_name.
    namespace: &'a str,
    /// `cartridge.source_uri` (also the conflict-message spelling).
    source_uri: String,
    /// `namespace.source_path` value (file path or locator).
    source_path: String,
    /// `namespace.provenance` ("file" | "bytes").
    provenance: &'static str,
    /// sys::connections registration: resource / mechanism / identity.
    conn_resource: &'a str,
    conn_mechanism: &'static str,
    conn_identity: Option<String>,
    conn_description: String,
    /// The engine schema this resource is ALREADY open under on the session
    /// connection, when it is.
    ///
    /// A file may be the connection's own `main`, or already attached for
    /// another namespace. Attaching it a second time gives one connection two
    /// independent handles on one file, and SQLite refuses to let one of them
    /// write while the other is reading it — "database is locked", from a
    /// statement with no second party anywhere in sight. One resource, one
    /// schema: the namespace is the naming, and naming a file twice must not
    /// open it twice.
    existing_schema: Option<String>,
}

/// Guard for a freshly attached mount alias. Ordinary mount exits must call
/// `rollback` or `commit` explicitly; `Drop` is only an emergency backstop
/// for panic/unwinding and cannot be the required cleanup path.
struct DetachOnDrop<'a> {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
    alias: &'a str,
    armed: bool,
}

impl DetachOnDrop<'_> {
    fn commit(&mut self) {
        self.armed = false;
    }

    fn rollback(&mut self) -> Result<()> {
        if !self.armed {
            return Ok(());
        }
        let result = match self.connection.lock() {
            Ok(conn) => conn
                .execute(&format!("DETACH DATABASE '{}'", self.alias), &[])
                .map(|_| ())
                .map_err(|error| {
                    DelightQLError::database_error(
                        format!("Failed to detach mounted alias '{}'", self.alias),
                        error.to_string(),
                    )
                }),
            Err(error) => Err(DelightQLError::connection_poison_error(
                format!(
                    "Failed to acquire connection lock to detach '{}'",
                    self.alias
                ),
                format!("Connection was poisoned: {error}"),
            )),
        };
        // A failed explicit inverse is transferred to session health by the
        // caller. Disarm here so Drop never retries it; reset owns recovery.
        self.armed = false;
        result
    }
}

impl Drop for DetachOnDrop<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        match self.connection.lock() {
            Ok(conn) => {
                if let Err(e) = conn.execute(&format!("DETACH DATABASE '{}'", self.alias), &[]) {
                    debug!("mount cleanup: DETACH '{}' failed: {}", self.alias, e);
                }
            }
            Err(_) => debug!(
                "mount cleanup: connection lock poisoned; alias '{}' may leak",
                self.alias
            ),
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
            .map_err(|e| {
                DelightQLError::database_error("Failed to open bootstrap savepoint", e.to_string())
            })?;
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
                DelightQLError::database_error(
                    "Failed to release bootstrap savepoint",
                    e.to_string(),
                )
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

/// A host-bound database image: static rodata
/// (`include_bytes!`, referenced zero-copy at attach) or an owned
/// runtime-built buffer (copied into SQLite memory at attach). Both are
/// validated once, at bind time, in a scratch connection.
#[derive(Clone)]
pub(crate) enum ByteBinding {
    Static(&'static [u8]),
    Owned(std::sync::Arc<[u8]>),
}

/// Byte-binding names are lowercase capability labels: `[a-z][a-z0-9._-]*`.
fn valid_byte_binding_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
}

/// Validate a bound image ONCE, at bind time, in a scratch connection —
/// never on the session connection. sqlite3_deserialize installs a buffer
/// without validating it (validation is lazy, on first access), and a
/// garbage image POISONS the hosting connection: every later statement,
/// including the DETACH that would remove it, fails with "file is not a
/// database". Bindings are immutable, so one bind-time proof covers every
/// future mount.
fn validate_sqlite_image(name: &str, bytes: &[u8]) -> Result<()> {
    if bytes.len() < 100 || !bytes.starts_with(b"SQLite format 3\0") {
        return Err(DelightQLError::validation_error(
            format!(
                "byte binding '{}' is not a valid SQLite database image",
                name
            ),
            "delightql-bytes:// bindings must be complete SQLite images",
        ));
    }
    let mut scratch = Connection::open_in_memory().map_err(|e| {
        DelightQLError::database_error(
            "Failed to open scratch connection for image validation",
            e.to_string(),
        )
    })?;
    scratch
        .deserialize_read_exact("main", bytes, bytes.len(), true)
        .and_then(|_| {
            scratch.query_row("SELECT count(*) FROM sqlite_master", [], |row| {
                row.get::<_, i64>(0)
            })
        })
        .map_err(|e| {
            DelightQLError::validation_error(
                format!(
                    "byte binding '{}' is not a valid SQLite database image: {}",
                    name, e
                ),
                "delightql-bytes:// bindings must be complete SQLite images",
            )
        })?;
    Ok(())
}

fn create_mounted_namespace_path(
    conn: &Connection,
    namespace: &str,
    provenance: &str,
    source_path: &str,
) -> Result<(i32, Vec<String>)> {
    let mut specs =
        crate::import::namespace::parse_namespace_path(conn, namespace).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to construct namespace path '{}': {}", namespace, e),
                e.to_string(),
            )
        })?;

    let created_names: Vec<String> = specs.iter().map(|spec| spec.fq_name.clone()).collect();
    // Every segment this mount will CREATE must honor the
    // alias/namespace exclusivity invariant.
    for name in &created_names {
        ensure_namespace_available(conn, name)?;
    }
    for spec in &mut specs {
        if spec.fq_name == namespace {
            spec.kind = "data".into();
            spec.provenance = Some(provenance.into());
            spec.source_path = Some(source_path.into());
        } else {
            spec.kind = "container".into();
            spec.provenance = Some("mount".into());
            spec.source_path = None;
        }
    }

    crate::import::namespace::create_namespace_hierarchy(conn, &specs).map_err(|e| {
        DelightQLError::database_error(
            format!("Failed to create namespace path '{}': {}", namespace, e),
            e.to_string(),
        )
    })?;

    let leaf_id = if let Some(leaf) = specs.last() {
        leaf.id
    } else {
        conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [namespace],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to find namespace '{}': {}", namespace, e),
                e.to_string(),
            )
        })?
    };

    Ok((leaf_id, created_names))
}

fn register_mounted_catalog_wrappers(
    conn: &Connection,
    namespace: &str,
    created_names: &[String],
    sys_meta_ns_id: i32,
    catalog_id: i32,
) -> Result<()> {
    for fq_name in created_names {
        register_catalog_wrapper(conn, fq_name, sys_meta_ns_id, catalog_id)?;
    }
    // Reused empty leaves (notably `main`, and a former structural parent)
    // are absent from `created_names`; the idempotent registration covers both.
    register_catalog_wrapper(conn, namespace, sys_meta_ns_id, catalog_id)
}

/// RAII rollback for the imprint target transaction.
///
/// Holds a shared reference to the target connection (behind its `MutexGuard`)
/// and, unless `committed` is flipped, issues `ROLLBACK` on drop. This makes
/// every `?` early-return in the drop/create/CTAS sequence undo the whole
/// materialization automatically, so replace-mode cannot leave the old tables
/// destroyed with nothing in their place (pinned: `cli tests/imprint_atomicity.rs`).
/// `execute` takes `&self`, so re-borrowing the guarded connection
/// for the DDL statements alongside this borrow is sound.
struct TargetTxnGuard<'a> {
    conn: &'a dyn DatabaseConnection,
    committed: bool,
}

impl Drop for TargetTxnGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            let _ = self.conn.execute("ROLLBACK", &[]);
        }
    }
}

/// Next blueprint version under `target_ns_id`: `MAX(existing N) + 1` over
/// `_N_blueprint` children, NOT `COUNT`.
///
/// `COUNT` reuses an N whenever a `_N_blueprint` child was ever removed
/// (`namespace.fq_name` carries no UNIQUE constraint, so the resulting duplicate
/// is silent). Parsing the N and taking `MAX+1` is monotone
/// regardless of gaps. A failed query is a loud error (`?`) — `.unwrap_or(0)`
/// would silently produce a wrong version instead. The `GLOB` is a coarse
/// pre-filter; the Rust parse below is authoritative. Pinned by
/// `imprint_version_tests::next_blueprint_version_is_max_plus_one`.
//
// NOTE: a UNIQUE index on `namespace.fq_name` would make the collision
// impossible at the schema level; not added here.
fn next_blueprint_version(conn: &Connection, target_ns_id: i32) -> Result<i64> {
    let mut stmt = conn
        .prepare("SELECT name FROM namespace WHERE pid = ?1 AND name GLOB '_[0-9]*_blueprint'")
        .map_err(|e| {
            DelightQLError::database_error("prepare blueprint version scan", e.to_string())
        })?;
    let rows = stmt
        .query_map([target_ns_id], |r| r.get::<_, String>(0))
        .map_err(|e| DelightQLError::database_error("scan blueprint versions", e.to_string()))?;
    let mut max_n: Option<i64> = None;
    for name in rows {
        let name =
            name.map_err(|e| DelightQLError::database_error("read blueprint name", e.to_string()))?;
        // `name` is `_<N>_blueprint`; take the N between the leading `_` and the
        // `_blueprint` suffix. Anything that doesn't parse is ignored.
        if let Some(inner) = name
            .strip_prefix('_')
            .and_then(|s| s.strip_suffix("_blueprint"))
        {
            if let Ok(parsed) = inner.parse::<i64>() {
                max_n = Some(max_n.map_or(parsed, |m: i64| m.max(parsed)));
            }
        }
    }
    Ok(max_n.map_or(0, |m| m + 1))
}

/// Linear imprint: consume the source lib namespace into a versioned blueprint
/// archive under the target, freeing the source path.
///
/// `imprint!` is linear: after a successful
/// materialization the source is *moved, not destroyed* to
/// `{target}::_{N}_blueprint`. The move is a rename/re-parent of the source
/// namespace (and its `_internal`/descendants), which both vacates the original
/// path — so use-after-imprint errors and the path is free to re-consult
/// (delete-and-reuse) — and creates the archive. The archive is visible
/// (a catalog wrapper is registered for it) but inert (`kind='blueprint'`,
/// enlistment removed). Returns the blueprint fq_name.
#[allow(clippy::too_many_arguments)]
fn consume_source_to_blueprint(
    conn: &Connection,
    source_ns: &str,
    source_ns_id: i32,
    target_ns: &str,
    target_ns_id: i32,
    sys_meta_ns_id: i32,
    catalog_id: i32,
) -> Result<String> {
    // Version N = MAX(existing N)+1 over `_N_blueprint` children (loud on
    // query failure). See `next_blueprint_version` for why not COUNT.
    let n = next_blueprint_version(conn, target_ns_id)?;
    let bp_name = format!("_{}_blueprint", n);
    let bp_fq = format!("{}::{}", target_ns, bp_name);

    // Descendants (e.g. `_internal`), captured before renaming so we can rewrite
    // their fq_names / cartridges / catalog wrappers. Membership comes from an
    // EXACT string-prefix test in Rust (`starts_with("{src}::")`), NOT
    // `fq_name LIKE '{src}::%'`: `_`/`%` in a namespace name are LIKE
    // wildcards — that pattern kidnaps unrelated siblings (imprinting
    // `lib::a_b` also matches `lib::acb::…`), silently renaming them under the
    // blueprint. Pinned by companion_linear--69 (sibling untouched) and
    // --72 (descendants DO move). NOT a pid-recursive walk either: today every
    // consult-created namespace has pid = NULL (the hierarchy lives only in
    // fq_name), so a pid walk finds nothing and silently strands descendants
    // (`_internal`, nested consults) live at their old paths — the vacate half
    // of linearity breaks and re-consulting mints duplicate fq_names. If/when
    // pid becomes a real tree, a pid walk can replace this.
    let descendants: Vec<(i32, String)> = {
        let prefix = format!("{}::", source_ns);
        let mut stmt = conn
            .prepare("SELECT id, fq_name FROM namespace")
            .map_err(|e| DelightQLError::database_error("prepare descendants", e.to_string()))?;
        let rows = stmt
            .query_map([], |r| Ok((r.get::<_, i32>(0)?, r.get::<_, String>(1)?)))
            .map_err(|e| DelightQLError::database_error("query descendants", e.to_string()))?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| DelightQLError::database_error("read descendants", e.to_string()))?
            .into_iter()
            .filter(|(_, fq)| fq.starts_with(&prefix))
            .collect()
    };

    // Drop a namespace's sys::meta catalog wrapper (entity named `{fq}::`).
    // `?`-loud, not `let _ =`: consume runs inside imprint's bootstrap txn,
    // which rolls back cleanly on any error, so a failed wrapper cleanup
    // aborts the whole catalog update instead of leaving a half-renamed
    // blueprint behind — `let _ =` here would swallow that failure and let
    // the half-rename stand.
    let drop_wrapper = |wrapper_name: &str| -> Result<()> {
        conn.execute(
            "DELETE FROM activated_entity WHERE entity_id IN (SELECT id FROM entity WHERE name = ?1)",
            [wrapper_name],
        )
        .map_err(|e| {
            DelightQLError::database_error("drop wrapper activated_entity", e.to_string())
        })?;
        conn.execute(
            "DELETE FROM entity_clause WHERE entity_id IN (SELECT id FROM entity WHERE name = ?1)",
            [wrapper_name],
        )
        .map_err(|e| DelightQLError::database_error("drop wrapper entity_clause", e.to_string()))?;
        conn.execute("DELETE FROM entity WHERE name = ?1", [wrapper_name])
            .map_err(|e| DelightQLError::database_error("drop wrapper entity", e.to_string()))?;
        Ok(())
    };

    for (id, old_fq) in &descendants {
        // Membership is pid-verified, so `source_ns` is a genuine prefix; strip
        // it and re-root under the blueprint fq. `strip_prefix` (not byte
        // slicing) so a catalog inconsistency surfaces loudly, never panics.
        let suffix = old_fq.strip_prefix(source_ns).ok_or_else(|| {
            DelightQLError::database_error(
                "rename descendant ns",
                format!(
                    "descendant '{}' is not under source '{}'",
                    old_fq, source_ns
                ),
            )
        })?;
        let new_fq = format!("{}{}", bp_fq, suffix);
        conn.execute(
            "UPDATE namespace SET fq_name = ?1 WHERE id = ?2",
            rusqlite::params![new_fq, id],
        )
        .map_err(|e| DelightQLError::database_error("rename descendant ns", e.to_string()))?;
        conn.execute(
            "UPDATE cartridge SET source_ns = ?1 WHERE source_ns = ?2",
            rusqlite::params![new_fq, old_fq],
        )
        .map_err(|e| DelightQLError::database_error("move descendant cartridge", e.to_string()))?;
        drop_wrapper(&format!("{}::", old_fq))?;
    }

    // Root: rename, re-parent under target, mark inert.
    conn.execute(
        "UPDATE namespace SET name = ?1, fq_name = ?2, pid = ?3, kind = 'blueprint' WHERE id = ?4",
        rusqlite::params![bp_name, bp_fq, target_ns_id, source_ns_id],
    )
    .map_err(|e| DelightQLError::database_error("rename source ns to blueprint", e.to_string()))?;
    conn.execute(
        "UPDATE cartridge SET source_ns = ?1 WHERE source_ns = ?2",
        rusqlite::params![bp_fq, source_ns],
    )
    .map_err(|e| DelightQLError::database_error("move source cartridge", e.to_string()))?;
    drop_wrapper(&format!("{}::", source_ns))?;

    // Remove all enlistment of the consumed namespaces (root + descendants), in
    // BOTH directions, and clean enlisted_entity too — mirroring unmount (:4188).
    // enlist! writes the enlisted ns as `from_namespace_id` (:3665) and the
    // resolver serves it via `from_namespace_id` (resolution/registry.rs:597), so
    // `WHERE to_namespace_id = ?` alone deletes nothing: an enlisted source's
    // archived rules would stay resolvable UNQUALIFIED after imprint.
    // Pinned by companion_linear--68.
    let mut consumed_ns_ids: Vec<i32> = descendants.iter().map(|(id, _)| *id).collect();
    consumed_ns_ids.push(source_ns_id);
    for ns_id in &consumed_ns_ids {
        conn.execute(
            "DELETE FROM enlisted_entity WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
            [ns_id],
        )
        .map_err(|e| DelightQLError::database_error("delist consumed entity", e.to_string()))?;
        conn.execute(
            "DELETE FROM enlisted_namespace WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
            [ns_id],
        )
        .map_err(|e| DelightQLError::database_error("delist consumed ns", e.to_string()))?;
    }

    // D2: register a catalog wrapper for the blueprint so it is visible.
    register_catalog_wrapper(conn, &bp_fq, sys_meta_ns_id, catalog_id)?;

    Ok(bp_fq)
}

/// Refuse an operation that would ANIMATE an archived blueprint namespace —
/// the enforcement half of "visible-but-INERT". `imprint!` consumes a source
/// namespace into `{target}::_N_blueprint`,
/// stamping `kind='blueprint'` on the archive ROOT (see
/// `consume_source_to_blueprint`); its descendants keep only their `fq_name`
/// rewritten (kind stays NULL). So inertness is an ANCESTOR-OR-SELF test:
/// `fq_name` is inert iff it equals, or is nested directly under, some
/// blueprint-kind namespace. Membership is exact string-prefix (`==` or
/// `starts_with("{bp}::")`) — mirroring the consume-side descendant discovery,
/// no `LIKE` (`_`/`%` are ordinary namespace-name characters). Blueprints are
/// rare, so the scan is a handful of rows; callers invoke this only on the
/// namespace-qualified resolution / `enlist!` / `ground!` paths, never on
/// bare table lookups. The `sys::meta` catalog functor stays VISIBLE because
/// it resolves through `sys::meta`, never through the blueprint path — it does
/// not call this. Pinned by companion_linear--70 (query), --71 (enlist),
/// --73 (ground), --74 (function call); the visible half is pinned by --61.
///
/// Two layers (defense in depth): the LOUD front doors (relation resolution
/// via `resolve_namespace_path`, function inlining via
/// `ConsultRegistry::refuse_if_blueprint_fq`, `enlist!`, `ground!`) use
/// `refuse_if_blueprint` for the badged error; the quiet safety net inside
/// `ConsultRegistry::lookup_entity` uses `blueprint_shadowing` so ANY other
/// present-or-future lookup route degrades to a clean not-found rather than
/// silently executing archived rules — trusting the front doors alone is a
/// tempting regression: a lookup route can bypass them without a compile error.
pub(crate) fn blueprint_shadowing(conn: &Connection, fq_name: &str) -> Result<Option<String>> {
    let mut stmt = conn
        .prepare("SELECT fq_name FROM namespace WHERE kind = 'blueprint'")
        .map_err(|e| {
            DelightQLError::database_error("prepare blueprint inertness scan", e.to_string())
        })?;
    let blueprints = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| DelightQLError::database_error("scan blueprint namespaces", e.to_string()))?;
    for bp in blueprints {
        let bp = bp
            .map_err(|e| DelightQLError::database_error("read blueprint fq_name", e.to_string()))?;
        if fq_name == bp || fq_name.starts_with(&format!("{}::", bp)) {
            return Ok(Some(bp));
        }
    }
    Ok(None)
}

/// Which of imprint!'s two verbs is running, named at the call site instead
/// of a positional `replace: bool` flag — a bare `bool` argument reads as
/// noise at the call site and invites verb confusion.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ImprintMode {
    /// `imprint!` — refuse if any target object already exists.
    Strict,
    /// `imprint_replace!` — drop each clashing target object, then recreate.
    Replace,
}

/// Wrap an identifier in double quotes, doubling any internal `"` per SQL's
/// quoted-identifier escaping. Used at every imprint DDL build site that
/// interpolates a schema alias or entity name (the qualified-name builder,
/// CTAS/VIEW CREATE, the INSERT target, replace-mode DROP, the clash pre-flight
/// `sqlite_master`/`sqlite_temp_master`, and the `table_info`/`foreign_key_check`
/// PRAGMAs). The schema alias is the load-bearing case: it comes from a mount
/// file path / ATTACH alias, not a DQL identifier, so it cannot be validated
/// away. Entity names are additionally forbidden a `"` at manifest-read
/// (`manifest::validate_entity_name`), because the declared-table branch routes
/// its CREATE through the DDL generator, which this helper cannot reach.
/// Pinned by `system::imprint_helper_tests::quote_ident_doubles_internal_quote`.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build the imprint clash-probe query for one entity name. `master` is the
/// (possibly schema-qualified) `sqlite_master` relation; `name_lit` is the
/// entity name already escaped for a single-quoted SQL string literal. The
/// query UNIONs `sqlite_master` with the connection-local, always-unqualified
/// `sqlite_temp_master` so a temp object of the same name is not missed by the
/// strict-clash / replace-drop pre-flight. Pinned by
/// `system::imprint_helper_tests::clash_probe_sees_temp_object`.
pub(crate) fn imprint_clash_probe_sql(master: &str, name_lit: &str) -> String {
    format!(
        "SELECT type FROM {m} WHERE name = '{n}' \
         UNION ALL SELECT type FROM sqlite_temp_master WHERE name = '{n}'",
        m = master,
        n = name_lit
    )
}

/// The engine's OWN default schema for an unqualified fatboy mount (the read
/// side of `fatboy_exec.rs::default_schema`, which is the write side the
/// introspector uses). 3 = postgres → `public`, 4 = duckdb →
/// `main`; every other type (SQLite primary, siso, unknown) has no derivable
/// engine schema and answers `None`. Pinned by
/// `schema_mount_recording_tests::bare_mount_falls_back_to_the_engine_default`.
pub(crate) fn default_engine_schema_for_type(connection_type: Option<i64>) -> Option<String> {
    match connection_type {
        Some(3) => Some("public".to_string()),
        Some(4) => Some("main".to_string()),
        _ => None,
    }
}

/// The introspection SQL created-object registration routes to a created
/// object's OWN connection, selected by that connection's dialect.
/// Answers `(sql, name_col, type_col)` —
/// the 0-based row positions of the column name and its engine type.
///
/// - **SQLite** (and every dialect without a specific arm): `PRAGMA
///   table_info` verbatim.
/// - **DuckDB**: PRAGMA table_info is KEPT, deliberately: it works there,
///   its boolean-shaped `notnull`/`pk` columns
///   sit at positions this parse never reads (name = 1, type = 2), and the
///   information_schema alternative would need CATALOG scoping (temp
///   objects live in catalog `temp`, durable in the file's basename
///   catalog) for zero gain. Pinned by
///   `duckdb_readback_keeps_pragma_and_tolerates_the_boolean_shape`.
/// - **Postgres**: the information_schema form, mirroring the fatboy
///   mount's own working introspection (fatboy_exec.rs `introspect_sql` —
///   the proven pattern over the relay), scoped to ONE schema: the
///   session's temp schema when the name is temp-held (the shadow
///   preference resolution itself applies), else the MOUNTED schema.
///   `None` when the mounted schema is not
///   derivable — the caller abstains (registers nothing) rather than
///   guess. Pinned by
///   `pg_readback_routes_information_schema_sql_to_the_objects_connection`
///   and `system::readback_sql_tests`. Live verification that
///   information_schema.columns lists the session's own temp tables on PG
///   remains open (compile-only here — effects on fatboys are struck).
fn created_object_readback_sql_scoped(
    dialect: crate::pipeline::generator::SqlDialect,
    name: &str,
    mounted_schema: Option<&str>,
    sqlite_schema: Option<&str>,
) -> Option<(String, usize, usize)> {
    match dialect {
        crate::pipeline::generator::SqlDialect::PostgreSQL => {
            let schema_lit = mounted_schema?.replace('\'', "''");
            let name_lit = name.replace('\'', "''");
            Some((
                format!(
                    "SELECT c.column_name, c.data_type \
                     FROM information_schema.columns c \
                     WHERE c.table_name = '{n}' \
                       AND c.table_schema = COALESCE(\
                           (SELECT tn.nspname FROM pg_class t \
                             JOIN pg_namespace tn ON tn.oid = t.relnamespace \
                            WHERE t.relname = '{n}' \
                              AND t.relnamespace = pg_my_temp_schema()), \
                           '{s}') \
                     ORDER BY c.ordinal_position",
                    n = name_lit,
                    s = schema_lit
                ),
                0,
                1,
            ))
        }
        crate::pipeline::generator::SqlDialect::SQLite => Some((
            match sqlite_schema {
                Some(schema) => format!(
                    "PRAGMA {}.table_info({})",
                    quote_ident(schema),
                    quote_ident(name)
                ),
                None => format!("PRAGMA table_info({})", quote_ident(name)),
            },
            1,
            2,
        )),
        _ => Some((format!("PRAGMA table_info({})", quote_ident(name)), 1, 2)),
    }
}

/// Target-specific existence probe for a run-created object. This is a
/// separate question from column metadata: an existing zero-column relation
/// must not be mistaken for an object that was skipped by an exit latch.
fn created_object_existence_sql_scoped(
    dialect: crate::pipeline::generator::SqlDialect,
    name: &str,
    mounted_schema: Option<&str>,
    sqlite_schema: Option<&str>,
) -> Option<String> {
    let name_lit = name.replace('\'', "''");
    match dialect {
        crate::pipeline::generator::SqlDialect::SQLite => {
            let master = sqlite_schema
                .map(|schema| format!("{}.sqlite_master", quote_ident(schema)))
                .unwrap_or_else(|| "sqlite_master".to_string());
            Some(format!(
                "SELECT type, 'attached' AS source FROM {master} WHERE name = '{name_lit}' \
                 UNION ALL SELECT type, 'temp' AS source FROM sqlite_temp_master WHERE name = '{name_lit}'"
            ))
        }
        crate::pipeline::generator::SqlDialect::DuckDB => Some(format!(
            "SELECT table_name FROM information_schema.tables WHERE table_name = '{name_lit}'"
        )),
        crate::pipeline::generator::SqlDialect::PostgreSQL => {
            let schema_lit = mounted_schema?.replace('\'', "''");
            Some(format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_name = '{name_lit}' \
                   AND table_schema = COALESCE(\
                       (SELECT tn.nspname FROM pg_class t \
                         JOIN pg_namespace tn ON tn.oid = t.relnamespace \
                        WHERE t.relname = '{name_lit}' \
                          AND t.relnamespace = pg_my_temp_schema()), \
                       '{schema_lit}')"
            ))
        }
        crate::pipeline::generator::SqlDialect::MySQL
        | crate::pipeline::generator::SqlDialect::SqlServer => None,
    }
}

/// The loud half of `blueprint_shadowing`: badged `imprint/blueprint/inert`.
pub(crate) fn refuse_if_blueprint(conn: &Connection, fq_name: &str) -> Result<()> {
    if let Some(bp) = blueprint_shadowing(conn, fq_name)? {
        // The target the source was consumed into = the blueprint's
        // parent (`{target}::_N_blueprint`); strip the last `::` segment.
        let target = bp.rsplit_once("::").map(|(p, _)| p).unwrap_or(bp.as_str());
        return Err(DelightQLError::validation_error_categorized(
            "imprint/blueprint/inert",
            format!(
                "'{}' is an archived blueprint (imprint! consumed it into '{}'); \
                 blueprints are visible but inert — re-consult the source path \
                 for a live copy",
                bp, target
            ),
            "archived blueprint is inert",
        ));
    }
    Ok(())
}

/// Guard a USER-TYPED namespace-creation target against the reserved system
/// name pool.
///
/// The top level of the namespace tree stays OPEN to user names — `consult!`
/// and `mount!` mint exactly where they say. What this refuses, loudly and
/// badged (`error://namespace/name/...`):
///
///   (a) a top-level name that IS a bare system name (`sys`/`std`/`main`/
///       `home`) — creating AS it, or taking it over (e.g. `mount!(…,"home")(*)`);
///   (b) a top-level name PREFIXED `sys`/`std`, case-insensitive (`sysinfo`,
///       `stdlib`, `std2`, `SYS_foo`) — the system's room to mint future
///       siblings. Exact `main`/`home` are NOT prefix rules, so `maintenance`
///       and `homework` stay legal;
///   (c) ANY segment beginning `_` (`_internal`, `_N_blueprint`) — the system
///       machinery convention, formally reserved EVERYWHERE (including under
///       `home`);
///   (d) creation UNDER `sys::`/`std::` — the system subtree.
///
/// Relaxations:
///   * Creating UNDER `home` is the user's right (`consult!(…,"home::x")(*)`), and
///     inside `home` the prefix prong (b) relaxes — `home::sysinfo` is yours.
///     Only prong (c) stays strict there.
///   * Creating UNDER `main` (`main::x`) is not this guard's business — main
///     is a bootstrap fixture with its own kind/fidelity contract, left alone
///     here (prong (c)'s `_` check still applies, since machinery names are
///     reserved everywhere).
///
/// CRITICAL: call this at the USER-facing verb boundary with the USER-TYPED
/// string only. System-minted names (`_internal` companion children,
/// `_N_blueprint` imprint archives) are created by internal machinery that
/// never routes through here, so they keep working.
pub(crate) fn validate_user_namespace_target(fq: &str) -> Result<()> {
    let segments: Vec<&str> = fq.split("::").collect();
    let top = segments[0];
    let top_lc = top.to_ascii_lowercase();

    // Prong (c): the `_` prefix is reserved for system machinery, on ANY
    // segment, EVERYWHERE. Checked first so it fires regardless of which
    // top-level branch a path would otherwise take (including `home::_y` and
    // `main::_x`).
    for seg in &segments {
        if seg.starts_with('_') {
            return Err(DelightQLError::validation_error_categorized(
                "namespace/name/reserved",
                format!(
                    "cannot create namespace '{}': the segment '{}' begins with '_', \
                     which is reserved for system machinery (e.g. _internal, \
                     _N_blueprint). Choose a name that does not begin with '_'.",
                    fq, seg
                ),
                "reserved system name",
            ));
        }
    }

    // `main` is the primary DATA namespace — governed by its own
    // kind/fidelity contract, NOT this name guard. Both the bare form and the
    // `main::x` subtree are left alone here:
    //   * bare `main`: the sanctioned primary-data bind. The CLI establishes
    //     every session with `mount!("<db>", "main")(*)` (commands/query.rs); the
    //     harness-generated primary mount is textually a user mount and routes
    //     through this guard, so refusing bare `main` would break every session.
    //   * `main::x`: creating under main is that contract's business.
    // (Prong (c)'s `_` check above still applied — machinery names stay reserved
    // even here.) This is why prong (a) below lists only sys/std/home, not main.
    if top_lc == "main" {
        return Ok(());
    }

    // Under `home`: creating a child is the user's right, and prong (b) relaxes
    // (`home::sysinfo` is legal). Only prong (c) — already checked — stays
    // strict here.
    if top_lc == "home" && segments.len() > 1 {
        return Ok(());
    }

    // Prong (d): creating UNDER `sys::`/`std::` — the reserved system subtree.
    if (top_lc == "sys" || top_lc == "std") && segments.len() > 1 {
        return Err(DelightQLError::validation_error_categorized(
            "namespace/name/system_subtree",
            format!(
                "cannot create namespace '{}': the '{}::' subtree is reserved for \
                 system machinery. Create your namespace at the top level (or under \
                 home::) instead.",
                fq, top_lc
            ),
            "reserved system subtree",
        ));
    }

    // Prong (a): the bare top-level system name itself — creating AS it, or
    // taking it over (e.g. `mount!(…, "home")(*)`). `main` is exempt (handled
    // above — it is the data namespace); `home` reaches here only
    // in its bare form (its subtree case returned above, since creating under
    // home is the user's right).
    if matches!(top_lc.as_str(), "sys" | "std" | "home") {
        return Err(DelightQLError::validation_error_categorized(
            "namespace/name/reserved",
            format!(
                "cannot create namespace '{}': '{}' is a reserved system name. \
                 Choose a different top-level name (to author scratch under home, \
                 write home::{}).",
                fq, top_lc, top_lc
            ),
            "reserved system name",
        ));
    }

    // Prong (b): a top-level name PREFIXED `sys`/`std` (case-insensitive) — the
    // system's room to mint future siblings. Exact `main`/`home` handled above;
    // `maintenance`/`homework` are not prefix hits and pass through.
    if top_lc.starts_with("sys") || top_lc.starts_with("std") {
        return Err(DelightQLError::validation_error_categorized(
            "namespace/name/reserved",
            format!(
                "cannot create namespace '{}': the top-level name '{}' begins with a \
                 reserved system prefix (sys*/std*). Choose a name not beginning with \
                 sys or std (the prefix relaxes under home:: — home::{} is legal).",
                fq, top, top
            ),
            "reserved system name",
        ));
    }

    Ok(())
}

/// Expand a plain namespace qualifier that FAILED exact resolution, by
/// consulting the enlist set — the MIDDLE access rung between exact and
/// full-path resolution ("`chutzpah.shout` — plain qualifier; home resolves
/// first"). Enlisting a
/// namespace makes its child namespaces *plain-addressable*: once `home` is
/// enlisted, `chz` alone resolves to `home::chz`.
///
/// Contract — the precedence is fixed, do not reorder:
///   1. `path` is a namespace path that ALREADY MISSED exact resolution. This
///      is consulted ONLY on a miss, so every path that resolves today — full
///      names, top-level names, table aliases, `table.column` refs — is
///      structurally untouched (existing resolution
///      always wins; the expansion is pure addition).
///   2. HOME FIRST: if `home::{path}` exists, it wins outright.
///   3. Otherwise exactly one non-home enlisted parent may match → use it;
///      MULTIPLE non-home matches → loud badged ambiguity
///      (`namespace/plain/ambiguous`), listing the candidate fqs.
///   4. NON-TRANSITIVE (rule 4): only DIRECT children of enlisted namespaces
///      are tried. Multi-segment paths never expand — enlisting `home` grants
///      `home::chz` (a child), never `home::a::b` (a grandchild). One prefix
///      join per parent, no recursion.
///
/// The enlist set is `enlisted_namespace` joined to its `to = 'home'` session
/// scope (the interactive environment's own namespace). The
/// returned fq re-enters the normal
/// resolution path at every call site, so the blueprint-inertness guard applies
/// to the expanded fq unchanged.
pub(crate) fn expand_plain_namespace(conn: &Connection, path: &str) -> Result<Option<String>> {
    // Rule 4, non-transitive: only a single-segment child qualifier expands. A
    // `::` in the path means the user reached PAST a direct child; enlisting
    // never leaks a grandchild, so such a path is left to miss.
    if path.contains("::") {
        return Ok(None);
    }

    // The enlisted parents (from_namespace) for the session scope
    // (to='home'), PLUS the scope itself — named scratch lives at
    // home::<name>, and home is its own parent now that it IS the scope
    // (precedence rule 2 already prefers the home child).
    let mut stmt = conn
        .prepare(
            "SELECT p.fq_name
             FROM enlisted_namespace en
             JOIN namespace p ON p.id = en.from_namespace_id
             JOIN namespace target ON target.id = en.to_namespace_id
             WHERE target.fq_name = 'home'
             UNION
             SELECT 'home'",
        )
        .map_err(|e| DelightQLError::database_error("prepare enlist-set scan", e.to_string()))?;
    let parents: Vec<String> = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| DelightQLError::database_error("scan enlist set", e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();

    let mut home_match: Option<String> = None;
    let mut other_matches: Vec<String> = Vec::new();
    for parent in &parents {
        let candidate = format!("{}::{}", parent, path);
        let exists = conn
            .query_row(
                "SELECT 1 FROM namespace WHERE fq_name = ?1",
                [&candidate],
                |_| Ok(()),
            )
            .optional()
            .map_err(|e| {
                DelightQLError::database_error("probe enlisted child namespace", e.to_string())
            })?
            .is_some();
        if exists {
            if parent == "home" {
                home_match = Some(candidate);
            } else {
                other_matches.push(candidate);
            }
        }
    }

    // Rule 2, HOME FIRST: a home child beats every other enlisted parent.
    if let Some(h) = home_match {
        return Ok(Some(h));
    }
    match other_matches.len() {
        0 => Ok(None),
        1 => Ok(Some(other_matches.into_iter().next().unwrap())),
        _ => {
            // Rule 3, loud ambiguity: two enlisted parents each hold a child of
            // this plain name — the user must spell the full path.
            other_matches.sort();
            Err(DelightQLError::validation_error_categorized(
                "namespace/plain/ambiguous",
                format!(
                    "plain namespace qualifier '{}' is ambiguous: it names a child of \
                     multiple enlisted namespaces [{}]. Spell the full path to \
                     disambiguate.",
                    path,
                    other_matches.join(", "),
                ),
                "ambiguous plain namespace qualifier",
            ))
        }
    }
}

/// The SHADOW half of the plain-qualifier access rung — the ratified SOFTENING
/// of "home resolves first": strict home-first would
/// let a scratch `home::chz` SILENTLY shadow a pre-existing top-level `chz`,
/// changing a query that resolves today (a silent-wrong). Resolution stays
/// monotonic instead: an existing top-level namespace wins (precedence rule
/// 1 — expansion never fires when the exact name already resolves), and we WARN
/// the other way. Returns true iff `path` — a plain qualifier that JUST resolved
/// to a top-level namespace — ALSO has an enlisted `home::{path}` child sitting
/// shadowed behind it, so the caller can emit `log::warn!`.
pub(crate) fn home_child_shadows(conn: &Connection, path: &str) -> bool {
    // Only a single-segment top-level name can be shadowed this way; the
    // always-present session names are never user home children.
    if path.contains("::") || path == "home" || path == "main" {
        return false;
    }
    let candidate = format!("home::{}", path);
    conn.query_row(
        "SELECT 1 FROM namespace WHERE fq_name = ?1",
        [&candidate],
        |_| Ok(()),
    )
    .optional()
    .ok()
    .flatten()
    .is_some()
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
mod expand_plain_namespace_tests {
    //! The middle access rung between exact and full-path resolution:
    //! plain-qualifier expansion over the enlist set. `expand_plain_namespace` is a pure
    //! function of the bootstrap `namespace`/`enlisted_namespace` tables, so
    //! these are the authoritative behavior pins for its precedence (home-first,
    //! single-match, ambiguity, non-transitivity, miss). The balls prove the
    //! wiring at the three resolution doors.
    use super::{expand_plain_namespace, home_child_shadows};
    use rusqlite::Connection;

    /// Build a bootstrap-shaped `namespace` + `enlisted_namespace` fixture.
    /// `enlisted` names the from-namespaces enlisted into 'main' (the session
    /// scope). Every fq in `namespaces` gets a row.
    fn fixture(namespaces: &[&str], enlisted: &[&str]) -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE namespace (id INTEGER PRIMARY KEY, fq_name TEXT NOT NULL);
             CREATE TABLE enlisted_namespace (from_namespace_id INTEGER, to_namespace_id INTEGER);",
        )
        .unwrap();
        // 'home' is always present (the session scope);
        // 'main' stays as the ambient data namespace.
        let mut all: Vec<&str> = vec!["home", "main"];
        all.extend_from_slice(namespaces);
        all.dedup_by(|a, b| a == b);
        let mut seen = std::collections::HashSet::new();
        all.retain(|fq| seen.insert(*fq));
        for (i, fq) in all.iter().enumerate() {
            c.execute(
                "INSERT OR IGNORE INTO namespace (id, fq_name) VALUES (?1, ?2)",
                rusqlite::params![i as i64 + 1, fq],
            )
            .unwrap();
        }
        let id_of = |fq: &str| -> i64 {
            c.query_row("SELECT id FROM namespace WHERE fq_name = ?1", [fq], |r| {
                r.get(0)
            })
            .unwrap()
        };
        let home_id = id_of("home");
        for e in enlisted {
            c.execute(
                "INSERT INTO enlisted_namespace (from_namespace_id, to_namespace_id) VALUES (?1, ?2)",
                rusqlite::params![id_of(e), home_id],
            )
            .unwrap();
        }
        c
    }

    #[test]
    fn home_child_resolves() {
        let c = fixture(&["home", "home::chz"], &["home"]);
        assert_eq!(
            expand_plain_namespace(&c, "chz").unwrap(),
            Some("home::chz".to_string())
        );
    }

    #[test]
    fn miss_returns_none() {
        let c = fixture(&["home", "home::chz"], &["home"]);
        // `nope` is not a child of any enlisted namespace.
        assert_eq!(expand_plain_namespace(&c, "nope").unwrap(), None);
    }

    #[test]
    fn home_first_beats_other_enlisted_parent() {
        // Both home::dup and wh::dup exist and both parents are enlisted —
        // home wins ("home resolves first").
        let c = fixture(&["home", "home::dup", "wh", "wh::dup"], &["home", "wh"]);
        assert_eq!(
            expand_plain_namespace(&c, "dup").unwrap(),
            Some("home::dup".to_string())
        );
    }

    #[test]
    fn single_non_home_parent_resolves() {
        let c = fixture(&["home", "wh", "wh::sales"], &["home", "wh"]);
        assert_eq!(
            expand_plain_namespace(&c, "sales").unwrap(),
            Some("wh::sales".to_string())
        );
    }

    #[test]
    fn multiple_non_home_parents_are_ambiguous() {
        // a::x and b::x, both enlisted, no home child → loud badged ambiguity.
        let c = fixture(&["a", "a::x", "b", "b::x"], &["a", "b"]);
        let err = expand_plain_namespace(&c, "x").unwrap_err();
        assert!(
            err.error_uri().contains("namespace/plain/ambiguous"),
            "expected ambiguity badge, got {}",
            err.error_uri()
        );
    }

    #[test]
    fn non_transitive_no_grandchild_leak() {
        // home is enlisted, home::a exists, home::a::b exists — but `b` is a
        // GRANDCHILD of home, never plain-addressable. And a multi-segment path
        // never expands at all.
        let c = fixture(&["home", "home::a", "home::a::b"], &["home"]);
        assert_eq!(expand_plain_namespace(&c, "b").unwrap(), None);
        assert_eq!(expand_plain_namespace(&c, "a::b").unwrap(), None);
    }

    #[test]
    fn unenlisted_parent_does_not_grant() {
        // wh::sales exists but wh is NOT enlisted → `sales` does not resolve.
        let c = fixture(&["home", "wh", "wh::sales"], &["home"]);
        assert_eq!(expand_plain_namespace(&c, "sales").unwrap(), None);
    }

    #[test]
    fn shadow_detects_home_child_behind_top_level() {
        // A top-level `dup` and a home::dup both exist: the shadow probe fires.
        let c = fixture(&["home", "home::dup", "dup"], &["home"]);
        assert!(home_child_shadows(&c, "dup"));
        // No home child → no shadow.
        assert!(!home_child_shadows(&c, "main"));
        let c2 = fixture(&["home", "solo"], &["home"]);
        assert!(!home_child_shadows(&c2, "solo"));
    }
}

#[cfg(test)]
mod imprint_version_tests {
    //! Blueprint versioning: the version N chosen for a new
    //! `{target}::_N_blueprint` must be `MAX(existing N)+1`, not `COUNT`, so a
    //! removed blueprint never causes the next imprint to reuse a live N; and a
    //! failed version query must be loud, never a silent 0.
    use super::next_blueprint_version;
    use rusqlite::Connection;

    fn ns_conn() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch(
            "CREATE TABLE namespace (id INTEGER PRIMARY KEY, name TEXT NOT NULL, pid INTEGER, fq_name TEXT);
             INSERT INTO namespace (id, name, pid, fq_name) VALUES (1, 'main', NULL, 'main');",
        )
        .unwrap();
        c
    }

    #[test]
    fn next_blueprint_version_is_max_plus_one() {
        let c = ns_conn();
        // No blueprints yet → 0.
        assert_eq!(next_blueprint_version(&c, 1).unwrap(), 0);

        c.execute(
            "INSERT INTO namespace (id, name, pid, fq_name)
             VALUES (2, '_0_blueprint', 1, 'main::_0_blueprint'),
                    (3, '_1_blueprint', 1, 'main::_1_blueprint')",
            [],
        )
        .unwrap();
        assert_eq!(next_blueprint_version(&c, 1).unwrap(), 2);

        // Delete _0_blueprint: COUNT would now return 1 — a collision with the
        // surviving _1_blueprint. MAX+1 stays 2.
        c.execute("DELETE FROM namespace WHERE id = 2", []).unwrap();
        assert_eq!(
            next_blueprint_version(&c, 1).unwrap(),
            2,
            "MAX+1 must not reuse an existing N after a gap (finding 8)"
        );

        // Non-blueprint children and blueprints of other parents are ignored.
        c.execute(
            "INSERT INTO namespace (id, name, pid, fq_name)
             VALUES (4, '_internal', 1, 'main::_internal'),
                    (5, '_9_blueprint', 99, 'other::_9_blueprint')",
            [],
        )
        .unwrap();
        assert_eq!(next_blueprint_version(&c, 1).unwrap(), 2);
    }

    #[test]
    fn next_blueprint_version_errors_on_missing_table() {
        // A failed version query is a loud error (`?`), not the silent 0 that
        // `.unwrap_or(0)` masked.
        let c = Connection::open_in_memory().unwrap();
        assert!(next_blueprint_version(&c, 1).is_err());
    }
}

#[cfg(test)]
mod imprint_helper_tests {
    //! Identifier quoting for the imprint DDL path. The schema
    //! alias is the load-bearing case (it comes from a mount path / ATTACH
    //! alias, not a validatable identifier); entity names are additionally
    //! forbidden a `"` at manifest-read (manifest::validate_entity_name).
    use super::{imprint_clash_probe_sql, quote_ident};
    use rusqlite::Connection;

    #[test]
    fn quote_ident_doubles_internal_quote() {
        assert_eq!(quote_ident("plain"), "\"plain\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
        // The empty and all-quotes edge cases stay well-formed.
        assert_eq!(quote_ident(""), "\"\"");
        assert_eq!(quote_ident("\""), "\"\"\"\"");
    }

    #[test]
    fn quote_ident_output_is_valid_sql_identifier() {
        // The doubled form must round-trip through SQLite as the literal name,
        // not a truncated/injected one. Create a table whose name embeds a `"`
        // and read it back — proof the escaping is real, not cosmetic.
        let c = Connection::open_in_memory().unwrap();
        let ident = quote_ident("we\"ird");
        c.execute_batch(&format!("CREATE TABLE {ident} (x INTEGER)"))
            .unwrap();
        let name: String = c
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(name, "we\"ird");
    }

    #[test]
    fn clash_probe_sees_temp_object() {
        // A temp object is invisible to sqlite_master but
        // must still register as a clash. The probe UNIONs sqlite_temp_master.
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TEMP TABLE foo (x INTEGER)")
            .unwrap();

        // sqlite_master alone misses it (the blind spot the clash probe closes)...
        let master_only: Option<String> = c
            .query_row(
                "SELECT type FROM sqlite_master WHERE name = 'foo'",
                [],
                |r| r.get(0),
            )
            .ok();
        assert_eq!(master_only, None);

        // ...the clash probe catches it.
        let probe = imprint_clash_probe_sql("sqlite_master", "foo");
        assert!(probe.contains("sqlite_temp_master"), "{}", probe);
        let ty: String = c.query_row(&probe, [], |r| r.get(0)).unwrap();
        assert_eq!(ty, "table");
    }
}

#[cfg(test)]
mod readback_sql_tests {
    //! The registration read-back's per-dialect SQL SELECTION.
    //! Compile-only — routing and shape tolerance are pinned in
    //! effect_transformer/tests.rs; live behavior is verified separately.
    use super::{created_object_existence_sql_scoped, created_object_readback_sql_scoped};
    use crate::pipeline::generator::SqlDialect;

    #[test]
    fn readback_sql_is_pragma_table_info_on_sqlite_and_duckdb() {
        for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
            let (sql, name_col, type_col) =
                created_object_readback_sql_scoped(dialect, "staged", None, None)
                    .expect("sqlite/duckdb read-back never abstains");
            assert_eq!(sql, "PRAGMA table_info(\"staged\")");
            // name at 1, type at 2 — DuckDB's boolean-shaped notnull/pk
            // (positions 3/5) are never read.
            assert_eq!((name_col, type_col), (1, 2));
        }
    }

    #[test]
    fn readback_sql_on_postgres_scopes_the_mounted_schema_and_prefers_temp() {
        let (sql, name_col, type_col) =
            created_object_readback_sql_scoped(SqlDialect::PostgreSQL, "dur", Some("public"), None)
                .expect("a known mounted schema yields the information_schema form");
        assert!(sql.contains("information_schema.columns"), "{sql}");
        assert!(sql.contains("c.table_name = 'dur'"), "{sql}");
        // ONE schema: the session temp schema when the name is temp-held
        // (resolution's shadow preference), else the mounted schema.
        assert!(sql.contains("pg_my_temp_schema()"), "{sql}");
        assert!(sql.contains("'public'"), "{sql}");
        assert!(sql.contains("ORDER BY c.ordinal_position"), "{sql}");
        assert_eq!((name_col, type_col), (0, 1));
        // Literal escaping is real: a quote in the name cannot break out.
        let (evil, _, _) =
            created_object_readback_sql_scoped(SqlDialect::PostgreSQL, "a'b", Some("public"), None)
                .unwrap();
        assert!(evil.contains("'a''b'"), "{evil}");
        assert!(!evil.contains("= 'a'b'"), "{evil}");
    }

    #[test]
    fn readback_abstains_on_postgres_without_a_mounted_schema() {
        assert!(
            created_object_readback_sql_scoped(SqlDialect::PostgreSQL, "dur", None, None).is_none(),
            "an unknowable mounted schema abstains (never a guessed schema)"
        );
    }

    #[test]
    fn existence_sql_is_separate_and_target_scoped() {
        let sqlite = created_object_existence_sql_scoped(SqlDialect::SQLite, "staged", None, None)
            .expect("sqlite existence probe");
        assert!(sqlite.contains("sqlite_master"), "{sqlite}");
        assert!(sqlite.contains("sqlite_temp_master"), "{sqlite}");
        assert!(sqlite.contains("name = 'staged'"), "{sqlite}");
        let attached = created_object_existence_sql_scoped(
            SqlDialect::SQLite,
            "staged",
            None,
            Some("_imported_7"),
        )
        .expect("scoped sqlite existence probe");
        assert!(
            attached.contains("\"_imported_7\".sqlite_master"),
            "{attached}"
        );
        let (attached_metadata, _, _) = created_object_readback_sql_scoped(
            SqlDialect::SQLite,
            "staged",
            None,
            Some("_imported_7"),
        )
        .expect("scoped sqlite metadata probe");
        assert_eq!(
            attached_metadata,
            "PRAGMA \"_imported_7\".table_info(\"staged\")"
        );

        let duckdb = created_object_existence_sql_scoped(SqlDialect::DuckDB, "staged", None, None)
            .expect("duckdb existence probe");
        assert!(duckdb.contains("information_schema.tables"), "{duckdb}");
        assert!(!duckdb.contains("PRAGMA table_info"), "{duckdb}");

        let postgres = created_object_existence_sql_scoped(
            SqlDialect::PostgreSQL,
            "staged",
            Some("public"),
            None,
        )
        .expect("postgres existence probe");
        assert!(postgres.contains("information_schema.tables"), "{postgres}");
        assert!(postgres.contains("pg_my_temp_schema()"), "{postgres}");
        assert!(postgres.contains("'public'"), "{postgres}");
        assert!(
            created_object_existence_sql_scoped(SqlDialect::PostgreSQL, "staged", None, None)
                .is_none()
        );
        assert!(
            created_object_existence_sql_scoped(SqlDialect::MySQL, "staged", None, None).is_none()
        );
        assert!(
            created_object_existence_sql_scoped(SqlDialect::SqlServer, "staged", None, None)
                .is_none()
        );

        let (evil, _, _) =
            created_object_existence_sql_scoped(SqlDialect::SQLite, "a'b", None, None)
                .map(|sql| (sql, 0, 0))
                .expect("sqlite existence probe with quote");
        assert!(evil.contains("'a''b'"), "{evil}");
        assert!(!evil.contains("= 'a'b'"), "{evil}");
    }
}

#[cfg(test)]
mod created_object_catalog_tests {
    use super::{
        CatalogSavepoint, CreatedObjectCatalog, DelightQLSystem, RealCreatedObjectCatalog,
    };
    use crate::external_effects::CreatedObjectRegistration;
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

    fn fresh_system() -> DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    #[test]
    fn failed_later_registration_rolls_back_the_complete_batch() {
        let system = fresh_system();
        let bootstrap = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        // A fault trigger on a protected catalog table is engine-level
        // surgery; the scoped capability is the one road that admits it.
        let window = system.bootstrap_guard.migration_window();
        bootstrap
            .execute_batch(
                "CREATE TRIGGER fail_second_created_object
                 BEFORE INSERT ON entity
                 WHEN NEW.name = 'second'
                 BEGIN
                     SELECT RAISE(FAIL, 'second registration refused');
                 END;",
            )
            .expect("install failure trigger");
        window.close(&bootstrap).expect("re-seal");

        let registrations = vec![
            CreatedObjectRegistration {
                name: "first".to_string(),
                is_view: false,
                connection_id: 2,
                namespace_id: 1,
                attributes: vec![("id".to_string(), "INTEGER".to_string())],
            },
            CreatedObjectRegistration {
                name: "second".to_string(),
                is_view: false,
                connection_id: 2,
                namespace_id: 1,
                attributes: vec![("id".to_string(), "INTEGER".to_string())],
            },
        ];
        let savepoint = CatalogSavepoint::begin(
            &bootstrap,
            "dql_test_created_object_batch",
            "begin test savepoint",
        )
        .expect("begin test savepoint");
        let _window = system.catalog_window();
        let error = RealCreatedObjectCatalog
            .reconcile(&bootstrap, &registrations)
            .expect_err("the trigger must reject the second registration");
        drop(savepoint);

        assert!(error.to_string().contains("second registration refused"));
        let count: i64 = bootstrap
            .query_row(
                "SELECT COUNT(*) FROM entity e
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE c.source_uri = 'session://materialized'",
                [],
                |row| row.get(0),
            )
            .expect("count session materialized entities");
        assert_eq!(count, 0, "the failed batch must leave no sibling behind");
    }
}

#[cfg(test)]
mod schema_mount_recording_tests {
    //! The mounted
    //! engine schema is a RECORDED per-mount fact. Mount qualification is read
    //! from `mount`; cartridge.source_ns remains source metadata and is never
    //! a second policy authority. A mount given a specific schema records THAT
    //! schema (and introspects it); a bare mount records unqualified policy and
    //! the namespace-keyed lookup resolves the engine default downstream.
    use super::DelightQLSystem;
    use delightql_types::factory::ConnectionComponents;
    use delightql_types::introspect::{
        DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity,
    };
    use delightql_types::test_utils::{MockDatabaseConnection, MockSchemaProvider};
    use delightql_types::Result;
    use std::sync::{Arc, Mutex};

    /// Discovers ONE entity named after the schema it was built for — the
    /// mock analog of `FatboyIntrospector` introspecting its own bound
    /// schema. Proves the schema flows to INTROSPECTION, not merely to the
    /// recorded fact.
    struct SchemaEchoIntrospector {
        schema: Option<String>,
    }
    impl DatabaseIntrospector for SchemaEchoIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            let s = self.schema.as_deref().unwrap_or("public");
            Ok(vec![DiscoveredEntity {
                name: format!("in_{s}").into(),
                entity_type_id: 10,
                attributes: vec![DiscoveredAttribute {
                    name: "id".into(),
                    data_type: "INTEGER".to_string(),
                    position: 0,
                    is_nullable: true,
                }],
            }])
        }
        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(
            conn,
            Box::new(SchemaEchoIntrospector { schema: None }),
            "sqlite",
        )
        .expect("fresh in-memory system should build")
    }

    /// A postgres-typed mock mount whose `ConnectionComponents.mounted_schema`
    /// and introspector schema AGREE (the wiring `create_fatboy_system_components`
    /// builds).
    fn pg_components(mounted_schema: Option<&str>) -> ConnectionComponents {
        ConnectionComponents {
            connection: Arc::new(Mutex::new(MockDatabaseConnection::new())),
            schema: Box::new(MockSchemaProvider::new()),
            introspector: Box::new(SchemaEchoIntrospector {
                schema: mounted_schema.map(str::to_string),
            }),
            db_type: "postgresql".to_string(),
            mechanism: "fatboy".to_string(),
            identity: None,
            mounted_schema: mounted_schema.map(str::to_string),
        }
    }

    fn shared_pg_components(
        mounted_schema: &str,
        identity: &str,
        connection: Arc<Mutex<dyn delightql_types::DatabaseConnection>>,
    ) -> ConnectionComponents {
        ConnectionComponents {
            connection,
            schema: Box::new(MockSchemaProvider::new()),
            introspector: Box::new(SchemaEchoIntrospector {
                schema: Some(mounted_schema.to_string()),
            }),
            db_type: "postgresql".to_string(),
            mechanism: "fatboy".to_string(),
            identity: Some(identity.to_string()),
            mounted_schema: Some(mounted_schema.to_string()),
        }
    }

    /// A mount given a SPECIFIC schema records it on `mount`; the namespace-
    /// keyed lookup reads that recorded fact VERBATIM (type 3 would
    /// have DERIVED `public`); and the mount introspected THAT schema's
    /// entity. A connection-type derivation alone would ignore the recorded
    /// schema and answer `public` here regardless.
    #[test]
    fn mount_records_the_engine_schema_and_the_lookup_reads_it() {
        let mut system = fresh_system();
        let (conn_id, count) = system
            .register_external_connection(pg_components(Some("reporting")), "rep", "mock://rep")
            .expect("register the reporting-schema mount");
        assert_eq!(count, 1, "the schema's one entity is introspected");
        // The recorded fact, read back namespace-keyed — verbatim, NOT derived.
        assert_eq!(
            system.mounted_engine_schema_for_namespace("rep").unwrap(),
            Some("reporting".to_string()),
            "the lookup reads the RECORDED schema, not the connection-type default"
        );
        // The connection-keyed shim agrees via the namespace route.
        assert_eq!(
            system
                .mounted_engine_schema_for_connection(conn_id)
                .unwrap(),
            Some("reporting".to_string()),
        );
        // Introspection followed the schema: the discovered entity is named
        // for 'reporting' and is registered in the namespace.
        assert!(
            system
                .get_canonical_entity_name("rep", "in_reporting")
                .unwrap()
                .is_some(),
            "the mount introspected the 'reporting' schema's entity"
        );
    }

    /// A BARE mount records unqualified policy; the lookup falls back to the
    /// engine default for the connection type
    /// (postgres → `public`, duckdb → `main`) — byte-identical to the
    /// pre-Phase-A derivation, keeping reads unqualified.
    #[test]
    fn bare_mount_falls_back_to_the_engine_default() {
        let mut system = fresh_system();
        let (conn_id, _count) = system
            .register_external_connection(pg_components(None), "plain", "mock://plain")
            .expect("register the bare mount");
        assert_eq!(
            system.mounted_engine_schema_for_namespace("plain").unwrap(),
            Some("public".to_string()),
            "unqualified mount policy resolves the engine default downstream"
        );
        assert_eq!(
            system
                .mounted_engine_schema_for_connection(conn_id)
                .unwrap(),
            Some("public".to_string()),
            "the connection shim agrees with the namespace lookup"
        );
        // The default introspection discovered the default schema's entity.
        assert!(system
            .get_canonical_entity_name("plain", "in_public")
            .unwrap()
            .is_some());
    }

    /// Qualified mounts create catalog topology, rather than storing the full
    /// path in a flat leaf row. This is shared by URI mounts and SQLite ATTACH
    /// mounts, so the cheap mock path pins the engine-level invariant while the
    /// corpus exercises the real file mount.
    #[test]
    fn qualified_mount_materializes_and_protects_its_container_parent() {
        let mut system = fresh_system();
        system
            .register_external_connection(
                pg_components(Some("reporting")),
                "client::reporting",
                "mock://nested-reporting",
            )
            .expect("register a qualified external mount");

        let bootstrap = system.get_bootstrap_connection();
        let conn = bootstrap.lock().unwrap();
        let root_id: i32 = conn
            .query_row("SELECT id FROM namespace WHERE fq_name = '_'", [], |row| {
                row.get(0)
            })
            .unwrap();
        let (parent_id, parent_name, parent_pid, parent_kind): (i32, String, Option<i32>, String) =
            conn.query_row(
                "SELECT id, name, pid, kind FROM namespace WHERE fq_name = 'client'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        let (leaf_name, leaf_pid, leaf_kind): (String, Option<i32>, String) = conn
            .query_row(
                "SELECT name, pid, kind FROM namespace WHERE fq_name = 'client::reporting'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        assert_eq!(parent_name, "client");
        assert_eq!(parent_pid, Some(root_id));
        assert_eq!(parent_kind, "container");
        assert_eq!(leaf_name, "reporting", "leaf name is one path segment");
        assert_eq!(leaf_pid, Some(parent_id));
        assert_eq!(leaf_kind, "data");

        let wrapper_count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM entity e
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE c.source_uri = 'catalog://sys::meta'
                   AND e.name IN ('client::', 'client::reporting::')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(wrapper_count, 2, "parent and leaf each have one wrapper");
        drop(conn);

        let err = system
            .unconsult_namespace("client")
            .expect_err("a structural mount parent is not a consulted library");
        assert!(err.to_string().contains("structural container"));
    }

    #[test]
    fn shared_connection_survives_until_the_last_tree_binding_is_unmounted() {
        let mut system = fresh_system();
        let shared: Arc<Mutex<dyn delightql_types::DatabaseConnection>> =
            Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let (connection_id, _) = system
            .register_external_connection(
                shared_pg_components("a", "pg-system-id:tree", Arc::clone(&shared)),
                "tree::a",
                "mock://tree",
            )
            .expect("mount first schema");
        let (same_connection_id, _) = system
            .register_external_connection(
                shared_pg_components("b", "pg-system-id:tree", Arc::clone(&shared)),
                "tree::b",
                "mock://tree",
            )
            .expect("mount second schema");
        assert_eq!(connection_id, same_connection_id);

        system
            .unmount_database("tree::a")
            .expect("unmount first leaf");
        assert!(
            system.get_connection(connection_id).is_ok(),
            "a sibling mount row still owns the shared live connection"
        );

        system
            .unmount_database("tree::b")
            .expect("unmount final leaf");
        assert!(
            system.get_connection(connection_id).is_err(),
            "the last mount row releases the shared live connection"
        );
    }

    struct FailingIntrospector;
    impl DatabaseIntrospector for FailingIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Err(delightql_types::DelightQLError::database_error(
                "induced external introspection failure",
                "schema_mount_recording_tests",
            ))
        }

        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    #[test]
    fn failed_external_mount_rolls_back_connection_namespace_and_binding() {
        let mut system = fresh_system();
        let components = ConnectionComponents {
            connection: Arc::new(Mutex::new(MockDatabaseConnection::new())),
            schema: Box::new(MockSchemaProvider::new()),
            introspector: Box::new(FailingIntrospector),
            db_type: "postgresql".to_string(),
            mechanism: "fatboy".to_string(),
            identity: Some("pg-system-id:failing".to_string()),
            mounted_schema: Some("public".to_string()),
        };

        system
            .register_external_connection(components, "failed", "mock://failed")
            .expect_err("the injected introspection failure must abort the mount");

        let conn = system.bootstrap_connection.lock().unwrap();
        let leftovers: i64 = conn
            .query_row(
                "SELECT
                    (SELECT count(*) FROM namespace WHERE fq_name = 'failed') +
                    (SELECT count(*) FROM connection WHERE resource_uri = 'mock://failed') +
                    (SELECT count(*) FROM cartridge WHERE source_uri = 'mock://failed') +
                    (SELECT count(*) FROM mount m JOIN namespace n ON n.id = m.namespace_id
                     WHERE n.fq_name = 'failed')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            leftovers, 0,
            "external mount failure must leave no catalog fragment"
        );
    }
}

/// Validate value-function clause discipline for a multi-clause definition —
/// the FUNCTIONAL half of "The Two Algebras".
///
/// # The fork this function sits on
///
/// A colon-functor `f:(…)` is a VALUE FUNCTION: the grammar parses it as
/// `function_definition`/`constant_definition` → `DefKind::Function` → entity
/// type 1 (`DqlFunctionExpression`). Its clauses are ORDERED first-match
/// alternatives, compiled to a CASE expression, BECAUSE a function is
/// deterministic — it must return exactly one value per input.
///
/// A plain-functor `f(…)` (with a boolean body) is a SIGMA PREDICATE: the
/// grammar parses it as `sigma_definition` → `DefKind::Sigma` → entity
/// type 9 (`DqlTemporarySigmaRule`). Its clauses OR together (the RELATIONAL
/// algebra — independent truths about membership) and are expanded by
/// `resolver::resolving::predicates::expand_consulted_sigma`, never here.
///
/// The two are DISTINCT ENTITY TYPES decided SYNTACTICALLY at parse time (the
/// colon), NOT one definition used context-dependently. So gating this check on
/// `DefKind::Function` is exact: sigma predicates never reach it, and the
/// multi-clause sigma OR (ddl/320) is untouched.
///
/// # The rules
///
/// - **Rule 3 (unguarded multiplicity):** at most ONE unguarded
///   clause. Two or more are indistinguishable — nothing selects between them,
///   and the selection assembler (`grounding::build_case_body_from_clauses`)
///   would emit a guardless `ClauseSelection`, which lowers to the degenerate
///   `CASE ELSE <last> END` (zero WHEN arms → unexecutable
///   SQL). Fires even when NO clause is guarded — an entirely unguarded
///   multi-clause group is exactly as ambiguous as a partially-guarded one with
///   two unguarded arms. Also catches duplicate
///   constants (`nl :- …` twice: a constant is a zero-arity value function).
/// - **Rule 4 (unguarded position):** the single unguarded clause is the
///   default/ELSE and must be last. Guarded clauses are tried first-match; the
///   default is the fallthrough.
///
/// Badging note: Rule 3 uses `ddl/head/unguarded_multiplicity` (mirrors the
/// `semantic/ddl/head/` sibling family — `arity`, `name_conflict`,
/// `unnamed_ground_position`). Rule 4 still carries the generic `parse/general`
/// badge; a future rebadge to `ddl/head/unguarded_position` will follow this
/// function's pattern.
fn validate_function_clause_discipline(
    group: &crate::pipeline::asts::ddl::DefinitionGroup,
) -> Result<()> {
    use crate::pipeline::asts::ddl::{DefKind, HoParam};

    // Fork gate: only value functions (colon-functors). Sigma predicates and any
    // non-function head are validated on their own paths.
    if group.kind() != DefKind::Function {
        return Ok(());
    }
    let defs = group.clauses();
    let name = group.name();

    // A clause is "unguarded" when none of its params carries a guard (a
    // constant's empty param list is vacuously unguarded). Every clause is a
    // function clause: the group is one declared kind by construction.
    let unguarded_indices: Vec<usize> = defs
        .iter()
        .enumerate()
        .filter(|(_, d)| {
            d.params()
                .iter()
                .all(|p| !matches!(p, HoParam::Scalar { guard: Some(_), .. }))
        })
        .map(|(i, _)| i)
        .collect();

    // Rule 3 (RULE 2): at most one unguarded clause.
    if unguarded_indices.len() > 1 {
        return Err(DelightQLError::validation_error_categorized(
            "ddl/head/unguarded_multiplicity",
            format!(
                "Disjunctive definition '{}': found {} unguarded clauses, but a value \
                 function (a colon-functor `{}:(…)`) must return exactly one value per \
                 input. Its clauses are ordered first-match alternatives — the functional \
                 algebra: functions are deterministic, so clause order is load-bearing and \
                 exactly one clause fires. Two or more unguarded clauses are \
                 indistinguishable — nothing selects between them (they would silently \
                 collapse to an unexecutable `CASE ELSE <last-clause> END`). Guard all but \
                 the last: the single unguarded clause is the default/ELSE. (A plain-functor \
                 `{}(…)` would instead be a sigma predicate, whose clauses OR together — the \
                 relational algebra — and this restriction does not apply.) See \
                 clause-head-catechism.md §II, \"The Two Algebras\".",
                name,
                unguarded_indices.len(),
                name,
                name,
            ),
            "Unguarded clause multiplicity",
        ));
    }

    // Rule 4: the single unguarded (default) clause must be last.
    // Badged into the ddl/head family beside unguarded_multiplicity,
    // not the generic parse_error family.
    if let Some(&idx) = unguarded_indices.first() {
        if idx != defs.len() - 1 {
            return Err(DelightQLError::validation_error_categorized(
                "ddl/head/unguarded_position",
                format!(
                    "Disjunctive definition '{}': unguarded clause is at position {} \
                     but must be the last clause (position {}). \
                     Move the default clause to the end.",
                    name,
                    idx + 1,
                    defs.len()
                ),
                "unguarded clause must be last",
            ));
        }
    }

    Ok(())
}

/// Validate effect-algebra discipline for one name-group at consult time —
/// the sibling of `validate_function_clause_discipline`. Covers the
/// per-definition structural rules of the effect algebra:
///
/// - **R1 (purity)**: a rule whose head lacks `!` must not contain a
///   directive. Pinned by the effects ball (rules--25_r1_purity:
///   "its head lacks '!' but its body demands").
/// - **R2 (ending rule)**: an effect body must end in a directive.
///   Pinned by rules--26_r2_ending ("must end in a directive").
/// - **R3 (body grammar)**: enforced structurally by
///   `EffectBody::from_query` (single expression + CTEs).
/// - **R4 (effect labels), refusal half**: a CTE whose expression demands a
///   directive must carry a `!`-marked label. Pinned by rules--27_r4_label
///   ("its label must be '!'-marked"). The WARN half (a `!` label on a pure
///   CTE) is DEFERRED — the runner has no warning channel.
/// - **R9 (session directives are liminal-only)**: session directives and
///   `run!` refuse in effect bodies (`doc!` and `run_namespace!` exempt).
///   Pinned by rules--30_r9_session_in_body ("legal only in the liminal
///   space") and rules--31_r9_run_in_body ("never changes the world it was
///   compiled against").
/// - **F2 (main! single clause)**: pinned by main--23_main_second_clause
///   ("may only be single-claused").
///
/// R7 (vacuity WARN) is deferred with R4's WARN half. R6 (no recursion) is
/// a file-level check — see `validate_effect_rule_recursion`.
///
/// Returns `Some((rule_name, demanded_names))` when the group is an effect
/// rule (the R6 DAG edges), `None` for pure groups.
fn validate_effect_algebra_discipline(
    group: &crate::pipeline::asts::ddl::DefinitionGroup,
) -> Result<Option<(String, Vec<String>)>> {
    use crate::pipeline::asts::ddl::{DdlBody, DefKind};
    use crate::pipeline::asts::effects;

    let defs = group.clauses();
    let name = group.name();

    // --- Pure heads: R1 (purity). ---
    if group.kind() != DefKind::Effect {
        for def in defs {
            // Only relational bodies can demand directives today (scalar
            // function bodies have no relational directive positions).
            if let DdlBody::Relational(ref query) = def.body {
                let invocations = effects::collect_directive_invocations_in_query(query);
                if let Some(inv) = invocations.first() {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/rule/purity",
                        format!(
                            "definition '{}': its head lacks '!' but its body demands \
                             the directive '{}' — a rule without the effect marker must \
                             not contain a directive (EFFECT-ALGEBRA R1). Declare the \
                             effect in the head: '{}!(*) :- …'.",
                            name, inv.name, name
                        ),
                        "directive in a pure rule body",
                    ));
                }
            }
        }
        return Ok(None);
    }

    // --- Effect-rule heads. ---

    // F2: main! may only be single-claused.
    if name == "main!" && defs.len() > 1 {
        return Err(DelightQLError::validation_error_categorized(
            "effect/main/multi_clause",
            format!(
                "effect rule 'main!' may only be single-claused (EFFECT-ALGEBRA F2): \
                 found {} clauses. Split the arms into named effect rules and demand \
                 them from the single main! body.",
                defs.len()
            ),
            "main! may only be single-claused",
        ));
    }

    let rule = effects::EffectRule::from_definition_group(group)?;
    let mut demanded: Vec<String> = Vec::new();

    for clause in &rule.clauses {
        // R2: the body expression must end in a directive.
        if !effects::ends_in_directive(&clause.body.expression) {
            return Err(DelightQLError::validation_error_categorized(
                "effect/rule/ending",
                format!(
                    "effect rule '{}': its body must end in a directive \
                     (EFFECT-ALGEBRA R2). To return an ordinary table, pipe it \
                     through the post-pipe returning! directive: '… |> returning!(*)'.",
                    rule.name
                ),
                "effect body must end in a directive",
            ));
        }

        // R4 (refusal half): an effect-demanding CTE must wear a ! label.
        for cte in clause.body.ctes() {
            if effects::expression_demands_directive(cte.body()) && !cte.subject().declares_effect()
            {
                let name = cte
                    .subject()
                    .authored_name()
                    .expect("effect bodies contain only authored bindings");
                return Err(DelightQLError::validation_error_categorized(
                    "effect/cte/label",
                    format!(
                        "effect rule '{}': the CTE '{}' demands a directive, so \
                         its label must be '!'-marked — write ': {}!' \
                         (EFFECT-ALGEBRA R4).",
                        rule.name, name, name
                    ),
                    "effect CTE without ! label",
                ));
            }
        }

        // R9: session directives (doc! exempt) and run! refuse in bodies.
        for inv in effects::demanded_directive_names(&clause.body) {
            match inv.category {
                effects::DirectiveCategory::Session if inv.name != "doc!" => {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/body/session_directive",
                        format!(
                            "effect rule '{}': its body demands the session directive \
                             '{}'. Session directives alter what the compiler resolves \
                             against — rules, connections, names — and are legal only in \
                             the liminal space and at the REPL/CLI top level \
                             (EFFECT-ALGEBRA R9): mount and consult before the run, not \
                             during it. Shape the session in the file's liminal space \
                             (above the rules) and let the body demand the work. \
                             (Exempt, because they cannot change resolution: doc! — \
                             annotation only — and run_namespace! — its target's rules \
                             already exist when the body is compiled.)",
                            rule.name, inv.name
                        ),
                        "session directive in effect body",
                    ));
                }
                effects::DirectiveCategory::Execution if inv.name == "run!" => {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/body/run",
                        format!(
                            "effect rule '{}': its body demands run!. run! consults its \
                             file, which would extend the set of rules the run was \
                             compiled against while the run is executing (EFFECT-ALGEBRA \
                             R9) — a run never changes the world it was compiled against. \
                             consult! the file in the liminal space and demand \
                             run_namespace!(ns) from the body instead: its target's rules \
                             already exist at compile time.",
                            rule.name
                        ),
                        "run! in effect body",
                    ));
                }
                _ => {}
            }
            demanded.push(inv.name);
        }
    }

    Ok(Some((rule.name, demanded)))
}

/// R6 (no recursion): an effect rule must not invoke itself, directly or
/// transitively — every effect rule expands to a finite static DAG
/// (rule R6: every effect rule expands to a finite demand set). Checked
/// over the effect rules of one consulted file
/// (cross-file cycles are impossible: an already-registered rule was
/// validated against the rules that existed at ITS registration, which
/// cannot include this file's). Pinned by the effects ball
/// (rules--28_r6_recursion: "must not recurse").
fn validate_effect_rule_recursion(edges: &[(String, Vec<String>)]) -> Result<()> {
    use std::collections::HashMap;
    let graph: HashMap<&str, &Vec<String>> = edges.iter().map(|(n, d)| (n.as_str(), d)).collect();

    fn visit<'a>(
        node: &'a str,
        graph: &HashMap<&'a str, &'a Vec<String>>,
        path: &mut Vec<&'a str>,
    ) -> Option<Vec<String>> {
        if let Some(pos) = path.iter().position(|n| *n == node) {
            let mut cycle: Vec<String> = path[pos..].iter().map(|s| s.to_string()).collect();
            cycle.push(node.to_string());
            return Some(cycle);
        }
        let Some(demands) = graph.get(node) else {
            return None;
        };
        path.push(node);
        for next in demands.iter() {
            if graph.contains_key(next.as_str()) {
                if let Some(cycle) = visit(next, graph, path) {
                    return Some(cycle);
                }
            }
        }
        path.pop();
        None
    }

    for (name, _) in edges {
        let mut path = Vec::new();
        if let Some(cycle) = visit(name, &graph, &mut path) {
            return Err(DelightQLError::validation_error_categorized(
                "effect/rule/recursion",
                format!(
                    "effect rule '{}' must not recurse, directly or transitively \
                     (EFFECT-ALGEBRA R6: every effect rule expands to a finite \
                     static DAG). Cycle: {}.",
                    name,
                    cycle.join(" -> ")
                ),
                "recursive effect rule",
            ));
        }
    }
    Ok(())
}

/// Register catalog views in sys::meta at bootstrap time.
///
/// 1. Loads the generator HO view from embedded sys/meta.dql
/// 2. Creates thin wrapper views for every existing namespace
/// 3. Auto-enlists sys::meta into main
///
/// Returns the cartridge_id used for catalog wrapper entities.
fn register_catalog_views(bootstrap_conn: &Connection) -> Result<i32> {
    // Parse and register the generator HO view via consult_file_inner.
    // Shared DDL front end — the same parsing/cleaning seam every DDL entry
    // point uses (autoload, inline DDL, consult!).
    let consulted = crate::bin_cartridge::prelude::consult::Consulted::read_without_directives(
        SYS_META_SOURCE,
        "sys::meta",
    )?;
    // Embedded system modules have no liminal space: they are created by
    // other means, so their liminal is empty.
    let definitions = consulted.into_definitions();
    let count = definitions.len();
    DelightQLSystem::consult_file_inner(
        bootstrap_conn,
        "embedded://sys::meta",
        "sys::meta",
        definitions,
        count,
        None,
        false,
    )?;

    // Create a separate cartridge for the catalog wrapper entities
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (1, ?1, 'catalog://sys::meta', 'sys::meta', 1, 1, 0)",
            rusqlite::params![SourceType::FileBin.as_i32()],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create catalog wrapper cartridge: {}", e),
                e.to_string(),
            )
        })?;
    let catalog_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    // Get sys::meta namespace ID
    let sys_meta_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::meta namespace: {}", e),
                e.to_string(),
            )
        })?;

    // Register a catalog wrapper for every existing namespace
    let mut stmt = bootstrap_conn
        .prepare("SELECT fq_name FROM namespace ORDER BY id")
        .map_err(|e| {
            DelightQLError::database_error("Failed to prepare namespace query", e.to_string())
        })?;
    let ns_names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .map_err(|e| DelightQLError::database_error("Failed to query namespaces", e.to_string()))?
        .filter_map(|r| r.ok())
        .collect();
    drop(stmt);

    for ns_fq in &ns_names {
        register_catalog_wrapper(bootstrap_conn, ns_fq, sys_meta_ns_id, catalog_cartridge_id)?;
    }

    // Auto-enlist sys::meta into `home` — the interactive scope
    // (enlistment edges are owned by the environment they extend).
    let home_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'home'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query home namespace for enlist: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
             VALUES (?1, ?2)",
            [sys_meta_ns_id, home_ns_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to enlist sys::meta into home: {}", e),
                e.to_string(),
            )
        })?;

    // Auto-enlist `main` into `home`: the
    // interactive session's scope is `home`, and bare table names keep
    // working BECAUSE `main` — the default data namespace — is enlisted
    // into it. The edge direction matters: an inverted home→main edge
    // would pretend the session itself was `main`.
    if let Ok(main_ns_id) = bootstrap_conn.query_row(
        "SELECT id FROM namespace WHERE fq_name = 'main'",
        [],
        |row| row.get::<_, i32>(0),
    ) {
        bootstrap_conn
            .execute(
                "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
                 VALUES (?1, ?2)",
                [main_ns_id, home_ns_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to enlist main into home: {}", e),
                    e.to_string(),
                )
            })?;
    }

    debug!(
        "register_catalog_views: Registered {} catalog wrappers, enlisted sys::meta + main into home",
        ns_names.len()
    );

    Ok(catalog_cartridge_id)
}

/// Lazily initialize catalog views. Uses Cell for interior mutability so
/// callers holding &self (e.g. ensure_stdlib_loaded) can trigger initialization.
fn ensure_catalog_initialized(
    catalog_cartridge_id: &Cell<Option<i32>>,
    bootstrap_conn: &Connection,
) -> Result<i32> {
    if let Some(id) = catalog_cartridge_id.get() {
        // Validate before reuse: the Cell can be set inside a mount savepoint that later
        // ROLLS BACK — and SQLite may then REUSE the freed rowid for an
        // unrelated cartridge, so existence of the id alone proves
        // nothing. The row must also carry the catalog cartridge's
        // identity markers (the exact values register_catalog_views
        // stamps). Anything else: drop the cache and re-initialize.
        let is_catalog: bool = bootstrap_conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM cartridge
                 WHERE id = ?1
                   AND source_uri = 'catalog://sys::meta'
                   AND source_ns = 'sys::meta')",
                [id],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to validate catalog cartridge cache",
                    e.to_string(),
                )
            })?;
        if is_catalog {
            return Ok(id);
        }
        catalog_cartridge_id.set(None);
    }
    let id = register_catalog_views(bootstrap_conn)?;
    catalog_cartridge_id.set(Some(id));
    Ok(id)
}

/// Check that a namespace fq_name is not already registered in bootstrap.
/// Returns Ok(()) if available, Err if already taken.
fn ensure_namespace_available(conn: &rusqlite::Connection, fq_name: &str) -> Result<()> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM namespace WHERE fq_name = ?1)",
            [fq_name],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error("Failed to check namespace existence", e.to_string())
        })?;

    if exists {
        return Err(DelightQLError::database_error(
            format!(
                "Namespace '{}' already exists. Cannot register the same namespace twice.",
                fq_name
            ),
            "Duplicate namespace",
        ));
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
        .map_err(|e| {
            DelightQLError::database_error("Failed to check alias collision", e.to_string())
        })?;

    if let Some(target) = alias_target {
        return Err(DelightQLError::database_error(
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

/// The production implementation of the created-object catalog seam. The
/// caller owns the surrounding savepoint; this method only performs catalog
/// writes and returns the first failure so the savepoint can roll back the
/// complete batch.
pub(crate) struct RealCreatedObjectCatalog;

impl CreatedObjectCatalog for RealCreatedObjectCatalog {
    fn reconcile(
        &self,
        catalog: &Connection,
        registrations: &[CreatedObjectRegistration],
    ) -> Result<()> {
        for registration in registrations {
            let cartridge_id: i64 = match catalog
                .query_row(
                    "SELECT id FROM cartridge
                     WHERE source_uri = 'session://materialized' AND connection_id = ?1",
                    [registration.connection_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    DelightQLError::database_error(
                        "query session-materialization cartridge",
                        e.to_string(),
                    )
                })? {
                Some(id) => id,
                None => {
                    catalog
                        .execute(
                            "INSERT INTO cartridge (language, source_type_enum, source_uri, \
                             source_ns, connected, connection_id, is_universal)
                             VALUES (?1, ?2, 'session://materialized', NULL, 1, ?3, 0)",
                            rusqlite::params![
                                3,
                                SourceType::Db.as_i32(),
                                registration.connection_id,
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to create session-materialization cartridge",
                                e.to_string(),
                            )
                        })?;
                    catalog.last_insert_rowid()
                }
            };

            let stale_ids: Vec<i64> = {
                let mut statement = catalog
                    .prepare(
                        "SELECT e.id FROM entity e
                         JOIN activated_entity ae ON ae.entity_id = e.id
                         WHERE ae.namespace_id = ?1 AND e.name = ?2
                           AND e.cartridge_id = ?3",
                    )
                    .map_err(|e| {
                        DelightQLError::database_error("query stale created entity", e.to_string())
                    })?;
                let rows = statement
                    .query_map(
                        rusqlite::params![
                            registration.namespace_id,
                            &registration.name,
                            cartridge_id
                        ],
                        |row| row.get(0),
                    )
                    .map_err(|e| {
                        DelightQLError::database_error("query stale created entity", e.to_string())
                    })?;
                rows.collect::<std::result::Result<Vec<i64>, _>>()
                    .map_err(|e| {
                        DelightQLError::database_error("read stale created entity", e.to_string())
                    })?
            };
            for entity_id in stale_ids {
                for (table, label) in [
                    ("activated_entity", "retire stale activation"),
                    ("entity_attribute", "retire stale attributes"),
                    ("functional_dependency", "retire stale mode"),
                    ("entity_clause", "retire stale clauses"),
                ] {
                    catalog
                        .execute(
                            &format!("DELETE FROM {table} WHERE entity_id = ?1"),
                            [entity_id],
                        )
                        .map_err(|e| DelightQLError::database_error(label, e.to_string()))?;
                }
                catalog
                    .execute("DELETE FROM entity WHERE id = ?1", [entity_id])
                    .map_err(|e| {
                        DelightQLError::database_error("retire stale entity", e.to_string())
                    })?;
            }

            catalog
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        &registration.name,
                        if registration.is_view {
                            crate::enums::EntityType::DbTemporaryView.as_i32()
                        } else {
                            crate::enums::EntityType::DbTemporaryTable.as_i32()
                        },
                        cartridge_id,
                        "Session-materialized by a DDL directive",
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to register created object '{}'", registration.name),
                        e.to_string(),
                    )
                })?;
            let entity_id = catalog.last_insert_rowid();
            for (position, (column_name, column_type)) in registration.attributes.iter().enumerate()
            {
                catalog
                    .execute(
                        "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, \
                         data_type, position, is_nullable, default_value)
                         VALUES (?1, ?2, 'output_column', ?3, ?4, 1, NULL)",
                        rusqlite::params![
                            entity_id,
                            column_name,
                            column_type,
                            position as i64 + 1,
                        ],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!(
                                "Failed to register attribute '{}' for '{}'",
                                column_name, registration.name
                            ),
                            e.to_string(),
                        )
                    })?;
            }
            catalog
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) \
                     VALUES (?1, ?2, ?3)",
                    rusqlite::params![entity_id, registration.namespace_id, cartridge_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to activate created object '{}'", registration.name),
                        e.to_string(),
                    )
                })?;
        }
        Ok(())
    }
}

impl DelightQLSystem {
    /// Read the authoritative mount binding for a namespace.  The connection
    /// is intentionally derived through the cartridge: `mount` owns the
    /// namespace↔cartridge binding, and `cartridge.connection_id` is the
    /// single connection authority.
    fn mount_binding(
        bootstrap_conn: &Connection,
        namespace_id: i64,
    ) -> Result<Option<MountBinding>> {
        bootstrap_conn
            .query_row(
                "SELECT m.namespace_id, m.cartridge_id, c.connection_id,
                        m.attach_alias, m.attachment, m.qualification,
                        m.engine_schema, m.class
                 FROM mount m
                 JOIN cartridge c ON c.id = m.cartridge_id
                 WHERE m.namespace_id = ?1",
                [namespace_id],
                |row| {
                    Ok(MountBinding {
                        namespace_id: row.get(0)?,
                        cartridge_id: row.get(1)?,
                        connection_id: row.get(2)?,
                        attach_alias: row.get(3)?,
                        attachment: row.get(4)?,
                        qualification: row.get(5)?,
                        engine_schema: row.get(6)?,
                        class: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to read namespace mount binding",
                    e.to_string(),
                )
            })
    }

    /// Insert the single binding for a mounted namespace. The UNIQUE/FK
    /// constraints in `mount` make orphaned bindings and a second namespace
    /// on one cartridge loud. A shared attach ALIAS is not among them: one
    /// file may be named by several namespaces, and the alternative — opening
    /// it once per name — is a self-deadlock. Teardown refcounts instead.
    /// Refresh explicitly clears the old row before inserting its
    /// replacement; an unexpected second writer therefore refuses rather
    /// than silently re-pointing a live namespace. Callers must invoke this
    /// inside their catalog transaction.
    fn record_mount_binding(bootstrap_conn: &Connection, binding: &MountBinding) -> Result<()> {
        bootstrap_conn
            .execute(
                "INSERT INTO mount
                 (namespace_id, cartridge_id, attach_alias, attachment,
                  qualification, engine_schema, class)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    binding.namespace_id,
                    binding.cartridge_id,
                    binding.attach_alias,
                    binding.attachment,
                    binding.qualification,
                    binding.engine_schema,
                    binding.class,
                ],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to record mount binding", e.to_string())
            })?;
        Ok(())
    }

    /// Remove a binding before its cartridge/namespace is deleted.  Callers
    /// must invoke this inside the same catalog transaction as the cascade.
    fn clear_mount_binding(bootstrap_conn: &Connection, namespace_id: i64) -> Result<()> {
        bootstrap_conn
            .execute("DELETE FROM mount WHERE namespace_id = ?1", [namespace_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to clear mount binding", e.to_string())
            })?;
        Ok(())
    }

    /// Create a new DelightQL system from an injected connection
    ///
    /// Creates:
    /// 1. Session tables in user database (sys, _c, delightql_diagnostics)
    /// 2. Internal _bootstrap SQLite database (separate, not attached to user DB)
    /// 3. Initializes _bootstrap with meta-circular metadata system
    ///
    /// # Arguments
    /// * `connection` - User database connection trait object (for execution)
    /// * `introspector` - Backend-specific introspector for discovering schema
    /// * `db_type` - Database type string ("sqlite", "duckdb", "postgres")
    ///
    /// # Returns
    /// A DelightQLSystem ready for query execution
    pub fn new(
        connection: Arc<Mutex<dyn DatabaseConnection>>,
        introspector: Box<dyn crate::bootstrap::introspect::DatabaseIntrospector>,
        db_type: &str,
    ) -> Result<Self> {
        // Create internal _bootstrap metadata store (ALWAYS SQLite)
        let bootstrap_conn = Connection::open_in_memory().map_err(|e| {
            DelightQLError::database_error_with_source(
                "Failed to create _bootstrap metadata store",
                format!("SQLite error: {}", e),
                Box::new(e),
            )
        })?;

        // Initialize _bootstrap schema and seed data
        crate::bootstrap::initialize_bootstrap_db(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to initialize _bootstrap schema: {}", e),
                e.to_string(),
            )
        })?;

        // Create session tables on bootstrap (assertions, danger, finding)
        setup_assertions_table_on_bootstrap(&bootstrap_conn)?;
        setup_danger_table_on_bootstrap(&bootstrap_conn)?;
        setup_finding_table_on_bootstrap(&bootstrap_conn)?;

        // Register bootstrap connection (id=1) BEFORE installing cartridge
        // (cartridge has FK to connection)
        let bootstrap_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "session:bootstrap",
            "in-process",
            None,
            5, // bootstrap connection type
            "Internal engine metadata store",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register bootstrap connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // Sanity check: bootstrap connection should always be id=1
        if bootstrap_conn_id != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "Bootstrap connection has unexpected ID: expected id=1, got id={}",
                    bootstrap_conn_id
                ),
                "Internal consistency error".to_string(),
            ));
        }

        // Install bootstrap://sys cartridge and activate entities
        // Note: introspects the _bootstrap database itself (schema = None, it's main)
        let cartridge_id = crate::import::install_cartridge(
            &bootstrap_conn,
            "bootstrap://sys",
            crate::import::SourceType::Db,
            3,       // SQLite language ID
            None,    // _bootstrap tables are in main schema, not attached
            Some(1), // connection_id=1 (bootstrap connection)
            false,   // not universal
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to install bootstrap cartridge: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::create_bootstrap_namespaces(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create bootstrap namespaces: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::activate_bootstrap_entities(&bootstrap_conn, cartridge_id).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate bootstrap entities: {}", e),
                e.to_string(),
            )
        })?;

        // Initialize bin cartridge registry and sync to bootstrap
        let mut bin_registry = crate::bin_cartridge::registry::BinCartridgeRegistry::new();

        // Register the prelude cartridge (contains import!, enlist!, delist!)
        bin_registry.register_cartridge(crate::bin_cartridge::prelude::create_prelude_cartridge());

        // Register the predicates cartridge (contains like(), etc.)
        bin_registry
            .register_cartridge(crate::bin_cartridge::predicates::create_predicates_cartridge());

        // Sync all bin cartridges to bootstrap metadata
        let universal_namespaces =
            crate::bootstrap::sync_bin_cartridges_to_bootstrap(&bootstrap_conn, &bin_registry)
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to sync bin cartridges to bootstrap: {}", e),
                        e.to_string(),
                    )
                })?;
        // Register user connection in bootstrap metadata
        // Determine connection type ID from database type string (case-insensitive)
        let db_type_lower = db_type.to_lowercase();
        let connection_type = match db_type_lower.as_str() {
            "sqlite" => {
                // TODO: Distinguish between file and memory SQLite
                // For now, default to file (type 1)
                1 // sqlite-file
            }
            "duckdb" => 4,
            "postgres" | "postgresql" => 3,
            _ => {
                return Err(DelightQLError::validation_error(
                    "Unsupported database type",
                    format!("Database type '{}' is not supported", db_type),
                ));
            }
        };

        let user_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "session:primary",
            if db_type_lower == "sqlite" {
                "in-process"
            } else {
                "fatboy"
            },
            None,
            connection_type,
            "User target database (pre-mount placeholder)",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register user connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // "main" namespace is created empty by create_bootstrap_namespaces().
        // No user cartridge, no introspection — the CLI sends mount!("path", "main")(*)
        // as its first query to populate the namespace.

        // Register session table metadata in bootstrap so they're queryable via DQL
        // Create a cartridge for the sys schema session tables (on user connection)
        bootstrap_conn
            .execute(
                "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (?1, ?2, 'sys://session', NULL, 1, ?3, 0)",
                rusqlite::params![
                    3, // SQLite language (bootstrap is always SQLite)
                    SourceType::Db.as_i32(),
                    bootstrap_conn_id,
                ],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to create sys session cartridge: {}", e),
                    e.to_string(),
                )
            })?;
        let sys_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

        // Insert assertions entity (type 10 = DBPermanentTable)
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES ('assertions', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.assertions entity: {}", e),
                    e.to_string(),
                )
            })?;
        let assertions_entity_id = bootstrap_conn.last_insert_rowid() as i32;

        // Insert entity clause for assertions
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- sys.assertions system table')",
                rusqlite::params![assertions_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.assertions entity clause: {}", e),
                    e.to_string(),
                )
            })?;

        // Insert column attributes for assertions entity
        let assertion_columns = [
            ("id", "INTEGER", 1, false),
            ("name", "TEXT", 2, true),
            ("source_file", "TEXT", 3, true),
            ("source_line", "INTEGER", 4, true),
            ("body", "TEXT", 5, false),
            ("outcome", "TEXT", 6, false),
            ("detail", "TEXT", 7, true),
            ("run_id", "TEXT", 8, false),
        ];
        for (col_name, data_type, position, nullable) in &assertion_columns {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![
                        assertions_entity_id,
                        col_name,
                        data_type,
                        position,
                        nullable,
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to insert sys.assertions column '{}': {}",
                            col_name, e
                        ),
                        e.to_string(),
                    )
                })?;
        }

        // Insert danger entity (type 10 = DBPermanentTable)
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES ('danger', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger entity: {}", e),
                    e.to_string(),
                )
            })?;
        let danger_entity_id = bootstrap_conn.last_insert_rowid() as i32;

        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- sys.danger system table')",
                rusqlite::params![danger_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger entity clause: {}", e),
                    e.to_string(),
                )
            })?;

        let danger_columns = [
            ("uri", "TEXT", 1, false),
            ("state", "TEXT", 2, false),
            ("cli_overridable", "INTEGER", 3, false),
            ("description", "TEXT", 4, true),
        ];
        for (col_name, data_type, position, nullable) in &danger_columns {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![danger_entity_id, col_name, data_type, position, nullable,],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.danger column '{}': {}", col_name, e),
                        e.to_string(),
                    )
                })?;
        }

        // Insert errors entity (type 10 = DBPermanentTable)
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id)
                 VALUES ('errors', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors entity: {}", e),
                    e.to_string(),
                )
            })?;
        let errors_entity_id = bootstrap_conn.last_insert_rowid() as i32;

        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- sys.errors system table')",
                rusqlite::params![errors_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors entity clause: {}", e),
                    e.to_string(),
                )
            })?;

        let errors_columns = [
            ("id", "INTEGER", 1, false),
            ("uri", "TEXT", 2, false),
            ("message", "TEXT", 3, false),
            ("query_text", "TEXT", 4, true),
            ("timestamp", "TEXT", 5, true),
        ];
        for (col_name, data_type, position, nullable) in &errors_columns {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute
                     (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![errors_entity_id, col_name, data_type, position, nullable,],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.errors column '{}': {}", col_name, e),
                        e.to_string(),
                    )
                })?;
        }

        // Get sys namespace ID and activate sys entities there
        let sys_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to query sys namespace: {}", e),
                    e.to_string(),
                )
            })?;

        crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            sys_cartridge_id,
            sys_ns_id,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys.assertions in sys namespace: {}", e),
                e.to_string(),
            )
        })?;

        // Register the burned identifier table (rows authored in
        // bootstrap/schema.sql) as sys::identifiers.identifier. Its own cartridge so the bulk
        // activation above cannot leak it into bare `sys`.
        register_sys_identifier_table(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::diagnostics.finding: the session's own refusals and selftest
        // findings, queryable. Own cartridge for the same reason.
        register_sys_diagnostics_table(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::format: the burned formatter style-bundle table (book row
        // = frozen defaults).
        register_sys_format_table(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::connections: the curated safe-subset `connection` entity
        // (non-secret columns only). Own cartridge so the bulk activation
        // above cannot leak it into bare `sys`.
        register_sys_connection_table(&bootstrap_conn, bootstrap_conn_id)?;
        // sys::ns: curated public-column `namespace` entity (the physical
        // table carries the internal mount relation).
        register_sys_ns_namespace_table(&bootstrap_conn, bootstrap_conn_id)?;

        // Initialize connection routing map
        let mut connection_map: HashMap<i64, Arc<Mutex<dyn DatabaseConnection>>> = HashMap::new();
        connection_map.insert(user_conn_id, Arc::clone(&connection)); // User connection

        // Installation is complete: SEAL the catalog. Everything the
        // canonical schema authority and the registrations above created is
        // now protected against structural DDL, whatever SQL road reaches
        // this connection. Later lazy loads (stdlib, seeds, catalog views)
        // are row DML and pass untouched.
        let bootstrap_guard = crate::bootstrap::guard::BootstrapGuard::seal(&bootstrap_conn)?;

        let bootstrap_arc = Arc::new(Mutex::new(bootstrap_conn));
        let schema = Box::new(crate::bootstrap_schema::BootstrapBackedSchema::new(
            bootstrap_arc.clone(),
        ));

        let system = DelightQLSystem {
            connection,
            bootstrap_connection: bootstrap_arc,
            schema: Some(schema),
            connection_map,
            introspector,
            bin_registry: Arc::new(bin_registry),
            namespace_authoritative: true,
            connection_factory: None,
            schema_map: HashMap::new(),
            catalog_cartridge_id: Cell::new(None),
            db_type: db_type.to_string(),
            effects_executed: Cell::new(0),
            session_materialized_names: Cell::new(false),
            active_liminal_program: RefCell::new(None),
            session_health: SessionHealth::default(),
            byte_bindings: HashMap::new(),
            bootstrap_guard,
        };

        // Eagerly load stdlib DQL overlays for universal (auto-enlisted) namespaces
        for ns in &universal_namespaces {
            system.ensure_stdlib_loaded(ns);
        }

        Ok(system)
    }

    /// Get the injected database schema
    ///
    /// Returns a reference to the database schema provider that was injected
    /// during system construction. This allows the Pipeline to access schema
    /// information without knowing about concrete backend implementations.
    ///
    /// # Returns
    /// Reference to the DatabaseSchema trait object
    ///
    /// # Errors
    /// Returns error if no schema was injected (old code path)
    pub fn get_schema(&self) -> Result<&dyn DatabaseSchema> {
        self.schema
            .as_ref()
            .ok_or_else(|| {
                DelightQLError::validation_error(
                    "No database schema configured",
                    "Use DelightQLSystem::new_with_schema() to inject a schema",
                )
            })
            .map(|boxed| boxed.as_ref())
    }

    /// Ask the target's live introspector for one relation. Passthrough
    /// accesses use this when the mounted catalog correctly omits a
    /// backend-owned relation that remains directly addressable.
    pub(crate) fn introspect_passthrough_relation(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<delightql_types::introspect::DiscoveredRelation>> {
        self.introspector.introspect_relation(schema, relation_name)
    }

    /// Get a reference to the bootstrap connection (for session tables: assertions, danger, errors).
    pub fn bootstrap_connection(&self) -> &Arc<Mutex<Connection>> {
        &self.bootstrap_connection
    }

    /// The ONE writer of `sys::diagnostics.finding`. Recording never
    /// defeats the caller's real work: a failed insert is dropped, because
    /// the finding is on its way to the caller as an error already.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn record_finding(
        &self,
        kind: crate::diagnostics::Severity,
        uri: &str,
        message: &str,
        input: Option<&str>,
        provider: &str,
    ) {
        let Ok(conn) = self.bootstrap_connection.lock() else {
            return;
        };
        // The engine stamps the row: RFC 3339 UTC to the millisecond, the
        // same shape the client's tables use, without a time dependency.
        let _ = conn.execute(
            "INSERT INTO finding (occurred_at, kind, uri, message, input, provider)
             VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![kind.as_str(), uri, message, input, provider],
        );
    }

    /// Publish `armed` into `sys::execution.compiler_limit`.
    ///
    /// EVERY row and EVERY column comes from the one typed policy: the rows
    /// are [`crate::compiler_limits::ALL`] walked in order, the policy columns
    /// are each resource's descriptor, and the effective value is what the
    /// CALLING COMPILATION armed for that resource. The schema declares the
    /// table and nothing else: a row copied there by hand is a second
    /// authority that a later safety adjustment can leave stale while both
    /// sides still compile.
    ///
    /// The effective value is the caller's and not a fresh read of process
    /// policy, because those are different numbers whenever a host moves a
    /// setting after a compilation's arena is minted — and the catalog is
    /// supposed to answer the compilation reading it, not the next one.
    ///
    /// Best-effort by construction: a catalog that cannot be written must
    /// not fail the compilation, because publishing the policy is not the
    /// policy. The guards themselves read no SQLite.
    pub(crate) fn publish_compiler_limits(&self, armed: &crate::compiler_limits::ArmedLimits) {
        let Ok(conn) = self.bootstrap_connection.lock() else {
            return;
        };
        for kind in crate::compiler_limits::ALL.iter().copied() {
            let limit = kind.descriptor();
            let _ = conn.execute(
                "INSERT INTO compiler_limit
                     (name, default_value, effective_value, hard_ceiling, unit, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(name) DO UPDATE SET
                     default_value   = excluded.default_value,
                     effective_value = excluded.effective_value,
                     hard_ceiling    = excluded.hard_ceiling,
                     unit            = excluded.unit,
                     error           = excluded.error",
                rusqlite::params![
                    limit.name(),
                    limit.default_value() as i64,
                    armed.effective(kind) as i64,
                    limit.ceiling() as i64,
                    limit.unit(),
                    limit.error_identity(),
                ],
            );
        }
    }

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

    /// Refuse a created-object directive before its DDL runs when the target
    /// has no deterministic registration road. Registration is part of the
    /// `table!`/`temp_table!`/`temp_view!` promise: creating an object that
    /// the next statement cannot resolve by name is not a successful effect.
    /// A later `Unsupported` from an otherwise approved read-back is kept as
    /// an invariant-breach path and is handled by the relay's quarantine
    /// boundary instead.
    pub(crate) fn refuse_unregistrable_created_object(
        &self,
        operation: &str,
        target: &str,
        connection_id: Option<i64>,
    ) -> Result<()> {
        let connection_id = connection_id.unwrap_or(PRIMARY_CONNECTION_ID);
        let dialect = self.dialect_for_connection(Some(connection_id));
        let mounted_schema =
            if matches!(dialect, crate::pipeline::generator::SqlDialect::PostgreSQL) {
                self.mounted_engine_schema_for_connection(connection_id)?
            } else {
                None
            };
        let existence_supported =
            created_object_existence_sql_scoped(dialect, target, mounted_schema.as_deref(), None)
                .is_some();
        let readback_supported =
            created_object_readback_sql_scoped(dialect, target, mounted_schema.as_deref(), None)
                .is_some();
        if existence_supported && readback_supported {
            return Ok(());
        }

        let reason = if !existence_supported {
            format!("{} has no target existence probe", dialect.family_name())
        } else {
            format!("{} has no target metadata read-back", dialect.family_name())
        };
        Err(DelightQLError::validation_error_categorized(
            "effect/ddl/created_object_registration_unsupported",
            format!(
                "{operation}!({target}) refuses: this target cannot register created objects; \
                 the object would not resolve by name ({reason})"
            ),
            "created-object registration unsupported",
        ))
    }

    /// The connection id of the session's `main` mount, when that mount is
    /// a FATBOY-backed engine (connection_type 3 = postgres, 4 = duckdb;
    /// namespace.source_path ↔ connection.resource_uri, both written by
    /// `register_external_connection`).
    /// This is the None-plan settling road: an effect plan whose walk
    /// resolved NO connection executes wherever the user pointed dql — the
    /// main mount — ("one plan, one engine"), instead of silently
    /// converging on the in-memory SQLite hub. Deliberately scoped to the
    /// fatboy types: SQLite ATTACH mains live ON the hub (convergence is
    /// correct there), and pipe/siso mains keep today's hub convergence
    /// untouched.
    ///
    /// NOTE: this helper is ATTRIBUTION — it answers WHERE a None-plan
    /// executes, not whether it may. Pinned by
    /// `anon_source_plan_with_fatboy_main_stamps_the_main_connection`
    /// (pipeline/effect_transformer/tests.rs).
    pub fn fatboy_main_connection_for_effect_plan(&self) -> Option<i64> {
        let conn = self.bootstrap_connection.lock().ok()?;
        // Mount identity via the STORED link —
        // main's cartridge records its connection directly, instead of
        // matching source_path against connection.resource_uri strings.
        conn.query_row(
            "SELECT co.id FROM namespace n
             JOIN mount m ON m.namespace_id = n.id
             JOIN cartridge c ON c.id = m.cartridge_id
             JOIN connection co ON co.id = c.connection_id
             WHERE n.fq_name = 'main'
               AND co.connection_type IN (3, 4)
             LIMIT 1",
            [],
            |r| r.get(0),
        )
        .ok()
    }

    /// Is this connection a siso/pipe mount
    /// (connection_type 6)? Effect plans that settle on one refuse at
    /// compile — the siso transport is error-blind, so the
    /// failure-aborts bracket discipline cannot be honored over it. A
    /// `None` connection is never siso: anon-source plans settle only on
    /// fatboy mains (`fatboy_main_connection_for_effect_plan`, types 3/4)
    /// or stay on the hub. Used only by the effect transformer's
    /// `refuse_siso_connection`. Pinned by
    /// `effect_plan_on_siso_connection_refuses` /
    /// `anon_source_plan_with_siso_mount_elsewhere_still_compiles`
    /// (pipeline/effect_transformer/tests.rs).
    pub fn siso_connection_for_effect_plan(&self, connection_id: Option<i64>) -> bool {
        let Some(id) = connection_id else {
            return false;
        };
        let Ok(conn) = self.bootstrap_connection.lock() else {
            return false;
        };
        conn.query_row(
            "SELECT connection_type FROM connection WHERE id = ?1",
            [id],
            |r| r.get::<_, i64>(0),
        )
        .map(|t| t == 6)
        .unwrap_or(false)
    }

    /// Get the schema map for imported connections
    pub fn get_schema_map(&self) -> &HashMap<i64, Box<dyn DatabaseSchema>> {
        &self.schema_map
    }

    /// Register an external connection: introspect, register in bootstrap, activate in namespace.
    /// Used by import! when a ConnectionFactory is available (for delightql-siso://, file://, etc.).
    ///
    /// Returns (connection_id, entity_count) on success.
    pub fn register_external_connection(
        &mut self,
        components: ConnectionComponents,
        namespace: &str,
        connection_uri: &str,
    ) -> Result<(i64, usize)> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for external connection",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Idempotent mount: if namespace already exists with the SAME URI, return
        // existing connection info. If a different URI, that's an error.
        // A namespace that exists but has NO activated entities (e.g. the
        // empty "main" pre-created by open()) is NOT "already mounted" — it
        // is reused and populated below.
        let mut empty_namespace_id: Option<i32> = None;
        {
            // Mount identity via the STORED link — never entity joins.
            let existing: Option<String> = match bootstrap_conn.query_row(
                "SELECT c.source_uri FROM namespace n
                 JOIN mount m ON m.namespace_id = n.id
                 JOIN cartridge c ON c.id = m.cartridge_id
                 WHERE n.fq_name = ?1",
                [namespace],
                |row| row.get(0),
            ) {
                Ok(uri) => Some(uri),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    // No mount link. The namespace may still EXIST — and if
                    // it holds activated entities it is someone else's
                    // (a lib/consult namespace deliberately has a NULL
                    // link): reusing it would flip it to 'data' and mix the
                    // new cartridge into its definitions. Mirror the attach
                    // spine's occupancy check: only
                    // a genuinely EMPTY namespace is reusable.
                    if let Ok(ns_id) = bootstrap_conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = ?1",
                        [namespace],
                        |row| row.get::<_, i32>(0),
                    ) {
                        let occupied: bool = bootstrap_conn
                            .query_row(
                                "SELECT EXISTS(SELECT 1 FROM activated_entity WHERE namespace_id = ?1)",
                                [ns_id],
                                |row| row.get(0),
                            )
                            .map_err(|e| {
                                DelightQLError::database_error(
                                    "Failed to check namespace occupancy",
                                    e.to_string(),
                                )
                            })?;
                        if occupied {
                            return Err(DelightQLError::database_error(
                                format!(
                                    "Namespace '{}' already exists and is in use, cannot mount '{}' over it",
                                    namespace, connection_uri
                                ),
                                "Namespace occupied",
                            ));
                        }
                        empty_namespace_id = Some(ns_id);
                    }
                    None
                }
                Err(e) => {
                    return Err(DelightQLError::database_error(
                        "Failed to check namespace existence",
                        e.to_string(),
                    ));
                }
            };
            if let Some(existing_uri) = existing {
                if existing_uri == connection_uri {
                    // Same database — return existing connection info
                    let conn_id: i64 = bootstrap_conn
                        .query_row(
                            "SELECT id FROM connection WHERE resource_uri = ?1",
                            [connection_uri],
                            |row| row.get(0),
                        )
                        .unwrap_or(0);
                    let entity_count: usize = bootstrap_conn
                        .query_row(
                            "SELECT COUNT(*) FROM namespace n JOIN activated_entity ae ON ae.namespace_id = n.id WHERE n.fq_name = ?1",
                            [namespace],
                            |row| row.get(0),
                        )
                        .unwrap_or(0);
                    drop(bootstrap_conn);
                    return Ok((conn_id, entity_count));
                } else {
                    // Different SPELLING may still be the same RESOURCE
                    // (postgres:///db vs postgres://localhost:5433/db):
                    // compare resource-asserted identity before declaring
                    // a conflict (connect-before-dedupe —
                    // the new connection is already live, so its identity
                    // is in hand).
                    let existing_identity: Option<String> = bootstrap_conn
                        .query_row(
                            "SELECT co.identity FROM namespace n
                             JOIN mount m ON m.namespace_id = n.id
                             JOIN cartridge c ON c.id = m.cartridge_id
                             JOIN connection co ON co.id = c.connection_id
                             WHERE n.fq_name = ?1",
                            [namespace],
                            |row| row.get(0),
                        )
                        .ok()
                        .flatten();
                    if let (Some(new_id), Some(old_id)) =
                        (components.identity.as_deref(), existing_identity.as_deref())
                    {
                        if new_id == old_id {
                            // Same resource, different spelling — idempotent.
                            let conn_id: i64 = bootstrap_conn
                                .query_row(
                                    "SELECT co.id FROM connection co WHERE co.identity = ?1",
                                    [new_id],
                                    |row| row.get(0),
                                )
                                .unwrap_or(0);
                            let entity_count: usize = bootstrap_conn
                                .query_row(
                                    "SELECT COUNT(*) FROM namespace n JOIN activated_entity ae ON ae.namespace_id = n.id WHERE n.fq_name = ?1",
                                    [namespace],
                                    |row| row.get(0),
                                )
                                .unwrap_or(0);
                            drop(bootstrap_conn);
                            return Ok((conn_id, entity_count));
                        }
                    }
                    return Err(DelightQLError::database_error(
                        format!(
                            "Namespace '{}' already exists (mounted from '{}'), cannot re-mount from '{}'",
                            namespace, existing_uri, connection_uri
                        ),
                        "Duplicate namespace with different source",
                    ));
                }
            }
        }

        // Determine connection type from db_type string
        let db_type_lower = components.db_type.to_lowercase();
        let connection_type = match db_type_lower.as_str() {
            "sqlite" => 1,
            "duckdb" => 4,
            "postgres" | "postgresql" => 3,
            other => panic!(
                "catch-all hit in system.rs mount_database: unexpected db_type: {}",
                other
            ),
        };

        // Atomic registration: every bootstrap write
        // from here — connection, cartridge, entities, namespace, link,
        // wrappers — rolls back together on any failure, so a partially
        // registered external mount (e.g. a linked namespace whose
        // connection never reached connection_map) cannot exist.
        let txn = BootstrapTxn::begin(&bootstrap_conn, "external_mount")?;

        // Register the connection in bootstrap
        let connection_id = crate::import::register_connection(
            &bootstrap_conn,
            connection_uri,
            &components.mechanism,
            components.identity.as_deref(),
            connection_type,
            &format!("Mounted database: {}", namespace),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register connection: {}", e),
                e.to_string(),
            )
        })? as i64;

        // Introspect the connection to discover entities
        let entities = components.introspector.introspect_entities().map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to introspect imported database: {}", e),
                e.to_string(),
            )
        })?;

        // Install the source cartridge. `source_ns` remains source metadata;
        // the binding written below owns qualification policy. A specific
        // schema (`#schema` / `mount_tree!`) is retained here
        // for non-mount source consumers too, but mounted reads do not infer
        // policy from this nullable field.
        let cartridge_id = {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                    rusqlite::params![
                        connection_type,
                        crate::bootstrap::SourceType::Db.as_i32(),
                        connection_uri,
                        components.mounted_schema.as_deref(),
                        connection_id,
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert cartridge",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // Insert discovered entities into bootstrap metadata
        let entity_count = entities.len();
        crate::bootstrap::introspect::insert_discovered_entities(
            &bootstrap_conn,
            cartridge_id,
            &entities,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to insert discovered entities: {}", e),
                e.to_string(),
            )
        })?;

        // Create the namespace — or reuse a pre-existing EMPTY one (the
        // "main" namespace open() pre-creates), recording its new source.
        let (namespace_id, created_names) = if let Some(id) = empty_namespace_id {
            bootstrap_conn
                .execute(
                    "UPDATE namespace
                     SET kind = 'data', provenance = 'uri', source_path = ?2
                     WHERE id = ?1",
                    rusqlite::params![id, connection_uri],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to update empty namespace",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            (id, Vec::new())
        } else {
            create_mounted_namespace_path(&bootstrap_conn, namespace, "uri", connection_uri)?
        };

        // Activate all entities from the cartridge in the namespace
        crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            cartridge_id,
            namespace_id,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate entities: {}", e),
                e.to_string(),
            )
        })?;

        // External/factory mounts have no ATTACH alias.  Their qualification
        // policy is the mounted engine schema (or unqualified engine default)
        // and their live connection is retained in the registry-owned maps.
        Self::record_mount_binding(
            &bootstrap_conn,
            &MountBinding::external(
                namespace_id as i64,
                cartridge_id as i64,
                connection_id,
                components.mounted_schema.clone(),
            ),
        )?;

        // Register catalog wrapper for the new namespace (lazy-init catalog if needed)
        let catalog_id = ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)?;
        let sys_meta_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to query sys::meta namespace for catalog wrapper",
                    e.to_string(),
                )
            })?;
        register_mounted_catalog_wrappers(
            &bootstrap_conn,
            namespace,
            &created_names,
            sys_meta_ns_id,
            catalog_id,
        )?;

        debug!(
            "register_external_connection: Registered {} entities in namespace '{}' (connection_id={})",
            entity_count, namespace, connection_id
        );

        // All bootstrap writes landed — commit before touching the
        // in-memory maps, so catalog and maps can never disagree.
        txn.commit()?;

        // Drop bootstrap lock before mutating self's maps
        drop(bootstrap_conn);

        // Store connection and schema in routing maps
        self.connection_map
            .insert(connection_id, components.connection);
        self.schema_map.insert(connection_id, components.schema);
        self.journal_external_connection(connection_id);

        Ok((connection_id, entity_count))
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
        self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                context,
                format!("Connection was poisoned: {e}"),
            )
        })
    }

    /// Get the bin cartridge registry
    ///
    /// Returns a reference to the registry containing all registered bin cartridges
    /// and their entities. Used by the effect executor to look up pseudo-predicates
    /// for execution.
    pub fn bin_registry(&self) -> Arc<crate::bin_cartridge::registry::BinCartridgeRegistry> {
        Arc::clone(&self.bin_registry)
    }

    /// Reset the system to a clean state equivalent to `System::new()`.
    ///
    /// Drops and rebuilds the in-memory bootstrap database, clears session tables,
    /// re-introspects the user connection, and resets all ancillary state.
    /// Used by the server to cheaply reset between test queries (~5ms).
    pub fn reinit_bootstrap(&mut self) -> Result<()> {
        use crate::bootstrap::{
            setup_assertions_table_on_bootstrap, setup_danger_table_on_bootstrap,
            setup_finding_table_on_bootstrap,
        };

        // A quarantined reset first retries every pending inverse. If any
        // inverse still fails, leave the incident and its inventory intact;
        // replacing the catalog cannot make uncertain external state safe.
        self.recover_pending_external_effects()?;

        // 1. DETACH all imported schemas from user connection
        {
            let user_conn = self.connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire user connection lock for reinit",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            // Query PRAGMA database_list and detach everything except "main", "temp", and "sys".
            // "sys" is an in-memory ATTACH used for session tables — we keep it and clear its tables.
            let schemas: Vec<String> = {
                match user_conn.query_all_rows("PRAGMA database_list", &[]) {
                    Ok((_cols, rows)) => rows
                        .iter()
                        .filter_map(|row| row.get(1).and_then(|v| v.as_wire_text()))
                        .filter(|s| s != "main" && s != "temp" && s != "sys")
                        .collect(),
                    Err(_) => Vec::new(),
                }
            };
            for schema in &schemas {
                // A failed DETACH must ABORT the reinit:
                // proceeding would replace the catalog — and every recorded
                // cleanup identity — while the database stays physically
                // attached. Failing here leaves the old catalog intact and
                // consistent with the attachment state.
                if let Err(e) = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]) {
                    return Err(DelightQLError::database_error(
                        format!(
                            "reset aborted: could not DETACH '{}' — the session \
                             catalog is left intact: {}",
                            schema, e
                        ),
                        e.to_string(),
                    ));
                }
            }
        }

        // 2. Create fresh in-memory bootstrap (session tables are created on bootstrap below)
        let bootstrap_conn = Connection::open_in_memory().map_err(|e| {
            DelightQLError::database_error_with_source(
                "Failed to create _bootstrap metadata store during reinit",
                format!("SQLite error: {}", e),
                Box::new(e),
            )
        })?;

        crate::bootstrap::initialize_bootstrap_db(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to initialize _bootstrap schema during reinit: {}",
                    e
                ),
                e.to_string(),
            )
        })?;

        // 3. Create session tables on bootstrap
        setup_assertions_table_on_bootstrap(&bootstrap_conn)?;
        setup_danger_table_on_bootstrap(&bootstrap_conn)?;
        setup_finding_table_on_bootstrap(&bootstrap_conn)?;

        // 4. Register connections (bootstrap=1, user=2)
        let bootstrap_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "session:bootstrap",
            "in-process",
            None,
            5,
            "Internal engine metadata store",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!(
                    "Failed to register bootstrap connection during reinit: {}",
                    e
                ),
                e.to_string(),
            )
        })? as i64;

        if bootstrap_conn_id != 1 {
            return Err(DelightQLError::database_error(
                format!(
                    "Bootstrap connection has unexpected ID during reinit: expected id=1, got id={}",
                    bootstrap_conn_id
                ),
                "Internal consistency error".to_string(),
            ));
        }

        let db_type_lower = self.db_type.to_lowercase();
        let connection_type = match db_type_lower.as_str() {
            "sqlite" => 1,
            "duckdb" => 4,
            "postgres" | "postgresql" => 3,
            _ => {
                return Err(DelightQLError::validation_error(
                    "Unsupported database type during reinit",
                    format!("Database type '{}' is not supported", self.db_type),
                ));
            }
        };

        let user_conn_id = crate::import::register_connection(
            &bootstrap_conn,
            "session:primary",
            if db_type_lower == "sqlite" {
                "in-process"
            } else {
                "fatboy"
            },
            None,
            connection_type,
            "User target database (pre-mount placeholder)",
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register user connection during reinit: {}", e),
                e.to_string(),
            )
        })? as i64;

        // 5. Install bootstrap cartridge, namespaces, entities
        let cartridge_id = crate::import::install_cartridge(
            &bootstrap_conn,
            "bootstrap://sys",
            crate::import::SourceType::Db,
            3,
            None,
            Some(1),
            false,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to install bootstrap cartridge during reinit: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::create_bootstrap_namespaces(&bootstrap_conn).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create bootstrap namespaces during reinit: {}", e),
                e.to_string(),
            )
        })?;

        crate::import::activate_bootstrap_entities(&bootstrap_conn, cartridge_id).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate bootstrap entities during reinit: {}", e),
                e.to_string(),
            )
        })?;

        // 6. Sync bin cartridges
        let universal_namespaces =
            crate::bootstrap::sync_bin_cartridges_to_bootstrap(&bootstrap_conn, &self.bin_registry)
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to sync bin cartridges during reinit: {}", e),
                        e.to_string(),
                    )
                })?;

        // 7. Leave "main" namespace EMPTY — caller is expected to mount! the db they need.
        //    This allows pack-man to reset + mount a different db each time.
        //    The user connection still exists (connection_id=2) for SQL execution;
        //    mount! will register entities and ATTACH the target db.

        // 8. Register session table metadata in bootstrap (sys.assertions, sys.danger, sys.errors)
        bootstrap_conn
            .execute(
                "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (?1, ?2, 'sys://session', NULL, 1, ?3, 0)",
                rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to create sys session cartridge during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        let sys_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

        // Register assertions entity
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES ('assertions', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to insert sys.assertions entity during reinit: {}",
                        e
                    ),
                    e.to_string(),
                )
            })?;
        let assertions_entity_id = bootstrap_conn.last_insert_rowid() as i32;
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, '-- sys.assertions system table')",
                rusqlite::params![assertions_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.assertions clause during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        for (col_name, data_type, position, nullable) in &[
            ("id", "INTEGER", 1, false),
            ("name", "TEXT", 2, true),
            ("source_file", "TEXT", 3, true),
            ("source_line", "INTEGER", 4, true),
            ("body", "TEXT", 5, false),
            ("outcome", "TEXT", 6, false),
            ("detail", "TEXT", 7, true),
            ("run_id", "TEXT", 8, false),
        ] {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable) VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![assertions_entity_id, col_name, data_type, position, nullable],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.assertions column during reinit: {}", e),
                        e.to_string(),
                    )
                })?;
        }

        // Register danger entity
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES ('danger', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger entity during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        let danger_entity_id = bootstrap_conn.last_insert_rowid() as i32;
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, '-- sys.danger system table')",
                rusqlite::params![danger_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.danger clause during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        for (col_name, data_type, position, nullable) in &[
            ("uri", "TEXT", 1, false),
            ("state", "TEXT", 2, false),
            ("cli_overridable", "INTEGER", 3, false),
            ("description", "TEXT", 4, true),
        ] {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable) VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![danger_entity_id, col_name, data_type, position, nullable],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.danger column during reinit: {}", e),
                        e.to_string(),
                    )
                })?;
        }

        // Register errors entity
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES ('errors', 10, ?1)",
                rusqlite::params![sys_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors entity during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        let errors_entity_id = bootstrap_conn.last_insert_rowid() as i32;
        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, 1, '-- sys.errors system table')",
                rusqlite::params![errors_entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys.errors clause during reinit: {}", e),
                    e.to_string(),
                )
            })?;
        for (col_name, data_type, position, nullable) in &[
            ("id", "INTEGER", 1, false),
            ("uri", "TEXT", 2, false),
            ("message", "TEXT", 3, false),
            ("query_text", "TEXT", 4, true),
            ("timestamp", "TEXT", 5, true),
        ] {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable) VALUES (?1, ?2, 'output_column', ?3, ?4, ?5)",
                    rusqlite::params![errors_entity_id, col_name, data_type, position, nullable],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert sys.errors column during reinit: {}", e),
                        e.to_string(),
                    )
                })?;
        }

        // Activate sys entities
        let sys_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'sys'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to query sys namespace during reinit: {}", e),
                    e.to_string(),
                )
            })?;

        crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            sys_cartridge_id,
            sys_ns_id,
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to activate sys entities during reinit: {}", e),
                e.to_string(),
            )
        })?;

        // sys::format: burned formatter style bundles (mirrors the
        // primary bootstrap path).
        register_sys_format_table(&bootstrap_conn, bootstrap_conn_id)?;
        // sys::connections: curated safe-subset `connection` entity, own
        // cartridge (mirrors the primary bootstrap path).
        register_sys_connection_table(&bootstrap_conn, bootstrap_conn_id)?;
        // sys::ns: curated public-column `namespace` entity (the physical
        // table carries the internal mount relation).
        register_sys_ns_namespace_table(&bootstrap_conn, bootstrap_conn_id)?;

        // Installation is complete: SEAL the rebuilt catalog exactly as
        // construction seals a fresh one (the later steps are row DML).
        self.bootstrap_guard = crate::bootstrap::guard::BootstrapGuard::seal(&bootstrap_conn)?;

        // 9. Swap bootstrap connection
        *self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for reinit swap",
                format!("Connection was poisoned: {}", e),
            )
        })? = bootstrap_conn;

        // 10. Reset ancillary state
        self.connection_map.clear();
        self.connection_map
            .insert(user_conn_id, Arc::clone(&self.connection));
        self.schema_map.clear();
        self.schema = Some(Box::new(
            crate::bootstrap_schema::BootstrapBackedSchema::new(self.bootstrap_connection.clone()),
        )); // Empty until mount! runs again
        self.catalog_cartridge_id.set(None);

        // 11. Eagerly load stdlib DQL overlays for universal namespaces
        //     (mirrors the same step in DelightQLSystem::new)
        for ns in &universal_namespaces {
            self.ensure_stdlib_loaded(ns);
        }

        // 12. Run embedded seed programs for their effects (idempotent).
        //     Mirrors the post-construction seed step in open().
        self.run_seed_programs()?;

        // Reset is the recovery boundary. Do not clear a quarantine before
        // every rebuild step above has succeeded.
        self.active_liminal_program.replace(None);
        self.session_health = SessionHealth::Healthy;

        Ok(())
    }

    /// Ensure a stdlib module is consulted into the bootstrap DB, if it exists.
    ///
    /// Checks whether `namespace_fq` matches a stdlib module (e.g., "std::info").
    /// If the namespace doesn't yet exist in the bootstrap DB but a matching
    /// embedded module is available, consults it on the fly.
    ///
    /// Ensure catalog views (sys::meta) are initialized.
    /// Called lazily on first access to sys::meta entities.
    /// TEST-HARNESS ONLY: the definition-catalog write capability, for
    /// tests that seed catalog state directly. Production writers reach
    /// the window through the PRIVATE `bootstrap_guard` field — possession
    /// of the guard handle is the capability, and no production accessor
    /// hands it out, so compiler code cannot open the fence.
    #[cfg(test)]
    pub(crate) fn catalog_window(&self) -> crate::bootstrap::guard::CatalogWindow {
        self.bootstrap_guard.catalog_window()
    }

    pub fn ensure_catalog_loaded(&self) {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        if self.catalog_cartridge_id.get().is_some() {
            return;
        }
        if let Ok(conn) = self.bootstrap_connection.lock() {
            let _ = ensure_catalog_initialized(&self.catalog_cartridge_id, &conn);
        }
    }

    /// Lazily load an embedded stdlib/autoload module for `namespace_fq`.
    ///
    /// Returns a [`StdlibLoad`] rather than a bool: the old boolean crushed
    /// "not a stdlib namespace", "already loaded", and "failed to load" into
    /// one `false`, so a broken autoload was indistinguishable from an
    /// absent one and surfaced only as a misleading `Table not found`. The
    /// `Failed` variant carries the parse/consult cause so callers can
    /// surface it. A newly-loaded module is [`StdlibLoad::Loaded`] (caller
    /// should retry the lookup).
    pub fn ensure_stdlib_loaded(&self, namespace_fq: &str) -> StdlibLoad {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // Find matching embedded module (covers std::*, sys::*, etc.)
        let module = crate::stdlib_manifest::STDLIB_MODULES
            .iter()
            .find(|(ns, _)| *ns == namespace_fq);

        let Some((_namespace, source)) = module else {
            return StdlibLoad::NotAModule;
        };

        // Check if already loaded (namespace row exists in bootstrap DB)
        let bootstrap_conn = match self.bootstrap_connection.lock() {
            Ok(c) => c,
            Err(_) => return StdlibLoad::NotAModule,
        };

        let source_uri = format!("embedded://{}", namespace_fq);
        let already_loaded: bool = bootstrap_conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM cartridge WHERE source_uri = ?1 AND source_ns = ?2",
                rusqlite::params![&source_uri, namespace_fq],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if already_loaded {
            return StdlibLoad::AlreadyLoaded;
        }

        // Consult the module. Route through the shared DDL front end
        // so autoloads parse identically to
        // consult!() files — same whitespace handling, and embedded
        // directives are refused loudly rather than silently misparsed.
        let consulted =
            match crate::bin_cartridge::prelude::consult::Consulted::read_without_directives(
                source,
                &format!("autoload module '{namespace_fq}'"),
            ) {
                Ok(d) => d,
                Err(e) => {
                    report_stdlib_load_failure(namespace_fq, &e);
                    return StdlibLoad::Failed {
                        phase: LoadPhase::Parse,
                        error: e,
                    };
                }
            };

        // Autoload modules have no liminal space: they are created by other
        // means, so their liminal is empty.
        let definitions = consulted.into_definitions();
        let count = definitions.len();
        let path = format!("embedded://{}", namespace_fq);

        let transaction = match CatalogSavepoint::begin(
            &bootstrap_conn,
            "dql_stdlib_load",
            "Failed to begin stdlib load transaction",
        ) {
            Ok(transaction) => transaction,
            Err(error) => {
                report_stdlib_load_failure(namespace_fq, &error);
                return StdlibLoad::Failed {
                    phase: LoadPhase::Consult,
                    error,
                };
            }
        };

        match Self::consult_file_inner(
            &bootstrap_conn,
            &path,
            namespace_fq,
            definitions,
            count,
            None,
            false,
        ) {
            Ok(_) => {
                // Register catalog wrapper for the newly-loaded stdlib namespace
                if let Ok(catalog_id) =
                    ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)
                {
                    if let Ok(sys_meta_ns_id) = bootstrap_conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                        [],
                        |row| row.get::<_, i32>(0),
                    ) {
                        let _ = register_catalog_wrapper(
                            &bootstrap_conn,
                            namespace_fq,
                            sys_meta_ns_id,
                            catalog_id,
                        );
                    }
                }
                match transaction.commit("Failed to commit stdlib load transaction") {
                    Ok(()) => StdlibLoad::Loaded,
                    Err(error) => {
                        report_stdlib_load_failure(namespace_fq, &error);
                        StdlibLoad::Failed {
                            phase: LoadPhase::Consult,
                            error,
                        }
                    }
                }
            }
            Err(e) => {
                drop(transaction);
                report_stdlib_load_failure(namespace_fq, &e);
                StdlibLoad::Failed {
                    phase: LoadPhase::Consult,
                    error: e,
                }
            }
        }
    }

    /// Compile and execute the effects of every statement in a seed program.
    ///
    /// Seed programs (the embedded `seed/` bucket) are effect programs RUN at
    /// startup — distinct from autoload modules, which INSTALL definitions via
    /// consult. This is the core-internal entry reachable from both `open()`
    /// (system fully built) and `reinit_bootstrap` (`&mut self`, no handle),
    /// mirroring what `session.query()` does under the hood minus result
    /// formatting: split the source into statements, then for each statement
    /// build the unresolved AST and run the effect executor.
    ///
    /// Seeds run on EVERY startup and every reinit, so each program must be
    /// idempotent. `doc!` qualifies (setting the same doc is a no-op-in-effect).
    pub fn run_seed_program(&mut self, src: &str) -> Result<()> {
        // The arena is minted first because it is where this compilation arms
        // its limits, and the extent stays open across the parse and every
        // statement's effects — a seed is one compilation, judged under one
        // depth.
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let _running = crate::compiler_limits::Running::under(registry.limits_shared());

        // A seed program is a SEQUENCE of statements run in order, which is
        // the utility entrance's whole purpose.
        let tree = crate::pipeline::parse::query_sequence(src).map_err(|e| {
            DelightQLError::database_error(
                format!("seed program failed to parse: {}", e),
                "Seed parse error",
            )
        })?;

        let normalized = crate::pipeline::normalize::query_sequence(&tree, registry.names())
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("seed program failed to build AST: {}", e),
                    "Seed build error",
                )
            })?;

        for (idx, goal) in normalized.into_queries().into_iter().enumerate() {
            let query = goal.query;
            // A seed statement exists solely for its effects. If executing it
            // fires zero effects, it is a typo by definition — a mistyped
            // directive (e.g. `doc(...)` for `doc!(...)(*)`) parses as a plain
            // table read and is silently discarded — a quiet failure invisible
            // without this check. Refuse loudly, naming the offending statement; the
            // caller (`run_seed_programs`) prepends the culprit seed's name.
            let before = self.effects_executed_count();
            crate::pipeline::effect_executor::execute_effects(query, self, registry.shared())?;
            if self.effects_executed_count() == before {
                return Err(DelightQLError::database_error(
                    format!(
                        "statement #{} produced no effects — a seed statement \
                         must be effectful; a directive mistyped without its \
                         `!` parses as a plain table read and is silently \
                         discarded",
                        idx + 1
                    ),
                    "Zero-effect seed statement",
                ));
            }
        }

        Ok(())
    }

    /// Record that one Effect-Executor effect (a pseudo-predicate or directive
    /// terminal) actually executed. Called at every `EffectExecutable::execute`
    /// site in the effect executor. See `effects_executed`.
    pub(crate) fn note_effect_executed(&self) {
        self.effects_executed.set(self.effects_executed.get() + 1);
    }

    /// Monotonic count of effects executed since construction.
    pub(crate) fn effects_executed_count(&self) -> u64 {
        self.effects_executed.get()
    }

    /// Run every embedded seed program (the `seed/` bucket) for its effects.
    ///
    /// Called after the system is fully constructed. Seeds are idempotent, so
    /// this is safe to invoke on both fresh `open()` and `reinit_bootstrap`.
    pub fn run_seed_programs(&mut self) -> Result<()> {
        for (name, source) in crate::seed_manifest::SEED_PROGRAMS {
            self.run_seed_program(source).map_err(|e| {
                DelightQLError::database_error(
                    format!("seed program '{}' failed: {}", name, e),
                    "Seed execution error",
                )
            })?;
        }
        Ok(())
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
                DelightQLError::validation_error(
                    "Unknown connection ID",
                    format!(
                        "Connection ID {} is not recognized. Valid IDs: 1 (bootstrap), 2 (user)",
                        connection_id
                    ),
                )
            })
    }

    /// Mount a database and register it with a namespace
    ///
    /// Install the connection factory used to mount URI-scheme databases
    /// (`delightql-siso://`, etc.). Without it, `mount_database` on a URI errors with
    /// "connection factory not available in this context". Installed by
    /// `open()` when the embedding provides a types-level factory.
    pub fn set_connection_factory(&mut self, factory: Box<dyn ConnectionFactory>) {
        self.connection_factory = Some(factory);
    }

    /// This is called by the `mount!()` pseudo-predicate to:
    /// 1. Open a database connection at the specified path or URI
    /// 2. Register it in the bootstrap connection table
    /// 3. Introspect its schema and install as a cartridge
    /// 4. Activate all entities into the specified namespace
    /// 5. Add the connection to the routing map
    ///
    /// # Arguments
    /// * `db_path` - Path to the database file or URI (e.g., "delightql-siso://snowflake")
    /// * `namespace` - Namespace name to register (e.g., "mfg", "sales")
    ///
    /// # Returns
    /// * `Ok(())` - Database successfully mounted and namespace registered
    /// * `Err(...)` - If database cannot be opened, introspected, or registered
    ///
    /// # Example
    /// ```ignore
    /// system.mount_database("./data.db", "mydata")?;
    /// // Now can query: mydata::users(*)
    /// ```
    /// `mount_tree!()`'s system half:
    /// enumerate the target's PERSISTENT schemas and bind one sub-namespace
    /// per schema (`namespace::<schema>`), ALL on ONE connection.
    ///
    /// The factory's `create_tree` opens a single connection (one fatboy
    /// child) and hands back one `ConnectionComponents` per schema, every
    /// one carrying the SAME resource identity. `register_external_connection`
    /// deduplicates the bootstrap `connection` row by identity, so every
    /// sub-namespace lands on ONE `connection_id` (a cross-schema
    /// `run!` is a single-connection, one-bracket plan). Returns the created
    /// sub-namespaces in enumeration order (for the receipt's JSON array).
    /// SQLite/siso targets refuse inside `create_tree`.
    pub fn mount_database_tree(&mut self, uri: &str, namespace: &str) -> Result<Vec<String>> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // System name guard: the USER-TYPED root may not take over a reserved
        // system name (the sub-namespaces derive from it).
        validate_user_namespace_target(namespace)?;

        // Enumerate + build per-schema components (all sharing one child).
        // The factory borrow ends here (NLL), freeing `&mut self` for the
        // registration loop below (mount_database's own pattern).
        let per_schema = {
            let factory = self.connection_factory.as_ref().ok_or_else(|| {
                DelightQLError::validation_error(
                    format!(
                        "Cannot mount_tree! '{}': URI schemes require a connection factory \
                         (not available in this context)",
                        uri
                    ),
                    "No connection factory configured",
                )
            })?;
            factory.create_tree(uri).map_err(|e| {
                DelightQLError::database_error(
                    format!("mount_tree!() failed for '{}': {}", uri, e),
                    e.to_string(),
                )
            })?
        };

        if per_schema.is_empty() {
            return Err(DelightQLError::database_error(
                format!("mount_tree!() found no persistent schemas on '{}'", uri),
                "Empty schema tree",
            ));
        }

        let mut created = Vec::with_capacity(per_schema.len());
        for (schema, components) in per_schema {
            let sub_ns = format!("{}::{}", namespace, schema);
            self.register_external_connection(components, &sub_ns, uri)?;
            created.push(sub_ns);
        }
        Ok(created)
    }

    /// Bind a static database image under a host-chosen name, resolvable by
    /// `mount!("delightql-bytes://<name>", ...)(*)`.
    /// Names are lowercase capability labels (`[a-z][a-z0-9._-]*`) and are
    /// immutable for the life of the handle: rebinding refuses, even to the
    /// same bytes, so a locator's referent can never change underneath a
    /// mounted namespace.
    pub fn bind_static_bytes(&mut self, name: &str, bytes: &'static [u8]) -> Result<()> {
        if !valid_byte_binding_name(name) {
            return Err(DelightQLError::validation_error(
                format!(
                    "invalid byte-binding name '{}': expected [a-z][a-z0-9._-]*",
                    name
                ),
                "Binding names are lowercase capability labels",
            ));
        }
        if self.byte_bindings.contains_key(name) {
            return Err(DelightQLError::validation_error(
                format!(
                    "byte binding '{}' already exists — bindings are immutable",
                    name
                ),
                "Rebinding refuses so a locator's referent cannot change",
            ));
        }
        validate_sqlite_image(name, bytes)?;
        self.byte_bindings
            .insert(name.to_string(), ByteBinding::Static(bytes));
        Ok(())
    }

    /// Owned-buffer sibling of `bind_static_bytes` (same grammar,
    /// immutability, and bind-time validation): for images built at
    /// runtime, e.g. the CLI's live surface database. The buffer is copied
    /// into SQLite-owned memory at attach.
    pub fn bind_owned_bytes(&mut self, name: &str, bytes: Vec<u8>) -> Result<()> {
        if !valid_byte_binding_name(name) {
            return Err(DelightQLError::validation_error(
                format!(
                    "invalid byte-binding name '{}': expected [a-z][a-z0-9._-]*",
                    name
                ),
                "Binding names are lowercase capability labels",
            ));
        }
        if self.byte_bindings.contains_key(name) {
            return Err(DelightQLError::validation_error(
                format!(
                    "byte binding '{}' already exists — bindings are immutable",
                    name
                ),
                "Rebinding refuses so a locator's referent cannot change",
            ));
        }
        validate_sqlite_image(name, &bytes)?;
        self.byte_bindings.insert(
            name.to_string(),
            ByteBinding::Owned(std::sync::Arc::from(bytes)),
        );
        Ok(())
    }

    pub fn mount_database(&mut self, db_path: &str, namespace: &str) -> Result<()> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // System name guard: a USER-TYPED mount target
        // may not take over or nest under a reserved system name. mount_database
        // is only reached from the user-facing mount! verb (surface + embedded
        // directive), never from system-minted machinery.
        validate_user_namespace_target(namespace)?;

        // delightql-bytes:// resolves BEFORE the generic URI→factory routing:
        // it is attach-class — the image joins the
        // session connection's schema space so the mounted namespace is
        // joinable — never a separate factory-created backend.
        if let Some(binding_name) = db_path.strip_prefix("delightql-bytes://") {
            return self.mount_database_from_static_bytes(binding_name, namespace);
        }

        // If a ConnectionFactory is available and the path looks like a URI scheme,
        // use the factory path (supports delightql-siso://, postgres://, etc.)
        let has_uri_scheme = db_path.contains("://");
        if has_uri_scheme {
            if let Some(factory) = self.connection_factory.as_ref() {
                let components = factory.create(db_path).map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to create connection for '{}': {}", db_path, e),
                        e.to_string(),
                    )
                })?;
                self.register_external_connection(components, namespace, db_path)?;
                return Ok(());
            } else {
                return Err(DelightQLError::validation_error(
                    format!(
                        "Cannot mount '{}': URI schemes require a connection factory (not available in this context)",
                        db_path
                    ),
                    "No connection factory configured",
                ));
            }
        }

        // Plain file path: use the existing ATTACH DATABASE path (SQLite-to-SQLite optimization)

        // Resolve relative path against session CWD (for test isolation).
        let resolved_path = crate::session_cwd::resolve_path(db_path);
        let db_path = resolved_path.display().to_string();
        let db_path = db_path.as_str();

        // Guard: file must exist and be a valid SQLite database
        let path = std::path::Path::new(db_path);
        if !path.exists() {
            return Err(DelightQLError::database_error(
                format!(
                    "mount!() failed: file '{}' does not exist. \
                     Use create!() to make a new database.",
                    db_path
                ),
                "File not found",
            ));
        }
        {
            use std::io::Read;
            let mut file = std::fs::File::open(path).map_err(|e| {
                DelightQLError::database_error(
                    format!("mount!() failed: cannot open '{}': {}", db_path, e),
                    "File open failed",
                )
            })?;
            let mut header = [0u8; 16];
            let bytes_read = file.read(&mut header).map_err(|e| {
                DelightQLError::database_error(
                    format!("mount!() failed: cannot read '{}': {}", db_path, e),
                    "File read failed",
                )
            })?;
            // DuckDB file (magic "DUCK" at offset 8): route through the
            // connection factory like any external resource — the factory
            // classifies the path and picks the duckdb adapter
            // (resource-first surface).
            if bytes_read >= 12 && &header[8..12] == b"DUCK" {
                if let Some(factory) = self.connection_factory.as_ref() {
                    let components = factory.create(db_path).map_err(|e| {
                        DelightQLError::database_error(
                            format!("mount!() failed for '{}': {}", db_path, e),
                            e.to_string(),
                        )
                    })?;
                    self.register_external_connection(components, namespace, db_path)?;
                    return Ok(());
                }
                return Err(DelightQLError::database_error(
                    format!(
                        "mount!() failed: '{}' is a DuckDB database but no \
                         connection factory is available",
                        db_path
                    ),
                    "No connection factory",
                ));
            }
            // mount! is attach-only. An empty (0-byte, e.g. /dev/null) or
            // short file is not a
            // valid SQLite database and is rejected here — create intent
            // belongs to mount_new!, which materializes a valid header first.
            // Pinned by new_test_suite/balls/ddl_bugs/bug_nullmount--02 and
            // crates/delightql-cli/tests/mount_validation.rs.
            if bytes_read < 16 || &header != b"SQLite format 3\0" {
                return Err(DelightQLError::database_error(
                    format!(
                        "mount!() failed: '{}' is not a valid SQLite database",
                        db_path
                    ),
                    "Invalid database file",
                ));
            }
        }

        // ── Everything past validation is the shared attach-class spine:
        // authoritative idempotency, atomic
        // registration, one unforgettable tail. Only the file-mount
        // specifics live here, as closures.
        let attach_identity = std::fs::canonicalize(db_path)
            .ok()
            .map(|abs| format!("realpath:{}", abs.display()));
        let same_path = db_path.to_string();
        let same_source = move |existing: &str| -> bool {
            // Different SPELLING may still be the same FILE (the symlink
            // trap): compare filesystem identity.
            if existing == same_path {
                return true;
            }
            match (
                std::fs::canonicalize(existing),
                std::fs::canonicalize(&same_path),
            ) {
                (Ok(a), Ok(b)) => a == b,
                _ => false,
            }
        };
        let existing_schema = self.open_schema_for_file(db_path);
        let attach_path = db_path.to_string();
        let attach = move |conn: &dyn DatabaseConnection, alias: &str| -> Result<()> {
            conn.execute(
                &format!("ATTACH DATABASE '{}' AS '{}'", attach_path, alias),
                &[],
            )
            .map(|_| ())
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to attach database: {}", e),
                    e.to_string(),
                )
            })
        };
        self.mount_attach_class(
            AttachClassMount {
                namespace,
                source_uri: format!("file://{}", db_path),
                source_path: db_path.to_string(),
                provenance: "file",
                conn_resource: db_path,
                conn_mechanism: "attach",
                conn_identity: attach_identity,
                conn_description: format!("Mounted database: {}", namespace),
                existing_schema,
            },
            &same_source,
            &attach,
        )
    }

    /// The engine schema the session connection ALREADY holds this file
    /// under, if any.
    ///
    /// The connection answers, not the catalog: `PRAGMA database_list` is
    /// where the session's own `main` shows up, and the session's main is the
    /// case the catalog cannot see — nothing mounted it, so no namespace
    /// records it. Filesystem identity, not spelling, because `main.sqlite`
    /// and `./main.sqlite` and a symlink to either are one file.
    ///
    /// `None` for anything unresolvable: a connection that cannot answer, a
    /// path that cannot be canonicalized, an in-memory or temp schema (empty
    /// file). Not knowing means attaching, which is what happened before.
    fn open_schema_for_file(&self, db_path: &str) -> Option<String> {
        let want = std::fs::canonicalize(db_path).ok()?;
        let guard = self.connection.lock().ok()?;
        let (_cols, rows) = guard.query_all_rows("PRAGMA database_list", &[]).ok()?;
        rows.iter().find_map(|row| {
            let alias = row.get(1)?.as_wire_text()?;
            let file = row.get(2)?.as_wire_text()?;
            if file.is_empty() {
                return None;
            }
            (std::fs::canonicalize(file).ok()? == want).then_some(alias)
        })
    }

    /// The shared attach-class mount spine. Every
    /// attach-class mount path (file, `delightql-bytes://`; `mount_new!` by
    /// delegation) runs through here, so the recipe is correct exactly once:
    ///
    /// - IDENTITY IS AUTHORITATIVE: idempotency consults
    ///   `namespace.source_path` — never `activated_entity` joins — so a
    ///   valid-but-empty database has an identity too.
    /// - FAILURE IS ATOMIC: all bootstrap writes run inside one transaction,
    ///   and any failure after attachment rolls the metadata back AND
    ///   detaches the alias — a failed mount leaves nothing behind.
    /// - THE TAIL IS UNFORGETTABLE: connection → cartridge → entities →
    ///   namespace → activation → catalog wrappers → schema refresh happen
    ///   here, so a new mount path cannot partially transcribe the recipe
    ///   (the `m::(*)` bug class).
    fn mount_attach_class(
        &mut self,
        m: AttachClassMount<'_>,
        same_source: &dyn Fn(&str) -> bool,
        attach: &dyn Fn(&dyn DatabaseConnection, &str) -> Result<()>,
    ) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for mount",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // ── Authoritative idempotency (namespace.source_path) ──
        let existing: Option<(i32, Option<String>)> = match bootstrap_conn.query_row(
            "SELECT id, source_path FROM namespace WHERE fq_name = ?1",
            [m.namespace],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ) {
            Ok(pair) => Some(pair),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(e) => {
                return Err(DelightQLError::database_error(
                    "Failed to check namespace existence",
                    e.to_string(),
                ));
            }
        };
        let existing_namespace_id: Option<i32> = match existing {
            None => None,
            Some((_, Some(ref existing_source))) if !existing_source.is_empty() => {
                if same_source(existing_source) {
                    // Same resource (any spelling) — idempotent, skip.
                    drop(bootstrap_conn);
                    return Ok(());
                }
                return Err(DelightQLError::database_error(
                    format!(
                        "Namespace '{}' already exists (mounted from '{}'), cannot re-mount from '{}'",
                        m.namespace, existing_source, m.source_uri
                    ),
                    "Duplicate namespace with different source",
                ));
            }
            Some((ns_id, _)) => {
                // Namespace exists with no recorded source (e.g. a
                // pre-created empty "main"). Reusable only while nothing is
                // activated in it — an occupied namespace is someone else's.
                let occupied: bool = bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM activated_entity WHERE namespace_id = ?1)",
                        [ns_id],
                        |row| row.get(0),
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to check namespace occupancy",
                            e.to_string(),
                        )
                    })?;
                if occupied {
                    return Err(DelightQLError::database_error(
                        format!(
                            "Namespace '{}' already exists and is in use, cannot mount '{}' over it",
                            m.namespace, m.source_uri
                        ),
                        "Namespace occupied",
                    ));
                }
                Some(ns_id)
            }
        };

        // Auto-generate unique SQLite schema alias
        let next_id: i32 = bootstrap_conn
            .query_row(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM cartridge",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to query next cartridge ID",
                    e.to_string(),
                    Box::new(e),
                )
            })?;
        let schema_alias = match &m.existing_schema {
            Some(alias) => alias.clone(),
            None => format!("_imported_{}", next_id),
        };
        debug!(
            "mount_attach_class: alias {} for {} -> {}",
            schema_alias, m.source_uri, m.namespace
        );

        // The identity check and alias allocation are complete. Release the
        // catalog lock before touching the target and before any explicit
        // inverse may need to update session health.
        drop(bootstrap_conn);

        // ── Attach, unless the connection already holds this resource. No
        // bootstrap writes have happened yet, so an attach failure needs no
        // rollback. ──
        if m.existing_schema.is_none() {
            {
                let user_conn = self.connection.lock().map_err(|e| {
                    DelightQLError::connection_poison_error(
                        "Failed to acquire user connection lock",
                        format!("Connection was poisoned: {}", e),
                    )
                })?;
                attach(&*user_conn, &schema_alias)?;
            }
            // A liminal program abort must detach what it attached; the
            // journal rollback is best-effort, so a mount failure that
            // already detached the alias makes the abort's DETACH a harmless
            // no-op.
            self.journal_attached_sqlite(schema_alias.clone());
        }

        // ── Registration, atomically: one bootstrap transaction, with
        // the alias guard armed from here — BEGIN failure, registration
        // failure, and COMMIT failure all detach on every exit path.
        //
        // Armed only for a schema THIS mount attached. Detaching one it found
        // already open would close a database somebody else is standing on —
        // and for the session's own `main` there is nothing to detach at all.
        let mut alias_guard = DetachOnDrop {
            connection: Arc::clone(&self.connection),
            alias: &schema_alias,
            armed: m.existing_schema.is_none(),
        };
        let registration = (|| -> Result<()> {
            let bootstrap_conn = self.bootstrap_connection.lock().map_err(|error| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire bootstrap database lock for mount registration",
                    format!("Connection was poisoned: {error}"),
                )
            })?;
            // A nestable savepoint, not BEGIN: mount! is liminal-eligible, so
            // this registration may run INSIDE the liminal-program savepoint
            // (where a raw BEGIN is "cannot start a transaction within a
            // transaction" — the semantic merge conflict between the mount
            // reification and the program spine).
            if let Err(error) = bootstrap_conn.execute_batch("SAVEPOINT dql_mount_attach") {
                return Err(DelightQLError::database_error(
                    "Failed to begin mount transaction",
                    error.to_string(),
                ));
            }
            let transaction = CatalogSavepoint {
                conn: &bootstrap_conn,
                name: "dql_mount_attach",
                active: true,
            };
            let registered = (|| -> Result<()> {
                crate::import::register_connection(
                    &bootstrap_conn,
                    m.conn_resource,
                    m.conn_mechanism,
                    m.conn_identity.as_deref(),
                    1, // sqlite-format data
                    &m.conn_description,
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to register connection: {}", e),
                        e.to_string(),
                    )
                })?;

                let entities = self
                    .introspector
                    .introspect_entities_in_schema(&schema_alias)
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!(
                                "Failed to introspect attached database schema '{}': {}",
                                schema_alias, e
                            ),
                            e.to_string(),
                        )
                    })?;

                // source_ns records the physical source namespace uniformly,
                // main included. Whether generated SQL spells it is a separate
                // fact, carried by mount.qualification — encoding that decision
                // as NULL-ness here would conflate "which namespace" with
                // "whether to write it".
                let effective_source_ns = Some(schema_alias.as_str());
                let cartridge_id = {
                    let sql = r#"
                    INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                    VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)
                "#;
                    bootstrap_conn
                        .execute(
                            sql,
                            rusqlite::params![
                                3, // SQLite language ID
                                crate::bootstrap::SourceType::Db.as_i32(),
                                &m.source_uri,
                                effective_source_ns,
                                2, // user connection (the schema is attached there)
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert cartridge",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                    bootstrap_conn.last_insert_rowid() as i32
                };

                crate::bootstrap::introspect::insert_discovered_entities(
                    &bootstrap_conn,
                    cartridge_id,
                    &entities,
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to insert discovered entities: {}", e),
                        e.to_string(),
                    )
                })?;

                let (namespace_id, created_names) = if let Some(ns_id) = existing_namespace_id {
                    bootstrap_conn
                        .execute(
                            "UPDATE namespace
                         SET kind = 'data', provenance = ?2, source_path = ?3
                         WHERE id = ?1",
                            rusqlite::params![ns_id, m.provenance, &m.source_path],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to record mount source on reused namespace",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                    (ns_id, Vec::new())
                } else {
                    create_mounted_namespace_path(
                        &bootstrap_conn,
                        m.namespace,
                        m.provenance,
                        &m.source_path,
                    )?
                };

                let activated_count = crate::import::activate_entities_from_cartridge(
                    &bootstrap_conn,
                    cartridge_id,
                    namespace_id,
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to activate entities: {}", e),
                        e.to_string(),
                    )
                })?;
                debug!(
                    "mount_attach_class: activated {} entities in '{}'",
                    activated_count, m.namespace
                );

                // Authoritative mount relation: `main` keeps unqualified
                // resolution policy while the relation retains the physical
                // attachment alias separately.
                Self::record_mount_binding(
                    &bootstrap_conn,
                    &MountBinding::attach(
                        namespace_id as i64,
                        cartridge_id as i64,
                        2,
                        &schema_alias,
                        // Whoever attached it may detach it, and this mount
                        // did so only when it found nothing already open.
                        match m.existing_schema {
                            Some(_) => Attachment::Borrowed,
                            None => Attachment::Owned,
                        },
                        if m.namespace == "main" {
                            "unqualified"
                        } else {
                            "aliased"
                        },
                    ),
                )?;

                // Catalog wrapper: what makes `ns::(*)` resolve for the mount.
                let catalog_id =
                    ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)?;
                let sys_meta_ns_id: i32 = bootstrap_conn
                    .query_row(
                        "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to query sys::meta namespace for catalog wrapper",
                            e.to_string(),
                        )
                    })?;
                register_mounted_catalog_wrappers(
                    &bootstrap_conn,
                    m.namespace,
                    &created_names,
                    sys_meta_ns_id,
                    catalog_id,
                )?;
                Ok(())
            })();
            if let Err(e) = registered {
                drop(transaction); // rolls back the savepoint
                return Err(e);
            }
            // A failed RELEASE rolls back via the savepoint's Drop. The
            // explicit alias inverse runs after this closure releases the
            // bootstrap lock, so cleanup failures can update session health.
            transaction.commit("Failed to commit mount transaction")?;
            drop(bootstrap_conn);
            Ok(())
        })();
        if let Err(error) = registration {
            // The alias is already attached and journaled. Make the inverse
            // explicit even when registration failed before a catalog row was
            // written; a failed inverse is retained by session health instead
            // of disappearing into Drop.
            let cleanup = alias_guard.rollback();
            return Err(self.mount_error_after_alias_rollback(&schema_alias, cleanup, error));
        }
        alias_guard.commit();

        // Drop the bootstrap lock so sequential execution sees the mount,
        // then point the schema provider at the refreshed metadata.
        self.schema = Some(Box::new(
            crate::bootstrap_schema::BootstrapBackedSchema::new(self.bootstrap_connection.clone()),
        ));

        Ok(())
    }

    /// Mount a host-bound static database image (`delightql-bytes://<name>`).
    /// Attach-class via the shared mount spine: the
    /// image is deserialized into a fresh in-memory schema ATTACHed to the
    /// session connection (joinable with `main`), read-only by rule. All
    /// bytes-specific refusals (bad name, unbound name, non-SQLite primary
    /// via the trait default) fire before any attachment.
    fn mount_database_from_static_bytes(
        &mut self,
        binding_name: &str,
        namespace: &str,
    ) -> Result<()> {
        let locator = format!("delightql-bytes://{}", binding_name);

        if !valid_byte_binding_name(binding_name) {
            return Err(DelightQLError::validation_error(
                format!(
                    "mount!() failed: invalid byte-binding name '{}': expected [a-z][a-z0-9._-]*",
                    binding_name
                ),
                "Binding names are lowercase capability labels",
            ));
        }
        let Some(binding) = self.byte_bindings.get(binding_name).cloned() else {
            // Bound names are an intentionally enumerable, non-secret host
            // surface — the miss teaches, like dql man.
            let mut known: Vec<&str> = self.byte_bindings.keys().map(|s| s.as_str()).collect();
            known.sort_unstable();
            return Err(DelightQLError::database_error(
                format!(
                    "mount!() failed: no byte binding named '{}' (bound: {})",
                    binding_name,
                    if known.is_empty() {
                        "none".to_string()
                    } else {
                        known.join(", ")
                    }
                ),
                "delightql-bytes:// resolves only names the host has bound",
            ));
        };

        let expected = locator.clone();
        let same_source = move |existing: &str| existing == expected;
        let attach = move |conn: &dyn DatabaseConnection, alias: &str| -> Result<()> {
            match &binding {
                // Static rodata: referenced in place, zero-copy.
                ByteBinding::Static(b) => conn.attach_static_bytes(alias, b),
                // Runtime-built buffer: copied into SQLite-owned memory.
                ByteBinding::Owned(a) => conn.attach_bytes_copied(alias, a),
            }
        };
        self.mount_attach_class(
            AttachClassMount {
                namespace,
                source_uri: locator.clone(),
                source_path: locator.clone(),
                provenance: "bytes",
                conn_resource: &locator,
                conn_mechanism: "deserialize",
                conn_identity: Some(format!("host-binding:{}", binding_name)),
                conn_description: format!("Mounted embedded database: {}", namespace),
                // A bytes image is deserialized into a FRESH in-memory schema
                // every time; there is no file for the connection to already
                // hold, and two locators are two images.
                existing_schema: None,
            },
            &same_source,
            &attach,
        )
    }

    /// Provision a fresh, valid, empty SQLite database at `db_path` and bind it
    /// as namespace `namespace`. The create-intent counterpart of `mount_database`: where
    /// `mount!` ATTACHES an existing database and rejects a missing/empty/
    /// invalid path, `mount_new!` MATERIALIZES the database first, then binds
    /// it exactly as `mount!` would.
    ///
    /// CLOBBER POLICY (the `table!`/`table_replace!` refuse-over-clobber
    /// posture): refuse when the path already holds content — a real database
    /// OR any other non-empty bytes; only a MISSING or 0-byte path is
    /// materialized. On refusal the existing file is left untouched.
    ///
    /// v1 SCOPE: SQLite files only. A URI scheme (`postgres://`, …) or
    /// a DuckDB target refuses cleanly — extending to other engines is a
    /// future increment.
    ///
    /// MATERIALIZE: `rusqlite::Connection::open(path)` + `PRAGMA user_version =
    /// 0` forces the SQLite header page out (a valid 4096-byte empty db;
    /// a 0-byte file or a bare
    /// read-only open does NOT). Then delegate to `mount_database`, so the
    /// resulting mount — reserved-name refusal, namespace registration,
    /// catalog wrapper — is identical to `mount!` of a valid empty database.
    ///
    /// Pinned by `mount_new_database_tests` (below) and the CLI
    /// `mount_new_roundtrip` integration test (mount_new! then mount!).
    pub fn mount_new_database(&mut self, db_path: &str, namespace: &str) -> Result<()> {
        // Reserved-name refusal is inherited from mount_database; run it up
        // front so we never materialize a file for a target we will refuse
        // anyway. (mount_database re-runs it harmlessly on delegation.)
        validate_user_namespace_target(namespace)?;

        // v1 SCOPE: SQLite files only. A URI scheme refuses cleanly — the same
        // `://` classification mount_database itself uses to route URIs.
        if db_path.contains("://") {
            let engine = db_path.split("://").next().unwrap_or("that engine");
            return Err(DelightQLError::database_error(
                format!(
                    "mount_new!() creates a new SQLite database; to create on {}, \
                     use its native tooling then mount!()",
                    engine
                ),
                "Unsupported create target",
            ));
        }

        // Resolve against session CWD exactly as mount_database will, so the
        // file we materialize is the file it attaches.
        let resolved = crate::session_cwd::resolve_path(db_path);
        let path = resolved.as_path();

        // CLOBBER POLICY: refuse a path already holding content. Only a MISSING
        // or 0-byte path is ours to create. (A non-empty DuckDB file lands here
        // too — refused as a clobber; attach it with mount!.)
        let prior_state = if path.exists() {
            let len = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            if len > 0 {
                return Err(DelightQLError::database_error(
                    format!(
                        "mount_new!() failed: '{}' already exists; use mount!() to attach it",
                        resolved.display()
                    ),
                    "Refuse to clobber",
                ));
            }
            CreatedFilePriorState::Empty
        } else {
            CreatedFilePriorState::Absent
        };

        // MATERIALIZE a valid empty SQLite database (a header-bearing 4096-byte
        // file). `PRAGMA user_version = 0` forces the header page out — a bare
        // open would leave a 0-byte file that mount!'s attach-only guard
        // rejects.
        {
            let conn = rusqlite::Connection::open(path).map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "mount_new!() failed: cannot create database at '{}': {}",
                        resolved.display(),
                        e
                    ),
                    e.to_string(),
                )
            })?;
            conn.execute_batch("PRAGMA user_version = 0;")
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "mount_new!() failed: cannot initialize database at '{}': {}",
                            resolved.display(),
                            e
                        ),
                        e.to_string(),
                    )
                })?;
        }

        // The file exists independently of the bootstrap catalog. If an
        // enclosing consultation later aborts, catalog rollback/unmount is not
        // enough: restore the exact pre-program filesystem state as well.
        self.journal_created_file(path.to_path_buf(), prior_state);

        // Delegate to the ordinary mount path: the resulting mount is identical
        // to mount!() of a valid empty database.
        self.mount_database(db_path, namespace)
    }

    /// Consult a DQL file containing definitions (functions and views)
    ///
    /// Load definitions from a parsed DDL file into the bootstrap metadata system.
    ///
    /// For each definition: creates an entity row, activates it in the namespace.
    /// The bootstrap DB is the single source of truth — no in-memory cache.
    ///
    /// PUBLISH ONE LOAD — the one road by which a prepared load becomes
    /// catalog state. The load owns everything publication needs: its
    /// destination, its source (a file's path, or an inline block — which
    /// alone receives the ambient data world), and its mode. A FRESH load
    /// registers inside one savepoint; a REPLACEMENT first deletes the
    /// namespace's current load whole, spends the new one, records its
    /// source path, and rebuilds every derived world that depends on the
    /// namespace — a refusal anywhere rolls the deletion, the replacement,
    /// and the rebuilds back together. Nothing about source, ambient
    /// license, or fresh-vs-replacement is chosen here.
    ///
    /// # Returns
    /// The published load: definitions loaded, replaced entity names, and
    /// the ledger rows for the witnesses that follow.
    pub(crate) fn publish(&mut self, load: PreparedLoad) -> Result<PublishedLoad> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let namespace = load.namespace().to_string();
        let namespace = namespace.as_str();
        debug!(
            "publish: {} definitions into namespace '{}'",
            load.definitions.len(),
            namespace
        );

        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for consult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        if matches!(load.mode, LoadMode::Replacement) {
            let transaction = CatalogSavepoint::begin(
                &bootstrap_conn,
                "dql_reconsult_namespace",
                "Failed to begin reconsult transaction",
            )?;
            let ns_id: i64 = bootstrap_conn
                .query_row(
                    "SELECT id FROM namespace WHERE fq_name = ?1",
                    [namespace],
                    |row| row.get(0),
                )
                .map_err(|_| {
                    DelightQLError::database_error(
                        format!("Namespace '{}' not found", namespace),
                        "Namespace not found",
                    )
                })?;
            // DELETE the old load whole — its families, declared edges, and
            // ledger — inside this savepoint. A failure anywhere below rolls
            // the deletion back with the partial replacement, so the prior
            // load stands whole; a statement already compiling holds its
            // own catalog read and is not here to observe either.
            Self::delete_namespace_load(&bootstrap_conn, ns_id)?;
            let source_path = match &load.source {
                LoadSource::File { path } => Some(path.clone()),
                LoadSource::Inline => None,
            };
            // SPEND THE LOAD: families, doc!s, and declared edges land
            // together. The answer is the proof the replacement is COMPLETE
            // — only it can ask dependent derived worlds to rebuild, so no
            // rebuild ever reads a source whose edges are still to come.
            let published = load.spend_on(&bootstrap_conn, None)?;
            if let Some(path) = source_path {
                bootstrap_conn
                    .execute(
                        "UPDATE namespace SET source_path = ?1 WHERE id = ?2",
                        rusqlite::params![&path, ns_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to update source_path",
                            e.to_string(),
                        )
                    })?;
            }
            // Every derived world that derives from this namespace — as its
            // root's source or as a transitive dependency — is rebuilt whole
            // from the COMPLETE replacement and re-admitted; a refusal rolls
            // the whole reload back, so a published world is never left
            // broken by a replacement it cannot admit.
            crate::defuse::grounded_world::rebuild_dependents(
                &bootstrap_conn,
                crate::defuse::CatalogRead::of(self),
                &published,
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Grounding contract violation: lib '{namespace}'. {e}"),
                    "Grounding contract violated",
                )
            })?;
            transaction.commit("Failed to commit reconsult transaction")?;
            return Ok(published);
        }

        let transaction = CatalogSavepoint::begin(
            &bootstrap_conn,
            "dql_consult_file",
            "Failed to begin consult transaction",
        )?;

        // THE AMBIENT DATA WORLD is the inline load's alone: a scratch
        // namespace's views read the primary data namespace (typically
        // "main") without explicit grounding, as the scratch law grants;
        // a file load receives none.
        let ambient_data_ns = if matches!(load.source, LoadSource::Inline) {
            bootstrap_conn
                .query_row(
                    "SELECT fq_name FROM namespace WHERE kind = 'data' AND fq_name = 'main'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .ok()
        } else {
            None
        };

        // THE LOAD IS SPENT HERE: definitions, doc!s, and declared edges land
        // in this one transaction — any failure rolls the whole consultation
        // back.
        let result = load.spend_on(&bootstrap_conn, ambient_data_ns.as_deref());

        if result.is_ok() {
            transaction.commit("Failed to commit consult transaction")?;

            // If consult created a new namespace, register a catalog wrapper for it.
            // Check by looking for an existing wrapper entity named "namespace::" in sys::meta.
            let wrapper_name = format!("{}::", namespace);
            let already_has_wrapper: bool = bootstrap_conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON ae.namespace_id = n.id
                     WHERE e.name = ?1 AND n.fq_name = 'sys::meta')",
                    rusqlite::params![&wrapper_name],
                    |row| row.get(0),
                )
                .unwrap_or(true);

            if !already_has_wrapper {
                if let Ok(catalog_id) =
                    ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)
                {
                    if let Ok(sys_meta_ns_id) = bootstrap_conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                        [],
                        |row| row.get::<_, i32>(0),
                    ) {
                        let _ = register_catalog_wrapper(
                            &bootstrap_conn,
                            namespace,
                            sys_meta_ns_id,
                            catalog_id,
                        );
                    }
                }
            }
        } else {
            drop(transaction);
        }

        drop(bootstrap_conn);

        result
    }

    /// THE LIMINAL RELATION: persist one load's ledger, one row per TOP-LEVEL
    /// FORM, in file-appearance order (rowid = insertion order — the
    /// engine-courtesy contract, no sequence column).
    ///
    /// Called AFTER registration and after the relational goals are proved,
    /// because a witness may name what the load defines and a row cannot be
    /// written before its verdict exists. It runs inside the liminal
    /// program's savepoint, so an abort anywhere still rolls the ledger away
    /// with the namespace (pinned by `liminal_ledger_abort_leaves_no_ledger`
    /// and `liminal_ledger_registration_refusal_rolls_ledger_back`). A
    /// deferred liminal `doc!` keeps its FILE position: the rows were
    /// collected in one pass over the file's forms, before deferral (pinned
    /// by `liminal_ledger_doc_keeps_file_position`). A repeat consult into an
    /// existing namespace APPENDS; reconsult REPLACES whole via
    /// `clear_namespace_contents`.
    pub(crate) fn record_liminal_ledger(&self, namespace: &str, rows: &[LiminalRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for the liminal ledger",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let namespace_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                rusqlite::params![namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("the ledger's namespace '{namespace}' is not in the catalog"),
                    e.to_string(),
                )
            })?;
        for row in rows {
            bootstrap_conn
                .execute(
                    "INSERT INTO liminal_receipt (namespace_id, operation, echoes, receipt)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        namespace_id,
                        row.operation(),
                        row.echoes_json(),
                        row.receipt_json()
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error("Failed to record ledger row", e.to_string())
                })?;
        }
        Ok(())
    }

    /// Register one source's definitions into a namespace, inside the
    /// caller's catalog savepoint. `replacing` is the reconsult window: the
    /// namespace's previous definitions were deleted in the same savepoint,
    /// so the replacement lands whole or not at all.
    fn consult_file_inner(
        bootstrap_conn: &Connection,
        path: &str,
        namespace: &str,
        definitions: Vec<crate::pipeline::asts::ddl::ClauseDecl>,
        count: usize,
        default_data_ns: Option<&str>,
        replacing: bool,
    ) -> Result<ConsultResult> {
        // Embedded stdlib modules use their path directly as the URI;
        // filesystem consults get a file:// prefix.
        let (source_uri, source_type) = if path.starts_with("embedded://") {
            (path.to_string(), SourceType::FileBin)
        } else {
            (format!("file://{}", path), SourceType::File)
        };

        // Get or create namespace.
        // Allows appending definitions from different files to an existing namespace
        // (needed when DDL files contain embedded consult!() directives targeting
        // the same namespace). Errors if the exact same source file has already
        // been consulted into this namespace (duplicate consult detection).
        let namespace_id = {
            let existing_id: Option<i32> = bootstrap_conn
                .query_row(
                    "SELECT id FROM namespace WHERE fq_name = ?1",
                    rusqlite::params![namespace],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to check namespace existence",
                        e.to_string(),
                    )
                })?;

            match existing_id {
                Some(id) => {
                    // Write protection: only scratch namespaces accept inline DDL
                    if path == "(inline)" {
                        let writable: bool = bootstrap_conn
                            .query_row(
                                "SELECT writable FROM namespace WHERE id = ?1",
                                [id],
                                |row| row.get::<_, i32>(0).map(|v| v != 0),
                            )
                            .unwrap_or(false);
                        if !writable {
                            let (ns_kind, ns_source): (String, Option<String>) = bootstrap_conn
                                .query_row(
                                    "SELECT kind, source_path FROM namespace WHERE id = ?1",
                                    [id],
                                    |row| Ok((row.get(0)?, row.get(1)?)),
                                )
                                .unwrap_or(("unknown".into(), None));
                            let source_info = ns_source
                                .map(|s| format!(" (from {})", s))
                                .unwrap_or_default();
                            return Err(DelightQLError::database_error_categorized(
                                "runtime",
                                format!(
                                    "Cannot write definitions to namespace '{}' — \
                                     it is a {} namespace{} and is not writable. \
                                     Use (~~ddl:\"name\" ~~) to create a scratch namespace instead.",
                                    namespace, ns_kind, source_info
                                ),
                                "Write protection",
                            ));
                        }
                    }

                    // ONE CONSULTED SOURCE OWNS ONE NAMESPACE. A second
                    // source landing in a namespace that already holds
                    // authored definitions is cross-source append, whatever
                    // entrance reached here — the surface directive died
                    // with consult_concat_into_ns!, and the registration
                    // writer refuses the capability itself. The lawful
                    // arrivals at an existing namespace row are its FIRST
                    // source (pre-created hierarchy and seeded module rows
                    // hold no definitions), the replacement window
                    // (reconsult, which deleted the prior definitions
                    // first), and scratch inline blocks (write protection
                    // above guards those).
                    if path != "(inline)" && !replacing {
                        let has_source: bool = bootstrap_conn
                            .query_row(
                                "SELECT EXISTS(
                                     SELECT 1 FROM activated_entity ae
                                     JOIN entity e ON e.id = ae.entity_id
                                     WHERE ae.namespace_id = ?1
                                       AND e.type IN (1, 2, 3, 4, 8, 9, 16, 17, 20))",
                                [id],
                                |row| row.get(0),
                            )
                            .unwrap_or(false);
                        if has_source {
                            return Err(DelightQLError::validation_error_categorized(
                                "directive/consult/exists",
                                format!(
                                    "consult! creates namespace '{namespace}' from one source, and it \
                                     already holds one. Reload the same source with \
                                     reconsult!(\"{namespace}\") or remove it first with \
                                     unconsult!(\"{namespace}\") — one consulted source owns one \
                                     namespace, and a second consult is never a merge"
                                ),
                                "consult lifecycle",
                            ));
                        }
                    }
                    id
                }
                None => {
                    ensure_namespace_available(&bootstrap_conn, namespace)?;
                    let (ns_kind, ns_provenance, ns_source, ns_writable) = if path == "(inline)" {
                        ("scratch", "scratch", None, 1i32)
                    } else if path.starts_with("embedded://") {
                        ("system", "bootstrap", Some(path), 0i32)
                    } else {
                        ("lib", "file", Some(path), 0i32)
                    };
                    let sql = r#"
                        INSERT INTO namespace (name, pid, fq_name, default_data_ns, kind, provenance, source_path, writable)
                        VALUES (?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7)
                    "#;
                    let name = namespace.split("::").last().unwrap_or(namespace);
                    bootstrap_conn
                        .execute(
                            sql,
                            rusqlite::params![
                                name,
                                namespace,
                                default_data_ns,
                                ns_kind,
                                ns_provenance,
                                ns_source,
                                ns_writable
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to create consult namespace",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                    bootstrap_conn.last_insert_rowid() as i32
                }
            }
        };

        // For inline DDL: drop-and-replace conflicting entities by name.
        // Only entities whose names match a definition in the new DDL block are
        // removed; other entities from earlier inline blocks are preserved.
        let replaced_entities: Vec<String> = if path == "(inline)" {
            // Collect entity names from the incoming clauses
            let new_names_deduped: std::collections::HashSet<String> =
                definitions.iter().map(|d| d.front.name()).collect();

            let mut replaced_names: Vec<String> = Vec::new();

            for name in &new_names_deduped {
                // Find existing inline entities with this name in this namespace
                let conflicting: Vec<(i64, i64)> = {
                    let mut stmt = bootstrap_conn
                        .prepare(
                            "SELECT e.id, e.cartridge_id FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             JOIN cartridge c ON e.cartridge_id = c.id
                             WHERE e.name = ?1 AND ae.namespace_id = ?2
                               AND c.source_uri LIKE '%inline%'",
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to query conflicting inline entities",
                                e.to_string(),
                            )
                        })?;
                    let rows = stmt
                        .query_map(rusqlite::params![name, namespace_id], |row| {
                            Ok((row.get(0)?, row.get(1)?))
                        })
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to query conflicting inline entities",
                                e.to_string(),
                            )
                        })?;
                    rows.flatten().collect()
                };

                if !conflicting.is_empty() {
                    replaced_names.push(name.to_string());
                }

                for (entity_id, cartridge_id) in &conflicting {
                    Self::clear_single_entity(bootstrap_conn, *entity_id)?;

                    // Clean up cartridge if it has no remaining entities
                    let remaining: i64 = bootstrap_conn
                        .query_row(
                            "SELECT COUNT(*) FROM entity WHERE cartridge_id = ?1",
                            [cartridge_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(1);
                    if remaining == 0 {
                        bootstrap_conn
                            .execute("DELETE FROM cartridge WHERE id = ?1", [cartridge_id])
                            .map_err(|e| {
                                DelightQLError::database_error(
                                    "Failed to delete empty cartridge",
                                    e.to_string(),
                                )
                            })?;
                    }
                }
            }

            if !replaced_names.is_empty() {
                log::warn!(
                    "Inline DDL: replacing {} entit{} in namespace '{}': {}",
                    replaced_names.len(),
                    if replaced_names.len() == 1 {
                        "y"
                    } else {
                        "ies"
                    },
                    namespace,
                    replaced_names.join(", ")
                );
            }

            replaced_names
        } else {
            Vec::new()
        };

        // A load without definitions — a facade file of liminal directives
        // only — registers no families and mints no cartridge: the lifecycle
        // reaches a cartridge only through the entities activated under it,
        // so an entity-free one would be a row nothing could ever remove.
        if definitions.is_empty() {
            return Ok(ConsultResult {
                definitions_loaded: 0,
                replaced_entities,
            });
        }

        // Create cartridge for the consulted file
        let cartridge_id = {
            let sql = r#"
                INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)
            "#;
            bootstrap_conn
                .execute(
                    sql,
                    rusqlite::params![
                        1, // DqlStandard language ID
                        source_type.as_i32(),
                        &source_uri,
                        Some(namespace),
                        1, // bootstrap connection
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert consult cartridge",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // Group clauses by the SUBJECT'S OWN IDENTITY to support disjunctive
        // definitions. Multiple clauses under one subject (multi-clause sigma
        // predicates, guarded functions) become a single entity whose per-
        // clause sources are stored in authored order. Keying on the catalog
        // SPELLING instead would fold nothing: `Counter` and `counter` are
        // one unstropped name by the identifier law, and registering them
        // apart leaves two entity rows the unqualified lookup reaches at
        // once. The liminal ledger's DEFINE rows read the same authority.
        let groups = crate::pipeline::asts::ddl::group_by_subject(definitions);

        // R6 DAG edges collected per effect-rule group; cycle-checked after
        // the loop (the whole consult is one transaction — a refusal rolls
        // back every registration). See validate_effect_rule_recursion.
        let mut effect_rule_edges: Vec<(String, Vec<String>)> = Vec::new();

        for (subject, decls) in groups {
            let subject = subject.catalog_name();
            if decls.len() > 1 {
                debug!(
                    "consult_file: Grouping {} clauses for '{}' into single entity",
                    decls.len(),
                    subject
                );
            }

            // THE CLAUSES ARE ALREADY BUILT. Assembly runs over the
            // declarations this consultation normalized, so registration
            // parses nothing it has already read. Fact clauses elaborate
            // inside the assembler — mixed fact/rule sets included — so the
            // catalog stores the AUTHORED clause sources and reconstruction
            // re-elaborates through the same one door.
            let clause_sources: Vec<String> = decls.iter().map(|d| d.full_source.clone()).collect();
            let ddl_group =
                crate::pipeline::asts::ddl::DefinitionGroup::assemble(decls).map_err(|e| {
                    // Semantic constraint errors (TransformationError,
                    // categorized ValidationError) propagate directly to
                    // preserve their specific URI subcategory.
                    if matches!(
                        &e,
                        DelightQLError::TransformationError { .. }
                            | DelightQLError::ValidationError {
                                subcategory: Some(_),
                                ..
                            }
                    ) {
                        return e;
                    }
                    DelightQLError::validation_error(
                        format!("DDL definition '{subject}' has an invalid body: {e}"),
                        "DDL body validation failed",
                    )
                })?;

            // foo/foo! name collision: a namespace may not hold both a
            // functor `foo` and
            // an effect rule `foo!`. Enforced at registration time in BOTH
            // directions, before either insert branch below (normal and
            // deferred-HO alike). Same-file collisions are caught too:
            // earlier groups of this consult are already inserted and
            // activated on this connection when later groups arrive. This is
            // the prerequisite for making doc! targets explicit — the `!`
            // fallback in consult_body stays sound only while the two names
            // cannot coexist. Pinned by effects-ball
            // rules--47_name_collision_effect_second and
            // rules--48_name_collision_entity_second.
            {
                let group_name = ddl_group.name();
                let group_name = group_name.as_str();
                if let Some(base) = group_name.strip_suffix('!') {
                    // The effect rule arrives second: refuse if ANY entity
                    // named `foo` (view/table/function/fact/…) is active in
                    // this namespace — the whole functor namespace collides.
                    let base_exists: bool = bootstrap_conn
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE e.name = ?1 AND ae.namespace_id = ?2)",
                            rusqlite::params![base, namespace_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);
                    if base_exists {
                        return Err(DelightQLError::validation_error_categorized(
                            "effect/rule/name_collision",
                            format!(
                                "cannot register effect rule '{}': namespace '{}' \
                                 already holds an entity named '{}' — a namespace \
                                 may not hold both '{}' and '{}'.",
                                group_name, namespace, base, base, group_name
                            ),
                            "effect-rule name collision",
                        ));
                    }
                } else {
                    // The plain entity arrives second: refuse if an EFFECT
                    // RULE named `foo!` (entity type 20) is active in this
                    // namespace. Restricted to type 20 so `!`-named built-in
                    // pseudo-predicates can never block a plain name.
                    let banged = format!("{}!", group_name);
                    let effect_rule_exists: bool = bootstrap_conn
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE e.name = ?1 AND e.type = 20 AND ae.namespace_id = ?2)",
                            rusqlite::params![&banged, namespace_id],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);
                    if effect_rule_exists {
                        return Err(DelightQLError::validation_error_categorized(
                            "effect/rule/name_collision",
                            format!(
                                "cannot register '{}': namespace '{}' already \
                                 holds an effect rule '{}' — a namespace may not \
                                 hold both '{}' and '{}'.",
                                group_name, namespace, banged, group_name, banged
                            ),
                            "effect-rule name collision",
                        ));
                    }
                }
            }

            // Subject, declared kind, parameter arity, and the whole head
            // algebra were decided by `DefinitionGroup::assemble` before this
            // group existed. What remains here is discipline no assembler can
            // own — clause ORDER for value functions, and the effect algebra.

            // Value-function clause discipline: at most one unguarded clause
            // (RULE 2, the fix for the all-unguarded defect that the old
            // `has_any_guard` gate let slip through) and the unguarded clause
            // must be last. Gated on DefKind::Function inside the helper, so
            // sigma predicates (the relational OR path) are untouched.
            validate_function_clause_discipline(&ddl_group)?;

            // Effect-rule discipline (rules R1/R2/R3/R4/R9 + F2)
            // — every group, single- or multi-clause; the effect sibling of
            // validate_function_clause_discipline above.
            if let Some(edges) = validate_effect_algebra_discipline(&ddl_group)? {
                effect_rule_edges.push(edges);
            }

            let group_name = ddl_group.name();

            // F2: at most one main! per namespace. A second file consulted
            // into the same namespace must not smuggle in another main!.
            if group_name == "main!" {
                let already_has_main: bool = bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM entity e
                         JOIN activated_entity ae ON ae.entity_id = e.id
                         WHERE e.name = 'main!' AND ae.namespace_id = ?1)",
                        [namespace_id],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);
                if already_has_main {
                    return Err(DelightQLError::validation_error_categorized(
                        "effect/main/duplicate",
                        format!(
                            "namespace '{}' already has a main! — at most one main! \
                             per namespace (EFFECT-ALGEBRA F2).",
                            namespace
                        ),
                        "duplicate main! in namespace",
                    ));
                }
            }

            debug!(
                "consult_file: Registering {:?} '{}' ({} clause{})",
                ddl_group.kind(),
                group_name,
                ddl_group.clauses().len(),
                if ddl_group.clauses().len() > 1 {
                    "s"
                } else {
                    ""
                }
            );

            // The catalog reads the GROUP's identity. Nothing here picks a
            // clause and nothing here re-parses a head: everything below is
            // what the assembler already made every clause agree on, and a
            // deferred body does not make a group any less assembled.
            let entity_type = ddl_group.entity_type().as_i32();
            let param_names: Vec<&str> = ddl_group
                .bound_param_names()
                .into_iter()
                .map(delightql_types::SqlIdentifier::as_str)
                .collect();

            // Insert entity (without definition — clauses go into entity_clause).
            // The name's strop bit is identity, so the catalog keeps it.
            let name_stropped = ddl_group
                .name_identifier()
                .is_some_and(delightql_types::SqlIdentifier::is_stropped);
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, name_stropped, type, cartridge_id, doc) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        &group_name,
                        name_stropped,
                        entity_type,
                        cartridge_id,
                        &ddl_group.doc(),
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert consult entity",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            let entity_id = bootstrap_conn.last_insert_rowid() as i32;

            // Insert each clause into entity_clause. `clause_sources` is the
            // fact-union-rewritten text where that transform fired, else the
            // original per-clause sources (see the transform above).
            for (ordinal, src) in clause_sources.iter().enumerate() {
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, ?2, ?3)",
                        rusqlite::params![entity_id, (ordinal + 1) as i32, src],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert entity clause",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
            }

            // Record input parameters as entity attributes. The parameter's
            // ROLE is a catalog fact: a `f:()`-spelled code formal writes
            // 'code_param', a value formal 'input_param' — so a call site
            // can partition its members and resolve its actuals BEFORE the
            // body is admitted and opened.
            {
                use crate::pipeline::asts::ddl::HoParam;
                let mut position = 0i32;
                for param in ddl_group.params() {
                    let HoParam::Scalar { name, callable, .. } = param else {
                        continue;
                    };
                    let attribute_type = if *callable {
                        "code_param"
                    } else {
                        "input_param"
                    };
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position) VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![entity_id, name.as_str(), attribute_type, position],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert entity attribute",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                    position += 1;
                }
            }

            // For HO views, write structured param metadata with cross-clause position analysis
            {
                let positions =
                    crate::pipeline::resolver::grounding::build_ho_position_analysis(&ddl_group);
                if !positions.is_empty() {
                    Self::write_ho_params_to_bootstrap(bootstrap_conn, entity_id, &positions)?;
                }
            }

            // For edges, write the selection keys to join_edge: the two
            // ground terms as NAKED canonical spellings plus the context
            // symbol — the match key and the stored key are the same
            // bytes. The subject holds the pair in sorted order, which is
            // what makes lookup symmetric.
            if let crate::pipeline::asts::ddl::DefSubject::Edge {
                left,
                right,
                context,
            } = ddl_group.subject()
            {
                for idx in 0..ddl_group.clauses().len() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO join_edge (entity_id, left_spelling, right_spelling, context_name, clause_ordinal) VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![entity_id, left, right, context, (idx + 1) as i32],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert join_edge",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }
            }

            // For a fact function, write THE DECLARED MODE. Callable
            // selection consumes these typed rows; relational capability is
            // already fixed in the group's entity type.
            if let Some(mode) = ddl_group.declared_mode() {
                Self::write_functional_dependency(bootstrap_conn, entity_id as i64, mode)?;
            }

            // Extract references from ALL clauses (union of references)
            {
                use crate::pipeline::asts::ddl::DdlBody;
                let mut all_refs = Vec::new();
                for ddl_def in ddl_group.clauses() {
                    let clause_refs = match &ddl_def.body {
                        DdlBody::Scalar(expr) => {
                            crate::ddl::analyzer::extract_references_from_domain(expr)
                        }
                        DdlBody::Truth(expr) => {
                            crate::ddl::analyzer::extract_references_from_truth(expr)
                        }
                        DdlBody::Relational(query) => {
                            crate::ddl::analyzer::extract_references_from_query(query)
                        }
                        // A mode's references live in every authored output
                        // cell, including the default. Extract them directly:
                        // a default-bearing mode has no relational body to
                        // synthesize merely for analysis.
                        DdlBody::FactFunction(definition) => {
                            let mode = definition.mode();
                            let mut refs = Vec::new();
                            for arm in mode.arms.iter() {
                                for output in arm.outputs.iter() {
                                    refs.extend(
                                        crate::ddl::analyzer::extract_references_from_domain(
                                            output,
                                        ),
                                    );
                                }
                            }
                            if let Some(default) = &mode.default {
                                for output in default.iter() {
                                    refs.extend(
                                        crate::ddl::analyzer::extract_references_from_domain(
                                            output,
                                        ),
                                    );
                                }
                            }
                            refs
                        }
                        // A deferred TEMPLATE has no parsed body to read, so
                        // it is proffer-parsed: synthetic bindings stand in
                        // for the call site's arguments, which is enough to
                        // reach the references and to catch a body that is
                        // broken rather than merely unsubstituted. The
                        // deferral is the BODY's; the group it belongs to was
                        // assembled with everyone else's.
                        DdlBody::Deferred { source } => {
                            let proffer_identities =
                                crate::relation::Planning::open(crate::names::Registry::new(&[]));
                            let bindings =
                                crate::pipeline::resolver::grounding::create_proffer_bindings(
                                    &ddl_def.head,
                                    &proffer_identities,
                                )?;
                            // A REFUSAL THAT AWAITS SUBSTITUTION IS THE
                            // DEFERRAL ITSELF. The proffer supplies stand-ins,
                            // and a stand-in cannot be the integer a bound
                            // wants — so refusing here would undo the deferral
                            // the clause already made. Its references become
                            // known at invocation, with the real arguments.
                            match crate::ddl::reconstruct::bound_relex(source, bindings) {
                                Ok(query) => {
                                    crate::ddl::analyzer::extract_references_from_query(&query)
                                }
                                Err(e) if crate::pipeline::normalize::awaits_substitution(&e) => {
                                    Vec::new()
                                }
                                Err(e) => {
                                    return Err(DelightQLError::validation_error(
                                        format!(
                                            "HO view '{group_name}' body has a syntax error: {e}"
                                        ),
                                        "DDL body validation failed",
                                    ))
                                }
                            }
                        }
                    };
                    all_refs.extend(clause_refs);
                }

                // Filter out bound parameters from free variable references.
                // HO view params like T in active_only(T)(*) are bound, not free.
                let refs: Vec<_> = all_refs
                    .into_iter()
                    .filter(|r| !param_names.contains(&r.name.as_str()))
                    .collect();

                for ext_ref in &refs {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO referenced_entity (name, namespace, apparent_type, containing_entity_id) VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                &ext_ref.name,
                                &ext_ref.namespace,
                                ext_ref.apparent_type,
                                entity_id,
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert referenced entity",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }

                debug!(
                    "consult_file: Extracted {} references from '{}' ({} clause{})",
                    refs.len(),
                    group_name,
                    ddl_group.clauses().len(),
                    if ddl_group.clauses().len() > 1 {
                        "s"
                    } else {
                        ""
                    }
                );
            }

            // Register interior schemas for tree group columns
            {
                use crate::pipeline::asts::ddl::DdlBody;
                for ddl_def in ddl_group.clauses() {
                    if let DdlBody::Relational(query) = &ddl_def.body {
                        register_interior_schemas_from_query(bootstrap_conn, entity_id, query)?;
                    }
                }
            }

            // Activate in namespace. The store's family-identity trigger
            // refuses a second same-named family in one namespace; hand
            // that refusal back as the clause-agreement teaching, not raw
            // SQL.
            bootstrap_conn
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![entity_id, namespace_id, cartridge_id],
                )
                .map_err(|e| {
                    if e.to_string().contains("definition_family_identity") {
                        return DelightQLError::validation_error_categorized(
                            "ddl/family/one_name_one_entity",
                            format!(
                                "'{group_name}' is already defined in this source — one fully \
                                 qualified name identifies one entity, and category or arity \
                                 never selects among same-named definitions (heads-law, CLAUSE \
                                 AGREEMENT). Same-kind clauses of one entity belong under one \
                                 head; a different definition needs a different name."
                            ),
                            "one name, one entity",
                        );
                    }
                    DelightQLError::database_error_with_source(
                        "Failed to activate consult entity",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
        }

        // R6 (no recursion): the file's effect rules must form a DAG.
        // Checked after all groups are known (forward references between
        // rules in one file are legal); a refusal rolls the whole consult
        // transaction back.
        validate_effect_rule_recursion(&effect_rule_edges)?;

        debug!(
            "consult_file: Successfully loaded {} definitions into '{}'",
            count, namespace
        );
        Ok(ConsultResult {
            definitions_loaded: count,
            replaced_entities,
        })
    }

    /// Engage a namespace (enables unqualified entity resolution)
    ///
    /// Creates an enlisted_namespace record in bootstrap, allowing entities from
    /// the specified namespace to be resolved without qualification.
    ///
    /// # Arguments
    /// * `namespace` - The namespace path to enlist (e.g., "mfg", "std::string")
    ///
    /// # Returns
    /// * `Ok(())` - Namespace enlisted successfully
    /// * `Err(...)` - Namespace not found or enlist failed
    pub fn enlist_namespace(&mut self, namespace: &str) -> Result<()> {
        self.perform_enlist(namespace).map(|_| ())
    }

    /// THE ENLISTMENT ACT: enlist `namespace` at the session and answer with
    /// the edge performed. Private — a load's [`PreparedLoad::enlist`] is
    /// the only holder of the answer; the prompt-level directive discards
    /// it through [`Self::enlist_namespace`], because a prompt is not a
    /// load.
    fn perform_enlist(&mut self, namespace: &str) -> Result<DeclaredEdge> {
        // Lazy-load stdlib module if needed (e.g., "std::reshape")
        self.ensure_stdlib_loaded(namespace);

        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for enlist",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up the namespace ID. PLAIN-NAME ENLIST (middle rung):
        // `enlist!("chz")(*)` when `chz` is a direct child of an already-enlisted
        // namespace (home::chz) — expand on the exact miss, then re-look-up.
        // `resolved_fq` is the fq actually enlisted; it drives the blueprint
        // guard below and every downstream check.
        let (from_namespace_id, resolved_fq): (i32, String) = match bootstrap_conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [namespace],
            |row| row.get::<_, i32>(0),
        ) {
            Ok(id) => (id, namespace.to_string()),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                match expand_plain_namespace(&bootstrap_conn, namespace)? {
                    Some(expanded) => {
                        let id: i32 = bootstrap_conn
                            .query_row(
                                "SELECT id FROM namespace WHERE fq_name = ?1",
                                [&expanded],
                                |row| row.get(0),
                            )
                            .map_err(|e| {
                                DelightQLError::database_error(
                                    format!(
                                        "Namespace '{}' (expanded to '{}') not found.",
                                        namespace, expanded
                                    ),
                                    e.to_string(),
                                )
                            })?;
                        (id, expanded)
                    }
                    None => {
                        return Err(DelightQLError::database_error(
                            format!(
                                "Namespace '{}' not found. Make sure to mount!() it first.",
                                namespace
                            ),
                            "namespace not found",
                        ));
                    }
                }
            }
            Err(e) => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found. Make sure to mount!() it first.",
                        namespace
                    ),
                    e.to_string(),
                ));
            }
        };

        // Blueprint inertness: enlisting an archived blueprint (or a
        // descendant of one) would make its inert rules resolvable UNQUALIFIED
        // — the opposite of "consumed and archived". Refuse. Checks the RESOLVED
        // fq so a plain-name enlist of an archived-blueprint child stays refused.
        refuse_if_blueprint(&bootstrap_conn, &resolved_fq)?;

        // The interactive session's scope is `home`: a
        // prompt-level enlist attaches its edge to home, never to the
        // `main` data namespace.
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'home'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Session namespace 'home' not found in bootstrap (database corruption)",
                    e.to_string(),
                )
            })?;

        // Check for ER-context name collisions with already-enlisted namespaces.
        // Two enlisted namespaces with the same context name create ambiguity for
        // `under ctx:` lookups that search enlisted namespaces.
        {
            let mut collision_stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT new_er.context_name, existing_ns.fq_name
                     FROM join_edge new_er
                     JOIN entity new_e ON new_e.id = new_er.entity_id
                     JOIN activated_entity new_ae ON new_ae.entity_id = new_e.id
                        AND new_ae.namespace_id = ?1
                     JOIN join_edge existing_er ON existing_er.context_name = new_er.context_name
                     JOIN entity existing_e ON existing_e.id = existing_er.entity_id
                     JOIN activated_entity existing_ae ON existing_ae.entity_id = existing_e.id
                     JOIN namespace existing_ns ON existing_ns.id = existing_ae.namespace_id
                     JOIN enlisted_namespace en ON en.from_namespace_id = existing_ns.id
                        AND en.to_namespace_id = ?2
                     WHERE existing_ns.id != ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to prepare ER-context collision check",
                        e.to_string(),
                    )
                })?;

            let collisions: Vec<(String, String)> = collision_stmt
                .query_map(
                    rusqlite::params![from_namespace_id, to_namespace_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to check ER-context collisions",
                        e.to_string(),
                    )
                })?
                .filter_map(|r| r.ok())
                .collect();

            if !collisions.is_empty() {
                let details: Vec<String> = collisions
                    .iter()
                    .map(|(ctx, ns)| format!("context '{}' (already enlisted from '{}')", ctx, ns))
                    .collect();
                return Err(DelightQLError::validation_error(
                    format!(
                        "Cannot enlist namespace '{}': ER-context name collision — {}. \
                         Use qualified access (ns.view(*)) instead of enlist to avoid ambiguity.",
                        namespace,
                        details.join(", "),
                    ),
                    "ER-context collision on enlist",
                ));
            }
        }

        // Insert enlisted_namespace record (or ignore if already enlisted)
        bootstrap_conn
            .execute(
                "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
                 VALUES (?1, ?2)",
                [from_namespace_id, to_namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to enlist namespace '{}': {}", namespace, e),
                    e.to_string(),
                )
            })?;

        debug!(
            "enlist_namespace: Enlisted '{}' into default namespace",
            namespace
        );

        // Explicitly drop the bootstrap connection lock
        drop(bootstrap_conn);

        // THE ACT ANSWERS WITH THE EDGE IT PERFORMED: an enlistment of the
        // namespace it selected, whole — the only evidence a load can
        // declare.
        Ok(DeclaredEdge(LexicalAct::Enlist {
            target: i64::from(from_namespace_id),
        }))
    }

    /// Register a namespace alias (e.g., "l" → "lib::math")
    ///
    /// Creates a namespace_alias record in bootstrap, allowing a short alias
    /// to be used in place of a fully-qualified namespace path.
    pub fn register_namespace_alias(&mut self, alias: &str, namespace: &str) -> Result<()> {
        self.perform_alias(alias, namespace).map(|_| ())
    }

    /// THE ALIAS ACT: register `alias` → `namespace` at the session and
    /// answer with the edge performed, shorthand included. Private, as
    /// [`Self::perform_enlist`] is.
    fn perform_alias(&mut self, alias: &str, namespace: &str) -> Result<DeclaredEdge> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for namespace alias",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found. Cannot create alias '{}'.",
                        namespace, alias
                    ),
                    e.to_string(),
                )
            })?;

        // A shorthand that names an EXISTING namespace makes every lookup
        // for that name two-headed: the entity resolution query ORs the
        // exact-fq and alias branches with no ordering, so which head
        // wins is scan order. Refuse the collision at registration.
        let collision: Option<i64> = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [alias],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| {
                DelightQLError::database_error("Failed to check alias collision", e.to_string())
            })?;
        if collision.is_some() {
            return Err(DelightQLError::validation_error(
                format!(
                    "alias!() shorthand '{}' collides with an existing namespace of \
                     the same name — lookups for '{}' would be ambiguous. Choose a \
                     different shorthand.",
                    alias, alias
                ),
                "Alias shorthand collision",
            ));
        }

        // Colliding with an existing SHORTHAND is the same two-headed
        // ambiguity as colliding with a namespace, and gets the same
        // refusal. Re-binding by replacement was a silent last-writer-wins
        // collision policy; a taken shorthand refuses, naming its holder.
        // Re-declaring the SAME binding stays idempotent.
        let taken: Option<(i64, String)> = bootstrap_conn
            .query_row(
                "SELECT a.target_namespace_id, n.fq_name FROM namespace_alias a
                 JOIN namespace n ON n.id = a.target_namespace_id
                 WHERE a.alias = ?1",
                [alias],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| {
                DelightQLError::database_error("Failed to check alias holder", e.to_string())
            })?;
        if let Some((holder_id, holder_fq)) = taken {
            if i64::from(ns_id) == holder_id {
                // Idempotent: the same binding, the same edge performed.
                return Ok(DeclaredEdge(LexicalAct::Alias {
                    shorthand: alias.to_string(),
                    target: i64::from(ns_id),
                }));
            }
            return Err(DelightQLError::validation_error(
                format!(
                    "alias!() shorthand '{}' is already taken by '{}' — lookups for \
                     '{}' would be ambiguous. Choose a different shorthand.",
                    alias, holder_fq, alias
                ),
                "Alias shorthand collision",
            ));
        }

        bootstrap_conn
            .execute(
                "INSERT INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
                rusqlite::params![alias, ns_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to register namespace alias '{}' → '{}': {}",
                        alias, namespace, e
                    ),
                    e.to_string(),
                )
            })?;

        debug!("register_namespace_alias: '{}' → '{}'", alias, namespace);

        drop(bootstrap_conn);
        // THE ACT ANSWERS WITH THE EDGE IT PERFORMED: the shorthand it
        // registered, bound to the namespace it selected — no caller pairs
        // them afterwards.
        Ok(DeclaredEdge(LexicalAct::Alias {
            shorthand: alias.to_string(),
            target: i64::from(ns_id),
        }))
    }

    /// SELECT THE TARGET OF AN EXPOSURE a consulted file declares: the
    /// child namespace `child_fq` names, exactly — the file's own child by
    /// the facade law — as it stands when the directive executes. The
    /// exposing namespace itself may not have its row yet (a fresh
    /// consult creates it at registration); the law is a relationship of
    /// names, checked here and again at publication.
    fn perform_expose(&self, exposing_fq: &str, child_fq: &str) -> Result<DeclaredEdge> {
        if !child_fq.starts_with(&format!("{exposing_fq}::")) {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot expose '{child_fq}' through '{exposing_fq}': not a child namespace"
                ),
                "Invalid expose target",
            ));
        }
        let conn = self.lock_bootstrap("Failed to acquire bootstrap lock for expose")?;
        let id: i64 = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [child_fq],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{child_fq}' not found for expose"),
                    "Namespace not found",
                )
            })?;
        Ok(DeclaredEdge(LexicalAct::Expose { target: id }))
    }

    /// Delist a namespace (disables unqualified entity resolution)
    ///
    /// Removes the enlisted_namespace record from bootstrap, preventing entities
    /// from the specified namespace from being resolved without qualification.
    /// Qualified access (e.g., `mfg.suppliers(*)`) still works after delist.
    ///
    /// # Arguments
    /// * `namespace` - The namespace path to delist (e.g., "mfg", "std::string")
    ///
    /// # Returns
    /// * `Ok(())` - Namespace delisted successfully
    /// * `Err(...)` - Namespace not found or delist failed
    pub fn delist_namespace(&mut self, namespace: &str) -> Result<()> {
        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for delist",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up the namespace ID
        let from_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    e.to_string(),
                )
            })?;

        // The interactive session's scope is `home`: a
        // prompt-level enlist attaches its edge to home, never to the
        // `main` data namespace.
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'home'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Session namespace 'home' not found in bootstrap (database corruption)",
                    e.to_string(),
                )
            })?;

        // Delete enlisted_namespace record
        let rows_affected = bootstrap_conn
            .execute(
                "DELETE FROM enlisted_namespace
                 WHERE from_namespace_id = ?1 AND to_namespace_id = ?2",
                [from_namespace_id, to_namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to delist namespace '{}': {}", namespace, e),
                    e.to_string(),
                )
            })?;

        if rows_affected == 0 {
            return Err(DelightQLError::database_error_categorized(
                "useafterfree",
                format!("Namespace '{}' is not currently enlisted", namespace),
                "delist!() requires a prior enlist!() on the same namespace",
            ));
        } else {
            debug!("delist_namespace: Delisted namespace '{}'", namespace);
        }

        // Also clean up any namespace aliases pointing to this namespace
        bootstrap_conn
            .execute(
                "DELETE FROM namespace_alias WHERE target_namespace_id = ?1",
                [from_namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Failed to clean up aliases for namespace '{}': {}",
                        namespace, e
                    ),
                    e.to_string(),
                )
            })?;

        // Explicitly drop the bootstrap connection lock
        drop(bootstrap_conn);

        Ok(())
    }

    /// Snapshot the current enlisted_namespace state.
    /// Returns all (from_namespace_id, to_namespace_id) rows for later restoration.
    pub fn save_enlisted_state(&self) -> Result<Vec<(i32, i32)>> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for save_enlisted_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut stmt = bootstrap_conn
            .prepare("SELECT from_namespace_id, to_namespace_id FROM enlisted_namespace")
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare enlisted_namespace snapshot",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(i32, i32)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to snapshot enlisted_namespace",
                    e.to_string(),
                )
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Restore the enlisted_namespace state from a previous snapshot.
    /// Deletes all current rows and re-inserts the saved ones.
    pub fn restore_enlisted_state(&mut self, saved: &[(i32, i32)]) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for restore_enlisted_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn
            .execute("DELETE FROM enlisted_namespace", [])
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to clear enlisted_namespace for restore",
                    e.to_string(),
                )
            })?;

        for (from_id, to_id) in saved {
            bootstrap_conn
                .execute(
                    "INSERT INTO enlisted_namespace (from_namespace_id, to_namespace_id) VALUES (?1, ?2)",
                    [from_id, to_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to restore enlisted_namespace row",
                        e.to_string(),
                    )
                })?;
        }

        Ok(())
    }

    /// Snapshot the current namespace_alias state.
    /// Returns all (alias, target_namespace_id) rows for later restoration.
    pub fn save_alias_state(&self) -> Result<Vec<(String, i32)>> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for save_alias_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut stmt = bootstrap_conn
            .prepare("SELECT alias, target_namespace_id FROM namespace_alias")
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare namespace_alias snapshot",
                    e.to_string(),
                )
            })?;

        let rows: Vec<(String, i32)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(|e| {
                DelightQLError::database_error("Failed to snapshot namespace_alias", e.to_string())
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Restore namespace_alias to a previously saved state.
    pub fn restore_alias_state(&mut self, saved: &[(String, i32)]) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for restore_alias_state",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn
            .execute("DELETE FROM namespace_alias", [])
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to clear namespace_alias for restore",
                    e.to_string(),
                )
            })?;

        for (alias, target_id) in saved {
            bootstrap_conn
                .execute(
                    "INSERT INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![alias, target_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to restore namespace_alias row",
                        e.to_string(),
                    )
                })?;
        }

        Ok(())
    }

    /// Destroy a namespace and cascade-delete all its bootstrap metadata.
    ///
    /// Returns `(connection_id, source_ns)` from the cartridge so the caller
    /// can handle physical cleanup (DETACH, connection_map removal).
    /// Empty `main` back to its pre-created bootstrap state (the unmount
    /// counterpart of open()'s pre-creation): contents and the mount
    /// cartridge go, the ROW and its wiring (home enlistment, routing)
    /// stay, and the mount facts are cleared so the next mount reuses it.
    /// Returns the physical cleanup identity like destroy_namespace.
    fn empty_main_namespace(bootstrap_conn: &Connection) -> Result<(Option<i64>, Option<String>)> {
        let ns_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'main'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| DelightQLError::database_error("main namespace missing", e.to_string()))?;

        // Physical cleanup identity, read from the authoritative relation
        // BEFORE clearing it.
        let link_identity = Self::mount_binding(&bootstrap_conn, ns_id)?.map(|binding| {
            (
                binding.cartridge_id,
                Some(binding.connection_id),
                binding.detachable_alias(),
            )
        });

        let txn = BootstrapTxn::begin(&bootstrap_conn, "empty_main")?;
        // FK choreography: un-point before the cartridge dies; restore the
        // row's pre-created face.
        Self::clear_mount_binding(&bootstrap_conn, ns_id)?;
        bootstrap_conn
            .execute(
                "UPDATE namespace
                 SET source_path = NULL, provenance = 'bootstrap'
                 WHERE id = ?1",
                [ns_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to reset main namespace", e.to_string())
            })?;
        Self::clear_namespace_contents(&bootstrap_conn, ns_id)?;
        if let Some((cart_id, _, _)) = link_identity {
            Self::clear_cartridge_entities(&bootstrap_conn, cart_id)?;
        }
        txn.commit()?;

        Ok(link_identity
            .map(|(_, conn_id, alias)| (conn_id, alias))
            .unwrap_or((None, None)))
    }

    /// Catalog-only destruction. Takes the bootstrap connection from the
    /// CALLER: unmount holds one guard — and one
    /// transaction window — across every destroy AND the physical DETACH,
    /// so a failed DETACH rolls the catalog back instead of losing the
    /// cleanup identity.
    fn destroy_namespace(
        bootstrap_conn: &Connection,
        namespace_fq: &str,
    ) -> Result<(Option<i64>, Option<String>)> {
        // Look up namespace
        let namespace_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace_fq],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace_fq),
                    "Namespace not found",
                )
            })?;

        // Find ALL cartridge(s) and their connection info. The mount's own
        // cartridge comes from the STORED link —
        // authoritative even for a
        // valid-but-EMPTY image, so its alias always DETACHes. The entity
        // join still contributes any entity-bearing cartridges. The old
        // source-match fallback (and its double-empty ambiguity) is
        // REPEALED: identity is read, never inferred.
        let cartridge_infos: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT c.id, c.connection_id, c.source_ns
                     FROM cartridge c
                     WHERE c.id = (SELECT cartridge_id FROM mount WHERE namespace_id = ?1)
                        OR c.id IN (SELECT e.cartridge_id FROM entity e
                                    JOIN activated_entity ae ON ae.entity_id = e.id
                                    WHERE ae.namespace_id = ?1)",
                )
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query cartridges", e.to_string())
                })?;
            let rows = stmt
                .query_map([namespace_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query cartridges", e.to_string())
                })?;
            rows.flatten().collect()
        };

        // Physical-cleanup identity comes from the LINK, read BEFORE it is
        // cleared: the union above deliberately includes
        // entity-bearing auxiliary cartridges as a DELETION set, so taking
        // `.first()` of it could hand physical cleanup an auxiliary's
        // (connection, alias) and leave the real mount attached. Unlinked
        // data namespaces fall back to the union's first row, as before.
        // Only QueryReturnedNoRows means "unlinked": any
        // other SQLite error must ABORT the destroy rather than silently
        // falling back to the arbitrary auxiliary path — the exact behavior
        // this read exists to eliminate.
        let link_identity = Self::mount_binding(bootstrap_conn, namespace_id)?
            .map(|binding| (Some(binding.connection_id), binding.detachable_alias()));

        // Atomic cascade: the link clear and every
        // delete below share one savepoint — a mid-cascade failure rolls the
        // whole destroy back, identity fact included, instead of leaving
        // partial metadata with the link already lost.
        let txn = BootstrapTxn::begin(&bootstrap_conn, "destroy_namespace")?;

        // FK choreography: clear the
        // link BEFORE the cascade deletes its cartridge row — under the
        // restrictive FK an accidental ordering mistake is loud, never a
        // silently orphaned namespace.
        Self::clear_mount_binding(&bootstrap_conn, namespace_id)?;
        let (connection_id, source_ns) = link_identity.unwrap_or_else(|| {
            cartridge_infos
                .first()
                .map(|(_, conn_id, src_ns)| (*conn_id, src_ns.clone()))
                .unwrap_or((None, None))
        });

        // Cascade delete — order matters for FK constraints
        // 1. Namespace linking tables
        bootstrap_conn.execute(
            "DELETE FROM namespace_local_alias WHERE namespace_id = ?1 OR target_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete namespace_local_alias", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM namespace_local_enlist WHERE namespace_id = ?1 OR enlisted_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete namespace_local_enlist", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM enlisted_entity WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete enlisted_entity", e.to_string())
            })?;

        bootstrap_conn.execute(
            "DELETE FROM enlisted_namespace WHERE from_namespace_id = ?1 OR to_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete enlisted_namespace", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM exposed_namespace WHERE exposing_namespace_id = ?1 OR exposed_namespace_id = ?1",
            [namespace_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete exposed_namespace", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM namespace_alias WHERE target_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete namespace_alias", e.to_string())
            })?;

        // The liminal ledger dies with its namespace (EFFECT-ALGEBRA §8:
        // catalog state, session-scoped; pinned by
        // `liminal_ledger_dies_with_namespace`).
        bootstrap_conn
            .execute(
                "DELETE FROM liminal_receipt WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete liminal_receipt", e.to_string())
            })?;

        // 2. Grounding table
        bootstrap_conn
            .execute(
                "DELETE FROM grounding WHERE grounded_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete grounding", e.to_string())
            })?;

        // 3. Entity-level tables (via cartridge)
        if !cartridge_infos.is_empty() {
            for (cartridge_id, _, _) in &cartridge_infos {
                // interior_entity_attribute (FK to interior_entity)
                bootstrap_conn.execute(
                    "DELETE FROM interior_entity_attribute WHERE interior_entity_id IN (
                        SELECT ie.id FROM interior_entity ie JOIN entity e ON ie.parent_entity_id = e.id
                        WHERE e.cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete interior_entity_attribute", e.to_string()))?;

                // interior_entity
                bootstrap_conn.execute(
                    "DELETE FROM interior_entity WHERE parent_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete interior_entity", e.to_string()))?;

                // ho_param_column (FK to ho_param)
                bootstrap_conn
                    .execute(
                        "DELETE FROM ho_param_column WHERE ho_param_id IN (
                        SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                        WHERE e.cartridge_id = ?1)",
                        [cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to delete ho_param_column",
                            e.to_string(),
                        )
                    })?;

                // entity_resolution
                bootstrap_conn.execute(
                    "DELETE FROM entity_resolution WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete entity_resolution", e.to_string()))?;

                // ho_param
                bootstrap_conn.execute(
                    "DELETE FROM ho_param WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete ho_param", e.to_string()))?;

                // join_edge
                bootstrap_conn.execute(
                    "DELETE FROM join_edge WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete join_edge", e.to_string()))?;

                bootstrap_conn.execute(
                    "DELETE FROM functional_dependency WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete functional_dependency", e.to_string()))?;

                // referenced_entity
                bootstrap_conn.execute(
                    "DELETE FROM referenced_entity WHERE containing_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete referenced_entity", e.to_string()))?;

                // entity_attribute
                bootstrap_conn.execute(
                    "DELETE FROM entity_attribute WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete entity_attribute", e.to_string()))?;

                // entity_clause
                bootstrap_conn.execute(
                    "DELETE FROM entity_clause WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete entity_clause", e.to_string()))?;

                // activated_entity
                bootstrap_conn.execute(
                    "DELETE FROM activated_entity WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete activated_entity", e.to_string()))?;

                // entity
                bootstrap_conn
                    .execute("DELETE FROM entity WHERE cartridge_id = ?1", [cartridge_id])
                    .map_err(|e| {
                        DelightQLError::database_error("Failed to delete entity", e.to_string())
                    })?;

                // cartridge
                bootstrap_conn
                    .execute("DELETE FROM cartridge WHERE id = ?1", [cartridge_id])
                    .map_err(|e| {
                        DelightQLError::database_error("Failed to delete cartridge", e.to_string())
                    })?;
            }
        } else {
            // No cartridge — still clean up activated_entity rows referencing this namespace
            bootstrap_conn
                .execute(
                    "DELETE FROM activated_entity WHERE namespace_id = ?1",
                    [namespace_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to delete activated_entity",
                        e.to_string(),
                    )
                })?;
        }

        // 4. Delete namespace itself
        bootstrap_conn
            .execute("DELETE FROM namespace WHERE id = ?1", [namespace_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete namespace", e.to_string())
            })?;

        txn.commit()?;

        Ok((connection_id, source_ns))
    }

    /// Unmount a data namespace, releasing its database connection.
    ///
    /// Validates the namespace is of kind 'data' and is not borrowed by any
    /// grounded namespace. If clear, cascade-deletes all bootstrap metadata
    /// and performs physical cleanup (DETACH or connection_map removal).
    pub fn unmount_database(&mut self, namespace: &str) -> Result<()> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // A consulted file may undo a mount it created itself, but may not
        // rearrange a mount owned by the caller's pre-program session. The
        // savepoint makes the catalog mutation reversible; this remains a
        // language/session policy rather than a rollback limitation.
        self.refuse_preexisting_namespace_mutation_in_program(
            namespace,
            "unmounting",
            "directive/unmount/uncompensable",
        )?;
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for unmount",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and is 'data' kind
        let (_ns_id, kind): (i64, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        if kind != "data" {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot unmount '{}' — it is a {} namespace. Use unconsult!() for lib/grounded namespaces.",
                    namespace, kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 2. Discover all descendant namespaces (for cascade)
        let pattern = format!("{}::%", namespace);
        let descendants: Vec<(String, String)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT fq_name, kind FROM namespace
                     WHERE fq_name LIKE ?1
                     ORDER BY length(fq_name) DESC",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([&pattern], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // 3. Borrow check: parent + descendants against external borrowers
        //    (the borrower named is the derived world's ROOT)
        {
            let borrower_info: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.root_namespace_id
                     JOIN namespace n_source ON n_source.id = g.data_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = borrower_info {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unmount '{}' — {} is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }

            // Also check lib borrows from descendants
            let lib_borrower: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.root_namespace_id
                     JOIN namespace n_source ON n_source.id = g.lib_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = lib_borrower {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unmount '{}' — {} is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }
        }

        // 4–6 run under ONE transaction window: the
        // catalog cascade AND the physical DETACHes commit or fail
        // together. A failed DETACH rolls the catalog back, so the mount
        // identity is retained and the operation reports failure — never
        // "unmounted" in the catalog with the database still attached.
        // (In-memory map removals are collected and applied only after
        // COMMIT, so memory follows the catalog.)
        // Snapshot the re-attach plan BEFORE the cascade (inside the window
        // the destroyed rows are already invisible): alias → source_path
        // for every attach-class mount under the target, so a mid-cascade
        // DETACH failure can restore already-detached siblings.
        let reattach_paths: std::collections::HashMap<String, String> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT m.attach_alias, n.source_path
                     FROM namespace n
                     JOIN mount m ON m.namespace_id = n.id
                     JOIN cartridge c ON c.id = m.cartridge_id
                     WHERE (n.fq_name = ?1 OR n.fq_name LIKE ?1 || '::%')
                       AND c.connection_id = ?2
                       AND m.class = 'attach'",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to snapshot unmount re-attach plan",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map(rusqlite::params![namespace, PRIMARY_CONNECTION_ID], |r| {
                    Ok((
                        r.get::<_, Option<String>>(0)?,
                        r.get::<_, Option<String>>(1)?,
                    ))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to snapshot unmount re-attach plan",
                        e.to_string(),
                    )
                })?;
            rows.flatten()
                .filter_map(|(alias, path)| Some((alias?, path?)))
                .collect()
        };

        let unmount_txn = BootstrapTxn::begin(&bootstrap_conn, "unmount_window")?;

        let mut schemas_to_detach: Vec<String> = Vec::new();
        let mut connections_to_remove: Vec<i64> = Vec::new();

        // Cascade delete: descendants first (deepest first), then parent
        for (desc_fq, desc_kind) in &descendants {
            let (connection_id, source_ns) = Self::destroy_namespace(&bootstrap_conn, desc_fq)?;
            if desc_kind == "data" {
                if let Some(conn_id) = connection_id {
                    if conn_id > 2 {
                        connections_to_remove.push(conn_id);
                    }
                }
                if let Some(schema) = source_ns {
                    schemas_to_detach.push(schema);
                }
            }
        }
        // `main` is a bootstrap FIXTURE:
        // unmount EMPTIES it back to its pre-created state instead of
        // destroying the row — destroying loses the wiring open() gave it
        // (home enlistment, unqualified-read routing), which a later
        // remount cannot recreate. The
        // next mount then takes the ordinary reuse-empty branch.
        let (connection_id, source_ns) = if namespace == "main" {
            Self::empty_main_namespace(&bootstrap_conn)?
        } else {
            Self::destroy_namespace(&bootstrap_conn, namespace)?
        };
        if let Some(conn_id) = connection_id {
            if conn_id > 2 {
                connections_to_remove.push(conn_id);
            }
        }
        if let Some(schema) = source_ns {
            schemas_to_detach.push(schema);
        }

        // Refcount, and the handover that makes it complete. One file may be
        // bound by more than one namespace — mounting it a second time reuses
        // the schema rather than opening the file twice — so unmounting one
        // binding must not pull the database out from under the others.
        //
        // Every alias reaching here was OWNED by a binding this cascade
        // destroyed; a borrowed one contributed none. If a binding survives
        // naming it, that binding inherits the ownership the destroyed one
        // held and the schema stays attached: someone must still be able to
        // close it, and without the handover the last one out would find
        // only borrowed rows and leak the attachment for the session.
        let mut retained = Vec::with_capacity(schemas_to_detach.len());
        for schema in schemas_to_detach {
            let heir: Option<i64> = bootstrap_conn
                .query_row(
                    "SELECT namespace_id FROM mount WHERE attach_alias = ?1 LIMIT 1",
                    [&schema],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to check surviving mount bindings",
                        e.to_string(),
                    )
                })?;
            match heir {
                Some(namespace_id) => {
                    bootstrap_conn
                        .execute(
                            "UPDATE mount SET attachment = 'owned' WHERE namespace_id = ?1",
                            [namespace_id],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to hand the attachment to a surviving binding",
                                e.to_string(),
                            )
                        })?;
                }
                None => retained.push(schema),
            }
        }
        let schemas_to_detach = retained;

        // Physical DETACH, inside the window: any failure returns Err and
        // the guard rolls the whole catalog cascade back. (`unmount_txn` is
        // on the BOOTSTRAP connection; the DETACH runs on the USER
        // connection, so the two cannot deadlock.)
        if !schemas_to_detach.is_empty() {
            let user_conn = self.connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire user connection lock for unmount detach",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            for (i, schema) in schemas_to_detach.iter().enumerate() {
                if let Err(e) = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]) {
                    // Best-effort: re-ATTACH any schemas already detached in
                    // this cascade (paths from the pre-cascade snapshot —
                    // inside the window the destroyed rows are invisible) so
                    // physical state matches the catalog the guard is about
                    // to restore. Failures here are logged — the returned
                    // error is the story.
                    for reattach in &schemas_to_detach[..i] {
                        if let Some(path) = reattach_paths.get(reattach) {
                            if let Err(re) = user_conn.execute(
                                &format!("ATTACH DATABASE '{}' AS '{}'", path, reattach),
                                &[],
                            ) {
                                debug!("unmount cleanup: re-ATTACH '{}' failed: {}", reattach, re);
                            }
                        }
                    }
                    return Err(DelightQLError::database_error(
                        format!(
                            "unmount!() failed: could not DETACH '{}' — the mount is \
                             retained (catalog rolled back): {}",
                            schema, e
                        ),
                        e.to_string(),
                    )); // unmount_txn rolls back on unwind
                }
            }
        }

        unmount_txn.commit()?;

        // A mount_tree! creates several binding rows over one connection.
        // Do not retire a live routing resource while a sibling binding still
        // references it; the relation is now the refcount authority.
        connections_to_remove.sort_unstable();
        connections_to_remove.dedup();
        let connections_to_remove: Vec<i64> = connections_to_remove
            .into_iter()
            .filter(|connection_id| {
                !bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(
                             SELECT 1 FROM mount m
                             JOIN cartridge c ON c.id = m.cartridge_id
                             WHERE c.connection_id = ?1
                         )",
                        [connection_id],
                        |row| row.get::<_, bool>(0),
                    )
                    .unwrap_or(true)
            })
            .collect();
        drop(bootstrap_conn);

        // Memory follows the committed catalog.
        for conn_id in connections_to_remove {
            self.connection_map.remove(&conn_id);
            self.schema_map.remove(&conn_id);
        }

        debug!(
            "unmount_database: Unmounted namespace '{}' (cascade-deleted {} descendants)",
            namespace,
            descendants.len()
        );
        Ok(())
    }

    /// Unconsult a lib/grounded/scratch namespace, removing all its definitions.
    ///
    /// Validates the namespace is not of kind 'data', 'system', or 'container'. For lib namespaces,
    /// checks that no grounded namespace borrows from it. Then cascade-deletes all
    /// bootstrap metadata.
    pub fn unconsult_namespace(&mut self, namespace: &str) -> Result<()> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // See unmount_database: pre-program deletions are uncompensable.
        self.refuse_preexisting_namespace_mutation_in_program(
            namespace,
            "unconsulting",
            "directive/unconsult/uncompensable",
        )?;
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for unconsult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and check kind
        let (_ns_id, kind): (i64, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        match kind.as_str() {
            "data" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — it is a data namespace. Use unmount!() instead.",
                        namespace
                    ),
                    "Wrong namespace kind",
                ));
            }
            "system" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — system namespaces cannot be removed.",
                        namespace
                    ),
                    "Protected namespace",
                ));
            }
            "container" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — structural container namespaces cannot be removed. Unmount or unconsult their child namespaces instead.",
                        namespace
                    ),
                    "Protected namespace",
                ));
            }
            "lib" | "grounded" | "scratch" | "unknown" => {
                // These are all acceptable for unconsult
            }
            other => panic!(
                "catch-all hit in system.rs unconsult_namespace: unexpected namespace kind: {}",
                other
            ),
        }

        // 2. Discover all descendant namespaces (deepest first for bottom-up deletion)
        let pattern = format!("{}::%", namespace);
        let descendants: Vec<(String, String)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT fq_name, kind FROM namespace
                     WHERE fq_name LIKE ?1
                     ORDER BY length(fq_name) DESC",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([&pattern], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query descendant namespaces",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // 3. Borrow check: find external borrowers of any namespace in the tree
        //    (lib borrowed as a derived world's source, data borrowed as its
        //     data world — the borrower is the world's ROOT, and only one
        //     OUTSIDE the tree counts)
        {
            let borrower_info: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.root_namespace_id
                     JOIN namespace n_source ON n_source.id = g.lib_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = borrower_info {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — descendant '{}' is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }

            // Also check data namespace borrows
            let data_borrower: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.root_namespace_id
                     JOIN namespace n_source ON n_source.id = g.data_namespace_id
                     WHERE (n_source.fq_name = ?1 OR n_source.fq_name LIKE ?2)
                       AND n_borrower.fq_name != ?1
                       AND n_borrower.fq_name NOT LIKE ?2
                     LIMIT 1",
                    rusqlite::params![namespace, &pattern],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((borrower_name, source_name)) = data_borrower {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot unconsult '{}' — descendant '{}' is borrowed by grounded namespace '{}'. \
                         Unconsult the grounded namespace first.",
                        namespace, source_name, borrower_name
                    ),
                    "Namespace borrowed",
                ));
            }
        }

        // 4. Cascade delete: descendants first (deepest first), then parent.
        // Map removals are collected and applied after the guard drops
        // (destroy_namespace now takes the caller's connection).
        let mut connections_to_remove: Vec<i64> = Vec::new();
        for (desc_fq, desc_kind) in &descendants {
            let (connection_id, _source_ns) = Self::destroy_namespace(&bootstrap_conn, desc_fq)?;
            if desc_kind == "data" {
                if let Some(conn_id) = connection_id {
                    if conn_id > 2 {
                        connections_to_remove.push(conn_id);
                    }
                }
            }
        }
        let _result = Self::destroy_namespace(&bootstrap_conn, namespace)?;
        drop(bootstrap_conn);
        for conn_id in connections_to_remove {
            self.connection_map.remove(&conn_id);
            self.schema_map.remove(&conn_id);
        }

        debug!(
            "unconsult_namespace: Unconsulted namespace '{}' (cascade-deleted {} descendants)",
            namespace,
            descendants.len()
        );
        Ok(())
    }

    /// Write HO parameter metadata to bootstrap from cross-clause position analysis.
    ///
    /// Inserts rows into ho_param and ho_param_column.
    /// based on the unified HoPositionInfo computed by `build_ho_position_analysis`.
    fn write_ho_params_to_bootstrap(
        bootstrap_conn: &Connection,
        entity_id: i32,
        positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
    ) -> Result<()> {
        use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundPattern};

        for pos_info in positions {
            let kind_str = match &pos_info.column_kind {
                HoColumnKind::TableGlob => "glob",
                HoColumnKind::TableArgumentative(_) => "argumentative",
                HoColumnKind::Rule(_) => "rule",
                HoColumnKind::Scalar => match &pos_info.ground_pattern {
                    Some(HoGroundPattern::AllClauses) => "ground_scalar",
                    Some(HoGroundPattern::SomeClauses) | None => "scalar",
                },
            };

            // Use column_name for param_name when available, fall back to position-based name
            let param_name_owned;
            let param_name = match &pos_info.column_name {
                Some(name) => name.as_str(),
                None => {
                    param_name_owned = format!("_pos{}", pos_info.position);
                    &param_name_owned
                }
            };

            bootstrap_conn
                .execute(
                    "INSERT INTO ho_param (entity_id, param_name, position, kind, column_name) VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![entity_id, param_name, pos_info.position as i32, kind_str, &pos_info.column_name],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert ho_param",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            let ho_param_id = bootstrap_conn.last_insert_rowid() as i32;

            // Write argumentative columns
            if let HoColumnKind::TableArgumentative(ref columns) = pos_info.column_kind {
                for (col_pos, col_name) in columns.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO ho_param_column (ho_param_id, column_name, column_position) VALUES (?1, ?2, ?3)",
                            rusqlite::params![ho_param_id, col_name, col_pos as i32],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert ho_param_column",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }
            }
        }

        Ok(())
    }

    /// THE DECLARED MODE, written as typed rows.
    ///
    /// Ordered by role and position, with the authored identifier's stropping
    /// bit beside its bytes — a stropped name compares verbatim and an
    /// unstropped one folds, and a pick agrees with a declared output only by
    /// that comparison.
    fn write_functional_dependency(
        conn: &Connection,
        entity_id: i64,
        mode: &crate::pipeline::asts::core::FactFunctionMode<
            crate::pipeline::asts::core::Unresolved,
        >,
    ) -> Result<()> {
        let rows = mode
            .inputs
            .iter()
            .map(|name| ("input", name))
            .enumerate()
            .chain(mode.outputs.iter().map(|name| ("output", name)).enumerate());
        for (position, (role, name)) in rows {
            conn.execute(
                "INSERT INTO functional_dependency \
                 (entity_id, role, position, attribute_name, stropped) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    entity_id,
                    role,
                    position as i64,
                    name.as_str(),
                    name.is_stropped() as i64,
                ],
            )
            .map_err(|e| {
                DelightQLError::database_error_with_source(
                    "Failed to insert functional_dependency",
                    e.to_string(),
                    Box::new(e),
                )
            })?;
        }
        Ok(())
    }

    /// Deep-copy all sub-tables for an entity (clause, attribute, referenced,
    /// ho_param+columns, join_edge, functional_dependency,
    /// interior_entity+attributes).
    pub(crate) fn copy_entity_subtables(
        conn: &Connection,
        old_entity_id: i32,
        new_entity_id: i32,
    ) -> Result<()> {
        // entity_clause
        conn.execute(
            "INSERT INTO entity_clause (entity_id, ordinal, definition, location)
             SELECT ?1, ordinal, definition, location
             FROM entity_clause WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        )
        .map_err(|e| {
            DelightQLError::database_error("Failed to copy entity_clause", e.to_string())
        })?;

        // entity_attribute
        conn.execute(
            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable, default_value)
             SELECT ?1, attribute_name, attribute_type, data_type, position, is_nullable, default_value
             FROM entity_attribute WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        ).map_err(|e| DelightQLError::database_error("Failed to copy entity_attribute", e.to_string()))?;

        // referenced_entity
        conn.execute(
            "INSERT INTO referenced_entity (name, namespace, apparent_type, containing_entity_id, location)
             SELECT name, namespace, apparent_type, ?1, location
             FROM referenced_entity WHERE containing_entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        ).map_err(|e| DelightQLError::database_error("Failed to copy referenced_entity", e.to_string()))?;

        // ho_param + ho_param_column (FK chain: entity → ho_param → child)
        {
            let mut stmt = conn
                .prepare("SELECT id, param_name, position, kind, column_name FROM ho_param WHERE entity_id = ?1")
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query ho_param", e.to_string())
                })?;
            let old_params: Vec<(i32, String, i32, String, Option<String>)> = stmt
                .query_map([old_entity_id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                })
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query ho_param", e.to_string())
                })?
                .flatten()
                .collect();

            for (old_hp_id, param_name, position, kind, column_name) in &old_params {
                conn.execute(
                    "INSERT INTO ho_param (entity_id, param_name, position, kind, column_name) VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![new_entity_id, param_name, position, kind, column_name],
                ).map_err(|e| DelightQLError::database_error("Failed to copy ho_param", e.to_string()))?;
                let new_hp_id = conn.last_insert_rowid() as i32;

                conn.execute(
                    "INSERT INTO ho_param_column (ho_param_id, column_name, column_position)
                     SELECT ?1, column_name, column_position
                     FROM ho_param_column WHERE ho_param_id = ?2",
                    rusqlite::params![new_hp_id, old_hp_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error("Failed to copy ho_param_column", e.to_string())
                })?;
            }
        }

        // join_edge
        conn.execute(
            "INSERT INTO join_edge (entity_id, left_spelling, right_spelling, context_name, clause_ordinal)
             SELECT ?1, left_spelling, right_spelling, context_name, clause_ordinal
             FROM join_edge WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        )
        .map_err(|e| DelightQLError::database_error("Failed to copy join_edge", e.to_string()))?;

        // functional_dependency — the declared mode travels with the entity
        // it is a capability of, or the copy would be relation-only.
        conn.execute(
            "INSERT INTO functional_dependency (entity_id, role, position, attribute_name, stropped)
             SELECT ?1, role, position, attribute_name, stropped
             FROM functional_dependency WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        )
        .map_err(|e| {
            DelightQLError::database_error("Failed to copy functional_dependency", e.to_string())
        })?;

        // interior_entity + interior_entity_attribute (FK chain)
        {
            let mut stmt = conn
                .prepare("SELECT id, column_name FROM interior_entity WHERE parent_entity_id = ?1")
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query interior_entity", e.to_string())
                })?;
            let old_ies: Vec<(i32, String)> = stmt
                .query_map([old_entity_id], |row| Ok((row.get(0)?, row.get(1)?)))
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query interior_entity", e.to_string())
                })?
                .flatten()
                .collect();

            for (old_ie_id, column_name) in &old_ies {
                conn.execute(
                    "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
                    rusqlite::params![new_entity_id, column_name],
                )
                .map_err(|e| {
                    DelightQLError::database_error("Failed to copy interior_entity", e.to_string())
                })?;
                let new_ie_id = conn.last_insert_rowid() as i32;

                conn.execute(
                    "INSERT INTO interior_entity_attribute (interior_entity_id, attribute_name, position, child_interior_entity_id)
                     SELECT ?1, attribute_name, position, child_interior_entity_id
                     FROM interior_entity_attribute WHERE interior_entity_id = ?2",
                    rusqlite::params![new_ie_id, old_ie_id],
                ).map_err(|e| DelightQLError::database_error("Failed to copy interior_entity_attribute", e.to_string()))?;
            }
        }

        Ok(())
    }

    /// Delete all entity sub-tables and the cartridge row for a single cartridge.
    /// FK-safe deletion order: interior_entity_attribute, interior_entity,
    /// ho_param_column, entity_resolution, ho_param, join_edge,
    /// functional_dependency,
    /// referenced_entity, entity_attribute, entity_clause, activated_entity, entity, cartridge.
    fn clear_cartridge_entities(bootstrap_conn: &Connection, cartridge_id: i64) -> Result<()> {
        bootstrap_conn
            .execute(
                "DELETE FROM interior_entity_attribute WHERE interior_entity_id IN (
                SELECT ie.id FROM interior_entity ie JOIN entity e ON ie.parent_entity_id = e.id
                WHERE e.cartridge_id = ?1)",
                [cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete interior_entity_attribute",
                    e.to_string(),
                )
            })?;

        bootstrap_conn.execute(
            "DELETE FROM interior_entity WHERE parent_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete interior_entity", e.to_string()))?;

        bootstrap_conn
            .execute(
                "DELETE FROM ho_param_column WHERE ho_param_id IN (
                SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                WHERE e.cartridge_id = ?1)",
                [cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete ho_param_column", e.to_string())
            })?;

        bootstrap_conn.execute(
            "DELETE FROM entity_resolution WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete entity_resolution", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM ho_param WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete ho_param", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM join_edge WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete join_edge", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM functional_dependency WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete functional_dependency", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM referenced_entity WHERE containing_entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete referenced_entity", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM entity_attribute WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete entity_attribute", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM entity_clause WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete entity_clause", e.to_string()))?;

        bootstrap_conn.execute(
            "DELETE FROM activated_entity WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete activated_entity", e.to_string()))?;

        bootstrap_conn
            .execute("DELETE FROM entity WHERE cartridge_id = ?1", [cartridge_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM cartridge WHERE id = ?1", [cartridge_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete cartridge", e.to_string())
            })?;

        Ok(())
    }

    /// Delete a single entity and all its sub-table rows.
    /// FK-safe deletion order matching clear_cartridge_entities.
    /// Does NOT delete the parent cartridge (caller may have other entities in it).
    fn clear_single_entity(bootstrap_conn: &Connection, entity_id: i64) -> Result<()> {
        bootstrap_conn
            .execute(
                "DELETE FROM interior_entity_attribute WHERE interior_entity_id IN (
            SELECT ie.id FROM interior_entity ie WHERE ie.parent_entity_id = ?1)",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete interior_entity_attribute",
                    e.to_string(),
                )
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM interior_entity WHERE parent_entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete interior_entity", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM ho_param_column WHERE ho_param_id IN (
            SELECT hp.id FROM ho_param hp WHERE hp.entity_id = ?1)",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete ho_param_column", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM entity_resolution WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity_resolution", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM ho_param WHERE entity_id = ?1", [entity_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete ho_param", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM join_edge WHERE entity_id = ?1", [entity_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete join_edge", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM functional_dependency WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete functional_dependency",
                    e.to_string(),
                )
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM referenced_entity WHERE containing_entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete referenced_entity", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM entity_attribute WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity_attribute", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM entity_clause WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity_clause", e.to_string())
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM activated_entity WHERE entity_id = ?1",
                [entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete activated_entity", e.to_string())
            })?;

        bootstrap_conn
            .execute("DELETE FROM entity WHERE id = ?1", [entity_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete entity", e.to_string())
            })?;

        Ok(())
    }

    /// Clear all content tables for a namespace, preserving identity/shell.
    ///
    /// Deletes: cartridge(s), entity(+all sub-tables), activated_entity,
    /// namespace_local_enlist, namespace_local_alias.
    /// Preserves: namespace row, enlisted_namespace, enlisted_entity,
    /// namespace_alias, grounding.
    ///
    /// Returns deleted cartridge metadata for physical cleanup by caller.
    pub(crate) fn clear_namespace_contents(
        bootstrap_conn: &Connection,
        namespace_id: i64,
    ) -> Result<Vec<(i64, Option<i64>, Option<String>)>> {
        // Collect ALL cartridge IDs for this namespace
        let cartridge_infos: Vec<(i64, Option<i64>, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT DISTINCT c.id, c.connection_id, c.source_ns
                 FROM cartridge c
                 JOIN entity e ON e.cartridge_id = c.id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE ae.namespace_id = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query cartridges for clear",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([namespace_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query cartridges for clear",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // Delete entity sub-tables for each cartridge in FK-safe order
        for (cartridge_id, _, _) in &cartridge_infos {
            Self::clear_cartridge_entities(bootstrap_conn, *cartridge_id)?;
        }

        // Clean up namespace-local tables
        bootstrap_conn
            .execute(
                "DELETE FROM namespace_local_enlist WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete namespace_local_enlist",
                    e.to_string(),
                )
            })?;

        bootstrap_conn
            .execute(
                "DELETE FROM namespace_local_alias WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete namespace_local_alias",
                    e.to_string(),
                )
            })?;

        // Safety: catch orphan activated_entity rows
        bootstrap_conn
            .execute(
                "DELETE FROM activated_entity WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete activated_entity orphans",
                    e.to_string(),
                )
            })?;

        // Reconsulting replaces the liminal ledger WHOLE (EFFECT-ALGEBRA §8:
        // the record describes THE load, not the history of loads; pinned by
        // `liminal_ledger_reconsult_replaces_whole`). The new file's receipts
        // are re-inserted by consult_file_inner in the same transaction.
        bootstrap_conn
            .execute(
                "DELETE FROM liminal_receipt WHERE namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to clear liminal_receipt", e.to_string())
            })?;

        Ok(cartridge_infos)
    }

    /// DELETE a namespace's current load whole — its definition families
    /// (with every sub-table), its declared enlist/alias/exposure edges,
    /// and its liminal ledger — inside the caller's savepoint, so the
    /// replacement that follows lands atomically with the deletion or the
    /// prior load stands untouched.
    fn delete_namespace_load(bootstrap_conn: &Connection, namespace_id: i64) -> Result<()> {
        Self::clear_namespace_contents(bootstrap_conn, namespace_id)?;
        bootstrap_conn
            .execute(
                "DELETE FROM exposed_namespace WHERE exposing_namespace_id = ?1",
                [namespace_id],
            )
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete exposed_namespace", e.to_string())
            })?;
        Ok(())
    }

    /// Ground a lib namespace into a new namespace, binding it to a data namespace
    ///
    /// Derives the reachable lexical definition closure of `lib_ns` — its
    /// families and, as derivatives under the new namespace, every library
    /// it reaches — bound to `data_ns`, and admits every reference of every
    /// derivative before anything is published (see
    /// `defuse::grounded_world`). The new namespace has `default_data_ns`
    /// set so its bodies' data holes read `data_ns`.
    ///
    /// # Arguments
    /// * `data_ns` - Data namespace (e.g., "data::production")
    /// * `lib_ns` - Library namespace containing definitions (e.g., "lib::analytics")
    /// * `new_ns_name` - Name for the new grounded namespace (e.g., "lib::analytics_prod")
    ///
    /// # Returns
    /// Number of entities grounded
    pub fn ground_namespace(
        &mut self,
        data_ns: &str,
        lib_ns: &str,
        new_ns_name: &str,
    ) -> Result<usize> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // System name guard: `new_ns_name` is the
        // USER-TYPED creation target. (`data_ns`/`lib_ns` must already exist, so
        // they are validated by lookup below, not by this creation guard.)
        validate_user_namespace_target(new_ns_name)?;

        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for ground",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate data_ns exists
        let data_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [data_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Data namespace '{}' not found. Mount it first with mount!().",
                        data_ns
                    ),
                    "Namespace not found",
                )
            })?;

        // 2. Validate lib_ns exists
        let lib_ns_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [lib_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Library namespace '{}' not found. Consult it first with consult!().",
                        lib_ns
                    ),
                    "Namespace not found",
                )
            })?;

        // Blueprint inertness (M2): grounding animates lib_ns's rules against
        // data_ns's data. If EITHER is an archived blueprint (or a descendant
        // of one), the grounded namespace would resurrect the inert archive —
        // rules going live from lib_ns, or archived data being read from
        // data_ns. Refuse both directions. (new_ns_name cannot be a blueprint:
        // ensure_namespace_available below refuses any existing name, and a
        // blueprint fq always already exists.)
        refuse_if_blueprint(&bootstrap_conn, lib_ns)?;
        refuse_if_blueprint(&bootstrap_conn, data_ns)?;

        // 3. Validate new_ns_name does NOT exist
        ensure_namespace_available(&bootstrap_conn, new_ns_name)?;

        // 4. Retrieve all entities from lib_ns
        let entities: Vec<(i32, String, bool, i32, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.id, e.name, e.name_stropped, e.type, e.doc
                     FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON n.id = ae.namespace_id
                     WHERE n.fq_name = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query lib namespace entities",
                        e.to_string(),
                    )
                })?;

            let rows = match stmt.query_map([lib_ns], |row| {
                Ok((
                    row.get::<_, i32>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, i32>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            }) {
                Ok(r) => r,
                Err(e) => {
                    return Err(DelightQLError::database_error(
                        "Failed to query lib namespace entities",
                        e.to_string(),
                    ));
                }
            };
            rows.flatten().collect()
        };

        // 4b. Discover manifest-only entities from _internal (if lib_ns has none of its own)
        use crate::ddl::manifest;
        let internal_ns_id = manifest::find_internal_ns(&bootstrap_conn, lib_ns)?;

        let manifest_entity_names: Vec<String> = if entities.is_empty() {
            if let Some(int_ns_id) = internal_ns_id {
                manifest::discover_schema_entities(&bootstrap_conn, int_ns_id)?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // 5. NO INTERSECTION: a name defined by BOTH the library and the
        //    data namespace would make every use of that name two-headed,
        //    so grounding refuses it by name and creates nothing. This is a
        //    namespace-level law over the two name sets; it classifies no
        //    reference — the lexical-link / data-hole judgment is the
        //    authority's, applied to the derived world below.
        for (_, entity_name, _entity_stropped, _entity_type, _doc) in &entities {
            let intersects: bool = bootstrap_conn
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM entity e
                        JOIN activated_entity ae ON ae.entity_id = e.id
                        WHERE ae.namespace_id = ?1 AND e.name = ?2 COLLATE NOCASE
                    )",
                    rusqlite::params![data_ns_id, entity_name],
                    |row| row.get(0),
                )
                .unwrap_or(false);
            if intersects {
                return Err(DelightQLError::validation_error_categorized(
                    crate::uri_registry::subcat::GROUND_NAME_INTERSECTION,
                    format!(
                        "ground!() refuses: '{entity_name}' is defined by BOTH the \
                         library '{lib_ns}' and the data namespace '{data_ns}'. A \
                         shared name makes every use of it ambiguous — grounding is \
                         refused whole and nothing is created (No intersection)."
                    ),
                    "grounding name intersection",
                ));
            }
        }
        // THE DERIVATION IS ONE TRANSACTION: the namespace, its lexical
        // graph, its families, and the admission judgment land together or
        // not at all — a refusal below rolls the derivation back whole.
        let transaction = CatalogSavepoint::begin(
            &bootstrap_conn,
            "dql_ground_namespace",
            "Failed to begin ground transaction",
        )?;

        // 6. Create new namespace with default_data_ns
        let new_ns_id = {
            let name = new_ns_name.split("::").last().unwrap_or(new_ns_name);
            bootstrap_conn
                .execute(
                    "INSERT INTO namespace (name, pid, fq_name, default_data_ns, kind, provenance)
                     VALUES (?1, NULL, ?2, ?3, 'grounded', 'ground')",
                    rusqlite::params![name, new_ns_name, data_ns],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to create grounded namespace",
                        e.to_string(),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // 7. THE DERIVED WORLD: the root's families, the library's declared
        // lexical graph with every derivable target rewired to its own
        // derivative, and every reachable dependency derived the same way
        // under this one data world — the closure the catalog records,
        // one `grounding` row per derivative.
        let world = crate::defuse::grounded_world::DerivedWorld::derive(
            &bootstrap_conn,
            new_ns_id as i64,
            lib_ns_id as i64,
            data_ns_id as i64,
        )?;
        // 8. The root's manifest (`_internal`) companions: a TEMP table
        // for each derived family the manifest describes.
        let mut count = world.root_families();
        for (_, entity_name, _, _, _) in &entities {
            // If entity has manifest data in _internal, create TEMP table from it
            if let Some(int_ns_id) = internal_ns_id {
                if let Some(result) = crate::ddl_pipeline::create_temp_table_from_manifest(
                    &bootstrap_conn,
                    int_ns_id,
                    entity_name,
                    self.bin_registry(),
                )? {
                    bootstrap_conn
                        .execute_batch(&result.create_sql)
                        .map_err(|e| {
                            DelightQLError::database_error(
                                format!(
                                    "Failed to CREATE TEMP TABLE for '{}': {}",
                                    entity_name, result.create_sql
                                ),
                                e.to_string(),
                            )
                        })?;
                }
            }
        }

        // 8b. Create manifest-only entities (discovered from _internal, no entity in lib_ns)
        if let (Some(int_ns_id), false) = (internal_ns_id, manifest_entity_names.is_empty()) {
            // They register under the root's derivation cartridge, minted
            // here when the root derived no family of its own — a
            // cartridge exists only where entities stand under it.
            let cartridge_id = match world.root_cartridge() {
                Some(cartridge_id) => cartridge_id,
                None => crate::defuse::grounded_world::derivation_cartridge(
                    &bootstrap_conn,
                    lib_ns,
                    data_ns,
                )?,
            };
            for entity_name in &manifest_entity_names {
                let result = match crate::ddl_pipeline::create_temp_table_from_manifest(
                    &bootstrap_conn,
                    int_ns_id,
                    entity_name,
                    self.bin_registry(),
                )? {
                    Some(r) => r,
                    None => continue,
                };
                let crate::ddl_pipeline::ManifestCreateResult {
                    create_sql,
                    schema_rows,
                } = result;
                bootstrap_conn.execute_batch(&create_sql).map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to CREATE TEMP TABLE for '{}': {}",
                            entity_name, create_sql
                        ),
                        e.to_string(),
                    )
                })?;

                // Register entity in bootstrap
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![
                            entity_name,
                            EntityType::DbTemporaryTable.as_i32(),
                            cartridge_id,
                            format!("Grounded from {} manifest", lib_ns),
                        ],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to create grounded entity '{}'", entity_name),
                            e.to_string(),
                        )
                    })?;
                let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

                // Register entity attributes from manifest schema rows
                for (position, sr) in schema_rows.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable)
                             VALUES (?1, ?2, 'output_column', ?3, ?4, 1)",
                            rusqlite::params![new_entity_id, &sr.name, &sr.col_type, position as i32 + 1],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                format!("Failed to register attribute '{}' for '{}'", sr.name, entity_name),
                                e.to_string(),
                            )
                        })?;
                }

                // Activate entity in grounded namespace
                bootstrap_conn
                    .execute(
                        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![new_entity_id, new_ns_id, cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to activate grounded entity '{}'", entity_name),
                            e.to_string(),
                        )
                    })?;

                count += 1;
            }
        }

        // A world that derives no family anywhere — not in the root, not in
        // any dependency, not from a manifest — has nothing to ground; a
        // pure facade over derivable children is not empty.
        if world.families() == 0 && manifest_entity_names.is_empty() {
            return Err(DelightQLError::database_error(
                format!("Library namespace '{}' has no entities to ground", lib_ns),
                "Empty namespace",
            ));
        }

        // 9. ADMISSION: every reference every derivative recorded is judged
        // by the one lexical-link / data-hole judgment body opening uses,
        // under the derivative's own reach and bound to the data namespace;
        // a qualified reference reaching a derivable namespace derives it.
        // A refusal rolls the derivation back whole.
        world.admit(&bootstrap_conn, crate::defuse::CatalogRead::of(self))?;

        transaction.commit("Failed to commit ground transaction")?;
        drop(bootstrap_conn);

        debug!(
            "ground_namespace: Grounded {} entities from '{}' into '{}' (data: '{}')",
            count, lib_ns, new_ns_name, data_ns
        );

        Ok(count)
    }

    /// Set the `doc` string on a catalog entity, addressed by its
    /// fully-qualified name (e.g. `"sys::identifiers.identifier"`).
    ///
    /// The fq name is `<namespace fq_name>.<entity name>`, so it is matched
    /// against `n.fq_name || '.' || e.name` over activated entities — the same
    /// namespace/activated_entity join every other catalog lookup uses. Only
    /// activated entities are considered; if the name still resolves to more
    /// than one entity (a same-name collision within one namespace across
    /// cartridges) it is reported as ambiguous.
    ///
    /// Session-scoped: writes the in-memory bootstrap catalog for this session.
    /// Conn-level doc write: the deferred
    /// liminal doc!s apply INSIDE the consult transaction. Same candidate
    /// resolution as `set_entity_doc`, against the provided connection.
    pub(crate) fn set_entity_doc_on(
        conn: &rusqlite::Connection,
        target: &str,
        doc: &str,
    ) -> Result<()> {
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT e.id FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE n.fq_name || '.' || e.name = ?1",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare entity lookup for doc!()",
                    e.to_string(),
                )
            })?;
        let ids: Vec<i64> = stmt
            .query_map([target], |row| row.get(0))
            .map_err(|e| {
                DelightQLError::database_error("Failed to resolve doc!() target", e.to_string())
            })?
            .collect::<std::result::Result<Vec<i64>, _>>()
            .map_err(|e| {
                DelightQLError::database_error("Failed to resolve doc!() target", e.to_string())
            })?;
        drop(stmt);
        match ids.as_slice() {
            [] => Err(DelightQLError::database_error(
                format!("no such entity '{}'", target),
                "doc!() target not found",
            )),
            [entity_id] => {
                conn.execute(
                    "UPDATE entity SET doc = ?1 WHERE id = ?2",
                    rusqlite::params![doc, entity_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to set doc on entity '{}'", target),
                        e.to_string(),
                    )
                })?;
                Ok(())
            }
            many => Err(DelightQLError::database_error(
                format!(
                    "ambiguous doc!() target '{}' resolves to {} entities",
                    target,
                    many.len()
                ),
                "Ambiguous entity reference",
            )),
        }
    }

    pub fn set_entity_doc(&mut self, target: &str, doc: &str) -> Result<(String, String)> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for doc",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let mut stmt = bootstrap_conn
            .prepare(
                "SELECT DISTINCT e.id FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE n.fq_name || '.' || e.name = ?1",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare entity lookup for doc!()",
                    e.to_string(),
                )
            })?;

        let ids: Vec<i64> = stmt
            .query_map([target], |row| row.get(0))
            .map_err(|e| {
                DelightQLError::database_error("Failed to resolve doc!() target", e.to_string())
            })?
            .collect::<std::result::Result<Vec<i64>, _>>()
            .map_err(|e| {
                DelightQLError::database_error("Failed to resolve doc!() target", e.to_string())
            })?;

        match ids.as_slice() {
            [] => Err(DelightQLError::database_error(
                format!("no such entity '{}'", target),
                "doc!() target not found",
            )),
            [entity_id] => {
                bootstrap_conn
                    .execute(
                        "UPDATE entity SET doc = ?1 WHERE id = ?2",
                        rusqlite::params![doc, entity_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to set doc on entity '{}'", target),
                            e.to_string(),
                        )
                    })?;
                Ok((target.to_string(), doc.to_string()))
            }
            many => Err(DelightQLError::database_error(
                format!(
                    "ambiguous doc!() target '{}' resolves to {} entities",
                    target,
                    many.len()
                ),
                "Ambiguous entity reference",
            )),
        }
    }

    /// Imprint definitions from a library namespace into a data namespace.
    ///
    /// Reads manifest data from the `_internal` child namespace (schema, constraints,
    /// defaults, imprinting HO entities), assembles CREATE TABLE DDL, and executes
    /// on the target database. For CTAS entities, populates via INSERT INTO ... SELECT.
    ///
    /// Returns a list of (entity_name, status, sql) tuples for reporting.
    /// [`ImprintMode::Strict`] (imprint!): a pre-flight clash on any target
    /// object fails the whole operation before anything is created.
    /// [`ImprintMode::Replace`] (imprint_replace!): each clashing target object
    /// is dropped first, then recreated. Either way the check/drop happens up
    /// front, atomically.
    pub fn imprint_namespace(
        &mut self,
        source_ns: &str,
        target_ns: &str,
        mode: ImprintMode,
    ) -> Result<Vec<(String, String, String)>> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let replace = matches!(mode, ImprintMode::Replace);
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for imprint",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate source namespace exists and is a lib namespace
        let (source_ns_id, source_kind): (i32, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [source_ns],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Source namespace '{}' not found. Consult it first with consult!().",
                        source_ns
                    ),
                    "Namespace not found",
                )
            })?;

        if source_kind == "data" || source_kind == "system" || source_kind == "container" {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() source '{}' is a {} namespace. Source must be a lib namespace.",
                    source_ns, source_kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 2. Check borrow: source must not be borrowed by any active grounding
        let borrowed: bool = bootstrap_conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM grounding WHERE lib_namespace_id = ?1)",
                [source_ns_id],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if borrowed {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() cannot consume '{}' — it is borrowed by an active grounding. \
                     Unconsult the grounded namespace first.",
                    source_ns
                ),
                "Source namespace borrowed",
            ));
        }

        // 3. Validate target namespace exists and is a data namespace
        let (target_ns_id, target_kind): (i32, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [target_ns],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!(
                        "Target namespace '{}' not found. Mount it first with mount!().",
                        target_ns
                    ),
                    "Namespace not found",
                )
            })?;

        if target_kind != "data" {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() target '{}' is a {} namespace. Target must be a data namespace.",
                    target_ns, target_kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 4. Get target connection info: schema alias + connection_id, via
        // the STORED mount link. An entity-path + source-match fallback
        // is ambiguous once simultaneous same-source mounts are legal — an
        // ORDER BY c.id DESC would choose the NEWEST cartridge for the source,
        // not the cartridge of the REQUESTED namespace, so imprinting into `a`
        // could target `b`'s image. The link answers for the namespace
        // itself, empty or not.
        let (target_schema_alias, connection_id): (Option<String>, i64) = bootstrap_conn
            .query_row(
                "SELECT CASE WHEN m.qualification IN ('aliased', 'engine_schema')
                                THEN COALESCE(m.attach_alias, m.engine_schema)
                            ELSE NULL END,
                        c.connection_id
                 FROM mount m
                 JOIN cartridge c ON c.id = m.cartridge_id
                 JOIN namespace n ON n.id = m.namespace_id
                 WHERE n.fq_name = ?1",
                [target_ns],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .or_else(|_| {
                // Non-mount data namespaces (no link) fall through to the
                // entity path: their contents live on whatever connection
                // their activated entities record.
                bootstrap_conn.query_row(
                    "SELECT c.source_ns, c.connection_id
                     FROM cartridge c
                     JOIN entity e ON e.cartridge_id = c.id
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     JOIN namespace n ON ae.namespace_id = n.id
                     WHERE n.fq_name = ?1
                     LIMIT 1",
                    [target_ns],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
            })
            .unwrap_or_else(|_| (None, 2)); // primary connection (CLI --db as primary), no schema

        let target_conn = self
            .connection_map
            .get(&connection_id)
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.connection));

        // 4b. A mounted "main" carries no alias in its cartridge (reads
        // resolve unqualified by design), but WRITES cannot: unqualified
        // CREATE always lands in the connection's primary schema, not the
        // mounted file. Recover the attach alias from the connection itself.
        let target_schema_alias: Option<String> = match target_schema_alias {
            Some(a) => Some(a),
            None => {
                let mounted_path: Option<String> = bootstrap_conn
                    .query_row(
                        "SELECT source_path FROM namespace
                         WHERE fq_name = ?1 AND source_path IS NOT NULL",
                        [target_ns],
                        |row| row.get(0),
                    )
                    .ok();
                mounted_path.and_then(|path| {
                    let want = std::fs::canonicalize(&path).ok()?;
                    let conn = target_conn.lock().ok()?;
                    let (_cols, rows) = conn.query_all_rows("PRAGMA database_list", &[]).ok()?;
                    rows.iter().find_map(|row| {
                        let alias = row.get(1)?.as_wire_text()?;
                        let file = row.get(2)?.as_wire_text()?;
                        if alias == "main" || file.is_empty() {
                            return None;
                        }
                        (std::fs::canonicalize(file).ok()? == want).then_some(alias)
                    })
                })
            }
        };

        // 5. Find _internal child namespace for manifest data
        use crate::ddl::manifest;

        let internal_ns_id =
            manifest::find_internal_ns(&bootstrap_conn, source_ns)?.ok_or_else(|| {
                DelightQLError::database_error(
                    format!(
                        "imprint!() source '{}' has no schema definitions to \
                         materialize. imprint! consumes a library whose .dql \
                         file declares companion definitions — schema `(^)`, \
                         constraints `(+)`, and/or defaults `($)` sigil blocks \
                         alongside the rules (these populate the library's \
                         _internal namespace). A rules-only library has \
                         nothing to imprint; to persist its VIEWS, run the \
                         queries via `dql query --to sql` against the target \
                         instead.",
                        source_ns
                    ),
                    "No _internal namespace",
                )
            })?;

        // Discover entities: prefer imprinting() manifest, fall back to schema() ground values
        let imprinting_rows = manifest::read_imprinting(&bootstrap_conn, internal_ns_id)?;

        struct EntityTodo {
            name: String,
            materialization: manifest::Materialization,
            extent: manifest::Extent,
        }

        let entity_todos: Vec<EntityTodo> = if !imprinting_rows.is_empty() {
            imprinting_rows
                .into_iter()
                .map(|row| EntityTodo {
                    name: row.entity,
                    materialization: row.materialization,
                    extent: row.extent,
                })
                .collect()
        } else {
            // No imprinting() — discover from schema() ground values
            let schema_entities =
                manifest::discover_schema_entities(&bootstrap_conn, internal_ns_id)?;
            schema_entities
                .into_iter()
                .map(|name| EntityTodo {
                    name,
                    materialization: manifest::Materialization::Table,
                    extent: manifest::Extent::Permanent,
                })
                .collect()
        };

        if entity_todos.is_empty() {
            return Err(DelightQLError::database_error(
                format!(
                    "imprint!() source '{}' has no manifest entities \
                     (no schema() or imprinting() definitions in _internal)",
                    source_ns
                ),
                "No manifest entities",
            ));
        }

        // --- Phase 0: Read ALL manifest data from bootstrap, then drop the lock ---
        // self.schema is BootstrapBackedSchema which locks self.bootstrap_connection
        // internally. compile_source_to_sql -> resolver -> schema.get_table_columns()
        // -> BootstrapBackedSchema::get_table_columns() -> self.bootstrap_conn.lock().
        // If we still hold bootstrap_conn here, that's a deadlock. So we read
        // everything we need, drop the lock, then compile in Phase 1.

        struct ManifestData {
            name: String,
            materialization: manifest::Materialization,
            extent: manifest::Extent,
            schema_rows: Vec<manifest::SchemaRow>,
            constraint_rows: Vec<manifest::ConstraintRow>,
            default_rows: Vec<manifest::DefaultRow>,
            ctas_body: Option<String>,
        }

        let mut manifest_items: Vec<ManifestData> = Vec::new();

        for todo in &entity_todos {
            let entity_name = &todo.name;

            let schema_rows = manifest::read_schema(&bootstrap_conn, internal_ns_id, entity_name)?;
            let constraint_rows =
                manifest::read_constraints(&bootstrap_conn, internal_ns_id, entity_name)?;
            let default_rows =
                manifest::read_defaults(&bootstrap_conn, internal_ns_id, entity_name)?;

            // Check for CTAS body: an entity with a view body in the source namespace
            let ctas_body: Option<String> = {
                let stmt = bootstrap_conn
                    .prepare(
                        "SELECT ec.definition FROM entity_clause ec
                         JOIN entity e ON ec.entity_id = e.id
                         JOIN activated_entity ae ON ae.entity_id = e.id
                         WHERE ae.namespace_id = ?1 AND e.name = ?2
                         ORDER BY ec.ordinal LIMIT 1",
                    )
                    .ok();
                stmt.and_then(|mut s| {
                    s.query_row(rusqlite::params![source_ns_id, entity_name], |row| {
                        row.get::<_, String>(0)
                    })
                    .ok()
                })
                .and_then(|def| {
                    let body = crate::ddl::reconstruct::body_text(&def);
                    (!body.is_empty() && body != def).then_some(body)
                })
            };

            manifest_items.push(ManifestData {
                name: entity_name.clone(),
                materialization: todo.materialization.clone(),
                extent: todo.extent.clone(),
                schema_rows,
                constraint_rows,
                default_rows,
                ctas_body,
            });
        }

        // Drop bootstrap lock -- Phase 1 needs schema access which locks bootstrap internally
        drop(bootstrap_conn);

        // --- Phase 1: Compile (no bootstrap lock held) ---
        // compile_source_to_sql -> resolver -> schema.get_table_columns()
        // -> BootstrapBackedSchema -> self.bootstrap_connection.lock(). Safe now.
        let empty_schema = crate::ddl::manifest::EmptySchema;
        let schema: &dyn DatabaseSchema = if connection_id == PRIMARY_CONNECTION_ID {
            self.schema
                .as_ref()
                .map(|s| s.as_ref())
                .unwrap_or(&empty_schema)
        } else {
            self.schema_map
                .get(&connection_id)
                .map(|s| s.as_ref())
                .unwrap_or(&empty_schema)
        };

        // How each prepared entity is materialized. A single enum instead of
        // shadow flags (a `materialization` string + a boolean CTAS flag + an
        // optional insert SQL + `effective_schema`-emptiness-as-type-tag)
        // makes the three variants exhaustive and the
        // discriminator un-driftable: a new materialization kind is a new
        // enum variant, never another boolean. Payload carries exactly what the catalog
        // pass needs per variant — DeclaredTable knows its columns up front (no
        // PRAGMA readback) and may carry an INSERT…SELECT; View/CtasTable read
        // their columns back from the committed object.
        enum Materialized {
            /// `CREATE VIEW … AS <select>`. entity_type = DbPermanentView; attrs read back.
            View,
            /// `CREATE TABLE … AS SELECT`. entity_type = DbPermanentTable; attrs read back.
            CtasTable,
            /// Typed `CREATE TABLE` from a schema()/constraints()/defaults()
            /// declaration (always non-empty schema), optionally populated by a
            /// trailing INSERT…SELECT when the entity carries a rule body.
            DeclaredTable {
                schema: Vec<manifest::SchemaRow>,
                insert: Option<String>,
            },
        }

        struct PreparedEntity {
            name: String,
            qualified_create: String,
            materialized: Materialized,
        }

        let mut prepared: Vec<PreparedEntity> = Vec::new();

        for item in &manifest_items {
            let entity_name = &item.name;

            let temp = item.extent == manifest::Extent::Temporary;

            // TEMP extent + a mounted/aliased target is invalid in SQLite:
            // `CREATE TEMP TABLE "alias"."x"` fails with "temporary table name
            // must be unqualified". Refuse cleanly at prepare time (before any
            // mutation) rather than letting it surface as a raw exec error
            // mid-imprint. Pinned by
            // system::imprint_helper_tests + companion_linear--77.
            if temp && target_schema_alias.is_some() {
                return Err(DelightQLError::database_error(
                    format!(
                        "imprint!() entity '{}' is temporary but the target namespace is a \
                         mounted (aliased) database — SQLite requires a temporary table/view \
                         name to be unqualified, so it cannot be created in an attached schema. \
                         Imprint it as permanent, or imprint into the primary (unmounted) target. \
                         (A future fix can create it in the mounted connection's own temp schema.)",
                        entity_name
                    ),
                    "Temporary imprint into a mounted target",
                ));
            }

            // Compile the rule body to SELECT once — used by view, CTAS, and the
            // declared-path INSERT (schema access locks bootstrap internally, safe now).
            let ctas_select_sql = if let Some(body) = &item.ctas_body {
                Some(crate::pipeline::compile_source_to_sql(body, schema)?)
            } else {
                None
            };

            let qualified_table = |name: &str| -> String {
                if let Some(schema_name) = target_schema_alias.as_deref() {
                    format!("{}.{}", quote_ident(schema_name), quote_ident(name))
                } else {
                    quote_ident(name)
                }
            };

            // Does the entity carry a declaration (schema / constraints / defaults)?
            let has_decl = !item.schema_rows.is_empty()
                || !item.constraint_rows.is_empty()
                || !item.default_rows.is_empty();

            // --- View materialization: a stored query, evaluated live on read;
            // no own data. Orthogonal to extent (TEMP applies as it does to tables). ---
            if item.materialization == manifest::Materialization::View {
                // A view cannot carry a declaration — no column types/constraints/
                // defaults on a view. v1 requires a bare rule.
                if has_decl {
                    return Err(DelightQLError::database_error(
                        format!(
                            "imprint!() entity '{}' is a view but declares schema/constraints/defaults — \
                             a view cannot carry them; drop the companions or materialize it as a table",
                            entity_name
                        ),
                        "View with a declaration",
                    ));
                }
                let select_sql = ctas_select_sql.ok_or_else(|| {
                    DelightQLError::database_error(
                        format!(
                            "imprint!() view '{}' has no rule body — a view is a query",
                            entity_name
                        ),
                        "View without a rule body",
                    )
                })?;
                let temp_kw = if temp { "TEMP " } else { "" };
                let qualified_create = format!(
                    "CREATE {}VIEW {} AS {}",
                    temp_kw,
                    qualified_table(entity_name),
                    select_sql
                );
                prepared.push(PreparedEntity {
                    name: entity_name.clone(),
                    qualified_create,
                    materialized: Materialized::View,
                });
                continue;
            }

            // --- Table materialization forks on the declaration signal:
            //   declared  → typed CREATE TABLE (from schema) [+ INSERT … SELECT]
            //   bare rule → CREATE TABLE … AS SELECT (engine derives real types)
            // Constraints/defaults require a
            // schema() so column types are always declared, not guessed.
            if item.schema_rows.is_empty()
                && (!item.constraint_rows.is_empty() || !item.default_rows.is_empty())
            {
                return Err(DelightQLError::database_error(
                    format!(
                        "imprint!() entity '{}' declares constraints/defaults but no schema() — \
                         declare column types in a schema(\"{}\") companion",
                        entity_name, entity_name
                    ),
                    "Constraints/defaults without a schema declaration",
                ));
            }

            if has_decl {
                // Declared path: typed CREATE from schema/constraints/defaults,
                // then INSERT … SELECT to populate if there is a rule body.
                let unresolved = crate::ddl_pipeline::assemble_manifest::assemble_from_manifest(
                    entity_name,
                    temp,
                    &item.schema_rows,
                    &item.constraint_rows,
                    &item.default_rows,
                )?;
                let (resolved, identities) = crate::ddl_pipeline::resolver::resolve(unresolved)?;
                let sql_ast = crate::ddl_pipeline::transformer::transform(resolved, &identities)?;
                let create_sql = crate::ddl_pipeline::generator::generate(
                    &sql_ast,
                    &identities,
                    self.bin_registry(),
                )?;

                // The generator emits `CREATE TABLE "<name>"` via its own
                // `write_quoted` (raw quotes). Since manifest-read forbids a `"`
                // in an entity name (manifest::validate_entity_name),
                // `quote_ident(name)` is byte-identical to what the generator
                // produced, so the search pattern matches and the replacement
                // qualifies the name with the (escaped) schema alias.
                let qualified_create = if let Some(schema_name) = target_schema_alias.as_deref() {
                    create_sql.replacen(
                        &format!("CREATE TABLE {}", quote_ident(entity_name)),
                        &format!(
                            "CREATE TABLE {}.{}",
                            quote_ident(schema_name),
                            quote_ident(entity_name)
                        ),
                        1,
                    )
                } else {
                    create_sql
                };

                let insert = ctas_select_sql.map(|select_sql| {
                    format!(
                        "INSERT INTO {} {}",
                        qualified_table(entity_name),
                        select_sql
                    )
                });

                prepared.push(PreparedEntity {
                    name: entity_name.clone(),
                    qualified_create,
                    materialized: Materialized::DeclaredTable {
                        schema: item.schema_rows.clone(),
                        insert,
                    },
                });
            } else if let Some(select_sql) = ctas_select_sql {
                // Bare rule, no declaration: CREATE TABLE … AS SELECT. The engine
                // derives real column types; attributes are read back post-create.
                let temp_kw = if temp { "TEMP " } else { "" };
                let qualified_create = format!(
                    "CREATE {}TABLE {} AS {}",
                    temp_kw,
                    qualified_table(entity_name),
                    select_sql
                );

                prepared.push(PreparedEntity {
                    name: entity_name.clone(),
                    qualified_create,
                    materialized: Materialized::CtasTable,
                });
            } else {
                return Err(DelightQLError::database_error(
                    format!(
                        "imprint!() entity '{}' has neither a schema() nor a rule body — \
                         nothing to materialize",
                        entity_name
                    ),
                    "No schema and no rule body",
                ));
            }
        }

        // --- Phase 2: Execute (re-acquire bootstrap + target locks) ---
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to re-acquire bootstrap lock for imprint execution",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let target_conn_guard = target_conn.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire target connection lock for imprint",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Enable FK enforcement on the shared target connection. This sets the
        // persistent, connection-global flag ON; the destructive DDL below runs
        // inside a transaction where `defer_foreign_keys` (not a toggle of this
        // flag) relaxes drop/create ordering, so no error path can leave
        // enforcement disabled (companion_linear--66).
        let _ = target_conn_guard.execute("PRAGMA foreign_keys = ON", &[]);

        // --- Pre-flight clash pass (READ-ONLY, before any mutation) ---
        // imprint! (replace=false)(*) fails if ANY target object already exists;
        // imprint_replace! (replace=true)(*) records each clashing object to drop.
        // This pass only *reads* the catalog: a strict clash returns here with
        // the target byte-for-byte untouched, so the strict path's atomicity is
        // by-construction, not by-rollback (pinned: companion_linear--67).
        //
        // Both sqlite_master AND sqlite_temp_master are consulted: a temp object
        // is connection-local and sqlite_master never lists it, so it would
        // otherwise bypass both the strict-clash refusal and the replace-mode
        // drop. sqlite_temp_master is unqualified (temp
        // objects are never in an attached schema; temp+mounted is refused at
        // prepare above), so it is queried as-is regardless of the target alias.
        // The SQL shape is pinned by
        // system::imprint_helper_tests::clash_probe_sees_temp_object; it is not
        // reachable through a ball because a temp-extent imprint is itself always
        // refused (every real target carries an alias), so no imprint ever
        // creates a temp object to clash with — the guard is defensive.
        let mut to_drop: Vec<(String, String)> = Vec::new(); // (name, type)
        {
            let master = match target_schema_alias.as_deref() {
                Some(a) => format!("{}.sqlite_master", quote_ident(a)),
                None => "sqlite_master".to_string(),
            };
            let mut clashes: Vec<String> = Vec::new();
            for entity in &prepared {
                let name_lit = entity.name.replace('\'', "''");
                let sql = imprint_clash_probe_sql(&master, &name_lit);
                let existing_type = target_conn_guard
                    .query_all_rows(&sql, &[])
                    .ok()
                    .and_then(|(_c, rows)| rows.first().and_then(|r| r.first()?.as_wire_text()));
                if let Some(ty) = existing_type {
                    if replace {
                        to_drop.push((entity.name.clone(), ty));
                    } else {
                        clashes.push(entity.name.clone());
                    }
                }
            }
            if !clashes.is_empty() {
                return Err(DelightQLError::database_error(
                    format!(
                        "imprint!() target object(s) already exist in '{}': {} — \
                         use imprint_replace!() to overwrite",
                        target_ns,
                        clashes.join(", ")
                    ),
                    "Imprint target clash",
                ));
            }
        }

        // --- Phase 2a: target transaction — every destructive/constructive
        // statement on the target connection (replace-mode drops + CREATEs +
        // CTAS INSERTs) commits or rolls back as a unit. A partial failure (a
        // later CREATE/CTAS erroring at exec time) rolls the whole thing back,
        // so replace-mode can never destroy the old tables and leave nothing in
        // their place (pinned: cli tests/imprint_atomicity.rs).
        //
        // `defer_foreign_keys = ON` set *inside* the txn makes drop/create
        // ordering FK-agnostic without touching the persistent `foreign_keys`
        // flag; SQLite auto-resets it at COMMIT/ROLLBACK, so no error path can
        // leak it OFF. At COMMIT the recreated CTAS tables carry no
        // FK constraints of their own, so there are no deferred constraints for
        // *this* txn to violate; any orphaned child of a replaced parent is
        // surfaced by the post-commit foreign_key_check below, not silently
        // accepted.
        target_conn_guard
            .execute("BEGIN IMMEDIATE", &[])
            .map_err(|e| {
                DelightQLError::database_error(
                    "imprint: failed to open target transaction",
                    e.to_string(),
                )
            })?;
        let mut target_txn = TargetTxnGuard {
            conn: &*target_conn_guard,
            committed: false,
        };
        let _ = target_conn_guard.execute("PRAGMA defer_foreign_keys = ON", &[]);

        // Replace-mode: drop the clashing objects (rolled back on any later error).
        for (name, ty) in &to_drop {
            let qualified = match target_schema_alias.as_deref() {
                Some(a) => format!("{}.{}", quote_ident(a), quote_ident(name)),
                None => quote_ident(name),
            };
            let kw = if ty == "view" { "VIEW" } else { "TABLE" };
            target_conn_guard
                .execute(&format!("DROP {} IF EXISTS {}", kw, qualified), &[])
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("imprint_replace!() failed to drop existing '{}'", name),
                        e.to_string(),
                    )
                })?;
        }

        // Create + populate each entity. A failure here (`?`) drops `target_txn`,
        // which ROLLs BACK the drops above — the old tables survive intact.
        for entity in &prepared {
            let entity_name = &entity.name;
            target_conn_guard
                .execute(&entity.qualified_create, &[])
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to execute CREATE TABLE for '{}': {}",
                            entity_name, entity.qualified_create,
                        ),
                        e.to_string(),
                    )
                })?;
            if let Materialized::DeclaredTable {
                insert: Some(insert),
                ..
            } = &entity.materialized
            {
                target_conn_guard.execute(insert, &[]).map_err(|e| {
                    DelightQLError::database_error(
                        format!(
                            "Failed to execute CTAS INSERT for '{}': {}",
                            entity_name, insert,
                        ),
                        e.to_string(),
                    )
                })?;
            }
        }

        target_conn_guard.execute("COMMIT", &[]).map_err(|e| {
            DelightQLError::database_error(
                "imprint: failed to commit target transaction",
                e.to_string(),
            )
        })?;
        target_txn.committed = true;

        // Post-commit FK audit (replace mode only). Recreated CTAS tables carry
        // none of the replaced tables' constraints; a child row that referenced
        // an old row now dangling is a silent orphan. We do NOT fail the imprint
        // (the data is committed) — we make it loud. NOTE: this
        // warning itself is not yet test-pinned (needs an external-FK-child
        // fixture).
        if !to_drop.is_empty() {
            let fk_check_sql = match target_schema_alias.as_deref() {
                Some(a) => format!("PRAGMA {}.foreign_key_check", quote_ident(a)),
                None => "PRAGMA foreign_key_check".to_string(),
            };
            if let Ok((_c, rows)) = target_conn_guard.query_all_rows(&fk_check_sql, &[]) {
                if !rows.is_empty() {
                    let mut by_table: std::collections::BTreeMap<String, usize> =
                        std::collections::BTreeMap::new();
                    for r in &rows {
                        if let Some(t) = r.first().and_then(|v| v.as_wire_text()) {
                            *by_table.entry(t).or_insert(0) += 1;
                        }
                    }
                    for (table, n) in by_table {
                        log::warn!(
                            "imprint_replace!() into '{}': {} orphaned foreign-key row(s) in \
                             table '{}' after replace — recreated tables carry no constraints; \
                             the rows were accepted, not rejected",
                            target_ns,
                            n,
                            table
                        );
                    }
                }
            }
        }

        // --- Phase 2b: bootstrap catalog — ordered AFTER the target commit, in
        // its own transaction on the (separate) bootstrap connection. The target
        // is already durable; if cataloging fails we roll the catalog back and
        // report loudly that the target WAS materialized (re-running the same
        // imprint is idempotent). Two connections cannot share one txn, so
        // "materialized-but-not-cataloged" is the worst residual window, never
        // "data destroyed".
        bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
            DelightQLError::database_error(
                "imprint: failed to begin catalog transaction",
                e.to_string(),
            )
        })?;

        let catalog_result = (|| -> Result<(Vec<(String, String, String)>, String)> {
            // Deregister stale bootstrap entities for replaced names, else
            // re-materializing duplicates their columns (id/id_2/…).
            for (name, _ty) in &to_drop {
                let stale_ids: Vec<i32> = {
                    let mut stmt = bootstrap_conn
                        .prepare(
                            "SELECT e.id FROM entity e
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE ae.namespace_id = ?1 AND e.name = ?2",
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "prepare stale entity lookup",
                                e.to_string(),
                            )
                        })?;
                    let rows = stmt
                        .query_map(rusqlite::params![target_ns_id, name], |r| r.get(0))
                        .map_err(|e| {
                            DelightQLError::database_error("query stale entity", e.to_string())
                        })?;
                    rows.filter_map(|r| r.ok()).collect()
                };
                for eid in stale_ids {
                    let _ = bootstrap_conn
                        .execute("DELETE FROM activated_entity WHERE entity_id = ?1", [eid]);
                    let _ = bootstrap_conn
                        .execute("DELETE FROM entity_attribute WHERE entity_id = ?1", [eid]);
                    let _ = bootstrap_conn
                        .execute("DELETE FROM entity_clause WHERE entity_id = ?1", [eid]);
                    let _ = bootstrap_conn.execute("DELETE FROM entity WHERE id = ?1", [eid]);
                }
            }

            // Create a cartridge for the imprinted entities.
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                    rusqlite::params![
                        3, // SQLite language ID
                        SourceType::Db.as_i32(),
                        &format!("imprint://{}->{}", source_ns, target_ns),
                        target_schema_alias,
                        connection_id,
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to create imprint cartridge",
                        e.to_string(),
                    )
                })?;
            let imprint_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

            let mut results: Vec<(String, String, String)> = Vec::new();
            for entity in &prepared {
                let entity_name = &entity.name;

                // Register the new entity in the target namespace. The
                // imprinted objects are engine tables/views on the target
                // database — served rows, never authored families.
                let entity_type = match entity.materialized {
                    Materialized::View => EntityType::DbPermanentView.as_i32(),
                    Materialized::CtasTable | Materialized::DeclaredTable { .. } => {
                        EntityType::DbPermanentTable.as_i32()
                    }
                };
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![
                            entity_name,
                            entity_type,
                            imprint_cartridge_id,
                            format!("Imprinted from {}", source_ns),
                        ],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to register imprinted entity '{}'", entity_name),
                            e.to_string(),
                        )
                    })?;
                let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

                // Attribute (name, type) pairs. DeclaredTable knows its columns
                // from the manifest schema; View and CtasTable let the engine
                // choose the columns, so read them back from the now-committed
                // object (PRAGMA table_info works on both tables and views).
                let attr_cols: Vec<(String, String)> = match &entity.materialized {
                    Materialized::DeclaredTable { schema, .. } => schema
                        .iter()
                        .map(|sr| (sr.name.clone(), sr.col_type.clone()))
                        .collect(),
                    Materialized::View | Materialized::CtasTable => {
                        let pragma = match target_schema_alias.as_deref() {
                            Some(schema_name) => format!(
                                "PRAGMA {}.table_info({})",
                                quote_ident(schema_name),
                                quote_ident(entity_name)
                            ),
                            None => format!("PRAGMA table_info({})", quote_ident(entity_name)),
                        };
                        let (_cols, rows) = target_conn_guard
                            .query_all_rows(&pragma, &[])
                            .map_err(|e| {
                                DelightQLError::database_error(
                                    format!(
                                        "Failed to read back schema for materialized entity '{}'",
                                        entity_name
                                    ),
                                    e.to_string(),
                                )
                            })?;
                        // table_info columns: cid(0), name(1), type(2), …
                        rows.iter()
                            .filter_map(|r| {
                                Some((r.get(1)?.as_wire_text()?, r.get(2)?.as_wire_text()?))
                            })
                            .collect()
                    }
                };

                // Register entity attributes
                for (position, (col_name, col_type)) in attr_cols.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, data_type, position, is_nullable, default_value)
                             VALUES (?1, ?2, 'output_column', ?3, ?4, 1, NULL)",
                            rusqlite::params![new_entity_id, col_name, col_type, position as i32 + 1],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                format!("Failed to register attribute '{}' for '{}'", col_name, entity_name),
                                e.to_string(),
                            )
                        })?;
                }

                // Activate entity in target namespace
                bootstrap_conn
                    .execute(
                        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![new_entity_id, target_ns_id, imprint_cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("Failed to activate imprinted entity '{}'", entity_name),
                            e.to_string(),
                        )
                    })?;

                // Populated = the CREATE also loaded rows: a CTAS table, or a
                // DeclaredTable with a trailing INSERT…SELECT. A bare View or an
                // unpopulated DeclaredTable is only "created".
                let status = match &entity.materialized {
                    Materialized::CtasTable
                    | Materialized::DeclaredTable {
                        insert: Some(_), ..
                    } => "created+populated",
                    Materialized::View | Materialized::DeclaredTable { insert: None, .. } => {
                        "created"
                    }
                };
                results.push((
                    entity_name.clone(),
                    status.to_string(),
                    entity.qualified_create.clone(),
                ));
            }

            // Linear imprint: consume the source into a blueprint archive under
            // the target. Moves the source namespace,
            // vacating its path (use-after-imprint = error; path free to
            // re-consult) and leaving the tables as the single source of truth.
            let catalog_id =
                ensure_catalog_initialized(&self.catalog_cartridge_id, &bootstrap_conn)?;
            let sys_meta_ns_id: i32 = bootstrap_conn
                .query_row(
                    "SELECT id FROM namespace WHERE fq_name = 'sys::meta'",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query sys::meta namespace for imprint consume",
                        e.to_string(),
                    )
                })?;
            let blueprint_fq = consume_source_to_blueprint(
                &bootstrap_conn,
                source_ns,
                source_ns_id,
                target_ns,
                target_ns_id,
                sys_meta_ns_id,
                catalog_id,
            )?;

            Ok((results, blueprint_fq))
        })();

        let (results, blueprint_fq) = match catalog_result {
            Ok(v) => {
                bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
                    DelightQLError::database_error(
                        "imprint: failed to commit catalog transaction",
                        e.to_string(),
                    )
                })?;
                v
            }
            Err(e) => {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(DelightQLError::database_error(
                    format!(
                        "imprint: target '{}' WAS materialized, but cataloging it failed: {}. \
                         The data is safe; re-run as imprint_replace!() to finish the catalog \
                         (strict imprint! would now refuse — the materialized tables count as \
                         a clash).",
                        target_ns, e
                    ),
                    "Imprint cataloging failed after materialization",
                ));
            }
        };

        // Release the target-txn guard's borrow before the connection guards are
        // dropped below (COMMIT already ran; this drop is a no-op ROLLBACK-skip).
        drop(target_txn);

        drop(target_conn_guard);
        drop(bootstrap_conn);

        debug!(
            "imprint_namespace: Consumed '{}' → archived at '{}'",
            source_ns, blueprint_fq
        );

        debug!(
            "imprint_namespace: Materialized {} entities from '{}' into '{}'",
            results.len(),
            source_ns,
            target_ns
        );

        Ok(results)
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
            DelightQLError::connection_poison_error(
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
                // The plain-qualifier SHADOW softening: the exact
                // top-level name won; if an enlisted `home::{fq_name}` sits
                // behind it, warn that it is shadowed and needs its full path.
                if home_child_shadows(&conn, &fq_name) {
                    log::warn!(
                        "plain qualifier '{n}' resolved to the top-level namespace \
                         '{n}'; an enlisted scratch child 'home::{n}' is shadowed \
                         behind it — spell 'home::{n}' to reach it",
                        n = fq_name
                    );
                }
                id
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // MIDDLE ACCESS RUNG: the exact fq missed — consult the
                // enlist set for an enlisted namespace whose DIRECT child bears
                // this plain name (home first). Fires ONLY here, on a confirmed
                // miss, so no path that resolves today is affected (rule 1). The
                // expanded fq re-enters via the same blueprint guard.
                let expanded_id = match expand_plain_namespace(&conn, &fq_name)? {
                    Some(expanded) => conn
                        .query_row(
                            "SELECT id FROM namespace WHERE fq_name = ?1",
                            [&expanded],
                            |row| row.get::<_, i64>(0),
                        )
                        .ok()
                        .map(|id| -> Result<i64> {
                            refuse_if_blueprint(&conn, &expanded)?;
                            Ok(id)
                        })
                        .transpose()?,
                    None => None,
                };
                match expanded_id {
                    Some(id) => id,
                    None => {
                        // LAST RUNG — alias! shorthands. Fires only after
                        // both the exact fq and the enlist-set expansion
                        // miss, so a real namespace always beats a
                        // same-named alias. The target re-enters the
                        // blueprint guard under its canonical name. This
                        // is the chokepoint that makes alias!'s success
                        // receipt true for TABLE access, not just for the
                        // entity registry's own alias arm.
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
                                return Err(DelightQLError::database_error_with_source(
                                    "Failed to resolve namespace alias",
                                    e.to_string(),
                                    Box::new(e),
                                ));
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if e.to_string().contains("no such table") {
                    // Bootstrap table doesn't exist - system not initialized
                    return Ok(None);
                }
                return Err(DelightQLError::database_error_with_source(
                    "Failed to query bootstrap.namespace",
                    e.to_string(),
                    Box::new(e),
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
                return Err(DelightQLError::database_error_with_source(
                    "Failed to resolve namespace mount binding",
                    e.to_string(),
                    Box::new(e),
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
            Err(e) => Err(DelightQLError::database_error_with_source(
                "Failed to resolve backend schema and connection from bootstrap",
                e.to_string(),
                Box::new(e),
            )),
        }
    }

    /// Enter a liminal program. If no program is active, this call becomes
    /// the OUTERMOST one (owning the catalog savepoint and external journal)
    /// and
    /// `true` is returned — the caller must `end_liminal_program()` on
    /// every exit path. A nested call
    /// leaves the enclosing boundary in place and returns `false`.
    pub(crate) fn begin_liminal_program(
        &self,
        mark: i64,
        kind: LiminalProgramKind,
    ) -> Result<bool> {
        self.begin_liminal_program_with(&RealLiminalCatalogBoundary, mark, kind)
    }

    fn begin_liminal_program_with<B: LiminalCatalogBoundary>(
        &self,
        boundary: &B,
        mark: i64,
        kind: LiminalProgramKind,
    ) -> Result<bool> {
        if self.active_liminal_program.borrow().is_none() {
            let conn = self.bootstrap_connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire bootstrap lock for liminal program",
                    format!("Connection was poisoned: {e}"),
                )
            })?;
            boundary.begin(&conn)?;
            drop(conn);
            self.active_liminal_program.replace(Some(ProgramContext {
                namespace_mark: mark,
                kind,
                external_effects: Vec::new(),
            }));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Close the OUTERMOST liminal program (see `begin_liminal_program`).
    /// The catalog is one savepoint spanning directive execution through
    /// registration; failure restores pre-existing children as well as rows
    /// created by the program.
    pub(crate) fn end_liminal_program(&self, commit: bool) -> Result<()> {
        self.end_liminal_program_with(&RealLiminalCatalogBoundary, commit)
    }

    fn end_liminal_program_with<B: LiminalCatalogBoundary>(
        &self,
        boundary: &B,
        commit: bool,
    ) -> Result<()> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock to close liminal program",
                format!("Connection was poisoned: {e}"),
            )
        })?;
        let result = boundary.close(
            &conn,
            if commit {
                LiminalClose::Commit
            } else {
                LiminalClose::Rollback
            },
        );
        if result.is_ok() && !commit {
            // Rust-side caches of catalog rows must not outlive a rollback:
            // if this program CREATED the catalog
            // cartridge (a session that touched no catalog feature before
            // consulting), the memoized id now points at erased rows —
            // re-verify and forget it so the next use re-initializes.
            if let Some(id) = self.catalog_cartridge_id.get() {
                let still_there: bool = conn
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM cartridge WHERE id = ?1)",
                        [id],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);
                if !still_there {
                    self.catalog_cartridge_id.set(None);
                }
            }
        }
        drop(conn);
        // A failed close leaves the context, and therefore its compensation
        // journal, owned by the coordinator. Clearing it here would make a
        // failed RELEASE impossible to recover from.
        if result.is_ok() {
            self.active_liminal_program.replace(None);
        }
        result
    }

    /// Record a file materialized by `mount_new!` while a liminal program is
    /// active. The prior state is deliberately explicit: on abort an absent
    /// path is removed, while a caller-owned zero-byte placeholder is restored
    /// to zero bytes rather than deleted.
    fn journal_created_file(&self, path: PathBuf, prior_state: CreatedFilePriorState) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            context
                .external_effects
                .push(ExternalEffect::CreatedFile { path, prior_state });
        }
    }

    fn journal_attached_sqlite(&self, schema_alias: String) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            context
                .external_effects
                .push(ExternalEffect::AttachedSqlite { schema_alias });
        }
    }

    fn unjournal_attached_sqlite(&self, schema_alias: &str) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            if let Some(index) = context.external_effects.iter().rposition(|effect| {
                matches!(
                    effect,
                    ExternalEffect::AttachedSqlite { schema_alias: alias }
                        if alias == schema_alias
                )
            }) {
                context.external_effects.remove(index);
            }
        }
    }

    fn mount_error_after_alias_rollback(
        &mut self,
        schema_alias: &str,
        cleanup: Result<()>,
        primary: DelightQLError,
    ) -> DelightQLError {
        self.unjournal_attached_sqlite(schema_alias);
        match cleanup {
            Ok(()) => primary,
            Err(cleanup_error) => {
                let primary_uri = primary.error_uri();
                let cleanup_uri = cleanup_error.error_uri();
                let message = format!(
                    "{primary}; mount cleanup failed: {cleanup_error} [{cleanup_uri}] [{primary_uri}]"
                );
                self.quarantine_session_with_pending(
                    "mount alias compensation",
                    message.clone(),
                    vec![ExternalEffect::AttachedSqlite {
                        schema_alias: schema_alias.to_string(),
                    }],
                );
                DelightQLError::database_error_categorized(
                    "session_health/external_effect",
                    message,
                    "external effect recovery remains uncertain",
                )
            }
        }
    }

    fn journal_external_connection(&self, connection_id: i64) {
        if let Some(context) = self.active_liminal_program.borrow_mut().as_mut() {
            context
                .external_effects
                .push(ExternalEffect::RegisteredExternalConnection { connection_id });
        }
    }

    /// Reverse non-catalog effects in LIFO order. Every failed inverse is
    /// returned and transferred to session health; a cleanup problem cannot
    /// hide the program's original failure or disappear into `let _ =`.
    pub(crate) fn rollback_liminal_external_effects(&mut self) -> Vec<CompensationFailure> {
        self.compensate_liminal_external_effects_with(&RealLiminalFileOps)
    }

    fn compensate_liminal_external_effects_with<F: LiminalFileOps>(
        &mut self,
        file_ops: &F,
    ) -> Vec<CompensationFailure> {
        let effects = self
            .active_liminal_program
            .borrow_mut()
            .as_mut()
            .map(|context| std::mem::take(&mut context.external_effects))
            .unwrap_or_default();
        let failures = self.reverse_external_effects_with(effects, file_ops);
        if !failures.is_empty() {
            let pending_effects = failures
                .iter()
                .rev()
                .map(|failure| failure.effect.clone())
                .collect();
            let message = failures
                .iter()
                .map(|failure| format!("{} [{}]", failure.error, failure.error.error_uri()))
                .collect::<Vec<_>>()
                .join("; ");
            self.quarantine_session_with_pending(
                "liminal external-effect compensation",
                message,
                pending_effects,
            );
        }
        failures
    }

    /// Apply the inverse for each journal entry in LIFO order. This helper is
    /// shared by the ordinary rollback path and Reset's retry of a quarantined
    /// incident; it reports failures without deciding how session health is
    /// recorded.
    fn reverse_external_effects_with<F: LiminalFileOps>(
        &mut self,
        effects: Vec<ExternalEffect>,
        file_ops: &F,
    ) -> Vec<CompensationFailure> {
        let mut failures = Vec::new();
        for effect in effects.into_iter().rev() {
            let result = match &effect {
                ExternalEffect::AttachedSqlite { schema_alias } => {
                    let result = match self.connection.lock() {
                        Ok(conn) => {
                            let escaped = schema_alias.replace('\'', "''");
                            conn.execute(&format!("DETACH DATABASE '{escaped}'"), &[])
                                .map(|_| ())
                                .map_err(|error| {
                                    DelightQLError::database_error(
                                        format!(
                                            "Failed to detach liminal alias '{}'",
                                            schema_alias
                                        ),
                                        error.to_string(),
                                    )
                                })
                        }
                        Err(error) => Err(DelightQLError::connection_poison_error(
                            "Failed to acquire connection lock for liminal detach",
                            format!("Connection was poisoned: {error}"),
                        )),
                    };
                    result
                }
                ExternalEffect::RegisteredExternalConnection { connection_id } => {
                    self.connection_map.remove(&connection_id);
                    self.schema_map.remove(&connection_id);
                    Ok(())
                }
                ExternalEffect::CreatedFile { path, prior_state } => match prior_state {
                    CreatedFilePriorState::Absent => file_ops.remove_created(path),
                    CreatedFilePriorState::Empty => file_ops.restore_empty(path),
                },
            };
            if let Err(error) = result {
                failures.push(CompensationFailure { effect, error });
            }
        }
        failures
    }

    /// Retry the inverses recorded on a quarantined incident. Reset is the
    /// only recovery boundary: a failed inverse remains pending and prevents
    /// the health latch from clearing.
    fn recover_pending_external_effects(&mut self) -> Result<()> {
        let effects = match &mut self.session_health {
            SessionHealth::Healthy => return Ok(()),
            SessionHealth::Quarantined(incident) => std::mem::take(&mut incident.pending_effects),
        };
        if effects.is_empty() {
            return Ok(());
        }

        let failures = self.reverse_external_effects_with(effects, &RealLiminalFileOps);
        if failures.is_empty() {
            return Ok(());
        }

        let pending_effects = failures
            .iter()
            .rev()
            .map(|failure| failure.effect.clone())
            .collect::<Vec<_>>();
        let message = failures
            .iter()
            .map(|failure| format!("{} [{}]", failure.error, failure.error.error_uri()))
            .collect::<Vec<_>>()
            .join("; ");
        if let SessionHealth::Quarantined(incident) = &mut self.session_health {
            incident.pending_effects = pending_effects;
            incident.message =
                format!("{}; reset compensation failed: {message}", incident.message);
        }
        Err(DelightQLError::database_error_categorized(
            "session_health/external_effect",
            format!("reset could not complete pending external-effect compensation: {message}"),
            "external-effect recovery remains uncertain",
        ))
    }

    /// The active liminal program's (mark, kind), if any.
    pub(crate) fn active_liminal_program(&self) -> Option<(i64, LiminalProgramKind)> {
        self.active_liminal_program
            .borrow()
            .as_ref()
            .map(|context| (context.namespace_mark, context.kind))
    }

    /// Refuse new work once recovery has become uncertain. Close and reset
    /// remain legal protocol operations; the relay owns that distinction.
    pub(crate) fn require_healthy(&self) -> Result<()> {
        match &self.session_health {
            SessionHealth::Healthy => Ok(()),
            SessionHealth::Quarantined(incident) => Err(
                DelightQLError::database_error_categorized(
                    "session_health/external_effect",
                    format!(
                        "the session is quarantined after {}: {} — reset or reconnect before issuing another query",
                        incident.operation, incident.message
                    ),
                    "external effect recovery is uncertain",
                ),
            ),
        }
    }

    /// Record the first uncertain recovery incident. Keeping the original
    /// incident avoids replacing a useful primary failure with a later one.
    pub(crate) fn quarantine_session(
        &mut self,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.quarantine_session_with_pending(operation, message, Vec::new());
    }

    pub(crate) fn quarantine_session_with_pending(
        &mut self,
        operation: impl Into<String>,
        message: impl Into<String>,
        pending_effects: Vec<ExternalEffect>,
    ) {
        if matches!(self.session_health, SessionHealth::Healthy) {
            self.session_health = SessionHealth::Quarantined(HealthIncident {
                operation: operation.into(),
                message: message.into(),
                pending_effects,
            });
        }
    }

    /// The quarantine incident, if one is latched: (operation, message).
    /// The typed answer behind `api::DqlHandle::session_health` — hosts read
    /// this, never error text.
    pub(crate) fn health_incident(&self) -> Option<(&str, &str)> {
        match &self.session_health {
            SessionHealth::Healthy => None,
            SessionHealth::Quarantined(incident) => {
                Some((incident.operation.as_str(), incident.message.as_str()))
            }
        }
    }

    /// The namespace row id for an fq name, if it exists.
    pub(crate) fn namespace_id(&self, fq: &str) -> Result<Option<i64>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for namespace id",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        conn.query_row("SELECT id FROM namespace WHERE fq_name = ?1", [fq], |r| {
            r.get(0)
        })
        .optional()
        .map_err(|e| DelightQLError::database_error("namespace id", e.to_string()))
    }

    /// Policy refusal for session-rearranging operations against namespaces
    /// that predate a consulted program. The catalog savepoint makes such an
    /// operation technically reversible; the refusal stands because a file
    /// describes its library rather than imperatively rearranging its caller's
    /// session. It lives on the shared road so every invocation route receives
    /// the policy inductively. The badge spells `uncompensable`; the reason it
    /// publishes is that policy, not an inability to undo.
    pub(crate) fn refuse_preexisting_namespace_mutation_in_program(
        &self,
        target_fq: &str,
        verb: &str,
        badge: &'static str,
    ) -> Result<()> {
        let Some((mark, _kind)) = self.active_liminal_program() else {
            return Ok(());
        };
        if let Some(id) = self.namespace_id(target_fq)? {
            if id <= mark {
                return Err(DelightQLError::validation_error_categorized(
                    badge,
                    format!(
                        "a consulted file executes as ONE atomic program: if any part \
                         fails, everything the program created is torn down. \
                         '{target_fq}' existed BEFORE this program began, so {verb} it \
                         here could not be undone by that teardown — the program \
                         refuses rather than risk leaving the session half-changed. \
                         Do it at the prompt, outside the file"
                    ),
                    "uncompensable in liminal program",
                ));
            }
        }
        Ok(())
    }

    pub fn max_namespace_id(&self) -> Result<i64> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for namespace snapshot",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        Ok(conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM namespace", [], |r| {
                r.get(0)
            })
            .unwrap_or(0))
    }

    /// Does a namespace row exist for this fq name?
    pub fn namespace_exists(&self, fq: &str) -> Result<bool> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for namespace existence",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        Ok(conn
            .query_row("SELECT 1 FROM namespace WHERE fq_name = ?1", [fq], |_| {
                Ok(())
            })
            .optional()
            .map_err(|e| DelightQLError::database_error("namespace existence", e.to_string()))?
            .is_some())
    }

    pub fn resolve_unqualified_entity(
        &self,
        entity_name: &str,
        current_namespace: &str,
        fallback_namespace: Option<&str>,
    ) -> Result<
        Option<(
            delightql_types::namespace::NamespacePath,
            delightql_types::SqlIdentifier,
        )>,
    > {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap connection lock",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Step 1: Get namespace_id for current namespace
        let current_ns_id: i64 = match conn.query_row(
            "SELECT id FROM namespace WHERE fq_name = ?1",
            [current_namespace],
            |row| row.get(0),
        ) {
            Ok(id) => id,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // Current namespace doesn't exist
                return Ok(None);
            }
            Err(e) => {
                return Err(DelightQLError::database_error_with_source(
                    "Failed to query current namespace",
                    e.to_string(),
                    Box::new(e),
                ));
            }
        };

        // Step 2 & 3: Find entity in current namespace OR enlisted namespaces.
        // Collect ALL matches across namespaces to detect ambiguity.
        let query = "
            WITH RECURSIVE
            direct(ns_id) AS (
                SELECT ?2 AS ns_id
                UNION
                SELECT en.from_namespace_id
                FROM enlisted_namespace en
                WHERE en.to_namespace_id = ?2
                UNION
                SELECT nle.enlisted_namespace_id
                FROM namespace_local_enlist nle
                WHERE nle.namespace_id = ?2
            ),
            reachable(ns_id) AS (
                SELECT ns_id FROM direct
                UNION
                SELECT exp.exposed_namespace_id
                FROM exposed_namespace exp
                JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
            )
            SELECT DISTINCT n.fq_name, e.name, et.is_fn
            FROM activated_entity ae
            JOIN entity e ON ae.entity_id = e.id
            JOIN entity_type_enum et ON et.id = e.type
            JOIN namespace n ON ae.namespace_id = n.id
            JOIN reachable r ON r.ns_id = ae.namespace_id
            WHERE e.name = ?1 COLLATE NOCASE
        ";

        let mut stmt = conn.prepare(query).map_err(|e| {
            DelightQLError::database_error(
                "Failed to prepare unqualified entity resolution",
                e.to_string(),
            )
        })?;

        let matches: Vec<(String, String, bool)> = stmt
            .query_map(rusqlite::params![entity_name, current_ns_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            })
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to resolve unqualified entity",
                    e.to_string(),
                )
            })?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);

        // When a fallback namespace is provided (DDL view body resolution),
        // also search the fallback scope and merge results. Ambiguity across
        // both scopes is still an error (e.g., DDL-enlisted `items` overlapping
        // with main's `items`).
        let mut all_matches = matches;
        if let Some(fallback_ns) = fallback_namespace {
            let fallback_ns_id: i64 = match conn.query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [fallback_ns],
                |row| row.get(0),
            ) {
                Ok(id) => id,
                Err(_) => {
                    return Self::resolve_matches(all_matches, entity_name, current_namespace)
                }
            };

            let mut fallback_stmt = conn
                .prepare(
                    "WITH RECURSIVE
                     direct(ns_id) AS (
                         SELECT ?2 AS ns_id
                         UNION
                         SELECT en.from_namespace_id
                         FROM enlisted_namespace en
                         WHERE en.to_namespace_id = ?2
                     ),
                     reachable(ns_id) AS (
                         SELECT ns_id FROM direct
                         UNION
                         SELECT exp.exposed_namespace_id
                         FROM exposed_namespace exp
                         JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
                     )
                     SELECT DISTINCT n.fq_name, e.name, et.is_fn
                     FROM activated_entity ae
                     JOIN entity e ON ae.entity_id = e.id
                     JOIN entity_type_enum et ON et.id = e.type
                     JOIN namespace n ON ae.namespace_id = n.id
                     JOIN reachable r ON r.ns_id = ae.namespace_id
                     WHERE e.name = ?1 COLLATE NOCASE",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to prepare fallback entity resolution",
                        e.to_string(),
                    )
                })?;

            let fallback_matches: Vec<(String, String, bool)> = fallback_stmt
                .query_map(rusqlite::params![entity_name, fallback_ns_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                    ))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to resolve entity in fallback namespace",
                        e.to_string(),
                    )
                })?
                .filter_map(|r| r.ok())
                .collect();

            // Merge, dedup by fq_name
            for m in fallback_matches {
                if !all_matches.iter().any(|(ns, _, _)| *ns == m.0) {
                    all_matches.push(m);
                }
            }
        }

        Self::resolve_matches(all_matches, entity_name, current_namespace)
    }

    /// Helper: interpret a set of entity matches — 0 = not found, 1 = found, 2+ = ambiguous.
    fn resolve_matches(
        matches: Vec<(String, String, bool)>,
        entity_name: &str,
        scope_namespace: &str,
    ) -> Result<
        Option<(
            delightql_types::namespace::NamespacePath,
            delightql_types::SqlIdentifier,
        )>,
    > {
        // POSITION CAPABILITY: this discovery serves RELATION position, and
        // a pure value callable cannot stand there at all — so a function
        // candidate never perturbs relation selection. It is kept only when
        // nothing relation-capable answers, so the kind teaching ("is a
        // function, not a relation") still reaches the author.
        let capable: Vec<&(String, String, bool)> =
            matches.iter().filter(|(_, _, is_fn)| !is_fn).collect();
        let matches: Vec<(String, String, bool)> = if capable.is_empty() {
            matches
        } else {
            capable.into_iter().cloned().collect()
        };
        match matches.len() {
            0 => Ok(None),
            1 => {
                let (fq_name, canonical_name, _) = &matches[0];
                let namespace_path =
                    delightql_types::namespace::NamespacePath::from_fq_string(fq_name);
                Ok(Some((
                    namespace_path,
                    delightql_types::SqlIdentifier::new(canonical_name),
                )))
            }
            _ => {
                // Multiple matches from different namespaces — ambiguous.
                let namespaces: Vec<&str> = matches.iter().map(|(ns, _, _)| ns.as_str()).collect();
                let enlisted_ns = namespaces
                    .iter()
                    .find(|ns| **ns != scope_namespace)
                    .unwrap_or(namespaces.last().unwrap_or(&"ns"));
                Err(DelightQLError::validation_error(
                    format!(
                        "Ambiguous entity '{}': found in namespaces {}. \
                         enlist!() brought overlapping names into scope. \
                         Fix: use qualified access ({}.{}(*)), \
                         or delist!(\"{}\") to remove the namespace.",
                        entity_name,
                        namespaces.join(", "),
                        enlisted_ns,
                        entity_name,
                        enlisted_ns,
                    ),
                    "Ambiguous unqualified entity resolution",
                ))
            }
        }
    }

    /// Register an object a run's DDL directive created
    /// (`temp_table!`/`table!`/`temp_view!` — "catalog-registered … its
    /// name resolves like any table's") so
    /// post-run statements resolve it bare. Called by the run's entry
    /// point (relay/entry.rs) after a successful run; pinned by the
    /// effects ball's ddl_receipt--12/--13/--14 and util--36 post-state
    /// reads.
    ///
    /// The recipe is `imprint_namespace`'s registration tail: read the
    /// engine-typed columns back via CONNECTION-APPROPRIATE introspection
    /// SQL (`created_object_readback_sql` — PRAGMA table_info on
    /// SQLite/DuckDB, information_schema on postgres: PRAGMA on a PG
    /// connection silently registers
    /// nothing), retire the run's own stale registration (fresh scratch
    /// per run), then write entity + output_column attributes +
    /// activation, on a per-connection `session://materialized` cartridge
    /// with NO schema alias — the generator then spells reads unqualified,
    /// which is exactly how SQLite finds temp-schema objects.
    ///
    /// The retirement is SCOPED to the session cartridge: a same-name
    /// mount-introspected physical entity keeps
    /// its registration — the temp SHADOWS it for unqualified resolution
    /// (a preference, not a catalog edit), and a
    /// qualified read still reaches it. Pinned by session_shadow_tests::
    /// {physical_registration_survives_temp_registration,
    /// reregistration_retires_prior_session_entry_only} and the effects
    /// ball's scratch--52_qualified_read_reaches_physical.
    ///
    /// Returns `NotPresent` when an independent existence probe proves the
    /// object does not exist (an exit-flagged run skipped its CREATE).
    /// A missing catalog namespace is an internal registration failure, not
    /// evidence that the target object was absent. Full nominal
    /// placement (`<conn-ns>::temp` mirror + scoped enlistment edges) is
    /// later work; this is the session-catalog slice the
    /// entry points need.
    /// The RESOLVED owner of an
    /// effect target — identity, not spelling. A spelled qualifier
    /// resolves through the exact fq, then the global alias table; a bare
    /// target resolves through the scope's enlisted reachability. The
    /// answer is the owning namespace's fq and CATALOG KIND, so the
    /// engine-ownership refusal covers aliases, enlistment, and future
    /// indirections automatically. `None` = the target does not resolve
    /// here; later resolution speaks with its own diagnostic.
    pub fn effect_target_owner(
        &self,
        target: &str,
        spelled_namespace: Option<&str>,
        scope_namespace: &str,
    ) -> Result<Option<(String, String)>> {
        use rusqlite::OptionalExtension;
        let db = |what: &str, e: rusqlite::Error| {
            DelightQLError::database_error(format!("{what}: {e}"), "effect target ownership")
        };
        let fq: Option<String> = match spelled_namespace {
            Some(ns) => {
                let conn = self.get_bootstrap_connection();
                let guard = conn.lock().map_err(|e| {
                    DelightQLError::connection_poison_error(
                        "Failed to acquire bootstrap lock for target ownership",
                        format!("Connection was poisoned: {}", e),
                    )
                })?;
                let exact: Option<String> = guard
                    .query_row(
                        "SELECT fq_name FROM namespace WHERE fq_name = ?1",
                        [ns],
                        |r| r.get(0),
                    )
                    .optional()
                    .map_err(|e| db("resolving spelled namespace", e))?;
                match exact {
                    Some(f) => Some(f),
                    None => guard
                        .query_row(
                            "SELECT n.fq_name FROM namespace_alias a \
                             JOIN namespace n ON n.id = a.target_namespace_id \
                             WHERE a.alias = ?1",
                            [ns],
                            |r| r.get(0),
                        )
                        .optional()
                        .map_err(|e| db("resolving namespace alias", e))?,
                }
            }
            // Bare target: ambiguity or lookup errors are not ownership's
            // business — later resolution speaks.
            None => self
                .resolve_unqualified_entity(target, scope_namespace, None)
                .unwrap_or(None)
                .map(|(path, _)| {
                    path.iter()
                        .map(|seg| seg.name.to_string())
                        .collect::<Vec<_>>()
                        .join("::")
                }),
        };
        let Some(fq) = fq else {
            return Ok(None);
        };
        let conn = self.get_bootstrap_connection();
        let guard = conn.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for target ownership",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let kind: Option<String> = guard
            .query_row(
                "SELECT kind FROM namespace WHERE fq_name = ?1",
                [&fq],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| db("reading namespace kind", e))?;
        Ok(kind.map(|k| (fq, k)))
    }

    /// Materialize the typed effect plan's OBSERVATIONAL PROJECTION
    /// into the engine-owned sys::execution relations (effect_plan /
    /// effect_guard / effect_requirement — a normalized shape).
    /// Clear-then-insert is the lifecycle: rows persist after a run
    /// for post-mortem inspection and clear at the START of the next
    /// compile (the fresh-scratch-per-run precedent). Only the engine
    /// calls this; the rows execute nothing (the typed Rust plan
    /// stays the single executable source).
    pub fn materialize_effect_plan(
        &self,
        typed: &crate::pipeline::compiled_query::TypedEffectPlan,
    ) -> Result<()> {
        use crate::pipeline::compiled_query::GuardPolarity;
        let conn = self.get_bootstrap_connection();
        let guard = conn.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for effect-plan materialization",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let step = |msg: &str, e: rusqlite::Error| {
            DelightQLError::database_error(format!("{msg}: {e}"), "effect-plan materialization")
        };
        // Atomic clear-and-replace: a mid-
        // materialization failure must not leave a partial "canonical"
        // projection behind.
        guard
            .execute_batch(
                "BEGIN; \
                 DELETE FROM effect_run; \
                 DELETE FROM effect_requirement; \
                 DELETE FROM effect_guard; \
                 DELETE FROM effect_plan;",
            )
            .map_err(|e| step("clearing the prior plan", e))?;
        let finish = |guard: &std::sync::MutexGuard<'_, Connection>, r: Result<()>| match r {
            Ok(()) => guard
                .execute_batch("COMMIT")
                .map_err(|e| step("committing", e)),
            Err(e) => {
                let _ = guard.execute_batch("ROLLBACK");
                Err(e)
            }
        };
        let body = (|| -> Result<()> {
            for (ordinal, s) in typed.steps.iter().enumerate() {
                let (step_kind, action_kind) = s.kind().projection_kinds();
                guard
                    .execute(
                        "INSERT INTO effect_plan (plan_id, step_id, ordinal, occurrence_id, \
                     step_kind, action_kind, operation, route, sql_display) \
                     VALUES (1, ?1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        rusqlite::params![
                            ordinal as i64,
                            s.occurrence,
                            step_kind,
                            action_kind,
                            s.operation.clone(),
                            s.route,
                            s.sql_display(),
                        ],
                    )
                    .map_err(|e| step("inserting a step", e))?;
                for r in &s.requirements {
                    guard
                        .execute(
                            "INSERT INTO effect_requirement (plan_id, step_id, guard_id, \
                         polarity, reason) VALUES (1, ?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                ordinal as i64,
                                r.guard_id as i64,
                                match r.polarity {
                                    GuardPolarity::Present => "present",
                                    GuardPolarity::Absent => "absent",
                                },
                                r.reason,
                            ],
                        )
                        .map_err(|e| step("inserting a requirement edge", e))?;
                }
            }
            for g in &typed.guards {
                guard
                    .execute(
                        "INSERT INTO effect_guard (plan_id, guard_id, sql_display) \
                     VALUES (1, ?1, ?2)",
                        rusqlite::params![g.guard_id as i64, g.sql],
                    )
                    .map_err(|e| step("inserting a guard definition", e))?;
            }
            Ok(())
        })();
        finish(&guard, body)
    }

    /// Materialize the run's per-step outcomes:
    /// tracked in memory during the walk, written ONCE at the run's
    /// boundary, persisting for post-mortem inspection until the next
    /// compile clears them with the plan. The caller treats failure as
    /// best-effort — bookkeeping never outranks the run.
    pub fn materialize_effect_run(
        &self,
        outcomes: &[(&'static str, Option<String>)],
    ) -> Result<()> {
        let conn = self.get_bootstrap_connection();
        let guard = conn.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for effect-run materialization",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        // Atomic: a bookkeeping failure must not leave
        // a PARTIAL post-mortem behind — same boundary discipline as the
        // plan materializer.
        guard.execute_batch("BEGIN").map_err(|e| {
            DelightQLError::database_error(
                format!("beginning the run-outcome batch: {e}"),
                "effect-run materialization",
            )
        })?;
        let body = (|| -> Result<()> {
            for (step_id, (status, detail)) in outcomes.iter().enumerate() {
                guard
                    .execute(
                        "INSERT OR REPLACE INTO effect_run (plan_id, step_id, status, detail) \
                         VALUES (1, ?1, ?2, ?3)",
                        rusqlite::params![step_id as i64, status, detail],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            format!("inserting a run outcome: {e}"),
                            "effect-run materialization",
                        )
                    })?;
            }
            Ok(())
        })();
        match body {
            Ok(()) => guard.execute_batch("COMMIT").map_err(|e| {
                DelightQLError::database_error(
                    format!("committing the run-outcome batch: {e}"),
                    "effect-run materialization",
                )
            }),
            Err(e) => {
                let _ = guard.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Append one assertion verdict to the session ledger. The ledger lives
    /// on bootstrap rather than the run's target connection, so a failing
    /// assertion remains observable after its target transaction rolls back.
    pub fn record_assertion_verdict(
        &self,
        verdict: &crate::pipeline::verdict::Verdict,
        run_id: &str,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let conn = self.bootstrap_connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire bootstrap lock for assertion recording",
                    format!("Connection was poisoned: {e}"),
                )
            })?;
            conn.execute(
                "INSERT INTO assertions \
                 (name, source_file, source_line, body, outcome, detail, run_id) \
                 VALUES (?1, NULL, NULL, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    verdict.identity.name,
                    verdict.identity.body_text,
                    match verdict.outcome {
                        crate::pipeline::verdict::VerdictOutcome::Pass => "pass",
                        crate::pipeline::verdict::VerdictOutcome::Fail => "fail",
                    },
                    verdict.detail,
                    run_id,
                ],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("recording an assertion verdict: {e}"),
                    "assertion verdict materialization",
                )
            })?;
        }
        Ok(())
    }

    fn readback_created_object(
        &self,
        name: &str,
        connection_id: i64,
    ) -> Result<CreatedObjectReadback> {
        let dialect = self.dialect_for_connection(Some(connection_id));
        let sqlite_schema = if matches!(dialect, crate::pipeline::generator::SqlDialect::SQLite) {
            let namespace_fq = self
                .connection_namespace_fq(connection_id)?
                .unwrap_or_else(|| "main".to_string());
            self.physical_schema_alias_for_namespace(&namespace_fq, connection_id)?
        } else {
            None
        };
        let mounted_schema =
            if matches!(dialect, crate::pipeline::generator::SqlDialect::PostgreSQL) {
                self.mounted_engine_schema_for_connection(connection_id)?
            } else {
                None
            };
        let Some(existence_sql) = created_object_existence_sql_scoped(
            dialect,
            name,
            mounted_schema.as_deref(),
            sqlite_schema.as_deref(),
        ) else {
            return Ok(CreatedObjectReadback {
                existence: ObjectExistence::Unsupported {
                    reason: format!(
                        "created-object existence is not implemented for {}",
                        dialect.family_name()
                    ),
                },
                attributes: Vec::new(),
            });
        };
        let conn_arc = if connection_id == PRIMARY_CONNECTION_ID {
            self.connection.clone()
        } else {
            self.get_connection(connection_id)?
        };
        let guard = conn_arc.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire connection lock for created-object read-back",
                format!("Connection was poisoned: {e}"),
            )
        })?;
        let (_existence_columns, existence_rows) =
            guard.query_all_rows(&existence_sql, &[]).map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to probe created object '{name}' existence"),
                    e.to_string(),
                )
            })?;
        if existence_rows.is_empty() {
            return Ok(CreatedObjectReadback {
                existence: ObjectExistence::Absent,
                attributes: Vec::new(),
            });
        }
        if existence_rows.iter().any(|row| row.is_empty()) {
            return Err(DelightQLError::validation_error(
                format!("created-object existence probe for '{name}' returned a malformed row"),
                "target metadata response is malformed",
            ));
        }

        // SQLite can expose a durable object through an attached schema and a
        // temp object of the same name simultaneously. Temp is the visible
        // object for an unqualified read, so its metadata wins when the probe
        // reports both rows; otherwise use the recovered attached schema.
        let metadata_sqlite_schema =
            if matches!(dialect, crate::pipeline::generator::SqlDialect::SQLite)
                && existence_rows
                    .iter()
                    .any(|row| row.get(1).and_then(|v| v.as_text()) == Some("temp"))
            {
                None
            } else {
                sqlite_schema.as_deref()
            };
        let Some((readback_sql, name_col, type_col)) = created_object_readback_sql_scoped(
            dialect,
            name,
            mounted_schema.as_deref(),
            metadata_sqlite_schema,
        ) else {
            return Ok(CreatedObjectReadback {
                existence: ObjectExistence::Unsupported {
                    reason: format!(
                        "created-object metadata is not available for {}",
                        dialect.family_name()
                    ),
                },
                attributes: Vec::new(),
            });
        };
        let (metadata_columns, metadata_rows) =
            guard.query_all_rows(&readback_sql, &[]).map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to read created object '{name}' metadata"),
                    e.to_string(),
                )
            })?;
        let required_columns = name_col.max(type_col) + 1;
        if metadata_columns.len() < required_columns {
            return Err(DelightQLError::validation_error(
                format!(
                    "created-object metadata for '{name}' has {} columns; expected at least {required_columns}",
                    metadata_columns.len()
                ),
                "target metadata response is malformed",
            ));
        }
        let attributes = metadata_rows
            .into_iter()
            .map(|row| {
                if row.len() < required_columns {
                    return Err(DelightQLError::validation_error(
                        format!("created-object metadata row for '{name}' is truncated"),
                        "target metadata response is malformed",
                    ));
                }
                Ok((
                    row[name_col].as_wire_text().unwrap_or_default(),
                    row[type_col].as_wire_text().unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(CreatedObjectReadback {
            existence: ObjectExistence::Present,
            attributes,
        })
    }

    pub(crate) fn register_run_created_objects_with<C: CreatedObjectCatalog>(
        &mut self,
        objects: &[crate::pipeline::compiled_query::PlanCreatedObject],
        catalog: &C,
    ) -> Result<Vec<RegistrationOutcome>> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let mut outcomes = Vec::with_capacity(objects.len());
        let mut registrations = Vec::new();
        let mut unsupported_reason = None;
        for object in objects {
            let readback = self.readback_created_object(
                &object.name,
                object.connection_id.unwrap_or(PRIMARY_CONNECTION_ID),
            )?;
            match readback.existence {
                ObjectExistence::Absent => outcomes.push(RegistrationOutcome::NotPresent),
                ObjectExistence::Unsupported { reason } => {
                    unsupported_reason.get_or_insert(reason.clone());
                    outcomes.push(RegistrationOutcome::Unsupported { reason });
                }
                ObjectExistence::Present => {
                    let connection_id = object.connection_id.unwrap_or(PRIMARY_CONNECTION_ID);
                    let namespace_fq = self
                        .connection_namespace_fq(connection_id)?
                        .unwrap_or_else(|| "main".to_string());
                    let bootstrap = self.bootstrap_connection.lock().map_err(|e| {
                        DelightQLError::connection_poison_error(
                            "Failed to acquire bootstrap lock for created-object namespace",
                            format!("Connection was poisoned: {e}"),
                        )
                    })?;
                    let namespace_id = bootstrap
                        .query_row(
                            "SELECT id FROM namespace WHERE fq_name = ?1",
                            [namespace_fq.as_str()],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "query created-object namespace",
                                e.to_string(),
                            )
                        })?;
                    drop(bootstrap);
                    let Some(namespace_id) = namespace_id else {
                        return Err(DelightQLError::validation_error(
                            format!(
                                "created-object target namespace '{namespace_fq}' is not present"
                            ),
                            "created-object catalog namespace is unavailable",
                        ));
                    };
                    registrations.push(CreatedObjectRegistration {
                        name: object.name.clone(),
                        is_view: object.is_view,
                        connection_id,
                        namespace_id,
                        attributes: readback.attributes,
                    });
                    outcomes.push(RegistrationOutcome::Registered);
                }
            }
        }
        if let Some(reason) = unsupported_reason {
            for outcome in &mut outcomes {
                if matches!(outcome, RegistrationOutcome::Registered) {
                    *outcome = RegistrationOutcome::Unsupported {
                        reason: format!("created-object batch was not reconciled: {reason}"),
                    };
                }
            }
            return Ok(outcomes);
        }
        if registrations.is_empty() {
            return Ok(outcomes);
        }

        let bootstrap = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for created-object reconciliation",
                format!("Connection was poisoned: {e}"),
            )
        })?;
        let transaction = CatalogSavepoint::begin(
            &bootstrap,
            "dql_created_object_reconcile",
            "Failed to begin created-object reconciliation",
        )?;
        catalog.reconcile(&bootstrap, &registrations)?;
        transaction.commit("Failed to commit created-object reconciliation")?;
        drop(bootstrap);
        self.session_materialized_names.set(true);
        Ok(outcomes)
    }

    /// The session shadow split: when BOTH a session-materialized entity
    /// (created-object registration's cartridge) and an entity from any
    /// other cartridge (e.g. a mount-introspected physical table) hold
    /// `entity_name` activated in `namespace_fq`, return their entity ids
    /// as `(session, competitor)`. `None` when there is no such collision.
    ///
    /// This is the seam qualified resolution uses to punch through the
    /// temp shadow (the shadow covers UNQUALIFIED
    /// names only). Gated by `session_materialized_names`, so sessions
    /// that never materialized anything answer without touching bootstrap.
    /// Pinned by session_shadow_tests::
    /// qualified_read_reaches_physical_after_same_name_temp.
    pub fn session_shadow_split(
        &self,
        namespace_fq: &str,
        entity_name: &str,
    ) -> Result<Option<(i64, i64)>> {
        if !self.session_materialized_names.get() {
            return Ok(None);
        }
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for shadow-split probe",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let (session_id, competitor_id): (Option<i64>, Option<i64>) = conn
            .query_row(
                "SELECT
                     MAX(CASE WHEN c.source_uri = 'session://materialized'
                              THEN e.id END),
                     MAX(CASE WHEN c.source_uri <> 'session://materialized'
                              THEN e.id END)
                 FROM activated_entity ae
                 JOIN entity e ON e.id = ae.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE e.name = ?1 COLLATE NOCASE AND n.fq_name = ?2",
                rusqlite::params![entity_name, namespace_fq],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|e| DelightQLError::database_error("shadow-split probe", e.to_string()))?;
        Ok(match (session_id, competitor_id) {
            (Some(s), Some(c)) => Some((s, c)),
            _ => None,
        })
    }

    /// The engine schema (ATTACH alias) where a mounted namespace's
    /// PHYSICAL tables live, recovered from the connection itself via
    /// PRAGMA database_list — the `imprint_namespace` precedent (its step
    /// 4b): a mounted "main" deliberately carries no alias in its
    /// cartridge (reads resolve unqualified via SQLite's cross-schema
    /// search), so an operation that must BYPASS the temp schema — here
    /// the F2 qualified-read punch-through — matches the namespace's
    /// source file against the attached databases. `None` when the
    /// namespace has no source file or no attached schema matches (the
    /// caller then has no physical schema to punch through to).
    pub fn physical_schema_alias_for_namespace(
        &self,
        namespace_fq: &str,
        connection_id: i64,
    ) -> Result<Option<String>> {
        let source_path: Option<String> = {
            let conn = self.bootstrap_connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire bootstrap lock for schema-alias recovery",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            conn.query_row(
                "SELECT source_path FROM namespace
                 WHERE fq_name = ?1 AND source_path IS NOT NULL",
                [namespace_fq],
                |row| row.get(0),
            )
            .ok()
        };
        let Some(path) = source_path else {
            return Ok(None);
        };
        let Ok(want) = std::fs::canonicalize(&path) else {
            return Ok(None);
        };
        let target_conn = self
            .connection_map
            .get(&connection_id)
            .cloned()
            .unwrap_or_else(|| Arc::clone(&self.connection));
        let guard = target_conn.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire connection lock for schema-alias recovery",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let Ok((_cols, rows)) = guard.query_all_rows("PRAGMA database_list", &[]) else {
            return Ok(None);
        };
        Ok(rows.iter().find_map(|row| {
            let alias = row.get(1)?.as_wire_text()?;
            let file = row.get(2)?.as_wire_text()?;
            if file.is_empty() {
                return None; // temp / :memory: — never a punch-through target
            }
            (std::fs::canonicalize(file).ok()? == want).then_some(alias)
        }))
    }

    /// The registered `output_column` attributes of one entity, in position
    /// order — the per-entity form of what `BootstrapBackedSchema` answers
    /// by name. Used by the qualified-resolution shadow punch-through to
    /// read the COMPETITOR's columns when a session-materialized entity
    /// shares its name (name-keyed lookups would answer the session entity).
    pub fn output_columns_for_entity(
        &self,
        entity_id: i64,
    ) -> Result<Vec<delightql_types::schema::ColumnInfo>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for entity columns",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let mut stmt = conn
            .prepare(
                "SELECT attribute_name, position, is_nullable, data_type
                 FROM entity_attribute
                 WHERE entity_id = ?1 AND attribute_type = 'output_column'
                 ORDER BY position",
            )
            .map_err(|e| DelightQLError::database_error("entity column query", e.to_string()))?;
        let cols = stmt
            .query_map([entity_id], |row| {
                let name: String = row.get(0)?;
                let position: i32 = row.get(1)?;
                let is_nullable: Option<i32> = row.get(2)?;
                let data_type: Option<String> = row.get(3)?;
                Ok(delightql_types::schema::ColumnInfo {
                    name: name.into(),
                    nullable: is_nullable.unwrap_or(1) != 0,
                    position: (position + 1) as usize, // 0-based to 1-based
                    declared_type: data_type.filter(|t| !t.is_empty()),
                })
            })
            .map_err(|e| DelightQLError::database_error("entity column query", e.to_string()))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(cols)
    }

    /// The catalog namespace a connection's entities live under: `main`
    /// for the primary user connection (id 2, the register_connection
    /// convention entry.rs and created-object registration share), else
    /// the namespace whose mount-introspected entities carry the
    /// connection's cartridge. Used to key durable placement,
    /// the durable clash universe, and created-object registration on the
    /// CONNECTION rather than an unconditional `main`. Pinned (primary case,
    /// end-to-end) by the CLI integration test
    /// `table_bang_persists_to_the_db_file_across_sessions`; the
    /// non-primary lookup is exercised by the effect_transformer
    /// two-connection refusal tests and remains end-to-end unpinned until
    /// a real second engine is testable (PG/DuckDB ferry).
    pub fn connection_namespace_fq(&self, connection_id: i64) -> Result<Option<String>> {
        if connection_id == PRIMARY_CONNECTION_ID {
            return Ok(Some("main".to_string()));
        }
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for connection-namespace lookup",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        // Mount identity via the STORED link — resolves even for a mount
        // whose image holds zero entities.
        Ok(conn
            .query_row(
                "SELECT n.fq_name
                 FROM namespace n
                 JOIN mount m ON m.namespace_id = n.id
                 JOIN cartridge c ON c.id = m.cartridge_id
                 WHERE c.connection_id = ?1
                   AND c.source_uri <> 'session://materialized'
                 LIMIT 1",
                [connection_id],
                |row| row.get(0),
            )
            .ok())
    }

    /// The ENGINE SCHEMA a namespace's mount binds — a durable home on
    /// targets, a per-mount RECORDED fact. The fact is
    /// `mount.qualification` plus `mount.engine_schema`:
    /// - a SPELLED schema (`#schema` / `mount_tree!`) is
    ///   returned verbatim — the mount introspected THAT schema, so durable
    ///   placement and read qualification agree with it;
    /// - an unqualified binding (a bare mount — "the engine's own default")
    ///   resolves DOWNSTREAM here to the engine default by connection type
    ///   (3 = postgres → `public`, 4 = duckdb → `main`), keeping a bare
    ///   mount's reads unqualified;
    /// - anything else — the SQLite primary (alias-recovery territory),
    ///   siso pipes, missing rows — answers `None`, and the PG durable CTAS
    ///   REFUSES rather than emit search_path-fragile unqualified DDL.
    ///
    /// NAMESPACE-keyed because one connection legitimately holds MANY
    /// schemas under `mount_tree!`; the connection-keyed shim below
    /// exists only for callers holding just a connection whose
    /// namespace↔schema mapping is 1:1 today. Pinned by
    /// `pg_table_bang_ctas_spells_the_mounted_schema_and_registers_on_the_connection`
    /// (effect_transformer/tests.rs) and
    /// `mount_records_the_engine_schema_and_the_lookup_reads_it`
    /// (schema_mount_recording_tests).
    pub fn mounted_engine_schema_for_namespace(
        &self,
        namespace_fq: &str,
    ) -> Result<Option<String>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for mounted-schema lookup",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        // The binding carries qualification/schema; the cartridge supplies
        // only the connection whose type seeds an unqualified default.
        // Mount identity via the STORED link — an empty mounted image has
        // a recorded schema too.
        let recorded: Option<(Option<String>, Option<i64>)> = conn
            .query_row(
                "SELECT CASE
                            WHEN m.qualification = 'engine_schema' THEN m.engine_schema
                            WHEN m.qualification = 'aliased' THEN m.attach_alias
                            ELSE NULL
                        END,
                        co.connection_type
                 FROM namespace n
                 JOIN mount m ON m.namespace_id = n.id
                 JOIN cartridge c ON c.id = m.cartridge_id
                 LEFT JOIN connection co ON co.id = c.connection_id
                 WHERE n.fq_name = ?1
                   AND c.source_uri <> 'session://materialized'
                 LIMIT 1",
                [namespace_fq],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .ok();
        Ok(match recorded {
            Some((Some(schema), _)) => Some(schema),
            Some((None, ct)) => default_engine_schema_for_type(ct),
            None => None,
        })
    }

    /// Connection-keyed shim over `mounted_engine_schema_for_namespace`:
    /// the durable-placement and read-back callers
    /// hold the object's CONNECTION and create in that connection's
    /// namespace, a 1:1 mapping today. Routes to the recorded fact via the
    /// connection's namespace; a connection with no resolvable namespace
    /// falls back to the connection-type default directly (defensive, for
    /// SQLite-primary/siso rows with no mount binding).
    pub fn mounted_engine_schema_for_connection(
        &self,
        connection_id: i64,
    ) -> Result<Option<String>> {
        if let Some(ns) = self.connection_namespace_fq(connection_id)? {
            return self.mounted_engine_schema_for_namespace(&ns);
        }
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for mounted-schema lookup",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let connection_type: Option<i64> = conn
            .query_row(
                "SELECT connection_type FROM connection WHERE id = ?1",
                [connection_id],
                |r| r.get(0),
            )
            .ok();
        Ok(default_engine_schema_for_type(connection_type))
    }

    /// The KIND (`is_view`) of the session-materialized object holding
    /// `name` on `connection_id`, if the session catalog knows one — the
    /// compile-time holder probe behind the cross-kind temp replace
    /// (EFFECT-ALGEBRA §3: replacement is by NAME, not
    /// kind; the replace DROP must match whatever HOLDS the name). Gated
    /// like `session_shadow_split`: sessions that never materialized
    /// anything answer without touching bootstrap. An object minted
    /// outside the catalog stays invisible here and surfaces the engine's
    /// own error. Pinned by the
    /// CLI integration tests `temp_view_over_temp_table_replaces_the_table`
    /// / `temp_table_over_temp_view_replaces_the_view`.
    pub fn session_created_object_kind(
        &self,
        name: &str,
        connection_id: i64,
    ) -> Result<Option<bool>> {
        if !self.session_materialized_names.get() {
            return Ok(None);
        }
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for holder-kind probe",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let kind: Option<i64> = conn
            .query_row(
                "SELECT e.type
                 FROM entity e
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE c.source_uri = 'session://materialized'
                   AND c.connection_id = ?1
                   AND e.name = ?2 COLLATE NOCASE
                 ORDER BY e.id DESC
                 LIMIT 1",
                rusqlite::params![connection_id, name],
                |row| row.get(0),
            )
            .ok();
        Ok(kind.map(|t| t == i64::from(EntityType::DbTemporaryView.as_i32())))
    }

    /// Refresh a data namespace by re-introspecting its source database.
    ///
    /// Clears all entity metadata and re-discovers entities from the same
    /// database source. Preserves namespace identity, enlistments, aliases,
    /// and groundings. Validates grounding contracts after refresh.
    pub fn refresh_namespace(&mut self, namespace: &str) -> Result<usize> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for refresh",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and is 'data' kind
        let (ns_id, kind): (i64, String) = bootstrap_conn
            .query_row(
                "SELECT id, kind FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        if kind != "data" {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot refresh '{}' — it is a {} namespace. refresh!() only works on data namespaces. \
                     Use reconsult!() for lib namespaces.",
                    namespace, kind
                ),
                "Wrong namespace kind",
            ));
        }

        // 2. Retrieve cartridge metadata for re-introspection
        // The mount's cartridge is the STORED link — authoritative even for a
        // valid-but-EMPTY image (a file image re-introspects to zero
        // entities; a bytes image reaches the immutable refusal below). The
        // old entity-join lookup and its source-match fallback are REPEALED.
        let cartridge_meta: Option<(
            i64,
            Option<i64>,
            Option<String>,
            Option<String>,
            Option<String>,
            String,
            Option<String>,
            Option<String>,
        )> = bootstrap_conn
            .query_row(
                "SELECT m.cartridge_id, c.connection_id, c.source_ns, c.source_uri,
                            m.attach_alias, m.class, m.engine_schema, m.attachment
                     FROM mount m
                     JOIN cartridge c ON c.id = m.cartridge_id
                     WHERE m.namespace_id = ?1",
                [ns_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to read mount binding for refresh",
                    e.to_string(),
                )
            })?;

        let (
            old_cartridge_id,
            connection_id,
            source_ns,
            source_uri,
            attach_alias,
            mount_class,
            engine_schema,
            mount_attachment,
        ) = match &cartridge_meta {
            Some((cart_id, conn_id, src_ns, src_uri, alias, class, engine_schema, attachment)) => (
                *cart_id,
                conn_id.unwrap_or(2),
                src_ns.clone(),
                src_uri.clone().unwrap_or_default(),
                alias.clone(),
                class.clone(),
                engine_schema.clone(),
                match attachment.as_deref() {
                    Some("borrowed") => Attachment::Borrowed,
                    _ => Attachment::Owned,
                },
            ),
            None => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Namespace '{}' has no cartridge — cannot refresh",
                        namespace
                    ),
                    "No cartridge found",
                ));
            }
        };

        // delightql-bytes:// mounts refuse refresh: an immutable embedded
        // image cannot have changed, so a refresh has
        // nothing to observe. unmount!/mount! if a re-mount is really wanted.
        if source_uri.starts_with("delightql-bytes://") {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot refresh '{}': it is mounted from an immutable embedded image ({}). \
                     Use unmount!() and mount!() to re-mount.",
                    namespace, source_uri
                ),
                "Embedded images are immutable",
            ));
        }

        // 3. Begin a nestable transaction: refresh! is liminal-eligible and
        // may therefore run under the program-level savepoint.
        let transaction = CatalogSavepoint::begin(
            &bootstrap_conn,
            "dql_refresh_namespace",
            "Failed to begin refresh transaction",
        )?;

        // 4. Clear contents — FK choreography first: un-point the link BEFORE
        // the old cartridge row is deleted, inside this transaction, so a
        // rollback restores link and cartridge TOGETHER. Then clear the
        // entity-discovered cartridges plus the authoritative one from the
        // link (an ENTITYLESS cartridge is invisible to the entity scan;
        // the explicit clear is a no-op when the scan
        // already removed it).
        let clear_result = Self::clear_mount_binding(&bootstrap_conn, ns_id)
            .and_then(|_| Self::clear_namespace_contents(&bootstrap_conn, ns_id))
            .and_then(|_| Self::clear_cartridge_entities(&bootstrap_conn, old_cartridge_id));
        if let Err(e) = clear_result {
            return Err(e);
        }

        // 5. Re-introspect
        let entities = if connection_id == PRIMARY_CONNECTION_ID {
            // ATTACH path: the PHYSICAL alias comes from the mount binding.
            // Substituting the namespace NAME would introspect SQLite's hub
            // instead of the attached database.
            // The read is LOUD: for an attach-class
            // mount the alias is REQUIRED identity — a catalog error or a
            // NULL is an internal consistency failure, never a silent
            // fallback that would recreate the hub-introspection bug.
            let Some(alias) = attach_alias.as_deref() else {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot refresh '{}': it has a mount cartridge but no recorded \
                         attachment alias — internal catalog inconsistency",
                        namespace
                    ),
                    "Missing attach_alias for attach-class mount",
                ));
            };
            match self.introspector.introspect_entities_in_schema(alias) {
                Ok(e) => e,
                Err(e) => {
                    return Err(DelightQLError::database_error(
                        format!("Failed to re-introspect schema '{}': {}", alias, e),
                        e.to_string(),
                    ));
                }
            }
        } else {
            // Factory path: use connection_factory
            match &self.connection_factory {
                Some(factory) => {
                    let components = match factory.create(&source_uri) {
                        Ok(c) => c,
                        Err(e) => {
                            return Err(DelightQLError::database_error(
                                format!("Failed to create connection for refresh: {}", e),
                                e.to_string(),
                            ));
                        }
                    };
                    match components.introspector.introspect_entities() {
                        Ok(e) => e,
                        Err(e) => {
                            return Err(DelightQLError::database_error(
                                format!("Failed to re-introspect '{}': {}", source_uri, e),
                                e.to_string(),
                            ));
                        }
                    }
                }
                None => {
                    return Err(DelightQLError::database_error(
                        "Cannot refresh factory-mounted namespace without connection factory",
                        "No connection factory",
                    ));
                }
            }
        };

        // 6. Re-register: new cartridge + entities
        let cartridge_id = {
            let language = if connection_id == PRIMARY_CONNECTION_ID {
                3
            } else {
                // Determine from source_uri
                if source_uri.starts_with("postgres://") || source_uri.starts_with("postgresql://")
                {
                    3
                } else {
                    3
                }
            };
            bootstrap_conn.execute(
                "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                 VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                rusqlite::params![
                    language,
                    crate::bootstrap::SourceType::Db.as_i32(),
                    &source_uri,
                    source_ns.as_deref(),
                    connection_id,
                ],
            ).map_err(|e| {
                DelightQLError::database_error("Failed to create refresh cartridge", e.to_string())
            })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        let replacement = if mount_class == "attach" {
            MountBinding::attach(
                ns_id,
                cartridge_id as i64,
                connection_id,
                attach_alias.clone().ok_or_else(|| {
                    DelightQLError::database_error(
                        "Cannot refresh attach mount without an attachment alias",
                        "Missing mount alias",
                    )
                })?,
                // Refresh re-reads a schema that is already open; it opens
                // nothing, so it cannot turn a borrowed handle into an owned
                // one. The fact travels from the row being replaced.
                mount_attachment,
                if namespace == "main" {
                    "unqualified"
                } else {
                    "aliased"
                },
            )
        } else {
            MountBinding::external(ns_id, cartridge_id as i64, connection_id, engine_schema)
        };
        if let Err(e) = Self::record_mount_binding(&bootstrap_conn, &replacement) {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
            return Err(e);
        }

        let entity_count = entities.len();
        if let Err(e) = crate::bootstrap::introspect::insert_discovered_entities(
            &bootstrap_conn,
            cartridge_id,
            &entities,
        ) {
            return Err(DelightQLError::database_error(
                format!("Failed to insert discovered entities: {}", e),
                e.to_string(),
            ));
        }

        if let Err(e) = crate::import::activate_entities_from_cartridge(
            &bootstrap_conn,
            cartridge_id,
            ns_id as i32,
        ) {
            return Err(DelightQLError::database_error(
                format!("Failed to activate entities: {}", e),
                e.to_string(),
            ));
        }

        // 7. The refreshed data world must still answer every derived
        // world's data holes: re-admit each world bound to it, whole, by
        // the same admission that published it.
        for root_id in crate::defuse::grounded_world::roots_bound_to(&bootstrap_conn, ns_id)? {
            crate::defuse::grounded_world::DerivedWorld::current(&bootstrap_conn, root_id)?
                .admit(&bootstrap_conn, crate::defuse::CatalogRead::of(self))
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Grounding contract violation: data '{namespace}'. {e}"),
                        "Grounding contract violated",
                    )
                })?;
        }

        // 8. Commit
        transaction.commit("Failed to commit refresh transaction")?;

        drop(bootstrap_conn);

        debug!(
            "refresh_namespace: Refreshed namespace '{}' with {} entities",
            namespace, entity_count
        );

        Ok(entity_count)
    }

    /// Reconsult a lib/scratch namespace by re-reading and re-parsing its source file.
    ///
    /// Clears all entity definitions and re-loads from the same (or new) source file.
    /// Preserves namespace identity, enlistments, aliases. If grounded namespaces
    /// borrow from this lib, validates the grounding contract and auto-rebuilds.
    pub fn reconsult_namespace(
        &mut self,
        namespace: &str,
        new_file_path: Option<&str>,
    ) -> Result<usize> {
        // SANCTIONED CATALOG WRITER: the store fence admits definition-table
        // writes only while this window is open.
        let _catalog_window = self.bootstrap_guard.catalog_window();
        // A fresh consultation may not reload a caller-owned namespace as a
        // side effect. During reconsult, the same shape is intentional tree
        // replay: existing children reload atomically under the outer
        // savepoint, so it stays allowed.
        if let Some((_, LiminalProgramKind::Consult)) = self.active_liminal_program() {
            self.refuse_preexisting_namespace_mutation_in_program(
                namespace,
                "reloading",
                "directive/reconsult/uncompensable",
            )?;
        }
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for reconsult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // 1. Validate namespace exists and check kind
        let (ns_id, kind, source_path): (i64, String, Option<String>) = bootstrap_conn
            .query_row(
                "SELECT id, kind, source_path FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found", namespace),
                    "Namespace not found",
                )
            })?;

        match kind.as_str() {
            "data" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot reconsult '{}' — it is a data namespace. Use refresh!() instead.",
                        namespace
                    ),
                    "Wrong namespace kind",
                ));
            }
            "system" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot reconsult '{}' — system namespaces cannot be modified.",
                        namespace
                    ),
                    "Protected namespace",
                ));
            }
            "container" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot reconsult '{}' — structural container namespaces have no authored source. Reconsult their child namespaces instead.",
                        namespace
                    ),
                    "Protected namespace",
                ));
            }
            "grounded" => {
                return Err(DelightQLError::database_error(
                    format!(
                        "Cannot reconsult '{}' — it is a grounded namespace. Reconsult the source lib namespace instead.",
                        namespace
                    ),
                    "Wrong namespace kind",
                ));
            }
            "lib" | "scratch" | "unknown" => { /* acceptable */ }
            other => panic!(
                "catch-all hit in system.rs reconsult_namespace: unexpected namespace kind: {}",
                other
            ),
        }

        // 2. Determine source file
        let file_path = match new_file_path {
            Some(p) => p.to_string(),
            None => {
                if let Some(ref sp) = source_path {
                    sp.clone()
                } else {
                    // Try to find from cartridge source_uri
                    let uri: Option<String> = bootstrap_conn
                        .query_row(
                            "SELECT c.source_uri
                             FROM cartridge c
                             JOIN entity e ON e.cartridge_id = c.id
                             JOIN activated_entity ae ON ae.entity_id = e.id
                             WHERE ae.namespace_id = ?1
                             LIMIT 1",
                            [ns_id],
                            |row| row.get(0),
                        )
                        .ok();
                    match uri {
                        Some(u) if u.starts_with("file://") => u[7..].to_string(),
                        _ => {
                            return Err(DelightQLError::database_error(
                                format!(
                                    "Cannot determine source file for namespace '{}'. \
                                     Provide a file path: reconsult!(\"ns\", \"path/to/file.dql\")",
                                    namespace
                                ),
                                "No source file",
                            ));
                        }
                    }
                }
            }
        };

        // 3. Read + parse new file
        drop(bootstrap_conn);

        // Resolve relative path against session CWD (for test isolation).
        let resolved_path = crate::session_cwd::resolve_path(&file_path);
        let file_path = resolved_path.display().to_string();

        let source = std::fs::read_to_string(&file_path).map_err(|e| {
            DelightQLError::database_error(
                format!("reconsult!() failed to read file '{}': {}", file_path, e),
                "File read error",
            )
        })?;

        // ONE PARSE PER CONSULTED SUBMISSION, reconsult's as much as consult's.
        let consulted = crate::bin_cartridge::prelude::consult::Consulted::read(&source).map_err(
            |e| match e {
                DelightQLError::ParseError { .. } => DelightQLError::database_error(
                    format!("reconsult!() failed to parse '{}': {}", file_path, e),
                    "Parse error",
                ),
                other => other,
            },
        )?;

        // The shared loader owns the program savepoint and caller-state
        // restoration. Reconsult supplies only its replacement body.
        crate::bin_cartridge::prelude::consult::run_liminal_program(
            self,
            LiminalProgramKind::Reconsult,
            |this| {
                let self_ = this;

                // The ONE liminal interpreter supplies the same validation,
                // deferral, ordering, receipts, and entity dispatch as consult.
                // Replay mode's only distinction is that an existing nested
                // consult! child reloads.
                let crate::bin_cartridge::prelude::consult::Consulted {
                    forms,
                    ddl_blocks: _,
                } = consulted;
                // A file declaring only liminal directives — a facade that
                // consults and exposes children — is a lawful consult, so it
                // is a lawful reconsult: its load is its lexical graph.
                let load = crate::bin_cartridge::prelude::consult::execute_liminal_forms(
                    self_,
                    forms,
                    namespace,
                    &file_path,
                    crate::bin_cartridge::prelude::consult::LiminalDirectiveMode::Replay,
                )?;

                // 4. PUBLISH THE REPLACEMENT: the load is born in replacement
                // mode, so publication deletes the current load whole, spends
                // the new one, records its source, and rebuilds every
                // dependent derived world — one transaction, one road.
                let published = self_.publish(load)?;
                let entity_count = published.definitions_loaded();

                // THE RELOADED FILE'S OWN LEDGER, whole. `delete_namespace_load`
                // dropped the previous one, and the witnesses prove against the
                // definitions this reload just registered — reconsult REPLACES a
                // namespace's ledger, because the record describes THE load.
                let rows = crate::bin_cartridge::prelude::consult::prove_witnesses(
                    self_,
                    namespace,
                    published.into_ledger(),
                )?;
                self_.record_liminal_ledger(namespace, &rows)?;

                debug!(
                    "reconsult_namespace: Reconsulted namespace '{}' from '{}' with {} entities",
                    namespace, file_path, entity_count
                );

                Ok(entity_count)
            },
        )
    }

    /// THE LIMINAL RELATION (EFFECT-ALGEBRA §8): the corresponding-union echo
    /// columns of a namespace's liminal ledger, in first-appearance order
    /// across its receipts (the union corresponding of the file's mixed
    /// directive schemas — later columns NULL-pad, never break).
    ///
    /// `Ok(None)` = no such namespace (the caller falls through to the
    /// ordinary catalog-functor resolution and its "Table not found" error).
    /// `Ok(Some(vec![]))` = the namespace exists with an EMPTY liminal — a
    /// namespace created by other means (mount, imprint, ground, inline DDL)
    /// or a liminal-space-free file; the drill then presents only the bare
    /// receipt prefix (success, operation) over zero receipt rows (pinned by
    /// `liminal_ledger_empty_for_non_consulted`).
    ///
    /// Accepts an alias as well as a fq name — returns the CANONICAL fq name
    /// alongside the columns so the caller synthesizes against the real
    /// namespace.
    pub fn liminal_echo_columns(&self, ns_fq: &str) -> Result<Option<(String, Vec<String>)>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for liminal ledger",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let found: Option<(i64, String)> = conn
            .query_row(
                "SELECT id, fq_name FROM namespace WHERE fq_name = ?1",
                [ns_fq],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| {
                DelightQLError::database_error("Failed to look up namespace", e.to_string())
            })?
            .or_else(|| {
                conn.query_row(
                    "SELECT n.id, n.fq_name FROM namespace_alias a
                     JOIN namespace n ON n.id = a.target_namespace_id
                     WHERE a.alias = ?1",
                    [ns_fq],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .ok()
                .flatten()
            });

        let Some((ns_id, canonical_fq)) = found else {
            return Ok(None);
        };

        // Union the per-receipt echo lists in insertion (= file-appearance)
        // order; first appearance wins the column position.
        let mut stmt = conn
            .prepare("SELECT echoes FROM liminal_receipt WHERE namespace_id = ?1 ORDER BY id")
            .map_err(|e| {
                DelightQLError::database_error("Failed to read liminal ledger", e.to_string())
            })?;
        let echo_lists: Vec<String> = stmt
            .query_map([ns_id], |row| row.get::<_, String>(0))
            .map_err(|e| {
                DelightQLError::database_error("Failed to read liminal ledger", e.to_string())
            })?
            .flatten()
            .collect();

        let mut union: Vec<String> = Vec::new();
        for list in echo_lists {
            let names: Vec<String> = serde_json::from_str(&list).map_err(|e| {
                DelightQLError::database_error("Corrupt liminal receipt echo list", e.to_string())
            })?;
            for name in names {
                if !union.contains(&name) {
                    union.push(name);
                }
            }
        }
        Ok(Some((canonical_fq, union)))
    }

    /// Test-inspection: the namespace's ledger `operation` column in
    /// insertion (= file-appearance) order; None if no such namespace.
    /// Serves the liminal_ledger_* pins in consult.rs.
    #[cfg(test)]
    pub(crate) fn liminal_ledger_operations(&self, ns_fq: &str) -> Result<Option<Vec<String>>> {
        let conn = self.bootstrap_connection.lock().expect("bootstrap lock");
        let ns_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [ns_fq],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| DelightQLError::database_error("ns lookup", e.to_string()))?;
        let Some(ns_id) = ns_id else { return Ok(None) };
        let mut stmt = conn
            .prepare("SELECT operation FROM liminal_receipt WHERE namespace_id = ?1 ORDER BY id")
            .map_err(|e| DelightQLError::database_error("ledger read", e.to_string()))?;
        let ops = stmt
            .query_map([ns_id], |row| row.get::<_, String>(0))
            .map_err(|e| DelightQLError::database_error("ledger read", e.to_string()))?
            .flatten()
            .collect();
        Ok(Some(ops))
    }

    /// Test-inspection: total ledger rows across ALL namespaces — proves an
    /// aborted or unconsulted load left no orphan receipts behind.
    #[cfg(test)]
    pub(crate) fn liminal_receipt_row_count(&self) -> i64 {
        let conn = self.bootstrap_connection.lock().expect("bootstrap lock");
        conn.query_row("SELECT COUNT(*) FROM liminal_receipt", [], |row| row.get(0))
            .unwrap_or(-1)
    }

    /// Get the canonical (bootstrap-stored) name for an entity in a specific namespace.
    /// Used for namespace-qualified and grounded lookups where resolve_unqualified_entity
    /// is not used.
    pub fn get_canonical_entity_name(
        &self,
        namespace_fq: &str,
        entity_name: &str,
    ) -> Result<Option<delightql_types::SqlIdentifier>> {
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap connection lock",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let query = "
            SELECT e.name
            FROM activated_entity ae
            JOIN entity e ON ae.entity_id = e.id
            JOIN namespace n ON ae.namespace_id = n.id
            WHERE e.name = ?1 COLLATE NOCASE
              AND n.fq_name = ?2
            LIMIT 1
        ";

        match conn.query_row(query, rusqlite::params![entity_name, namespace_fq], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(canonical) => Ok(Some(delightql_types::SqlIdentifier::new(canonical))),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DelightQLError::database_error_with_source(
                "Failed to get canonical entity name",
                e.to_string(),
                Box::new(e),
            )),
        }
    }
}

// =============================================================================
// Interior Schema Registration (for drill-down support)
// =============================================================================

/// Walk an unresolved query AST and register any tree group interior schemas
/// into the `interior_entity` / `interior_entity_attribute` sys tables.
fn register_interior_schemas_from_query(
    conn: &Connection,
    entity_id: i32,
    query: &crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Unresolved>,
) -> Result<()> {
    use crate::pipeline::ast_visit::walk_visit_query;

    let mut registrar = InteriorSchemaRegistrar { conn, entity_id };
    walk_visit_query(&mut registrar, query)?;
    Ok(())
}

struct InteriorSchemaRegistrar<'a> {
    conn: &'a Connection,
    entity_id: i32,
}

impl crate::pipeline::ast_visit::AstVisit<crate::pipeline::asts::core::Unresolved>
    for InteriorSchemaRegistrar<'_>
{
    /// A tree group's interior schema is filed under the name its PUBLICATION
    /// gave it. An unnamed one files nothing: there is no name to file it
    /// under, and inventing one would answer a question the author declined.
    fn enter_out_item(
        &mut self,
        item: &crate::pipeline::asts::core::OutItem<crate::pipeline::asts::core::Unresolved>,
    ) -> Result<crate::pipeline::ast_visit::Descent> {
        use crate::pipeline::ast_visit::Descent;
        use crate::pipeline::asts::core::{DomainExpression, FunctionApplication, OutItem};

        let OutItem::One(one) = item else {
            return Ok(Descent::Continue);
        };
        let Some(naming) = one.naming.as_ref() else {
            return Ok(Descent::Continue);
        };
        if let DomainExpression::Application(FunctionApplication::Enclyph(
            crate::pipeline::asts::core::Enclyph::Record(record),
        )) = &one.expr
        {
            register_tree_group(self.conn, self.entity_id, &record.members, naming.as_str())?;
            // register_record_members owns the nested tree-group recursion for
            // this schema. Do not register a nested record a second time as if
            // it were another top-level result column.
            return Ok(Descent::SkipSubtree);
        }
        Ok(Descent::Continue)
    }
}

/// Register one aliased RECORD discovered anywhere in the unresolved query.
fn register_tree_group(
    conn: &Connection,
    entity_id: i32,
    members: &crate::pipeline::asts::vocabulary::Vec1<
        crate::pipeline::asts::core::RecordMember<crate::pipeline::asts::core::Unresolved>,
    >,
    alias: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
        rusqlite::params![entity_id, alias],
    )
    .map_err(|e| {
        DelightQLError::database_error_with_source(
            "Failed to insert interior_entity",
            e.to_string(),
            Box::new(e),
        )
    })?;
    let interior_entity_id = conn.last_insert_rowid() as i32;

    register_record_members(conn, interior_entity_id, entity_id, members)?;

    Ok(())
}

/// Register record members as interior_entity_attribute rows.
/// Handles nesting: an induced member is a level of its own, and recurses.
fn register_record_members(
    conn: &Connection,
    interior_entity_id: i32,
    parent_entity_id: i32,
    members: &crate::pipeline::asts::vocabulary::Vec1<
        crate::pipeline::asts::core::RecordMember<crate::pipeline::asts::core::Unresolved>,
    >,
) -> Result<()> {
    use crate::pipeline::asts::core::{Enclyph, NamedReference, RecordMember};

    for (position, member) in members.iter().enumerate() {
        match member {
            RecordMember::SelfKeyed(NamedReference(column)) => {
                let column = &column.name;
                conn.execute(
                    "INSERT INTO interior_entity_attribute \
                     (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                     VALUES (?1, ?2, ?3, NULL)",
                    rusqlite::params![interior_entity_id, column.as_str(), position as i32],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert interior_entity_attribute",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            }
            RecordMember::Induced { key, value } => {
                {
                    // Nested tree group: create a child interior_entity
                    if let Enclyph::Record(child) = value.as_ref() {
                        let child_members = &child.members;
                        // Insert child interior_entity (no alias needed for nested)
                        conn.execute(
                            "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
                            rusqlite::params![parent_entity_id, key.as_str()],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert child interior_entity",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                        let child_interior_entity_id = conn.last_insert_rowid() as i32;

                        // Register child members recursively
                        register_record_members(
                            conn,
                            child_interior_entity_id,
                            parent_entity_id,
                            child_members,
                        )?;

                        // Insert attribute pointing to child
                        conn.execute(
                            "INSERT INTO interior_entity_attribute \
                             (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                             VALUES (?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                interior_entity_id,
                                key.as_str(),
                                position as i32,
                                child_interior_entity_id
                            ],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert interior_entity_attribute (nested)",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                    }
                }
            }
            RecordMember::Keyed { key, .. } => {
                conn.execute(
                    "INSERT INTO interior_entity_attribute \
                     (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                     VALUES (?1, ?2, ?3, NULL)",
                    rusqlite::params![interior_entity_id, key.as_str(), position as i32],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert interior_entity_attribute",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            }
            // A metadata member's interior keys are data, so there is no
            // static child heading to register; the attribute row alone.
            RecordMember::Metadata { key, .. } => {
                conn.execute(
                    "INSERT INTO interior_entity_attribute \
                     (interior_entity_id, attribute_name, position, child_interior_entity_id) \
                     VALUES (?1, ?2, ?3, NULL)",
                    rusqlite::params![interior_entity_id, key.as_str(), position as i32],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to insert interior_entity_attribute",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            }
            // A spread is spent at resolution and this registrar reads the
            // authored tree, so the columns it stands for are not yet known.
            RecordMember::Spread(_) => {}
        }
    }

    Ok(())
}

/// Outcome of a lazy autoload (stdlib) module load — a union replacing a
/// bool that could not distinguish "not a module" from "module, but broken"
/// (an information hole: three states in two values). See
/// [`DelightQLSystem::ensure_stdlib_loaded`].
/// Which phase of a load failed. Distinguished because their remediations
/// differ (a parse failure is a syntax fix; a consult failure is a missing
/// reference), which the diagnostics `autoloads` provider maps to distinct
/// `delightql-diagnostic://autoload/{parse,consult}_failed` identifiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPhase {
    Parse,
    Consult,
}

#[derive(Debug)]
pub enum StdlibLoad {
    /// `namespace_fq` is not an embedded stdlib module (resolver falls through).
    NotAModule,
    /// The module was already loaded on a previous call.
    AlreadyLoaded,
    /// The module was parsed and consulted on this call.
    Loaded,
    /// The namespace IS an embedded module but failed; carries which phase
    /// and the cause, so callers can surface a phase-specific remediation
    /// instead of a bare `Table not found`.
    Failed {
        phase: LoadPhase,
        error: DelightQLError,
    },
}

/// Report an autoload module load failure. Always logs at warn; on dev
/// builds it also prints to stderr, because otherwise a broken autoload
/// surfaces only as a misleading `Table not found` with the real cause
/// hidden behind `RUST_LOG=warn`. The build-time `every_stdlib_module_parses`
/// test keeps this path unreachable for shipped modules.
fn report_stdlib_load_failure(namespace_fq: &str, err: &DelightQLError) {
    log::warn!("Failed to load stdlib '{}': {}", namespace_fq, err);
    if cfg!(debug_assertions) {
        eprintln!(
            "delightql: autoload module '{}' failed to load and was skipped:\n  {}",
            namespace_fq, err
        );
    }
}

#[cfg(test)]
mod stdlib_load_tests {
    //! Autoload modules are static `include_str!` content: a shipped binary
    //! must never carry an unparseable one. The lazy loader
    //! (`ensure_stdlib_loaded`) only fires for namespaces a session
    //! references, so a per-session run can miss a broken module — this
    //! test parses every one, unconditionally, at `cargo test` time.
    //!
    //! Regression guard for the silent-autoload-failure class: a syntax
    //! error in any `autoload/**/*.dql` fails here with the module name and
    //! the parse error, instead of surfacing later as a misleading
    //! `Table not found`.

    #[test]
    fn every_stdlib_module_parses() {
        for (ns, src) in crate::stdlib_manifest::STDLIB_MODULES {
            if let Err(e) = crate::bin_cartridge::prelude::consult::Consulted::read(src) {
                panic!("autoload module '{ns}' failed to parse: {e}");
            }
        }
    }
}

#[cfg(test)]
mod seed_program_tests {
    //! Embedded `seed/*.dql` programs RUN at every `open()` and every
    //! `reinit_bootstrap` for their effects (unlike autoloads, which only
    //! parse-and-consult on demand). `open()` already runs them collaterally,
    //! so a broken seed fails half the suite with no clue which seed is at
    //! fault (the ptzxpkmx lesson). These tests are TARGETED: they build a
    //! fresh in-memory system and run each seed individually, naming the
    //! culprit on failure.

    use super::DelightQLSystem;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use std::sync::{Arc, Mutex};

    /// Minimal introspector: seeds only touch the bootstrap catalog (sys::
    /// tables), never the user target, so an empty target is correct here.
    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    /// Every embedded seed program parses, builds, and executes cleanly on a
    /// fresh system. On failure the panic names the culprit seed instead of
    /// half the suite going red with a misleading downstream error.
    #[test]
    fn every_seed_program_parses_and_executes() {
        for (name, source) in crate::seed_manifest::SEED_PROGRAMS {
            let mut system = fresh_system();
            if let Err(e) = system.run_seed_program(source) {
                panic!("seed program '{name}' failed to execute: {e}");
            }
        }
    }

    /// A seed statement that fires zero effects is a typo by definition (a
    /// mistyped directive parses as a plain table read and is silently
    /// discarded). `run_seed_program` must refuse it loudly. RED before the
    /// zero-effect guard landed. Constructed here so no broken seed file
    /// ships.
    #[test]
    fn seed_no_effect_statement_is_refused() {
        let mut system = fresh_system();
        // A plain relation read: no directive terminal, so the effect
        // executor executes nothing and returns it unchanged.
        let err = system
            .run_seed_program("sys::entities.entity(*)")
            .expect_err("a no-effect seed statement must be refused");
        let msg = format!("{err}");
        assert!(
            msg.contains("produced no effects"),
            "error should explain the zero-effect refusal, got: {msg}"
        );
    }

    /// The real shipped seeds are all-effect, so the zero-effect guard must
    /// never fire for them (a companion to the RED test above).
    #[test]
    fn shipped_seeds_are_all_effect() {
        for (name, source) in crate::seed_manifest::SEED_PROGRAMS {
            let mut system = fresh_system();
            system
                .run_seed_program(source)
                .unwrap_or_else(|e| panic!("shipped seed '{name}' tripped the guard: {e}"));
        }
    }
}

#[cfg(test)]
mod detach_guard_tests {
    //! The alias guard's contract:
    //! Armed from immediately after ATTACH to just after COMMIT, it must
    //! DETACH on every early exit and stay silent once disarmed.

    use super::DetachOnDrop;
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::DatabaseConnection;
    use std::sync::{Arc, Mutex};

    #[test]
    fn armed_guard_detaches_and_disarmed_guard_does_not() {
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();

        // Armed guard dropped (any early return after ATTACH) → DETACH runs.
        {
            let _guard = DetachOnDrop {
                connection: Arc::clone(&conn),
                alias: "_imported_991",
                armed: true,
            };
        }
        assert!(
            mock.lock()
                .unwrap()
                .assert_executed("DETACH DATABASE '_imported_991'"),
            "armed guard must DETACH its alias on drop"
        );

        // Disarmed guard (post-COMMIT) → no DETACH.
        {
            let mut guard = DetachOnDrop {
                connection: Arc::clone(&conn),
                alias: "_imported_992",
                armed: true,
            };
            guard.armed = false;
        }
        assert!(
            !mock.lock().unwrap().assert_executed("_imported_992"),
            "disarmed guard must not DETACH"
        );
    }

    #[test]
    fn explicit_rollback_detaches_without_relying_on_drop() {
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();
        let mut guard = DetachOnDrop {
            connection: Arc::clone(&conn),
            alias: "_imported_993",
            armed: true,
        };

        guard.rollback().expect("explicit inverse should detach");
        assert!(mock
            .lock()
            .unwrap()
            .assert_executed("DETACH DATABASE '_imported_993'"));
        drop(guard);
        assert_eq!(
            mock.lock()
                .unwrap()
                .get_executed_queries()
                .iter()
                .filter(|query| query.sql.contains("_imported_993"))
                .count(),
            1,
            "Drop must not retry an explicitly completed inverse"
        );
    }

    #[test]
    fn failed_explicit_rollback_is_disarmed_for_health_recovery() {
        let mock = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        mock.lock()
            .unwrap()
            .expect_error("DETACH DATABASE '_imported_994'", "scripted detach failure");
        let conn: Arc<Mutex<dyn DatabaseConnection>> = mock.clone();
        let mut guard = DetachOnDrop {
            connection: Arc::clone(&conn),
            alias: "_imported_994",
            armed: true,
        };

        let error = guard
            .rollback()
            .expect_err("scripted inverse must be observable");
        assert!(error.to_string().contains("Failed to detach"));
        drop(guard);
        assert_eq!(
            mock.lock()
                .unwrap()
                .get_executed_queries()
                .iter()
                .filter(|query| query.sql.contains("_imported_994"))
                .count(),
            1,
            "Drop must not retry a failed explicit inverse"
        );
    }
}

#[cfg(test)]
mod mount_link_tests {
    //! The stored mount fact. Written
    //! by the spine, re-pointed by refresh, cleared by unmount; UNIQUE
    //! encodes the 1:1 claim; identity consumers read it (an EMPTY image is
    //! a full citizen); a failed refresh rolls back link and cartridge
    //! TOGETHER; the bootstrap catalog stays FK-consistent.

    use super::DelightQLSystem;
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
                Err(delightql_types::DelightQLError::database_error(
                    "induced introspection failure",
                    "mount_link_tests",
                ))
            }
        }
    }

    fn system_with(introspector: Box<dyn DatabaseIntrospector>) -> DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(conn, introspector, "sqlite").expect("system should build")
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
        let mut system = DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite").unwrap();

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
            let id = super::ensure_catalog_initialized(&system.catalog_cartridge_id, &conn)
                .expect("initialized (or cached from construction)");
            // Simulate the rolled-back initialization: the catalog
            // cartridge row vanishes while the Cell keeps its id — and
            // SQLite may then REUSE the freed rowid for an UNRELATED
            // cartridge. An existence-only validation
            // accepts this impostor as the catalog cartridge; the
            // identity-marker validation must reject it.
            // (FKs are enforced on bootstrap — clear the cartridge's
            // dependents the same way the production paths do.)
            super::DelightQLSystem::clear_cartridge_entities(&conn, id as i64).unwrap();
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
            super::ensure_catalog_initialized(&system.catalog_cartridge_id, &conn)
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
        let mut system = DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite").unwrap();
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

        // The mounted-engine-schema lookup resolves for a ZERO-entity mount
        // (an entity-only derivation would fail here, since an empty mount
        // has none).
        let schema = system
            .mounted_engine_schema_for_namespace("idns")
            .expect("lookup runs");
        assert!(
            schema.as_deref().map(|s| s.starts_with("_imported_")) == Some(true),
            "empty mount must resolve its engine schema via the link, got {schema:?}"
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

    #[test]
    fn mount_catalog_mutations_stay_in_their_typed_helper() {
        let source = include_str!("system.rs");
        for mutation in [
            ["INSERT", "INTO", "mount"].join(" "),
            ["DELETE", "FROM", "mount"].join(" "),
        ] {
            assert_eq!(
                source.matches(&mutation).count(),
                1,
                "mount catalog mutation '{mutation}' must stay centralized in its typed helper"
            );
        }
    }
}

#[cfg(test)]
mod mount_new_database_tests {
    //! `mount_new!` (EFFECT-ALGEBRA §6): PROVISION a fresh, valid, empty SQLite
    //! database and bind it — the create-intent counterpart of `mount!`. These
    //! pin the three new behaviors (materialization, clobber refusal, v1
    //! SQLite-only scope) + reserved-name inheritance. The end-to-end
    //! round-trip (mount_new! then mount! the same file, with a real read-back)
    //! is the CLI `mount_new_roundtrip` integration test.

    use super::{DelightQLSystem, LiminalProgramKind};
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;
    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    /// A valid empty SQLite database has the 16-byte header magic.
    fn is_valid_sqlite(path: &std::path::Path) -> bool {
        use std::io::Read;
        let mut header = [0u8; 16];
        std::fs::File::open(path)
            .and_then(|mut f| f.read_exact(&mut header))
            .map(|()| &header == b"SQLite format 3\0")
            .unwrap_or(false)
    }

    /// mount_new! on a MISSING path materializes a valid, non-zero SQLite
    /// database (header-bearing) and the namespace resolves.
    #[test]
    fn mount_new_provisions_a_valid_database_and_binds_the_namespace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fresh.db");
        assert!(!path.exists(), "path must start missing");

        let mut system = fresh_system();
        system
            .mount_new_database(path.to_str().unwrap(), "freshns")
            .expect("mount_new! should provision + bind");

        // File exists, non-zero, valid SQLite header.
        assert!(path.exists(), "database file must exist after mount_new!");
        let len = std::fs::metadata(&path).expect("metadata").len();
        assert!(
            len > 0,
            "materialized db must be non-empty, got {len} bytes"
        );
        assert!(
            is_valid_sqlite(&path),
            "materialized db must carry the SQLite header"
        );

        // The namespace is registered and reachable: enlist_namespace requires
        // the namespace to exist (the enlisted-guard-classification test's
        // proof-of-registration pattern). The CLI round-trip test proves an
        // in-session read end-to-end.
        system
            .enlist_namespace("freshns")
            .expect("mount_new!'s namespace must be registered + reachable");
    }

    /// CLOBBER: mount_new! on a path holding a REAL database refuses with the
    /// substring, and the existing database is left untouched.
    #[test]
    fn mount_new_refuses_to_clobber_an_existing_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("existing.db");
        // A real db with a table + row.
        {
            let conn = rusqlite::Connection::open(&path).expect("seed db");
            conn.execute_batch("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (7);")
                .expect("seed schema");
        }
        let before = std::fs::read(&path).expect("read before");

        let mut system = fresh_system();
        let err = system
            .mount_new_database(path.to_str().unwrap(), "clobberns")
            .expect_err("mount_new! must refuse to clobber a non-empty path");
        let msg = format!("{err}");
        assert!(
            msg.contains("already exists; use mount!() to attach it"),
            "clobber message must teach mount!(): {msg}"
        );

        // The existing database is byte-for-byte untouched.
        let after = std::fs::read(&path).expect("read after");
        assert_eq!(
            before, after,
            "existing db must be untouched on clobber refusal"
        );
    }

    /// A 0-byte file is NOT content — mount_new! materializes over it.
    #[test]
    fn mount_new_materializes_over_a_zero_byte_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.db");
        std::fs::write(&path, b"").expect("touch 0-byte file");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        let mut system = fresh_system();
        system
            .mount_new_database(path.to_str().unwrap(), "zerons")
            .expect("mount_new! should materialize over a 0-byte file");
        assert!(is_valid_sqlite(&path), "0-byte file must become a valid db");
    }

    /// The external journal distinguishes a caller-owned zero-byte placeholder
    /// from an absent path. Aborting the program restores the placeholder; it
    /// must not delete it as though mount_new! had created the path itself.
    #[test]
    fn aborted_program_restores_zero_byte_mount_new_placeholder() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("placeholder.db");
        std::fs::write(&path, b"").expect("touch 0-byte file");

        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        system
            .mount_new_database(path.to_str().unwrap(), "temporary_mount")
            .expect("mount_new inside program");
        assert!(std::fs::metadata(&path).unwrap().len() > 0);

        system.rollback_liminal_external_effects();
        system
            .end_liminal_program(false)
            .expect("rollback program catalog");

        assert!(path.exists(), "caller-owned placeholder must remain");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            0,
            "placeholder must return to its exact pre-program state"
        );
    }

    /// v1 SCOPE: a URI target (postgres://, …) refuses cleanly with the
    /// SQLite-only substring — no file is created.
    #[test]
    fn mount_new_refuses_non_sqlite_targets() {
        let mut system = fresh_system();
        let err = system
            .mount_new_database("postgres://localhost/db", "pgns")
            .expect_err("mount_new! is SQLite-only in v1");
        let msg = format!("{err}");
        assert!(
            msg.contains("mount_new!() creates a new SQLite database"),
            "v1-scope message must state SQLite-only: {msg}"
        );
    }

    /// Reserved-name refusal is inherited from mount_database: a `_`-prefixed
    /// target is refused BEFORE any file is materialized.
    #[test]
    fn mount_new_inherits_the_reserved_name_refusal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("reserved.db");

        let mut system = fresh_system();
        let err = system
            .mount_new_database(path.to_str().unwrap(), "_secret")
            .expect_err("mount_new! must refuse a reserved namespace");
        assert!(
            format!("{err}").contains("reserved"),
            "reserved-name refusal must survive: {err}"
        );
        // No file materialized for a refused target.
        assert!(
            !path.exists(),
            "no db must be created for a reserved-name refusal"
        );
    }
}

#[cfg(test)]
mod function_clause_discipline_tests {
    //! The FUNCTIONAL half of "The Two Algebras". A value function's clauses are
    //! ordered first-match alternatives — at most one may be unguarded (the
    //! default), and it must be last. The chokepoint is
    //! `validate_function_clause_discipline`, gated on `DefKind::Function`, so
    //! sigma predicates (the relational OR path) are exempt.
    use super::validate_function_clause_discipline;
    use crate::ddl::reconstruct;

    fn discipline(source: &str) -> crate::error::Result<()> {
        let group = reconstruct::group(source).expect("source should build");
        validate_function_clause_discipline(&group)
    }

    #[test]
    fn two_unguarded_value_fn_clauses_refuse() {
        // The defect: N unguarded clauses would emit `CASE ELSE <last> END`.
        let err = discipline("f:(x) :- x + 1\nf:(x) :- x * 10")
            .expect_err("two unguarded value-function clauses must refuse");
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/unguarded_multiplicity"
        );
        let msg = format!("{err}");
        assert!(msg.contains("value function"), "msg: {msg}");
        assert!(msg.contains('f'), "should name the entity: {msg}");
    }

    #[test]
    fn duplicate_constants_refuse() {
        // A constant is a zero-arity value function; two are indistinguishable.
        let err = discipline("nl :- char:(10)\nnl :- char:(13)")
            .expect_err("duplicate constants must refuse");
        assert_eq!(
            err.error_uri(),
            "delightql-error://semantic/ddl/head/unguarded_multiplicity"
        );
    }

    #[test]
    fn guarded_with_single_unguarded_default_ok() {
        // fizzbuzz family (ddl/321): guards + one trailing default.
        discipline(
            "fizzbuzz:(n | (n % 15) = 0) :- \"fizzbuzz\"\n\
             fizzbuzz:(n | (n % 3) = 0) :- \"fizz\"\n\
             fizzbuzz:(n | (n % 5) = 0) :- \"buzz\"\n\
             fizzbuzz:(n) :- n",
        )
        .expect("guarded function with one trailing default is legal");
    }

    #[test]
    fn all_guarded_no_default_ok() {
        // Zero unguarded clauses: a CASE with no ELSE — legal.
        discipline("sign:(x | x > 0) :- 1\nsign:(x | x < 0) :- -1")
            .expect("all-guarded function is legal");
    }

    #[test]
    fn single_clause_function_ok() {
        discipline("double:(x) :- x * 2").expect("single-clause function is legal");
    }

    #[test]
    fn single_constant_ok() {
        discipline("nl :- char:(10)").expect("single constant is legal");
    }

    #[test]
    fn unguarded_not_last_refuses_on_position() {
        // Rule 4 still fires (currently parse/general; RULE 3 will rebadge it).
        let err = discipline(
            "fizzbuzz:(n) :- n\n\
             fizzbuzz:(n | (n % 3) = 0) :- \"fizz\"",
        )
        .expect_err("unguarded-not-last must refuse");
        let msg = format!("{err}");
        assert!(msg.contains("must be the last clause"), "msg: {msg}");
    }

    #[test]
    fn multi_clause_sigma_is_exempt() {
        // Plain-functor sigma predicate (ddl/320): DefKind::Sigma, not
        // Function — the helper is a no-op, clauses OR together elsewhere.
        discipline("empty(column) :- null = column\nempty(column) :- trim:(column) = \"\"")
            .expect("multi-clause sigma predicate must NOT be touched by this check");
    }
}

// =============================================================================
// The live W9 tree-group registration walker is structurally
// incomplete (system.rs `walk_relational_for_
// tree_groups`). It inspects ONLY `Group::Reduce.reductions`, so a tree group
// (a named record) in `keys` — a legal, surface-constructible
// position (`%({region, "kids": ~> {order_id}} as grp ~> count:(*))`,
// TreeGroupLocation::InKeys) — is never registered, leaving absent
// interior_entity metadata and later drill-down failures.
//
// The keys tree group IS constructible (verified
// against the parser), so this is a real hole, not a fabricated shape. RED
// today — the walker skips keys.
// =============================================================================
#[cfg(test)]
mod red5_w9_tree_group_tests {
    use rusqlite::Connection;

    /// A minimal bootstrap: just the two interior-schema tables the W9 walker
    /// writes into (FKs are unenforced by default, so no `entity` row needed).
    fn interior_schema_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory conn");
        conn.execute_batch(
            "CREATE TABLE interior_entity (
                 id INTEGER PRIMARY KEY,
                 parent_entity_id INTEGER NOT NULL,
                 column_name TEXT NOT NULL
             );
             CREATE TABLE interior_entity_attribute (
                 id INTEGER PRIMARY KEY,
                 interior_entity_id INTEGER NOT NULL,
                 attribute_name TEXT NOT NULL,
                 position INTEGER NOT NULL,
                 child_interior_entity_id INTEGER
             );",
        )
        .expect("create interior tables");
        conn
    }

    fn parse_one(
        dql: &str,
    ) -> crate::pipeline::asts::core::Query<crate::pipeline::asts::core::Unresolved> {
        let tree = crate::pipeline::parse::query_sequence(dql).expect("parse");
        let normalized = crate::pipeline::parse::normalize_sequence(&tree).expect("normalize");
        let mut queries = normalized.into_queries();
        assert_eq!(queries.len(), 1, "one statement expected");
        queries.remove(0).query
    }

    fn interior_column_names(conn: &Connection) -> Vec<String> {
        let mut stmt = conn
            .prepare("SELECT column_name FROM interior_entity ORDER BY id")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .flatten()
            .collect()
    }

    /// Control: a tree group in `reductions` DOES register today — proves the
    /// harness (parse → register → interior_entity read) works end to end.
    #[test]
    fn reducing_on_tree_group_registers_control() {
        let conn = interior_schema_conn();
        let query = parse_one("orders(*) |> %(region ~> {order_id} as kids)");
        super::register_interior_schemas_from_query(&conn, 1, &query).expect("registration walk");
        assert!(
            interior_column_names(&conn).iter().any(|c| c == "kids"),
            "reductions tree group must register (control): {:?}",
            interior_column_names(&conn)
        );
    }

    /// A tree group in `keys` must ALSO register its interior
    /// schema — the W9 walker skips it today (it only descends reductions), so
    /// its drill-down metadata is silently lost. RED until W9 is migrated onto
    /// the shared `AstVisit` walk or its input boundary enforced.
    #[test]
    fn reducing_by_tree_group_registers_interior_schema() {
        let conn = interior_schema_conn();
        let query =
            parse_one("orders(*) |> %({region, \"kids\": ~> {order_id}} as grp ~> count:(*) as c)");
        super::register_interior_schemas_from_query(&conn, 1, &query).expect("registration walk");
        let names = interior_column_names(&conn);
        assert!(
            names.iter().any(|c| c == "grp"),
            "keys tree group `grp` was NOT registered — W9 \
             (walk_relational_for_tree_groups) inspects only reductions, dropping \
             keys tree groups and their interior_entity metadata; reached: {:?}",
            names
        );
    }
}

#[cfg(test)]
mod liminal_boundary_tests {
    use super::{DelightQLSystem, LiminalProgramKind};
    use crate::external_effects::{
        CreatedFilePriorState, ExternalEffect, LiminalCatalogBoundary, LiminalClose, LiminalFileOps,
    };
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use rusqlite::Connection;
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;

    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }

        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> DelightQLSystem {
        let connection = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(connection, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh system should build")
    }

    #[test]
    fn unregistrable_postgres_target_is_refused_before_ddl() {
        let mut system = fresh_system();
        system.db_type = "postgres".to_string();
        let error = system
            .refuse_unregistrable_created_object("table", "staged", None)
            .expect_err("postgres with no mounted schema cannot promise registration");
        let message = error.to_string();
        assert!(
            message.contains("this target cannot register created objects"),
            "{message}"
        );
        assert!(
            message.contains("object would not resolve by name"),
            "{message}"
        );
    }

    struct ScriptedBoundary {
        fail_commit: bool,
        fail_rollback: bool,
        calls: Mutex<Vec<LiminalClose>>,
    }

    impl ScriptedBoundary {
        fn new(fail_commit: bool, fail_rollback: bool) -> Self {
            Self {
                fail_commit,
                fail_rollback,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl LiminalCatalogBoundary for ScriptedBoundary {
        fn begin(&self, _catalog: &Connection) -> Result<()> {
            Ok(())
        }

        fn close(&self, _catalog: &Connection, close: LiminalClose) -> Result<()> {
            self.calls.lock().unwrap().push(close);
            let failed = match close {
                LiminalClose::Commit => self.fail_commit,
                LiminalClose::Rollback => self.fail_rollback,
            };
            if failed {
                Err(delightql_types::DelightQLError::database_error(
                    "scripted liminal close failure",
                    format!("{close:?}"),
                ))
            } else {
                Ok(())
            }
        }
    }

    struct ScriptedFileOps {
        fail_remove: bool,
        fail_restore: bool,
        calls: Mutex<Vec<String>>,
    }

    impl ScriptedFileOps {
        fn new(fail_remove: bool, fail_restore: bool) -> Self {
            Self {
                fail_remove,
                fail_restore,
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    impl LiminalFileOps for ScriptedFileOps {
        fn remove_created(&self, path: &Path) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("remove:{}", path.display()));
            if self.fail_remove {
                Err(delightql_types::DelightQLError::database_error(
                    "scripted remove failure",
                    path.display().to_string(),
                ))
            } else {
                Ok(())
            }
        }

        fn restore_empty(&self, path: &Path) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("restore:{}", path.display()));
            if self.fail_restore {
                Err(delightql_types::DelightQLError::database_error(
                    "scripted restore failure",
                    path.display().to_string(),
                ))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn failed_commit_close_keeps_the_program_context_until_rollback() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        let boundary = ScriptedBoundary::new(true, false);

        assert!(system
            .begin_liminal_program_with(&boundary, mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        assert!(system.active_liminal_program().is_some());

        let error = system
            .end_liminal_program_with(&boundary, true)
            .expect_err("scripted commit close must fail");
        assert!(error.to_string().contains("scripted liminal close failure"));
        assert!(
            system.active_liminal_program().is_some(),
            "a failed RELEASE must not discard the compensation journal"
        );

        system.rollback_liminal_external_effects();
        system
            .end_liminal_program_with(&boundary, false)
            .expect("rollback close should recover the boundary");
        assert!(system.active_liminal_program().is_none());
        assert_eq!(
            *boundary.calls.lock().unwrap(),
            vec![LiminalClose::Commit, LiminalClose::Rollback]
        );
    }

    #[test]
    fn quarantine_is_a_sticky_new_query_refusal_until_reset() {
        let mut system = fresh_system();
        assert!(system.require_healthy().is_ok());

        system.quarantine_session("test operation", "uncertain cleanup");
        let error = system
            .require_healthy()
            .expect_err("quarantine must refuse new work");
        assert_eq!(
            error.error_uri(),
            "delightql-error://runtime/session_health/external_effect"
        );
        assert!(system.health_incident().is_some());

        system
            .reinit_bootstrap()
            .expect("a successful reset clears the quarantine");
        assert!(system.require_healthy().is_ok());
        assert!(system.health_incident().is_none());
    }

    #[test]
    fn failed_file_inverse_is_returned_and_remains_pending() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path = std::path::PathBuf::from("/tmp/dql-scripted-created.db");
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        let failures = system.compensate_liminal_external_effects_with(&file_ops);
        assert_eq!(failures.len(), 1);
        assert!(matches!(
            &failures[0].effect,
            ExternalEffect::CreatedFile { path: actual, prior_state: CreatedFilePriorState::Absent }
                if actual == &path
        ));
        assert!(failures[0]
            .error
            .to_string()
            .contains("scripted remove failure"));
        assert_eq!(
            file_ops.calls.lock().unwrap().as_slice(),
            &[format!("remove:{}", path.display())]
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
            }
            other => panic!("failed inverse must quarantine the session: {other:?}"),
        }
    }

    #[test]
    fn failed_mount_inverse_uses_health_identity_and_retains_both_uris() {
        let mut system = fresh_system();
        let primary = crate::error::DelightQLError::database_error_categorized(
            "mount/registration",
            "mount registration failed",
            "catalog registration",
        );
        let cleanup = crate::error::DelightQLError::database_error_categorized(
            "mount/detach",
            "detach failed",
            "alias cleanup",
        );
        let primary_uri = primary.error_uri();
        let cleanup_uri = cleanup.error_uri();

        let error = system.mount_error_after_alias_rollback("_imported_7", Err(cleanup), primary);

        assert_eq!(
            error.error_uri(),
            "delightql-error://runtime/session_health/external_effect"
        );
        let message = error.to_string();
        assert!(
            message.contains(&primary_uri),
            "primary URI omitted: {message}"
        );
        assert!(
            message.contains(&cleanup_uri),
            "cleanup URI omitted: {message}"
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
                assert!(incident.message.contains(&primary_uri));
                assert!(incident.message.contains(&cleanup_uri));
            }
            other => panic!("failed mount inverse must quarantine: {other:?}"),
        }
    }

    #[test]
    fn absent_file_inverse_treats_missing_path_as_success() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path =
            std::env::temp_dir().join(format!("dql-file-inverse-absent-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);

        assert!(system.rollback_liminal_external_effects().is_empty());
        assert!(system.health_incident().is_none());
        assert!(!path.exists());
    }

    #[test]
    fn empty_file_inverse_refuses_to_recreate_a_missing_path() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path =
            std::env::temp_dir().join(format!("dql-file-inverse-empty-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        system.journal_created_file(path.clone(), CreatedFilePriorState::Empty);

        let failures = system.rollback_liminal_external_effects();
        assert_eq!(failures.len(), 1);
        assert!(system.health_incident().is_some());
        assert!(
            !path.exists(),
            "a failed inverse must not recreate the path"
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
            }
            other => panic!("missing prior-empty file must quarantine: {other:?}"),
        }
    }

    #[test]
    fn pending_inverses_keep_journal_order_for_the_next_reset() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let first = std::path::PathBuf::from("/tmp/dql-first-created.db");
        let second = std::path::PathBuf::from("/tmp/dql-second-created.db");
        system.journal_created_file(first.clone(), CreatedFilePriorState::Absent);
        system.journal_created_file(second.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        let failures = system.compensate_liminal_external_effects_with(&file_ops);
        assert_eq!(failures.len(), 2);
        assert_eq!(
            file_ops.calls.lock().unwrap().as_slice(),
            &[
                format!("remove:{}", second.display()),
                format!("remove:{}", first.display()),
            ]
        );
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(
                    incident.pending_effects,
                    vec![
                        ExternalEffect::CreatedFile {
                            path: first,
                            prior_state: CreatedFilePriorState::Absent,
                        },
                        ExternalEffect::CreatedFile {
                            path: second,
                            prior_state: CreatedFilePriorState::Absent,
                        },
                    ]
                );
            }
            other => panic!("failed inverse must quarantine the session: {other:?}"),
        }
    }

    #[test]
    fn reset_retries_pending_inverse_and_keeps_quarantine_when_it_fails() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path = std::env::temp_dir();
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        assert_eq!(
            system
                .compensate_liminal_external_effects_with(&file_ops)
                .len(),
            1
        );
        let error = system
            .reinit_bootstrap()
            .expect_err("reset must refuse while the pending inverse still fails");
        assert_eq!(
            error.error_uri(),
            "delightql-error://runtime/session_health/external_effect"
        );
        assert!(
            error.to_string().contains("delightql-error://"),
            "the compensation URI remains wrapped in the health message: {error}"
        );
        assert!(system.health_incident().is_some());
        match &system.session_health {
            crate::external_effects::SessionHealth::Quarantined(incident) => {
                assert_eq!(incident.pending_effects.len(), 1);
                assert!(incident.message.contains("reset compensation failed"));
            }
            other => panic!("failed reset must preserve quarantine: {other:?}"),
        }
    }

    #[test]
    fn reset_clears_quarantine_after_pending_inverse_and_rebuild_succeed() {
        let mut system = fresh_system();
        let mark = system.max_namespace_id().expect("namespace mark");
        assert!(system
            .begin_liminal_program(mark, LiminalProgramKind::Consult)
            .expect("begin program"));
        let path = std::env::temp_dir().join(format!(
            "dql-reset-recovery-{}-created.db",
            std::process::id()
        ));
        std::fs::write(&path, b"").expect("create recovery fixture");
        system.journal_created_file(path.clone(), CreatedFilePriorState::Absent);
        let file_ops = ScriptedFileOps::new(true, false);

        assert_eq!(
            system
                .compensate_liminal_external_effects_with(&file_ops)
                .len(),
            1
        );
        system
            .reinit_bootstrap()
            .expect("reset should retry and complete the pending inverse");
        assert!(system.health_incident().is_none());
        assert!(
            !path.exists(),
            "successful reset must remove the created file"
        );
    }
}

// =============================================================================
// THE PUBLISHED COMPILER LIMITS
// =============================================================================
#[cfg(test)]
mod compiler_limit_publication_tests {
    use super::DelightQLSystem;
    use crate::compiler_limits::{
        ArmedLimits, CompilerLimit, ProcessLimitLease, ALL, NESTING, REFINEMENT_DEPTH,
    };
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;

    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }

        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> DelightQLSystem {
        let connection = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(connection, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    /// One published row, read back whole.
    #[derive(Debug, PartialEq, Eq)]
    struct Row {
        name: String,
        default_value: i64,
        effective_value: i64,
        hard_ceiling: i64,
        unit: String,
        error: String,
    }

    fn published(system: &DelightQLSystem) -> Vec<Row> {
        let conn = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        let mut statement = conn
            .prepare(
                "SELECT name, default_value, effective_value, hard_ceiling, unit, error
                 FROM compiler_limit ORDER BY rowid",
            )
            .expect("the schema declares the relation");
        let rows = statement
            .query_map([], |row| {
                Ok(Row {
                    name: row.get(0)?,
                    default_value: row.get(1)?,
                    effective_value: row.get(2)?,
                    hard_ceiling: row.get(3)?,
                    unit: row.get(4)?,
                    error: row.get(5)?,
                })
            })
            .expect("read the published policy")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("every column is NOT NULL");
        rows
    }

    fn described(limit: &CompilerLimit, effective: usize) -> Row {
        Row {
            name: limit.name().to_string(),
            default_value: limit.default_value() as i64,
            effective_value: effective as i64,
            hard_ceiling: limit.ceiling() as i64,
            unit: limit.unit().to_string(),
            error: limit.error_identity(),
        }
    }

    /// The catalog says what the guards enforce, FIELD FOR FIELD.
    ///
    /// This is the whole reason the policy is typed once. A default, ceiling,
    /// unit, identity or name that moved on only one side used to be a
    /// disagreement both halves still compiled through; here it is a failure
    /// naming the column.
    #[test]
    fn every_published_row_is_its_runtime_descriptor() {
        let _lease = ProcessLimitLease::take();
        let system = fresh_system();
        let armed = ArmedLimits::from_policy();
        system.publish_compiler_limits(&armed);

        let expected = vec![
            described(&NESTING, armed.nesting().levels()),
            described(&REFINEMENT_DEPTH, armed.refinement().max()),
        ];
        assert_eq!(
            published(&system),
            expected,
            "the catalog and the typed policy are one description"
        );
    }

    /// Every bounded resource has a row, and no row outlives its resource.
    /// A limit added to the typed policy without reaching publication would
    /// leave `compiler_limit(*)` a partial answer to a total question.
    #[test]
    fn the_published_rows_are_exactly_the_bounded_resources() {
        let _lease = ProcessLimitLease::take();
        let system = fresh_system();
        system.publish_compiler_limits(&ArmedLimits::from_policy());

        let published: Vec<String> = published(&system).into_iter().map(|row| row.name).collect();
        let described: Vec<String> = ALL
            .iter()
            .map(|kind| kind.descriptor().name().to_string())
            .collect();
        assert_eq!(published, described);
    }

    /// Publication is idempotent and repairs, rather than accumulating. A
    /// second write of ONE compilation's limits restores the row it already
    /// wrote, and does not add another.
    ///
    /// One `ArmedLimits` for both writes, under the lease: the claim is about
    /// republication, so re-arming between the two would make the comparison
    /// depend on whatever a neighbouring test had stored in the process cells
    /// in that instant.
    #[test]
    fn republishing_repairs_the_row_rather_than_adding_one() {
        let _lease = ProcessLimitLease::take();
        let system = fresh_system();
        let armed = ArmedLimits::from_policy();
        system.publish_compiler_limits(&armed);
        let once = published(&system);

        {
            let conn = system
                .bootstrap_connection()
                .lock()
                .expect("bootstrap lock");
            conn.execute(
                "UPDATE compiler_limit SET hard_ceiling = 1, unit = 'stale', error = 'stale'",
                [],
            )
            .expect("corrupt the published policy");
        }

        system.publish_compiler_limits(&armed);
        assert_eq!(
            published(&system),
            once,
            "a stale ceiling, unit or identity is corrected, not preserved"
        );
    }

    /// THE COMPILATION'S limits are what the catalog reports.
    ///
    /// The registry arms when it is minted; the process moves afterwards. A
    /// publisher that re-read process policy would report the number the
    /// compilation is NOT bounded by, and its reader would have no way to
    /// tell.
    #[test]
    fn the_catalog_reports_the_armed_compilation_not_the_later_process() {
        let _lease = ProcessLimitLease::take();
        // Both settings are at or above their defaults: the cells are
        // process-wide, and a smaller one would refuse whatever else the
        // harness is compiling in this instant.
        assert_eq!(NESTING.set(700).effective(), 700);
        assert_eq!(REFINEMENT_DEPTH.set(1024).effective(), 1024);

        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let names = registry.names();

        assert_eq!(NESTING.set(900).effective(), 900);
        assert_eq!(REFINEMENT_DEPTH.set(2048).effective(), 2048);

        let mut system = fresh_system();
        {
            // A pipeline built on the registry minted under the first
            // setting: constructing it IS the publication.
            let _pipeline = crate::pipeline::Pipeline::new_with_config_and_registry(
                "users(*)",
                &mut system,
                Default::default(),
                crate::pipeline::sql_optimizer::OptimizationLevel::Basic,
                registry,
            );
        }

        let rows = published(&system);
        assert_eq!(
            rows,
            vec![described(&NESTING, 700), described(&REFINEMENT_DEPTH, 1024)],
            "the catalog answers the compilation reading it"
        );
        assert_eq!(
            names.limits().nesting().levels(),
            700,
            "and the compilation is still bounded by what it armed"
        );
        assert_eq!(names.refinement().max(), 1024);
    }
}

// =============================================================================
// A COMPILATION'S EXTENT IS ITS EXECUTION, NOT ITS OBJECT
// =============================================================================
#[cfg(test)]
mod compilation_extent_tests {
    use super::DelightQLSystem;
    use crate::compiler_limits::{ArmedLimits, NestingBudget, ProcessLimitLease, Running, NESTING};
    use crate::names::Registry;
    use crate::pipeline::Pipeline;
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;

    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }

        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> DelightQLSystem {
        let connection = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(connection, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    fn pipeline<'a>(
        system: &'a mut DelightQLSystem,
        registry: crate::relation::Planning,
    ) -> Pipeline<'a> {
        Pipeline::new_with_config_and_registry(
            "users(*)",
            system,
            Default::default(),
            crate::pipeline::sql_optimizer::OptimizationLevel::Basic,
            registry,
        )
    }

    const ARMED: usize = 1000;
    const LATER: usize = 700;

    /// A pipeline kept alive to have its cached stages read is NOT a
    /// compilation in progress. The next compilation built beside it is
    /// independent and must arm from the policy in force now — a retained
    /// object answering for work happening next to it would hand that work a
    /// boundary its host has already moved.
    #[test]
    fn an_independent_compilation_built_beside_a_retained_pipeline_arms_from_policy() {
        let _lease = ProcessLimitLease::take();
        let mut system = fresh_system();

        NESTING.set(ARMED);
        let retained_arena = crate::relation::Planning::open(Registry::new(&[]));
        // The capability goes INTO the pipeline; what the test keeps is the
        // naming handle, which is what these assertions are about.
        let retained_names = retained_arena.names();
        let retained = pipeline(&mut system, retained_arena);
        assert_eq!(retained_names.limits().nesting().levels(), ARMED);

        // The host lowers the boundary. `retained` is alive, but nothing is
        // executing.
        NESTING.set(LATER);
        assert_eq!(
            NestingBudget::current().levels(),
            LATER,
            "a retained pipeline does not answer for work beside it"
        );

        let independent = crate::relation::Planning::open(Registry::new(&[]));
        assert_eq!(
            independent.limits().nesting().levels(),
            LATER,
            "the next compilation gets the next compilation's policy"
        );
        assert!(
            !Rc::ptr_eq(
                &retained_names.limits_shared(),
                &independent.limits_shared()
            ),
            "and does not share the retained compilation's frames"
        );

        drop(retained);
    }

    /// Work RESUMED on that pipeline answers to it again, even with another
    /// arena retained beside it and policy since moved.
    #[test]
    fn work_resumed_on_a_retained_pipeline_answers_to_that_pipeline() {
        let _lease = ProcessLimitLease::take();
        let mut system = fresh_system();

        NESTING.set(ARMED);
        let arena = crate::relation::Planning::open(Registry::new(&[]));
        let arena_names = arena.names();
        let retained = pipeline(&mut system, arena);

        NESTING.set(LATER);
        let _beside = crate::relation::Planning::open(Registry::new(&[]));

        {
            // What every execution method opens for the duration of its work.
            let _running = Running::under(arena_names.limits_shared());
            assert_eq!(NestingBudget::current().levels(), ARMED);

            let refused = crate::ddl::reconstruct::clauses(&ladder())
                .expect_err("past every budget under test");
            assert!(
                refused.error_uri().contains("operational/resource/nesting"),
                "{}",
                refused.error_uri()
            );
            assert!(
                refused.to_string().contains(&ARMED.to_string()),
                "a stored body read during this pipeline's work answers to it: {refused}"
            );
        }

        assert_eq!(
            NestingBudget::current().levels(),
            LATER,
            "and the extent closes with the work"
        );
        drop(retained);
    }

    /// An arena minted while a compilation is EXECUTING is nested work, and
    /// shares that compilation's limits whole.
    #[test]
    fn an_arena_minted_while_a_compilation_executes_is_nested() {
        let _lease = ProcessLimitLease::take();
        let mut system = fresh_system();

        NESTING.set(ARMED);
        let arena = crate::relation::Planning::open(Registry::new(&[]));
        let arena_names = arena.names();
        let outer = pipeline(&mut system, arena);

        let _running = Running::under(arena_names.limits_shared());
        NESTING.set(LATER);

        let nested = crate::relation::Planning::open(Registry::new(&[]));
        assert_eq!(nested.limits().nesting().levels(), ARMED);
        assert!(
            Rc::ptr_eq(&arena_names.limits_shared(), &nested.limits_shared()),
            "nested work spends the running compilation's frames, not a fresh allowance"
        );
        drop(outer);
    }

    /// Past both budgets under test, so the number a refusal states is what
    /// discriminates and nothing walks a tree that deep.
    fn ladder() -> String {
        format!(
            "deep(v) :- users(*) |> ({}age{} as v)",
            "(".repeat(1090),
            ")".repeat(1090)
        )
    }

    /// The armed pair is never half-inherited: a nested arena takes the outer
    /// REFINEMENT allowance too, however far policy has moved.
    #[test]
    fn nested_work_does_not_receive_a_fresh_refinement_allowance() {
        let _lease = ProcessLimitLease::take();
        let mut system = fresh_system();

        crate::compiler_limits::REFINEMENT_DEPTH.set(1024);
        let arena = crate::relation::Planning::open(Registry::new(&[]));
        let arena_names = arena.names();
        let outer = pipeline(&mut system, arena);
        assert_eq!(arena_names.refinement().max(), 1024);

        let _running = Running::under(arena_names.limits_shared());
        crate::compiler_limits::REFINEMENT_DEPTH.set(2048);

        let nested = crate::relation::Planning::open(Registry::new(&[]));
        assert_eq!(
            nested.refinement().max(),
            1024,
            "a re-entry may not be handed the allowance its caller never armed"
        );

        let _frame = arena_names
            .refinement()
            .enter()
            .expect("a frame is affordable");
        assert_eq!(
            nested.refinement().active(),
            1,
            "and the frames are one state, so nested work cannot spend them twice"
        );
        drop(outer);
    }

    /// A compilation that begins with nothing running arms both budgets from
    /// policy — the control for the three pins above.
    #[test]
    fn a_compilation_begun_alone_arms_both_budgets_from_policy() {
        let _lease = ProcessLimitLease::take();
        NESTING.set(LATER);
        crate::compiler_limits::REFINEMENT_DEPTH.set(2048);

        let alone = crate::relation::Planning::open(Registry::new(&[]));
        assert_eq!(alone.limits().nesting().levels(), LATER);
        assert_eq!(alone.refinement().max(), 2048);
        let _ = ArmedLimits::from_policy();
    }
}

/// The bootstrap structural backstop (RULINGS 2026-08-12), pinned on a REAL
/// system: the language's resolved-ownership refusal remains first (pinned
/// by directive_contract 42/45 and effects r03_dml_road_b_engine_owned);
/// what is pinned HERE is the layer beneath it — a deliberately lower-level
/// raw SQL road against the sealed catalog is denied by the authorizer.
#[cfg(test)]
mod bootstrap_guard_pins {
    use super::DelightQLSystem;
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

    fn fresh_system() -> DelightQLSystem {
        let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
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
