// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DelightQL System Management
//!
//! This module provides the `DelightQLSystem` struct which encapsulates
//! the user database connection and the internal _bootstrap metadata store.

use crate::bootstrap::SourceType;
use crate::error::{DelightQLError, Result};
use crate::pipeline::parser::{
    setup_assertions_table_on_bootstrap, setup_danger_table_on_bootstrap,
    setup_errors_table_on_bootstrap, DDLFile,
};
use delightql_types::{
    schema::DatabaseSchema, ConnectionComponents, ConnectionFactory, DatabaseConnection,
};
use log::debug;
use rusqlite::{Connection, OptionalExtension};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Result of a `consult_file` operation.
pub(crate) struct ConsultResult {
    /// Number of definitions loaded.
    pub definitions_loaded: usize,
    /// Entity names that were replaced (non-empty only for inline DDL drop-and-replace).
    pub replaced_entities: Vec<String>,
}

/// One liminal-directive receipt row, headed for the consulted namespace's
/// ledger (EFFECT-ALGEBRA §8, THE LIMINAL RELATION). Collected by the liminal
/// executor (bin_cartridge/prelude/consult.rs) in file-appearance order and
/// persisted by `consult_file_inner` inside the consult transaction. The row
/// schema is `success` (always 1 — session directives never answer NO),
/// `operation` (the directive's name as written, with `!`), then the named
/// echoes per the §8 table; echo VALUES are the arguments as written in the
/// file (receipts echo parameters — compile-time constants, §3).
#[derive(Debug, Clone)]
pub(crate) struct LiminalReceipt {
    /// Directive name as written, with the `!` (e.g. `"enlist!"`).
    pub operation: String,
    /// Echo columns in receipt order: (column name per the §8 table, value
    /// as written — `None` renders as SQL NULL, e.g. `enlist!`'s plain-form
    /// `into` or `reconsult!` re-reading the same file).
    pub echoes: Vec<(String, Option<String>)>,
}

impl LiminalReceipt {
    /// The ordered echo-column names as a JSON array (drives the ledger's
    /// corresponding-union presentation schema at drill time).
    pub fn echoes_json(&self) -> String {
        let names: Vec<&str> = self.echoes.iter().map(|(k, _)| k.as_str()).collect();
        serde_json::to_string(&names).expect("echo names serialize")
    }

    /// The receipt row as a JSON object: success, operation, then echoes.
    pub fn receipt_json(&self) -> String {
        let mut obj = String::from("{\"success\":1,\"operation\":");
        obj.push_str(
            &serde_json::to_string(&self.operation).expect("operation serializes"),
        );
        for (name, value) in &self.echoes {
            obj.push(',');
            obj.push_str(&serde_json::to_string(name).expect("echo name serializes"));
            obj.push(':');
            match value {
                Some(v) => {
                    obj.push_str(&serde_json::to_string(v).expect("echo value serializes"))
                }
                None => obj.push_str("null"),
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

    /// When true, the namespace resolver is authoritative: `Ok(None)` from
    /// `resolve_unqualified_entity` means the entity genuinely isn't enlisted.
    /// When false (pipe/SISO connections), namespace resolution is a stub and
    /// raw database lookup should be used as a fallback.
    pub namespace_authoritative: bool,

    /// Factory for creating connections from URIs (injected by CLI).
    /// Enables import! to handle pipe:// and other URI schemes.
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
    /// discarded — the review's "quiet path"). Pinned by the RED unit test
    /// `seed_no_effect_statement_is_refused`.
    effects_executed: Cell<u64>,

    /// True once `register_run_created_object` has registered anything on a
    /// `session://materialized` cartridge. Gates the shadow-split probe in
    /// qualified resolution (`session_shadow_split`) so sessions that never
    /// ran a DDL directive pay zero extra bootstrap queries. Never reset:
    /// a stale `true` only costs the probe query, never correctness.
    session_materialized_names: Cell<bool>,
}

/// Embedded DQL source for the sys::meta generator HO view.
/// This is the sole definition of the catalog functor join logic.
const SYS_META_SOURCE: &str = include_str!("../autoload/sys/meta.dql");

/// Register a thin catalog wrapper view for a namespace in sys::meta.
///
/// Creates an entity like `main::` with definition `sys::meta.generator("main")(*)`
/// so that `main::(*)` resolves through normal HO view expansion.
/// Register the `sys::help` tables (SYS-HELP-DESIGN.md) so they are
/// addressable as `sys::help.<name>(*)`. Ring 2 (`identifier`) is
/// burned from bootstrap/schema.sql rows; ring 1 (command/option/...)
/// is seeded at session init from the host binary's live surface
/// (`seed_help_surface`). Follows the sys.danger pattern (entity type
/// 10 = physical table on the bootstrap connection), on a dedicated
/// cartridge so nothing else's bulk activation sweeps these entities
/// into a different namespace.
fn register_sys_help_tables(bootstrap_conn: &Connection, bootstrap_conn_id: i64) -> Result<()> {
    bootstrap_conn
        .execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, 'sys://help', NULL, 1, ?3, 0)",
            rusqlite::params![3, SourceType::Db.as_i32(), bootstrap_conn_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to create sys::help cartridge: {}", e),
                e.to_string(),
            )
        })?;
    let help_cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

    let help_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'sys::help'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query sys::help namespace: {}", e),
                e.to_string(),
            )
        })?;

    // (table/entity name, columns as (name, sqlite type, nullable))
    type Col = (&'static str, &'static str, bool);
    let tables: &[(&str, &[Col])] = &[
        (
            "identifier",
            &[
                ("kind", "TEXT", false),
                ("hierarchy", "TEXT", false),
                ("summary", "TEXT", false),
                ("explanation", "TEXT", false),
            ],
        ),
        (
            "command",
            &[
                ("name", "TEXT", false),
                ("parent", "TEXT", true),
                ("alias", "TEXT", true),
                ("summary", "TEXT", false),
            ],
        ),
        (
            "option",
            &[
                ("command", "TEXT", false),
                ("long", "TEXT", false),
                ("short", "TEXT", true),
                ("value_name", "TEXT", true),
                ("default_value", "TEXT", true),
                ("global", "INTEGER", false),
                ("repeatable", "INTEGER", false),
                ("summary", "TEXT", false),
            ],
        ),
        (
            "option_value",
            &[
                ("command", "TEXT", false),
                ("option", "TEXT", false),
                ("value", "TEXT", false),
                ("summary", "TEXT", true),
                ("class", "TEXT", true),
                ("grade", "TEXT", true),
            ],
        ),
        (
            "dot_command",
            &[("name", "TEXT", false), ("summary", "TEXT", false)],
        ),
        (
            "env",
            &[
                ("name", "TEXT", false),
                ("effect", "TEXT", false),
                ("equivalent_flag", "TEXT", true),
            ],
        ),
        (
            "man_page",
            &[
                ("name", "TEXT", false),
                ("section", "INTEGER", false),
                ("troff", "TEXT", false),
                ("plain", "TEXT", false),
            ],
        ),
        (
            "exit_code",
            &[
                ("code", "INTEGER", false),
                ("context", "TEXT", false),
                ("meaning", "TEXT", false),
                ("class", "TEXT", true),
                ("grade", "TEXT", true),
            ],
        ),
    ];

    for (table, columns) in tables {
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id) VALUES (?1, 10, ?2)",
                rusqlite::params![table, help_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys::help.{} entity: {}", table, e),
                    e.to_string(),
                )
            })?;
        let entity_id = bootstrap_conn.last_insert_rowid() as i32;

        bootstrap_conn
            .execute(
                "INSERT INTO entity_clause (entity_id, ordinal, definition)
                 VALUES (?1, 1, '-- sys::help burned/seeded table (SYS-HELP-DESIGN.md)')",
                rusqlite::params![entity_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to insert sys::help.{} clause: {}", table, e),
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
                            "Failed to insert sys::help.{} column '{}': {}",
                            table, col_name, e
                        ),
                        e.to_string(),
                    )
                })?;
        }

        bootstrap_conn
            .execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                rusqlite::params![entity_id, help_ns_id, help_cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to activate sys::help.{}: {}", table, e),
                    e.to_string(),
                )
            })?;
    }

    Ok(())
}

/// Register the CURATED `connection` entity in sys::connections.
///
/// Register the `connection` entity in sys::connections as an explicit column
/// ALLOWLIST. Under the credential-sourcing policy (credentials come from the
/// environment, never embedded in a URI — SYS-NAMESPACE-TAXONOMY.md) no column
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
fn register_sys_connection_table(bootstrap_conn: &Connection, bootstrap_conn_id: i64) -> Result<()> {
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
             VALUES (?1, 1, '-- sys::connections curated safe-subset (SYS-NAMESPACE-TAXONOMY.md)')",
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

fn register_catalog_wrapper(
    conn: &Connection,
    ns_fq: &str,
    sys_meta_ns_id: i32,
    cartridge_id: i32,
) -> Result<()> {
    let entity_name = format!("{}::", ns_fq);
    let definition = format!(r#"_(*) :- sys::meta.generator("{}")(*)"#, ns_fq);

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

/// RAII rollback for the imprint target transaction.
///
/// Holds a shared reference to the target connection (behind its `MutexGuard`)
/// and, unless `committed` is flipped, issues `ROLLBACK` on drop. This makes
/// every `?` early-return in the drop/create/CTAS sequence undo the whole
/// materialization automatically, so replace-mode cannot leave the old tables
/// destroyed with nothing in their place (pinned: `cli tests/imprint_atomicity.rs`;
/// review C1). `execute` takes `&self`, so re-borrowing the guarded connection
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
/// is silent — review finding 8). Parsing the N and taking `MAX+1` is monotone
/// regardless of gaps. A failed query is a loud error (`?`), never the silent 0
/// the old `.unwrap_or(0)` produced. The `GLOB` is a coarse pre-filter; the
/// Rust parse below is authoritative. Pinned by
/// `imprint_version_tests::next_blueprint_version_is_max_plus_one`.
//
// NOTE: a UNIQUE index on `namespace.fq_name` would make the collision
// impossible at the schema level; deferred behind a mini-audit (plan Change 2).
fn next_blueprint_version(conn: &Connection, target_ns_id: i32) -> Result<i64> {
    let mut stmt = conn
        .prepare(
            "SELECT name FROM namespace WHERE pid = ?1 AND name GLOB '_[0-9]*_blueprint'",
        )
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
        if let Some(inner) = name.strip_prefix('_').and_then(|s| s.strip_suffix("_blueprint")) {
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
/// `imprint!` is linear (namespace-catechism §V): after a successful
/// materialization the source is *moved, not destroyed* to
/// `{target}::_{N}_blueprint`. The move is a rename/re-parent of the source
/// namespace (and its `_internal`/descendants), which both vacates the original
/// path — so use-after-imprint errors and the path is free to re-consult
/// (D1: delete-and-reuse) — and creates the archive. The archive is visible
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
    // D3: version N = MAX(existing N)+1 over `_N_blueprint` children (loud on
    // query failure). See `next_blueprint_version` for why not COUNT.
    let n = next_blueprint_version(conn, target_ns_id)?;
    let bp_name = format!("_{}_blueprint", n);
    let bp_fq = format!("{}::{}", target_ns, bp_name);

    // Descendants (e.g. `_internal`), captured before renaming so we can rewrite
    // their fq_names / cartridges / catalog wrappers. Membership comes from an
    // EXACT string-prefix test in Rust (`starts_with("{src}::")`), NOT
    // `fq_name LIKE '{src}::%'`: `_`/`%` in a namespace name are LIKE
    // wildcards, so the old pattern kidnapped unrelated siblings (imprinting
    // `lib::a_b` also matched `lib::acb::…`), silently renaming them under the
    // blueprint (M3). Pinned by companion_linear--69 (sibling untouched) and
    // --72 (descendants DO move). NOT a pid-recursive walk either: today every
    // consult-created namespace has pid = NULL (the hierarchy lives only in
    // fq_name), so a pid walk finds nothing and silently strands descendants
    // (`_internal`, nested consults) live at their old paths — the vacate half
    // of linearity breaks and re-consulting mints duplicate fq_names. If/when
    // pid becomes a real tree (namespace-catechism work), a pid walk can
    // replace this.
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
    // `?`-loud (was `let _ =`): consume runs inside imprint's bootstrap txn,
    // which rolls back cleanly on any error (Change 1), so a failed wrapper
    // cleanup now aborts the whole catalog update instead of leaving a
    // half-renamed blueprint behind.
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
                format!("descendant '{}' is not under source '{}'", old_fq, source_ns),
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
    // the old `WHERE to_namespace_id = ?` deleted nothing: an enlisted source's
    // archived rules stayed resolvable UNQUALIFIED after imprint (M1 — the former
    // "no unqualified leak" comment claimed the opposite of what the code did).
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
/// the enforcement half of "visible-but-INERT" (namespace-catechism §V D2;
/// M2). `imprint!` consumes a source namespace into `{target}::_N_blueprint`,
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
/// silently executing archived rules (review caught the function route
/// bypassing the first single-door design).
pub(crate) fn blueprint_shadowing(conn: &Connection, fq_name: &str) -> Result<Option<String>> {
    let mut stmt = conn
        .prepare("SELECT fq_name FROM namespace WHERE kind = 'blueprint'")
        .map_err(|e| {
            DelightQLError::database_error("prepare blueprint inertness scan", e.to_string())
        })?;
    let blueprints = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| {
            DelightQLError::database_error("scan blueprint namespaces", e.to_string())
        })?;
    for bp in blueprints {
        let bp = bp.map_err(|e| {
            DelightQLError::database_error("read blueprint fq_name", e.to_string())
        })?;
        if fq_name == bp || fq_name.starts_with(&format!("{}::", bp)) {
            return Ok(Some(bp));
        }
    }
    Ok(None)
}

/// Which of imprint!'s two verbs is running. Replaces the former
/// `imprint_namespace(…, replace: bool)` positional flag so the surface's two
/// verbs are named at the call site (review finding 9, interface hygiene).
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
/// strict-clash / replace-drop pre-flight (review finding 10). Pinned by
/// `system::imprint_helper_tests::clash_probe_sees_temp_object`.
pub(crate) fn imprint_clash_probe_sql(master: &str, name_lit: &str) -> String {
    format!(
        "SELECT type FROM {m} WHERE name = '{n}' \
         UNION ALL SELECT type FROM sqlite_temp_master WHERE name = '{n}'",
        m = master,
        n = name_lit
    )
}

/// The engine's OWN default schema for a fatboy `connection_type` — the
/// downstream resolution of a NULL (bare-mount) `source_ns` (schema-mount
/// Phase A; the read side of `fatboy_exec.rs::default_schema`, which is the
/// write side the introspector uses). 3 = postgres → `public`, 4 = duckdb →
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

/// The introspection SQL `register_run_created_object` routes to a created
/// object's OWN connection, selected by that connection's dialect (E-T4;
/// P2 "what breaks first" item 7). Answers `(sql, name_col, type_col)` —
/// the 0-based row positions of the column name and its engine type.
///
/// - **SQLite** (and every dialect without a specific arm): `PRAGMA
///   table_info` verbatim — the pre-E-T4 spelling, byte-identical.
/// - **DuckDB**: PRAGMA table_info is KEPT, deliberately: it works there
///   (REPORT-T-P3 §H wrinkle 7), its boolean-shaped `notnull`/`pk` columns
///   sit at positions this parse never reads (name = 1, type = 2), and the
///   information_schema alternative would need CATALOG scoping (temp
///   objects live in catalog `temp`, durable in the file's basename
///   catalog — P3 §B/§E) for zero gain. Pinned by
///   `duckdb_readback_keeps_pragma_and_tolerates_the_boolean_shape`.
/// - **Postgres**: the information_schema form, mirroring the fatboy
///   mount's own working introspection (fatboy_exec.rs `introspect_sql` —
///   the proven pattern over the relay), scoped to ONE schema: the
///   session's temp schema when the name is temp-held (the shadow
///   preference resolution itself applies — materialize-pipe §6, P1 §B),
///   else the MOUNTED schema. `None` when the mounted schema is not
///   derivable — the caller abstains (registers nothing) rather than
///   guess. Pinned by
///   `pg_readback_routes_information_schema_sql_to_the_objects_connection`
///   and `system::readback_sql_tests`. E-T5's live lane must confirm
///   information_schema.columns lists the session's own temp tables on PG
///   (compile-only here — effects on fatboys are struck).
pub(crate) fn created_object_readback_sql(
    dialect: crate::pipeline::generator_v3::SqlDialect,
    name: &str,
    mounted_schema: Option<&str>,
) -> Option<(String, usize, usize)> {
    match dialect {
        crate::pipeline::generator_v3::SqlDialect::PostgreSQL => {
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
        _ => Some((format!("PRAGMA table_info({})", quote_ident(name)), 1, 2)),
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
/// name pool (namespace-catechism Deviation #3, re-ruled 2026-07-07).
///
/// The top level of the namespace tree stays OPEN to user names — `consult!`
/// and `mount!` mint exactly where they say. What this refuses, loudly and
/// badged (`error://namespace/name/...`):
///
///   (a) a top-level name that IS a bare system name (`sys`/`std`/`main`/
///       `home`) — creating AS it, or taking it over (e.g. `mount!(…,"home")`);
///   (b) a top-level name PREFIXED `sys`/`std`, case-insensitive (`sysinfo`,
///       `stdlib`, `std2`, `SYS_foo`) — the system's room to mint future
///       siblings. Exact `main`/`home` are NOT prefix rules, so `maintenance`
///       and `homework` stay legal;
///   (c) ANY segment beginning `_` (`_internal`, `_N_blueprint`) — the system
///       machinery convention, formally reserved EVERYWHERE (including under
///       `home`);
///   (d) creation UNDER `sys::`/`std::` — the system subtree.
///
/// Relaxations, all per the re-ruling:
///   * Creating UNDER `home` is the user's right (`consult!(…,"home::x")`), and
///     inside `home` the prefix prong (b) relaxes — `home::sysinfo` is yours.
///     Only prong (c) stays strict there.
///   * Creating UNDER `main` (`main::x`) is §II's kind/fidelity contract
///     (Deviation #4), not this guard's business — left alone (prong (c)'s
///     `_` check still applies, since machinery names are reserved everywhere).
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

    // `main` is the primary DATA namespace — governed by §II's kind/fidelity
    // contract (Deviation #4), NOT this name guard. Both the bare form and the
    // `main::x` subtree are left alone here:
    //   * bare `main`: the sanctioned primary-data bind. The CLI establishes
    //     every session with `mount!("<db>", "main")` (commands/query.rs); the
    //     harness-generated primary mount is textually a user mount and routes
    //     through this guard, so refusing bare `main` would break every session.
    //   * `main::x`: creating under main is Deviation #4's business.
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
    // taking it over (e.g. `mount!(…, "home")`). `main` is exempt (handled
    // above — it is the data namespace, Deviation #4); `home` reaches here only
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
/// consulting the enlist set — the MIDDLE access rung of namespace-catechism
/// §IV ("`chutzpah.shout` — plain qualifier; home resolves first"). Enlisting a
/// namespace makes its child namespaces *plain-addressable*: once `home` is
/// enlisted, `chz` alone resolves to `home::chz`.
///
/// Contract — the precedence is fixed, do not reorder:
///   1. `path` is a namespace path that ALREADY MISSED exact resolution. This
///      is consulted ONLY on a miss, so every path that resolves today — full
///      names, top-level names, table aliases, `table.column` refs — is
///      structurally untouched (§IV precedence rule 1: existing resolution
///      always wins; the expansion is pure addition).
///   2. HOME FIRST (§IV): if `home::{path}` exists, it wins outright.
///   3. Otherwise exactly one non-home enlisted parent may match → use it;
///      MULTIPLE non-home matches → loud badged ambiguity
///      (`namespace/plain/ambiguous`), listing the candidate fqs.
///   4. NON-TRANSITIVE (rule 4): only DIRECT children of enlisted namespaces
///      are tried. Multi-segment paths never expand — enlisting `home` grants
///      `home::chz` (a child), never `home::a::b` (a grandchild). One prefix
///      join per parent, no recursion.
///
/// The enlist set is `enlisted_namespace` joined to its `to = 'main'` session
/// scope (Deviation #7's magic target). The returned fq re-enters the normal
/// resolution path at every call site, so the blueprint-inertness guard applies
/// to the expanded fq unchanged.
pub(crate) fn expand_plain_namespace(conn: &Connection, path: &str) -> Result<Option<String>> {
    // Rule 4, non-transitive: only a single-segment child qualifier expands. A
    // `::` in the path means the user reached PAST a direct child; enlisting
    // never leaks a grandchild, so such a path is left to miss.
    if path.contains("::") {
        return Ok(None);
    }

    // The enlisted parents (from_namespace) for this session scope (to='main').
    let mut stmt = conn
        .prepare(
            "SELECT p.fq_name
             FROM enlisted_namespace en
             JOIN namespace p ON p.id = en.from_namespace_id
             JOIN namespace target ON target.id = en.to_namespace_id
             WHERE target.fq_name = 'main'",
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

    // Rule 2, HOME FIRST (§IV): a home child beats every other enlisted parent.
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

/// The SHADOW half of §IV's plain-qualifier rung — the ratified SOFTENING of
/// "home resolves first". §IV's letter says home wins; strict home-first would
/// let a scratch `home::chz` SILENTLY shadow a pre-existing top-level `chz`,
/// changing a query that resolves today (a silent-wrong). The ruling keeps
/// resolution monotonic: an existing top-level namespace wins (precedence rule
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
    //! The system name guard (catechism Deviation #3, re-ruled 2026-07-07).
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
        // main is the primary DATA namespace (Deviation #4), not the name
        // guard's business — and the CLI binds every session with
        // mount!("<db>","main").
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
    //! The middle access rung of namespace-catechism §IV: plain-qualifier
    //! expansion over the enlist set. `expand_plain_namespace` is a pure
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
        // 'main' is always present (the session scope).
        let mut all: Vec<&str> = vec!["main"];
        all.extend_from_slice(namespaces);
        for (i, fq) in all.iter().enumerate() {
            c.execute(
                "INSERT OR IGNORE INTO namespace (id, fq_name) VALUES (?1, ?2)",
                rusqlite::params![i as i64 + 1, fq],
            )
            .unwrap();
        }
        let id_of = |fq: &str| -> i64 {
            c.query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [fq],
                |r| r.get(0),
            )
            .unwrap()
        };
        let main_id = id_of("main");
        for e in enlisted {
            c.execute(
                "INSERT INTO enlisted_namespace (from_namespace_id, to_namespace_id) VALUES (?1, ?2)",
                rusqlite::params![id_of(e), main_id],
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
        // home wins (§IV "home resolves first").
        let c = fixture(
            &["home", "home::dup", "wh", "wh::dup"],
            &["home", "wh"],
        );
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
    //! Blueprint versioning (review finding 8): the version N chosen for a new
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
    //! Identifier quoting for the imprint DDL path (review M5). The schema
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
        // review finding 10: a temp object is invisible to sqlite_master but
        // must still register as a clash. The probe UNIONs sqlite_temp_master.
        let c = Connection::open_in_memory().unwrap();
        c.execute_batch("CREATE TEMP TABLE foo (x INTEGER)").unwrap();

        // sqlite_master alone misses it (the pre-fix blind spot)...
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
    //! E-T4: the registration read-back's per-dialect SQL SELECTION
    //! (EFFECTS-ON-TARGETS-PLAN §3; P2 "what breaks first" item 7).
    //! Compile-only — routing and shape tolerance are pinned in
    //! effect_transformer/tests.rs; live behavior is E-T5's lane.
    use super::created_object_readback_sql;
    use crate::pipeline::generator_v3::SqlDialect;

    #[test]
    fn readback_sql_is_pragma_table_info_on_sqlite_and_duckdb() {
        for dialect in [SqlDialect::SQLite, SqlDialect::DuckDB] {
            let (sql, name_col, type_col) =
                created_object_readback_sql(dialect, "staged", None)
                    .expect("sqlite/duckdb read-back never abstains");
            assert_eq!(sql, "PRAGMA table_info(\"staged\")");
            // name at 1, type at 2 — DuckDB's boolean-shaped notnull/pk
            // (positions 3/5, P3 H7) are never read.
            assert_eq!((name_col, type_col), (1, 2));
        }
    }

    #[test]
    fn readback_sql_on_postgres_scopes_the_mounted_schema_and_prefers_temp() {
        let (sql, name_col, type_col) =
            created_object_readback_sql(SqlDialect::PostgreSQL, "dur", Some("public"))
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
            created_object_readback_sql(SqlDialect::PostgreSQL, "a'b", Some("public")).unwrap();
        assert!(evil.contains("'a''b'"), "{evil}");
        assert!(!evil.contains("= 'a'b'"), "{evil}");
    }

    #[test]
    fn readback_abstains_on_postgres_without_a_mounted_schema() {
        assert!(
            created_object_readback_sql(SqlDialect::PostgreSQL, "dur", None).is_none(),
            "an unknowable mounted schema abstains (never a guessed schema)"
        );
    }
}

#[cfg(test)]
mod schema_mount_recording_tests {
    //! schema-mount Phase A (EFFECTS-ON-TARGETS-PLAN §4.1): the mounted
    //! engine schema is a RECORDED per-mount fact — the cartridge's
    //! `source_ns`, fed by `ConnectionComponents.mounted_schema` — not a
    //! connection-type derivation. A mount given a specific schema records
    //! THAT schema (and introspects it); a bare mount records NULL and the
    //! namespace-keyed lookup resolves the engine default downstream, so it
    //! is behavior-identical to the pre-Phase-A derivation.
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

    /// A mount given a SPECIFIC schema records it as `source_ns`; the
    /// namespace-keyed lookup reads that recorded fact VERBATIM (type 3 would
    /// have DERIVED `public`); and the mount introspected THAT schema's
    /// entity. Red before Phase A: `mounted_engine_schema_for_connection`
    /// derived `public` from connection_type, ignoring the recorded schema.
    #[test]
    fn mount_records_the_schema_as_source_ns_and_the_lookup_reads_it() {
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
            system.mounted_engine_schema_for_connection(conn_id).unwrap(),
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

    /// A BARE mount (mounted_schema None) records a NULL `source_ns`; the
    /// lookup falls back to the engine default for the connection type
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
            "a NULL source_ns resolves the engine default downstream"
        );
        assert_eq!(
            system.mounted_engine_schema_for_connection(conn_id).unwrap(),
            Some("public".to_string()),
            "the connection shim agrees with the namespace lookup"
        );
        // The default introspection discovered the default schema's entity.
        assert!(
            system
                .get_canonical_entity_name("plain", "in_public")
                .unwrap()
                .is_some()
        );
    }
}

/// Validate value-function clause discipline for a multi-clause definition —
/// the FUNCTIONAL half of "The Two Algebras" (clause-head-catechism.md §II;
/// DDL-CLAUSE-ALGEBRA-ANALYSIS.md RULE 2).
///
/// # The fork this function sits on
///
/// A colon-functor `f:(…)` is a VALUE FUNCTION: the grammar parses it as
/// `function_definition`/`constant_definition` → `DdlHead::Function` → entity
/// type 1 (`DqlFunctionExpression`). Its clauses are ORDERED first-match
/// alternatives, compiled to a CASE expression, BECAUSE a function is
/// deterministic — it must return exactly one value per input.
///
/// A plain-functor `f(…)` (with a boolean body) is a SIGMA PREDICATE: the
/// grammar parses it as `sigma_definition` → `DdlHead::SigmaPredicate` → entity
/// type 9 (`DqlTemporarySigmaRule`). Its clauses OR together (the RELATIONAL
/// algebra — independent truths about membership) and are expanded by
/// `resolver::resolving::predicates::expand_consulted_sigma`, never here.
///
/// The two are DISTINCT ENTITY TYPES decided SYNTACTICALLY at parse time (the
/// colon), NOT one definition used context-dependently. So gating this check on
/// `DdlHead::Function` is exact: sigma predicates never reach it, and the
/// multi-clause sigma OR (ddl/320) is untouched.
///
/// # The rules
///
/// - **Rule 3 (RULE 2 / unguarded multiplicity):** at most ONE unguarded
///   clause. Two or more are indistinguishable — nothing selects between them,
///   and the CASE synthesizer (`grounding::build_case_body_from_clauses`) would
///   emit the degenerate `CASE ELSE <last> END` (zero WHEN arms → unexecutable
///   SQL). Fires even when NO clause is guarded — the previously-ungoverned
///   all-unguarded case (the defect this rule fixes). Also catches duplicate
///   constants (`nl :- …` twice: a constant is a zero-arity value function).
/// - **Rule 4 (unguarded position):** the single unguarded clause is the
///   default/ELSE and must be last. Guarded clauses are tried first-match; the
///   default is the fallthrough.
///
/// Badging note: Rule 3 uses `ddl/head/unguarded_multiplicity` (mirrors the
/// `semantic/ddl/head/` sibling family — `arity`, `name_conflict`,
/// `unnamed_ground_position`). Rule 4 still carries the generic `parse/general`
/// badge; RULE 3 of the analysis will rebadge it to `ddl/head/unguarded_position`
/// — this function is the pattern that rebadge will follow.
fn validate_function_clause_discipline(
    defs: &[crate::pipeline::asts::ddl::DdlDefinition],
) -> Result<()> {
    use crate::pipeline::asts::ddl::DdlHead;

    // Fork gate: only value functions (colon-functors). Sigma predicates and any
    // non-function head are validated on their own paths.
    let Some(first) = defs.first() else {
        return Ok(());
    };
    if !matches!(first.head, DdlHead::Function { .. }) {
        return Ok(());
    }

    // A clause is "unguarded" when none of its params carries a guard (a
    // constant's empty param list is vacuously unguarded). A non-Function
    // clause among Function clauses is treated as unguarded here, but the
    // same-kind check upstream (Rule 1) already refuses that mix.
    let unguarded_indices: Vec<usize> = defs
        .iter()
        .enumerate()
        .filter(|(_, d)| match &d.head {
            DdlHead::Function { params, .. } => params.iter().all(|p| p.guard.is_none()),
            _ => true,
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
                first.name,
                unguarded_indices.len(),
                first.name,
                first.name,
            ),
            "Unguarded clause multiplicity",
        ));
    }

    // Rule 4: the single unguarded (default) clause must be last.
    // Badged into the ddl/head family beside unguarded_multiplicity
    // (clause-algebra RULE 3 rebadge — was a generic parse_error).
    if let Some(&idx) = unguarded_indices.first() {
        if idx != defs.len() - 1 {
            return Err(DelightQLError::validation_error_categorized(
                "ddl/head/unguarded_position",
                format!(
                    "Disjunctive definition '{}': unguarded clause is at position {} \
                     but must be the last clause (position {}). \
                     Move the default clause to the end.",
                    first.name,
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
/// the sibling of `validate_function_clause_discipline` (the RULE 2
/// precedent; IMPLEMENTATION-PLAN §2.2). Covers the per-definition
/// structural rules of EFFECT-ALGEBRA.md:
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
///   CTE) is DEFERRED — the runner has no warning channel (plan §2.2 F4).
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
    defs: &[crate::pipeline::asts::ddl::DdlDefinition],
) -> Result<Option<(String, Vec<String>)>> {
    use crate::pipeline::asts::ddl::{DdlBody, DdlHead};
    use crate::pipeline::asts::effects;

    let Some(first) = defs.first() else {
        return Ok(None);
    };

    // --- Pure heads: R1 (purity). ---
    if !matches!(first.head, DdlHead::EffectRule { .. }) {
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
                            def.name, inv.name, def.name
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
    if first.name == "main!" && defs.len() > 1 {
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

    let rule = effects::EffectRule::from_ddl_definitions(&first.name, defs)?;
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
        for cte in &clause.body.ctes {
            if cte.demands_directive && !cte.effect_marked {
                return Err(DelightQLError::validation_error_categorized(
                    "effect/cte/label",
                    format!(
                        "effect rule '{}': the CTE '{}' demands a directive, so \
                         its label must be '!'-marked — write ': {}!' \
                         (EFFECT-ALGEBRA R4).",
                        rule.name, cte.name, cte.name
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
/// (EFFECT-ALGEBRA R6). Checked over the effect rules of one consulted file
/// (cross-file cycles are impossible: an already-registered rule was
/// validated against the rules that existed at ITS registration, which
/// cannot include this file's). Pinned by the effects ball
/// (rules--28_r6_recursion: "must not recurse").
fn validate_effect_rule_recursion(edges: &[(String, Vec<String>)]) -> Result<()> {
    use std::collections::HashMap;
    let graph: HashMap<&str, &Vec<String>> =
        edges.iter().map(|(n, d)| (n.as_str(), d)).collect();

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
    // Shared DDL front end (DDL-LOADING-PATHS.md Tier 1).
    let ddl = crate::bin_cartridge::prelude::consult::parse_ddl_source_no_directives(
        SYS_META_SOURCE,
        "sys::meta",
    )?;
    let count = ddl.definitions.len();
    DelightQLSystem::consult_file_inner(
        bootstrap_conn,
        "embedded://sys::meta",
        "sys::meta",
        ddl,
        count,
        None,
        // Embedded system modules have no liminal space (§8: created by
        // other means — empty liminal).
        &[],
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

    // Auto-enlist sys::meta into main
    let main_ns_id: i32 = bootstrap_conn
        .query_row(
            "SELECT id FROM namespace WHERE fq_name = 'main'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to query main namespace for enlist: {}", e),
                e.to_string(),
            )
        })?;

    bootstrap_conn
        .execute(
            "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
             VALUES (?1, ?2)",
            [sys_meta_ns_id, main_ns_id],
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to enlist sys::meta into main: {}", e),
                e.to_string(),
            )
        })?;

    // Auto-enlist `home` into main (catechism §I/§IV: home is one of the five
    // session-start enlistments — "everything you author in-session" is bare
    // because home is enlisted). Uses the same magic `to = main` target as the
    // rest of session bootstrap (Deviation #7); mirror the sys::meta idiom above.
    if let Ok(home_ns_id) = bootstrap_conn.query_row(
        "SELECT id FROM namespace WHERE fq_name = 'home'",
        [],
        |row| row.get::<_, i32>(0),
    ) {
        bootstrap_conn
            .execute(
                "INSERT OR IGNORE INTO enlisted_namespace (from_namespace_id, to_namespace_id)
                 VALUES (?1, ?2)",
                [home_ns_id, main_ns_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to enlist home into main: {}", e),
                    e.to_string(),
                )
            })?;
    }

    debug!(
        "register_catalog_views: Registered {} catalog wrappers, enlisted sys::meta + home into main",
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
        return Ok(id);
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
    Ok(())
}

impl DelightQLSystem {
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

        // Create session tables on bootstrap (assertions, danger, errors)
        setup_assertions_table_on_bootstrap(&bootstrap_conn)?;
        setup_danger_table_on_bootstrap(&bootstrap_conn)?;
        setup_errors_table_on_bootstrap(&bootstrap_conn)?;

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
            if db_type_lower == "sqlite" { "in-process" } else { "fatboy" },
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
        // No user cartridge, no introspection — the CLI sends mount!("path", "main")
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

        // sys::help ring 2 (SYS-HELP-DESIGN.md phase 1): register the
        // burned identifier table (rows authored in bootstrap/schema.sql)
        // as sys::help.identifier. Its own cartridge so the bulk
        // activation above cannot leak it into bare `sys`.
        register_sys_help_tables(&bootstrap_conn, bootstrap_conn_id)?;

        // sys::connections: the curated safe-subset `connection` entity
        // (non-secret columns only). Own cartridge so the bulk activation
        // above cannot leak it into bare `sys`.
        register_sys_connection_table(&bootstrap_conn, bootstrap_conn_id)?;

        // Initialize connection routing map
        let mut connection_map: HashMap<i64, Arc<Mutex<dyn DatabaseConnection>>> = HashMap::new();
        connection_map.insert(user_conn_id, Arc::clone(&connection)); // User connection

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
        };

        // Eagerly load stdlib DQL overlays for universal (auto-enlisted) namespaces
        for ns in &universal_namespaces {
            system.ensure_stdlib_loaded(ns);
        }

        Ok(system)
    }

    /// Create a new DelightQL system with injected database schema (Phase 2)
    ///
    /// This is the preferred constructor after Phase 2 refactor. It accepts
    /// a database schema implementation via dependency injection, allowing
    /// core to remain database-agnostic.
    ///
    /// # Arguments
    /// * `connection` - Database connection (created by backend/CLI)
    /// * `introspector` - Backend-specific introspector for discovering schema
    /// * `db_type` - Database type string ("sqlite", "duckdb", "postgres")
    /// * `schema` - Database schema implementation (created by CLI)
    ///
    /// # Returns
    /// A DelightQLSystem ready for query execution with schema support
    ///
    /// # Example
    /// ```ignore
    /// Get the injected database schema (Phase 2)
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

    /// Get a reference to the bootstrap connection (for session tables: assertions, danger, errors).
    pub fn bootstrap_connection(&self) -> &Arc<Mutex<Connection>> {
        &self.bootstrap_connection
    }

    /// The SQL dialect of the connection a query routes to — the
    /// dialect-from-connection inference (ALL-SQL-TARGETING). `None` or the
    /// user connection (id 2) resolve to the PRIMARY's db_type (so a
    /// `--db fatboy://postgres/...` primary compiles postgres-spelled SQL);
    /// mounted connections resolve via their `connection` row:
    /// connection_type 3 = postgres, 4 = duckdb, pipe (6) parses the
    /// `pipe://<profile>/...` profile. Anything unknown is canonical SQLite.
    pub fn dialect_for_connection(
        &self,
        connection_id: Option<i64>,
    ) -> crate::pipeline::generator_v3::SqlDialect {
        use crate::pipeline::generator_v3::SqlDialect;
        let primary = || {
            SqlDialect::from_family_name(&self.db_type.to_lowercase())
                .unwrap_or(SqlDialect::SQLite)
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

    /// E-T1 plan-to-connection attribution (EFFECTS-ON-TARGETS-PLAN §3):
    /// the connection id of the session's `main` mount, when that mount is
    /// a FATBOY-backed engine (connection_type 3 = postgres, 4 = duckdb;
    /// namespace.source_path ↔ connection.resource_uri, both written by
    /// `register_external_connection`).
    /// This is the None-plan settling road: an effect plan whose walk
    /// resolved NO connection executes wherever the user pointed dql — the
    /// main mount — per R-T1 ("one plan, one engine"), instead of silently
    /// converging on the in-memory SQLite hub. Deliberately scoped to the
    /// fatboy types: SQLite ATTACH mains live ON the hub (convergence is
    /// correct there), and pipe/siso mains keep today's hub convergence
    /// untouched (T0's scope — cross_test lanes unaffected).
    ///
    /// NOTE: this helper is ATTRIBUTION — it survived E-T5's deletion of
    /// the T0 strike (and of `fatboy_engine_for_effect_plan`) because it
    /// answers WHERE a None-plan executes, not whether it may. Pinned by
    /// `anon_source_plan_with_fatboy_main_stamps_the_main_connection`
    /// (pipeline/effect_transformer/tests.rs).
    pub fn fatboy_main_connection_for_effect_plan(&self) -> Option<i64> {
        let conn = self.bootstrap_connection.lock().ok()?;
        conn.query_row(
            "SELECT co.id FROM connection co
             WHERE co.resource_uri =
                   (SELECT source_path FROM namespace WHERE fq_name = 'main')
               AND co.connection_type IN (3, 4)
             LIMIT 1",
            [],
            |r| r.get(0),
        )
        .ok()
    }

    /// E-T5 siso refusal support (EFFECTS-ON-TARGETS-PLAN §3 E-T5, RULED
    /// 2026-07-11, PERMANENT): is this connection a siso/pipe mount
    /// (connection_type 6)? Effect plans that settle on one refuse at
    /// compile — the siso transport is error-blind, so R-T3's
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
    /// Used by import! when a ConnectionFactory is available (for pipe://, file://, etc.).
    ///
    /// Returns (connection_id, entity_count) on success.
    pub fn register_external_connection(
        &mut self,
        components: ConnectionComponents,
        namespace: &str,
        connection_uri: &str,
    ) -> Result<(i64, usize)> {
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
            let existing: Option<String> = match bootstrap_conn.query_row(
                "SELECT c.source_uri FROM namespace n
                 JOIN activated_entity ae ON ae.namespace_id = n.id
                 JOIN entity e ON e.id = ae.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE n.fq_name = ?1
                 LIMIT 1",
                [namespace],
                |row| row.get(0),
            ) {
                Ok(uri) => Some(uri),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    empty_namespace_id = bootstrap_conn
                        .query_row(
                            "SELECT id FROM namespace WHERE fq_name = ?1",
                            [namespace],
                            |row| row.get(0),
                        )
                        .ok();
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
                    // a conflict (URI-DESIGN.md §4, connect-before-dedupe —
                    // the new connection is already live, so its identity
                    // is in hand).
                    let existing_identity: Option<String> = bootstrap_conn
                        .query_row(
                            "SELECT co.identity FROM namespace n
                             JOIN activated_entity ae ON ae.namespace_id = n.id
                             JOIN entity e ON e.id = ae.entity_id
                             JOIN cartridge c ON c.id = e.cartridge_id
                             JOIN connection co ON co.id = c.connection_id
                             WHERE n.fq_name = ?1
                             LIMIT 1",
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

        // Install as a cartridge, RECORDING the mounted engine schema as
        // this cartridge's `source_ns` (schema-mount Phase A,
        // EFFECTS-ON-TARGETS-PLAN §4.1). `None` — a bare mount — stays NULL,
        // meaning "the engine's own default", resolved downstream by
        // `mounted_engine_schema_for_namespace`; reads therefore stay
        // unqualified, behavior-identical to the pre-Phase-A NULL. A specific
        // schema (Phase B `#schema` / Phase C `mount_tree!`) is spelled here
        // and flows to both durable placement and read qualification.
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
        let namespace_id = if let Some(id) = empty_namespace_id {
            bootstrap_conn
                .execute(
                    "UPDATE namespace SET provenance = 'uri', source_path = ?2 WHERE id = ?1",
                    rusqlite::params![id, connection_uri],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to update empty namespace",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            id
        } else {
            bootstrap_conn
                .execute(
                    "INSERT INTO namespace (name, pid, fq_name, kind, provenance, source_path)
                     VALUES (?1, NULL, ?2, 'data', 'uri', ?3)",
                    rusqlite::params![namespace, namespace, connection_uri],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to create namespace",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
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
        register_catalog_wrapper(&bootstrap_conn, namespace, sys_meta_ns_id, catalog_id)?;

        debug!(
            "register_external_connection: Registered {} entities in namespace '{}' (connection_id={})",
            entity_count, namespace, connection_id
        );

        // Drop bootstrap lock before mutating self's maps
        drop(bootstrap_conn);

        // Store connection and schema in routing maps
        self.connection_map
            .insert(connection_id, components.connection);
        self.schema_map.insert(connection_id, components.schema);

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
        use crate::pipeline::parser::{
            setup_assertions_table_on_bootstrap, setup_danger_table_on_bootstrap,
            setup_errors_table_on_bootstrap,
        };

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
                match user_conn.query_all_string_rows("PRAGMA database_list", &[]) {
                    Ok((_cols, rows)) => rows
                        .iter()
                        .filter_map(|row| row.get(1).cloned())
                        .filter(|s| s != "main" && s != "temp" && s != "sys")
                        .collect(),
                    Err(_) => Vec::new(),
                }
            };
            for schema in &schemas {
                let _ = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]);
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
        setup_errors_table_on_bootstrap(&bootstrap_conn)?;

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
            if db_type_lower == "sqlite" { "in-process" } else { "fatboy" },
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

        // sys::connections: curated safe-subset `connection` entity, own
        // cartridge (mirrors the primary bootstrap path).
        register_sys_connection_table(&bootstrap_conn, bootstrap_conn_id)?;

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
    pub fn ensure_catalog_loaded(&self) {
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
        // (DDL-LOADING-PATHS.md Tier 1) so autoloads parse identically to
        // consult!() files — same whitespace handling, and embedded
        // directives are refused loudly rather than silently misparsed.
        let ddl = match crate::bin_cartridge::prelude::consult::parse_ddl_source_no_directives(
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

        let count = ddl.definitions.len();
        let path = format!("embedded://{}", namespace_fq);

        bootstrap_conn.execute_batch("BEGIN").ok();

        // Autoload modules have no liminal space (§8: created by other
        // means — empty liminal), hence the empty receipts.
        match Self::consult_file_inner(&bootstrap_conn, &path, namespace_fq, ddl, count, None, &[])
        {
            Ok(_) => {
                let _ = bootstrap_conn.execute_batch("COMMIT");
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
                StdlibLoad::Loaded
            }
            Err(e) => {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
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
    /// build the unresolved AST and run the effect executor (Phase 1.X).
    ///
    /// Seeds run on EVERY startup and every reinit, so each program must be
    /// idempotent. `doc!` qualifies (setting the same doc is a no-op-in-effect).
    pub fn run_seed_program(&mut self, src: &str) -> Result<()> {
        use crate::pipeline::{builder_v2, parser};

        let tree = parser::parse(src).map_err(|e| {
            DelightQLError::database_error(
                format!("seed program failed to parse: {}", e),
                "Seed parse error",
            )
        })?;

        let (queries, _features, _assertions, _emits, _dangers, _options, _ddl_blocks) =
            builder_v2::parse_queries(&tree, src).map_err(|e| {
                DelightQLError::database_error(
                    format!("seed program failed to build AST: {}", e),
                    "Seed build error",
                )
            })?;

        for (idx, query) in queries.into_iter().enumerate() {
            // A seed statement exists solely for its effects. If executing it
            // fires zero effects, it is a typo by definition — a mistyped
            // directive (e.g. `doc(...)` for `doc!(...)`) parses as a plain
            // table read and is silently discarded (the review's "quiet
            // path"). Refuse loudly, naming the offending statement; the
            // caller (`run_seed_programs`) prepends the culprit seed's name.
            let before = self.effects_executed_count();
            crate::pipeline::effect_executor::execute_effects(query, self)?;
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
    /// (`pipe://`, etc.). Without it, `mount_database` on a URI errors with
    /// "connection factory not available in this context". Installed by
    /// `open()` when the embedding provides a types-level factory.
    /// Seed the sys::help ring-1 tables from the host binary's surface
    /// (SYS-HELP-DESIGN.md phase 2). Called once at open(); the tables
    /// were created empty by bootstrap/schema.sql and registered by
    /// register_sys_help_tables. Runtime generation from the live clap
    /// tree: the rows structurally cannot drift from the binary.
    pub fn seed_help_surface(&self, surface: &crate::api::HelpSurface) -> Result<()> {
        let conn = self.bootstrap_connection.lock().map_err(|_| {
            DelightQLError::database_error(
                "seed_help_surface: bootstrap connection poisoned".to_string(),
                String::new(),
            )
        })?;
        let db_err = |what: &str, e: rusqlite::Error| {
            DelightQLError::database_error(
                format!("seed_help_surface: {} insert failed: {}", what, e),
                e.to_string(),
            )
        };
        for (name, parent, alias, summary) in &surface.commands {
            conn.execute(
                "INSERT INTO command (name, parent, alias, summary) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![name, parent, alias, summary],
            )
            .map_err(|e| db_err("command", e))?;
        }
        for o in &surface.options {
            conn.execute(
                "INSERT INTO option (command, long, short, value_name, default_value, global, repeatable, summary)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                rusqlite::params![
                    o.command,
                    o.long,
                    o.short,
                    o.value_name,
                    o.default_value,
                    o.global,
                    o.repeatable,
                    o.summary
                ],
            )
            .map_err(|e| db_err("option", e))?;
        }
        for (command, option, value, summary, class, grade) in &surface.option_values {
            conn.execute(
                "INSERT INTO option_value (command, option, value, summary, class, grade)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![command, option, value, summary, class, grade],
            )
            .map_err(|e| db_err("option_value", e))?;
        }
        for (name, summary) in &surface.dot_commands {
            conn.execute(
                "INSERT INTO dot_command (name, summary) VALUES (?1, ?2)",
                rusqlite::params![name, summary],
            )
            .map_err(|e| db_err("dot_command", e))?;
        }
        for (name, effect, flag) in &surface.envs {
            conn.execute(
                "INSERT INTO env (name, effect, equivalent_flag) VALUES (?1, ?2, ?3)",
                rusqlite::params![name, effect, flag],
            )
            .map_err(|e| db_err("env", e))?;
        }
        for (name, section, troff, plain) in &surface.man_pages {
            conn.execute(
                "INSERT INTO man_page (name, section, troff, plain) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![name, section, troff, plain],
            )
            .map_err(|e| db_err("man_page", e))?;
        }
        for (code, context, meaning, class, grade) in &surface.exit_codes {
            conn.execute(
                "INSERT INTO exit_code (code, context, meaning, class, grade)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![code, context, meaning, class, grade],
            )
            .map_err(|e| db_err("exit_code", e))?;
        }
        Ok(())
    }

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
    /// * `db_path` - Path to the database file or URI (e.g., "pipe://snowflake")
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
    /// `mount_tree!()`'s system half (EFFECTS-ON-TARGETS §4.3, Phase C):
    /// enumerate the target's PERSISTENT schemas and bind one sub-namespace
    /// per schema (`namespace::<schema>`), ALL on ONE connection.
    ///
    /// The factory's `create_tree` opens a single connection (one fatboy
    /// child) and hands back one `ConnectionComponents` per schema, every
    /// one carrying the SAME resource identity. `register_external_connection`
    /// deduplicates the bootstrap `connection` row by identity, so every
    /// sub-namespace lands on ONE `connection_id` (R-S1: a cross-schema
    /// `run!` is a single-connection, one-bracket plan). Returns the created
    /// sub-namespaces in enumeration order (for the receipt's JSON array,
    /// R-S3). SQLite/siso targets refuse inside `create_tree` (R-S5).
    pub fn mount_database_tree(&mut self, uri: &str, namespace: &str) -> Result<Vec<String>> {
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

    pub fn mount_database(&mut self, db_path: &str, namespace: &str) -> Result<()> {
        // System name guard (catechism Deviation #3): a USER-TYPED mount target
        // may not take over or nest under a reserved system name. mount_database
        // is only reached from the user-facing mount! verb (surface + embedded
        // directive), never from system-minted machinery.
        validate_user_namespace_target(namespace)?;

        // If a ConnectionFactory is available and the path looks like a URI scheme,
        // use the factory path (supports pipe://, fatboy://, etc.)
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
            // (resource-first surface, URI-DESIGN.md §4).
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
            // mount! is attach-only (bugs/nullmount Phase 1; EFFECT-ALGEBRA
            // §6). An empty (0-byte, e.g. /dev/null) or short file is not a
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

        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for mount",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Idempotent mount: if namespace already exists with the SAME database file,
        // this mount is a no-op. If a different file, that's an error.
        // If the namespace exists but is empty (e.g. "main" with :memory:), fall through
        // and reuse the existing namespace_id.
        let existing_namespace_id: Option<i32>;
        {
            let connection_uri = format!("file://{}", db_path);
            let existing: Option<String> = match bootstrap_conn.query_row(
                "SELECT c.source_uri FROM namespace n
                 JOIN activated_entity ae ON ae.namespace_id = n.id
                 JOIN entity e ON e.id = ae.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 WHERE n.fq_name = ?1
                 LIMIT 1",
                [namespace],
                |row| row.get(0),
            ) {
                Ok(uri) => Some(uri),
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    // Namespace might exist but have no entities — check namespace directly
                    match bootstrap_conn.query_row(
                        "SELECT 1 FROM namespace WHERE fq_name = ?1",
                        [namespace],
                        |_| Ok(()),
                    ) {
                        Ok(()) => Some(String::new()), // exists but empty
                        Err(_) => None,                // doesn't exist
                    }
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
                    // Same database — true idempotent, skip
                    drop(bootstrap_conn);
                    return Ok(());
                } else if existing_uri.is_empty() {
                    // Namespace exists but has no file-backed entities (e.g. "main" with :memory:).
                    // Fall through to mount, reusing the existing namespace row.
                    let ns_id: i32 = bootstrap_conn
                        .query_row(
                            "SELECT id FROM namespace WHERE fq_name = ?1",
                            [namespace],
                            |row| row.get(0),
                        )
                        .map_err(|e| {
                            DelightQLError::database_error(
                                "Failed to query existing namespace id",
                                e.to_string(),
                            )
                        })?;
                    existing_namespace_id = Some(ns_id);
                } else {
                    // Different SPELLING may still be the same FILE (the
                    // symlink trap, URI-DESIGN.md §4): compare filesystem
                    // identity before declaring a conflict.
                    let existing_path = existing_uri.trim_start_matches("file://");
                    let same_file = match (
                        std::fs::canonicalize(existing_path),
                        std::fs::canonicalize(db_path),
                    ) {
                        (Ok(a), Ok(b)) => a == b,
                        _ => false,
                    };
                    if same_file {
                        // Same resource, different spelling — idempotent.
                        drop(bootstrap_conn);
                        return Ok(());
                    }
                    return Err(DelightQLError::database_error(
                        format!(
                            "Namespace '{}' already exists (mounted from '{}'), cannot re-mount from '{}'",
                            namespace, existing_uri, connection_uri
                        ),
                        "Duplicate namespace with different source",
                    ));
                }
            } else {
                existing_namespace_id = None;
            }
        }

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
        let schema_alias = format!("_imported_{}", next_id);
        debug!("mount_database: Generated schema alias: {}", schema_alias);

        // ATTACH the database to the user connection
        let user_conn = self.connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire user connection lock",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let attach_sql = format!("ATTACH DATABASE '{}' AS '{}'", db_path, schema_alias);
        debug!("mount_database: Executing ATTACH: {}", attach_sql);
        user_conn.execute(&attach_sql, &[]).map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to attach database: {}", e),
                e.to_string(),
            )
        })?;

        // Register the connection in bootstrap. Resource = the path as
        // given; identity = filesystem identity (canonical path), which
        // folds symlinked/re-spelled mounts of one file into one row.
        let attach_identity = std::fs::canonicalize(db_path)
            .ok()
            .map(|abs| format!("realpath:{}", abs.display()));
        let _connection_id = crate::import::register_connection(
            &bootstrap_conn,
            db_path,
            "attach",
            attach_identity.as_deref(),
            1, // sqlite-file
            &format!("Mounted database: {}", namespace),
        )
        .map_err(|e| {
            DelightQLError::database_error(
                format!("Failed to register connection: {}", e),
                e.to_string(),
            )
        })?;

        // Introspect the attached database using the schema-specific method
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
        debug!(
            "mount_database: Discovered {} entities in schema '{}'",
            entities.len(),
            schema_alias
        );

        // Install as a cartridge
        // When mounting into "main", set source_ns = NULL so unqualified table
        // references resolve via SQLite's cross-schema search (matches the --db
        // path behavior, and keeps generated SQL spelling target-portable).
        // CAUTION: reads may be unqualified, but WRITES may not — CREATE does
        // not search schemas. imprint_namespace recovers the attach alias via
        // PRAGMA database_list when it targets a mounted namespace whose
        // cartridge carries no alias.
        let effective_source_ns: Option<&str> = if namespace == "main" {
            None
        } else {
            Some(&schema_alias)
        };
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
                        &format!("file://{}", db_path),
                        effective_source_ns,
                        2, // connection_id=2 (user connection where database is attached)
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

        // Insert discovered entities into bootstrap
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

        // Create or reuse the namespace. The reuse branch records the new
        // source just like creation does — an empty pre-created "main" that
        // gets mounted over must carry the mount's locator.
        let namespace_id = if let Some(ns_id) = existing_namespace_id {
            debug!(
                "mount_database: Reusing existing namespace_id={} for '{}'",
                ns_id, namespace
            );
            bootstrap_conn
                .execute(
                    "UPDATE namespace SET provenance = 'file', source_path = ?2 WHERE id = ?1",
                    rusqlite::params![ns_id, db_path],
                )
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to record mount source on reused namespace",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            ns_id
        } else {
            let sql = r#"
                INSERT INTO namespace (name, pid, fq_name, kind, provenance, source_path)
                VALUES (?1, NULL, ?2, 'data', 'file', ?3)
            "#;
            debug!(
                "mount_database: Creating namespace name='{}', fq_name='{}'",
                namespace, namespace
            );
            bootstrap_conn
                .execute(sql, rusqlite::params![namespace, namespace, db_path])
                .map_err(|e| {
                    DelightQLError::database_error_with_source(
                        "Failed to create namespace",
                        e.to_string(),
                        Box::new(e),
                    )
                })?;
            let id = bootstrap_conn.last_insert_rowid() as i32;
            debug!("mount_database: Created namespace_id={}", id);
            id
        };

        // Activate all entities from the cartridge in the namespace
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
            "mount_database: Activated {} entities in namespace '{}'",
            activated_count, namespace
        );

        // Note: The ATTACH path shares the user connection (connection_id=2),
        // so no additional entry in connection_map is needed. The attached schema
        // is accessed through the existing connection via the schema alias prefix.

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
        register_catalog_wrapper(&bootstrap_conn, namespace, sys_meta_ns_id, catalog_id)?;

        // Explicitly drop the bootstrap connection lock to ensure all writes are committed
        // This is necessary for sequential query execution to see the mounted namespace
        drop(bootstrap_conn);

        // Set the schema provider to read from bootstrap metadata.
        // This replaces the old DynamicSqliteSchema that queried the live connection.
        // Now column information comes from bootstrap's entity_attribute table,
        // which was populated by the introspection above.
        self.schema = Some(Box::new(
            crate::bootstrap_schema::BootstrapBackedSchema::new(self.bootstrap_connection.clone()),
        ));

        Ok(())
    }

    /// Provision a fresh, valid, empty SQLite database at `db_path` and bind it
    /// as namespace `namespace` (EFFECT-ALGEBRA §6, the `mount_new!`
    /// paragraph). The create-intent counterpart of `mount_database`: where
    /// `mount!` ATTACHES an existing database and rejects a missing/empty/
    /// invalid path, `mount_new!` MATERIALIZES the database first, then binds
    /// it exactly as `mount!` would.
    ///
    /// CLOBBER POLICY (§6, the `table!`/`table_replace!` refuse-over-clobber
    /// posture): refuse when the path already holds content — a real database
    /// OR any other non-empty bytes; only a MISSING or 0-byte path is
    /// materialized. On refusal the existing file is left untouched.
    ///
    /// v1 SCOPE (ruled): SQLite files only. A URI scheme (`postgres://`, …) or
    /// a DuckDB target refuses cleanly — extending to other engines is a
    /// future increment.
    ///
    /// MATERIALIZE: `rusqlite::Connection::open(path)` + `PRAGMA user_version =
    /// 0` forces the SQLite header page out (a valid 4096-byte empty db — the
    /// mechanism proven in bugs/nullmount/ANALYSIS.md; a 0-byte file or a bare
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
        if path.exists() {
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
        }

        // MATERIALIZE a valid empty SQLite database (a header-bearing 4096-byte
        // file). `PRAGMA user_version = 0` forces the header page out — a bare
        // open would leave a 0-byte file that mount!'s attach-only guard
        // rejects (bugs/nullmount/ANALYSIS.md).
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
            conn.execute_batch("PRAGMA user_version = 0;").map_err(|e| {
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
    /// # Arguments
    /// * `path` - Path to the DQL file (for cartridge source_uri)
    /// * `namespace` - Namespace to register under (e.g., "lib::math")
    /// * `ddl` - Pre-parsed DDL file (consumed)
    /// * `liminal_receipts` - the file's liminal-space directive receipts, in
    ///   file-appearance order (EFFECT-ALGEBRA §8). Written into
    ///   `liminal_receipt` INSIDE the consult transaction, so an aborted load
    ///   rolls the ledger away with the namespace (pinned by
    ///   `liminal_ledger_abort_leaves_no_ledger`). Empty for inline DDL and
    ///   for namespaces created by other means (their liminal is empty).
    ///
    /// # Returns
    /// ConsultResult with definitions loaded count and any replaced entity names
    pub fn consult_file(
        &mut self,
        path: &str,
        namespace: &str,
        ddl: DDLFile,
        liminal_receipts: &[LiminalReceipt],
    ) -> Result<ConsultResult> {
        let count = ddl.definitions.len();
        debug!(
            "consult_file: Loading {} definitions from '{}' into namespace '{}'",
            count, path, namespace
        );

        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for consult",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
            DelightQLError::database_error("Failed to begin consult transaction", e.to_string())
        })?;

        // Determine ambient DataNs for scratch namespaces.
        // Inline DDL views should be able to reference base tables from the
        // primary data namespace (typically "main") without explicit grounding.
        let ambient_data_ns = if path == "(inline)" {
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

        let result = Self::consult_file_inner(
            &bootstrap_conn,
            path,
            namespace,
            ddl,
            count,
            ambient_data_ns.as_deref(),
            liminal_receipts,
        );

        if result.is_ok() {
            bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
                DelightQLError::database_error(
                    "Failed to commit consult transaction",
                    e.to_string(),
                )
            })?;

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
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
        }

        drop(bootstrap_conn);

        result
    }

    fn consult_file_inner(
        bootstrap_conn: &Connection,
        path: &str,
        namespace: &str,
        ddl: DDLFile,
        count: usize,
        default_data_ns: Option<&str>,
        liminal_receipts: &[LiminalReceipt],
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

                    // Namespace exists — check for duplicate source URI
                    // Skip for inline DDL: multiple (~~ddl:"name" ~~) blocks can append
                    // to the same scratch namespace (write protection above guards safety).
                    if path != "(inline)" {
                        let duplicate: bool = bootstrap_conn
                        .query_row(
                            "SELECT COUNT(*) > 0 FROM cartridge WHERE source_uri = ?1 AND source_ns = ?2",
                            rusqlite::params![&source_uri, namespace],
                            |row| row.get(0),
                        )
                        .unwrap_or(false);
                        if duplicate {
                            return Err(DelightQLError::database_error_categorized(
                                "runtime",
                                format!(
                                    "File '{}' has already been consulted into namespace '{}'",
                                    path, namespace
                                ),
                                "Duplicate consult",
                            ));
                        }
                    }
                    id
                }
                None => {
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

        // THE LIMINAL RELATION (EFFECT-ALGEBRA §8): persist the file's
        // liminal-space receipts, one row per directive, in file-appearance
        // order (rowid = insertion order — the engine-courtesy contract, no
        // sequence column). This runs INSIDE the consult transaction, so an
        // abort rolls the ledger away with the namespace (pinned by
        // `liminal_ledger_abort_leaves_no_ledger`). A deferred liminal doc!
        // keeps its FILE position here — the receipts vec was collected in a
        // single pass over the file, before deferral (pinned by
        // `liminal_ledger_doc_keeps_file_position`). A repeat consult into an
        // existing namespace APPENDS (the namespace's liminal is the union of
        // its files' liminal spaces); reconsult REPLACES whole via
        // clear_namespace_contents.
        for receipt in liminal_receipts {
            bootstrap_conn
                .execute(
                    "INSERT INTO liminal_receipt (namespace_id, operation, echoes, receipt)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        namespace_id,
                        &receipt.operation,
                        receipt.echoes_json(),
                        receipt.receipt_json()
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to record liminal receipt",
                        e.to_string(),
                    )
                })?;
        }

        // For inline DDL: drop-and-replace conflicting entities by name.
        // Only entities whose names match a definition in the new DDL block are
        // removed; other entities from earlier inline blocks are preserved.
        let replaced_entities: Vec<String> = if path == "(inline)" {
            // Collect entity names from the incoming DDL
            let new_names: Vec<&str> = ddl.definitions.iter().map(|d| d.name.as_str()).collect();
            let new_names_deduped: std::collections::HashSet<&str> =
                new_names.iter().copied().collect();

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

        // Group definitions by name to support disjunctive clauses.
        // Multiple definitions with the same name (e.g., multi-clause sigma predicates
        // or guarded functions) are stored as a single entity with concatenated source.
        let mut groups: indexmap::IndexMap<String, Vec<&crate::pipeline::parser::Definition>> =
            indexmap::IndexMap::new();
        for def in &ddl.definitions {
            groups.entry(def.name.clone()).or_default().push(def);
        }

        // R6 DAG edges collected per effect-rule group; cycle-checked after
        // the loop (the whole consult is one transaction — a refusal rolls
        // back every registration). See validate_effect_rule_recursion.
        let mut effect_rule_edges: Vec<(String, Vec<String>)> = Vec::new();

        for (_name, defs) in &groups {
            // For multi-clause groups, concatenate source texts
            let (source_to_store, first_def) = if defs.len() == 1 {
                (defs[0].full_source.clone(), defs[0])
            } else {
                let concatenated = defs
                    .iter()
                    .map(|d| d.full_source.as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                debug!(
                    "consult_file: Grouping {} clauses for '{}' into single entity",
                    defs.len(),
                    defs[0].name
                );
                (concatenated, defs[0])
            };

            // Build typed DDL AST(s) from source text — eager validation.
            // If the body can't be parsed, error now rather than at query time.
            //
            // Skip eager validation for HO views: their bodies contain
            // unsubstituted HO parameter references (T(*), V(v), etc.) that
            // may create syntax patterns the body re-parser can't handle
            // until substitution occurs at call time.
            let is_ho_view = first_def.def_type == crate::pipeline::parser::DefinitionType::HoView;

            let ddl_defs = match crate::ddl::ddl_builder::build_ddl_file(&source_to_store) {
                Ok(d) if !d.is_empty() => d,
                Ok(_) if is_ho_view => {
                    // HO view with empty result — skip validation, proceed
                    // with registration using parser-level metadata only.
                    debug!(
                        "consult_file: Skipping eager validation for HO view '{}'",
                        first_def.name
                    );
                    Vec::new()
                }
                Ok(_) => {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "DDL definition '{}' could not be compiled (no definitions produced)",
                            first_def.name
                        ),
                        "DDL body validation failed",
                    ));
                }
                Err(ref e)
                    if is_ho_view
                        && !matches!(
                            e,
                            DelightQLError::TransformationError { .. }
                                | DelightQLError::ValidationError {
                                    subcategory: Some(_),
                                    ..
                                }
                        ) =>
                {
                    // HO view body failure that's NOT a semantic constraint
                    // error. Parse failures are expected when the body has
                    // complex HO parameter syntax (V(, ...) etc.) that the
                    // DQL parser can't handle before substitution. Defer
                    // validation to call time.
                    // Semantic constraint errors (TransformationError,
                    // categorized ValidationError) are still propagated eagerly.
                    debug!(
                            "consult_file: Deferring validation for HO view '{}' (body needs HO substitution)",
                            first_def.name
                        );
                    Vec::new()
                }
                Err(e) => {
                    // Semantic constraint errors (TransformationError,
                    // categorized ValidationError) propagate directly to
                    // preserve their specific URI subcategory
                    // (e.g., dql/semantic/constraint/column_ordinal).
                    if matches!(
                        &e,
                        DelightQLError::TransformationError { .. }
                            | DelightQLError::ValidationError {
                                subcategory: Some(_),
                                ..
                            }
                    ) {
                        return Err(e);
                    }
                    // Other errors (DatabaseOperationError from body_parser,
                    // etc.) get wrapped in ValidationError so the URI is
                    // dql/semantic rather than dql/runtime.
                    return Err(DelightQLError::validation_error(
                        format!(
                            "DDL definition '{}' has an invalid body: {}",
                            first_def.name, e
                        ),
                        "DDL body validation failed",
                    ));
                }
            };

            // Fact-as-clause union (DDL-CLAUSE-ALGEBRA-ANALYSIS.md ruling 4 /
            // §4-DESIGN): when a name's clause set mixes Fact and View heads and
            // NOTHING else, rewrite each fact clause into an equivalent
            // argumentative-view clause and run the UNCHANGED view pipeline
            // (naming algebra, arity, union desugar, Ground-Position rule). Facts
            // already compile as UNION ALL views; this routes two already-
            // compatible arm sources through one union — the mixed_kind refusal
            // below was blocking a union the compiler already knows how to build
            // piecewise. Every OTHER mix (function+view, sigma+anything, HO+…)
            // keeps refusing there. Standard facts become Ground heads (which
            // ABSTAIN from naming); stacked facts become Free heads whose headers
            // OFFER names into the contest/abstention algebra. `clause_sources`
            // (the per-clause text stored in `entity_clause`) tracks the rewrite
            // so query-time re-parse sees homogeneous view clauses; in the
            // non-mixed case it is exactly the original clause sources.
            let mut clause_sources: Vec<String> =
                defs.iter().map(|d| d.full_source.clone()).collect();
            let ddl_defs = if ddl_defs.len() > 1 {
                let type_set: std::collections::HashSet<i32> =
                    ddl_defs.iter().map(|d| d.head.entity_type_id()).collect();
                // Exactly {Fact(16), View(4)}: both present, nothing else.
                let is_fact_view_union =
                    type_set.len() == 2 && type_set.contains(&16) && type_set.contains(&4);
                if is_fact_view_union {
                    let mut rewritten: Vec<String> = Vec::new();
                    for (clause, src) in ddl_defs.iter().zip(clause_sources.iter()) {
                        if matches!(clause.head, crate::pipeline::asts::ddl::DdlHead::Fact) {
                            rewritten.extend(
                                crate::ddl::ddl_builder::fact_clause_to_view_sources(src)?,
                            );
                        } else {
                            rewritten.push(src.clone());
                        }
                    }
                    clause_sources = rewritten;
                    // Rebuild the typed AST from the rewritten (all-view) clauses:
                    // first-clause metadata (entity type = view), the
                    // mixed_kind/arity checks, and storage all now see
                    // homogeneous view clauses.
                    crate::ddl::ddl_builder::build_ddl_file(&clause_sources.join("\n"))?
                } else {
                    ddl_defs
                }
            } else {
                ddl_defs
            };

            // foo/foo! name collision (IMPLEMENTATION-PLAN §3.0, ruled
            // 2026-07-11): a namespace may not hold both a functor `foo` and
            // an effect rule `foo!`. Enforced at registration time in BOTH
            // directions, before either insert branch below (normal and
            // deferred-HO alike). Same-file collisions are caught too:
            // earlier groups of this consult are already inserted and
            // activated on this connection when later groups arrive. This is
            // the prerequisite for making doc! targets explicit — the `!`
            // fallback in consult_body (REPORT-2.2 decision 3) stays sound
            // only while the two names cannot coexist. Pinned by effects-ball
            // rules--47_name_collision_effect_second and
            // rules--48_name_collision_entity_second.
            {
                let group_name = first_def.name.as_str();
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

            // When ddl_defs is empty (HO view body deferred), derive
            // entity metadata from the parser-level Definition instead.
            let entity_type: i32;
            let param_names: Vec<&str>;

            if ddl_defs.is_empty() {
                // HO view with deferred body validation. Parse just the head
                // to extract HO param metadata (kind, columns) needed for
                // parameter binding at call time.
                let (_head_name, head) = crate::ddl::ddl_builder::build_ddl_head(&source_to_store)?;
                entity_type = head.entity_type_id();
                param_names = head.param_names();

                debug!(
                    "consult_file: Registering {:?} '{}' (deferred body, type={})",
                    head, first_def.name, entity_type
                );

                // Proffer-parse each clause for early validation + reference extraction.
                // Creates synthetic bindings so the body can be parsed without real call-site args.
                let proffer_table_names: std::collections::HashSet<String> = head
                    .ho_param_names()
                    .iter()
                    .map(|n| format!("__proffer__{}", n))
                    .collect();
                let mut proffer_refs: Vec<crate::ddl::analyzer::ExtractedReference> = Vec::new();

                for def in defs.iter() {
                    let bindings =
                        crate::pipeline::resolver::grounding::create_proffer_bindings(&head);
                    match crate::ddl::body_parser::parse_view_body_with_bindings(
                        &def.full_source,
                        bindings,
                    ) {
                        Ok(query) => {
                            let clause_refs =
                                crate::ddl::analyzer::extract_references_from_query(&query);
                            proffer_refs.extend(
                                clause_refs
                                    .into_iter()
                                    .filter(|r| !proffer_table_names.contains(&r.name)),
                            );
                        }
                        Err(e) => {
                            return Err(DelightQLError::validation_error(
                                format!(
                                    "HO view '{}' body has a syntax error: {}",
                                    first_def.name, e
                                ),
                                "DDL body validation failed",
                            ));
                        }
                    }
                }

                // Insert entity
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, NULL)",
                        rusqlite::params![&first_def.name, entity_type, cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert consult entity",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
                let entity_id = bootstrap_conn.last_insert_rowid() as i32;

                // Insert each clause into entity_clause
                for (ordinal, def) in defs.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_clause (entity_id, ordinal, definition) VALUES (?1, ?2, ?3)",
                            rusqlite::params![entity_id, (ordinal + 1) as i32, &def.full_source],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert entity clause",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }

                // Record input parameters
                for (position, param_name) in param_names.iter().enumerate() {
                    bootstrap_conn
                        .execute(
                            "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position) VALUES (?1, ?2, 'input_param', ?3)",
                            rusqlite::params![entity_id, param_name, position as i32],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert entity attribute",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }

                // Write HO param metadata with cross-clause position analysis
                if let crate::pipeline::asts::ddl::DdlHead::HoView { .. } = head {
                    // Parse each clause's head to get per-clause HO params
                    let mut clause_heads: Vec<crate::pipeline::asts::ddl::DdlHead> = Vec::new();
                    for def in defs.iter() {
                        match crate::ddl::ddl_builder::build_ddl_head(&def.full_source) {
                            Ok((_name, clause_head)) => clause_heads.push(clause_head),
                            Err(_) => {
                                // If head parsing fails, use the primary head for this clause
                                clause_heads.push(head.clone());
                            }
                        }
                    }
                    if clause_heads.is_empty() {
                        clause_heads.push(head.clone());
                    }

                    // Extract HoParam vecs from heads
                    let param_vecs: Vec<Vec<crate::pipeline::asts::ddl::HoParam>> = clause_heads
                        .iter()
                        .filter_map(|h| match h {
                            crate::pipeline::asts::ddl::DdlHead::HoView { params, .. } => {
                                Some(params.clone())
                            }
                            _ => None,
                        })
                        .collect();
                    let head_refs: Vec<&Vec<crate::pipeline::asts::ddl::HoParam>> =
                        param_vecs.iter().collect();

                    let positions =
                        crate::pipeline::resolver::grounding::build_ho_position_analysis_from_heads(
                            &head_refs,
                        );

                    Self::write_ho_params_to_bootstrap(bootstrap_conn, entity_id, &positions)?;
                }

                // Store proffer-extracted references
                for ext_ref in &proffer_refs {
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

                // Activate in namespace
                bootstrap_conn
                    .execute(
                        "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                        rusqlite::params![entity_id, namespace_id, cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to activate consult entity",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;

                continue; // Skip typed DDL processing below
            }

            // Use first clause for entity metadata (type, params)
            let first_ddl = &ddl_defs[0];

            // Validate multi-clause (disjunctive) definitions
            if ddl_defs.len() > 1 {
                use crate::pipeline::asts::ddl::DdlHead;

                fn head_kind_name(head: &DdlHead) -> &'static str {
                    match head {
                        DdlHead::Function { .. } => "function",
                        DdlHead::View => "view",
                        DdlHead::ArgumentativeView { .. } => "view",
                        DdlHead::HoView { .. } => "higher-order view",
                        DdlHead::SigmaPredicate { .. } => "sigma predicate",
                        DdlHead::Fact => "fact",
                        DdlHead::ErRule { .. } => "er-context rule",
                        DdlHead::EffectRule { .. } => "effect rule",
                    }
                }

                let first_type_id = first_ddl.head.entity_type_id();
                let first_arity = first_ddl.head.param_count();

                for (i, clause) in ddl_defs.iter().enumerate().skip(1) {
                    // Rule 1: All clauses must have the same entity type.
                    // Badged into the ddl/head family (clause-algebra RULE 3
                    // rebadge — was a generic parse_error). NARROWED
                    // (DDL-CLAUSE-ALGEBRA-ANALYSIS ruling 4, SHIPPED): the
                    // {Fact, View} pair no longer reaches here — the fact-union
                    // transform above rewrote its fact clauses into view clauses,
                    // so `ddl_defs` is homogeneous. This refusal now fires only
                    // for every OTHER mix (function+view, sigma+anything, HO+…),
                    // which stays loud.
                    if clause.head.entity_type_id() != first_type_id {
                        return Err(DelightQLError::validation_error_categorized(
                            "ddl/head/mixed_kind",
                            format!(
                                "Disjunctive definition '{}': clause {} is a {} but clause 1 is a {}. \
                                 All clauses must be the same kind.",
                                first_ddl.name,
                                i + 1,
                                head_kind_name(&clause.head),
                                head_kind_name(&first_ddl.head)
                            ),
                            "mixed clause kinds in one definition",
                        ));
                    }

                    // Rule 2: All clauses must have the same arity (counting all positions,
                    // including GroundScalar). Different clauses may have ground at different
                    // positions (e.g., clause 1: GroundScalar + Glob, clause 2: Scalar + Glob)
                    // but they must have the same total number of positions.
                    let clause_arity = clause.head.param_count();
                    if clause_arity != first_arity {
                        return Err(DelightQLError::parse_error(format!(
                            "Disjunctive definition '{}': clause {} has {} parameter(s) but clause 1 has {}. \
                             All clauses must have the same arity.",
                            first_ddl.name, i + 1, clause_arity, first_arity
                        )));
                    }
                }

                // Note: argumentative head contract validation (mixed forms, arity, name conflict)
                // is done at expansion time in grounding::desugar_argumentative_defs so that
                // error assertions on query lines can catch the errors.

                // Rules 3 & 4: value-function clause discipline — at most one
                // unguarded clause (RULE 2, the fix for the all-unguarded defect
                // that the old `has_any_guard` gate let slip through) and the
                // unguarded clause must be last. Gated on DdlHead::Function inside
                // the helper, so sigma predicates (the relational OR path) are
                // untouched. See validate_function_clause_discipline.
                validate_function_clause_discipline(&ddl_defs)?;
            }

            // Effect-algebra discipline (EFFECT-ALGEBRA R1/R2/R3/R4/R9 + F2)
            // — every group, single- or multi-clause; the effect sibling of
            // validate_function_clause_discipline above.
            if let Some(edges) = validate_effect_algebra_discipline(&ddl_defs)? {
                effect_rule_edges.push(edges);
            }

            // F2: at most one main! per namespace. A second file consulted
            // into the same namespace must not smuggle in another main!.
            if first_ddl.name == "main!" {
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
                first_ddl.head,
                first_ddl.name,
                ddl_defs.len(),
                if ddl_defs.len() > 1 { "s" } else { "" }
            );

            entity_type = first_ddl.head.entity_type_id();
            param_names = first_ddl.head.param_names();

            // Insert entity (without definition — clauses go into entity_clause)
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![&first_ddl.name, entity_type, cartridge_id, &first_ddl.doc],
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

            // Record input parameters as entity attributes (from first clause)
            for (position, param_name) in param_names.iter().enumerate() {
                bootstrap_conn
                    .execute(
                        "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, position) VALUES (?1, ?2, 'input_param', ?3)",
                        rusqlite::params![entity_id, param_name, position as i32],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert entity attribute",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
            }

            // For HO views, write structured param metadata with cross-clause position analysis
            if matches!(
                first_ddl.head,
                crate::pipeline::asts::ddl::DdlHead::HoView { .. }
            ) {
                let positions =
                    crate::pipeline::resolver::grounding::build_ho_position_analysis(&ddl_defs);
                Self::write_ho_params_to_bootstrap(bootstrap_conn, entity_id, &positions)?;
            }

            // For ER-rules, write metadata to er_rule table
            // Each clause may have a different context, so iterate all clauses.
            // Use enumerate() so clause_ordinal (1-indexed) matches entity_clause.ordinal.
            for (idx, ddl_def) in ddl_defs.iter().enumerate() {
                if let crate::pipeline::asts::ddl::DdlHead::ErRule {
                    ref left_table,
                    ref right_table,
                    ref context,
                } = ddl_def.head
                {
                    // Canonical ordering: alphabetical pair
                    let (left, right) = if left_table <= right_table {
                        (left_table.as_str(), right_table.as_str())
                    } else {
                        (right_table.as_str(), left_table.as_str())
                    };
                    bootstrap_conn
                        .execute(
                            "INSERT INTO er_rule (entity_id, left_table, right_table, context_name, clause_ordinal) VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![entity_id, left, right, context, (idx + 1) as i32],
                        )
                        .map_err(|e| {
                            DelightQLError::database_error_with_source(
                                "Failed to insert er_rule",
                                e.to_string(),
                                Box::new(e),
                            )
                        })?;
                }
            }

            // Extract references from ALL clauses (union of references)
            {
                use crate::pipeline::asts::ddl::DdlBody;
                let mut all_refs = Vec::new();
                for ddl_def in &ddl_defs {
                    let clause_refs = match &ddl_def.body {
                        DdlBody::Scalar(expr) => {
                            crate::ddl::analyzer::extract_references_from_domain(expr)
                        }
                        DdlBody::Relational(query) => {
                            crate::ddl::analyzer::extract_references_from_query(query)
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
                    first_ddl.name,
                    ddl_defs.len(),
                    if ddl_defs.len() > 1 { "s" } else { "" }
                );
            }

            // Register interior schemas for tree group columns
            {
                use crate::pipeline::asts::ddl::DdlBody;
                for ddl_def in &ddl_defs {
                    if let DdlBody::Relational(query) = &ddl_def.body {
                        register_interior_schemas_from_query(bootstrap_conn, entity_id, query)?;
                    }
                }
            }

            // Activate in namespace
            bootstrap_conn
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![entity_id, namespace_id, cartridge_id],
                )
                .map_err(|e| {
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
        // Lazy-load stdlib module if needed (e.g., "std::reshape")
        self.ensure_stdlib_loaded(namespace);

        // Get bootstrap connection
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for enlist",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up the namespace ID. §IV PLAIN-NAME ENLIST (middle rung):
        // `enlist!("chz")` when `chz` is a direct child of an already-enlisted
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

        // Blueprint inertness (M2): enlisting an archived blueprint (or a
        // descendant of one) would make its inert rules resolvable UNQUALIFIED
        // — the opposite of "consumed and archived". Refuse. Checks the RESOLVED
        // fq so a plain-name enlist of an archived-blueprint child stays refused.
        refuse_if_blueprint(&bootstrap_conn, &resolved_fq)?;

        // Get the "main" namespace ID (to_namespace = "main")
        // This is the default namespace where entities are enlisted when no target is specified
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'main'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Default namespace 'main' not found in bootstrap (database corruption)",
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
                     FROM er_rule new_er
                     JOIN entity new_e ON new_e.id = new_er.entity_id
                     JOIN activated_entity new_ae ON new_ae.entity_id = new_e.id
                        AND new_ae.namespace_id = ?1
                     JOIN er_rule existing_er ON existing_er.context_name = new_er.context_name
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

        Ok(())
    }

    /// Record that `exposing_ns` re-exports `exposed_ns` through its facade.
    /// When someone enlists `exposing_ns`, entities from `exposed_ns` become
    /// visible via a recursive CTE at resolution time.
    pub fn expose_namespace(&mut self, exposing_ns: &str, exposed_ns: &str) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for expose",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let exposing_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [exposing_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found for expose", exposing_ns),
                    "Namespace not found",
                )
            })?;

        let exposed_id: i64 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [exposed_ns],
                |row| row.get(0),
            )
            .map_err(|_| {
                DelightQLError::database_error(
                    format!("Namespace '{}' not found for expose", exposed_ns),
                    "Namespace not found",
                )
            })?;

        // Validate: exposed must be a child of exposing
        if !exposed_ns.starts_with(&format!("{}::", exposing_ns)) {
            return Err(DelightQLError::database_error(
                format!(
                    "Cannot expose '{}' through '{}': not a child namespace",
                    exposed_ns, exposing_ns
                ),
                "Invalid expose target",
            ));
        }

        bootstrap_conn
            .execute(
                "INSERT OR IGNORE INTO exposed_namespace
                 (exposing_namespace_id, exposed_namespace_id) VALUES (?1, ?2)",
                rusqlite::params![exposing_id, exposed_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to expose namespace '{}': {}", exposed_ns, e),
                    e.to_string(),
                )
            })?;

        debug!(
            "expose_namespace: '{}' now re-exports '{}'",
            exposing_ns, exposed_ns
        );

        Ok(())
    }

    /// Register a namespace alias (e.g., "l" → "lib::math")
    ///
    /// Creates a namespace_alias record in bootstrap, allowing a short alias
    /// to be used in place of a fully-qualified namespace path.
    pub fn register_namespace_alias(&mut self, alias: &str, namespace: &str) -> Result<()> {
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

        bootstrap_conn
            .execute(
                "INSERT OR REPLACE INTO namespace_alias (alias, target_namespace_id) VALUES (?1, ?2)",
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
        Ok(())
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

        // Get the "main" namespace ID (to_namespace = "main")
        // This is the default namespace where entities are enlisted when no target is specified
        let to_namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = 'main'",
                [],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Default namespace 'main' not found in bootstrap (database corruption)",
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

    /// Record which namespaces were enlisted inside a DDL as namespace-local dependencies.
    /// The enlisted_namespace rows (from_namespace_id, to_namespace_id) represent the delta
    /// of enlists that happened during the DDL. We store them as local dependencies of the
    /// DDL's namespace so the resolver can activate them during view body resolution.
    pub fn record_namespace_local_enlists(
        &mut self,
        namespace: &str,
        new_enlists: &[(i32, i32)],
    ) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for record_namespace_local_enlists",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        // Look up the namespace ID
        let namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found for local enlist recording",
                        namespace
                    ),
                    e.to_string(),
                )
            })?;

        for (from_ns_id, _to_ns_id) in new_enlists {
            // The enlist was (from=enlisted_ns, to=main).
            // We record it as: namespace_id depends on from_ns_id.
            bootstrap_conn
                .execute(
                    "INSERT OR IGNORE INTO namespace_local_enlist (namespace_id, enlisted_namespace_id) VALUES (?1, ?2)",
                    rusqlite::params![namespace_id, from_ns_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to record namespace local enlist",
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

    /// Record which namespace aliases were created inside a DDL file.
    /// These are scoped to the DDL's namespace.
    pub fn record_namespace_local_aliases(
        &mut self,
        namespace: &str,
        new_aliases: &[(String, i32)],
    ) -> Result<()> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for record_namespace_local_aliases",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let namespace_id: i32 = bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [namespace],
                |row| row.get(0),
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!(
                        "Namespace '{}' not found for local alias recording",
                        namespace
                    ),
                    e.to_string(),
                )
            })?;

        for (alias, target_ns_id) in new_aliases {
            bootstrap_conn
                .execute(
                    "INSERT OR IGNORE INTO namespace_local_alias (namespace_id, alias, target_namespace_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![namespace_id, alias, target_ns_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to record namespace local alias",
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
    fn destroy_namespace(&mut self, namespace_fq: &str) -> Result<(Option<i64>, Option<String>)> {
        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap database lock for destroy_namespace",
                format!("Connection was poisoned: {}", e),
            )
        })?;

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

        // Find ALL cartridge(s) and their connection info
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

        let (connection_id, source_ns) = cartridge_infos
            .first()
            .map(|(_, conn_id, src_ns)| (*conn_id, src_ns.clone()))
            .unwrap_or((None, None));

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

                // ho_param_ground_value (FK to ho_param)
                bootstrap_conn
                    .execute(
                        "DELETE FROM ho_param_ground_value WHERE ho_param_id IN (
                        SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                        WHERE e.cartridge_id = ?1)",
                        [cartridge_id],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to delete ho_param_ground_value",
                            e.to_string(),
                        )
                    })?;

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

                // er_rule
                bootstrap_conn.execute(
                    "DELETE FROM er_rule WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
                    [cartridge_id],
                ).map_err(|e| DelightQLError::database_error("Failed to delete er_rule", e.to_string()))?;

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

        drop(bootstrap_conn);

        Ok((connection_id, source_ns))
    }

    /// Unmount a data namespace, releasing its database connection.
    ///
    /// Validates the namespace is of kind 'data' and is not borrowed by any
    /// grounded namespace. If clear, cascade-deletes all bootstrap metadata
    /// and performs physical cleanup (DETACH or connection_map removal).
    pub fn unmount_database(&mut self, namespace: &str) -> Result<()> {
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
        {
            let borrower_info: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
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
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
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

        drop(bootstrap_conn);

        // 4. Cascade delete: descendants first (deepest first), then parent
        let mut schemas_to_detach: Vec<String> = Vec::new();
        for (desc_fq, desc_kind) in &descendants {
            let (connection_id, source_ns) = self.destroy_namespace(desc_fq)?;
            if desc_kind == "data" {
                if let Some(conn_id) = connection_id {
                    if conn_id > 2 {
                        self.connection_map.remove(&conn_id);
                        self.schema_map.remove(&conn_id);
                    }
                }
                if let Some(schema) = source_ns {
                    schemas_to_detach.push(schema);
                }
            }
        }
        let (connection_id, source_ns) = self.destroy_namespace(namespace)?;

        // 5. Physical cleanup for parent
        if let Some(conn_id) = connection_id {
            if conn_id > 2 {
                self.connection_map.remove(&conn_id);
                self.schema_map.remove(&conn_id);
            }
        }
        if let Some(schema) = source_ns {
            schemas_to_detach.push(schema);
        }

        // 6. DETACH all released schemas from the user connection
        if !schemas_to_detach.is_empty() {
            let user_conn = self.connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire user connection lock for unmount detach",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            for schema in &schemas_to_detach {
                let _ = user_conn.execute(&format!("DETACH DATABASE '{}'", schema), &[]);
            }
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
    /// Validates the namespace is not of kind 'data' or 'system'. For lib namespaces,
    /// checks that no grounded namespace borrows from it. Then cascade-deletes all
    /// bootstrap metadata.
    pub fn unconsult_namespace(&mut self, namespace: &str) -> Result<()> {
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
        //    (lib borrowed as lib_namespace_id, data borrowed as data_namespace_id,
        //     but only if the borrower is OUTSIDE the tree)
        {
            let borrower_info: Option<(String, String)> = bootstrap_conn
                .query_row(
                    "SELECT n_borrower.fq_name, n_source.fq_name
                     FROM grounding g
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
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
                     JOIN namespace n_borrower ON n_borrower.id = g.grounded_namespace_id
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

        drop(bootstrap_conn);

        // 4. Cascade delete: descendants first (deepest first), then parent
        for (desc_fq, desc_kind) in &descendants {
            let (connection_id, _source_ns) = self.destroy_namespace(desc_fq)?;
            // Physical cleanup for data descendants
            if desc_kind == "data" {
                if let Some(conn_id) = connection_id {
                    if conn_id > 2 {
                        self.connection_map.remove(&conn_id);
                        self.schema_map.remove(&conn_id);
                    }
                }
            }
        }
        let _result = self.destroy_namespace(namespace)?;

        debug!(
            "unconsult_namespace: Unconsulted namespace '{}' (cascade-deleted {} descendants)",
            namespace,
            descendants.len()
        );
        Ok(())
    }

    /// Write HO parameter metadata to bootstrap from cross-clause position analysis.
    ///
    /// Inserts rows into ho_param, ho_param_column, and ho_param_ground_value
    /// based on the unified HoPositionInfo computed by `build_ho_position_analysis`.
    fn write_ho_params_to_bootstrap(
        bootstrap_conn: &Connection,
        entity_id: i32,
        positions: &[crate::pipeline::asts::ddl::HoPositionInfo],
    ) -> Result<()> {
        use crate::pipeline::asts::ddl::{HoColumnKind, HoGroundMode};

        for pos_info in positions {
            let kind_str = match &pos_info.column_kind {
                HoColumnKind::TableGlob => "glob",
                HoColumnKind::TableArgumentative(_) => "argumentative",
                HoColumnKind::Scalar => match &pos_info.ground_mode {
                    HoGroundMode::PureGround => "ground_scalar",
                    HoGroundMode::MixedGround => "scalar",
                    _ => "scalar",
                },
            };

            let ground_mode_str = match &pos_info.ground_mode {
                HoGroundMode::PureGround => Some("pure_ground"),
                HoGroundMode::MixedGround => Some("mixed_ground"),
                HoGroundMode::PureUnbound => Some("pure_unbound"),
                HoGroundMode::InputOnly => Some("input_only"),
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
                    "INSERT INTO ho_param (entity_id, param_name, position, kind, ground_mode, column_name) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![entity_id, param_name, pos_info.position as i32, kind_str, ground_mode_str, &pos_info.column_name],
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

            // Write per-clause ground values
            for (clause_ordinal, ground_value) in &pos_info.ground_values {
                bootstrap_conn
                    .execute(
                        "INSERT INTO ho_param_ground_value (ho_param_id, clause_ordinal, ground_value) VALUES (?1, ?2, ?3)",
                        rusqlite::params![ho_param_id, *clause_ordinal as i32, ground_value],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error_with_source(
                            "Failed to insert ho_param_ground_value",
                            e.to_string(),
                            Box::new(e),
                        )
                    })?;
            }
        }

        Ok(())
    }

    /// Deep-copy all sub-tables for an entity (clause, attribute, referenced,
    /// ho_param+columns, er_rule, interior_entity+attributes).
    fn copy_entity_subtables(
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

        // ho_param + ho_param_column + ho_param_ground_value (FK chain: entity → ho_param → children)
        {
            let mut stmt = conn
                .prepare("SELECT id, param_name, position, kind, ground_mode, column_name FROM ho_param WHERE entity_id = ?1")
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query ho_param", e.to_string())
                })?;
            let old_params: Vec<(i32, String, i32, String, Option<String>, Option<String>)> = stmt
                .query_map([old_entity_id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                })
                .map_err(|e| {
                    DelightQLError::database_error("Failed to query ho_param", e.to_string())
                })?
                .flatten()
                .collect();

            for (old_hp_id, param_name, position, kind, ground_mode, column_name) in &old_params {
                conn.execute(
                    "INSERT INTO ho_param (entity_id, param_name, position, kind, ground_mode, column_name) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    rusqlite::params![new_entity_id, param_name, position, kind, ground_mode, column_name],
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

                conn.execute(
                    "INSERT INTO ho_param_ground_value (ho_param_id, clause_ordinal, ground_value)
                     SELECT ?1, clause_ordinal, ground_value
                     FROM ho_param_ground_value WHERE ho_param_id = ?2",
                    rusqlite::params![new_hp_id, old_hp_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to copy ho_param_ground_value",
                        e.to_string(),
                    )
                })?;
            }
        }

        // er_rule
        conn.execute(
            "INSERT INTO er_rule (entity_id, left_table, right_table, context_name, clause_ordinal)
             SELECT ?1, left_table, right_table, context_name, clause_ordinal
             FROM er_rule WHERE entity_id = ?2",
            rusqlite::params![new_entity_id, old_entity_id],
        )
        .map_err(|e| DelightQLError::database_error("Failed to copy er_rule", e.to_string()))?;

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
    /// ho_param_ground_value, ho_param_column, entity_resolution, ho_param, er_rule,
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
                "DELETE FROM ho_param_ground_value WHERE ho_param_id IN (
                SELECT hp.id FROM ho_param hp JOIN entity e ON hp.entity_id = e.id
                WHERE e.cartridge_id = ?1)",
                [cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to delete ho_param_ground_value",
                    e.to_string(),
                )
            })?;

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
            "DELETE FROM er_rule WHERE entity_id IN (SELECT id FROM entity WHERE cartridge_id = ?1)",
            [cartridge_id],
        ).map_err(|e| DelightQLError::database_error("Failed to delete er_rule", e.to_string()))?;

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
            .execute("DELETE FROM er_rule WHERE entity_id = ?1", [entity_id])
            .map_err(|e| {
                DelightQLError::database_error("Failed to delete er_rule", e.to_string())
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
    fn clear_namespace_contents(
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
                DelightQLError::database_error(
                    "Failed to clear liminal_receipt",
                    e.to_string(),
                )
            })?;

        Ok(cartridge_infos)
    }

    /// Check that all unqualified references in a lib namespace resolve against
    /// a data namespace. Returns Ok(()) if contract holds, or Err with details.
    fn validate_grounding_contract(
        bootstrap_conn: &Connection,
        lib_ns_id: i64,
        lib_ns_fq: &str,
        data_ns_id: i64,
        data_ns_fq: &str,
    ) -> Result<()> {
        let mut stmt = bootstrap_conn
            .prepare(
                "SELECT DISTINCT re.name, e.name
             FROM referenced_entity re
             JOIN entity e ON re.containing_entity_id = e.id
             JOIN activated_entity ae ON ae.entity_id = e.id
             WHERE ae.namespace_id = ?1
               AND re.namespace IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM entity e2
                   JOIN activated_entity ae2 ON ae2.entity_id = e2.id
                   WHERE ae2.namespace_id = ?2
                     AND e2.name = re.name COLLATE NOCASE
               )",
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to prepare grounding contract query",
                    e.to_string(),
                )
            })?;

        let broken: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![lib_ns_id, data_ns_id], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(|e| {
                DelightQLError::database_error("Failed to query grounding contract", e.to_string())
            })?
            .flatten()
            .collect();

        if !broken.is_empty() {
            let details: Vec<String> = broken
                .iter()
                .map(|(ref_name, entity_name)| {
                    format!(
                        "'{}' references '{}' (not in '{}')",
                        entity_name, ref_name, data_ns_fq
                    )
                })
                .collect();
            return Err(DelightQLError::database_error(
                format!(
                    "Grounding contract violation: lib '{}' → data '{}'. Broken references: {}",
                    lib_ns_fq,
                    data_ns_fq,
                    details.join("; ")
                ),
                "Grounding contract violated",
            ));
        }
        Ok(())
    }

    /// Rebuild a grounded namespace's entity copies from its source lib namespace.
    fn rebuild_grounded_namespace(
        bootstrap_conn: &Connection,
        grounded_ns_id: i64,
        lib_ns_fq: &str,
        data_ns_fq: &str,
    ) -> Result<usize> {
        // Clear old contents
        Self::clear_namespace_contents(bootstrap_conn, grounded_ns_id)?;

        // Retrieve lib entities
        let entities: Vec<(i32, String, i32, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.id, e.name, e.type, e.doc
                 FROM entity e
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE n.fq_name = ?1",
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query lib entities for rebuild",
                        e.to_string(),
                    )
                })?;
            let rows = stmt
                .query_map([lib_ns_fq], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to query lib entities for rebuild",
                        e.to_string(),
                    )
                })?;
            rows.flatten().collect()
        };

        // Create new cartridge
        bootstrap_conn.execute(
            "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
             VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
            rusqlite::params![
                1, // DqlStandard
                SourceType::File.as_i32(),
                &format!("ground://{}<-{}", lib_ns_fq, data_ns_fq),
                rusqlite::types::Null,
                1, // bootstrap connection
            ],
        ).map_err(|e| DelightQLError::database_error("Failed to create rebuild cartridge", e.to_string()))?;
        let cartridge_id = bootstrap_conn.last_insert_rowid() as i32;

        let count = entities.len();
        for (old_entity_id, entity_name, entity_type, entity_doc) in &entities {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![entity_name, entity_type, cartridge_id, entity_doc],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to copy entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;
            let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

            Self::copy_entity_subtables(bootstrap_conn, *old_entity_id, new_entity_id)?;

            bootstrap_conn.execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                rusqlite::params![new_entity_id, grounded_ns_id, cartridge_id],
            ).map_err(|e| DelightQLError::database_error(format!("Failed to activate entity '{}'", entity_name), e.to_string()))?;
        }

        Ok(count)
    }

    /// Ground a lib namespace into a new namespace, binding it to a data namespace
    ///
    /// Validates all entities in `lib_ns` resolve against `data_ns`, then creates
    /// a new namespace with copies of those entities pre-bound to the data namespace.
    /// The new namespace has `default_data_ns` set so the resolver auto-applies grounding.
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
        // System name guard (catechism Deviation #3): `new_ns_name` is the
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
        let entities: Vec<(i32, String, i32, Option<String>)> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.id, e.name, e.type, e.doc
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
                    row.get::<_, i32>(2)?,
                    row.get::<_, Option<String>>(3)?,
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

        if entities.is_empty() && manifest_entity_names.is_empty() {
            return Err(DelightQLError::database_error(
                format!("Library namespace '{}' has no entities to ground", lib_ns),
                "Empty namespace",
            ));
        }

        // 5. STRICT VALIDATION: For each entity with references, check that all
        //    referenced entities (unqualified free variables) exist in the data namespace
        for (entity_id, entity_name, _entity_type, _doc) in &entities {
            let refs: Vec<String> = {
                let mut ref_stmt = bootstrap_conn
                    .prepare(
                        "SELECT re.name FROM referenced_entity re
                         WHERE re.containing_entity_id = ?1
                           AND re.namespace IS NULL",
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to query entity references",
                            e.to_string(),
                        )
                    })?;

                let rows = match ref_stmt.query_map([entity_id], |row| row.get::<_, String>(0)) {
                    Ok(r) => r,
                    Err(e) => {
                        return Err(DelightQLError::database_error(
                            "Failed to query entity references",
                            e.to_string(),
                        ));
                    }
                };
                rows.flatten().collect()
            };

            for ref_name in &refs {
                // Check if ref_name exists as an activated entity in data_ns
                let exists: bool = bootstrap_conn
                    .query_row(
                        "SELECT EXISTS(
                            SELECT 1 FROM entity e
                            JOIN activated_entity ae ON ae.entity_id = e.id
                            WHERE ae.namespace_id = ?1 AND e.name = ?2 COLLATE NOCASE
                        )",
                        rusqlite::params![data_ns_id, ref_name],
                        |row| row.get(0),
                    )
                    .unwrap_or(false);

                if !exists {
                    return Err(DelightQLError::database_error(
                        format!(
                            "ground!() validation failed: entity '{}' references '{}' \
                             which does not exist in data namespace '{}'",
                            entity_name, ref_name, data_ns
                        ),
                        "Unresolved reference",
                    ));
                }
            }
        }

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

        // 6b. Record grounding dependency for ownership enforcement
        bootstrap_conn
            .execute(
                "INSERT INTO grounding (grounded_namespace_id, data_namespace_id, lib_namespace_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![new_ns_id, data_ns_id, lib_ns_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    "Failed to record grounding dependency",
                    e.to_string(),
                )
            })?;

        // 7. Create cartridge for ground
        let cartridge_id = {
            bootstrap_conn
                .execute(
                    "INSERT INTO cartridge (language, source_type_enum, source_uri, source_ns, connected, connection_id, is_universal)
                     VALUES (?1, ?2, ?3, ?4, 1, ?5, 0)",
                    rusqlite::params![
                        1, // DqlStandard language ID
                        SourceType::File.as_i32(),
                        &format!("ground://{}<-{}", lib_ns, data_ns),
                        None::<String>, // No SQL schema qualifier for bootstrap-local temp tables
                        1, // bootstrap connection
                    ],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        "Failed to create ground cartridge",
                        e.to_string(),
                    )
                })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        // 8. Copy entities from lib_ns into new namespace
        let mut count = entities.len();
        for (old_entity_id, entity_name, entity_type, entity_doc) in &entities {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![entity_name, entity_type, cartridge_id, entity_doc],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to copy entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;
            let new_entity_id = bootstrap_conn.last_insert_rowid() as i32;

            Self::copy_entity_subtables(&bootstrap_conn, *old_entity_id, new_entity_id)?;

            // If entity has manifest data in _internal, create TEMP table from it
            if let Some(int_ns_id) = internal_ns_id {
                if let Some(result) = crate::ddl_pipeline::create_temp_table_from_manifest(
                    &bootstrap_conn,
                    int_ns_id,
                    entity_name,
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

            bootstrap_conn
                .execute(
                    "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) VALUES (?1, ?2, ?3)",
                    rusqlite::params![new_entity_id, new_ns_id, cartridge_id],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to activate entity '{}'", entity_name),
                        e.to_string(),
                    )
                })?;
        }

        // 8b. Create manifest-only entities (discovered from _internal, no entity in lib_ns)
        if let Some(int_ns_id) = internal_ns_id {
            for entity_name in &manifest_entity_names {
                let result = match crate::ddl_pipeline::create_temp_table_from_manifest(
                    &bootstrap_conn,
                    int_ns_id,
                    entity_name,
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
                            1, // Table entity type
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

        drop(bootstrap_conn);

        debug!(
            "ground_namespace: Grounded {} entities from '{}' into '{}' (data: '{}')",
            count, lib_ns, new_ns_name, data_ns
        );

        Ok(count)
    }

    /// Set the `doc` string on a catalog entity, addressed by its
    /// fully-qualified name (e.g. `"sys::help.identifier"`).
    ///
    /// The fq name is `<namespace fq_name>.<entity name>`, so it is matched
    /// against `n.fq_name || '.' || e.name` over activated entities — the same
    /// namespace/activated_entity join every other catalog lookup uses. Only
    /// activated entities are considered; if the name still resolves to more
    /// than one entity (a same-name collision within one namespace across
    /// cartridges) it is reported as ambiguous.
    ///
    /// Session-scoped: writes the in-memory bootstrap catalog for this session.
    pub fn set_entity_doc(&mut self, target: &str, doc: &str) -> Result<(String, String)> {
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

        if source_kind == "data" || source_kind == "system" {
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

        // 4. Get target connection info: schema alias + connection_id.
        // First try the entity path (populated namespaces), then the mount
        // linkage (namespace.source_path = cartridge.source_uri) — an EMPTY
        // mounted namespace has no activated entities, and falling through
        // to the primary connection would silently write the imprinted
        // tables into the wrong physical database (the session's primary
        // schema instead of the mounted file).
        let (target_schema_alias, connection_id): (Option<String>, i64) = bootstrap_conn
            .query_row(
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
            .or_else(|_| {
                // namespace.source_path stores the bare locator; cartridge
                // .source_uri may carry the file:// spelling of the same path.
                bootstrap_conn.query_row(
                    "SELECT c.source_ns, c.connection_id
                     FROM namespace n
                     JOIN cartridge c ON c.source_uri = n.source_path
                                      OR c.source_uri = 'file://' || n.source_path
                     WHERE n.fq_name = ?1 AND n.source_path IS NOT NULL
                     ORDER BY c.id DESC
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
                    let (_cols, rows) = conn
                        .query_all_string_rows("PRAGMA database_list", &[])
                        .ok()?;
                    rows.iter().find_map(|row| {
                        let alias = row.get(1)?;
                        let file = row.get(2)?;
                        if alias == "main" || file.is_empty() {
                            return None;
                        }
                        (std::fs::canonicalize(file).ok()? == want)
                            .then(|| alias.clone())
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
                        "imprint!() source '{}' has no _internal namespace \
                         (no schema/constraints/defaults definitions)",
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

            // Check for CTAS body: entity with :- or := view body in source namespace
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
                    if let Some(pos) = def.find(":-") {
                        let body = def[pos + 2..].trim();
                        if !body.is_empty() {
                            Some(body.to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
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
        let schema: &dyn DatabaseSchema = if connection_id == 2 {
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

        // How each prepared entity is materialized. Replaces the former
        // shadow flags (a `materialization` string + a boolean CTAS flag + an
        // optional insert SQL + `effective_schema`-emptiness-as-type-tag; review
        // finding 9): a single enum makes the three variants exhaustive and the
        // discriminator un-driftable. Payload carries exactly what the catalog
        // pass needs per variant — DeclaredTable knows its columns up front (no
        // PRAGMA readback) and may carry an INSERT…SELECT; View/CtasTable read
        // their columns back from the committed object.
        enum Materialized {
            /// `CREATE VIEW … AS <select>`. entity_type = 2; attrs read back.
            View,
            /// `CREATE TABLE … AS SELECT`. entity_type = 1; attrs read back.
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
            // mid-imprint (review finding 10). Pinned by
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
            // Option A (doeklund 2026-07-07): constraints/defaults require a
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
                let resolved = crate::ddl_pipeline::resolver::resolve(unresolved)?;
                let sql_ast = crate::ddl_pipeline::transformer::transform(resolved)?;
                let create_sql = crate::ddl_pipeline::generator::generate(&sql_ast);

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
                    format!("INSERT INTO {} {}", qualified_table(entity_name), select_sql)
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
        // enforcement disabled (companion_linear--66; review M4).
        let _ = target_conn_guard.execute("PRAGMA foreign_keys = ON", &[]);

        // --- Pre-flight clash pass (READ-ONLY, before any mutation) ---
        // imprint! (replace=false) fails if ANY target object already exists;
        // imprint_replace! (replace=true) records each clashing object to drop.
        // This pass only *reads* the catalog: a strict clash returns here with
        // the target byte-for-byte untouched, so the strict path's atomicity is
        // by-construction, not by-rollback (pinned: companion_linear--67).
        //
        // Both sqlite_master AND sqlite_temp_master are consulted: a temp object
        // is connection-local and sqlite_master never lists it, so it would
        // otherwise bypass both the strict-clash refusal and the replace-mode
        // drop (review finding 10). sqlite_temp_master is unqualified (temp
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
                    .query_all_string_rows(&sql, &[])
                    .ok()
                    .and_then(|(_c, rows)| rows.first().and_then(|r| r.first().cloned()));
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
        // their place (pinned: cli tests/imprint_atomicity.rs; review C1).
        //
        // `defer_foreign_keys = ON` set *inside* the txn makes drop/create
        // ordering FK-agnostic without touching the persistent `foreign_keys`
        // flag; SQLite auto-resets it at COMMIT/ROLLBACK, so no error path can
        // leak it OFF (review M4). At COMMIT the recreated CTAS tables carry no
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

        target_conn_guard
            .execute("COMMIT", &[])
            .map_err(|e| {
                DelightQLError::database_error(
                    "imprint: failed to commit target transaction",
                    e.to_string(),
                )
            })?;
        target_txn.committed = true;

        // Post-commit FK audit (replace mode only). Recreated CTAS tables carry
        // none of the replaced tables' constraints; a child row that referenced
        // an old row now dangling is a silent orphan. We do NOT fail the imprint
        // (the data is committed) — we make it loud (review M4). NOTE: this
        // warning itself is not yet test-pinned (needs an external-FK-child
        // fixture; candidate for the Change-3 test sweep).
        if !to_drop.is_empty() {
            let fk_check_sql = match target_schema_alias.as_deref() {
                Some(a) => format!("PRAGMA {}.foreign_key_check", quote_ident(a)),
                None => "PRAGMA foreign_key_check".to_string(),
            };
            if let Ok((_c, rows)) = target_conn_guard.query_all_string_rows(&fk_check_sql, &[]) {
                if !rows.is_empty() {
                    let mut by_table: std::collections::BTreeMap<String, usize> =
                        std::collections::BTreeMap::new();
                    for r in &rows {
                        if let Some(t) = r.first() {
                            *by_table.entry(t.clone()).or_insert(0) += 1;
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
        // "data destroyed" (review C1).
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

                // Register the new entity in the target namespace.
                // Entity type: 1 = table, 2 = view
                let entity_type = match entity.materialized {
                    Materialized::View => 2,
                    Materialized::CtasTable | Materialized::DeclaredTable { .. } => 1,
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
                            .query_all_string_rows(&pragma, &[])
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
                            .filter_map(|r| Some((r.get(1)?.clone(), r.get(2)?.clone())))
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
                    Materialized::View
                    | Materialized::DeclaredTable { insert: None, .. } => "created",
                };
                results.push((
                    entity_name.clone(),
                    status.to_string(),
                    entity.qualified_create.clone(),
                ));
            }

            // Linear imprint: consume the source into a blueprint archive under
            // the target (namespace-catechism §V). Moves the source namespace,
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
                // Blueprint inertness (M2): refuse to resolve any entity through
                // an archived blueprint namespace (or a descendant of one). This
                // is the sole namespace-qualified resolution chokepoint — bare
                // table lookups use `lookup_table` and never reach here, so the
                // scan stays off the hot path. The catalog functor
                // (`{blueprint}::(*)`) resolves through `sys::meta`, not this
                // path, so it stays visible (pinned by companion_linear--61).
                refuse_if_blueprint(&conn, &fq_name)?;
                // §IV plain-qualifier SHADOW (ratified softening): the exact
                // top-level name won; if an enlisted `home::{fq_name}` sits
                // behind it, warn that it is shadowed and needs its full path.
                if home_child_shadows(&conn, &fq_name) {
                    log::warn!(
                        "plain qualifier '{n}' resolved to the top-level namespace \
                         '{n}'; an enlisted scratch child 'home::{n}' is shadowed \
                         behind it — spell 'home::{n}' to reach it (namespace-catechism \
                         §IV, ratified top-level-wins softening of home-first)",
                        n = fq_name
                    );
                }
                id
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                // §IV MIDDLE ACCESS RUNG: the exact fq missed — consult the
                // enlist set for an enlisted namespace whose DIRECT child bears
                // this plain name (home first). Fires ONLY here, on a confirmed
                // miss, so no path that resolves today is affected (rule 1). The
                // expanded fq re-enters via the same blueprint guard.
                match expand_plain_namespace(&conn, &fq_name)? {
                    Some(expanded) => match conn.query_row(
                        "SELECT id FROM namespace WHERE fq_name = ?1",
                        [&expanded],
                        |row| row.get::<_, i64>(0),
                    ) {
                        Ok(id) => {
                            refuse_if_blueprint(&conn, &expanded)?;
                            id
                        }
                        _ => return Ok(None),
                    },
                    None => {
                        // Namespace not found
                        debug!("resolve_namespace_path: Namespace '{}' not found", fq_name);
                        return Ok(None);
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

        // Step 2: Get the backend schema (source_ns) and connection_id for this namespace
        // First try to find cartridges with activated entities
        let result = conn.query_row(
            "SELECT DISTINCT c.source_ns, c.connection_id
             FROM activated_entity ae
             JOIN cartridge c ON ae.cartridge_id = c.id
             WHERE ae.namespace_id = ?1
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

    /// Resolve an unqualified entity name to its namespace path
    ///
    /// Queries the bootstrap metadata to find where an entity is activated
    /// and whether it's accessible from the current namespace.
    ///
    /// # Algorithm
    /// 1. Look up namespace_id for current_namespace (e.g., "main")
    /// 2. Search activated_entity for entity_name in:
    ///    - Current namespace
    ///    - Engaged namespaces (via enlisted_namespace table)
    /// 3. If found, return the namespace path
    /// 4. If not found, return None
    ///
    /// # Arguments
    /// * `entity_name` - Unqualified entity name (e.g., "team")
    /// * `current_namespace` - Current namespace (typically "main")
    ///
    /// # Returns
    /// * `Ok(Some(namespace_path))` - Entity found in accessible namespace
    /// * `Ok(None)` - Entity not found or not accessible
    /// * `Err(...)` - Database error during resolution
    /// Resolve an unqualified entity name within a namespace scope.
    ///
    /// Searches `current_namespace` and its enlisted namespaces. When
    /// `fallback_namespace` is provided and the primary search yields no
    /// results, the fallback scope is searched too. This supports DDL view
    /// body resolution: the DDL namespace is primary (with its own enlists),
    /// and "main" is the fallback for database tables not in any enlist.
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
            ),
            reachable(ns_id) AS (
                SELECT ns_id FROM direct
                UNION
                SELECT exp.exposed_namespace_id
                FROM exposed_namespace exp
                JOIN reachable r ON r.ns_id = exp.exposing_namespace_id
            )
            SELECT DISTINCT n.fq_name, e.name
            FROM activated_entity ae
            JOIN entity e ON ae.entity_id = e.id
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

        let matches: Vec<(String, String)> = stmt
            .query_map(rusqlite::params![entity_name, current_ns_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
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
                     SELECT DISTINCT n.fq_name, e.name
                     FROM activated_entity ae
                     JOIN entity e ON ae.entity_id = e.id
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

            let fallback_matches: Vec<(String, String)> = fallback_stmt
                .query_map(rusqlite::params![entity_name, fallback_ns_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
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
                if !all_matches.iter().any(|(ns, _)| *ns == m.0) {
                    all_matches.push(m);
                }
            }
        }

        Self::resolve_matches(all_matches, entity_name, current_namespace)
    }

    /// Helper: interpret a set of entity matches — 0 = not found, 1 = found, 2+ = ambiguous.
    fn resolve_matches(
        matches: Vec<(String, String)>,
        entity_name: &str,
        scope_namespace: &str,
    ) -> Result<
        Option<(
            delightql_types::namespace::NamespacePath,
            delightql_types::SqlIdentifier,
        )>,
    > {
        match matches.len() {
            0 => Ok(None),
            1 => {
                let (fq_name, canonical_name) = &matches[0];
                let parts: Vec<String> = fq_name.split("::").map(|s| s.to_string()).collect();
                let namespace_path = delightql_types::namespace::NamespacePath::from_parts(parts);
                Ok(Some((
                    namespace_path,
                    delightql_types::SqlIdentifier::new(canonical_name),
                )))
            }
            _ => {
                // Multiple matches from different namespaces — ambiguous.
                let namespaces: Vec<&str> = matches.iter().map(|(ns, _)| ns.as_str()).collect();
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
    /// (`temp_table!`/`table!`/`temp_view!` — materialize-pipe.md §1:
    /// "catalog-registered … its name resolves like any table's") so
    /// post-run statements resolve it bare. Called by the Epic-3.3 entry
    /// point (relay/entry.rs) after a successful run; pinned by the
    /// effects ball's ddl_receipt--12/--13/--14 and util--36 post-state
    /// reads.
    ///
    /// The recipe is `imprint_namespace`'s registration tail: read the
    /// engine-typed columns back via CONNECTION-APPROPRIATE introspection
    /// SQL (`created_object_readback_sql` — PRAGMA table_info on
    /// SQLite/DuckDB, information_schema on postgres; E-T4, P2 "what
    /// breaks first" item 7: PRAGMA on a PG connection silently registered
    /// nothing), retire the run's own stale registration (fresh scratch
    /// per run), then write entity + output_column attributes +
    /// activation, on a per-connection `session://materialized` cartridge
    /// with NO schema alias — the generator then spells reads unqualified,
    /// which is exactly how SQLite finds temp-schema objects
    /// (materialize-pipe §3).
    ///
    /// The retirement is SCOPED to the session cartridge (F2, RULED
    /// 2026-07-11): a same-name mount-introspected physical entity keeps
    /// its registration — the temp SHADOWS it for unqualified resolution
    /// (materialize-pipe §6, a preference, not a catalog edit), and a
    /// qualified read still reaches it. Pinned by session_shadow_tests::
    /// {physical_registration_survives_temp_registration,
    /// reregistration_retires_prior_session_entry_only} and the effects
    /// ball's scratch--52_qualified_read_reaches_physical.
    ///
    /// Returns Ok(false) when there is nothing to register: the object
    /// does not exist (an exit-flagged run skipped its CREATE) or the
    /// namespace 'main' is absent. materialize-pipe's full nominal
    /// placement (`<conn-ns>::temp` mirror + scoped enlistment edges) is
    /// that spec's own later work; this is the session-catalog slice the
    /// entry points need.
    pub fn register_run_created_object(
        &mut self,
        name: &str,
        is_view: bool,
        connection_id: i64,
    ) -> Result<bool> {
        // 1. Read the columns back from the object's own connection, with
        // the connection's own introspection SQL (E-T4: the read-back is
        // dialect-selected; a PG mount whose mounted schema is unknowable
        // ABSTAINS — the F7/abstain doctrine, never a guessed schema).
        let dialect = self.dialect_for_connection(Some(connection_id));
        let mounted_schema = if matches!(
            dialect,
            crate::pipeline::generator_v3::SqlDialect::PostgreSQL
        ) {
            self.mounted_engine_schema_for_connection(connection_id)?
        } else {
            None
        };
        let Some((readback_sql, name_col, type_col)) =
            created_object_readback_sql(dialect, name, mounted_schema.as_deref())
        else {
            return Ok(false); // PG with no derivable mounted schema: abstain
        };
        let conn_arc = if connection_id == 2 {
            self.connection.clone()
        } else {
            self.get_connection(connection_id)?
        };
        let attr_cols: Vec<(String, String)> = {
            let guard = conn_arc.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire connection lock for created-object read-back",
                    format!("Connection was poisoned: {}", e),
                )
            })?;
            match guard.query_all_string_rows(&readback_sql, &[]) {
                Ok((_cols, rows)) => rows
                    .iter()
                    .filter_map(|r| Some((r.get(name_col)?.clone(), r.get(type_col)?.clone())))
                    .collect(),
                Err(_) => Vec::new(),
            }
        };
        if attr_cols.is_empty() {
            return Ok(false); // not created (e.g. skipped past the exit flag)
        }

        // 2. Target namespace: the object's OWN connection's namespace —
        // `main` for the primary, the mounted namespace for an external
        // connection (REPORT-3.3 discovery 5's revisit: registering
        // cross-connection creations into `main` put them in a namespace
        // their connection cannot serve). Falls back to `main` when the
        // connection has no resolvable namespace.
        let ns_fq = self
            .connection_namespace_fq(connection_id)?
            .unwrap_or_else(|| "main".to_string());

        let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for created-object registration",
                format!("Connection was poisoned: {}", e),
            )
        })?;

        let ns_id: i64 = match bootstrap_conn
            .query_row(
                "SELECT id FROM namespace WHERE fq_name = ?1",
                [ns_fq.as_str()],
                |row| row.get(0),
            )
            .ok()
        {
            Some(id) => id,
            None => return Ok(false),
        };

        // 3. The per-connection session cartridge (created on first use).
        let cartridge_id: i64 = match bootstrap_conn
            .query_row(
                "SELECT id FROM cartridge
                 WHERE source_uri = 'session://materialized' AND connection_id = ?1",
                [connection_id],
                |row| row.get(0),
            )
            .ok()
        {
            Some(id) => id,
            None => {
                bootstrap_conn
                    .execute(
                        "INSERT INTO cartridge (language, source_type_enum, source_uri, \
                         source_ns, connected, connection_id, is_universal)
                         VALUES (?1, ?2, 'session://materialized', NULL, 1, ?3, 0)",
                        rusqlite::params![
                            3, // SQLite language ID
                            SourceType::Db.as_i32(),
                            connection_id,
                        ],
                    )
                    .map_err(|e| {
                        DelightQLError::database_error(
                            "Failed to create session-materialization cartridge",
                            e.to_string(),
                        )
                    })?;
                bootstrap_conn.last_insert_rowid()
            }
        };

        // 4. Retire the run's own stale registration (a re-run re-creates).
        // SCOPED to the session cartridge: a same-name entity from any other
        // cartridge (e.g. the mount-introspected physical table) keeps its
        // registration — the shadow is a resolution preference, not a
        // catalog edit (F2 ruling; materialize-pipe §6). Pinned by
        // session_shadow_tests::physical_registration_survives_temp_registration.
        let stale_ids: Vec<i64> = {
            let mut stmt = bootstrap_conn
                .prepare(
                    "SELECT e.id FROM entity e
                     JOIN activated_entity ae ON ae.entity_id = e.id
                     WHERE ae.namespace_id = ?1 AND e.name = ?2
                       AND e.cartridge_id = ?3",
                )
                .map_err(|e| {
                    DelightQLError::database_error("query stale created entity", e.to_string())
                })?;
            let rows = stmt
                .query_map(rusqlite::params![ns_id, name, cartridge_id], |r| r.get(0))
                .map_err(|e| {
                    DelightQLError::database_error("query stale created entity", e.to_string())
                })?;
            rows.filter_map(|r| r.ok()).collect()
        };
        for eid in stale_ids {
            let _ = bootstrap_conn
                .execute("DELETE FROM activated_entity WHERE entity_id = ?1", [eid]);
            let _ = bootstrap_conn
                .execute("DELETE FROM entity_attribute WHERE entity_id = ?1", [eid]);
            let _ =
                bootstrap_conn.execute("DELETE FROM entity_clause WHERE entity_id = ?1", [eid]);
            let _ = bootstrap_conn.execute("DELETE FROM entity WHERE id = ?1", [eid]);
        }

        // 5. Entity + attributes + activation (entity type: 1 table, 2 view).
        bootstrap_conn
            .execute(
                "INSERT INTO entity (name, type, cartridge_id, doc) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    name,
                    if is_view { 2 } else { 1 },
                    cartridge_id,
                    "Session-materialized by a DDL directive",
                ],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to register created object '{}'", name),
                    e.to_string(),
                )
            })?;
        let entity_id = bootstrap_conn.last_insert_rowid();
        for (position, (col_name, col_type)) in attr_cols.iter().enumerate() {
            bootstrap_conn
                .execute(
                    "INSERT INTO entity_attribute (entity_id, attribute_name, attribute_type, \
                     data_type, position, is_nullable, default_value)
                     VALUES (?1, ?2, 'output_column', ?3, ?4, 1, NULL)",
                    rusqlite::params![entity_id, col_name, col_type, position as i64 + 1],
                )
                .map_err(|e| {
                    DelightQLError::database_error(
                        format!("Failed to register attribute '{}' for '{}'", col_name, name),
                        e.to_string(),
                    )
                })?;
        }
        bootstrap_conn
            .execute(
                "INSERT INTO activated_entity (entity_id, namespace_id, cartridge_id) \
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![entity_id, ns_id, cartridge_id],
            )
            .map_err(|e| {
                DelightQLError::database_error(
                    format!("Failed to activate created object '{}'", name),
                    e.to_string(),
                )
            })?;

        // Arm the shadow-split probe (see `session_materialized_names`).
        self.session_materialized_names.set(true);

        Ok(true)
    }

    /// The F2 shadow split: when BOTH a session-materialized entity
    /// (`register_run_created_object`'s cartridge) and an entity from any
    /// other cartridge (e.g. a mount-introspected physical table) hold
    /// `entity_name` activated in `namespace_fq`, return their entity ids
    /// as `(session, competitor)`. `None` when there is no such collision.
    ///
    /// This is the seam qualified resolution uses to punch through the
    /// temp shadow (materialize-pipe §6: the shadow covers UNQUALIFIED
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
            .map_err(|e| {
                DelightQLError::database_error("shadow-split probe", e.to_string())
            })?;
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
        let Ok((_cols, rows)) = guard.query_all_string_rows("PRAGMA database_list", &[]) else {
            return Ok(None);
        };
        Ok(rows.iter().find_map(|row| {
            let alias = row.get(1)?;
            let file = row.get(2)?;
            if file.is_empty() {
                return None; // temp / :memory: — never a punch-through target
            }
            (std::fs::canonicalize(file).ok()? == want).then(|| alias.clone())
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
            .map_err(|e| {
                DelightQLError::database_error("entity column query", e.to_string())
            })?;
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
            .map_err(|e| {
                DelightQLError::database_error("entity column query", e.to_string())
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(cols)
    }

    /// The catalog namespace a connection's entities live under: `main`
    /// for the primary user connection (id 2, the register_connection
    /// convention entry.rs and `register_run_created_object` share), else
    /// the namespace whose mount-introspected entities carry the
    /// connection's cartridge. Used by Epic 4.1 to key durable placement,
    /// the durable clash universe, and created-object registration on the
    /// CONNECTION rather than an unconditional `main` (materialize-pipe §2
    /// counts connections; REPORT-3.3 discovery 5). Pinned (primary case,
    /// end-to-end) by the CLI integration test
    /// `table_bang_persists_to_the_db_file_across_sessions`; the
    /// non-primary lookup is exercised by the effect_transformer
    /// two-connection refusal tests and remains end-to-end unpinned until
    /// a real second engine is testable (PG/DuckDB ferry).
    pub fn connection_namespace_fq(&self, connection_id: i64) -> Result<Option<String>> {
        if connection_id == 2 {
            return Ok(Some("main".to_string()));
        }
        let conn = self.bootstrap_connection.lock().map_err(|e| {
            DelightQLError::connection_poison_error(
                "Failed to acquire bootstrap lock for connection-namespace lookup",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        Ok(conn
            .query_row(
                "SELECT n.fq_name
                 FROM activated_entity ae
                 JOIN entity e ON e.id = ae.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 JOIN namespace n ON n.id = ae.namespace_id
                 WHERE c.connection_id = ?1
                   AND c.source_uri <> 'session://materialized'
                 LIMIT 1",
                [connection_id],
                |row| row.get(0),
            )
            .ok())
    }

    /// The ENGINE SCHEMA a namespace's mount binds — R-T4's durable home on
    /// targets, now a per-mount RECORDED fact (schema-mount Phase A,
    /// EFFECTS-ON-TARGETS-PLAN §4.1, ratified 2026-07-12; it closes the
    /// E-T4 coupling note this method's comment used to carry). The fact is
    /// the cartridge's `source_ns`, written by `register_external_connection`
    /// from `ConnectionComponents.mounted_schema`:
    /// - a SPELLED schema (Phase B `#schema` / Phase C `mount_tree!`) is
    ///   returned verbatim — the mount introspected THAT schema, so durable
    ///   placement and read qualification agree with it;
    /// - a NULL `source_ns` (a bare mount — "the engine's own default")
    ///   resolves DOWNSTREAM here to the engine default by connection type
    ///   (3 = postgres → `public`, 4 = duckdb → `main`), so a bare mount is
    ///   byte-identical to the pre-Phase-A derivation while its reads stay
    ///   unqualified;
    /// - anything else — the SQLite primary (alias-recovery territory),
    ///   siso pipes, missing rows — answers `None`, and the PG durable CTAS
    ///   REFUSES rather than emit search_path-fragile unqualified DDL
    ///   (REPORT-T-P1 §E).
    ///
    /// NAMESPACE-keyed because one connection legitimately holds MANY
    /// schemas under `mount_tree!` (R-S1); the connection-keyed shim below
    /// exists only for callers holding just a connection whose
    /// namespace↔schema mapping is 1:1 today. Pinned by
    /// `pg_table_bang_ctas_spells_the_mounted_schema_and_registers_on_the_connection`
    /// (effect_transformer/tests.rs) and
    /// `mount_records_the_schema_as_source_ns_and_the_lookup_reads_it`
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
        // The mount cartridge (never the session-materialization cartridge,
        // whose source_ns is deliberately NULL) carries both the recorded
        // schema and the connection type that seeds the default.
        let recorded: Option<(Option<String>, Option<i64>)> = conn
            .query_row(
                "SELECT c.source_ns, co.connection_type
                 FROM activated_entity ae
                 JOIN entity e ON e.id = ae.entity_id
                 JOIN cartridge c ON c.id = e.cartridge_id
                 JOIN namespace n ON n.id = ae.namespace_id
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

    /// Connection-keyed shim over `mounted_engine_schema_for_namespace`
    /// (schema-mount Phase A): the durable-placement and read-back callers
    /// hold the object's CONNECTION and create in that connection's
    /// namespace, a 1:1 mapping today. Routes to the recorded fact via the
    /// connection's namespace; a connection with no resolvable namespace
    /// falls back to the connection-type default directly (defensive — the
    /// SQLite-primary/siso rows that answered `None` before Phase A).
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
    /// (EFFECT-ALGEBRA §3, ruled 2026-07-11: replacement is by NAME, not
    /// kind; the replace DROP must match whatever HOLDS the name). Gated
    /// like `session_shadow_split`: sessions that never materialized
    /// anything answer without touching bootstrap. An object minted
    /// outside the catalog (legacy-path DDL) stays invisible here and
    /// surfaces the engine's own error — the F7 doctrine. Pinned by the
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
        Ok(kind.map(|t| t == 2))
    }

    /// Refresh a data namespace by re-introspecting its source database.
    ///
    /// Clears all entity metadata and re-discovers entities from the same
    /// database source. Preserves namespace identity, enlistments, aliases,
    /// and groundings. Validates grounding contracts after refresh.
    pub fn refresh_namespace(&mut self, namespace: &str) -> Result<usize> {
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
        let cartridge_meta: Option<(i64, Option<i64>, Option<String>, Option<String>)> =
            bootstrap_conn
                .query_row(
                    "SELECT c.id, c.connection_id, c.source_ns, c.source_uri
                 FROM cartridge c
                 JOIN entity e ON e.cartridge_id = c.id
                 JOIN activated_entity ae ON ae.entity_id = e.id
                 WHERE ae.namespace_id = ?1
                 LIMIT 1",
                    [ns_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .ok();

        let (connection_id, schema_alias, source_uri) = match &cartridge_meta {
            Some((_, conn_id, src_ns, src_uri)) => (
                conn_id.unwrap_or(2),
                src_ns.clone(),
                src_uri.clone().unwrap_or_default(),
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

        // 3. Begin transaction
        bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
            DelightQLError::database_error("Failed to begin refresh transaction", e.to_string())
        })?;

        // 4. Clear contents
        let clear_result = Self::clear_namespace_contents(&bootstrap_conn, ns_id);
        if let Err(e) = clear_result {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
            return Err(e);
        }

        // 5. Re-introspect
        let entities = if connection_id == 2 {
            // ATTACH path: use schema alias
            let alias = schema_alias.as_deref().unwrap_or(namespace);
            match self.introspector.introspect_entities_in_schema(alias) {
                Ok(e) => e,
                Err(e) => {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
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
                            let _ = bootstrap_conn.execute_batch("ROLLBACK");
                            return Err(DelightQLError::database_error(
                                format!("Failed to create connection for refresh: {}", e),
                                e.to_string(),
                            ));
                        }
                    };
                    match components.introspector.introspect_entities() {
                        Ok(e) => e,
                        Err(e) => {
                            let _ = bootstrap_conn.execute_batch("ROLLBACK");
                            return Err(DelightQLError::database_error(
                                format!("Failed to re-introspect '{}': {}", source_uri, e),
                                e.to_string(),
                            ));
                        }
                    }
                }
                None => {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    return Err(DelightQLError::database_error(
                        "Cannot refresh factory-mounted namespace without connection factory",
                        "No connection factory",
                    ));
                }
            }
        };

        // 6. Re-register: new cartridge + entities
        let cartridge_id = {
            let language = if connection_id == 2 {
                3
            } else {
                // Determine from source_uri
                if source_uri.starts_with("fatboy://duckdb/") {
                    4
                } else if source_uri.starts_with("postgres://")
                    || source_uri.starts_with("postgresql://")
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
                    schema_alias.as_deref(),
                    connection_id,
                ],
            ).map_err(|e| {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                DelightQLError::database_error("Failed to create refresh cartridge", e.to_string())
            })?;
            bootstrap_conn.last_insert_rowid() as i32
        };

        let entity_count = entities.len();
        if let Err(e) = crate::bootstrap::introspect::insert_discovered_entities(
            &bootstrap_conn,
            cartridge_id,
            &entities,
        ) {
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
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
            let _ = bootstrap_conn.execute_batch("ROLLBACK");
            return Err(DelightQLError::database_error(
                format!("Failed to activate entities: {}", e),
                e.to_string(),
            ));
        }

        // 7. Validate groundings: check all grounded namespaces borrowing this data ns
        {
            let mut gnd_stmt = bootstrap_conn
                .prepare(
                    "SELECT g.grounded_namespace_id, g.lib_namespace_id, gn.fq_name, ln.fq_name
                 FROM grounding g
                 JOIN namespace gn ON gn.id = g.grounded_namespace_id
                 JOIN namespace ln ON ln.id = g.lib_namespace_id
                 WHERE g.data_namespace_id = ?1",
                )
                .map_err(|e| {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    DelightQLError::database_error("Failed to query groundings", e.to_string())
                })?;
            let groundings: Vec<(i64, i64, String, String)> = gnd_stmt
                .query_map([ns_id], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .map_err(|e| {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    DelightQLError::database_error("Failed to query groundings", e.to_string())
                })?
                .flatten()
                .collect();

            for (_grounded_id, lib_id, _grounded_fq, lib_fq) in &groundings {
                if let Err(e) = Self::validate_grounding_contract(
                    &bootstrap_conn,
                    *lib_id,
                    lib_fq,
                    ns_id,
                    namespace,
                ) {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    return Err(e);
                }
            }
        }

        // 8. Commit
        bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
            DelightQLError::database_error("Failed to commit refresh transaction", e.to_string())
        })?;

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

        let (cleaned_source, directives) =
            crate::bin_cartridge::prelude::consult::extract_embedded_directives(&source)?;

        // Save enlist/alias state before processing embedded directives
        let saved_enlisted = self.save_enlisted_state()?;
        let saved_aliases = self.save_alias_state()?;

        // Execute embedded directives (resolve .:: and :: prefixes relative to namespace)
        // THE LIMINAL RELATION (EFFECT-ALGEBRA §8): reconsult REPLACES the
        // ledger whole — clear_namespace_contents deletes the old rows and
        // consult_file_inner re-inserts THIS pass's receipts, both inside the
        // reconsult transaction (pinned by
        // `liminal_ledger_reconsult_replaces_whole`).
        let mut liminal_receipts: Vec<LiminalReceipt> = Vec::new();
        for directive in &directives {
            liminal_receipts.push(crate::bin_cartridge::prelude::consult::liminal_receipt_for(
                &directive.name,
                &directive.args,
            ));
            match directive.name.as_str() {
                "consult" => {
                    if directive.args.len() == 2 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[1], namespace)?;
                        crate::bin_cartridge::prelude::consult::execute_consult(
                            self,
                            &directive.args[0],
                            &resolved_ns,
                            Some(namespace),
                        )?;
                    }
                }
                "mount" => {
                    if directive.args.len() == 2 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[1], namespace)?;
                        self.mount_database(&directive.args[0], &resolved_ns)?;
                    }
                }
                "enlist" => {
                    if directive.args.len() == 1 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[0], namespace)?;
                        self.enlist_namespace(&resolved_ns)?;
                    }
                }
                "delist" => {
                    if directive.args.len() == 1 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[0], namespace)?;
                        self.delist_namespace(&resolved_ns)?;
                    }
                }
                "alias" => {
                    if directive.args.len() == 2 {
                        let resolved_ns = crate::bin_cartridge::prelude::consult::resolve_ns_prefix(&directive.args[0], namespace)?;
                        self.register_namespace_alias(&directive.args[1], &resolved_ns)?;
                    }
                }
                other => panic!("catch-all hit in system.rs reconsult_namespace directive processing: unexpected directive name: {}", other),
            }
        }

        // Parse DDL
        let ddl = crate::pipeline::parser::parse_ddl_file(&cleaned_source).map_err(|e| {
            DelightQLError::database_error(
                format!("reconsult!() failed to parse '{}': {}", file_path, e),
                "Parse error",
            )
        })?;

        if ddl.definitions.is_empty() {
            // Restore state and return error
            let _ = self.restore_enlisted_state(&saved_enlisted);
            let _ = self.restore_alias_state(&saved_aliases);
            return Err(DelightQLError::database_error(
                format!(
                    "reconsult!() failed: '{}' contains no DDL definitions.",
                    file_path
                ),
                "Not a DDL file",
            ));
        }

        // 4. Transaction: clear old contents, insert new, validate groundings
        let entity_count = {
            let bootstrap_conn = self.bootstrap_connection.lock().map_err(|e| {
                DelightQLError::connection_poison_error(
                    "Failed to acquire bootstrap database lock for reconsult",
                    format!("Connection was poisoned: {}", e),
                )
            })?;

            bootstrap_conn.execute_batch("BEGIN").map_err(|e| {
                DelightQLError::database_error(
                    "Failed to begin reconsult transaction",
                    e.to_string(),
                )
            })?;

            // 5. Clear old contents
            if let Err(e) = Self::clear_namespace_contents(&bootstrap_conn, ns_id) {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(e);
            }

            // 6. Insert new entities (via consult_file_inner — namespace row already exists)
            let count = ddl.definitions.len();
            let result = Self::consult_file_inner(
                &bootstrap_conn,
                &file_path,
                namespace,
                ddl,
                count,
                None,
                &liminal_receipts,
            );
            if let Err(e) = result {
                let _ = bootstrap_conn.execute_batch("ROLLBACK");
                return Err(e);
            }
            let entity_count = result.unwrap().definitions_loaded;

            // 7. Validate + rebuild groundings
            {
                let mut gnd_stmt = bootstrap_conn.prepare(
                    "SELECT g.grounded_namespace_id, g.data_namespace_id, gn.fq_name, dn.fq_name
                     FROM grounding g
                     JOIN namespace gn ON gn.id = g.grounded_namespace_id
                     JOIN namespace dn ON dn.id = g.data_namespace_id
                     WHERE g.lib_namespace_id = ?1",
                ).map_err(|e| {
                    let _ = bootstrap_conn.execute_batch("ROLLBACK");
                    DelightQLError::database_error("Failed to query groundings for reconsult", e.to_string())
                })?;
                let groundings: Vec<(i64, i64, String, String)> = gnd_stmt
                    .query_map([ns_id], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })
                    .map_err(|e| {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        DelightQLError::database_error(
                            "Failed to query groundings for reconsult",
                            e.to_string(),
                        )
                    })?
                    .flatten()
                    .collect();

                for (grounded_id, data_id, _grounded_fq, data_fq) in &groundings {
                    if let Err(e) = Self::validate_grounding_contract(
                        &bootstrap_conn,
                        ns_id,
                        namespace,
                        *data_id,
                        data_fq,
                    ) {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        return Err(e);
                    }

                    if let Err(e) = Self::rebuild_grounded_namespace(
                        &bootstrap_conn,
                        *grounded_id,
                        namespace,
                        data_fq,
                    ) {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        return Err(e);
                    }
                }
            }

            // 8. Update source_path if new file was provided
            if new_file_path.is_some() {
                bootstrap_conn
                    .execute(
                        "UPDATE namespace SET source_path = ?1 WHERE id = ?2",
                        rusqlite::params![&file_path, ns_id],
                    )
                    .map_err(|e| {
                        let _ = bootstrap_conn.execute_batch("ROLLBACK");
                        DelightQLError::database_error(
                            "Failed to update source_path",
                            e.to_string(),
                        )
                    })?;
            }

            // 9. Commit
            bootstrap_conn.execute_batch("COMMIT").map_err(|e| {
                DelightQLError::database_error(
                    "Failed to commit reconsult transaction",
                    e.to_string(),
                )
            })?;

            entity_count
        }; // bootstrap_conn dropped here

        // 10. Record namespace-local enlists/aliases, restore caller state
        let current_enlisted = self.save_enlisted_state()?;
        let current_aliases = self.save_alias_state()?;
        let new_enlists: Vec<(i32, i32)> = current_enlisted
            .iter()
            .filter(|row| !saved_enlisted.contains(row))
            .cloned()
            .collect();
        let new_aliases: Vec<(String, i32)> = current_aliases
            .iter()
            .filter(|row| !saved_aliases.contains(row))
            .cloned()
            .collect();

        if !new_enlists.is_empty() {
            self.record_namespace_local_enlists(namespace, &new_enlists)?;
        }
        if !new_aliases.is_empty() {
            self.record_namespace_local_aliases(namespace, &new_aliases)?;
        }
        self.restore_enlisted_state(&saved_enlisted)?;
        self.restore_alias_state(&saved_aliases)?;

        debug!(
            "reconsult_namespace: Reconsulted namespace '{}' from '{}' with {} entities",
            namespace, file_path, entity_count
        );

        Ok(entity_count)
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
                DelightQLError::database_error(
                    "Corrupt liminal receipt echo list",
                    e.to_string(),
                )
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
    use crate::pipeline::asts::core::Query;

    match query {
        Query::Relational(rel_expr) => {
            walk_relational_for_tree_groups(conn, entity_id, rel_expr)?;
        }
        Query::WithCtes {
            ctes,
            query: main_expr,
        } => {
            walk_relational_for_tree_groups(conn, entity_id, main_expr)?;
            for cte in ctes {
                walk_relational_for_tree_groups(conn, entity_id, &cte.expression)?;
            }
        }
        // WithCfes: recurse into inner query
        Query::WithCfes { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
        // WithPrecompiledCfes: recurse into inner query
        Query::WithPrecompiledCfes { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
        // ReplTempTable/ReplTempView: recurse into inner query
        Query::ReplTempTable { query: inner, .. } | Query::ReplTempView { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
        // WithErContext: consumed before registration — shouldn't appear
        Query::WithErContext { query: inner, .. } => {
            register_interior_schemas_from_query(conn, entity_id, inner)?;
        }
    }

    Ok(())
}

#[stacksafe::stacksafe]
fn walk_relational_for_tree_groups(
    conn: &Connection,
    entity_id: i32,
    expr: &crate::pipeline::asts::core::RelationalExpression<
        crate::pipeline::asts::core::Unresolved,
    >,
) -> Result<()> {
    use crate::pipeline::asts::core::specs::ModuloSpec;
    use crate::pipeline::asts::core::{RelationalExpression, UnaryRelationalOperator};

    match expr {
        RelationalExpression::Pipe(pipe) => {
            // Walk source
            walk_relational_for_tree_groups(conn, entity_id, &pipe.source)?;
            // Check operator for tree groups
            match &pipe.operator {
                UnaryRelationalOperator::Modulo { spec, .. } => {
                    if let ModuloSpec::GroupBy { reducing_on, .. } = spec {
                        for ode in reducing_on {
                            register_tree_group_from_domain_expr(conn, entity_id, &ode.expr)?;
                        }
                    }
                }
                // All non-Modulo operators: no tree groups to register
                // (General, ProjectOut, Embed, MapCover, RenameCover, Transform, etc.)
                _ => {}
            }
        }
        RelationalExpression::SetOperation { operands, .. } => {
            for operand in operands {
                walk_relational_for_tree_groups(conn, entity_id, operand)?;
            }
        }
        // Relation: leaf — no tree groups
        RelationalExpression::Relation(_) => {}
        // Filter: recurse into source
        RelationalExpression::Filter { source, .. } => {
            walk_relational_for_tree_groups(conn, entity_id, source)?;
        }
        // Join: recurse both sides
        RelationalExpression::Join { left, right, .. } => {
            walk_relational_for_tree_groups(conn, entity_id, left)?;
            walk_relational_for_tree_groups(conn, entity_id, right)?;
        }
        // ER chains: walk the contained relations for tree groups
        RelationalExpression::ErJoinChain { relations, .. } => {
            for rel in relations {
                walk_relational_for_tree_groups(
                    conn,
                    entity_id,
                    &RelationalExpression::Relation(rel.clone()),
                )?;
            }
        }
        RelationalExpression::ErTransitiveJoin { left, right, .. } => {
            walk_relational_for_tree_groups(conn, entity_id, left)?;
            walk_relational_for_tree_groups(conn, entity_id, right)?;
        }
        RelationalExpression::IntersectCorresponding { .. } => {
            unreachable!("IntersectCorresponding only exists in Refined/Addressed phases")
        }
    }

    Ok(())
}

/// If a domain expression is a Curly (tree group) with an alias, register it
/// as an interior_entity with its members as interior_entity_attribute rows.
fn register_tree_group_from_domain_expr(
    conn: &Connection,
    entity_id: i32,
    expr: &crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Unresolved>,
) -> Result<()> {
    use crate::pipeline::asts::core::{DomainExpression, FunctionExpression};

    if let DomainExpression::Function(FunctionExpression::Curly {
        members,
        alias: Some(alias),
        ..
    }) = expr
    {
        let alias_str = alias.as_str();
        // Insert interior_entity
        conn.execute(
            "INSERT INTO interior_entity (parent_entity_id, column_name) VALUES (?1, ?2)",
            rusqlite::params![entity_id, alias_str],
        )
        .map_err(|e| {
            DelightQLError::database_error_with_source(
                "Failed to insert interior_entity",
                e.to_string(),
                Box::new(e),
            )
        })?;
        let interior_entity_id = conn.last_insert_rowid() as i32;

        // Insert members as interior_entity_attribute rows
        register_curly_members(conn, interior_entity_id, entity_id, members)?;
    }

    Ok(())
}

/// Register curly members as interior_entity_attribute rows.
/// Handles nesting: if a member is a nested tree group, recurse.
fn register_curly_members(
    conn: &Connection,
    interior_entity_id: i32,
    parent_entity_id: i32,
    members: &[crate::pipeline::asts::core::CurlyMember<crate::pipeline::asts::core::Unresolved>],
) -> Result<()> {
    use crate::pipeline::asts::core::{CurlyMember, DomainExpression, FunctionExpression};

    for (position, member) in members.iter().enumerate() {
        match member {
            CurlyMember::Shorthand { column, .. } => {
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
            CurlyMember::KeyValue {
                key,
                value,
                nested_reduction,
                ..
            } => {
                if *nested_reduction {
                    // Nested tree group: create a child interior_entity
                    if let DomainExpression::Function(FunctionExpression::Curly {
                        members: child_members,
                        ..
                    }) = value.as_ref()
                    {
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
                        register_curly_members(
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
                } else {
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
            }
            other => panic!("catch-all hit in system.rs register_curly_members: unexpected CurlyMember variant: {:?}", other),
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
    Failed { phase: LoadPhase, error: DelightQLError },
}

impl StdlibLoad {
    /// True only when a module was newly loaded this call (caller should
    /// retry the lookup that triggered the load).
    pub fn just_loaded(&self) -> bool {
        matches!(self, StdlibLoad::Loaded)
    }
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
            if let Err(e) = crate::pipeline::parser::parse_ddl_file(src) {
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
mod mount_new_database_tests {
    //! `mount_new!` (EFFECT-ALGEBRA §6): PROVISION a fresh, valid, empty SQLite
    //! database and bind it — the create-intent counterpart of `mount!`. These
    //! pin the three new behaviors (materialization, clobber refusal, v1
    //! SQLite-only scope) + reserved-name inheritance. The end-to-end
    //! round-trip (mount_new! then mount! the same file, with a real read-back)
    //! is the CLI `mount_new_roundtrip` integration test.

    use super::DelightQLSystem;
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
        assert!(len > 0, "materialized db must be non-empty, got {len} bytes");
        assert!(is_valid_sqlite(&path), "materialized db must carry the SQLite header");

        // The namespace is registered and reachable: enlist_namespace requires
        // the namespace to exist (the enlisted-guard-classification test's
        // proof-of-registration pattern). resolve_namespace_path returns None
        // for an EMPTY db (no activated entities), so it is not the right probe
        // here — the CLI round-trip test proves an in-session read end-to-end.
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
        assert_eq!(before, after, "existing db must be untouched on clobber refusal");
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
        assert!(!path.exists(), "no db must be created for a reserved-name refusal");
    }
}

#[cfg(test)]
mod function_clause_discipline_tests {
    //! The FUNCTIONAL half of "The Two Algebras" (clause-head-catechism.md §II;
    //! DDL-CLAUSE-ALGEBRA-ANALYSIS.md RULE 2). A value function's clauses are
    //! ordered first-match alternatives — at most one may be unguarded (the
    //! default), and it must be last. The chokepoint is
    //! `validate_function_clause_discipline`, gated on `DdlHead::Function`, so
    //! sigma predicates (the relational OR path) are exempt.
    use super::validate_function_clause_discipline;
    use crate::ddl::ddl_builder::build_ddl_file;

    fn discipline(source: &str) -> crate::error::Result<()> {
        let defs = build_ddl_file(source).expect("source should build");
        validate_function_clause_discipline(&defs)
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
        // Plain-functor sigma predicate (ddl/320): DdlHead::SigmaPredicate, not
        // Function — the helper is a no-op, clauses OR together elsewhere.
        discipline("empty(column) :- null = column\nempty(column) :- trim:(column) = \"\"")
            .expect("multi-clause sigma predicate must NOT be touched by this check");
    }
}
