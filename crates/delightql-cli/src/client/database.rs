// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The interactive client's one live database.
//!
//! `ClientDatabase` is client state: the REPL's own surface, configuration,
//! input history, and contained-failure evidence, in one in-memory SQLite
//! database that lives exactly as long as the interactive process. It is not
//! the user's target database, not the compiler bootstrap, and not the static
//! `cli::surface` image. It reaches disk only through [`ClientDatabase::serialize`],
//! and only on an explicit dump or bug-report action.
//!
//! The read/write boundary is the connection itself, not adapter discipline:
//! a sealed SQLite authorizer denies row mutation, schema mutation,
//! attach/detach, and write-capable pragmas, and only a host-held
//! [`ReplWriteWindow`] crosses it. DQL reads the same connection through the
//! REPL mount; Core receives no capability that can mint a window. `PRAGMA
//! query_only` is deliberately not used — it is connection-wide and would
//! refuse the host writer along with DQL — and SQL-text classification is not
//! the boundary; the authorizer judges the operations a prepared statement
//! actually requests.

use rusqlite::config::DbConfig;
use rusqlite::hooks::{AuthAction, Authorization};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use super::incident::Incident;

/// Physical schema revision, stamped as `PRAGMA user_version` and into the
/// `session` row a dump carries.
pub const REPL_SCHEMA_VERSION: i64 = 1;

/// Input-ledger ring bound: the ledger replaces the in-memory history
/// vectors, whose largest bound was the TUI's 50-entry ring; a session ledger
/// also feeds bug reports, so it retains an order of magnitude more.
const INPUT_RING_CAPACITY: i64 = 500;

/// Incident bounds: rows are deduplicated specimens, and the retained
/// exact inputs are additionally capped in total bytes.
const INCIDENT_ROW_CAPACITY: i64 = 200;
const INCIDENT_INPUT_BYTE_CAPACITY: i64 = 1_048_576;

/// Bounded parent-owned pending queue for writes that found the connection
/// busy. Flushed before the next prompt; overflow is a counted loss, never a
/// wait.
const PENDING_QUEUE_CAPACITY: usize = 64;

/// Writers never wait: a busy connection is the pending queue's case, and a
/// prompt (or containment recovery) must not stall behind a running query.
const WRITER_LOCK_BUDGET: Duration = Duration::ZERO;

/// Reads and the flush wait briefly — they run at prompt boundaries where a
/// short wait is cheap and the queue has no substitute for them.
const READER_LOCK_BUDGET: Duration = Duration::from_millis(250);

/// Pragmas that are read-only in every form SQLite offers them. The
/// authorizer cannot distinguish `PRAGMA x = v` from `PRAGMA x(v)` — both
/// arrive with a value — so admission is by NAME, and only names with no
/// write-capable form are here.
const READ_ONLY_PRAGMAS: &[&str] = &[
    "table_info",
    "table_xinfo",
    "table_list",
    "index_list",
    "index_info",
    "index_xinfo",
    "foreign_key_list",
    "foreign_key_check",
    "database_list",
    "collation_list",
    "compile_options",
    "function_list",
    "module_list",
    "pragma_list",
    "page_count",
    "freelist_count",
    "integrity_check",
    "quick_check",
];

/// Pragmas that ACT without a value. Everything else valueless is a read.
const VALUELESS_WRITE_PRAGMAS: &[&str] = &["wal_checkpoint", "optimize", "incremental_vacuum"];

/// One closed input kind for the ordered ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputKind {
    DotCommand,
    Dql,
    Sql,
}

impl InputKind {
    fn as_str(self) -> &'static str {
        match self {
            InputKind::DotCommand => "dot_command",
            InputKind::Dql => "dql",
            InputKind::Sql => "sql",
        }
    }
}

/// One closed dispatch outcome for the ordered ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputOutcome {
    Succeeded,
    Refused,
    Failed,
    Interrupted,
}

impl InputOutcome {
    fn as_str(self) -> &'static str {
        match self {
            InputOutcome::Succeeded => "succeeded",
            InputOutcome::Refused => "refused",
            InputOutcome::Failed => "failed",
            InputOutcome::Interrupted => "interrupted",
        }
    }
}

/// What a write attempt did. Recording failure never defeats the caller's
/// real work: `Queued` and `Lost` are reporting states, not errors to bail on.
#[derive(Debug)]
pub enum WriteOutcome {
    Applied,
    /// The connection was busy; the write sits in the bounded pending queue
    /// and flushes before the next prompt.
    Queued,
    /// Insertion and queueing both failed; the reason is for the fallback
    /// diagnostic.
    Lost(String),
}

/// What recording an incident did — `Recorded` carries the row id for
/// the terminal warning; `Queued` carries a temporary pending id.
#[derive(Clone, Debug)]
pub enum IncidentRecordOutcome {
    Recorded { incident_id: i64 },
    Queued { pending_id: u64 },
    Lost(String),
}

/// One queued write, applied in arrival order on flush.
enum PendingWrite {
    Option {
        name: String,
        value: Option<String>,
        value_kind: &'static str,
        default_value: Option<String>,
        source: String,
        changed_at: String,
    },
    Input {
        id: i64,
        occurred_at: String,
        kind: InputKind,
        input: String,
    },
    CloseInput {
        id: i64,
        completed_at: String,
        outcome: InputOutcome,
        error: Option<String>,
        generated_sql: Option<String>,
        elapsed_ms: Option<f64>,
    },
    Incident {
        incident: Incident,
        first_seen_at: String,
    },
    EditorRoad(super::context::EditorRoad),
    Exit {
        exited_at: String,
        exit_code: i32,
    },
}

/// Seal state shared with the installed authorizer closure.
#[derive(Default)]
struct SealState {
    open_windows: u32,
}

/// A history row read back for presentation DTOs (the TUI ring, the bug
/// manifest). The ledger is the authority; this is a projection of it.
#[derive(Clone, Debug)]
pub struct HistoryRow {
    pub occurred_at: String,
    pub completed_at: Option<String>,
    pub kind: String,
    pub input: String,
    pub outcome: String,
    pub error: Option<String>,
    pub generated_sql: Option<String>,
    pub elapsed_ms: Option<f64>,
}

/// The live client database. All methods take `&self`: the database is
/// shared between the prompt loop, the dot-command dispatcher, and the
/// parser-worker controller through one `Arc`.
pub struct ClientDatabase {
    connection: Arc<Mutex<Connection>>,
    seal: Arc<Mutex<SealState>>,
    pending: Mutex<VecDeque<PendingWrite>>,
    next_input_id: AtomicI64,
    next_pending_id: AtomicI64,
    session_id: String,
    started_ms: i64,
    mode: super::context::Mode,
    dql_build: String,
}

/// One row of `repl::surface.dot_command`: the dot-command registry's
/// projection, supplied by the REPL that owns the registry.
#[derive(Clone, Copy, Debug)]
pub struct SurfaceRow {
    pub spelling: &'static str,
    pub canonical_name: &'static str,
    pub is_alias: bool,
    pub args: &'static str,
    pub section: &'static str,
    pub summary: &'static str,
    pub example: &'static str,
}

/// RFC 3339 UTC to the millisecond, the ONE timestamp shape of every
/// client table — and of core's `finding` rows, which SQLite stamps with
/// the same format, so the exit merge sorts the two by text.
pub(crate) fn now_rfc3339() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

pub(crate) fn input_sha256(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

impl ClientDatabase {
    /// Create the complete schema, seed the dot-command surface and session
    /// identity, and SEAL the connection. No schema mutation is required
    /// afterwards; ordinary activity only changes rows.
    pub fn open(
        context: super::context::ProcessContext,
        surface: &[SurfaceRow],
    ) -> anyhow::Result<ClientDatabase> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(&format!(
            "PRAGMA user_version = {REPL_SCHEMA_VERSION};
             CREATE TABLE dot_command (
                 spelling        TEXT PRIMARY KEY,
                 canonical_name  TEXT NOT NULL,
                 is_alias        INTEGER NOT NULL CHECK (is_alias IN (0, 1)),
                 args            TEXT NOT NULL,
                 section         TEXT NOT NULL,
                 summary         TEXT NOT NULL,
                 example         TEXT NOT NULL
             );
             CREATE TABLE option (
                 name           TEXT PRIMARY KEY,
                 value          TEXT,
                 value_kind     TEXT NOT NULL,
                 default_value  TEXT,
                 source         TEXT NOT NULL,
                 changed_at     TEXT NOT NULL
             );
             CREATE TABLE input (
                 id              INTEGER PRIMARY KEY AUTOINCREMENT,
                 occurred_at     TEXT NOT NULL,
                 completed_at    TEXT,
                 kind            TEXT NOT NULL
                                 CHECK (kind IN ('dot_command', 'dql', 'sql')),
                 input           TEXT NOT NULL,
                 outcome         TEXT NOT NULL
                                 CHECK (outcome IN (
                                     'started', 'succeeded', 'refused',
                                     'failed', 'interrupted'
                                 )),
                 error           TEXT,
                 generated_sql   TEXT,
                 elapsed_ms      REAL
             );
             CREATE TABLE incident (
                 id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                 first_seen_at         TEXT NOT NULL,
                 last_seen_at          TEXT NOT NULL,
                 occurrence_count      INTEGER NOT NULL,

                 kind                  TEXT NOT NULL
                                       CHECK (kind IN ('error', 'warning', 'info', 'panic')),
                 road                  TEXT NOT NULL,
                 uri                   TEXT NOT NULL,
                 message               TEXT NOT NULL,
                 location              TEXT,
                 thread                TEXT,

                 operation             TEXT,
                 entrance              TEXT,
                 input                 TEXT,
                 input_sha256          TEXT,
                 input_bytes           INTEGER,
                 cursor_byte           INTEGER,

                 budget_ms             INTEGER,
                 last_elapsed_ms       REAL,
                 max_elapsed_ms        REAL,
                 last_progress_byte    INTEGER,

                 containment           TEXT,
                 worker_generation     INTEGER,
                 parser_runtime        TEXT,
                 grammar_fingerprint   TEXT,
                 dql_build             TEXT NOT NULL,

                 specimen_key          TEXT NOT NULL UNIQUE
             );
             CREATE TABLE session (
                 schema_version              INTEGER NOT NULL,
                 session_id                  TEXT NOT NULL,
                 started_ms                  INTEGER NOT NULL,
                 started_at                  TEXT NOT NULL,
                 exited_at                   TEXT,
                 exit_code                   INTEGER,
                 mode                        TEXT NOT NULL
                                             CHECK (mode IN ('repl', 'query', 'server', 'worker', 'other')),
                 pid                         INTEGER NOT NULL,
                 cwd                         TEXT NOT NULL,
                 dql_build                   TEXT NOT NULL,
                 stdin_is_tty                INTEGER NOT NULL CHECK (stdin_is_tty IN (0, 1)),
                 stdout_is_tty               INTEGER NOT NULL CHECK (stdout_is_tty IN (0, 1)),
                 editor_road                 TEXT CHECK (editor_road IN ('rich', 'plain')),
                 terminal_columns            INTEGER,
                 terminal_rows               INTEGER,
                 evicted_inputs              INTEGER NOT NULL,
                 evicted_incidents           INTEGER NOT NULL,
                 evicted_incident_input_bytes INTEGER NOT NULL
             );
             CREATE TABLE argument (
                 ordinal INTEGER PRIMARY KEY,
                 value   TEXT NOT NULL
             );
             CREATE TABLE environment (
                 name   TEXT PRIMARY KEY,
                 is_set INTEGER NOT NULL CHECK (is_set IN (0, 1)),
                 value  TEXT
                        CHECK ((is_set = 1) = (value IS NOT NULL))
             );"
        ))?;

        // The session's stamp is its start instant; the files written at
        // exit share it, so a triple on disk is recognizably one session.
        let session_id = format!("dql-{}-{}", context.pid, context.started_ms);
        let dql_build = delightql_buildinfo::human_static().to_string();
        conn.execute(
            "INSERT INTO session (
                 schema_version, session_id, started_ms, started_at, mode, pid, cwd,
                 dql_build, stdin_is_tty, stdout_is_tty, terminal_columns, terminal_rows,
                 evicted_inputs, evicted_incidents, evicted_incident_input_bytes
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 0, 0, 0)",
            params![
                REPL_SCHEMA_VERSION,
                session_id,
                context.started_ms,
                context.started_at,
                context.mode.as_str(),
                context.pid,
                context.cwd,
                dql_build,
                context.stdin_is_tty as i64,
                context.stdout_is_tty as i64,
                context.columns,
                context.rows,
            ],
        )?;
        {
            let mut insert = conn.prepare("INSERT INTO argument VALUES (?1, ?2)")?;
            for (ordinal, value) in context.arguments.iter().enumerate() {
                insert.execute(params![ordinal as i64, value])?;
            }
            let mut insert = conn.prepare("INSERT INTO environment VALUES (?1, ?2, ?3)")?;
            for row in &context.environment {
                insert.execute(params![row.name, row.value.is_some() as i64, row.value])?;
            }
        }

        {
            // Exhaustive projection of the registry: one row per accepted
            // spelling; aliases point at the canonical spelling.
            let mut insert =
                conn.prepare("INSERT INTO dot_command VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)")?;
            for row in surface {
                insert.execute(params![
                    row.spelling,
                    row.canonical_name,
                    row.is_alias as i64,
                    row.args,
                    row.section,
                    row.summary,
                    row.example
                ])?;
            }
        }

        let seal = Arc::new(Mutex::new(SealState::default()));
        install_authorizer(&conn, Arc::clone(&seal))?;
        // Defense in depth beneath the authorizer, same as the bootstrap
        // seal: the engine itself refuses the schema-corrupting roads.
        let armed = conn.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
        anyhow::ensure!(
            armed,
            "the engine reported defensive mode disarmed after arming"
        );

        Ok(ClientDatabase {
            connection: Arc::new(Mutex::new(conn)),
            seal,
            pending: Mutex::new(VecDeque::new()),
            next_input_id: AtomicI64::new(1),
            next_pending_id: AtomicI64::new(1),
            session_id,
            started_ms: context.started_ms,
            mode: context.mode,
            dql_build,
        })
    }

    /// Capture the context on `mode` and seed the surface from the dot
    /// command registry (empty without the REPL feature: no surface).
    pub fn open_on(mode: super::context::Mode) -> anyhow::Result<ClientDatabase> {
        let context = super::context::ProcessContext::capture(mode);
        Self::open(context, &dot_command_surface())
    }

    /// The road this process is on.
    pub fn mode(&self) -> super::context::Mode {
        self.mode
    }

    /// The session's stamp: its start instant in epoch milliseconds.
    pub fn started_ms(&self) -> i64 {
        self.started_ms
    }

    /// Stamp the exit: when, and with what code. Applied directly — the
    /// exit road holds no other client lock — or queued behind a busy
    /// connection, which the exit flush drains right after.
    pub fn record_exit(&self, exit_code: i32) -> WriteOutcome {
        let exited_at = now_rfc3339();
        match self.lock_within(WRITER_LOCK_BUDGET) {
            Some(conn) => {
                let window = self.write_window(conn);
                match set_exit(&window.conn, &exited_at, exit_code) {
                    Ok(()) => WriteOutcome::Applied,
                    Err(e) => WriteOutcome::Lost(e.to_string()),
                }
            }
            None => self.queue(PendingWrite::Exit {
                exited_at,
                exit_code,
            }),
        }
    }

    /// Record which line-editing road the prompt took. Applied within the
    /// reader lock budget or queued like any other write.
    pub fn record_editor_road(&self, road: super::context::EditorRoad) -> WriteOutcome {
        match self.lock_within(READER_LOCK_BUDGET) {
            Some(conn) => {
                let window = self.write_window(conn);
                match set_editor_road(&window.conn, road) {
                    Ok(()) => WriteOutcome::Applied,
                    Err(e) => WriteOutcome::Lost(e.to_string()),
                }
            }
            None => self.queue(PendingWrite::EditorRoad(road)),
        }
    }

    /// The live connection, for the REPL mount factory alone. Handing this
    /// out shares STORAGE, not mutation authority: the connection is sealed,
    /// and nothing reachable from it can open a write window.
    pub fn connection_arc(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    pub fn dql_build(&self) -> &str {
        &self.dql_build
    }

    /// Bounded connection acquisition: try, then retry on a short tick until
    /// the budget is spent. Never blocks indefinitely — a busy connection is
    /// the pending queue's case, not a wait.
    fn lock_within(&self, budget: Duration) -> Option<MutexGuard<'_, Connection>> {
        let deadline = std::time::Instant::now() + budget;
        loop {
            match self.connection.try_lock() {
                Ok(guard) => return Some(guard),
                Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                    return Some(poisoned.into_inner())
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    if std::time::Instant::now() >= deadline {
                        return None;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }

    /// Open the sealed write capability over an already-held connection
    /// lock. The window closes before the lock releases (field drop order),
    /// so no reader ever runs against an open window.
    fn write_window<'a>(&self, conn: MutexGuard<'a, Connection>) -> ReplWriteWindow<'a> {
        if let Ok(mut state) = self.seal.lock() {
            state.open_windows += 1;
        }
        ReplWriteWindow {
            _closer: WindowCloser {
                seal: Arc::clone(&self.seal),
            },
            conn,
        }
    }

    fn queue(&self, write: PendingWrite) -> WriteOutcome {
        let Ok(mut pending) = self.pending.lock() else {
            return WriteOutcome::Lost("pending queue poisoned".to_string());
        };
        if pending.len() >= PENDING_QUEUE_CAPACITY {
            return WriteOutcome::Lost(format!(
                "pending queue full ({PENDING_QUEUE_CAPACITY} writes)"
            ));
        }
        pending.push_back(write);
        WriteOutcome::Queued
    }

    /// Project one effective option row. The typed Rust value remains the
    /// operational authority; this row is its queryable rendering.
    pub fn set_option(
        &self,
        name: &str,
        value: Option<String>,
        value_kind: &'static str,
        default_value: Option<String>,
        source: &str,
    ) -> WriteOutcome {
        let changed_at = now_rfc3339();
        match self.lock_within(WRITER_LOCK_BUDGET) {
            Some(conn) => {
                let window = self.write_window(conn);
                let result = window.conn.execute(
                    "INSERT INTO option (name, value, value_kind, default_value, source, changed_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(name) DO UPDATE SET
                         value = excluded.value,
                         value_kind = excluded.value_kind,
                         default_value = excluded.default_value,
                         source = excluded.source,
                         changed_at = excluded.changed_at",
                    params![name, value, value_kind, default_value, source, changed_at],
                );
                match result {
                    Ok(_) => WriteOutcome::Applied,
                    Err(e) => WriteOutcome::Lost(e.to_string()),
                }
            }
            None => self.queue(PendingWrite::Option {
                name: name.to_string(),
                value,
                value_kind,
                default_value,
                source: source.to_string(),
                changed_at,
            }),
        }
    }

    /// Open one ledger row as `started`. The id is minted client-side so a
    /// queued open and its later close stay paired.
    pub fn record_input(&self, kind: InputKind, input: &str) -> (i64, WriteOutcome) {
        let id = self.next_input_id.fetch_add(1, Ordering::SeqCst);
        let occurred_at = now_rfc3339();
        let outcome = match self.lock_within(WRITER_LOCK_BUDGET) {
            Some(conn) => {
                let window = self.write_window(conn);
                match insert_input(&window.conn, id, &occurred_at, kind, input) {
                    Ok(()) => WriteOutcome::Applied,
                    Err(e) => WriteOutcome::Lost(e.to_string()),
                }
            }
            None => self.queue(PendingWrite::Input {
                id,
                occurred_at,
                kind,
                input: input.to_string(),
            }),
        };
        (id, outcome)
    }

    /// Close a `started` ledger row with its dispatched outcome.
    pub fn close_input(
        &self,
        id: i64,
        outcome: InputOutcome,
        error: Option<String>,
        generated_sql: Option<String>,
        elapsed_ms: Option<f64>,
    ) -> WriteOutcome {
        let completed_at = now_rfc3339();
        match self.lock_within(WRITER_LOCK_BUDGET) {
            Some(conn) => {
                let window = self.write_window(conn);
                match close_input_row(
                    &window.conn,
                    id,
                    &completed_at,
                    outcome,
                    error.as_deref(),
                    generated_sql.as_deref(),
                    elapsed_ms,
                ) {
                    Ok(()) => WriteOutcome::Applied,
                    Err(e) => WriteOutcome::Lost(e.to_string()),
                }
            }
            None => self.queue(PendingWrite::CloseInput {
                id,
                completed_at,
                outcome,
                error,
                generated_sql,
                elapsed_ms,
            }),
        }
    }

    /// Upsert one incident through the raw writer. Never calls DQL, so it
    /// is safe from any thread that holds no other client lock. An input
    /// beyond the retained-evidence cap is refused with the loss counted in
    /// the session row; the caller's real work is never defeated.
    pub fn record_incident(&self, incident: Incident) -> IncidentRecordOutcome {
        let input_bytes = incident.input.as_deref().map_or(0, |i| i.len() as i64);
        if input_bytes > INCIDENT_INPUT_BYTE_CAPACITY {
            if let Some(conn) = self.lock_within(WRITER_LOCK_BUDGET) {
                let window = self.write_window(conn);
                let _ = window.conn.execute(
                    "UPDATE session SET evicted_incidents = evicted_incidents + 1,
                                        evicted_incident_input_bytes = evicted_incident_input_bytes + ?1",
                    params![input_bytes],
                );
            }
            return IncidentRecordOutcome::Lost(format!(
                "the exact input ({input_bytes} bytes) exceeds the retained-evidence \
                 cap ({INCIDENT_INPUT_BYTE_CAPACITY} bytes); the specimen was not stored"
            ));
        }
        let now = now_rfc3339();
        match self.lock_within(WRITER_LOCK_BUDGET) {
            Some(conn) => {
                let window = self.write_window(conn);
                match upsert_incident(&window.conn, &incident, &now, &self.dql_build) {
                    Ok(incident_id) => IncidentRecordOutcome::Recorded { incident_id },
                    Err(e) => IncidentRecordOutcome::Lost(e.to_string()),
                }
            }
            None => {
                let pending_id = self.next_pending_id.fetch_add(1, Ordering::SeqCst) as u64;
                match self.queue(PendingWrite::Incident {
                    incident,
                    first_seen_at: now,
                }) {
                    WriteOutcome::Queued => IncidentRecordOutcome::Queued { pending_id },
                    WriteOutcome::Lost(reason) => IncidentRecordOutcome::Lost(reason),
                    WriteOutcome::Applied => unreachable!("queue() never applies"),
                }
            }
        }
    }

    /// Move every panic the hook queued into the incident table. Called at
    /// each prompt boundary and at exit. Returns how many became rows.
    pub fn drain_panics(&self) -> usize {
        let mut recorded = 0;
        for record in super::incident::drain_panics() {
            if !matches!(
                self.record_incident(record.into_incident()),
                IncidentRecordOutcome::Lost(_)
            ) {
                recorded += 1;
            }
        }
        recorded
    }

    /// Apply every queued write in arrival order. Called before each prompt
    /// and after session recovery. Returns how many were applied and how
    /// many were lost (with the first loss's reason).
    pub fn flush_pending(&self) -> (usize, usize, Option<String>) {
        let drained: Vec<PendingWrite> = {
            let Ok(mut pending) = self.pending.lock() else {
                return (0, 0, Some("pending queue poisoned".to_string()));
            };
            pending.drain(..).collect()
        };
        if drained.is_empty() {
            return (0, 0, None);
        }
        let Some(conn) = self.lock_within(READER_LOCK_BUDGET) else {
            // Still busy: put them back, bounded as ever.
            let mut lost = 0usize;
            if let Ok(mut pending) = self.pending.lock() {
                for write in drained {
                    if pending.len() >= PENDING_QUEUE_CAPACITY {
                        lost += 1;
                    } else {
                        pending.push_back(write);
                    }
                }
            }
            return (
                0,
                lost,
                (lost > 0).then(|| "pending queue full".to_string()),
            );
        };
        let window = self.write_window(conn);
        let mut applied = 0usize;
        let mut lost = 0usize;
        let mut first_loss = None;
        for write in drained {
            let result = match write {
                PendingWrite::Option {
                    name,
                    value,
                    value_kind,
                    default_value,
                    source,
                    changed_at,
                } => window
                    .conn
                    .execute(
                        "INSERT INTO option (name, value, value_kind, default_value, source, changed_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                         ON CONFLICT(name) DO UPDATE SET
                             value = excluded.value,
                             value_kind = excluded.value_kind,
                             default_value = excluded.default_value,
                             source = excluded.source,
                             changed_at = excluded.changed_at",
                        params![name, value, value_kind, default_value, source, changed_at],
                    )
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                PendingWrite::Input {
                    id,
                    occurred_at,
                    kind,
                    input,
                } => insert_input(&window.conn, id, &occurred_at, kind, &input)
                    .map_err(|e| e.to_string()),
                PendingWrite::CloseInput {
                    id,
                    completed_at,
                    outcome,
                    error,
                    generated_sql,
                    elapsed_ms,
                } => close_input_row(
                    &window.conn,
                    id,
                    &completed_at,
                    outcome,
                    error.as_deref(),
                    generated_sql.as_deref(),
                    elapsed_ms,
                )
                .map_err(|e| e.to_string()),
                PendingWrite::Incident {
                    incident,
                    first_seen_at,
                } => upsert_incident(&window.conn, &incident, &first_seen_at, &self.dql_build)
                    .map(|_| ())
                    .map_err(|e| e.to_string()),
                PendingWrite::EditorRoad(road) => {
                    set_editor_road(&window.conn, road).map_err(|e| e.to_string())
                }
                PendingWrite::Exit {
                    exited_at,
                    exit_code,
                } => set_exit(&window.conn, &exited_at, exit_code).map_err(|e| e.to_string()),
            };
            match result {
                Ok(()) => applied += 1,
                Err(reason) => {
                    lost += 1;
                    first_loss.get_or_insert(reason);
                }
            }
        }
        (applied, lost, first_loss)
    }

    /// A consistent snapshot of the live database, on explicit request only.
    /// The live database remains in memory and continues collecting.
    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let conn = self
            .lock_within(READER_LOCK_BUDGET)
            .ok_or_else(|| anyhow::anyhow!("the live REPL database is busy; try again"))?;
        let data = conn.serialize("main")?;
        Ok(data.to_vec())
    }

    /// The ledger read back for presentation DTOs, oldest first.
    pub fn history_rows(&self) -> anyhow::Result<Vec<HistoryRow>> {
        let conn = self
            .lock_within(READER_LOCK_BUDGET)
            .ok_or_else(|| anyhow::anyhow!("the live REPL database is busy"))?;
        let mut stmt = conn.prepare(
            "SELECT occurred_at, completed_at, kind, input, outcome, error,
                    generated_sql, elapsed_ms
             FROM input ORDER BY id",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(HistoryRow {
                    occurred_at: row.get(0)?,
                    completed_at: row.get(1)?,
                    kind: row.get(2)?,
                    input: row.get(3)?,
                    outcome: row.get(4)?,
                    error: row.get(5)?,
                    generated_sql: row.get(6)?,
                    elapsed_ms: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

/// The sealed RAII write capability. Its existence is what admits writes;
/// success, error, and unwind all close it, and the connection lock it holds
/// releases only after the seal is restored.
pub struct ReplWriteWindow<'a> {
    // Declared before `conn`: dropped first, so the window closes while the
    // connection is still exclusively held. Held for Drop alone.
    _closer: WindowCloser,
    conn: MutexGuard<'a, Connection>,
}

struct WindowCloser {
    seal: Arc<Mutex<SealState>>,
}

impl Drop for WindowCloser {
    fn drop(&mut self) {
        if let Ok(mut state) = self.seal.lock() {
            state.open_windows = state.open_windows.saturating_sub(1);
        }
    }
}

/// The dot-command registry projected into surface rows.
fn dot_command_surface() -> Vec<SurfaceRow> {
    #[cfg(feature = "repl")]
    {
        crate::repl::commands::dot_command_surface()
    }
    #[cfg(not(feature = "repl"))]
    {
        Vec::new()
    }
}

fn set_exit(conn: &Connection, exited_at: &str, exit_code: i32) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE session SET exited_at = ?1, exit_code = ?2",
        params![exited_at, exit_code],
    )
    .map(|_| ())
}

fn set_editor_road(conn: &Connection, road: super::context::EditorRoad) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE session SET editor_road = ?1",
        params![road.as_str()],
    )
    .map(|_| ())
}

fn insert_input(
    conn: &Connection,
    id: i64,
    occurred_at: &str,
    kind: InputKind,
    input: &str,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO input (id, occurred_at, kind, input, outcome)
         VALUES (?1, ?2, ?3, ?4, 'started')",
        params![id, occurred_at, kind.as_str(), input],
    )?;
    // Ring bound: oldest rows beyond capacity leave, counted in the session
    // metadata so a dump can say evidence was discarded.
    let evicted = conn.execute(
        "DELETE FROM input WHERE id <= (SELECT MAX(id) FROM input) - ?1",
        params![INPUT_RING_CAPACITY],
    )?;
    if evicted > 0 {
        conn.execute(
            "UPDATE session SET evicted_inputs = evicted_inputs + ?1",
            params![evicted as i64],
        )?;
    }
    Ok(())
}

fn close_input_row(
    conn: &Connection,
    id: i64,
    completed_at: &str,
    outcome: InputOutcome,
    error: Option<&str>,
    generated_sql: Option<&str>,
    elapsed_ms: Option<f64>,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE input SET completed_at = ?2, outcome = ?3, error = ?4,
                          generated_sql = ?5, elapsed_ms = ?6
         WHERE id = ?1",
        params![
            id,
            completed_at,
            outcome.as_str(),
            error,
            generated_sql,
            elapsed_ms
        ],
    )?;
    Ok(())
}

fn upsert_incident(
    conn: &Connection,
    incident: &Incident,
    seen_at: &str,
    dql_build: &str,
) -> rusqlite::Result<i64> {
    let key = incident.specimen_key();
    let w = incident.worker.as_ref();
    // Update first, insert only when absent: an upsert's conflicting
    // insert still burns an AUTOINCREMENT id, and the ids are what the
    // terminal names ("recorded as repl::errors.incident #3").
    let updated = conn.execute(
        "UPDATE incident SET
             occurrence_count = occurrence_count + 1,
             last_seen_at = ?2,
             message = ?3,
             thread = ?4,
             last_elapsed_ms = ?5,
             max_elapsed_ms = MAX(COALESCE(max_elapsed_ms, 0), COALESCE(?5, 0)),
             last_progress_byte = ?6,
             cursor_byte = ?7,
             worker_generation = ?8
         WHERE specimen_key = ?1",
        params![
            key,
            seen_at,
            incident.message,
            incident.thread,
            w.map(|w| w.elapsed_ms),
            w.and_then(|w| w.last_progress_byte).map(|b| b as i64),
            incident.cursor_byte.map(|b| b as i64),
            w.map(|w| w.worker_generation as i64),
        ],
    )?;
    if updated == 0 {
        let sha = incident.input.as_deref().map(input_sha256);
        let bytes = incident.input.as_deref().map(|i| i.len() as i64);
        conn.execute(
            "INSERT INTO incident (
                 first_seen_at, last_seen_at, occurrence_count,
                 kind, road, uri, message, location, thread,
                 operation, entrance, input, input_sha256, input_bytes, cursor_byte,
                 budget_ms, last_elapsed_ms, max_elapsed_ms, last_progress_byte,
                 containment, worker_generation, parser_runtime, grammar_fingerprint,
                 dql_build, specimen_key
             ) VALUES (?1, ?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                       ?14, ?15, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
            params![
                seen_at,
                incident.kind.as_str(),
                incident.road,
                incident.uri,
                incident.message,
                incident.location,
                incident.thread,
                w.map(|w| w.operation),
                w.map(|w| w.entrance),
                incident.input,
                sha,
                bytes,
                incident.cursor_byte.map(|b| b as i64),
                w.map(|w| w.budget_ms as i64),
                w.map(|w| w.elapsed_ms),
                w.and_then(|w| w.last_progress_byte).map(|b| b as i64),
                w.map(|w| w.containment),
                w.map(|w| w.worker_generation as i64),
                w.map(|_| delightql_cst::PARSER_RUNTIME),
                w.map(|_| delightql_cst::GRAMMAR_FINGERPRINT),
                dql_build,
                key,
            ],
        )?;
    }
    let incident_id: i64 = conn.query_row(
        "SELECT id FROM incident WHERE specimen_key = ?1",
        params![key],
        |row| row.get(0),
    )?;
    evict_incidents(conn)?;
    Ok(incident_id)
}

/// Incident bounds: a specimen-row cap and a total retained-input byte cap,
/// evicting oldest-last-seen first, counted in session metadata.
fn evict_incidents(conn: &Connection) -> rusqlite::Result<()> {
    loop {
        let (rows, bytes): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(input_bytes), 0) FROM incident",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if rows <= INCIDENT_ROW_CAPACITY && bytes <= INCIDENT_INPUT_BYTE_CAPACITY {
            return Ok(());
        }
        let (victim, victim_bytes): (i64, i64) = conn.query_row(
            "SELECT id, COALESCE(input_bytes, 0) FROM incident ORDER BY last_seen_at, id LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        conn.execute("DELETE FROM incident WHERE id = ?1", params![victim])?;
        conn.execute(
            "UPDATE session SET evicted_incidents = evicted_incidents + 1,
                                evicted_incident_input_bytes = evicted_incident_input_bytes + ?1",
            params![victim_bytes],
        )?;
    }
}

/// Install the sealed authorizer. While no window is open it denies row
/// mutation, schema mutation, attach/detach, and write-capable pragmas —
/// whatever SQL road produced them. Reads, functions, transactions, and
/// read-only pragmas stay ordinary.
fn install_authorizer(conn: &Connection, seal: Arc<Mutex<SealState>>) -> anyhow::Result<()> {
    conn.authorizer(Some(move |context: rusqlite::hooks::AuthContext<'_>| {
        let sealed = match seal.lock() {
            Ok(state) => state.open_windows == 0,
            // A poisoned seal fails CLOSED.
            Err(_) => true,
        };
        if !sealed {
            return Authorization::Allow;
        }
        use AuthAction::*;
        match context.action {
            Insert { .. } | Update { .. } | Delete { .. } => Authorization::Deny,
            CreateIndex { .. }
            | CreateTable { .. }
            | CreateTempIndex { .. }
            | CreateTempTable { .. }
            | CreateTempTrigger { .. }
            | CreateTempView { .. }
            | CreateTrigger { .. }
            | CreateView { .. }
            | CreateVtable { .. }
            | DropIndex { .. }
            | DropTable { .. }
            | DropTempIndex { .. }
            | DropTempTable { .. }
            | DropTempTrigger { .. }
            | DropTempView { .. }
            | DropTrigger { .. }
            | DropView { .. }
            | DropVtable { .. }
            | AlterTable { .. }
            | Reindex { .. }
            | Analyze { .. } => Authorization::Deny,
            Attach { .. } | Detach { .. } => Authorization::Deny,
            Pragma {
                pragma_name,
                pragma_value,
            } => {
                // The authorizer sees `PRAGMA x = v` and `PRAGMA x(v)`
                // identically, so a VALUED pragma passes only when its name
                // has no write-capable form at all. A valueless pragma is a
                // read, except the few that act without a value.
                let read_only = READ_ONLY_PRAGMAS
                    .iter()
                    .any(|p| pragma_name.eq_ignore_ascii_case(p));
                let valueless_writer = VALUELESS_WRITE_PRAGMAS
                    .iter()
                    .any(|p| pragma_name.eq_ignore_ascii_case(p));
                if read_only || (pragma_value.is_none() && !valueless_writer) {
                    Authorization::Allow
                } else {
                    Authorization::Deny
                }
            }
            Read { .. } | Select | Function { .. } | Recursive => Authorization::Allow,
            Transaction { .. } | Savepoint { .. } => Authorization::Allow,
            Unknown { .. } => Authorization::Deny,
            // Future authorizer actions fail CLOSED while sealed.
            _ => Authorization::Deny,
        }
    }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> ClientDatabase {
        ClientDatabase::open_on(super::super::context::Mode::Other).expect("open repl database")
    }

    /// Schema: exactly the expected tables and the stamped schema version.
    #[test]
    fn a_new_database_carries_the_complete_schema() {
        let db = open();
        let conn = db.connection.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            tables,
            [
                "argument",
                "dot_command",
                "environment",
                "incident",
                "input",
                "option",
                "session",
                "sqlite_sequence"
            ]
        );
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, REPL_SCHEMA_VERSION);
        let (schema_version, session_id, dql_build): (i64, String, String) = conn
            .query_row(
                "SELECT schema_version, session_id, dql_build FROM session",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(schema_version, REPL_SCHEMA_VERSION);
        assert_eq!(session_id, db.session_id());
        assert_eq!(dql_build, db.dql_build());
    }

    /// Surface exhaustiveness: exactly every registry spelling, with alias
    /// rows pointing at their canonical spelling and carrying its metadata.
    #[test]
    fn dot_command_rows_are_the_exhaustive_registry_projection() {
        let db = open();
        let conn = db.connection.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT spelling, canonical_name, is_alias, args, section, summary, example
                 FROM dot_command",
            )
            .unwrap();
        let rows: std::collections::BTreeMap<
            String,
            (String, i64, String, String, String, String),
        > = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    (
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ),
                ))
            })
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        let expected: std::collections::BTreeSet<String> =
            crate::repl::commands::dot_command_spellings()
                .map(String::from)
                .collect();
        let actual: std::collections::BTreeSet<String> = rows.keys().cloned().collect();
        assert_eq!(actual, expected, "one row per accepted spelling, no more");

        for cmd in crate::repl::commands::DOT_COMMANDS {
            let (canonical, is_alias, args, section, summary, example) = &rows[cmd.name];
            assert_eq!(canonical, cmd.name);
            assert_eq!(*is_alias, 0);
            assert_eq!(args, cmd.args);
            assert_eq!(section, cmd.section);
            assert_eq!(summary, cmd.summary);
            assert_eq!(example, cmd.example);
            for alias in cmd.aliases {
                let (canonical, is_alias, ..) = &rows[*alias];
                assert_eq!(
                    canonical, cmd.name,
                    "alias rows point at the canonical spelling"
                );
                assert_eq!(*is_alias, 1);
            }
        }
    }

    /// Read-only boundary: row DML, schema DDL, attach, and write-capable
    /// pragmas are denied by the authorizer while sealed; reads and
    /// read-only pragmas pass.
    #[test]
    fn the_sealed_authorizer_denies_every_mutation_road() {
        let db = open();
        let conn = db.connection.lock().unwrap();
        for sql in [
            "INSERT INTO option (name, value_kind, source, changed_at) VALUES ('x','string','t','t')",
            "UPDATE dot_command SET summary = 'x'",
            "DELETE FROM dot_command",
            "CREATE TABLE intruder (x INTEGER)",
            "CREATE TEMP TABLE intruder (x INTEGER)",
            "CREATE VIEW intruder AS SELECT 1",
            "DROP TABLE dot_command",
            "ALTER TABLE dot_command ADD COLUMN extra TEXT",
            "ATTACH DATABASE ':memory:' AS other",
            "PRAGMA user_version = 99",
            "PRAGMA journal_mode = WAL",
            "PRAGMA writable_schema = ON",
        ] {
            let err = conn.execute_batch(sql).expect_err(sql);
            assert!(
                err.to_string().contains("not authorized"),
                "'{sql}' must be denied by the AUTHORIZER, got: {err}"
            );
        }
        // Reads and read-only pragmas stay ordinary.
        conn.query_row("SELECT COUNT(*) FROM dot_command", [], |r| {
            r.get::<_, i64>(0)
        })
        .expect("reads pass");
        conn.query_row("PRAGMA table_info(dot_command)", [], |r| r.get::<_, i64>(0))
            .expect("read-only pragmas pass");
    }

    /// Scoped writer: each write succeeds inside its window, and the seal is
    /// restored on success, error, and unwind.
    #[test]
    fn writers_cross_only_inside_a_window_and_the_seal_restores() {
        let db = open();
        match db.set_option(
            "output_format",
            Some("table".into()),
            "enum",
            None,
            "startup",
        ) {
            WriteOutcome::Applied => {}
            other => panic!("set_option must apply on an idle connection: {other:?}"),
        }
        {
            let conn = db.connection.lock().unwrap();
            let value: String = conn
                .query_row(
                    "SELECT value FROM option WHERE name = 'output_format'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(value, "table");
            // The window is closed again: direct mutation refuses.
            assert!(conn.execute("DELETE FROM option", []).is_err());
        }
        // Error path: a write that fails inside the window still restores
        // the seal (the CHECK refuses an unknown outcome spelling).
        {
            let conn = db.connection.lock().unwrap();
            let window = db.write_window(conn);
            assert!(window
                .conn
                .execute(
                    "INSERT INTO input (id, occurred_at, kind, input, outcome)
                     VALUES (1, 't', 'dql', 'x', 'nonsense')",
                    [],
                )
                .is_err());
        }
        let conn = db.connection.lock().unwrap();
        assert!(
            conn.execute("DELETE FROM option", []).is_err(),
            "sealed again after error"
        );
    }

    /// History authority: open-as-started, close-with-outcome, one ordered
    /// ledger, ring-bounded with counted eviction.
    #[test]
    fn the_input_ledger_opens_closes_and_evicts() {
        let db = open();
        let (id, outcome) = db.record_input(InputKind::Dql, "users(*)");
        assert!(matches!(outcome, WriteOutcome::Applied));
        assert!(matches!(
            db.close_input(
                id,
                InputOutcome::Succeeded,
                None,
                Some("SELECT 1".into()),
                Some(1.5)
            ),
            WriteOutcome::Applied
        ));
        let rows = db.history_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].kind, "dql");
        assert_eq!(rows[0].outcome, "succeeded");
        assert_eq!(rows[0].generated_sql.as_deref(), Some("SELECT 1"));

        for i in 0..(INPUT_RING_CAPACITY + 10) {
            let (id, _) = db.record_input(InputKind::DotCommand, &format!(".zebra {i}"));
            db.close_input(id, InputOutcome::Succeeded, None, None, None);
        }
        let rows = db.history_rows().unwrap();
        assert_eq!(rows.len() as i64, INPUT_RING_CAPACITY);
        let conn = db.connection.lock().unwrap();
        let evicted: i64 = conn
            .query_row("SELECT evicted_inputs FROM session", [], |r| r.get(0))
            .unwrap();
        assert_eq!(evicted, 11, "eviction is counted in session metadata");
    }

    fn specimen(input: &str, budget_ms: u64, containment: &'static str) -> Incident {
        use super::super::incident::{hierarchy, IncidentKind, WorkerEvidence};
        let mut incident = Incident::plain(
            IncidentKind::Error,
            "parser_worker",
            hierarchy::WORKER_BUDGET,
            "prompt parser exceeded its budget".to_string(),
        );
        incident.input = Some(input.to_string());
        incident.cursor_byte = Some(input.len() as u64);
        incident.worker = Some(WorkerEvidence {
            operation: "prompt_well_formed",
            entrance: "prompt",
            budget_ms,
            elapsed_ms: budget_ms as f64 + 1.0,
            last_progress_byte: Some(3),
            containment,
            worker_generation: 1,
        });
        incident
    }

    /// Incident capture and deduplication: identical specimens increment one
    /// row; changed input, budget, or containment produce distinct rows; the
    /// exact input and every ruled fact land.
    #[test]
    fn incident_specimens_deduplicate_and_retain_the_exact_input() {
        let db = open();
        let toxic = "(~~ddln(*)_(1):a a(),|1|<";
        let first = db.record_incident(specimen(toxic, 25, "worker_kill"));
        let IncidentRecordOutcome::Recorded { incident_id } = first else {
            panic!("recording on an idle connection must apply: {first:?}");
        };
        db.record_incident(specimen(toxic, 25, "worker_kill"));
        db.record_incident(specimen(toxic, 50, "worker_kill"));
        db.record_incident(specimen(toxic, 25, "cooperative_cancel"));
        db.record_incident(specimen("users(*) |>", 25, "worker_kill"));

        let conn = db.connection.lock().unwrap();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM incident", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 4, "four distinct specimen keys");
        let (count, input, runtime, fingerprint, build): (i64, String, String, String, String) =
            conn.query_row(
                "SELECT occurrence_count, input, parser_runtime, grammar_fingerprint, dql_build
                 FROM incident WHERE id = ?1",
                params![incident_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .unwrap();
        assert_eq!(
            count, 2,
            "the identical specimen upserts, never a second row"
        );
        assert_eq!(input, toxic, "the exact input is retained");
        assert_eq!(runtime, delightql_cst::PARSER_RUNTIME);
        assert_eq!(fingerprint, delightql_cst::GRAMMAR_FINGERPRINT);
        assert_eq!(build, db.dql_build());
    }

    /// Failure containment: a busy connection queues within bounds, the
    /// flush applies in order, and overflow is a counted loss — never a wait.
    #[test]
    fn a_busy_connection_queues_and_the_flush_applies() {
        let db = open();
        let held = db.connection.lock().unwrap();
        let (id, outcome) = db.record_input(InputKind::Sql, "SELECT 1");
        assert!(matches!(outcome, WriteOutcome::Queued));
        assert!(matches!(
            db.close_input(id, InputOutcome::Succeeded, None, None, Some(0.5)),
            WriteOutcome::Queued
        ));
        match db.record_incident(specimen("x", 25, "worker_kill")) {
            IncidentRecordOutcome::Queued { pending_id } => assert!(pending_id > 0),
            other => panic!("a busy connection must queue: {other:?}"),
        }
        drop(held);
        let (applied, lost, reason) = db.flush_pending();
        assert_eq!((applied, lost), (3, 0), "loss reason: {reason:?}");
        let rows = db.history_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, "succeeded");

        // Overflow: the queue is bounded and refuses rather than growing.
        let held = db.connection.lock().unwrap();
        let mut lost_any = false;
        for i in 0..(PENDING_QUEUE_CAPACITY + 5) {
            let (_, outcome) = db.record_input(InputKind::Sql, &format!("SELECT {i}"));
            if matches!(outcome, WriteOutcome::Lost(_)) {
                lost_any = true;
            }
        }
        assert!(
            lost_any,
            "overflow past the bound must be a loss, not growth"
        );
        drop(held);
        db.flush_pending();
    }

    /// Dump: serialization answers a loadable image with the current rows
    /// and does not mutate the live database.
    #[test]
    fn serialize_snapshots_without_mutating() {
        let db = open();
        let (id, _) = db.record_input(InputKind::Dql, "users(*)");
        db.close_input(id, InputOutcome::Succeeded, None, None, None);
        let image = db.serialize().unwrap();

        let mut loaded = Connection::open_in_memory().unwrap();
        loaded
            .deserialize_read_exact("main", &image[..], image.len(), false)
            .unwrap();
        let n: i64 = loaded
            .query_row("SELECT COUNT(*) FROM input", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        let (schema_version, session_id): (i64, String) = loaded
            .query_row("SELECT schema_version, session_id FROM session", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(schema_version, REPL_SCHEMA_VERSION);
        assert_eq!(session_id, db.session_id());

        // Still alive and still sealed afterwards.
        let (id2, outcome) = db.record_input(InputKind::Dql, "users(*) |> (id)");
        assert!(matches!(outcome, WriteOutcome::Applied));
        db.close_input(id2, InputOutcome::Failed, Some("boom".into()), None, None);
        assert_eq!(db.history_rows().unwrap().len(), 2);
    }
}

#[cfg(test)]
mod over_cap_tests {
    use super::*;

    fn giant_specimen(bytes: usize) -> Incident {
        use super::super::incident::{hierarchy, IncidentKind, WorkerEvidence};
        let mut incident = Incident::plain(
            IncidentKind::Error,
            "parser_worker",
            hierarchy::WORKER_BUDGET,
            "prompt parser exceeded its budget".to_string(),
        );
        incident.input = Some("x".repeat(bytes));
        incident.worker = Some(WorkerEvidence {
            operation: "submission_preflight",
            entrance: "prompt",
            budget_ms: 2_000,
            elapsed_ms: 2_001.0,
            last_progress_byte: None,
            containment: "worker_kill",
            worker_generation: 1,
        });
        incident
    }

    /// Just over the cap: refused before insertion with an honest reason,
    /// counted as discarded evidence, and no durable incident id printed
    /// for a row that would not exist.
    #[test]
    fn a_just_over_cap_specimen_is_refused_honestly() {
        let db = ClientDatabase::open_on(super::super::context::Mode::Other).unwrap();
        let outcome = db.record_incident(giant_specimen(INCIDENT_INPUT_BYTE_CAPACITY as usize + 1));
        let IncidentRecordOutcome::Lost(reason) = outcome else {
            panic!("an over-cap specimen must refuse, got {outcome:?}");
        };
        assert!(reason.contains("exceeds the retained-evidence cap"));

        let conn = db.connection.lock().unwrap();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM incident", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 0, "nothing was inserted");
        let (discarded, discarded_bytes): (i64, i64) = conn
            .query_row(
                "SELECT evicted_incidents, evicted_incident_input_bytes FROM session",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(discarded, 1, "the discard is counted");
        assert_eq!(discarded_bytes, INCIDENT_INPUT_BYTE_CAPACITY + 1);
    }

    /// Exactly at the cap: admitted, retained, and never self-evicted —
    /// the incident id it reports still exists.
    #[test]
    fn an_at_cap_specimen_is_admitted_and_survives() {
        let db = ClientDatabase::open_on(super::super::context::Mode::Other).unwrap();
        let outcome = db.record_incident(giant_specimen(INCIDENT_INPUT_BYTE_CAPACITY as usize));
        let IncidentRecordOutcome::Recorded { incident_id } = outcome else {
            panic!("an at-cap specimen must be admitted, got {outcome:?}");
        };
        let conn = db.connection.lock().unwrap();
        let bytes: i64 = conn
            .query_row(
                "SELECT input_bytes FROM incident WHERE id = ?1",
                params![incident_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            bytes, INCIDENT_INPUT_BYTE_CAPACITY,
            "the reported incident exists"
        );
    }
}
