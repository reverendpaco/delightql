// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Fatboy execution helpers: the engine side of `fatboy://` connections.
//!
//! A fatboy is a separate process speaking the relay protocol over its
//! stdin/stdout, with a foreign engine behind it (dql-fatboy-postgres,
//! dql-fatboy-duckdb — the LSP model). dql spawns it on demand and reaps
//! it on drop; the pipe is the lifecycle. The engine consumes it two ways:
//!
//! - **Query execution**: a `RemoteHandler` over `StdioTransport` — the
//!   engine's own protocol terms (Version included) forward verbatim to
//!   the fatboy. This is the relay's backend-facing side running for
//!   real (ALL-SQL-TARGETING-STATE.md, step 4).
//! - **Mount/introspection**: `FatboyIntrospector`/`FatboySchema` issue
//!   catalog queries as ORDINARY relay queries (`information_schema`
//!   SQL) — relay-role question #4's convention: catalog discovery is
//!   just Query, no new protocol terms.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;

use delightql_protocol::stdio::StdioTransport;
use delightql_protocol::{
    Client, FetchResponse, Handler, Orientation, Projection, QueryResponse, RemoteHandler,
    ServerTerm, Session, Transport, VersionResult,
};
use delightql_types::db_traits::{DatabaseConnection, DbValue};
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity};
use delightql_types::schema::{ColumnInfo, DatabaseSchema};

/// Known fatboy profiles and their per-profile bits. Catalog discovery
/// is ordinary queries (the relay convention) — both engines speak
/// information_schema; only the schema name differs.
pub const PROFILES: &[&str] = &["postgres", "duckdb"];

fn default_schema(profile: &str) -> &'static str {
    match profile {
        "duckdb" => "main",
        _ => "public",
    }
}

fn introspect_sql(profile: &str) -> String {
    format!(
        "SELECT t.table_name, t.table_type, \
         c.ordinal_position - 1 AS cid, c.column_name, c.data_type, \
         CASE WHEN c.is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull \
         FROM information_schema.tables t \
         JOIN information_schema.columns c \
           ON t.table_catalog = c.table_catalog \
          AND t.table_schema = c.table_schema \
          AND t.table_name = c.table_name \
         WHERE t.table_schema = '{}' \
         ORDER BY t.table_name, c.ordinal_position",
        default_schema(profile)
    )
}

/// Locate the fatboy binary, git-exec-path style: env override →
/// sibling of the running dql → bare name (PATH).
fn fatboy_binary(profile: &str) -> PathBuf {
    let env_key = format!("DQL_FATBOY_{}_BIN", profile.to_uppercase());
    if let Ok(p) = std::env::var(&env_key) {
        return PathBuf::from(p);
    }
    let name = format!("dql-fatboy-{}", profile);
    if let Ok(me) = std::env::current_exe() {
        let sibling = me.with_file_name(&name);
        if sibling.is_file() {
            return sibling;
        }
    }
    PathBuf::from(name) // PATH lookup at spawn time
}

// ---------------------------------------------------------------------------
// FatboyRelay — one handshaken relay session, shared via Mutex
// ---------------------------------------------------------------------------

pub struct FatboyRelay {
    // Boxed so the transport (socket vs. stdio) is chosen at runtime;
    // the relay's query path is identical either way.
    session: Mutex<Session<Box<dyn Transport + Send>>>,
    rows: delightql_protocol::AgreedOrientation,
}

impl FatboyRelay {
    /// Handshake over an already-built transport. Shared by both the
    /// socket (`connect`) and stdio (`from_child`) constructors.
    fn from_transport(transport: Box<dyn Transport + Send>) -> Result<Self, String> {
        let client = Client::new(transport);
        let session = match client
            .version(1_000_000, b"relay0".to_vec(), 300_000, vec![Orientation::Rows])
            .map_err(|e| format!("fatboy handshake: {}", e.message))?
        {
            VersionResult::Accepted(s) => s,
            VersionResult::Rejected { message, .. } => {
                return Err(format!(
                    "fatboy rejected handshake: {}",
                    String::from_utf8_lossy(&message)
                ))
            }
        };
        let rows = session
            .agreed_orientation(Orientation::Rows)
            .ok_or("fatboy does not support Rows orientation")?;
        Ok(Self {
            session: Mutex::new(session),
            rows,
        })
    }

    /// Take a freshly spawned fatboy child and talk to it over its
    /// stdin/stdout (the default transport). The transport owns the
    /// child and reaps it on drop.
    pub fn from_child(child: Child) -> Result<Self, String> {
        let transport = StdioTransport::from_child(child).map_err(|e| e.message)?;
        Self::from_transport(Box::new(transport))
    }

    /// Run one SQL statement; rows with NULL fidelity (None = SQL NULL).
    pub fn query_nullable(
        &self,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>), String> {
        let mut session = self
            .session
            .lock()
            .map_err(|_| "fatboy session poisoned".to_string())?;

        let (handle, dimensions) = match session
            .query(sql.as_bytes().to_vec())
            .map_err(|e| format!("fatboy query: {}", e.message))?
        {
            QueryResponse::Header { handle, dimensions } => (handle, dimensions),
            QueryResponse::Error { identity, message, .. } => {
                return Err(format_protocol_error(&identity, &message))
            }
        };
        let columns: Vec<String> = dimensions
            .iter()
            .map(|d| String::from_utf8_lossy(&d.name).into_owned())
            .collect();

        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        loop {
            match session
                .fetch(&handle, Projection::All, 10_000, self.rows)
                .map_err(|e| format!("fatboy fetch: {}", e.message))?
            {
                FetchResponse::Data { cells } => {
                    for row in cells {
                        rows.push(
                            row.into_iter()
                                .map(|c| c.map(|b| String::from_utf8_lossy(&b).into_owned()))
                                .collect(),
                        );
                    }
                }
                FetchResponse::End => break,
                FetchResponse::Error { identity, message, .. } => {
                    return Err(format_protocol_error(&identity, &message))
                }
            }
        }
        let _ = session.close(handle);
        Ok((columns, rows))
    }
}

/// Render a protocol Error for CLI-mode strings, keeping the identity
/// URI visible: `[delightql-error://target/postgres/<class>/<sqlstate>] <message>`.
/// (Server mode needs none of this — the relay session forwards backend
/// Error terms verbatim, identity included.)
fn format_protocol_error(identity: &[u8], message: &[u8]) -> String {
    let msg = String::from_utf8_lossy(message);
    if identity.is_empty() {
        msg.into_owned()
    } else {
        format!("[{}] {}", String::from_utf8_lossy(identity), msg)
    }
}

/// DelightQLError for fatboy failures, with the static subcategory so
/// the outer error URI is `delightql-error://target/postgres` (the precise
/// per-error identity rides in the message via format_protocol_error).
fn fatboy_db_error(context: &str, detail: String) -> delightql_types::DelightQLError {
    delightql_types::DelightQLError::DatabaseOperationError {
        message: context.to_string(),
        details: detail,
        source: None,
        subcategory: Some("target/postgres"),
    }
}

// ---------------------------------------------------------------------------
// FatboyManager — what ConnectionManager::Fatboy holds
// ---------------------------------------------------------------------------

pub struct FatboyManager {
    pub profile: String,
    /// Display name of the database (path, dbname, or the resource URL's
    /// path component) — for connection_info, never for spawning.
    pub db: String,
    /// How to (re)spawn children for this connection.
    spawn: SpawnSpec,
    relay: FatboyRelay,
}

/// The spawn contract for a fatboy child.
#[derive(Clone)]
enum SpawnSpec {
    /// `--database <name-or-path>` (duckdb files; env-completed postgres).
    Database(String),
    /// `--conninfo <libpq-string-or-url>` (worldly postgres:// resources —
    /// libpq accepts its own URL format verbatim).
    Conninfo(String),
}

impl FatboyManager {
    /// Connect by spawning a PRIVATE, parent-scoped fatboy child and
    /// talking the relay protocol over its stdin/stdout (the LSP model).
    /// No socket file, no PDEATHSIG, no lease watchdog: the pipe is the
    /// lifecycle, and the transport reaps the child on drop, so it cannot
    /// outlive us. Portable across Linux/macOS/Windows with no per-OS code.
    pub fn connect(profile: &str, db: &str) -> Result<Self, String> {
        Self::connect_spec(profile, db.to_string(), SpawnSpec::Database(db.to_string()))
    }

    /// Connect to a worldly `postgres://` resource: the URL is handed to
    /// libpq verbatim as the conninfo (worldly syntax, worldly semantics).
    pub fn connect_postgres_url(url: &str, display_db: &str) -> Result<Self, String> {
        Self::connect_spec(
            "postgres",
            display_db.to_string(),
            SpawnSpec::Conninfo(url.to_string()),
        )
    }

    fn connect_spec(profile: &str, db: String, spawn: SpawnSpec) -> Result<Self, String> {
        let child = spawn_fatboy_stdio(profile, &spawn)?;
        let relay = FatboyRelay::from_child(child)?;
        Ok(Self {
            profile: profile.to_string(),
            db,
            spawn,
            relay,
        })
    }

    pub fn relay(&self) -> &FatboyRelay {
        &self.relay
    }

    /// A fresh protocol handler = a fresh fatboy child = a fresh
    /// foreign-engine session (1:1 pipe). Falls back to an always-erroring
    /// handler if the spawn fails (factories cannot return Err).
    pub fn new_remote_handler(&self) -> Box<dyn Handler + Send> {
        match spawn_fatboy_stdio(&self.profile, &self.spawn) {
            Ok(child) => match StdioTransport::from_child(child) {
                Ok(t) => Box::new(RemoteHandler::new(t)),
                Err(e) => Box::new(DeadFatboyHandler { message: e.message }),
            },
            Err(message) => Box::new(DeadFatboyHandler { message }),
        }
    }
}

/// Spawn a fatboy child: relay protocol over its stdin/stdout, diagnostics
/// inherited onto our stderr. The returned `Child` is handed to a
/// `StdioTransport`, which reaps it on drop.
fn spawn_fatboy_stdio(profile: &str, spawn: &SpawnSpec) -> Result<Child, String> {
    let bin = fatboy_binary(profile);
    let (flag, value) = match spawn {
        SpawnSpec::Database(db) => ("--database", db.as_str()),
        SpawnSpec::Conninfo(ci) => ("--conninfo", ci.as_str()),
    };
    Command::new(&bin)
        .arg(flag)
        .arg(value)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // stderr inherits — fatboy errors surface on dql's stderr.
        .spawn()
        .map_err(|e| {
            // The anti-"is the docker daemon running?" clause: name the
            // binary, the cause, and the fix.
            format!(
                "cannot spawn fatboy binary '{}': {e}\n\
                 (build it with `cargo build --bin dql-fatboy-{}`, put it \
                 on PATH, or set DQL_FATBOY_{}_BIN)",
                bin.display(),
                profile,
                profile.to_uppercase()
            )
        })
}

/// Handler returned when the fatboy disappeared between sessions:
/// answers every term with a Connection error (fail loud, not silent).
struct DeadFatboyHandler {
    message: String,
}

impl Handler for DeadFatboyHandler {
    fn handle(&mut self, _term: delightql_protocol::ClientTerm) -> ServerTerm {
        ServerTerm::Error {
            kind: delightql_protocol::ErrorKind::Connection,
            identity: b"delightql-error://target/postgres/connect".to_vec(),
            message: self.message.clone().into_bytes(),
        }
    }
}

// ---------------------------------------------------------------------------
// DatabaseConnection / DatabaseIntrospector / DatabaseSchema over the relay
// ---------------------------------------------------------------------------

pub struct FatboyConnection {
    relay: std::sync::Arc<FatboyManager>,
}

impl FatboyConnection {
    pub fn new(relay: std::sync::Arc<FatboyManager>) -> Self {
        Self { relay }
    }

    fn run(&self, sql: &str) -> delightql_types::Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        self.relay
            .relay()
            .query_nullable(sql)
            .map_err(|e| fatboy_db_error("Fatboy query failed", e))
    }
}

impl DatabaseConnection for FatboyConnection {
    fn execute(&self, sql: &str, _params: &[DbValue]) -> delightql_types::Result<usize> {
        let (_cols, rows) = self.run(sql)?;
        Ok(rows.len())
    }

    fn last_insert_rowid(&self) -> delightql_types::Result<i64> {
        Ok(0)
    }

    fn query_row_values(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> delightql_types::Result<Option<Vec<DbValue>>> {
        let (_cols, rows) = self.run(sql)?;
        Ok(rows.into_iter().next().map(|row| {
            row.into_iter()
                .map(|v| match v {
                    None => DbValue::Null,
                    Some(s) => DbValue::Text(s),
                })
                .collect()
        }))
    }

    fn query_all_string_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> delightql_types::Result<(Vec<String>, Vec<Vec<String>>)> {
        let (cols, rows) = self.run(sql)?;
        let string_rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| v.unwrap_or_else(|| "NULL".to_string()))
                    .collect()
            })
            .collect();
        Ok((cols, string_rows))
    }

    fn query_all_nullable_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> delightql_types::Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        self.run(sql)
    }
}

pub struct FatboyIntrospector {
    relay: std::sync::Arc<FatboyManager>,
}

impl FatboyIntrospector {
    pub fn new(relay: std::sync::Arc<FatboyManager>) -> Self {
        Self { relay }
    }
}

impl DatabaseIntrospector for FatboyIntrospector {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        let (_cols, rows) = self
            .relay
            .relay()
            .query_nullable(&introspect_sql(&self.relay.profile))
            .map_err(|e| fatboy_db_error("Fatboy introspection failed", e))?;

        // Rows ordered by (table_name, ordinal): fold into entities.
        let mut entities: Vec<DiscoveredEntity> = Vec::new();
        for row in rows {
            let get = |i: usize| -> String {
                row.get(i).cloned().flatten().unwrap_or_default()
            };
            let (table, ttype) = (get(0), get(1));
            let entity_type_id = if ttype.eq_ignore_ascii_case("VIEW") { 11 } else { 10 };
            if entities.last().map(|e: &DiscoveredEntity| e.name.as_str() != table)
                .unwrap_or(true)
            {
                entities.push(DiscoveredEntity {
                    name: table.clone().into(),
                    entity_type_id,
                    attributes: Vec::new(),
                });
            }
            let position: i32 = get(2).parse().unwrap_or(0);
            let notnull = get(5) == "1";
            entities.last_mut().unwrap().attributes.push(DiscoveredAttribute {
                name: get(3).into(),
                data_type: get(4),
                position,
                is_nullable: !notnull,
            });
        }
        Ok(entities)
    }

    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        // No ATTACH semantics through the fatboy; same as the pipe.
        Ok(vec![])
    }
}

pub struct FatboySchema {
    relay: std::sync::Arc<FatboyManager>,
}

impl FatboySchema {
    pub fn new(relay: std::sync::Arc<FatboyManager>) -> Self {
        Self { relay }
    }
}

impl DatabaseSchema for FatboySchema {
    fn get_table_columns(&self, _schema: Option<&str>, table_name: &str) -> Option<Vec<ColumnInfo>> {
        let escaped = table_name.replace('\'', "''");
        let sql = format!(
            "SELECT c.column_name AS name, \
             CASE WHEN c.is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull, \
             c.ordinal_position - 1 AS cid \
             FROM information_schema.columns c \
             WHERE c.table_schema = '{}' AND c.table_name = '{}' \
             ORDER BY c.ordinal_position",
            default_schema(&self.relay.profile),
            escaped
        );
        let (_cols, rows) = self.relay.relay().query_nullable(&sql).ok()?;
        if rows.is_empty() {
            return None;
        }
        Some(
            rows.iter()
                .enumerate()
                .map(|(i, row)| {
                    let get = |j: usize| row.get(j).cloned().flatten().unwrap_or_default();
                    ColumnInfo {
                        name: get(0).into(),
                        nullable: get(1) == "0",
                        position: get(2).parse().unwrap_or(i),
                    }
                })
                .collect(),
        )
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> bool {
        self.get_table_columns(schema, table_name)
            .map(|c| !c.is_empty())
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// ConnectionComponents assembly (mount path)
// ---------------------------------------------------------------------------

pub fn create_fatboy_system_components(
    mgr: &std::sync::Arc<FatboyManager>,
) -> anyhow::Result<delightql_types::ConnectionComponents> {
    Ok(delightql_types::ConnectionComponents {
        schema: Box::new(FatboySchema::new(mgr.clone())),
        connection: std::sync::Arc::new(Mutex::new(FatboyConnection::new(mgr.clone()))),
        introspector: Box::new(FatboyIntrospector::new(mgr.clone())),
        db_type: mgr.profile.clone(),
        mechanism: "fatboy".to_string(),
        identity: fatboy_resource_identity(mgr),
    })
}

/// Resource-asserted identity, obtained at connect (URI-DESIGN.md §4):
/// Postgres asserts its cluster system identifier; a DuckDB file's
/// identity is filesystem identity (canonical path). Failure to obtain
/// one degrades gracefully to None — identity strengthens dedupe, never
/// blocks a mount.
fn fatboy_resource_identity(mgr: &std::sync::Arc<FatboyManager>) -> Option<String> {
    match mgr.profile.as_str() {
        "postgres" => mgr
            .relay()
            .query_nullable("SELECT system_identifier FROM pg_control_system()")
            .ok()
            .and_then(|(_cols, rows)| rows.into_iter().next())
            .and_then(|row| row.into_iter().next().flatten())
            .map(|id| format!("pg-system-id:{id}")),
        "duckdb" => std::fs::canonicalize(&mgr.db)
            .ok()
            .map(|abs| format!("realpath:{}", abs.display())),
        _ => None,
    }
}

/// Execute SQL through the fatboy and return QueryResults (string-mode,
/// NULL rendered as empty — matches the engine's display convention).
pub(crate) fn execute_sql_with_fatboy(
    sql: &str,
    mgr: &std::sync::Arc<FatboyManager>,
) -> std::result::Result<delightql_backends::QueryResults, delightql_core::error::DelightQLError> {
    let (columns, rows) = mgr.relay().query_nullable(sql).map_err(|e| {
        delightql_core::error::DelightQLError::database_error(
            format!("Fatboy query failed: {}", e),
            e,
        )
    })?;
    let string_rows: Vec<Vec<String>> = rows
        .into_iter()
        .map(|row| row.into_iter().map(|v| v.unwrap_or_default()).collect())
        .collect();
    let row_count = string_rows.len();
    Ok(delightql_backends::QueryResults {
        columns,
        rows: string_rows,
        row_count,
    })
}
