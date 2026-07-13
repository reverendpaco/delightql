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
use std::sync::{Mutex, OnceLock};

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

/// The schema a mount introspects: the RECORDED per-mount schema when one
/// was bound (schema-mount Phase B/C), else the engine default for the
/// profile (a bare mount, Phase A). Keeping the fallback here means the
/// recorded `source_ns` and the introspected schema always name the same
/// thing (system.rs::default_engine_schema_for_type is the read-side twin).
fn effective_schema<'a>(mounted_schema: &'a Option<String>, profile: &str) -> &'a str {
    match mounted_schema.as_deref() {
        Some(s) => s,
        None => default_schema(profile),
    }
}

/// The per-engine enumeration of PERSISTENT schemas (EFFECTS-ON-TARGETS
/// §4.3, R-S2). PG keeps public + user + information_schema + pg_catalog
/// and excludes the transient `pg_temp_%` / `pg_toast*` prefixes; DuckDB
/// dedups and excludes `temp`/`system`. Shared by Phase B's existence
/// refusal and Phase C's `mount_tree!` enumeration.
fn schema_enumeration_sql(profile: &str) -> &'static str {
    match profile {
        "duckdb" => {
            "SELECT DISTINCT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('temp', 'system') \
             ORDER BY schema_name"
        }
        _ => {
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT LIKE 'pg_temp_%' \
               AND schema_name NOT LIKE 'pg_toast%' \
             ORDER BY schema_name"
        }
    }
}

/// Enumerate the target's persistent schemas over the fatboy relay.
fn enumerate_persistent_schemas(
    mgr: &std::sync::Arc<FatboyManager>,
) -> Result<Vec<String>, String> {
    let sql = schema_enumeration_sql(&mgr.profile);
    let (_cols, rows) = mgr.relay()?.query_nullable(sql)?;
    Ok(rows
        .into_iter()
        .filter_map(|r| r.into_iter().next().flatten())
        .collect())
}

fn introspect_sql(schema: &str) -> String {
    let schema_lit = schema.replace('\'', "''");
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
        schema_lit
    )
}

pub(crate) fn fatboy_name(profile: &str) -> String {
    format!("dql-fatboy-{}{}", profile, std::env::consts::EXE_SUFFIX)
}

/// The managed store — delightql's libexec (JOE-EVERYBODY-DISTRIBUTION.md
/// §3.2): per-version, private, never on PATH. Same ProjectDirs identity
/// as the REPL's history file. `dql target install` will create it; this
/// side only probes.
pub(crate) fn fatboy_store_dir() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "", "delightql").map(|dirs| {
        dirs.data_dir()
            .join("fatboys")
            .join(delightql_buildinfo::VERSION)
    })
}

/// The one env override: a DIRECTORY (git's GIT_EXEC_PATH precedent,
/// not a per-profile file variable), and a HARD pin — when set, dql
/// looks only here and a miss is a loud refusal. Fall-through would
/// let a typoed pin silently resolve to an older store binary: the
/// silent-wrong of configuration.
pub(crate) const FATBOY_DIR_ENV: &str = "DQL_FATBOY_DIR";

/// Where an adapter resolved from. One enum serves query-time
/// resolution, the refusal message, and `dql target list`, so their
/// vocabularies cannot drift.
pub(crate) enum FatboyLocation {
    /// DQL_FATBOY_DIR is set. The pin is hard: this is the answer
    /// whether or not the file exists — a missing pin refuses loudly
    /// at spawn instead of falling through.
    Pinned(PathBuf),
    Sibling(PathBuf),
    Store(PathBuf),
    OnPath(PathBuf),
    NotFound,
}

/// Walk the lookup chain: DQL_FATBOY_DIR (hard pin) → sibling of the
/// running dql → managed store → PATH.
pub(crate) fn locate_fatboy(profile: &str) -> FatboyLocation {
    let name = fatboy_name(profile);
    if let Ok(dir) = std::env::var(FATBOY_DIR_ENV) {
        return FatboyLocation::Pinned(PathBuf::from(dir).join(name));
    }
    if let Ok(me) = std::env::current_exe() {
        let sibling = me.with_file_name(&name);
        if sibling.is_file() {
            return FatboyLocation::Sibling(sibling);
        }
    }
    if let Some(store) = fatboy_store_dir() {
        let stored = store.join(&name);
        if stored.is_file() {
            return FatboyLocation::Store(stored);
        }
    }
    if let Some(paths) = std::env::var_os("PATH") {
        if let Some(found) = std::env::split_paths(&paths)
            .map(|d| d.join(&name))
            .find(|c| c.is_file())
        {
            return FatboyLocation::OnPath(found);
        }
    }
    FatboyLocation::NotFound
}

fn fatboy_binary(profile: &str) -> PathBuf {
    match locate_fatboy(profile) {
        FatboyLocation::Pinned(p)
        | FatboyLocation::Sibling(p)
        | FatboyLocation::Store(p)
        | FatboyLocation::OnPath(p) => p,
        // Hand the bare name to spawn so its NotFound carries the
        // OS-level cause; the refusal message does the explaining.
        FatboyLocation::NotFound => PathBuf::from(fatboy_name(profile)),
    }
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
    /// The handshaken relay session, spawned LAZILY on first `relay()`.
    /// Constructing a FatboyManager does NOT spawn a child: the CLI's
    /// `--db` entry (query.rs make_connection) builds one purely to carry
    /// route metadata (profile/db) and hands it to the REPL, where the
    /// only live fatboy operations refuse before touching the backend;
    /// the child that actually runs queries is spawned once by the mount!
    /// factory. Deferring the spawn is what collapses a one-shot fatboy
    /// `--db` query from two backend children to one.
    /// Pinned by tests/fatboy_spawn_count.rs.
    relay: OnceLock<FatboyRelay>,
    /// Serializes the lazy spawn so a concurrent first-use cannot mint two
    /// children (get_or_try_init is unstable; this is its stand-in).
    init: Mutex<()>,
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
        // Lazy: record the route and how to spawn, but do NOT spawn the
        // child here. The first `relay()` call materializes it. (See the
        // `relay` field's note — this is the one-spawn collapse.)
        Ok(Self {
            profile: profile.to_string(),
            db,
            spawn,
            relay: OnceLock::new(),
            init: Mutex::new(()),
        })
    }

    /// The handshaken relay session, spawning the fatboy child on the FIRST
    /// call (lazy — see the `relay` field). Later calls reuse it. Fallible:
    /// the spawn/handshake can fail, and that failure now surfaces here
    /// rather than at construction.
    pub fn relay(&self) -> Result<&FatboyRelay, String> {
        if let Some(r) = self.relay.get() {
            return Ok(r);
        }
        let _guard = self
            .init
            .lock()
            .map_err(|_| "fatboy init lock poisoned".to_string())?;
        // Re-check under the lock (another thread may have won the race).
        if let Some(r) = self.relay.get() {
            return Ok(r);
        }
        let child = spawn_fatboy_stdio(&self.profile, &self.spawn)?;
        let relay = FatboyRelay::from_child(child)?;
        let _ = self.relay.set(relay);
        Ok(self.relay.get().expect("relay just set under init lock"))
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
        .map_err(|e| fatboy_spawn_message(profile, &bin, &e))
}

/// The refusal a user reads when the adapter isn't there. Speaks the
/// user's register, not the workshop's (JOE-EVERYBODY-DISTRIBUTION.md
/// deviation 1): name the adapter, list every place dql looked, end
/// with the way forward. The from-source line is the install story
/// until `dql target install` exists to replace it.
fn fatboy_spawn_message(profile: &str, bin: &std::path::Path, e: &std::io::Error) -> String {
    if e.kind() != std::io::ErrorKind::NotFound {
        // Present but unstartable (permissions, wrong arch, …): saying
        // "not installed" would be a lie. Name the file and the cause.
        return format!(
            "cannot start the {} adapter '{}': {}",
            profile,
            bin.display(),
            e
        );
    }
    let name = fatboy_name(profile);
    if let Ok(dir) = std::env::var(FATBOY_DIR_ENV) {
        // The pin is hard: we looked only where it pointed, so listing
        // the other locations would misdescribe the search.
        return format!(
            "the {profile} adapter ({name}) is not in {FATBOY_DIR_ENV}.\n\
             {FATBOY_DIR_ENV} pins the adapter directory to: {dir}\n\
             dql looked only there; unset it to search normally."
        );
    }
    let sibling_dir = std::env::current_exe()
        .ok()
        .and_then(|me| me.parent().map(|d| d.display().to_string()))
        .unwrap_or_else(|| "<unknown>".to_string());
    let store_dir = fatboy_store_dir()
        .map(|d| d.display().to_string())
        .unwrap_or_else(|| "<no home directory>".to_string());
    format!(
        "the {profile} adapter ({name}) is not installed.\n\
         dql looked for it in:\n\
         - next to dql: {sibling_dir}\n\
         - the adapter store: {store_dir}\n\
         - PATH\n\
         To install it: dql target install {profile} --from <dir>\n\
         (from source: cargo build -p delightql-{profile}; \
         {FATBOY_DIR_ENV} overrides the search)"
    )
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
            .and_then(|r| r.query_nullable(sql))
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
    /// The mount's bound schema (schema-mount Phase A); `None` = the
    /// profile default. Feeds `introspect_sql` so a mount discovers the
    /// entities of the schema it actually bound, not always the default.
    mounted_schema: Option<String>,
}

impl FatboyIntrospector {
    pub fn new(relay: std::sync::Arc<FatboyManager>) -> Self {
        Self::with_schema(relay, None)
    }

    pub fn with_schema(
        relay: std::sync::Arc<FatboyManager>,
        mounted_schema: Option<String>,
    ) -> Self {
        Self {
            relay,
            mounted_schema,
        }
    }
}

impl DatabaseIntrospector for FatboyIntrospector {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        let schema = effective_schema(&self.mounted_schema, &self.relay.profile);
        let (_cols, rows) = self
            .relay
            .relay()
            .and_then(|r| r.query_nullable(&introspect_sql(schema)))
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
    /// The mount's bound schema (schema-mount Phase A); `None` = the
    /// profile default. Scopes the column lookup to the schema the mount
    /// actually bound.
    mounted_schema: Option<String>,
}

impl FatboySchema {
    pub fn new(relay: std::sync::Arc<FatboyManager>) -> Self {
        Self::with_schema(relay, None)
    }

    pub fn with_schema(
        relay: std::sync::Arc<FatboyManager>,
        mounted_schema: Option<String>,
    ) -> Self {
        Self {
            relay,
            mounted_schema,
        }
    }
}

impl DatabaseSchema for FatboySchema {
    fn get_table_columns(&self, _schema: Option<&str>, table_name: &str) -> Option<Vec<ColumnInfo>> {
        let escaped = table_name.replace('\'', "''");
        // Schema scoping, per profile (E-T5): on Postgres the lookup
        // prefers the SESSION'S OWN temp schema when the name is temp-held,
        // else the mounted schema — the same COALESCE scoping E-T4 ruled
        // for the registration read-back (`created_object_readback_sql`),
        // and PG's own resolution order (temp shadows public for
        // unqualified names, P1 §B). Without it a plan-created temp table
        // (`|> temp_table!(staged)`) is invisible to the NEXT statement's
        // column lookup — the read-back registers it, then live resolution
        // scoped to 'public' answers None ("Table not found"). DuckDB never
        // had the hole: its temp objects live in schema `main` (catalog
        // `temp`), the same schema name this query scopes to (P3 §B).
        // Pinned live by
        // `pg_temp_readback_round_trip_and_table_bang_lands_in_public`
        // (crates/delightql-cli/tests/effects_on_targets.rs).
        let mounted = effective_schema(&self.mounted_schema, &self.relay.profile);
        let mounted_lit = mounted.replace('\'', "''");
        let schema_scope = if self.relay.profile == "postgres" {
            format!(
                "COALESCE((SELECT tn.nspname FROM pg_class t \
                  JOIN pg_namespace tn ON tn.oid = t.relnamespace \
                  WHERE t.relname = '{}' \
                    AND t.relnamespace = pg_my_temp_schema()), '{}')",
                escaped, mounted_lit
            )
        } else {
            format!("'{}'", mounted_lit)
        };
        let sql = format!(
            "SELECT c.column_name AS name, \
             CASE WHEN c.is_nullable = 'YES' THEN 0 ELSE 1 END AS notnull, \
             c.ordinal_position - 1 AS cid, \
             c.data_type AS data_type \
             FROM information_schema.columns c \
             WHERE c.table_schema = {} AND c.table_name = '{}' \
             ORDER BY c.ordinal_position",
            schema_scope,
            escaped
        );
        let (_cols, rows) = self.relay.relay().ok()?.query_nullable(&sql).ok()?;
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
                        declared_type: Some(get(3)).filter(|t| !t.is_empty()),
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
    mounted_schema: Option<String>,
) -> anyhow::Result<delightql_types::ConnectionComponents> {
    // R-S4: refuse (loudly) when the resolved schema does not exist on the
    // target rather than bind an empty namespace. The resolved schema is the
    // spelled `#schema` (Phase B) or the engine default (public/main) for a
    // bare mount; both must be a persistent schema of the target. Checked
    // against the SAME enumeration Phase C mounts (build once, share).
    let resolved = effective_schema(&mounted_schema, &mgr.profile).to_string();
    let schemas = enumerate_persistent_schemas(mgr)
        .map_err(|e| anyhow::anyhow!("could not enumerate schemas on {}: {}", mgr.db, e))?;
    if !schemas.iter().any(|s| s == &resolved) {
        anyhow::bail!(
            "schema '{}' does not exist on {} database '{}' (available: {})",
            resolved,
            mgr.profile,
            mgr.db,
            schemas.join(", ")
        );
    }
    Ok(delightql_types::ConnectionComponents {
        schema: Box::new(FatboySchema::with_schema(mgr.clone(), mounted_schema.clone())),
        connection: std::sync::Arc::new(Mutex::new(FatboyConnection::new(mgr.clone()))),
        introspector: Box::new(FatboyIntrospector::with_schema(
            mgr.clone(),
            mounted_schema.clone(),
        )),
        db_type: mgr.profile.clone(),
        mechanism: "fatboy".to_string(),
        identity: fatboy_resource_identity(mgr),
        // A bare mount records `None` (the engine default, resolved
        // downstream — behavior-identical to Phase A). A spelled `#schema`
        // travels here → the cartridge's `source_ns` → read qualification
        // and durable placement (Phase A flag 3).
        mounted_schema,
    })
}

/// Phase C: one components per PERSISTENT schema, ALL sharing this ONE
/// FatboyManager (one child / one relay). Each components carries
/// `mounted_schema = Some(schema)` and the SAME resource identity, so the
/// bootstrap `connection` dedup (by identity) folds every sub-namespace
/// onto ONE connection_id — the load-bearing R-S1 property that makes a
/// cross-schema `run!` a single-connection, one-bracket plan.
pub fn create_fatboy_tree_components(
    mgr: &std::sync::Arc<FatboyManager>,
) -> anyhow::Result<Vec<(String, delightql_types::ConnectionComponents)>> {
    let schemas = enumerate_persistent_schemas(mgr)
        .map_err(|e| anyhow::anyhow!("could not enumerate schemas on {}: {}", mgr.db, e))?;
    let identity = fatboy_resource_identity(mgr);
    let mut out = Vec::with_capacity(schemas.len());
    for schema in schemas {
        out.push((
            schema.clone(),
            delightql_types::ConnectionComponents {
                schema: Box::new(FatboySchema::with_schema(mgr.clone(), Some(schema.clone()))),
                connection: std::sync::Arc::new(Mutex::new(FatboyConnection::new(mgr.clone()))),
                introspector: Box::new(FatboyIntrospector::with_schema(
                    mgr.clone(),
                    Some(schema.clone()),
                )),
                db_type: mgr.profile.clone(),
                mechanism: "fatboy".to_string(),
                identity: identity.clone(),
                mounted_schema: Some(schema),
            },
        ));
    }
    Ok(out)
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
            .ok()
            .and_then(|r| {
                r.query_nullable("SELECT system_identifier FROM pg_control_system()")
                    .ok()
            })
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
    let (columns, rows) = mgr
        .relay()
        .and_then(|r| r.query_nullable(sql))
        .map_err(|e| {
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
