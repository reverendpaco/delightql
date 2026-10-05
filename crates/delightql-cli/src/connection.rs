// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Multi-database connection wrapper
///
/// Provides a unified interface for SQLite, siso, and fatboy connections
use anyhow::Result;
use delightql_backends::SqliteConnectionManager;
use delightql_types::diagnostic::{Client, DelightQLError, Mount};
use delightql_types::DatabaseConnection;
use std::sync::{Arc, Mutex, OnceLock};

/// True when the string is URI-shaped (`scheme://...`) rather than a file
/// path. One shared test so no caller can fall through to file handling.
pub fn looks_like_uri(path: &str) -> bool {
    match path.find("://") {
        Some(end) if end > 0 => path[..end]
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')),
        _ => false,
    }
}

/// Split a trailing `#schema` FRAGMENT off a URI-shaped mount target,
/// CLIENT-SIDE (EFFECTS-ON-TARGETS-PLAN §4.2): a fragment is a locator
/// WITHIN the resource and is never sent to the server — libpq / the
/// DuckDB adapter receive only the base. `postgres:///db#production`
/// → (`postgres:///db`, Some("production")); no fragment → (input, None).
/// Only URI-shaped inputs are split; a bare file path keeps any `#`
/// verbatim (a legit filename character), so the fragment surface is a
/// deliberate URI feature. `?schema=` is NOT a fragment and is never
/// consulted here — it reads as fake conninfo, and is refused as such.
pub fn split_schema_fragment(input: &str) -> (String, Option<String>) {
    if !looks_like_uri(input) {
        return (input.to_string(), None);
    }
    match input.split_once('#') {
        Some((base, frag)) if !frag.is_empty() => (base.to_string(), Some(frag.to_string())),
        _ => (input.to_string(), None),
    }
}

/// One classified route for a `--db` / `mount!()` input — THE single
/// scheme-dispatch point (users speak in resources, DelightQL chooses
/// mechanisms). An unknown scheme teaches; it can never fall through to
/// file-path handling.
#[derive(Debug, Clone, PartialEq)]
pub enum Route {
    /// A SQLite file — in-process.
    Sqlite(String),
    /// A DuckDB file (magic bytes / extension) — the duckdb fatboy.
    DuckdbFatboy(String),
    /// A worldly postgres resource — the postgres fatboy, with the URL
    /// handed to libpq verbatim as conninfo (worldly syntax, worldly
    /// semantics; `postgres:///db` is libpq's own env-completed form).
    PostgresFatboy { url: String, display_db: String },
    /// `delightql-siso://<profile>[/<target>]` — the pipe-coprocess
    /// residue for resources with no worldly URI (osqueryi and kin).
    Siso { rest: String },
}

/// Classify a `--db` / mount input. `via` is the mechanism override
/// (`--via`); it applies to postgres resources (fatboy | siso).
/// A locator this session cannot open, as the typed mount refusal.
fn locator(message: String) -> DelightQLError {
    Mount::Locator { message }.into()
}

pub fn classify(input: &str, via: Option<&str>) -> delightql_types::Result<Route> {
    if let Some(v) = via {
        if !matches!(v, "fatboy" | "siso") {
            return Err(Client::Argument {
                message: format!("unknown --via '{v}' (known mechanisms: fatboy, siso)"),
            }
            .into());
        }
    }

    if let Some(rest) = input.strip_prefix("delightql-siso://") {
        if rest.is_empty() {
            return Err(locator(
                "delightql-siso:// needs a profile: delightql-siso://<profile>[/<target>]"
                    .to_string(),
            ));
        }
        return Ok(Route::Siso {
            rest: rest.to_string(),
        });
    }

    if looks_like_uri(input) {
        let scheme_end = input.find("://").expect("looks_like_uri checked");
        let scheme = input[..scheme_end].to_ascii_lowercase();
        return match scheme.as_str() {
            "postgres" | "postgresql" => {
                let url = url::Url::parse(input)
                    .map_err(|e| locator(format!("'{input}': not a valid postgres URL: {e}")))?;
                if url.password().is_some() {
                    return Err(locator(format!(
                        "'{input}': passwords are never accepted in connection URLs \
                         (they would persist into session metadata). Set PGPASSWORD \
                         in the environment instead."
                    )));
                }
                let display_db = url.path().trim_start_matches('/').to_string();
                match via {
                    None | Some("fatboy") => Ok(Route::PostgresFatboy {
                        url: input.to_string(),
                        display_db,
                    }),
                    Some("siso") => Ok(Route::Siso {
                        rest: if display_db.is_empty() {
                            "postgres".to_string()
                        } else {
                            format!("postgres/{display_db}")
                        },
                    }),
                    Some(_) => unreachable!("via validated above"),
                }
            }
            "file" => {
                // RFC 8089: file:///path (empty authority) or file://localhost/path.
                let url = url::Url::parse(input)
                    .map_err(|e| locator(format!("'{input}': not a valid file URL: {e}")))?;
                match url.host_str() {
                    None | Some("") | Some("localhost") => {}
                    Some(h) => {
                        return Err(locator(format!(
                            "'{input}': file URLs with a remote host ('{h}') are not \
                             supported — file:///absolute/path only."
                        )))
                    }
                }
                classify_file_path(url.path(), via)
            }
            other => Err(locator(format!(
                "'{input}': unsupported URI scheme '{other}://'. Known: \
                 postgres://, file://, delightql-siso://, or a plain file path."
            ))),
        };
    }

    classify_file_path(input, via)
}

/// Classify a filesystem path by magic bytes / extension.
fn classify_file_path(path: &str, via: Option<&str>) -> delightql_types::Result<Route> {
    use std::io::Read;

    if let Some(v) = via {
        if v != "fatboy" {
            return Err(Client::Argument {
                message: format!("--via {v} does not apply to file-backed databases"),
            }
            .into());
        }
    }

    // DuckDB magic: "DUCK" at offset 8 of the 16-byte header.
    if let Ok(mut file) = std::fs::File::open(path) {
        let mut header = [0u8; 16];
        if file.read_exact(&mut header).is_ok() && &header[8..12] == b"DUCK" {
            return Ok(Route::DuckdbFatboy(path.to_string()));
        }
    }
    if path.ends_with(".duckdb") || path.ends_with(".ddb") {
        return Ok(Route::DuckdbFatboy(path.to_string()));
    }

    // SQLite for .db/.sqlite/anything else (including files to create).
    Ok(Route::Sqlite(path.to_string()))
}

/// Connection information structure (unified across all database types)
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionInfo {
    pub database_type: String,
    pub path: Option<String>,
    pub is_memory: bool,
    pub is_connected: bool,
}

/// Unified connection manager supporting multiple database backends
#[derive(Clone)]
pub enum ConnectionManager {
    SQLite(SqliteConnectionManager),
    Pipe(Arc<delightql_cli_siso::PipeConnectionManager>),
    /// A fatboy process: relay protocol over a Unix socket, foreign
    /// engine behind it (`postgres://host/<db>`, or a DuckDB file).
    Fatboy(Arc<crate::fatboy_exec::FatboyManager>),
}

impl ConnectionManager {
    /// Open a classified route (see [`classify`]).
    pub fn open_route(route: Route) -> delightql_types::Result<Self> {
        match route {
            Route::Sqlite(path) => Ok(ConnectionManager::SQLite(
                SqliteConnectionManager::new_file(&path)?,
            )),
            Route::DuckdbFatboy(path) => {
                let mgr = crate::fatboy_exec::FatboyManager::connect("duckdb", &path)?;
                Ok(ConnectionManager::Fatboy(Arc::new(mgr)))
            }
            Route::PostgresFatboy { url, display_db } => {
                let mgr =
                    crate::fatboy_exec::FatboyManager::connect_postgres_url(&url, &display_db)?;
                Ok(ConnectionManager::Fatboy(Arc::new(mgr)))
            }
            Route::Siso { rest } => {
                let mgr = delightql_cli_siso::PipeConnectionManager::from_uri(&format!(
                    "delightql-siso://{rest}"
                ))
                .map_err(|e| delightql_cli_siso::error::diagnostic("delightql-siso://", e))?;
                Ok(ConnectionManager::Pipe(Arc::new(mgr)))
            }
        }
    }

    /// Open from a `--db` / mount input string with a mechanism override.
    pub fn open(input: &str, via: Option<&str>) -> delightql_types::Result<Self> {
        Self::open_route(classify(input, via)?)
    }

    /// Create a new connection from a resource string (path or worldly
    /// URI), default mechanisms. Kept as the factory-facing entry point.
    pub fn new_file(path: &str) -> delightql_types::Result<Self> {
        Self::open(path, None)
    }

    /// Create a new in-memory connection (defaults to SQLite)
    pub fn new_memory() -> delightql_types::Result<Self> {
        Ok(ConnectionManager::SQLite(
            SqliteConnectionManager::new_memory()?,
        ))
    }

    /// Test the connection
    #[allow(dead_code)]
    pub fn test_connection(&self) -> delightql_types::Result<()> {
        match self {
            ConnectionManager::SQLite(conn) => Ok(conn.test_connection()?),
            ConnectionManager::Pipe(mgr) => {
                let _conn = mgr
                    .connect()
                    .map_err(|e| delightql_cli_siso::error::diagnostic("pipe connect", e))?;
                Ok(())
            }
            // Fatboy children connect lazily (fatboy_exec FatboyManager::relay);
            // there is no eager connection to test here.
            ConnectionManager::Fatboy(_) => Ok(()),
        }
    }

    /// Get the database type name
    pub fn database_type(&self) -> &str {
        match self {
            ConnectionManager::SQLite(_) => "SQLite",
            ConnectionManager::Pipe(mgr) => mgr.profile_name(),
            ConnectionManager::Fatboy(mgr) => &mgr.profile,
        }
    }

    #[allow(dead_code)]
    pub fn as_sqlite(&self) -> Option<&SqliteConnectionManager> {
        match self {
            ConnectionManager::SQLite(conn) => Some(conn),
            _ => None,
        }
    }

    /// The raw SQLite handle. Only the in-process SQLite manager owns one;
    /// every other backend reaches its database through a protocol, so
    /// asking them for a `rusqlite::Connection` is a caller error, not a
    /// missing capability.
    pub fn get_connection_arc(&self) -> std::sync::Arc<std::sync::Mutex<rusqlite::Connection>> {
        match self {
            ConnectionManager::SQLite(conn) => conn.get_connection_arc(),
            ConnectionManager::Pipe(_) => {
                panic!("Cannot get SQLite connection from Pipe - use database-agnostic APIs")
            }
            ConnectionManager::Fatboy(_) => {
                panic!("Cannot get SQLite connection from Fatboy - use database-agnostic APIs")
            }
        }
    }

    /// Get database connection as a trait object (database-agnostic)
    pub fn get_database_connection(&self) -> Arc<Mutex<dyn DatabaseConnection>> {
        match self {
            ConnectionManager::SQLite(conn) => {
                let adapter =
                    delightql_backends::sqlite::SqliteConnection::new(conn.get_connection_arc());
                Arc::new(Mutex::new(adapter))
            }
            ConnectionManager::Pipe(mgr) => {
                let conn = mgr.connect().expect("Failed to spawn pipe connection");
                Arc::new(Mutex::new(conn))
            }
            ConnectionManager::Fatboy(mgr) => Arc::new(Mutex::new(
                crate::fatboy_exec::FatboyConnection::new(mgr.clone()),
            )),
        }
    }

    /// Get connection information
    pub fn connection_info(&self) -> Result<ConnectionInfo> {
        match self {
            ConnectionManager::SQLite(conn) => {
                let info = conn.connection_info()?;
                Ok(ConnectionInfo {
                    database_type: info.database_type,
                    path: info.path,
                    is_memory: info.is_memory,
                    is_connected: info.is_connected,
                })
            }
            ConnectionManager::Pipe(mgr) => Ok(ConnectionInfo {
                database_type: format!("Pipe({})", mgr.profile_name()),
                path: mgr.target().map(|s| s.to_string()),
                is_memory: false,
                is_connected: true,
            }),
            ConnectionManager::Fatboy(mgr) => Ok(ConnectionInfo {
                database_type: format!("Fatboy({})", mgr.profile),
                path: Some(format!("{} [stdio]", mgr.db)),
                is_memory: false,
                is_connected: true,
            }),
        }
    }

    /// Attach another database file with a schema name (SQLite only for now)
    pub fn attach_database(&self, db_path: &str, schema_name: &str) -> Result<()> {
        match self {
            ConnectionManager::SQLite(conn) => conn
                .attach_database_file(db_path, schema_name)
                .map_err(|e| anyhow::anyhow!("Failed to attach database: {}", e)),
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("Database attachment not supported for pipe connections")
            }
            ConnectionManager::Fatboy(_) => {
                anyhow::bail!("Database attachment not supported for fatboy connections")
            }
        }
    }

    /// Get raw SQLite connection for import operations
    /// Returns the underlying Arc<Mutex<rusqlite::Connection>> for SQLite connections
    ///
    /// This is used by import operations that need direct access to the connection
    /// to work with _bootstrap.* tables.
    pub fn get_raw_sqlite_connection(&self) -> Result<Arc<Mutex<rusqlite::Connection>>> {
        match self {
            ConnectionManager::SQLite(conn) => Ok(conn.get_connection_arc()),
            ConnectionManager::Pipe(_) => {
                anyhow::bail!("Import operations not supported for pipe connections")
            }
            ConnectionManager::Fatboy(_) => {
                anyhow::bail!("Import operations not supported for fatboy connections")
            }
        }
    }

    /// Execute a SQL query against the underlying database connection.
    ///
    /// Dispatches to the appropriate backend (SQLite, Pipe, or Fatboy).
    pub fn execute_query(&self, sql: &str) -> Result<delightql_backends::QueryResults> {
        match self {
            ConnectionManager::SQLite(conn) => {
                delightql_backends::execute_sql_with_connection(sql.to_string(), conn)
                    .map_err(|e| anyhow::anyhow!("{}", e))
            }
            ConnectionManager::Pipe(mgr) => crate::pipe_exec::execute_sql_with_pipe(sql, mgr)
                .map_err(|e| anyhow::anyhow!("{}", e)),
            ConnectionManager::Fatboy(mgr) => crate::fatboy_exec::execute_sql_with_fatboy(sql, mgr)
                .map_err(|e| anyhow::anyhow!("{}", e)),
        }
    }

    /// Create ConnectionComponents for `open()`.
    ///
    /// The CLI never touches the individual components — it passes the
    /// opaque struct straight to `delightql_core::api::open()`.
    ///
    /// `mounted_schema` is the client-parsed `#schema` fragment (Phase B):
    /// it threads to the recorded `source_ns` AND the introspector/schema
    /// scope (so the recorded fact and the introspected schema agree). A
    /// fragment on a SQLite target REFUSES (R-S5): SQLite has no schemas.
    pub fn create_system_components(
        &self,
        mounted_schema: Option<String>,
    ) -> delightql_types::Result<delightql_types::ConnectionComponents> {
        match self {
            ConnectionManager::SQLite(sqlite_conn) => {
                if mounted_schema.is_some() {
                    return Err(locator(
                        "SQLite has no schemas; a #schema fragment is only meaningful \
                         on a Postgres or DuckDB target (use mount! without a fragment)"
                            .to_string(),
                    ));
                }
                let raw_conn_arc = sqlite_conn.get_connection_arc();
                let schema = Box::new(delightql_backends::DynamicSqliteSchema::new(
                    raw_conn_arc.clone(),
                ));
                let introspector = Box::new(delightql_backends::sqlite::SqliteIntrospector::new(
                    raw_conn_arc.clone(),
                ));
                let adapter =
                    delightql_backends::sqlite::SqliteConnection::new(raw_conn_arc.clone());
                let conn_arc: Arc<Mutex<dyn DatabaseConnection>> = Arc::new(Mutex::new(adapter));
                let identity = raw_conn_arc
                    .lock()
                    .ok()
                    .and_then(|c| c.path().map(|p| p.to_string()))
                    .filter(|p| !p.is_empty())
                    .and_then(|p| std::fs::canonicalize(&p).ok())
                    .map(|abs| format!("realpath:{}", abs.display()));
                Ok(delightql_types::ConnectionComponents {
                    schema,
                    connection: conn_arc,
                    introspector,
                    db_type: "sqlite".to_string(),
                    mechanism: "in-process".to_string(),
                    identity,
                    // SQLite has no schema concept (R-S5); leave unset.
                    mounted_schema: None,
                })
            }
            // siso (pipe) is a fallback mechanism outside the schema-mount
            // scope (R-S5: fatboy PG+DuckDB); a fragment on it is ignored.
            ConnectionManager::Pipe(mgr) => crate::pipe_exec::create_pipe_system_components(mgr),
            ConnectionManager::Fatboy(mgr) => {
                crate::fatboy_exec::create_fatboy_system_components(mgr, mounted_schema)
            }
        }
    }
}

/// THE SESSION PROFILE: the complete capability decision for one handle,
/// chosen once by the command that opens it. The profile owns which
/// types-level mount factory the handle receives, which namespaces are
/// published into it automatically, and which inert byte bindings it
/// carries; nothing below it rediscovers any of that from process state,
/// and a handle is constructed only through `SessionProfile::open`.
pub enum SessionProfile {
    /// The prompt, the one-shot query, and every other subcommand: the
    /// CLI's own surfaces are published — `repl::*` over the named client
    /// database, and `cli::surface` — beside the inert bindings. `None` is
    /// the road when the in-memory engine refused a client database:
    /// `repl::*` is unavailable and said so, and the handle is otherwise
    /// the same.
    Client(Option<std::sync::Arc<crate::client::database::ClientDatabase>>),
    /// `dql server`: the canonical Core image, explicit user/data mounts,
    /// and the inert `book`/`man`/`editor` bindings. The handle receives no
    /// client mount factory, so no query it serves can mount the process's
    /// client database, and it publishes neither `repl::*` nor
    /// `cli::surface` — before or after any protocol Reset, which restores
    /// only this profile. The process's client database still records the
    /// server's session and incidents on the host side.
    Server,
}

impl SessionProfile {
    /// The client profile over the process's own client database — the
    /// one every client-road command opens. The database is an ingredient
    /// the caller names; the profile is the caller's choice.
    pub fn client() -> Self {
        SessionProfile::Client(crate::client::context::process_database())
    }

    /// THE ONE HANDLE CONSTRUCTION. Every handle the CLI opens comes
    /// through here, and the profile decides everything in one exhaustive
    /// judgment: the mount factory at open, the inert bindings, and the
    /// automatic namespaces. Answers the handle and whether `repl::*` was
    /// installed on it (a failed install is degraded client diagnostics,
    /// said once, never a reason to refuse the handle).
    pub fn open(self) -> Result<(Box<dyn delightql_core::api::DqlHandle>, bool)> {
        let factory = Box::new(crate::connection_factory::CliConnectionFactory);
        // The types-level factory powers mount!/import! of URI-scheme
        // databases (postgres://, delightql-siso://, …). The client
        // profile's factory ALSO answers the private session locator with
        // the live client connection; the server's factory knows no such
        // locator, so the capability does not exist on that road.
        let mount_factory: Box<dyn delightql_types::ConnectionFactory> = match &self {
            SessionProfile::Client(Some(db)) => {
                Box::new(crate::client::mount::ReplMountFactory::over(db))
            }
            SessionProfile::Client(None) | SessionProfile::Server => {
                Box::new(crate::connection_factory::CliConnectionFactory)
            }
        };
        let mut handle = delightql_core::api::open(factory, Some(mount_factory), boot_settings())
            .map_err(anyhow::Error::new)?;
        // The CLI's embedded database images are BOUND on every profile. A
        // binding is a name→bytes map entry, not a mount: no attachment, no
        // I/O, no cost until a session actually runs
        // `mount!("delightql-bytes://book", ...)`. Binding on every handle
        // is what makes the locators typeable from any session.
        for (name, bytes) in [
            ("book", crate::embedded_db::BOOK_BYTES),
            ("man", crate::embedded_db::MAN_BYTES),
            ("editor", crate::embedded_db::EDITOR_BYTES),
        ] {
            handle
                .bind_static_bytes(name, bytes)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
        }
        match self {
            SessionProfile::Server => Ok((handle, false)),
            SessionProfile::Client(db) => {
                let installed = match db {
                    None => false,
                    Some(_) => match crate::client::mount::install_repl_namespace(&mut *handle) {
                        Ok(()) => true,
                        Err(e) => {
                            crate::client::incident::warning(
                                "namespace",
                                delightql_types::diagnostic::Client::NamespaceInstall {
                                    message: format!(
                                        "the repl::* namespace could not be installed ({e}); \
                                         repl::* will be unavailable until the next session reset"
                                    ),
                                },
                            );
                            false
                        }
                    },
                };
                let handle = crate::cli_surface::attach(handle)?;
                Ok((handle, installed))
            }
        }
    }
}

/// Open a DqlHandle under `profile`.
///
/// Returns `Box<dyn DqlHandle>` — the compiler-enforced API boundary.
/// The handle starts with an empty "main" namespace. The CLI must send
/// `mount!("path", "main")` to populate it.
///
/// This is a FREE function, not a method on `ConnectionManager`, because it
/// builds the session purely from the factories and never consulted a
/// manager's own backend — the session's one backend is created by the
/// `mount!` first-query, not by any pre-opened `ConnectionManager`. Keeping
/// it self-less makes that separation explicit: a `&self` method that
/// ignores `self` reads as if a manager fed the handle.
/// What `dql` tells core at boot. Only this process knows its working
/// directory, so it states it; a process whose directory cannot be read
/// states none, and core then refuses a relative path rather than guessing.
/// The dialect this invocation states at boot, fixed by `main` before any
/// handle opens. Unset in library use: every handle then states none, and
/// each query takes its connection's dialect.
static DIALECT: OnceLock<String> = OnceLock::new();

/// Fix the dialect every handle this process opens will state.
pub fn state_dialect(dialect: Option<String>) {
    if let Some(dialect) = dialect {
        let stated = DIALECT.get_or_init(|| dialect.clone());
        debug_assert_eq!(
            *stated, dialect,
            "the invocation's dialect was already stated"
        );
    }
}

fn boot_settings() -> delightql_core::api::BootSettings {
    let base = std::env::current_dir()
        .ok()
        .and_then(|dir| dir.to_str().map(str::to_string));
    let boot = delightql_core::api::BootSettings::new()
        .state(delightql_core::api::BASE_DIRECTORY, base.as_deref());
    match DIALECT.get() {
        Some(dialect) => boot.state(delightql_core::api::DIALECT, Some(dialect)),
        None => boot,
    }
}

pub fn open_handle(profile: SessionProfile) -> Result<Box<dyn delightql_core::api::DqlHandle>> {
    profile.open().map(|(handle, _)| handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The contract binds every host: one that states nothing is refused
    /// before any connection or catalog exists, and the refusal names what
    /// it owes.
    #[test]
    fn a_host_that_states_nothing_is_refused_at_open() {
        let refused = match delightql_core::api::open(
            Box::new(crate::connection_factory::CliConnectionFactory),
            None,
            delightql_core::api::BootSettings::new(),
        ) {
            Ok(_) => panic!("a host that stated nothing was admitted"),
            Err(error) => error.to_string(),
        };
        assert!(
            refused.contains("'base_directory' is required"),
            "{refused}"
        );
        assert!(refused.contains("state it as none"), "{refused}");
    }

    /// `dql` states its working directory, and core publishes it.
    #[test]
    fn dql_states_its_working_directory_at_boot() {
        let mut handle = open_handle(SessionProfile::client()).expect("handle");
        let mut session = handle.session().expect("session");
        let rows = crate::exec_ng::run_dql_query(
            "sys::config.setting(*) |> (key, layer, value)",
            &mut *session,
        )
        .expect("the settings answer");
        let here = std::env::current_dir().expect("cwd");
        assert_eq!(
            rows.rows,
            [[
                "base_directory".to_string(),
                "boot".to_string(),
                here.to_str().expect("utf-8 cwd").to_string()
            ]]
        );
    }

    /// A private client database on the `Other` road, never the process's.
    fn private_db() -> std::sync::Arc<crate::client::database::ClientDatabase> {
        std::sync::Arc::new(
            crate::client::database::ClientDatabase::open_on(crate::client::context::Mode::Other)
                .expect("client database"),
        )
    }

    fn run(
        session: &mut dyn delightql_core::api::DqlSession,
        dql: &str,
    ) -> std::result::Result<usize, String> {
        crate::exec_ng::run_dql_query(dql, session)
            .map(|r| r.rows.len())
            .map_err(|e| e.to_string())
    }

    /// What the server profile must refuse, and what it must still serve,
    /// in one world — asserted before and after a Reset, since a Reset
    /// restores only the profile.
    fn assert_server_world(handle: &mut dyn delightql_core::api::DqlHandle) {
        let mut session = handle.session().expect("session");
        let repl = run(&mut *session, "repl::context.session(*)");
        assert!(
            repl.is_err(),
            "repl::* is published on the server road: {repl:?}"
        );
        let surface = run(&mut *session, "cli::surface.command(*)");
        assert!(
            surface.is_err(),
            "cli::surface is published on the server road: {surface:?}"
        );
        let leak = run(
            &mut *session,
            "mount!(\"delightql-repl://session\", \"leak\")(*)",
        );
        assert!(
            leak.is_err(),
            "the server road can mount the process's client database: {leak:?}"
        );
        assert!(
            !leak.as_ref().unwrap_err().contains("leak.session"),
            "the refusal must be the locator's, before any relation: {leak:?}"
        );
        // The inert documentation bindings stay typeable and mountable.
        run(
            &mut *session,
            "mount!(\"delightql-bytes://book\", \"cli::book\")(*)",
        )
        .expect("the book binding mounts on the server road");
    }

    /// SERVER PROFILE: no `repl::*`, no `cli::surface`, no capability to
    /// mount the private session locator — before and after a Reset —
    /// while the inert `book`/`man`/`editor` bindings remain usable.
    #[test]
    fn a_server_profile_has_no_client_surface_before_or_after_reset() {
        let (mut handle, installed) = SessionProfile::Server.open().expect("server handle");
        assert!(!installed);
        assert_server_world(&mut *handle);
        handle.recover_session().expect("reset");
        assert_server_world(&mut *handle);
        let mut session = handle.session().expect("session");
        run(
            &mut *session,
            "mount!(\"delightql-bytes://man\", \"cli::man\")(*)",
        )
        .expect("the man binding mounts after a reset");
        run(
            &mut *session,
            "mount!(\"delightql-bytes://editor\", \"cli::editor\")(*)",
        )
        .expect("the editor binding mounts after a reset");
    }

    /// CLIENT PROFILE over a database: `repl::*` is installed and answers,
    /// `cli::surface` is attached, and the session locator is the
    /// factory's to answer.
    #[test]
    fn a_client_profile_publishes_its_surfaces() {
        let db = private_db();
        let (mut handle, installed) = SessionProfile::Client(Some(db.clone()))
            .open()
            .expect("client handle");
        assert!(installed, "repl::* was not installed on the client road");
        let mut session = handle.session().expect("session");
        assert_eq!(run(&mut *session, "repl::context.session(*)").unwrap(), 1);
        assert!(run(&mut *session, "cli::surface.command(*)").unwrap() > 0);
        run(
            &mut *session,
            "mount!(\"delightql-repl://session\", \"again\")(*)",
        )
        .expect("the client road's factory answers the session locator");
    }

    /// CLIENT PROFILE without a database (the engine refused one): the
    /// surface still attaches; `repl::*` is absent and the locator is
    /// unknown, because there is no client database to reach.
    #[test]
    fn a_client_profile_without_a_database_keeps_the_surface_only() {
        let (mut handle, installed) = SessionProfile::Client(None).open().expect("client handle");
        assert!(!installed);
        let mut session = handle.session().expect("session");
        assert!(run(&mut *session, "cli::surface.command(*)").unwrap() > 0);
        assert!(run(&mut *session, "repl::context.session(*)").is_err());
        assert!(run(
            &mut *session,
            "mount!(\"delightql-repl://session\", \"leak\")(*)"
        )
        .is_err());
    }

    /// HOST-SIDE INCIDENTS: a server's incident lands in the process's
    /// client database, and that database is not query-reachable from a
    /// server world.
    #[test]
    fn server_incidents_land_in_the_client_database_without_being_query_reachable() {
        use crate::client::incident::{Incident, IncidentKind};
        let db = private_db();
        let diagnostic: delightql_core::error::DelightQLError =
            delightql_types::diagnostic::Client::NamespaceInstall {
                message: "probe: a server-side incident".to_string(),
            }
            .into();
        db.record_incident(Incident::of(IncidentKind::Warning, "server", &diagnostic));
        let recorded: i64 = db
            .connection_arc()
            .lock()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM incident WHERE message LIKE '%server-side incident%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(recorded, 1, "the incident was not recorded host-side");
        let (mut handle, _) = SessionProfile::Server.open().expect("server handle");
        let mut session = handle.session().expect("session");
        assert!(run(&mut *session, "repl::errors.incident(*)").is_err());
        assert!(run(
            &mut *session,
            "mount!(\"delightql-repl://session\", \"leak\")(*)"
        )
        .is_err());
    }

    /// Bindings are immutable for the life of a handle — rebinding
    /// refuses, even to the same bytes, so a locator's referent can never
    /// change underneath a mounted namespace.
    #[test]
    fn byte_bindings_are_immutable() {
        let mut handle = open_handle(SessionProfile::client()).unwrap();
        let err = handle
            .bind_static_bytes("book", crate::embedded_db::BOOK_BYTES)
            .expect_err("rebinding 'book' must refuse");
        assert!(err.contains("already exists"), "got: {err}");
        // And names outside the grammar refuse before touching the table.
        let err = handle
            .bind_static_bytes("Not-Valid", b"")
            .expect_err("uppercase name must refuse");
        assert!(err.contains("invalid byte-binding name"), "got: {err}");
    }

    /// Invalid images refuse AT BIND, in a scratch connection — never on
    /// the session connection.
    /// (sqlite3_deserialize installs a buffer without validating it; a
    /// garbage image poisons the hosting connection, including the DETACH
    /// that would remove it. Bind-time validation makes that state
    /// unrepresentable.) And refusal-class mount failures leave nothing
    /// behind: the target namespace stays cleanly mountable.
    #[test]
    fn failed_mount_leaves_nothing_behind() {
        let mut handle = open_handle(SessionProfile::client()).unwrap();

        // Raw garbage: refused by the header check.
        let err = handle
            .bind_static_bytes("junk", b"this is not a sqlite database image")
            .expect_err("garbage must refuse at bind");
        assert!(
            err.contains("not a valid SQLite database image"),
            "got: {err}"
        );

        // Header-prefixed garbage: refused by the scratch-connection probe.
        static CRAFTED: [u8; 512] = {
            let mut b = [0x5au8; 512];
            let magic = *b"SQLite format 3\0";
            let mut i = 0;
            while i < 16 {
                b[i] = magic[i];
                i += 1;
            }
            b
        };
        let err = handle
            .bind_static_bytes("crafted", &CRAFTED)
            .expect_err("header-prefixed garbage must refuse at bind");
        assert!(
            err.contains("not a valid SQLite database image"),
            "got: {err}"
        );

        // Refusal-class mount failures (unbound name) leave the namespace
        // cleanly mountable afterwards.
        let mut session = handle.session().unwrap();
        crate::exec_ng::query(
            &mut *session,
            "mount!(\"delightql-bytes://junk\", \"spot\")(*)",
        )
        .err()
        .expect("unbound name must refuse");
        crate::exec_ng::query(
            &mut *session,
            "mount!(\"delightql-bytes://man\", \"spot\")(*)",
        )
        .expect("a refused mount must not leave metadata that blocks the namespace");
    }

    /// Owned bindings (bind_owned_bytes) share the whole contract: bind-time
    /// validation, locator mounting — and an EMPTY bytes image reaches the
    /// deliberate immutable-image refresh refusal rather than "no
    /// cartridge".
    #[test]
    fn owned_empty_image_refresh_refuses_as_immutable() {
        // A valid, empty, runtime-built image.
        let image = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t(x); DROP TABLE t;")
                .unwrap();
            conn.serialize("main").unwrap().to_vec()
        };
        let mut handle = open_handle(SessionProfile::client()).unwrap();
        handle.bind_owned_bytes("emptyimg", image).unwrap();
        let mut session = handle.session().unwrap();
        crate::exec_ng::query(
            &mut *session,
            "mount!(\"delightql-bytes://emptyimg\", \"emptyns\")(*)",
        )
        .expect("empty owned image must mount");
        let err = crate::exec_ng::query(&mut *session, "refresh!(\"emptyns\")(*)")
            .err()
            .expect("refresh of a bytes image must refuse");
        assert!(
            err.message.contains("immutable"),
            "empty bytes image must reach the immutable refusal, got: {err}"
        );
    }

    /// Refresh-to-empty transitions are LEGAL: each namespace's cartridge
    /// is a stored link, so multiple same-source empty mounts stay
    /// distinguishable. Both mounts refresh into empties, both unmount
    /// cleanly, the source mounts again — no leaked alias.
    #[test]
    fn refresh_to_empty_transition_keeps_lifecycle_sound() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("shrink.sqlite");
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("CREATE TABLE t(x);")
            .unwrap();
        let db_s = db.to_string_lossy().to_string();

        let mut handle = open_handle(SessionProfile::client()).unwrap();
        let mut session = handle.session().unwrap();
        crate::exec_ng::query(&mut *session, &format!("mount!(\"{db_s}\", \"a\")(*)"))
            .expect("non-empty mount a");
        crate::exec_ng::query(&mut *session, &format!("mount!(\"{db_s}\", \"b\")(*)"))
            .expect("non-empty same-source mount b is allowed");

        // The source loses its table out from under both mounts.
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("DROP TABLE t;")
            .unwrap();

        crate::exec_ng::query(&mut *session, "refresh!(\"a\")(*)").expect("first refresh-to-empty");
        crate::exec_ng::query(&mut *session, "refresh!(\"b\")(*)")
            .expect("second refresh-to-empty is legal under the stored link");

        // Lifecycle stays sound afterwards.
        crate::exec_ng::query(&mut *session, "unmount!(\"a\")(*)").expect("unmount a");
        crate::exec_ng::query(&mut *session, "unmount!(\"b\")(*)").expect("unmount b");
        crate::exec_ng::query(&mut *session, &format!("mount!(\"{db_s}\", \"c\")(*)"))
            .expect("no leaked alias: the source mounts again");
    }

    /// imprint! must target the REQUESTED namespace's mount, not the
    /// newest same-source cartridge. One owned image mounted twice (same
    /// locator → same source_path → two independent deserialized copies):
    /// imprinting into `ia` must land the table in ia's image and leave
    /// ib's untouched — an ORDER BY c.id DESC source-match fallback would
    /// route the imprint into `ib` instead.
    #[test]
    fn imprint_targets_the_requested_mount_not_the_newest() {
        let image = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE seed(x); DROP TABLE seed;")
                .unwrap();
            conn.serialize("main").unwrap().to_vec()
        };
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("lib.dql");
        std::fs::write(
            &lib,
            "t(*) :- _(x @ 1)\n\
             (~~ddl:\"_internal\"\n\
             imprinting(entity, materialization, extent) :-\n\
               _(entity, materialization, extent\n\
                 ---------------------------------\n\
                 \"t\", \"table\", \"permanent\")\n\
             ~~)\n",
        )
        .unwrap();
        let lib_path = lib.to_string_lossy().to_string();

        let mut handle = open_handle(SessionProfile::client()).unwrap();
        handle.bind_owned_bytes("imprintimg", image).unwrap();
        let mut session = handle.session().unwrap();
        crate::exec_ng::query(
            &mut *session,
            "mount!(\"delightql-bytes://imprintimg\", \"ia\")(*)",
        )
        .expect("mount ia");
        crate::exec_ng::query(
            &mut *session,
            "mount!(\"delightql-bytes://imprintimg\", \"ib\")(*)",
        )
        .expect("second same-source mount ib is legal under the link");
        crate::exec_ng::query(
            &mut *session,
            &format!("consult!(\"{lib_path}\", \"lib::imp\")(*)"),
        )
        .expect("consult");
        crate::exec_ng::query(&mut *session, "imprint!(\"lib::imp\", \"ia\")(*)")
            .expect("imprint into ia");

        crate::exec_ng::query(&mut *session, "ia.t(*)")
            .expect("the imprinted table must live in ia's image");
        crate::exec_ng::query(&mut *session, "ib.t(*)")
            .err()
            .expect("ib's image must be untouched");
    }

    /// Rows of an in-process query, decoded as text.
    fn rows_of(session: &mut dyn delightql_core::api::DqlSession, text: &str) -> Vec<Vec<String>> {
        let result =
            crate::exec_ng::query(&mut *session, text).unwrap_or_else(|e| panic!("{text}: {e}"));
        let fetched = session.fetch(&result.handle, 1_000).expect("fetch");
        session.close(result.handle).expect("close");
        fetched
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|cell| {
                        cell.map(|b| String::from_utf8_lossy(&b).into_owned())
                            .unwrap_or_default()
                    })
                    .collect()
            })
            .collect()
    }

    /// Blueprint inertness — the CATALOG half. A refused re-imprint of the
    /// archive must leave the session's catalog exactly as the lawful imprint
    /// left it: the archive still at `main::_0_blueprint`, still visible
    /// through the catalog functor, no blueprint minted under the
    /// fresh target, and nothing resolvable there. The data half (the files)
    /// is pinned by tests/blueprint_inertness.rs; the ball runner cannot
    /// observe either, because a sequence ends at its first error.
    ///
    /// RED-BEFORE: the archive materialized into `second` and was moved to
    /// `second::_0_blueprint`, vacating `main::_0_blueprint`.
    #[test]
    fn refused_reimprint_leaves_the_catalog_as_the_lawful_imprint_left_it() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("items.dql");
        std::fs::write(
            &lib,
            "items(*) :- _(x @ 1;2;3)\n\
             (~~ddl:\"_internal\"\n\
             imprinting(*) :- _(entity,materialization,extent @ \"items\",\"table\",\"permanent\")\n\
             ~~)\n",
        )
        .unwrap();
        let lib_path = lib.to_string_lossy().to_string();
        let main_db = dir.path().join("main.sqlite").to_string_lossy().to_string();
        let second_db = dir
            .path()
            .join("second.sqlite")
            .to_string_lossy()
            .to_string();

        let mut handle = open_handle(SessionProfile::client()).unwrap();
        let mut session = handle.session().unwrap();
        crate::exec_ng::query(
            &mut *session,
            &format!("mount_new!(\"{main_db}\", \"main\")(*)"),
        )
        .expect("main");
        crate::exec_ng::query(
            &mut *session,
            &format!("consult!(\"{lib_path}\", \"blue\")(*)"),
        )
        .expect("consult");
        crate::exec_ng::query(&mut *session, "imprint!(\"blue\", \"main\")(*)")
            .expect("lawful imprint");
        crate::exec_ng::query(
            &mut *session,
            &format!("mount_new!(\"{second_db}\", \"second\")(*)"),
        )
        .expect("fresh target");

        let census = |session: &mut dyn delightql_core::api::DqlSession| -> Vec<Vec<String>> {
            let mut rows: Vec<Vec<String>> =
                rows_of(session, "sys::ns.namespace(*) |> (fq_name, kind)")
                    .into_iter()
                    .filter(|row| row[0].contains("blueprint"))
                    .collect();
            rows.sort();
            rows
        };
        let before = census(&mut *session);
        assert_eq!(
            before,
            vec![
                vec!["main::_0_blueprint".to_string(), "blueprint".to_string()],
                vec![
                    "main::_0_blueprint::_internal".to_string(),
                    "scratch".to_string()
                ],
            ],
            "the lawful imprint archives the source under main"
        );

        let err = crate::exec_ng::query(
            &mut *session,
            "imprint!(\"main::_0_blueprint\", \"second\")(*)",
        )
        .err()
        .expect("re-imprinting the archive must refuse");
        assert_eq!(
            err.identity.as_deref(),
            Some("delightql-error://imprint/blueprint/inert"),
            "the refusal carries the inertness badge: {err}"
        );

        assert_eq!(
            census(&mut *session),
            before,
            "the catalog is unchanged by the refusal"
        );
        assert_eq!(
            rows_of(&mut *session, "main::_0_blueprint::(*) |> (name)"),
            vec![vec!["main::_0_blueprint".to_string()]],
            "the archive is still visible at its path through the catalog functor"
        );
        crate::exec_ng::query(&mut *session, "second.items(*)")
            .err()
            .expect("nothing resolves in the fresh target");
        assert_eq!(
            rows_of(&mut *session, "main.items(*)"),
            vec![
                vec!["1".to_string()],
                vec!["2".to_string()],
                vec!["3".to_string()]
            ],
            "the lawful materialization still answers"
        );
    }

    /// An imprint archive's lifecycle, in one session. `reconsult!` of the
    /// archive, or of a namespace inside it, refuses as inert and changes
    /// nothing. `unconsult!` removes the archive by its exact name once the
    /// manifest namespace beneath it has gone, and refuses and names that
    /// child before then. None of it takes the catalog down: the same session
    /// reads the catalog and the imprinted table afterward.
    ///
    /// RED-BEFORE: both verbs panicked on the archive's kind while holding
    /// the catalog lock, and `reconsult!` of the manifest namespace inside
    /// the archive reloaded it from the replacement file.
    #[test]
    fn archive_removal_and_refused_reconsult_leave_the_session_whole() {
        let dir = tempfile::tempdir().unwrap();
        let lib = dir.path().join("items.dql");
        std::fs::write(
            &lib,
            "items(*) :- _(x @ 1;2;3)\n\
             (~~ddl:\"_internal\"\n\
             imprinting(*) :- _(entity,materialization,extent @ \"items\",\"table\",\"permanent\")\n\
             ~~)\n",
        )
        .unwrap();
        let replacement = dir.path().join("replacement.dql");
        std::fs::write(&replacement, "fresh(*) :- _(y @ 42)\n").unwrap();
        let lib_path = lib.to_string_lossy().to_string();
        let replacement_path = replacement.to_string_lossy().to_string();
        let main_db = dir.path().join("main.sqlite").to_string_lossy().to_string();

        let mut handle = open_handle(SessionProfile::client()).unwrap();
        let mut session = handle.session().unwrap();
        crate::exec_ng::query(
            &mut *session,
            &format!("mount_new!(\"{main_db}\", \"main\")(*)"),
        )
        .expect("main");
        crate::exec_ng::query(
            &mut *session,
            &format!("consult!(\"{lib_path}\", \"blue\")(*)"),
        )
        .expect("consult");
        crate::exec_ng::query(&mut *session, "imprint!(\"blue\", \"main\")(*)")
            .expect("lawful imprint");

        // The archive's namespaces with the definitions active in each.
        let archive = |session: &mut dyn delightql_core::api::DqlSession| -> Vec<Vec<String>> {
            let mut rows = rows_of(
                session,
                "sys::ns.namespace(nid, _, _, fq_name, _, kind, _, source_path, _), \
                 sys::ns.activated_entity(eid, _, nid, _), \
                 sys::entities.entity(eid, name, _, _, _, _) \
                 |> (fq_name, kind, source_path, name)",
            );
            rows.retain(|row| row[0].starts_with("main::_0_blueprint"));
            rows.sort();
            rows
        };
        let before = archive(&mut *session);
        assert_eq!(
            before
                .iter()
                .map(|row| (row[0].as_str(), row[3].as_str()))
                .collect::<Vec<_>>(),
            [
                ("main::_0_blueprint", "items"),
                ("main::_0_blueprint::_internal", "imprinting"),
            ],
            "the lawful imprint archives the source and its manifest namespace"
        );

        for target in ["main::_0_blueprint", "main::_0_blueprint::_internal"] {
            let err = crate::exec_ng::query(
                &mut *session,
                &format!("reconsult!(\"{target}\", \"{replacement_path}\")(*)"),
            )
            .err()
            .unwrap_or_else(|| panic!("reconsult! of '{target}' must refuse"));
            assert_eq!(
                err.identity.as_deref(),
                Some("delightql-error://imprint/blueprint/inert"),
                "'{target}': {err}"
            );
        }
        assert_eq!(
            archive(&mut *session),
            before,
            "a refused reconsult! changes nothing"
        );

        let err = crate::exec_ng::query(&mut *session, "unconsult!(\"main::_0_blueprint\")(*)")
            .err()
            .expect("the archive's manifest namespace stands beneath it");
        assert!(
            err.to_string()
                .contains("'main::_0_blueprint::_internal' stands beneath it"),
            "{err}"
        );
        assert_eq!(
            archive(&mut *session),
            before,
            "the refusal removed nothing"
        );

        crate::exec_ng::query(
            &mut *session,
            "unconsult!(\"main::_0_blueprint::_internal\")(*)",
        )
        .expect("the manifest namespace is removed by its exact name");
        crate::exec_ng::query(&mut *session, "unconsult!(\"main::_0_blueprint\")(*)")
            .expect("the archive is removed by its exact name");
        assert!(archive(&mut *session).is_empty(), "the archive is gone");
        assert_eq!(
            rows_of(
                &mut *session,
                "sys::ns.namespace(*), fq_name = \"main::_0_blueprint\" |> (fq_name)"
            ),
            Vec::<Vec<String>>::new(),
            "no namespace row survives at the archive's path"
        );
        assert_eq!(
            rows_of(&mut *session, "main.items(*)"),
            vec![
                vec!["1".to_string()],
                vec!["2".to_string()],
                vec!["3".to_string()]
            ],
            "the imprinted table still answers"
        );
    }

    #[test]
    fn postgres_urls_route_to_the_fatboy() {
        let r = classify("postgres://alice@db.example:5432/prod", None).unwrap();
        assert_eq!(
            r,
            Route::PostgresFatboy {
                url: "postgres://alice@db.example:5432/prod".into(),
                display_db: "prod".into()
            }
        );
        // libpq's env-completed form
        let r = classify("postgres:///dql_core", None).unwrap();
        assert_eq!(
            r,
            Route::PostgresFatboy {
                url: "postgres:///dql_core".into(),
                display_db: "dql_core".into()
            }
        );
        // postgresql:// spelling too
        assert!(matches!(
            classify("postgresql://h/d", None).unwrap(),
            Route::PostgresFatboy { .. }
        ));
        // --via siso reroutes to the pipe coprocess
        assert_eq!(
            classify("postgres:///dql_core", Some("siso")).unwrap(),
            Route::Siso {
                rest: "postgres/dql_core".into()
            }
        );
    }

    #[test]
    fn secrets_never_enter_connection_urls() {
        let err = classify("postgres://alice:hunter2@h/d", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("passwords are never accepted"), "{err}");
        assert!(err.contains("PGPASSWORD"), "{err}");
    }

    #[test]
    fn file_urls_and_paths_classify_by_content() {
        assert_eq!(
            classify("some/dir/data.db", None).unwrap(),
            Route::Sqlite("some/dir/data.db".into())
        );
        assert_eq!(
            classify("weird:name.db", None).unwrap(),
            Route::Sqlite("weird:name.db".into())
        );
        // duckdb by extension routes to the fatboy — no teaching error
        assert_eq!(
            classify("analytics.duckdb", None).unwrap(),
            Route::DuckdbFatboy("analytics.duckdb".into())
        );
        assert_eq!(
            classify("file:///data/x.duckdb", None).unwrap(),
            Route::DuckdbFatboy("/data/x.duckdb".into())
        );
        let err = classify("file://remotehost/x.db", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("remote host"), "{err}");
    }

    #[test]
    fn unknown_schemes_teach_the_supported_set() {
        for input in ["mysql://host/db", "oracle://host/db", "delightql://x"] {
            let err = classify(input, None).unwrap_err().to_string();
            let scheme = &input[..input.find("://").unwrap()];
            assert!(
                err.contains(&format!("unsupported URI scheme '{scheme}://'")),
                "{input}: {err}"
            );
            assert!(err.contains("Known: postgres://"), "{input}: {err}");
        }
    }

    #[test]
    fn siso_scheme_routes() {
        assert_eq!(
            classify("delightql-siso://osqueryi", None).unwrap(),
            Route::Siso {
                rest: "osqueryi".into()
            }
        );
        assert_eq!(
            classify("delightql-siso://postgres/dql_core", None).unwrap(),
            Route::Siso {
                rest: "postgres/dql_core".into()
            }
        );
    }
}
