// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Multi-database connection wrapper
///
/// Provides a unified interface for SQLite, pipe-based, and fatboy connections
use anyhow::Result;
use delightql_backends::SqliteConnectionManager;
use delightql_types::DatabaseConnection;
use std::sync::{Arc, Mutex};

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
/// consulted here (it was rejected as fake conninfo, §4.9).
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
/// scheme-dispatch point (URI-DESIGN.md §4: users speak in resources,
/// DelightQL chooses mechanisms). An unknown scheme teaches; it can never
/// fall through to file-path handling.
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
pub fn classify(input: &str, via: Option<&str>) -> Result<Route> {
    if let Some(v) = via {
        if !matches!(v, "fatboy" | "siso") {
            anyhow::bail!("unknown --via '{v}' (known mechanisms: fatboy, siso)");
        }
    }

    if let Some(rest) = input.strip_prefix("delightql-siso://") {
        if rest.is_empty() {
            anyhow::bail!("delightql-siso:// needs a profile: delightql-siso://<profile>[/<target>]");
        }
        return Ok(Route::Siso { rest: rest.to_string() });
    }

    if looks_like_uri(input) {
        let scheme_end = input.find("://").expect("looks_like_uri checked");
        let scheme = input[..scheme_end].to_ascii_lowercase();
        return match scheme.as_str() {
            "postgres" | "postgresql" => {
                let url = url::Url::parse(input).map_err(|e| {
                    anyhow::anyhow!("'{input}': not a valid postgres URL: {e}")
                })?;
                if url.password().is_some() {
                    anyhow::bail!(
                        "'{input}': passwords are never accepted in connection URLs \
                         (they would persist into session metadata). Set PGPASSWORD \
                         in the environment instead."
                    );
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
                    .map_err(|e| anyhow::anyhow!("'{input}': not a valid file URL: {e}"))?;
                match url.host_str() {
                    None | Some("") | Some("localhost") => {}
                    Some(h) => anyhow::bail!(
                        "'{input}': file URLs with a remote host ('{h}') are not \
                         supported — file:///absolute/path only."
                    ),
                }
                classify_file_path(url.path(), via)
            }
            "fatboy" => anyhow::bail!(
                "'{input}': fatboy:// is retired. Name the resource instead: \
                 postgres:///<dbname> (or postgres://host:port/db) for Postgres, \
                 or the file path for DuckDB — the right adapter is chosen \
                 automatically."
            ),
            "pipe" => anyhow::bail!(
                "'{input}': pipe:// is now delightql-siso:// (same \
                 profile/target syntax)."
            ),
            other => anyhow::bail!(
                "'{input}': unsupported URI scheme '{other}://'. Known: \
                 postgres://, file://, delightql-siso://, or a plain file path."
            ),
        };
    }

    classify_file_path(input, via)
}

/// Classify a filesystem path by magic bytes / extension.
fn classify_file_path(path: &str, via: Option<&str>) -> Result<Route> {
    use std::io::Read;

    if let Some(v) = via {
        if v != "fatboy" {
            anyhow::bail!("--via {v} does not apply to file-backed databases");
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
    /// engine behind it (`fatboy://postgres/<db>`).
    Fatboy(Arc<crate::fatboy_exec::FatboyManager>),
}

impl ConnectionManager {
    /// Open a classified route (see [`classify`]).
    pub fn open_route(route: Route) -> Result<Self> {
        match route {
            Route::Sqlite(path) => Ok(ConnectionManager::SQLite(
                SqliteConnectionManager::new_file(&path)?,
            )),
            Route::DuckdbFatboy(path) => {
                let mgr = crate::fatboy_exec::FatboyManager::connect("duckdb", &path)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ConnectionManager::Fatboy(Arc::new(mgr)))
            }
            Route::PostgresFatboy { url, display_db } => {
                let mgr =
                    crate::fatboy_exec::FatboyManager::connect_postgres_url(&url, &display_db)
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ConnectionManager::Fatboy(Arc::new(mgr)))
            }
            Route::Siso { rest } => {
                let mgr = delightql_cli_siso::PipeConnectionManager::from_uri(&format!(
                    "delightql-siso://{rest}"
                ))
                .map_err(|e| anyhow::anyhow!("{}", e))?;
                Ok(ConnectionManager::Pipe(Arc::new(mgr)))
            }
        }
    }

    /// Open from a `--db` / mount input string with a mechanism override.
    pub fn open(input: &str, via: Option<&str>) -> Result<Self> {
        Self::open_route(classify(input, via)?)
    }

    /// Create a new connection from a resource string (path or worldly
    /// URI), default mechanisms. Kept as the factory-facing entry point.
    pub fn new_file(path: &str) -> Result<Self> {
        Self::open(path, None)
    }

    /// Create a new in-memory connection (defaults to SQLite)
    pub fn new_memory() -> Result<Self> {
        Ok(ConnectionManager::SQLite(
            SqliteConnectionManager::new_memory()?,
        ))
    }

    /// Test the connection
    #[allow(dead_code)]
    pub fn test_connection(&self) -> Result<()> {
        match self {
            ConnectionManager::SQLite(conn) => Ok(conn.test_connection()?),
            ConnectionManager::Pipe(mgr) => {
                let _conn = mgr.connect().map_err(|e| anyhow::anyhow!("{}", e))?;
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

    /// Get connection Arc (for SQLite - backward compatibility)
    /// TODO: Remove this once all code uses database-agnostic APIs
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
    /// The `db_label` is used for error messages in SQLite; Pipe ignores it.
    pub fn execute_query(
        &self,
        sql: &str,
        db_label: &str,
    ) -> Result<delightql_backends::QueryResults> {
        match self {
            ConnectionManager::SQLite(conn) => delightql_backends::execute_sql_with_connection(
                sql.to_string(),
                conn,
                std::path::Path::new(db_label),
            )
            .map_err(|e| anyhow::anyhow!("{}", e)),
            ConnectionManager::Pipe(mgr) => crate::pipe_exec::execute_sql_with_pipe(sql, mgr)
                .map_err(|e| anyhow::anyhow!("{}", e)),
            ConnectionManager::Fatboy(mgr) => {
                crate::fatboy_exec::execute_sql_with_fatboy(sql, mgr)
                    .map_err(|e| anyhow::anyhow!("{}", e))
            }
        }
    }

    /// Execute a SQL query with NULL fidelity preserved.
    ///
    /// Returns rows as `Vec<Vec<Option<String>>>` where `None` = SQL NULL.
    /// Used by the relay adapter to produce honest `Cell = Option<ByteSeq>`.
    pub fn execute_query_typed(
        &self,
        sql: &str,
        db_label: &str,
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        match self {
            ConnectionManager::SQLite(conn) => {
                let mut executor = delightql_backends::SqliteExecutorImpl::new(conn);
                let typed = executor
                    .execute_query_typed(sql)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                let rows = typed
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|val| match val {
                                delightql_backends::SqlValue::Null => None,
                                other => Some(other.to_display_string()),
                            })
                            .collect()
                    })
                    .collect();
                Ok((typed.columns, rows))
            }
            ConnectionManager::Pipe(_) => {
                let results = self.execute_query(sql, db_label)?;
                let rows = results
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(Some).collect())
                    .collect();
                Ok((results.columns, rows))
            }
            // NULL fidelity is native here: relay Cells are Option already.
            ConnectionManager::Fatboy(mgr) => mgr
                .relay()
                .and_then(|r| r.query_nullable(sql))
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
    ) -> Result<delightql_types::ConnectionComponents> {
        match self {
            ConnectionManager::SQLite(sqlite_conn) => {
                if mounted_schema.is_some() {
                    anyhow::bail!(
                        "SQLite has no schemas; a #schema fragment is only meaningful \
                         on a Postgres or DuckDB target (use mount! without a fragment)"
                    );
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

/// Open a DqlHandle using the factory-only API.
///
/// Returns `Box<dyn DqlHandle>` — the compiler-enforced API boundary.
/// The handle starts with an empty "main" namespace. The CLI must send
/// `mount!("path", "main")` to populate it.
///
/// This is a FREE function, not a method on `ConnectionManager`, because it
/// builds the session purely from the factories and never consulted a
/// manager's own backend — the session's one backend is created by the
/// `mount!` first-query, not by any pre-opened `ConnectionManager`. Keeping
/// it self-less makes that separation explicit (it used to be a `&self`
/// method that ignored `self`, which read as if a manager fed the handle;
/// bugs/duplicate-fatboy-spawn-one-shot).
pub fn open_handle() -> Result<Box<dyn delightql_core::api::DqlHandle>> {
    let factory = Box::new(crate::connection_factory::CliConnectionFactory);
    // Second factory (types-level) powers mount!/import! of URI-scheme
    // databases (pipe://, etc.). Same unit struct, both trait impls.
    let mount_factory = Box::new(crate::connection_factory::CliConnectionFactory);
    let mut handle = delightql_core::api::open(factory, Some(mount_factory))
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    // Bind the CLI's embedded database images (BYTES-SCHEME-DESIGN.md).
    // A binding is a name→bytes map entry, not a mount: no attachment, no
    // I/O, no cost until a session actually runs
    // `mount!("delightql-bytes://book", ...)`. Binding on every handle is
    // what makes the locators typeable from any session, REPL included.
    handle
        .bind_static_bytes("book", crate::embedded_db::BOOK_BYTES)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    handle
        .bind_static_bytes("man", crate::embedded_db::MAN_BYTES)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    crate::cli_surface::attach(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// BYTES-SCHEME-DESIGN.md: bindings are immutable for the life of a
    /// handle — rebinding refuses, even to the same bytes, so a locator's
    /// referent can never change underneath a mounted namespace.
    #[test]
    fn byte_bindings_are_immutable() {
        let mut handle = open_handle().unwrap();
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

    /// MOUNT-SPINE-PLAN.md Phase 1 (review R2): invalid images refuse AT
    /// BIND, in a scratch connection — never on the session connection.
    /// (sqlite3_deserialize installs a buffer without validating it; a
    /// garbage image poisons the hosting connection, including the DETACH
    /// that would remove it. Bind-time validation makes that state
    /// unrepresentable.) And refusal-class mount failures leave nothing
    /// behind: the target namespace stays cleanly mountable.
    #[test]
    fn failed_mount_leaves_nothing_behind() {
        let mut handle = open_handle().unwrap();

        // Raw garbage: refused by the header check.
        let err = handle
            .bind_static_bytes("junk", b"this is not a sqlite database image")
            .expect_err("garbage must refuse at bind");
        assert!(err.contains("not a valid SQLite database image"), "got: {err}");

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
        assert!(err.contains("not a valid SQLite database image"), "got: {err}");

        // Refusal-class mount failures (unbound name) leave the namespace
        // cleanly mountable afterwards.
        let mut session = handle.session().unwrap();
        session
            .query("mount!(\"delightql-bytes://junk\", \"spot\")")
            .err()
            .expect("unbound name must refuse");
        session
            .query("mount!(\"delightql-bytes://man\", \"spot\")")
            .expect("a refused mount must not leave metadata that blocks the namespace");
    }

    /// Owned bindings (bind_owned_bytes) share the whole contract: bind-time
    /// validation, locator mounting — and an EMPTY bytes image reaches the
    /// deliberate immutable-image refresh refusal (second review F2) rather
    /// than "no cartridge".
    #[test]
    fn owned_empty_image_refresh_refuses_as_immutable() {
        // A valid, empty, runtime-built image.
        let image = {
            let conn = rusqlite::Connection::open_in_memory().unwrap();
            conn.execute_batch("CREATE TABLE t(x); DROP TABLE t;").unwrap();
            conn.serialize("main").unwrap().to_vec()
        };
        let mut handle = open_handle().unwrap();
        handle.bind_owned_bytes("emptyimg", image).unwrap();
        let mut session = handle.session().unwrap();
        session
            .query("mount!(\"delightql-bytes://emptyimg\", \"emptyns\")")
            .expect("empty owned image must mount");
        let err = session
            .query("refresh!(\"emptyns\")")
            .err()
            .expect("refresh of a bytes image must refuse");
        assert!(
            err.contains("immutable"),
            "empty bytes image must reach the immutable refusal, got: {err}"
        );
    }

    /// Refresh-to-empty transitions are LEGAL
    /// (NAMESPACE-CARTRIDGE-LINK-DESIGN.md: each namespace's cartridge is
    /// a stored link, so multiple same-source empty mounts stay
    /// distinguishable — the interim refusal is repealed). Both mounts
    /// refresh into empties, both unmount cleanly, the source mounts
    /// again — no leaked alias.
    #[test]
    fn refresh_to_empty_transition_keeps_lifecycle_sound() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("shrink.sqlite");
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("CREATE TABLE t(x);")
            .unwrap();
        let db_s = db.to_string_lossy().to_string();

        let mut handle = open_handle().unwrap();
        let mut session = handle.session().unwrap();
        session
            .query(&format!("mount!(\"{db_s}\", \"a\")"))
            .expect("non-empty mount a");
        session
            .query(&format!("mount!(\"{db_s}\", \"b\")"))
            .expect("non-empty same-source mount b is allowed");

        // The source loses its table out from under both mounts.
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute_batch("DROP TABLE t;")
            .unwrap();

        session
            .query("refresh!(\"a\")")
            .expect("first refresh-to-empty");
        session
            .query("refresh!(\"b\")")
            .expect("second refresh-to-empty is legal under the stored link");

        // Lifecycle stays sound afterwards.
        session.query("unmount!(\"a\")").expect("unmount a");
        session.query("unmount!(\"b\")").expect("unmount b");
        session
            .query(&format!("mount!(\"{db_s}\", \"c\")"))
            .expect("no leaked alias: the source mounts again");
    }

    /// Fourth review, P1: imprint! must target the REQUESTED namespace's
    /// mount, not the newest same-source cartridge. One owned image mounted
    /// twice (same locator → same source_path → two independent deserialized
    /// copies): imprinting into `ia` must land the table in ia's image and
    /// leave ib's untouched. Pre-fix, the ORDER BY c.id DESC source-match
    /// fallback routed the imprint into `ib`.
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

        let mut handle = open_handle().unwrap();
        handle.bind_owned_bytes("imprintimg", image).unwrap();
        let mut session = handle.session().unwrap();
        session
            .query("mount!(\"delightql-bytes://imprintimg\", \"ia\")")
            .expect("mount ia");
        session
            .query("mount!(\"delightql-bytes://imprintimg\", \"ib\")")
            .expect("second same-source mount ib is legal under the link");
        session
            .query(&format!("consult!(\"{lib_path}\", \"lib::imp\")"))
            .expect("consult");
        session
            .query("imprint!(\"lib::imp\", \"ia\")")
            .expect("imprint into ia");

        session
            .query("ia.t(*)")
            .expect("the imprinted table must live in ia's image");
        session
            .query("ib.t(*)")
            .err()
            .expect("ib's image must be untouched");
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
            Route::Siso { rest: "postgres/dql_core".into() }
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
        let err = classify("file://remotehost/x.db", None).unwrap_err().to_string();
        assert!(err.contains("remote host"), "{err}");
    }

    #[test]
    fn retired_and_unknown_schemes_teach() {
        let err = classify("fatboy://postgres/db", None).unwrap_err().to_string();
        assert!(err.contains("retired"), "{err}");
        assert!(err.contains("postgres:///"), "{err}");
        let err = classify("pipe://psql", None).unwrap_err().to_string();
        assert!(err.contains("delightql-siso://"), "{err}");
        let err = classify("mysql://host/db", None).unwrap_err().to_string();
        assert!(err.contains("unsupported URI scheme 'mysql://'"), "{err}");
    }

    #[test]
    fn siso_scheme_routes() {
        assert_eq!(
            classify("delightql-siso://osqueryi", None).unwrap(),
            Route::Siso { rest: "osqueryi".into() }
        );
        assert_eq!(
            classify("delightql-siso://postgres/dql_core", None).unwrap(),
            Route::Siso { rest: "postgres/dql_core".into() }
        );
    }
}
