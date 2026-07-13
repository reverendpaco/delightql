// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DuckParty — the DuckDB fatboy's protocol handler.
//!
//! Pattern replication of `PgParty` (delightql-postgres) — the second
//! fatboy, proving `dql-fatboy-<target>` is a pattern and not a
//! one-off. Differences from postgres, all deliberate:
//!
//! - **Rendering parity with the in-process path**: values render
//!   through the SAME `delightql-backends` DuckDB executor that native
//!   `--db file.duckdb` uses, so fatboy hashes equal in-proc hashes by
//!   construction (no NULL-fidelity distinction either — exactly like
//!   in-proc; the fingerprint conflates NULL and '' anyway).
//! - **No server**: DuckDB is a file database. `connect` is fail-closed
//!   (a DuckParty whose database doesn't exist never exists) but LAZY:
//!   the file is not opened until the first Query. dql spawns two
//!   children per session on the same file (the idle spare from
//!   make_connection plus the mount's worker — REPORT-T-P2 §C), and a
//!   writable open takes DuckDB's exclusive lock (P3 §C: it excludes
//!   ALL other opens, even read-only) — an eager open would deadlock
//!   the pair. Lazy, only the party that actually speaks takes the
//!   lock; pinned by `connect_does_not_take_the_file_lock`.
//! - **Writable by default** (E-T3b, RULED 2026-07-11: DuckDB goes
//!   writable; EFFECTS-ON-TARGETS-PLAN §1 finding 2). `connect_readonly`
//!   preserves the old posture — concurrent read-only opens across
//!   processes, every write refused by the engine; the binary's
//!   `--readonly` flag reaches it (pinned by tests/readonly_flag.rs).
//! - **Descriptors are empty** (v1): the backends executor abstracts
//!   the statement away. Foreign descriptors were postgres's question
//!   to answer; here they're a noted gap, not a goal.
//! - **No load path** (v1): Prepare/Offer answer unimplemented, like
//!   SqlParty. DuckDB bulk loading wants the Appender API; that's a
//!   later slice if a workload asks.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use delightql_backends::duckdb::executor::QueryResult;
use delightql_backends::{DuckDBConnectionManager, DuckDBExecutor, DuckDBExecutorImpl};
use delightql_protocol::{
    resolve_projection, Cell, ClientTerm, Dimension, ErrorKind, Handle, Handler, MetaItem,
    Orientation, Projection, ServerTerm,
};

const IDENT_DUCK_ERROR: &str = "delightql-error://target/duckdb/error";
const IDENT_UNIMPLEMENTED: &str = "delightql-error://target/duckdb/unimplemented";

struct ResultState {
    columns: Vec<String>,
    rows: VecDeque<Vec<Cell>>,
    exec_ms: u64,
}

/// The database behind the party: verified at connect, opened at first
/// use. Lazy because dql runs TWO fatboy children on one file per
/// session (the idle spare + the mount's worker, REPORT-T-P2 §C) and a
/// writable open takes DuckDB's exclusive lock — eager opens would make
/// the spare starve the worker. Pinned by
/// `connect_does_not_take_the_file_lock`.
enum DbState {
    Closed { path: String, readonly: bool },
    Open(DuckDBConnectionManager),
}

pub struct DuckParty {
    db: DbState,
    handles: HashMap<Handle, ResultState>,
    next_handle_id: u64,
}

impl DuckParty {
    /// Connect WRITABLE (the default since the 2026-07-11 ruling),
    /// fail-closed (the file must already exist) and lazy (the file —
    /// and DuckDB's exclusive write lock — is not taken until the
    /// first Query).
    pub fn connect(db_path: &str) -> Result<Self, String> {
        Self::connect_mode(db_path, false)
    }

    /// Connect READ-ONLY: the pre-write-mode posture, kept as an
    /// explicit opt-in. Read-only opens can share one file across
    /// processes (all openers must be read-only — P3 §C); the engine
    /// refuses every write. Pinned by
    /// `connect_readonly_still_refuses_writes`.
    pub fn connect_readonly(db_path: &str) -> Result<Self, String> {
        Self::connect_mode(db_path, true)
    }

    fn connect_mode(db_path: &str, readonly: bool) -> Result<Self, String> {
        // Fail-closed at connect, WITHOUT opening: a DuckParty whose
        // database doesn't exist never exists, but taking the lock
        // waits for the first Query (see DbState).
        if db_path != ":memory:" && !std::path::Path::new(db_path).exists() {
            return Err(format!("Database file '{}' does not exist.", db_path));
        }
        Ok(Self {
            db: DbState::Closed { path: db_path.to_string(), readonly },
            handles: HashMap::new(),
            next_handle_id: 1,
        })
    }

    /// The open connection, opening it (and taking the lock, when
    /// writable) on first use. A lock conflict or late corruption
    /// surfaces here as a Query-time error rather than at connect —
    /// the price of laziness, and the error still travels the protocol.
    fn manager(&mut self) -> Result<&DuckDBConnectionManager, String> {
        if let DbState::Closed { path, readonly } = &self.db {
            let manager = if path == ":memory:" {
                DuckDBConnectionManager::new_memory()
            } else if *readonly {
                DuckDBConnectionManager::new_file_readonly(path)
            } else {
                DuckDBConnectionManager::new_file_existing(path)
            }
            .map_err(|e| e.to_string())?;
            self.db = DbState::Open(manager);
        }
        match &self.db {
            DbState::Open(m) => Ok(m),
            DbState::Closed { .. } => unreachable!("just opened above"),
        }
    }

    fn handle_query(&mut self, text: Vec<u8>) -> ServerTerm {
        let sql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(_) => {
                return error(ErrorKind::Syntax, IDENT_DUCK_ERROR, b"query is not UTF-8".to_vec())
            }
        };

        let started = Instant::now();
        let manager = match self.manager() {
            Ok(m) => m,
            Err(e) => return error(ErrorKind::Connection, IDENT_DUCK_ERROR, e.into_bytes()),
        };
        let mut executor = DuckDBExecutorImpl::new(manager);
        let result = match executor.execute_query(&sql) {
            Ok(r) => r,
            Err(e) => {
                return error(ErrorKind::Syntax, IDENT_DUCK_ERROR, e.to_string().into_bytes())
            }
        };
        let exec_ms = started.elapsed().as_millis() as u64;

        // DML/DDL with no result columns: the spec-appendix relation.
        let (columns, rows) = match shape_result(result) {
            Ok(cr) => cr,
            Err(msg) => return error(ErrorKind::Connection, IDENT_DUCK_ERROR, msg.into_bytes()),
        };

        let handle_id = self.next_handle_id;
        self.next_handle_id += 1;
        let handle: Handle = format!("duck{}", handle_id).into_bytes();

        let dimensions: Vec<Dimension> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| Dimension {
                position: (i + 1) as u64,
                name: name.as_bytes().to_vec(),
                descriptor: Vec::new(), // v1: no foreign descriptors
            })
            .collect();

        let cells: VecDeque<Vec<Cell>> = rows
            .into_iter()
            .map(|row| row.into_iter().map(|v| Some(v.into_bytes())).collect())
            .collect();

        self.handles.insert(
            handle.clone(),
            ResultState { columns, rows: cells, exec_ms },
        );
        ServerTerm::Header { handle, dimensions }
    }

    fn handle_fetch(
        &mut self,
        handle: Handle,
        projection: Projection,
        count: u64,
        orientation: Orientation,
    ) -> ServerTerm {
        if orientation != Orientation::Rows {
            return error(
                ErrorKind::Connection,
                "delightql-error://target/duckdb/orientation",
                b"orientation Columns not supported".to_vec(),
            );
        }
        let state = match self.handles.get_mut(&handle) {
            Some(s) => s,
            None => return unknown_handle(),
        };
        let n = std::cmp::min(count as usize, state.rows.len());
        if n == 0 {
            return ServerTerm::End;
        }
        let drained: Vec<Vec<Cell>> = state.rows.drain(..n).collect();
        let idx = resolve_projection(&projection, &state.columns);
        let cells = drained
            .iter()
            .map(|row| idx.iter().map(|&i| row[i].clone()).collect())
            .collect();
        ServerTerm::Data { cells }
    }

    fn handle_stat(&self, handle: Handle) -> ServerTerm {
        match self.handles.get(&handle) {
            None => unknown_handle(),
            Some(state) => ServerTerm::Metadata {
                items: vec![
                    MetaItem::Backend(b"duckdb".to_vec(), b"duck-fatboy".to_vec()),
                    MetaItem::ExecutionTime(state.exec_ms),
                ],
            },
        }
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        if self.handles.remove(&handle).is_some() {
            ServerTerm::Ok { count_hint: 0 }
        } else {
            unknown_handle()
        }
    }
}

/// Shape an executor result into the relay's (columns, rows) relation.
///
/// A result WITH columns passes through unchanged. A result with NO
/// columns is a DML/DDL outcome and becomes the spec-appendix
/// `affected_rows` relation — from the executor's count. Note that
/// today's DuckDB `execute_query` surfaces DML counts as a `Count`
/// column (so DML takes the pass-through arm) and NEVER populates
/// `affected_rows`; if the no-columns arm is reached without a count,
/// that is a LOUD error — fabricating 0 would let the receipts
/// feature read a YES as a NO and silently skip downstream work.
/// Pinned by `missing_affected_count_is_a_loud_error`.
fn shape_result(result: QueryResult) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    if result.columns.is_empty() {
        match result.affected_rows {
            Some(n) => Ok((vec!["affected_rows".to_string()], vec![vec![n.to_string()]])),
            None => Err(
                "executor returned no result columns and no affected-rows count; \
                 refusing to fabricate affected_rows = 0"
                    .to_string(),
            ),
        }
    } else {
        Ok((result.columns, result.rows))
    }
}

fn error(kind: ErrorKind, identity: &str, message: impl Into<Vec<u8>>) -> ServerTerm {
    ServerTerm::Error {
        kind,
        identity: identity.as_bytes().to_vec(),
        message: message.into(),
    }
}

fn unknown_handle() -> ServerTerm {
    error(ErrorKind::Connection, IDENT_DUCK_ERROR, b"unknown handle".to_vec())
}

impl Handler for DuckParty {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        match term {
            ClientTerm::Version {
                max_message_size,
                protocol_version,
                lease_ms,
                orientations,
            } => {
                let supported = [Orientation::Rows];
                let agreed: Vec<Orientation> = orientations
                    .iter()
                    .copied()
                    .filter(|o| supported.contains(o))
                    .collect();
                if agreed.is_empty() {
                    error(
                        ErrorKind::Connection,
                        "delightql-error://target/duckdb/orientation",
                        b"no common orientation".to_vec(),
                    )
                } else {
                    ServerTerm::Version {
                        max_message_size,
                        protocol_version,
                        lease_ms,
                        orientations: agreed,
                    }
                }
            }

            ClientTerm::Query { text } => self.handle_query(text),
            ClientTerm::Fetch { handle, projection, count, orientation } => {
                self.handle_fetch(handle, projection, count, orientation)
            }
            ClientTerm::Stat { handle } => self.handle_stat(handle),
            ClientTerm::Close { handle } => self.handle_close(handle),

            ClientTerm::Prepare { .. } | ClientTerm::Offer { .. } => error(
                ErrorKind::Permission,
                IDENT_UNIMPLEMENTED,
                b"load path not implemented for the duckdb fatboy (Appender API is a later slice)"
                    .to_vec(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delightql_protocol::{
        Client as RelayClient, DirectTransport, FetchResponse, QueryResponse, VersionResult,
    };

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    /// Self-contained: build a temp duckdb file via the backends
    /// manager, close it, hand it to DuckParty. No container, no
    /// network — this fatboy's tests run anywhere.
    fn temp_db() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "duckparty-test-{}-{}.duckdb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mgr = DuckDBConnectionManager::new_file(path.to_str().unwrap()).unwrap();
            let mut ex = DuckDBExecutorImpl::new(&mgr);
            ex.execute_query("CREATE TABLE t (id INTEGER, name TEXT)").ok();
            ex.execute_query("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')")
                .ok();
        } // manager dropped: single-writer lock released
        path
    }

    #[test]
    fn duckparty_full_query_path() {
        let path = temp_db();
        let party = DuckParty::connect(path.to_str().unwrap()).unwrap();
        let client = RelayClient::new(DirectTransport::new(party));
        let VersionResult::Accepted(mut session) = client
            .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
            .unwrap()
        else {
            panic!("handshake should succeed")
        };
        let rows_o = session.agreed_orientation(Orientation::Rows).unwrap();

        let QueryResponse::Header { handle, dimensions } = session
            .query(b("SELECT name FROM t ORDER BY id"))
            .unwrap()
        else {
            panic!("expected Header")
        };
        assert_eq!(dimensions[0].name, b("name"));

        match session.fetch(&handle, Projection::All, 100, rows_o).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(
                    cells,
                    vec![vec![Some(b("alpha"))], vec![Some(b("beta"))]]
                );
            }
            other => panic!("expected Data, got {:?}", other),
        }
        assert!(matches!(
            session.fetch(&handle, Projection::All, 100, rows_o).unwrap(),
            FetchResponse::End
        ));
        session.close(handle).unwrap();
        let _ = std::fs::remove_file(&path);
    }

    /// Task 1.7: the executor's `affected_rows` is an Option defaulted
    /// None and `execute_query` never populates it. A no-columns result
    /// without a count must be a LOUD error — never a fabricated
    /// `affected_rows = 0`, which the receipts feature would read as a
    /// NO and silently skip downstream work.
    #[test]
    fn missing_affected_count_is_a_loud_error() {
        let missing = QueryResult::new(vec![], vec![]);
        match shape_result(missing) {
            Err(msg) => {
                assert!(
                    msg.contains("affected"),
                    "error should name the missing count, got: {}",
                    msg
                );
            }
            Ok((columns, rows)) => panic!(
                "missing count must not shape into a relation, got columns={:?} rows={:?}",
                columns, rows
            ),
        }
    }

    /// Companion pin: a count the executor DID supply still becomes the
    /// spec-appendix `affected_rows` relation.
    #[test]
    fn populated_affected_count_becomes_the_relation() {
        let populated = QueryResult::new(vec![], vec![]).with_affected_rows(3);
        let (columns, rows) = shape_result(populated).expect("populated count must shape");
        assert_eq!(columns, vec!["affected_rows".to_string()]);
        assert_eq!(rows, vec![vec!["3".to_string()]]);
    }

    /// DuckDB surfaces DML counts as a `Count` column through
    /// `execute_query` (verified empirically, task 1.7); the relay must
    /// pass that relation through, not error on it.
    #[test]
    fn dml_count_relation_passes_through() {
        let mgr = DuckDBConnectionManager::new_memory().unwrap();
        let mut ex = DuckDBExecutorImpl::new(&mgr);
        ex.execute_query("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        let result = ex
            .execute_query("INSERT INTO t VALUES (3, 'gamma')")
            .unwrap();
        assert_eq!(
            result.affected_rows, None,
            "executor does not populate the Option (task 1.7 finding)"
        );
        let (columns, rows) = shape_result(result).expect("Count relation passes through");
        assert_eq!(columns, vec!["Count".to_string()]);
        assert_eq!(rows, vec![vec!["1".to_string()]]);
    }

    #[test]
    fn connect_is_fail_closed() {
        assert!(DuckParty::connect("/nonexistent/nope.duckdb").is_err());
    }

    /// Drive one SQL statement through a fresh relay session over the
    /// given party; Ok(handle columns) on Header, Err(message) on Error.
    fn run_sql(party: DuckParty, sql: &str) -> Result<Vec<String>, String> {
        let client = RelayClient::new(DirectTransport::new(party));
        let VersionResult::Accepted(mut session) = client
            .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
            .unwrap()
        else {
            panic!("handshake should succeed")
        };
        match session.query(b(sql)).unwrap() {
            QueryResponse::Header { dimensions, .. } => Ok(dimensions
                .iter()
                .map(|d| String::from_utf8_lossy(&d.name).into_owned())
                .collect()),
            QueryResponse::Error { message, .. } => {
                Err(String::from_utf8_lossy(&message).into_owned())
            }
        }
    }

    /// Read one scalar back from the file via a fresh read-only manager
    /// (a separate open — proves the write PERSISTED, not that it merely
    /// succeeded in a session).
    fn reopen_and_read(path: &std::path::Path, sql: &str) -> String {
        let mgr = DuckDBConnectionManager::new_file_readonly(path.to_str().unwrap()).unwrap();
        let mut ex = DuckDBExecutorImpl::new(&mgr);
        let result = ex.execute_query(sql).unwrap();
        result.rows[0][0].clone()
    }

    /// E-T3b RED: DuckDB is ruled WRITABLE (EFFECTS-ON-TARGETS-PLAN §1
    /// finding 2). A plain INSERT through the party against a FILE
    /// connection must succeed and persist across a reopen.
    #[test]
    fn file_insert_persists_across_reopen() {
        let path = temp_db();
        let party = DuckParty::connect(path.to_str().unwrap()).unwrap();
        run_sql(party, "INSERT INTO t VALUES (3, 'gamma')")
            .expect("INSERT through the party must succeed (writable ruling)");
        // Party dropped: lock released. Reopen the FILE and read back.
        assert_eq!(reopen_and_read(&path, "SELECT count(*) FROM t"), "3");
        let _ = std::fs::remove_file(&path);
    }

    /// E-T3b RED companion: CREATE TABLE (durable DDL) through the party
    /// persists across a reopen.
    #[test]
    fn file_create_table_persists_across_reopen() {
        let path = temp_db();
        {
            let party = DuckParty::connect(path.to_str().unwrap()).unwrap();
            let client = RelayClient::new(DirectTransport::new(party));
            let VersionResult::Accepted(mut session) = client
                .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
                .unwrap()
            else {
                panic!("handshake should succeed")
            };
            for sql in ["CREATE TABLE made (x INTEGER)", "INSERT INTO made VALUES (7)"] {
                match session.query(b(sql)).unwrap() {
                    QueryResponse::Header { .. } => {}
                    QueryResponse::Error { message, .. } => panic!(
                        "{} must succeed (writable ruling), got: {}",
                        sql,
                        String::from_utf8_lossy(&message)
                    ),
                }
            }
        }
        assert_eq!(reopen_and_read(&path, "SELECT x FROM made"), "7");
        let _ = std::fs::remove_file(&path);
    }

    /// E-T3b: the explicit read-only opt-in keeps the old posture —
    /// the engine refuses the write; reads still work.
    #[test]
    fn connect_readonly_still_refuses_writes() {
        let path = temp_db();
        let party = DuckParty::connect_readonly(path.to_str().unwrap()).unwrap();
        let err = run_sql(party, "INSERT INTO t VALUES (9, 'nope')")
            .expect_err("read-only opt-in must refuse the INSERT");
        assert!(
            err.contains("read-only"),
            "refusal should name read-only mode, got: {}",
            err
        );
        let party = DuckParty::connect_readonly(path.to_str().unwrap()).unwrap();
        run_sql(party, "SELECT * FROM t").expect("read-only still reads");
        assert_eq!(reopen_and_read(&path, "SELECT count(*) FROM t"), "2");
        let _ = std::fs::remove_file(&path);
    }

    /// E-T3b, the double-spawn constraint: one dql session spawns TWO
    /// fatboy children on the same file (the idle spare from
    /// make_connection plus the mount's worker — REPORT-T-P2 §C), and a
    /// writable open takes DuckDB's EXCLUSIVE lock (P3 §C). So `connect`
    /// must NOT open the file; the first Query does. Pin: two live
    /// parties on one file, and the one that actually speaks can write.
    #[test]
    fn connect_does_not_take_the_file_lock() {
        let path = temp_db();
        let idle_spare = DuckParty::connect(path.to_str().unwrap())
            .expect("first connect (the idle spare)");
        let worker = DuckParty::connect(path.to_str().unwrap())
            .expect("second connect must succeed while the spare is alive");
        run_sql(worker, "INSERT INTO t VALUES (4, 'delta')")
            .expect("the worker (second-connected) party must hold the write lock");
        drop(idle_spare);
        assert_eq!(reopen_and_read(&path, "SELECT count(*) FROM t"), "3");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn erroring_query_carries_identity() {
        let path = temp_db();
        let party = DuckParty::connect(path.to_str().unwrap()).unwrap();
        let client = RelayClient::new(DirectTransport::new(party));
        let VersionResult::Accepted(mut session) = client
            .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
            .unwrap()
        else {
            panic!("handshake should succeed")
        };
        match session.query(b("SELECT * FROM no_such_table")).unwrap() {
            QueryResponse::Error { identity, .. } => {
                assert_eq!(identity, b("delightql-error://target/duckdb/error"));
            }
            QueryResponse::Header { .. } => panic!("expected Error"),
        }
        let _ = std::fs::remove_file(&path);
    }
}
