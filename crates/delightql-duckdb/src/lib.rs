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
//! - **No server**: DuckDB is a file database. `connect` opens the file
//!   fail-closed (`new_file_existing` — a DuckParty whose database
//!   doesn't exist never exists). One party = one connection = the
//!   file's single writer; per-relay-connection parties serialize on
//!   DuckDB's own locking.
//! - **Descriptors are empty** (v1): the backends executor abstracts
//!   the statement away. Foreign descriptors were postgres's question
//!   to answer; here they're a noted gap, not a goal.
//! - **No load path** (v1): Prepare/Offer answer unimplemented, like
//!   SqlParty. DuckDB bulk loading wants the Appender API; that's a
//!   later slice if a workload asks.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use delightql_backends::{DuckDBConnectionManager, DuckDBExecutor, DuckDBExecutorImpl};
use delightql_protocol::{
    resolve_projection, Cell, ClientTerm, Dimension, ErrorKind, Handle, Handler, MetaItem,
    Orientation, Projection, ServerTerm,
};

const IDENT_DUCK_ERROR: &str = "dql/target/duckdb/error";
const IDENT_UNIMPLEMENTED: &str = "dql/target/duckdb/unimplemented";

struct ResultState {
    columns: Vec<String>,
    rows: VecDeque<Vec<Cell>>,
    exec_ms: u64,
}

pub struct DuckParty {
    manager: DuckDBConnectionManager,
    handles: HashMap<Handle, ResultState>,
    next_handle_id: u64,
}

impl DuckParty {
    /// Open the database file, fail-closed (must already exist).
    pub fn connect(db_path: &str) -> Result<Self, String> {
        // Read-only: lets multiple fatboy children share one file across
        // processes (the stdio model). Query-only fatboy, so no loss.
        let manager = if db_path == ":memory:" {
            DuckDBConnectionManager::new_memory().map_err(|e| e.to_string())?
        } else {
            DuckDBConnectionManager::new_file_readonly(db_path).map_err(|e| e.to_string())?
        };
        Ok(Self {
            manager,
            handles: HashMap::new(),
            next_handle_id: 1,
        })
    }

    fn handle_query(&mut self, text: Vec<u8>) -> ServerTerm {
        let sql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(_) => {
                return error(ErrorKind::Syntax, IDENT_DUCK_ERROR, b"query is not UTF-8".to_vec())
            }
        };

        let started = Instant::now();
        let mut executor = DuckDBExecutorImpl::new(&self.manager);
        let result = match executor.execute_query(&sql) {
            Ok(r) => r,
            Err(e) => {
                return error(ErrorKind::Syntax, IDENT_DUCK_ERROR, e.to_string().into_bytes())
            }
        };
        let exec_ms = started.elapsed().as_millis() as u64;

        // DML/DDL with no result columns: the spec-appendix relation.
        let (columns, rows): (Vec<String>, Vec<Vec<String>>) = if result.columns.is_empty() {
            let n = result.affected_rows.unwrap_or(0);
            (vec!["affected_rows".to_string()], vec![vec![n.to_string()]])
        } else {
            (result.columns, result.rows)
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
                "dql/target/duckdb/orientation",
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
                        "dql/target/duckdb/orientation",
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

    #[test]
    fn connect_is_fail_closed() {
        assert!(DuckParty::connect("/nonexistent/nope.duckdb").is_err());
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
                assert_eq!(identity, b("dql/target/duckdb/error"));
            }
            QueryResponse::Header { .. } => panic!("expected Error"),
        }
        let _ = std::fs::remove_file(&path);
    }
}
