// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! E-T3b: the dql-fatboy-duckdb BINARY's mode surface, end-to-end over
//! its real stdin/stdout — the exact channel dql speaks.
//!
//! - default (no flag): WRITABLE — an INSERT succeeds and persists in
//!   the file after the child exits.
//! - `--readonly`: the explicit opt-in — the engine refuses the INSERT,
//!   reads still work, and the file is untouched.
//!
//! Self-contained: fixtures are authored via the crate's own (writable)
//! backends manager; no duckdb CLI, no container.

use std::process::{Command, Stdio};

use delightql_backends::{DuckDBConnectionManager, DuckDBExecutorImpl, DuckDBExecutor};
use delightql_protocol::stdio::StdioTransport;
use delightql_protocol::{
    Client, FetchResponse, Orientation, Projection, QueryResponse, VersionResult,
};

fn fatboy_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql-fatboy-duckdb")
}

/// Author a fixture file holding `t` (2 rows), lock released on drop.
fn temp_db() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "duckfatboy-flag-test-{}-{}.duckdb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let mgr = DuckDBConnectionManager::new_file(path.to_str().unwrap()).unwrap();
        let mut ex = DuckDBExecutorImpl::new(&mgr);
        ex.execute_query("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        ex.execute_query("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')")
            .unwrap();
    }
    path
}

/// Spawn the real binary, handshake, run one statement, return
/// Ok(first column names) or Err(error message). Child reaped when the
/// transport drops.
fn run_via_binary(db: &std::path::Path, readonly: bool, sql: &str) -> Result<Vec<String>, String> {
    let mut cmd = Command::new(fatboy_bin());
    cmd.arg("--database").arg(db);
    if readonly {
        cmd.arg("--readonly");
    }
    let child = cmd
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn dql-fatboy-duckdb");
    let transport = StdioTransport::from_child(child).expect("stdio transport");
    let client = Client::new(transport);
    let VersionResult::Accepted(mut session) = client
        .version(1_000_000, b"relay0".to_vec(), 300_000, vec![Orientation::Rows])
        .unwrap()
    else {
        panic!("handshake should succeed")
    };
    let rows_o = session.agreed_orientation(Orientation::Rows).unwrap();
    match session.query(sql.as_bytes().to_vec()).unwrap() {
        QueryResponse::Header { handle, dimensions } => {
            // Drain so the statement fully completes before we return.
            loop {
                match session.fetch(&handle, Projection::All, 1000, rows_o).unwrap() {
                    FetchResponse::Data { .. } => {}
                    FetchResponse::End => break,
                    FetchResponse::Error { message, .. } => {
                        return Err(String::from_utf8_lossy(&message).into_owned())
                    }
                }
            }
            let _ = session.close(handle);
            Ok(dimensions
                .iter()
                .map(|d| String::from_utf8_lossy(&d.name).into_owned())
                .collect())
        }
        QueryResponse::Error { message, .. } => {
            Err(String::from_utf8_lossy(&message).into_owned())
        }
    }
}

fn count_rows(db: &std::path::Path) -> String {
    let mgr = DuckDBConnectionManager::new_file_readonly(db.to_str().unwrap()).unwrap();
    let mut ex = DuckDBExecutorImpl::new(&mgr);
    ex.execute_query("SELECT count(*) FROM t").unwrap().rows[0][0].clone()
}

/// Default mode is WRITABLE: the INSERT lands and persists in the file.
#[test]
fn binary_default_is_writable_and_persists() {
    let db = temp_db();
    run_via_binary(&db, false, "INSERT INTO t VALUES (3, 'gamma')")
        .expect("default-mode INSERT through the binary must succeed");
    assert_eq!(count_rows(&db), "3");
    let _ = std::fs::remove_file(&db);
}

/// `--readonly` opts back into the old posture: write refused by the
/// engine, read fine, file untouched.
#[test]
fn binary_readonly_flag_refuses_writes() {
    let db = temp_db();
    let err = run_via_binary(&db, true, "INSERT INTO t VALUES (9, 'nope')")
        .expect_err("--readonly must refuse the INSERT");
    assert!(
        err.contains("read-only"),
        "refusal should name read-only mode, got: {}",
        err
    );
    let cols = run_via_binary(&db, true, "SELECT name FROM t ORDER BY id")
        .expect("--readonly still reads");
    assert_eq!(cols, vec!["name".to_string()]);
    assert_eq!(count_rows(&db), "2");
    let _ = std::fs::remove_file(&db);
}
