// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Epic 4.1: `table!` DURABLE PLACEMENT + cross-kind temp replace.
//!
//! Why integration tests and not balls: persistence is a CROSS-SESSION
//! property — the object must survive the process that created it and be
//! visible to a second `dql` invocation reopening the same `--db` FILE. A
//! ball runs inside one session; it structurally cannot observe what the
//! file holds after exit (REPORT-3R-FIX-BATCH discovery 1: `table!`
//! answered success while the file held nothing — the CTAS landed in the
//! ephemeral `:memory:` primary, not the mounted file). The cross-kind
//! tests need the SESSION CATALOG between two plans on one session
//! (`--sequential`), which run-one ball statements also exercise, but the
//! red engine error ("use DROP TABLE to delete table …") was observed
//! here first against the pre-fix binary.
//!
//! materialize-pipe §2 (connection attribution) + EFFECT-ALGEBRA §3
//! (temp replacement is by NAME, not kind — ruled 2026-07-11).

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query` with `stdin = query`, cwd = `dir`, against `db`.
fn run_dql(dir: &Path, db: &str, query: &str, sequential: bool) -> (bool, String, String) {
    let mut cmd = Command::new(dql_bin());
    cmd.arg("query")
        .arg("--db")
        .arg(db)
        .arg("--to")
        .arg("results")
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if sequential {
        cmd.arg("--sequential");
    }
    let mut child = cmd.spawn().expect("spawn dql");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(query.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait dql");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// A db file with `orders` (3 rows, 2 EU / 1 US).
fn fixture(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("world.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE orders (order_id INTEGER, region TEXT, amount INTEGER);
         INSERT INTO orders VALUES (101, 'EU', 250), (102, 'US', 80), (103, 'EU', 40);",
    )
    .unwrap();
    db
}

/// The persistence pin (REPORT-3R-FIX-BATCH discovery 1's sharp repro,
/// red-observed 2026-07-11 against the pre-fix binary: invocation 1
/// answered a success receipt, but the file held no `archived` and
/// invocation 2 died "Table not found: archived").
///
/// `table!` is the DURABLE analog of `temp_table!` (materialize-pipe §1):
/// the CTAS must land in the backend schema of the connection its source
/// reads from — the mounted `--db` file — and survive the session.
#[test]
fn table_bang_persists_to_the_db_file_across_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let db_str = db.to_str().unwrap();

    // Session 1: create, read back in-session, exit.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        "orders(*), region = \"EU\" |> table!(archived)",
        false,
    );
    assert!(ok, "table! should succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("table!"),
        "expected a table! receipt.\nstdout:\n{stdout}"
    );

    // The FILE must hold the table and its rows after the process exits.
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'archived'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            n, 1,
            "the db FILE must hold 'archived' after exit (durable placement, \
             materialize-pipe §2/§3) — the pre-fix binary left it in the \
             ephemeral :memory: primary"
        );
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM archived", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 2, "archived should hold the 2 EU orders");
    }

    // Session 2: a fresh dql invocation on the same file reads it back.
    let (ok, stdout, stderr) = run_dql(dir.path(), db_str, "archived(*)", false);
    assert!(
        ok,
        "a second session must resolve the durable table.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("101") && stdout.contains("103"),
        "second session should read both EU rows.\nstdout:\n{stdout}"
    );
}

/// Cross-kind temp replace, direction 1 (EFFECT-ALGEBRA §3, ruled
/// 2026-07-11: replacement is by NAME, not kind). A temp view over an
/// existing temp TABLE drops the table first.
///
/// Red-observed 2026-07-11 against the pre-fix binary:
/// `Error: Connection: use DROP TABLE to delete table sw` (raw engine
/// error — the replace drop was kind-matched to the DIRECTIVE, not to
/// the holder).
#[test]
fn temp_view_over_temp_table_replaces_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "orders(*) |> temp_table!(sw)\n\n\
         orders(*), region = \"EU\" |> temp_view!(sw)\n\n\
         sw(*)",
        true,
    );
    assert!(
        ok,
        "temp_view! over a same-name temp table must replace it.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("101") && stdout.contains("103") && !stdout.contains("102"),
        "sw must be the VIEW's world (EU only) after the replace.\nstdout:\n{stdout}"
    );
}

/// Cross-kind temp replace, direction 2: a temp table over an existing
/// temp VIEW drops the view first.
///
/// Red-observed 2026-07-11 against the pre-fix binary:
/// `Error: Connection: use DROP VIEW to delete view sw`.
#[test]
fn temp_table_over_temp_view_replaces_the_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "orders(*) |> temp_view!(sw)\n\n\
         orders(*), region = \"US\" |> temp_table!(sw)\n\n\
         sw(*)",
        true,
    );
    assert!(
        ok,
        "temp_table! over a same-name temp view must replace it.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("102") && !stdout.contains("101"),
        "sw must be the TABLE's world (US only) after the replace.\nstdout:\n{stdout}"
    );
}
