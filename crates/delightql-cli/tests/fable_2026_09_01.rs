// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! CLI-only discriminators from the blinded 2026-09-01 binary campaign.
//!
//! An outside observer can see each defect, but a corpus ball cannot: balls do
//! not feed CSV/JSON stdin or start the REPL, and the runner itself uses
//! `--to hash` as its execution road.  These therefore belong at the real CLI
//! boundary rather than behind a compiler unit-test seam.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

fn run(dir: &Path, args: &[&str], stdin: &str) -> (bool, String, String) {
    let mut child = Command::new(dql_bin())
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn dql");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait dql");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// F-009: the documented numeric filter must not become a TEXT-vs-INTEGER
/// comparison that admits every CSV row.
#[test]
fn csv_numeric_filter_obeys_the_values_not_sqlite_storage_precedence() {
    let tmp = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "tools",
            "csvstruct",
            "--has-headers",
            "--format",
            "csv",
            "c(*), age > 26",
        ],
        "name,age\nA,30\nB,7\nC,\n",
    );
    assert!(ok, "csvstruct failed: {stderr}");
    assert!(stdout.contains("A,30"), "qualifying row missing: {stdout}");
    assert!(!stdout.contains("B,7"), "7 compared as TEXT: {stdout}");
    assert!(
        !stdout.contains("C,"),
        "empty TEXT compared as numeric: {stdout}"
    );
}

/// F-022: installation of the binary's own `repl::*` definitions must obey
/// the same keyword law as authored code.  Stropping the wrapper declaration
/// is insufficient if its body still cites the reserved data name bare.
#[cfg(feature = "repl")]
#[test]
fn embedded_repl_namespace_installs_and_answers() {
    use std::sync::Arc;

    use delightql_cli::client::context::Mode;
    use delightql_cli::client::database::ClientDatabase;
    use delightql_cli::client::mount::{install_repl_namespace, open_client_handle};
    use delightql_cli::exec_ng::run_dql_query;

    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("open the client database"));
    let mut handle = open_client_handle(&db).expect("open the client handle");
    install_repl_namespace(&mut *handle).expect("install repl::*");
    let mut session = handle.session().expect("session");
    // `option` is a reserved word: bare, the admission law refuses it in
    // every position, which is the very defect this test pins.
    run_dql_query("repl::config.`option`(*)", &mut *session)
        .expect("the installed repl::config wrapper answers");
}

/// F-026: every `--to` inspection spelling is non-executing.  The refusal and
/// unchanged database are both required; either alone leaves a side-effect
/// door.
#[test]
fn hash_inspection_does_not_execute_an_effect() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("main.sqlite");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TABLE sink(id INTEGER); INSERT INTO sink VALUES (1);")
        .unwrap();
    drop(conn);

    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "query",
            "--db",
            "main.sqlite",
            "--to",
            "hash",
            "_(id @ 2) |> insert!(sink(*))(*)",
        ],
        "",
    );
    assert!(!ok, "inspection executed successfully: {stdout}{stderr}");

    let conn = rusqlite::Connection::open(&db).unwrap();
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM sink", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "inspection inserted a row");
}

/// F-039: a renderer must not begin a JSON document after execution has
/// already failed.  The structured error is on stderr; stdout stays empty.
#[test]
fn json_format_emits_no_partial_document_after_runtime_error() {
    let tmp = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "tools",
            "jstruct",
            "--error-prefix",
            "",
            "--error-format",
            "json",
            "--format",
            "json",
            "j(j) |> (j:{.name} as name)",
        ],
        "not json",
    );
    assert!(!ok, "malformed JSON unexpectedly succeeded");
    assert!(stderr.contains("malformed JSON"), "wrong error: {stderr}");
    assert!(
        stdout.is_empty(),
        "partial JSON document escaped: {stdout:?}"
    );
}
