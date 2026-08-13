// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `stdout!` console-sink integration.
//!
//! Why an integration test and not a ball: ball tests structurally CANNOT
//! observe stdout! content — the server's stdout is discarded and the runner
//! is a pure socket client. Here we drive the
//! real `dql` binary: the console sink installed by the CLI
//! (`session_with_hooks` → `RelayHooks::on_ship`) prints each mid-run
//! shipped set on the CLI process's own stdout, which the test captures.
//!
//! The effects ball's util--36_stdout_passthrough pins the SEMANTIC half
//! (stdout! passes its relation through unchanged); these tests pin the
//! CONTENT half: the printed set reaches the console, in run order, before
//! the final result — and does NOT leak into machine outputs (`--to hash`).

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query --sequential` with `stdin = query`, cwd = `dir`.
fn run_dql(dir: &Path, db: &str, to: &str, query: &str) -> (bool, String, String) {
    let mut cmd = Command::new(dql_bin());
    cmd.arg("query")
        .arg("--db")
        .arg(db)
        .arg("--to")
        .arg(to)
        .arg("--sequential")
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
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

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn fixture(dir: &Path) {
    let conn = rusqlite::Connection::open(dir.join("w.sqlite")).unwrap();
    conn.execute_batch(
        "CREATE TABLE customers (customer_id INTEGER, region TEXT, name TEXT);
         INSERT INTO customers VALUES (1,'EU','Ada'),(2,'US','Bob'),(3,'EU','Carol');",
    )
    .unwrap();
    write(
        &dir.join("ddl/script.dql"),
        "main!(*) :- customers(*), region = \"EU\" |> stdout!(*) |> temp_table!(snap(*))(*)\n",
    );
}

/// The console sink: a mid-run `stdout!` set prints live on the CLI's
/// stdout during `--to results`, alongside (and before) the final result
/// (the run's return value — here temp_table!'s receipt).
#[test]
fn stdout_ship_prints_on_the_cli_console() {
    let tmp = tempfile::tempdir().unwrap();
    fixture(tmp.path());

    let (ok, stdout, stderr) = run_dql(
        tmp.path(),
        "w.sqlite",
        "results",
        "consult!(\"ddl/script.dql\", \"fx\")(*)\n\nrun_namespace!(fx)(*)\n",
    );
    assert!(ok, "run failed: {}", stderr);

    // The shipped set: the EU customers, printed by the on_ship console sink.
    assert!(
        stdout.contains("Ada") && stdout.contains("Carol"),
        "stdout! rows missing from console output:\n{}",
        stdout
    );
    assert!(
        !stdout.contains("Bob"),
        "non-EU row leaked into the shipped set:\n{}",
        stdout
    );
    // The final result (the run's return value: temp_table!'s receipt)
    // still prints — the ship is IN ADDITION to the wire response.
    assert!(
        stdout.contains("temp_table!") && stdout.contains("snap"),
        "final receipt missing from console output:\n{}",
        stdout
    );
    // Order: the mid-run ship precedes the final result.
    assert!(
        stdout.find("Ada").unwrap() < stdout.find("temp_table!").unwrap(),
        "stdout! set did not precede the final result:\n{}",
        stdout
    );
}

/// Machine outputs stay machine-readable: `--to hash` prints EXACTLY the
/// hash line — the console sink must not corrupt it (the ball runner and
/// run-one.py read this stream as a single hex value).
#[test]
fn stdout_ship_does_not_leak_into_hash_output() {
    let tmp = tempfile::tempdir().unwrap();
    fixture(tmp.path());

    let (ok, stdout, stderr) = run_dql(
        tmp.path(),
        "w.sqlite",
        "hash",
        "consult!(\"ddl/script.dql\", \"fx\")(*)\n\nrun_namespace!(fx)(*)\n",
    );
    assert!(ok, "run failed: {}", stderr);
    let trimmed = stdout.trim();
    assert!(
        !trimmed.is_empty()
            && trimmed.lines().count() == 1
            && trimmed.chars().all(|c| c.is_ascii_hexdigit()),
        "--to hash output is not a single hex line:\n{:?}",
        stdout
    );
}

/// Session-scope sanity (task §3.3 item 5): a second run on the SAME
/// session must not collide with the first's leftover scratch — the entry
/// point drops `__r_*`/`__exit` before replaying (fresh scratch per run).
/// Sequential mode keeps one session across statements, so two
/// run_namespace! statements exercise exactly the leftover case.
#[test]
fn run_twice_on_one_session_gets_fresh_scratch() {
    let tmp = tempfile::tempdir().unwrap();
    let conn = rusqlite::Connection::open(tmp.path().join("w.sqlite")).unwrap();
    conn.execute_batch("CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, msg TEXT);")
        .unwrap();
    drop(conn);
    write(
        &tmp.path().join("ddl/mark.dql"),
        "main!(*) :- _(msg @ \"ran\") |> insert!(audit_log(*))(*)\n",
    );

    let (ok, stdout, stderr) = run_dql(
        tmp.path(),
        "w.sqlite",
        "results",
        "consult!(\"ddl/mark.dql\", \"fx\")(*)\n\n\
         run_namespace!(fx)(*)\n\n\
         run_namespace!(fx)(*)\n\n\
         audit_log(*) ~> count:(*) as n\n",
    );
    assert!(ok, "second run collided with leftover scratch: {}", stderr);
    // Both runs' effects landed: two audit rows.
    assert!(
        stdout.contains('2'),
        "expected both runs' inserts (count 2):\n{}",
        stdout
    );
}
