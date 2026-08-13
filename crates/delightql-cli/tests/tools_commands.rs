// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `dql tools jstruct` / `csvstruct` / `filemunge` behavior.
//!
//! Each of these commands stages its input into a temporary SQLite file and
//! then binds it as `main` with a synthesized `mount!` before running the
//! user's query. Compiling is not evidence that the synthesized demand is
//! lawful: a higher-order directive whose receipt access is missing binds
//! zero arguments and refuses on arity, and the failure lands on the first
//! query the user runs, not at build time. These tests drive the real
//! binary end to end so the demand is exercised.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql <args...>` with the given stdin, returning (success, stdout, stderr).
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

/// The arity refusal a receipt-less `mount!(path, ns)` produces. No tools
/// command may ever print it: it means the staged database was never bound.
fn assert_mounted(what: &str, ok: bool, stdout: &str, stderr: &str) {
    let both = format!("{stdout}{stderr}");
    assert!(
        !both.contains("directive/binding/arity"),
        "{what}: the synthesized mount! bound no arguments — {both}"
    );
    assert!(ok, "{what}: exited nonzero — {stdout}{stderr}");
}

/// `jstruct` stages stdin as the single-column table `j`.
#[test]
fn jstruct_answers_the_users_query_over_piped_json() {
    let tmp = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &["tools", "jstruct", "--format", "tsv", "j(*)"],
        "{\"a\":1}\n",
    );
    assert_mounted("jstruct", ok, &stdout, &stderr);
    assert!(stdout.starts_with("j\n"), "jstruct heading: {stdout}");
    assert!(stdout.contains("\"a\""), "jstruct payload: {stdout}");
}

/// `csvstruct --has-headers` stages stdin as `c`, columns named by row one.
#[test]
fn csvstruct_answers_the_users_query_over_piped_csv() {
    let tmp = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "tools",
            "csvstruct",
            "--has-headers",
            "--format",
            "csv",
            "c(*), a = \"3\"",
        ],
        "a,b\n1,2\n3,4\n",
    );
    assert_mounted("csvstruct", ok, &stdout, &stderr);
    assert!(stdout.contains("3,4"), "csvstruct: {stdout}");
    assert!(!stdout.contains("1,2"), "csvstruct filter leaked: {stdout}");
}

#[test]
fn filemunge_answers_the_users_query_over_a_loaded_table() {
    let tmp = tempfile::tempdir().unwrap();
    let csv = tmp.path().join("t.csv");
    std::fs::write(&csv, "a,b\n1,2\n3,4\n").unwrap();
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "tools",
            "filemunge",
            "--table",
            "t:csv",
            csv.to_str().unwrap(),
            "--format",
            "csv",
            "t(*), a = \"3\"",
        ],
        "",
    );
    assert_mounted("filemunge", ok, &stdout, &stderr);
    assert!(stdout.contains("3,4"), "filemunge: {stdout}");
    assert!(!stdout.contains("1,2"), "filemunge filter leaked: {stdout}");
}
