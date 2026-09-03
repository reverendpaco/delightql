// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The exit road against the real binary: the three session files, when
//! they are written, and what they carry.

use std::path::{Path, PathBuf};
use std::process::Command;

fn dql_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_dql"))
}

fn files_in(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .map(|it| {
            it.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

fn read_one(dir: &Path, prefix: &str) -> String {
    let name = files_in(dir)
        .into_iter()
        .find(|n| n.starts_with(prefix))
        .unwrap_or_else(|| panic!("no {prefix}* in {}", dir.display()));
    std::fs::read_to_string(dir.join(name)).unwrap()
}

fn run(dir: &Path, args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let mut cmd = Command::new(dql_exe());
    cmd.args(args)
        .env("DQL_STATE_DIR", dir)
        .env_remove("RUST_BACKTRACE")
        .env_remove("DQL_TEST_PANIC");
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.output().expect("run dql")
}

/// A clean one-shot writes nothing: a suite run is thousands of these.
#[test]
fn a_clean_one_shot_writes_no_session_files() {
    let dir = tempfile::tempdir().unwrap();
    let out = run(dir.path(), &["query", "_(x@1)"], &[]);
    assert!(out.status.success());
    assert!(files_in(dir.path()).is_empty(), "{:?}", files_in(dir.path()));
    assert!(!String::from_utf8_lossy(&out.stderr).contains("session "));
}

/// A refusal alone writes nothing: the user just read it on stderr, and a
/// suite run is thousands of these.
#[test]
fn a_refusal_alone_writes_no_session_files() {
    let dir = tempfile::tempdir().unwrap();
    let out = run(dir.path(), &["query", "nosuch(*)"], &[]);
    assert!(!out.status.success());
    assert!(files_in(dir.path()).is_empty(), "{:?}", files_in(dir.path()));
}

/// A warning about a flag the user chose is said and recorded, and earns
/// no files.
#[test]
fn a_warning_alone_writes_no_session_files() {
    let dir = tempfile::tempdir().unwrap();
    let out = run(dir.path(), &["query", "--no-sanitize", "_(x@1)"], &[]);
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("warning: output sanitization"));
    assert!(files_in(dir.path()).is_empty(), "{:?}", files_in(dir.path()));
}

/// A client ERROR writes the triple, stamped alike; error.log carries the
/// client's row; context carries argv and the census; the announcement
/// is the LAST stderr line. The client error here: an unbadged refusal at
/// the boundary (an unknown DQL_DIALECT), recorded under client/unbadged.
#[test]
fn a_client_error_writes_the_triple() {
    let dir = tempfile::tempdir().unwrap();
    let out = run(dir.path(), &["query", "_(x@1)"], &[("DQL_DIALECT", "oracle")]);
    assert!(!out.status.success());
    let names = files_in(dir.path());
    assert_eq!(names.len(), 3, "{names:?}");
    let stamp = names[0].rsplit('.').next().unwrap().to_string();
    assert!(names.iter().all(|n| n.ends_with(&stamp)), "one stamp: {names:?}");

    let log = read_one(dir.path(), "error.log.");
    assert_eq!(log.lines().count(), 1, "{log}");
    assert!(log.contains("\"origin\": \"client\""), "{log}");
    assert!(log.contains("\"kind\": \"error\""), "{log}");
    assert!(log.contains("\"uri\": \"delightql-error://client/unbadged\""), "{log}");
    assert!(log.contains("unknown DQL_DIALECT"), "{log}");
    assert!(log.contains("\"road\": \"main\""), "{log}");

    let context = read_one(dir.path(), "context.");
    assert!(context.contains("\"relation\": \"session\""));
    assert!(context.contains("\"mode\": \"query\""));
    assert!(context.contains("\"exit_code\": \"1\""), "{context}");
    assert!(context.contains("\"relation\": \"argument\""));
    assert!(context.contains("\"value\": \"_(x@1)\""), "argv verbatim");
    assert!(context.contains("\"relation\": \"environment\""));
    let dialect_row = context
        .lines()
        .find(|l| l.contains("\"name\": \"DQL_DIALECT\""))
        .expect("the DQL_DIALECT row");
    assert!(dialect_row.contains("\"is_set\": \"1\"") && dialect_row.contains("\"value\": \"oracle\""), "{dialect_row}");
    assert!(context.contains("\"name\": \"DQL_STATE_DIR\""), "the census includes the override itself");
    // No option rows: only the interactive road seeds repl::config.
    assert!(!context.contains("\"relation\": \"option\""));

    let script = read_one(dir.path(), "replay-script.");
    assert!(script.starts_with("# dql replay 1\n# session dql-"), "{script}");

    let stderr = String::from_utf8_lossy(&out.stderr);
    let lines: Vec<&str> = stderr.lines().collect();
    let error = lines.iter().position(|l| l.contains("unknown DQL_DIALECT")).unwrap();
    let session = lines.iter().position(|l| l.starts_with("session ")).unwrap();
    assert!(error < session, "the error comes first: {stderr}");
    assert_eq!(session, lines.len() - 1, "the announcement is last: {stderr}");
}

/// A panic on the main thread — before any handle exists — still lands
/// in error.log as a client-side panic row.
#[test]
fn a_panic_writes_the_triple_with_the_panic_row() {
    let dir = tempfile::tempdir().unwrap();
    let out = run(dir.path(), &["query", "_(x@1)"], &[("DQL_TEST_PANIC", "1")]);
    assert_eq!(out.status.code(), Some(1));
    let log = read_one(dir.path(), "error.log.");
    assert_eq!(log.lines().count(), 1, "{log}");
    assert!(log.contains("\"origin\": \"client\""));
    assert!(log.contains("\"kind\": \"panic\""));
    assert!(log.contains("\"uri\": \"delightql-error://internal/panic\""));
    assert!(log.contains("deliberate test panic"));
    assert!(log.contains("\"road\": \"main\""), "{log}");
    assert!(log.contains("\"input\": null"), "a missing cell is null, never the text NULL: {log}");
    let context = read_one(dir.path(), "context.");
    assert!(context.contains("\"mode\": \"query\""), "the road was decided before the panic: {context}");
}
