// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! One backend per one-shot fatboy `--db` query — the spawn-count pin for
//! bugs/duplicate-fatboy-spawn-one-shot.
//!
//! The bug: a file/URI-backed one-shot `dql query --db <fatboy>` used to
//! spawn the foreign-engine child TWICE — once eagerly by make_connection
//! (thrown away, since open_handle builds the session from factories and
//! ignores it) and once for real by the `mount!` first-query. This lane
//! counts the children with a shim pinned via DQL_FATBOY_DIR (the hard-pin
//! lookup dir, fatboy_exec.rs FATBOY_DIR_ENV): a wrapper that appends to a
//! counter file then `exec`s the real adapter. Exactly ONE spawn is the
//! contract; it was observed at TWO against the pre-fix binary
//! (2026-07-12, both duckdb and the sweep-lane postgres).
//!
//! The spawn happens BEFORE resolution, so the query may error on
//! table-not-found and the count is still observable — but the target must
//! be REACHABLE, because the child's own handshake with the engine has to
//! SUCCEED for the second spawn to be reached (an unreachable target aborts
//! the run at the first child's failed handshake, masking the doubling).
//! So duckdb (a local file, always "reachable") is the portable pin;
//! postgres rides the sweep container when present.
//!
//! Gating mirrors effects_on_targets.rs: each test SKIPS with an eprintln
//! when its adapter / target is unavailable.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

fn fatboy_present(name: &str) -> bool {
    real_fatboy(name).map(|p| p.is_file()).unwrap_or(false)
}

/// The real adapter sibling of the dql under test.
fn real_fatboy(name: &str) -> Option<PathBuf> {
    PathBuf::from(dql_bin())
        .parent()
        .map(|d| d.join(format!("{}{}", name, std::env::consts::EXE_SUFFIX)))
}

fn cli_present(bin: &str) -> bool {
    Command::new(bin)
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Build a shim directory holding a `<name>` wrapper that appends one line
/// to `counter` per invocation then `exec`s the real adapter. Returns the
/// shim dir to hand to DQL_FATBOY_DIR. (Unix-only, like the rest of the
/// target lanes.)
fn make_spawn_shim(dir: &Path, name: &str, counter: &Path) -> PathBuf {
    let real = real_fatboy(name).expect("real adapter path");
    let shim_dir = dir.join("shim");
    std::fs::create_dir_all(&shim_dir).unwrap();
    let script = format!(
        "#!/bin/bash\necho spawn >> {counter:?}\nexec {real:?} \"$@\"\n",
        counter = counter,
        real = real,
    );
    let wrapper = shim_dir.join(format!("{}{}", name, std::env::consts::EXE_SUFFIX));
    std::fs::write(&wrapper, script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    shim_dir
}

fn spawn_count(counter: &Path) -> usize {
    std::fs::read_to_string(counter)
        .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0)
}

/// Run one `dql query --db <db>` with the fatboy adapter pinned to `shim_dir`.
fn run_one_query(shim_dir: &Path, db: &str, query: &str) -> (bool, String) {
    let out = Command::new(dql_bin())
        .arg("query")
        .arg("--db")
        .arg(db)
        .arg(query)
        .env("DQL_FATBOY_DIR", shim_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn dql");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// DuckDB is the portable pin: a local file, so the adapter's handshake
/// always succeeds and both would-be spawns are reachable. A query on a
/// missing table errors AFTER the backend opened, so exactly one child is
/// the whole story.
#[test]
fn duckdb_one_shot_query_spawns_the_backend_once() {
    let test = "duckdb_one_shot_query_spawns_the_backend_once";
    if !fatboy_present("dql-fatboy-duckdb") {
        eprintln!("SKIP {}: no dql-fatboy-duckdb next to {}", test, dql_bin());
        return;
    }
    if !cli_present("duckdb") {
        eprintln!("SKIP {}: no `duckdb` CLI on PATH (used to author the fixture)", test);
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("target.duckdb");
    // A valid duckdb file with one table (so classify routes to the duckdb
    // fatboy and the adapter opens it cleanly).
    let status = Command::new("duckdb")
        .arg(&db)
        .arg("CREATE TABLE t (x INTEGER);")
        .status()
        .expect("run duckdb CLI");
    assert!(status.success(), "duckdb CLI must author the fixture");

    let counter = dir.path().join("spawns.log");
    let shim_dir = make_spawn_shim(dir.path(), "dql-fatboy-duckdb", &counter);

    // A query on a MISSING table: the child spawns to open the backend,
    // then resolution errors — the count is complete regardless.
    let (_ok, _stderr) = run_one_query(&shim_dir, db.to_str().unwrap(), "orders(order_id)");
    assert_eq!(
        spawn_count(&counter),
        1,
        "a one-shot fatboy --db query must open exactly ONE backend child \
         (was two: make_connection's eager child was thrown away). \
         stderr:\n{_stderr}"
    );

    // And a WORKING query is still exactly one child.
    std::fs::write(&counter, "").unwrap();
    let (ok, stderr) = run_one_query(&shim_dir, db.to_str().unwrap(), "t(x)");
    assert!(ok, "the working query must succeed.\nstderr:\n{stderr}");
    assert_eq!(spawn_count(&counter), 1, "a working query is one child too.\nstderr:\n{stderr}");
}

/// Postgres via the sweep container (new_test_suite/sweep.py: `dql-sweep-pg`,
/// trust auth, 127.0.0.1:5433) — the filing's original repro. Reachable, so
/// the doubling manifested; now it is one.
#[test]
fn postgres_one_shot_query_spawns_the_backend_once() {
    let test = "postgres_one_shot_query_spawns_the_backend_once";
    let host = "127.0.0.1:5433";
    let reachable = std::net::TcpStream::connect_timeout(
        &host.parse().unwrap(),
        std::time::Duration::from_millis(500),
    )
    .is_ok();
    if !reachable {
        eprintln!(
            "SKIP {}: no PG at {} (start with new_test_suite/sweep.py postgres)",
            test, host
        );
        return;
    }
    if !fatboy_present("dql-fatboy-postgres") {
        eprintln!("SKIP {}: no dql-fatboy-postgres next to {}", test, dql_bin());
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let counter = dir.path().join("spawns.log");
    let shim_dir = make_spawn_shim(dir.path(), "dql-fatboy-postgres", &counter);

    let db = format!("postgres://postgres@{}/postgres", host);
    // Missing table → resolution error AFTER the backend opened; count intact.
    let (_ok, stderr) = run_one_query(&shim_dir, &db, "orders(order_id)");
    assert_eq!(
        spawn_count(&counter),
        1,
        "a one-shot fatboy --db query must open exactly ONE postgres backend \
         child (was two).\nstderr:\n{stderr}"
    );
}
