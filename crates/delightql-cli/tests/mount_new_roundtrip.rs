// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `mount_new!` (EFFECT-ALGEBRA §6) — the create verb, end to end.
//!
//! Why an integration test and not a ball: `mount_new!` PROVISIONS a file on
//! disk and the acceptance is a CROSS-INVOCATION round-trip — a first `dql`
//! provisions a fresh empty database, and a SECOND `dql` `mount!`-attaches
//! that same file (mount!'s attach-only guard rejects missing/empty/invalid
//! paths, so a successful mount! is proof the provisioned file is a valid
//! database). A ball runs inside one session and cannot observe the file the
//! way a second process reopening it can.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query` with `stdin = query`, cwd = `dir`, against `db`.
fn run_dql(dir: &Path, db: &str, query: &str) -> (bool, String, String) {
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

/// A valid empty SQLite database carries the 16-byte header magic.
fn is_valid_sqlite(path: &Path) -> bool {
    use std::io::Read;
    let mut header = [0u8; 16];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut header))
        .map(|()| &header == b"SQLite format 3\0")
        .unwrap_or(false)
}

/// A minimal host `--db` file: a valid SQLite database the session mounts as
/// `main` (the CLI establishes every session with `mount!("<db>", "main")`).
fn host_db(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("host.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("PRAGMA user_version = 0;").unwrap();
    db
}

/// mount_new! PROVISIONS a fresh empty database, then a SECOND session
/// mount!-attaches the same file — the create-then-attach round-trip that
/// justifies splitting mount!/mount_new! (EFFECT-ALGEBRA §6).
#[test]
fn mount_new_then_mount_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let host = host_db(dir.path());
    let host_str = host.to_str().unwrap();
    let fresh = dir.path().join("fresh.db");
    assert!(!fresh.exists(), "fresh.db must start missing");

    // Session 1: provision fresh.db.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        host_str,
        r#"mount_new!("fresh.db", "f")"#,
    );
    assert!(
        ok,
        "mount_new! should succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('f'),
        "mount_new! should echo the bound namespace.\nstdout:\n{stdout}"
    );

    // The FILE now exists and is a valid SQLite database (not a 0-byte stub).
    assert!(fresh.exists(), "mount_new! must materialize fresh.db");
    let len = std::fs::metadata(&fresh).unwrap().len();
    assert!(len > 0, "materialized db must be non-empty, got {len} bytes");
    assert!(
        is_valid_sqlite(&fresh),
        "materialized db must carry the SQLite header"
    );

    // Session 2: mount! the same file. mount!'s attach-only guard rejects
    // missing/empty/invalid paths, so success proves the round-trip.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        host_str,
        r#"mount!("fresh.db", "f")"#,
    );
    assert!(
        ok,
        "mount! of the provisioned file must succeed (create-then-attach).\n\
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// CLOBBER (EFFECT-ALGEBRA §6, refuse-over-clobber): mount_new! on a path
/// holding a real database refuses with the teaching substring, and the
/// existing database is left untouched.
#[test]
fn mount_new_refuses_to_clobber() {
    let dir = tempfile::tempdir().unwrap();
    let host = host_db(dir.path());
    let host_str = host.to_str().unwrap();

    // A real db with content at the target path.
    let occupied = dir.path().join("occupied.db");
    {
        let conn = rusqlite::Connection::open(&occupied).unwrap();
        conn.execute_batch("CREATE TABLE t (a INTEGER); INSERT INTO t VALUES (7);")
            .unwrap();
    }
    let before = std::fs::read(&occupied).unwrap();

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        host_str,
        r#"mount_new!("occupied.db", "occ")"#,
    );
    assert!(
        !ok,
        "mount_new! must refuse to clobber a non-empty path.\nstdout:\n{stdout}"
    );
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("already exists; use mount!() to attach it"),
        "clobber refusal must teach mount!().\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The existing database is byte-for-byte untouched.
    let after = std::fs::read(&occupied).unwrap();
    assert_eq!(before, after, "existing db must survive a clobber refusal");
}
