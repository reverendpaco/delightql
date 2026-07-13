// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `mount!` is attach-only (bugs/nullmount Phase 1; EFFECT-ALGEBRA §6).
//!
//! `mount!(path, ns)` attaches an EXISTING, valid SQLite database and rejects
//! a missing, empty (0-byte / `/dev/null`), or non-SQLite target. Create
//! intent is `mount_new!`'s; the CLI's `--make-new-db-if-missing` routes a
//! missing/empty `--db` target through `mount_new!` so a VALID empty database
//! is materialized before the session's primary mount.
//!
//! Why an integration test and not a ball: the failure surfaces at the `dql`
//! process boundary (a non-zero exit + the message substring on stderr), and
//! the create path is a CROSS-INVOCATION property — a first `dql` provisions
//! a fresh db and a SECOND `dql` reopens the same file. A ball runs inside one
//! session and cannot observe either.
//!
//! RED-BEFORE (pre-change binary, lenient `bytes_read > 0 &&` guard):
//! `mount_of_an_empty_file_errors` and `mount_of_dev_null_errors` FAIL — the
//! empty/`/dev/null` mount SUCCEEDED (asserted `!ok`, got `ok`). Verified by
//! reverting the system.rs guard, rebuilding `--bin dql`, and running this
//! file (both go RED); restoring the guard turns them GREEN.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query` with `stdin = query`, cwd = `dir`, against `db`.
/// `make_new` toggles `--make-new-db-if-missing`.
fn run_dql(dir: &Path, db: &str, query: &str, make_new: bool) -> (bool, String, String) {
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
    if make_new {
        cmd.arg("--make-new-db-if-missing");
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

/// A valid empty SQLite database carries the 16-byte header magic.
fn is_valid_sqlite(path: &Path) -> bool {
    use std::io::Read;
    let mut header = [0u8; 16];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut header))
        .map(|()| &header == b"SQLite format 3\0")
        .unwrap_or(false)
}

/// A minimal valid host `--db` file the session mounts as `main` (the CLI
/// establishes every session with `mount!("<db>", "main")`). It is a real,
/// non-empty SQLite database so the session starts cleanly and the in-query
/// `mount!(...)` under test is what fails or succeeds.
fn host_db(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("host.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("PRAGMA user_version = 0;").unwrap();
    db
}

const INVALID_SUBSTR: &str = "is not a valid SQLite database";

/// An empty (0-byte) file is not a valid SQLite database: mount! rejects it.
#[test]
fn mount_of_an_empty_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let host = host_db(dir.path());
    // A 0-byte stub — the exact idiom mount! used to accept.
    std::fs::write(dir.path().join("empty.db"), b"").unwrap();

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        host.to_str().unwrap(),
        r#"mount!("empty.db", "x")"#,
        false,
    );
    assert!(!ok, "mount! of a 0-byte file must error.\nstdout:\n{stdout}");
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains(INVALID_SUBSTR),
        "expected '{INVALID_SUBSTR}'.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// `/dev/null` reads as 0 bytes — the sharp bugs/nullmount repro.
#[test]
fn mount_of_dev_null_errors() {
    let dir = tempfile::tempdir().unwrap();
    let host = host_db(dir.path());

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        host.to_str().unwrap(),
        r#"mount!("/dev/null", "x")"#,
        false,
    );
    assert!(!ok, "mount! of /dev/null must error.\nstdout:\n{stdout}");
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains(INVALID_SUBSTR),
        "expected '{INVALID_SUBSTR}'.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// A path that does not exist: mount! is attach-only (pre-existing guard).
#[test]
fn mount_of_a_nonexistent_path_errors() {
    let dir = tempfile::tempdir().unwrap();
    let host = host_db(dir.path());

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        host.to_str().unwrap(),
        r#"mount!("nope.db", "x")"#,
        false,
    );
    assert!(!ok, "mount! of a missing path must error.\nstdout:\n{stdout}");
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("does not exist"),
        "expected a does-not-exist message.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// CONTROL: a real, valid SQLite database still mounts.
#[test]
fn mount_of_a_valid_file_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let host = host_db(dir.path());
    // A second valid db to mount from within the session.
    let other = dir.path().join("other.db");
    rusqlite::Connection::open(&other)
        .unwrap()
        .execute_batch("PRAGMA user_version = 0;")
        .unwrap();

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        host.to_str().unwrap(),
        r#"mount!("other.db", "x")"#,
        false,
    );
    assert!(
        ok,
        "mount! of a valid SQLite db must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// The CREATE path: `--make-new-db-if-missing` provisions a fresh VALID
/// database for a missing `--db` target (routing the primary mount through
/// mount_new!), the session writes a durable table, and a SECOND invocation
/// reopens the same file and reads it back.
#[test]
fn make_new_db_if_missing_creates_and_works() {
    let dir = tempfile::tempdir().unwrap();
    let new_db = dir.path().join("fresh.db");
    assert!(!new_db.exists(), "fresh.db must start missing");

    // Session 1: provision + durable write against the fresh main.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        new_db.to_str().unwrap(),
        r#"_(a, b @ 1, 2; 3, 4) |> table!(t)"#,
        true,
    );
    assert!(
        ok,
        "--make-new-db-if-missing create path must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("table!"),
        "expected a table! receipt.\nstdout:\n{stdout}"
    );

    // The file materialized as a real SQLite database (not a 0-byte stub).
    assert!(new_db.exists(), "the create path must materialize fresh.db");
    assert!(
        std::fs::metadata(&new_db).unwrap().len() > 0,
        "materialized db must be non-empty"
    );
    assert!(
        is_valid_sqlite(&new_db),
        "materialized db must carry the SQLite header"
    );

    // Session 2: reopen the same file (plain mount!, no create flag) and read
    // the durable table back — proof the create path produced a real, usable
    // database that mount!'s attach-only guard accepts.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        new_db.to_str().unwrap(),
        r#"main.t(*)"#,
        false,
    );
    assert!(
        ok,
        "reopening the provisioned db must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('1') && stdout.contains('4'),
        "reopened db must hold the durable rows.\nstdout:\n{stdout}"
    );
}
