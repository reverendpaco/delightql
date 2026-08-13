// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `mount!` is attach-only (EFFECT-ALGEBRA §6).
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

/// The same, `--sequential`: several statements on one session, which is the
/// only way a mount and a write beside it can meet.
fn run_dql_sequential(dir: &Path, db: &str, query: &str) -> (bool, String, String) {
    let mut cmd = Command::new(dql_bin());
    cmd.arg("query")
        .arg("--db")
        .arg(db)
        .arg("--to")
        .arg("results")
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
        r#"mount!("empty.db", "x")(*)"#,
        false,
    );
    assert!(
        !ok,
        "mount! of a 0-byte file must error.\nstdout:\n{stdout}"
    );
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
        r#"mount!("/dev/null", "x")(*)"#,
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
        r#"mount!("nope.db", "x")(*)"#,
        false,
    );
    assert!(
        !ok,
        "mount! of a missing path must error.\nstdout:\n{stdout}"
    );
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
        r#"mount!("other.db", "x")(*)"#,
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
        r#"_(a, b @ 1, 2; 3, 4) |> table!(t(*))(*)"#,
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
    let (ok, stdout, stderr) = run_dql(dir.path(), new_db.to_str().unwrap(), r#"main.t(*)"#, false);
    assert!(
        ok,
        "reopening the provisioned db must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('1') && stdout.contains('4'),
        "reopened db must hold the durable rows.\nstdout:\n{stdout}"
    );
}

/// Naming one file twice must not OPEN it twice.
///
/// The session's `--db` is already mounted as `main`. Mounting the same file
/// again under another name binds the schema the connection already holds
/// it under; attaching a second handle gives one connection two independent
/// pagers on one file, and a write through either then reports "database is
/// locked" from a statement with no second party anywhere in it.
///
/// Why an integration test and not a ball: the primary database is the
/// session's, established by the CLI before any query runs, so only a `dql`
/// invocation can put the same file on both sides.
///
/// RED-BEFORE: with the reuse removed, this fails on "database is locked".
#[test]
fn mounting_the_session_database_again_does_not_open_it_twice() {
    let dir = tempfile::tempdir().unwrap();
    let host = dir.path().join("host.db");
    let conn = rusqlite::Connection::open(&host).unwrap();
    conn.execute_batch(
        "CREATE TABLE t (k INTEGER NOT NULL, v TEXT NOT NULL); INSERT INTO t VALUES (1, 'before');",
    )
    .unwrap();
    drop(conn);

    let (ok, stdout, stderr) = run_dql_sequential(
        dir.path(),
        "host.db",
        "mount!(\"host.db\", \"twin\")(*)\n\
         t!!(*) |> $$(\"after\" as v) |> update!(t(*))(*)\n\
         t(*)\n",
    );
    let combined = format!("{stdout}{stderr}");
    assert!(
        ok,
        "a write beside a second name for the session's own database must not \
         deadlock.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        combined.contains("after"),
        "the update must have landed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// Unmounting a BORROWED schema does not close it.
///
/// The session's `--db` is already open; a second name for it borrows that
/// schema rather than attaching another handle. Ownership decides whether a
/// disappearing binding may `DETACH`, and refcounting decides only whether
/// anyone is still using it — two questions, and neither answers the other.
/// A borrowed schema may be SQLite's own `main`, which cannot be detached at
/// all; the law is ownership, never the spelling of the alias.
///
/// RED-BEFORE: the binding's alias reached physical cleanup with no record of
/// who opened it, so the last name to go detached a schema it never attached.
#[test]
fn unmounting_a_borrowed_schema_leaves_it_attached() {
    let dir = tempfile::tempdir().unwrap();
    let conn = rusqlite::Connection::open(dir.path().join("host.db")).unwrap();
    conn.execute_batch("CREATE TABLE t (k INTEGER NOT NULL); INSERT INTO t VALUES (1);")
        .unwrap();
    drop(conn);

    let (ok, stdout, stderr) = run_dql_sequential(
        dir.path(),
        "host.db",
        "mount!(\"host.db\", \"twin\")(*)\n\
         unmount!(\"twin\")(*)\n\
         t(*)\n",
    );
    assert!(
        ok,
        "unmounting a second name for the session's own database must \
         succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('1'),
        "the session's own database must still be readable after the borrowed \
         name goes.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// Two names for one file, through the binary: both mount, the first one out
/// leaves the second standing, and the file is usable again afterwards.
///
/// What this does NOT show is the physical detach. Mounting the file again
/// succeeds whether the last owner closed the attachment or leaked it —
/// a leaked schema is exactly what the reuse road looks for. That proof
/// needs the connection itself and lives in core's
/// `the_last_owner_out_closes_the_attachment_it_opened`.
#[test]
fn one_file_two_names_survives_the_first_unmount() {
    let dir = tempfile::tempdir().unwrap();
    let host = rusqlite::Connection::open(dir.path().join("host.db")).unwrap();
    host.execute_batch("PRAGMA user_version = 0;").unwrap();
    drop(host);
    let side = rusqlite::Connection::open(dir.path().join("side.db")).unwrap();
    side.execute_batch("CREATE TABLE s (k INTEGER NOT NULL); INSERT INTO s VALUES (7);")
        .unwrap();
    drop(side);

    // The first binding owns the attachment; the second borrows it.
    let (ok, stdout, stderr) = run_dql_sequential(
        dir.path(),
        "host.db",
        "mount!(\"side.db\", \"aa\")(*)\n\
         mount!(\"side.db\", \"bb\")(*)\n\
         sys::ns.mount(*)\n",
    );
    assert!(ok, "two names for one file must mount.\nstderr:\n{stderr}");
    assert!(
        stdout.contains("owned") && stdout.contains("borrowed"),
        "the catalog records WHO OPENED the schema.\nstdout:\n{stdout}"
    );

    // The owner goes first: the schema stays attached, and the survivor
    // INHERITS the right to close it. Without the handover the survivor
    // would stay borrowed and the attachment would outlive every name for
    // it — a leak no read can see, which is why the catalog is asked.
    let (ok, stdout, stderr) = run_dql_sequential(
        dir.path(),
        "host.db",
        "mount!(\"side.db\", \"aa\")(*)\n\
         mount!(\"side.db\", \"bb\")(*)\n\
         unmount!(\"aa\")(*)\n\
         bb.s(*)\n",
    );
    assert!(
        ok && stdout.contains('7'),
        "removing one binding must not detach the schema the other is \
         standing on.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    let (ok, stdout, stderr) = run_dql_sequential(
        dir.path(),
        "host.db",
        "mount!(\"side.db\", \"aa\")(*)\n\
         mount!(\"side.db\", \"bb\")(*)\n\
         unmount!(\"aa\")(*)\n\
         sys::ns.mount(*)\n",
    );
    assert!(ok, "the catalog read must succeed.\nstderr:\n{stderr}");
    assert!(
        !stdout.contains("borrowed"),
        "the surviving binding inherits the departed owner's attachment.\n\
         stdout:\n{stdout}"
    );

    // And the file is mountable again once both names are gone.
    let (ok, stdout, stderr) = run_dql_sequential(
        dir.path(),
        "host.db",
        "mount!(\"side.db\", \"aa\")(*)\n\
         mount!(\"side.db\", \"bb\")(*)\n\
         unmount!(\"aa\")(*)\n\
         unmount!(\"bb\")(*)\n\
         mount!(\"side.db\", \"cc\")(*)\n\
         cc.s(*)\n",
    );
    assert!(
        ok && stdout.contains('7'),
        "the file must be mountable again after its last name goes.\n\
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}
