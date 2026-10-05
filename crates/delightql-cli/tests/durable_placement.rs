// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `table!` DURABLE PLACEMENT + cross-kind temp replace.
//!
//! Why integration tests and not balls: persistence is a CROSS-SESSION
//! property — the object must survive the process that created it and be
//! visible to a second `dql` invocation reopening the same `--db` FILE. A
//! ball runs inside one session; it structurally cannot observe what the
//! file holds after exit, so a ball cannot catch a `table!` that answers
//! success while the CTAS lands in the ephemeral `:memory:` primary rather
//! than the mounted file. The cross-kind
//! tests need the SESSION CATALOG between two plans on one session
//! (`--sequential`), which run-one ball statements also exercise; the
//! raw engine error ("use DROP TABLE to delete table …") is only
//! observable at this level.
//!
//! Connection attribution + EFFECT-ALGEBRA §3
//! (temp replacement is by NAME, not kind).

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

/// Run `dql query` with `stdin = query` and no `--db`: `main` is backed by
/// the session's in-memory primary.
fn run_dql_without_db(dir: &Path, query: &str) -> (bool, String, String) {
    let mut child = Command::new(dql_bin())
        .arg("query")
        .arg("--to")
        .arg("results")
        .arg("--sequential")
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

/// The persistence pin: without durable placement, invocation 1
/// answers a success receipt while the file holds no `archived` (the
/// CTAS lands in the ephemeral `:memory:` primary) and invocation 2
/// dies "Table not found: archived".
///
/// `table!` is the DURABLE analog of `temp_table!` (materialize-pipe §1):
/// an unqualified target is the session's default write target, `main`, so
/// the CTAS lands in main's backing — the mounted `--db` file — and survives
/// the session.
#[test]
fn table_bang_persists_to_the_db_file_across_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let db_str = db.to_str().unwrap();

    // Session 1: create, read back in-session, exit.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        "orders(*), region = \"EU\" |> table!(archived(*))(*)",
        false,
    );
    assert!(
        ok,
        "table! should succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
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
             materialize-pipe §2/§3) — placing it in the \
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

/// Cross-kind temp replace, direction 1 (EFFECT-ALGEBRA §3:
/// replacement is by NAME, not kind). A temp view over an
/// existing temp TABLE drops the table first. A replace drop
/// kind-matched to the DIRECTIVE instead of the holder misbinds:
/// `Error: Connection: use DROP TABLE to delete table sw` (raw engine
/// error).
#[test]
fn temp_view_over_temp_table_replaces_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "orders(*) |> temp_table!(sw(*))(*)\n\n\
         orders(*), region = \"EU\" |> temp_view!(sw(*))(*)\n\n\
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
/// temp VIEW drops the view first (otherwise the engine dies
/// `Error: Connection: use DROP VIEW to delete view sw`).
#[test]
fn temp_table_over_temp_view_replaces_the_view() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "orders(*) |> temp_view!(sw(*))(*)\n\n\
         orders(*), region = \"US\" |> temp_table!(sw(*))(*)\n\n\
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

/// Without `--db`, `main` is backed by the in-memory primary: a session
/// object is created in its temp schema and read back through its recorded
/// placement, never re-derived from a mount `main` does not have (R9).
#[test]
fn temp_create_then_read_without_db() {
    let dir = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run_dql_without_db(
        dir.path(),
        "_(v @ 7) |> temp_table!(staged(*))(*)\n\nstaged(*)",
    );
    assert!(
        ok,
        "temp create-then-read must work without --db.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(stdout.trim(), "v\n7", "stdout:\n{stdout}");
}

/// The same with a durable `table!` first: it creates an ordinary object in
/// the in-memory primary, and the durable and session objects of one name
/// stay two identities — the exact durable route reads the durable one even
/// though the temp schema holds the name (R9, R5).
#[test]
fn durable_then_temp_create_then_read_without_db() {
    let dir = tempfile::tempdir().unwrap();
    for (read, expected) in [
        ("d(*) ; staged(*)", "v\n1\n2"),
        ("main.t(*)", "v\n3"),
        ("sys::shadow::main.t(*)", "v\n4"),
        ("t(*)", "v\n4"),
    ] {
        let (ok, stdout, stderr) = run_dql_without_db(
            dir.path(),
            &format!(
                "_(v @ 1) |> table!(d(*))(*)\n\n\
                 _(v @ 2) |> temp_table!(staged(*))(*)\n\n\
                 _(v @ 3) |> table!(t(*))(*)\n\n\
                 _(v @ 4) |> temp_table!(t(*))(*)\n\n\
                 {read}"
            ),
        );
        assert!(
            ok,
            "{read}: creation and read must work without --db.\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        assert_eq!(stdout.trim(), expected, "{read}: stdout:\n{stdout}");
    }
}

/// A qualified target places the object in its own namespace's file, even
/// though that file shares the primary connection with `main`; the `--db`
/// file never receives it (F26).
#[test]
fn qualified_table_bang_lands_in_the_mounted_file_across_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let other = dir.path().join("other.db");
    rusqlite::Connection::open(&other)
        .unwrap()
        .execute_batch("CREATE TABLE anchor (v INTEGER);")
        .unwrap();
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "mount!(\"other.db\", \"data::n\")(*)\n\n\
         orders(*), region = \"EU\" |> table!(data::n.kept(*))(*)",
        true,
    );
    assert!(
        ok,
        "a qualified table! should succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("data::n.kept"),
        "the receipt names the selected target.\nstdout:\n{stdout}"
    );
    let holds = |path: &Path| -> i64 {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'kept'",
                [],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(holds(&other), 1, "the mounted file holds the object");
    assert_eq!(holds(&db), 0, "the --db file does not");
}

/// Unmounting a session object's durable owner on the primary connection
/// leaves the connection's temp pool alive: the object stays exactly
/// readable at its shadow path, and a later owner of the name is refused
/// with the holder named.
#[test]
fn a_session_object_outlives_its_unmounted_owner_on_the_primary() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    for (name, rows) in [
        ("n.db", "seed(v INTEGER); INSERT INTO seed VALUES (7)"),
        ("k.db", "anchor(v INTEGER)"),
    ] {
        rusqlite::Connection::open(dir.path().join(name))
            .unwrap()
            .execute_batch(&format!("CREATE TABLE {rows};"))
            .unwrap();
    }
    let prefix = "mount!(\"n.db\", \"data::n\")(*)\n\n\
                  mount!(\"k.db\", \"data::k\")(*)\n\n\
                  data::n.seed(*) |> temp_table!(data::n.t(*))(*)\n\n\
                  unmount!(\"data::n\")(*)\n\n";

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        &format!("{prefix}sys::shadow::main.t(*)"),
        true,
    );
    assert!(
        ok,
        "the owner unmounts and the object reads.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(stdout.trim(), "v\n7", "stdout:\n{stdout}");

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        &format!("{prefix}_(v @ 1) |> temp_table!(data::k.t(*))(*)"),
        true,
    );
    assert!(
        !ok,
        "a new owner must not take the name.\nstdout:\n{stdout}"
    );
    assert!(
        stderr.contains("semantic/effect/ddl/temp_name_held")
            && stderr.contains("sys::shadow::main.t")
            && stderr.contains("no longer in the catalog"),
        "stderr:\n{stderr}"
    );
}

/// An immutable embedded image is not data backing a creation may write:
/// `cli::surface` refuses rather than landing the object in `main` (R8).
#[test]
fn cli_surface_is_not_a_creation_target() {
    let dir = tempfile::tempdir().unwrap();
    let db = fixture(dir.path());
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "_(v @ 1) |> temp_table!(cli::surface.zz(*))(*)",
        false,
    );
    assert!(!ok, "the creation must refuse.\nstdout:\n{stdout}");
    assert!(
        stderr.contains("semantic/effect/ddl/target_namespace")
            && stderr.contains("immutable embedded image"),
        "stderr:\n{stderr}"
    );
}
