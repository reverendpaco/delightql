// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Imprint atomicity: the transaction wrapped around
//! `imprint_namespace`'s target-connection work.
//!
//! Why an integration test and not a companion ball: the ball runner executes a
//! test's DQL statements as a sequence and aborts on the first error (the
//! failing `imprint_replace!`), and each ball run gets a fresh copy of the DB —
//! so a trailing "did the survivor survive?" query can never run in the same
//! ball. Here we drive the real `dql` binary against a *persisted* file across
//! two invocations and then inspect that file directly, which is the only way
//! to observe the residual DB state after a failed imprint.
//!
//! companion_linear--66 / --67 pin the clean *error*; these tests pin the
//! *data*: replace-mode never destroys the survivor, and strict-mode never
//! creates a sibling before refusing on a clash.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query` with `stdin = query`, cwd = `dir`, against `db`.
/// Returns (success, stdout, stderr).
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

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn table_exists(db: &Path, name: &str) -> bool {
    let conn = rusqlite::Connection::open(db).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table','view') AND name = ?1",
        [name],
        |r| r.get::<_, i64>(0),
    )
    .unwrap()
        > 0
}

/// C1: a partial-failure `imprint_replace!` must not destroy the survivor.
///
/// Manifest of two entities. `kept` pre-exists in the target with distinctive
/// rows, so replace-mode drops it up front. `broken` (listed first) creates
/// fine but its CTAS INSERT violates a PRIMARY KEY — a failure at exec time,
/// after `kept` is gone. If the drop commits first, `kept`
/// destroyed. Post-fix: the whole target txn rolls back → `kept` intact,
/// `broken` absent.
#[test]
fn replace_partial_failure_preserves_survivor() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let db = dir.join("main.sqlite");

    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
             INSERT INTO employees VALUES (1,'Ada',35),(2,'Bob',25),(3,'Carol',42);
             CREATE TABLE kept (tag TEXT);
             INSERT INTO kept VALUES ('ORIG-A'),('ORIG-B'),('ORIG-C');",
        )
        .unwrap();
    }

    write(
        &dir.join("ddl/lib.dql"),
        "broken(*) :- employees(*) |> (1 as k)\n\
         kept(*)   :- employees(*), age >= 30\n\
         (~~ddl:\"_internal\"\n\
         schema(\"broken\")(name, type) :-\n\
           _(name, type\n\
             -----------\n\
             \"k\",  \"INTEGER\")\n\
         constraints(\"broken\")(column, constraint, constraint_name) :-\n\
           _(column, constraint, constraint_name\n\
             ------------------------------------\n\
             \"k\",    \"%%\",       \"pk_broken\")\n\
         imprinting(entity, materialization, extent) :-\n\
           _(entity, materialization, extent\n\
             ---------------------------------\n\
             \"broken\", \"table\", \"permanent\" ;\n\
             \"kept\",   \"table\", \"permanent\")\n\
         ~~)\n",
    );

    // Invocation 1: the imprint_replace! must FAIL (broken can't materialize).
    let (ok, _out, err) = run_dql(
        dir,
        "main.sqlite",
        "consult!(\"ddl/lib.dql\", \"lib::a\")(*)\nimprint_replace!(\"lib::a\", \"main\")(*)\n",
        true,
    );
    assert!(!ok, "imprint_replace! should fail; stderr:\n{err}");
    assert!(
        err.contains("broken") || err.contains("UNIQUE"),
        "failure should name the failing entity/constraint; stderr:\n{err}"
    );

    // The survivor's ORIGINAL rows must still be present, and the half-built
    // `broken` table must have been rolled back.
    let conn = rusqlite::Connection::open(&db).unwrap();
    let mut rows: Vec<String> = conn
        .prepare("SELECT tag FROM kept ORDER BY tag")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec!["ORIG-A", "ORIG-B", "ORIG-C"],
        "kept must retain its original rows after the failed replace"
    );
    assert!(
        !table_exists(&db, "broken"),
        "the half-built `broken` table must have been rolled back"
    );
}

/// M6b: a strict `imprint!` over a multi-entity manifest that clashes must
/// refuse WITHOUT creating the non-clashing sibling (pre-flight is
/// atomic-by-construction). Pins the "A was not created" half that the ball
/// cannot observe.
#[test]
fn strict_multi_entity_clash_leaves_target_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let db = dir.join("main.sqlite");

    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
             INSERT INTO employees VALUES (1,'Ada',35),(2,'Bob',25),(3,'Carol',42);
             CREATE TABLE clash (existing TEXT);
             INSERT INTO clash VALUES ('PRE-EXISTING');",
        )
        .unwrap();
    }

    write(
        &dir.join("ddl/lib.dql"),
        "fresh(*) :- employees(*), age >= 30\n\
         clash(*) :- employees(*), age >= 40\n\
         (~~ddl:\"_internal\"\n\
         imprinting(entity, materialization, extent) :-\n\
           _(entity, materialization, extent\n\
             ---------------------------------\n\
             \"fresh\", \"table\", \"permanent\" ;\n\
             \"clash\", \"table\", \"permanent\")\n\
         ~~)\n",
    );

    let (ok, _out, err) = run_dql(
        dir,
        "main.sqlite",
        "consult!(\"ddl/lib.dql\", \"lib::a\")(*)\nimprint!(\"lib::a\", \"main\")(*)\n",
        true,
    );
    assert!(!ok, "strict imprint! over a clash should fail; stderr:\n{err}");
    assert!(
        err.contains("already exist"),
        "failure should report the clash; stderr:\n{err}"
    );

    // The non-clashing sibling must never have been created…
    assert!(
        !table_exists(&db, "fresh"),
        "entity `fresh` must NOT be created when a sibling clashes"
    );
    // …and the pre-existing clash table must be untouched.
    let conn = rusqlite::Connection::open(&db).unwrap();
    let val: String = conn
        .query_row("SELECT existing FROM clash", [], |r| r.get(0))
        .unwrap();
    assert_eq!(val, "PRE-EXISTING", "the clashing table must be untouched");
}
