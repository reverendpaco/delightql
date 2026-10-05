// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! An imprinted view is stored in the target database, so it must read the
//! same when that file is opened by a process that never saw the imprint.
//!
//! Why an integration test and not a ball cell: a ball runs every statement
//! in one session, where the target is attached under the session's own
//! alias. A view that names that alias still reads correctly there — even
//! through a second mount of the file — and is malformed for every other
//! reader. Only a fresh connection observes it, so each test imprints in
//! one `dql` process and reads the target from a new SQLite connection and a
//! new `dql` process.
//!
//! Every way a view's body can reach the target's own object — a qualified
//! name, an enlisted name, a free data name, a sibling entity — must store a
//! reference the file resolves on its own. A view that reads another
//! database is refused, and the refusal leaves the target untouched.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query --sequential` with `stdin = query`, cwd = `dir`, against
/// `db`. Returns (success, stdout, stderr).
fn run_dql(dir: &Path, db: &str, query: &str) -> (bool, String, String) {
    let mut child = Command::new(dql_bin())
        .arg("query")
        .arg("--db")
        .arg(db)
        .arg("--sequential")
        .arg("-f")
        .arg("tsv")
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

/// A primary database with a table of its own, a separate target holding
/// `source(17)`, and the blueprint.
fn fixture(blueprint: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    rusqlite::Connection::open(dir.join("primary.sqlite"))
        .unwrap()
        .execute_batch("CREATE TABLE users (id INTEGER); INSERT INTO users VALUES (5);")
        .unwrap();
    rusqlite::Connection::open(dir.join("target.sqlite"))
        .unwrap()
        .execute_batch("CREATE TABLE source (id INTEGER); INSERT INTO source VALUES (17);")
        .unwrap();
    std::fs::create_dir_all(dir.join("ddl")).unwrap();
    std::fs::write(dir.join("ddl/blueprint.dql"), blueprint).unwrap();
    tmp
}

const IMPRINT: &str = "mount!(\"target.sqlite\", \"data::dst\")(*)\n\
                       consult!(\"ddl/blueprint.dql\", \"lib::bp\")(*)\n\
                       imprint!(\"lib::bp\", \"data::dst\")(*)\n";

const VIEW_ONLY: &str = "(~~ddl:\"_internal\"\n\
                         imprinting(*) :- _(entity, materialization, extent @ \"vw\", \"view\", \"permanent\")\n\
                         ~~)\n";

/// Imprint, then read `vw` from a new SQLite connection and a new `dql`
/// process over the target file alone.
fn assert_portable(blueprint: &str) {
    let tmp = fixture(blueprint);
    let dir = tmp.path();
    let (ok, _out, err) = run_dql(dir, "primary.sqlite", IMPRINT);
    assert!(ok, "imprint! should succeed; stderr:\n{err}");

    let conn = rusqlite::Connection::open(dir.join("target.sqlite")).unwrap();
    let stored: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = 'vw'",
            [],
            |row| row.get(0),
        )
        .expect("a fresh connection reads the stored view");
    let ids: Vec<i64> = conn
        .prepare("SELECT id FROM vw")
        .unwrap_or_else(|e| panic!("a fresh connection opens the view: {e}\n{stored}"))
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(ids, vec![17], "the view reads the target's rows\n{stored}");
    drop(conn);

    let (ok, out, err) = run_dql(dir, "target.sqlite", "main.vw(*)\n");
    assert!(
        ok,
        "a fresh dql process opens the target; stderr:\n{err}\n{stored}"
    );
    assert!(
        out.contains("17"),
        "a fresh dql process reads the view:\n{out}"
    );
}

#[test]
fn a_qualified_read_of_the_target_is_stored_portably() {
    assert_portable(&format!("vw(*) :- data::dst.source(*)\n{VIEW_ONLY}"));
}

#[test]
fn an_enlisted_read_of_the_target_is_stored_portably() {
    assert_portable(&format!(
        "?- enlist!(\"data::dst\")(*)\nvw(*) :- source(*)\n{VIEW_ONLY}"
    ));
}

#[test]
fn a_free_data_name_filled_by_the_target_is_stored_portably() {
    assert_portable(&format!("vw(*) :- source(*)\n{VIEW_ONLY}"));
}

#[test]
fn a_read_of_a_sibling_entity_is_stored_portably() {
    assert_portable(
        "src(*) :- source(*)\n\
         vw(*) :- src(*)\n\
         (~~ddl:\"_internal\"\n\
         imprinting(*) :- _(entity, materialization, extent @ \"src\", \"table\", \"permanent\"; \"vw\", \"view\", \"permanent\")\n\
         schema(\"src\" as entity, name, type, ordinal) :- _(name, type, ordinal @ \"id\", \"INTEGER\", 1)\n\
         ~~)\n",
    );
}

/// The target is the session's own database, which the catalog reads
/// unqualified while the imprint creates under its attachment: every reach
/// of it is stored relative to the file.
#[test]
fn a_view_imprinted_into_the_session_database_is_stored_portably() {
    let tmp = fixture(
        "src(*) :- main.source(*)\n\
         vw(*) :- src(*)\n\
         raw(*) :- main.source(*)\n\
         (~~ddl:\"_internal\"\n\
         imprinting(*) :- _(entity, materialization, extent @ \"src\", \"table\", \"permanent\"; \"vw\", \"view\", \"permanent\"; \"raw\", \"view\", \"permanent\")\n\
         schema(\"src\" as entity, name, type, ordinal) :- _(name, type, ordinal @ \"id\", \"INTEGER\", 1)\n\
         ~~)\n",
    );
    let dir = tmp.path();
    let (ok, _out, err) = run_dql(
        dir,
        "target.sqlite",
        "consult!(\"ddl/blueprint.dql\", \"lib::bp\")(*)\nimprint!(\"lib::bp\", \"main\")(*)\n",
    );
    assert!(ok, "imprint! should succeed; stderr:\n{err}");

    let conn = rusqlite::Connection::open(dir.join("target.sqlite")).unwrap();
    for view in ["vw", "raw"] {
        let ids: Vec<i64> = conn
            .prepare(&format!("SELECT id FROM {view}"))
            .unwrap_or_else(|e| panic!("a fresh connection opens {view}: {e}"))
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert_eq!(ids, vec![17], "{view} reads the target's rows");
    }
}

/// A view in the target cannot read the primary database: the imprint is
/// refused, and nothing of it is left in the target file.
#[test]
fn a_view_reading_another_database_is_refused_and_leaves_nothing() {
    let tmp = fixture(&format!("vw(*) :- main.users(*)\n{VIEW_ONLY}"));
    let dir = tmp.path();
    let (ok, _out, err) = run_dql(dir, "primary.sqlite", IMPRINT);
    assert!(
        !ok,
        "imprint! of a cross-database view should fail; stderr:\n{err}"
    );

    let conn = rusqlite::Connection::open(dir.join("target.sqlite")).unwrap();
    let objects: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(
        objects,
        vec!["source"],
        "the refused imprint leaves the target as it was"
    );
}
