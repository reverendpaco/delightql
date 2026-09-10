// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Blueprint inertness at the materialization entrances — the DATA half.
//!
//! `imprint!` consumes its source into an inert `{target}::_N_blueprint`
//! archive. Re-imprinting that archive, or replacing from it, must refuse
//! — and a refusal is only proof of inertness if the target it would have
//! written stays byte-for-byte untouched. The ball runner aborts a
//! sequence at its first error, so the corpus pins the refusal; these
//! tests drive the real `dql` binary against persisted files and inspect
//! the files afterwards, the only way to see what the refusal left behind.
//!
//! Companion pins: blind_deep_20260907 cells 11/12 (the refusals),
//! companion_linear--78/--80 (a source, and a target, nested inside the
//! archive), --79/--81 (positive controls).

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

fn write(path: &Path, contents: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

const ITEMS_LIB: &str = "items(*) :- _(x @ 1;2;3)\n\
    (~~ddl:\"_internal\"\n\
    imprinting(*) :- _(entity,materialization,extent @ \"items\",\"table\",\"permanent\")\n\
    ~~)\n";

const SUBS_LIB: &str = "subs(*) :- _(y @ 7;8)\n\
    (~~ddl:\"_internal\"\n\
    imprinting(*) :- _(entity,materialization,extent @ \"subs\",\"table\",\"permanent\")\n\
    ~~)\n";

/// Seed `main.sqlite` with a marker row that no imprint may disturb.
fn seed_main(dir: &Path) {
    let conn = rusqlite::Connection::open(dir.join("main.sqlite")).unwrap();
    conn.execute_batch("CREATE TABLE marker (v INTEGER); INSERT INTO marker VALUES (99);")
        .unwrap();
}

/// Every user object in a database file, by (type, name).
fn objects(db: &Path) -> Vec<(String, String)> {
    let conn = rusqlite::Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT type, name FROM sqlite_master \
             WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
        )
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn ints(db: &Path, sql: &str) -> Vec<i64> {
    let conn = rusqlite::Connection::open(db).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

/// The main-side state a lawful imprint leaves and a refused re-imprint
/// must preserve: the marker row and the materialized `items` rows.
fn assert_main_intact(db: &Path) {
    assert_eq!(
        objects(db),
        vec![
            ("table".to_string(), "items".to_string()),
            ("table".to_string(), "marker".to_string()),
        ],
        "main must hold exactly the marker and the lawfully imprinted items"
    );
    assert_eq!(ints(db, "SELECT x FROM items ORDER BY x"), vec![1, 2, 3]);
    assert_eq!(ints(db, "SELECT v FROM marker"), vec![99]);
}

/// Re-imprinting the archive into a FRESH target must refuse with the
/// inertness badge and leave the fresh target empty. A fresh target is
/// the discriminating input: against `main` a table-exists clash would
/// refuse for the wrong reason.
///
/// RED-BEFORE: the source admission excluded data/system/container kinds
/// and never asked the inertness authority, so the archive root (kind
/// `blueprint`) materialized `items` with rows 1,2,3 into the fresh file.
#[test]
fn refused_reimprint_leaves_the_fresh_target_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    seed_main(dir);
    write(&dir.join("ddl/items.dql"), ITEMS_LIB);

    let (ok, _out, err) = run_dql(
        dir,
        "main.sqlite",
        "consult!(\"ddl/items.dql\", \"blue\")(*)\n\
         imprint!(\"blue\", \"main\")(*)\n\
         mount_new!(\"second.sqlite\", \"second\")(*)\n\
         imprint!(\"main::_0_blueprint\", \"second\")(*)\n",
    );
    assert!(!ok, "re-imprinting the archive must fail; stderr:\n{err}");
    assert!(
        err.contains("imprint/blueprint/inert"),
        "the refusal must carry the inertness badge; stderr:\n{err}"
    );

    let second = dir.join("second.sqlite");
    assert!(
        second.exists(),
        "mount_new! created the fresh target before the refusal"
    );
    assert!(
        objects(&second).is_empty(),
        "the refused imprint must create nothing in the fresh target, found {:?}",
        objects(&second)
    );
    assert_main_intact(&dir.join("main.sqlite"));
}

/// `imprint_replace!` from the archive back into its own target must refuse
/// without dropping or recreating anything: the lawfully imprinted table
/// keeps its rows, and the marker survives.
///
/// RED-BEFORE: replace-mode admitted the archive as a source, dropped
/// `items`, recreated it from the archived rule, and re-archived the
/// archive as `main::_1_blueprint`.
#[test]
fn refused_replace_leaves_the_target_data_untouched() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    seed_main(dir);
    write(&dir.join("ddl/items.dql"), ITEMS_LIB);

    let (ok, _out, err) = run_dql(
        dir,
        "main.sqlite",
        "consult!(\"ddl/items.dql\", \"blue\")(*)\n\
         imprint!(\"blue\", \"main\")(*)\n\
         imprint_replace!(\"main::_0_blueprint\", \"main\")(*)\n",
    );
    assert!(!ok, "replacing from the archive must fail; stderr:\n{err}");
    assert!(
        err.contains("imprint/blueprint/inert"),
        "the refusal must carry the inertness badge; stderr:\n{err}"
    );
    assert_main_intact(&dir.join("main.sqlite"));
}

/// Containment: a library consulted UNDER the source (`blue::sub`) is
/// relocated into the archive with it and keeps its own `lib` kind. It is
/// inert by ancestry, and imprinting it into a fresh target must refuse
/// and create nothing there.
///
/// RED-BEFORE: the child passed the kind check, materialized `subs` with
/// rows 7,8 into the fresh file, and was moved out of the archive under a
/// blueprint of the fresh target.
#[test]
fn refused_nested_source_leaves_the_fresh_target_empty() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    seed_main(dir);
    write(&dir.join("ddl/items.dql"), ITEMS_LIB);
    write(&dir.join("ddl/sub.dql"), SUBS_LIB);

    let (ok, _out, err) = run_dql(
        dir,
        "main.sqlite",
        "consult!(\"ddl/items.dql\", \"blue\")(*)\n\
         consult!(\"ddl/sub.dql\", \"blue::sub\")(*)\n\
         imprint!(\"blue\", \"main\")(*)\n\
         mount_new!(\"second.sqlite\", \"second\")(*)\n\
         imprint!(\"main::_0_blueprint::sub\", \"second\")(*)\n",
    );
    assert!(
        !ok,
        "imprinting a source inside the archive must fail; stderr:\n{err}"
    );
    assert!(
        err.contains("imprint/blueprint/inert"),
        "the refusal must carry the inertness badge; stderr:\n{err}"
    );
    assert!(
        objects(&dir.join("second.sqlite")).is_empty(),
        "the refused imprint must create nothing in the fresh target"
    );
    assert_main_intact(&dir.join("main.sqlite"));
}

/// Positive control: the same library, consulted fresh at the vacated
/// path, imprints into the fresh target. A blanket prohibition on
/// imprinting cannot pass this beside the refusals above.
#[test]
fn a_live_library_still_imprints_into_the_fresh_target() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    seed_main(dir);
    write(&dir.join("ddl/items.dql"), ITEMS_LIB);

    let (ok, _out, err) = run_dql(
        dir,
        "main.sqlite",
        "consult!(\"ddl/items.dql\", \"blue\")(*)\n\
         imprint!(\"blue\", \"main\")(*)\n\
         mount_new!(\"second.sqlite\", \"second\")(*)\n\
         consult!(\"ddl/items.dql\", \"blue\")(*)\n\
         imprint!(\"blue\", \"second\")(*)\n",
    );
    assert!(
        ok,
        "a re-consulted live library must imprint; stderr:\n{err}"
    );
    let second = dir.join("second.sqlite");
    assert_eq!(
        objects(&second),
        vec![("table".to_string(), "items".to_string())]
    );
    assert_eq!(
        ints(&second, "SELECT x FROM items ORDER BY x"),
        vec![1, 2, 3]
    );
    assert_main_intact(&dir.join("main.sqlite"));
}
