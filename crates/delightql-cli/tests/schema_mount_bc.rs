// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! SCHEMA-MOUNT Phases B & C (EFFECTS-ON-TARGETS-PLAN §4.2/§4.3) against
//! LIVE Postgres and DuckDB: fragment-spelled single-schema `mount!` and
//! the whole-database `mount_tree!`, verified through a real dql process, a
//! real fatboy child, and the engine's OWN door (psql / the duckdb CLI).
//!
//! Environment gating mirrors effects_on_targets.rs: PG needs TCP
//! 127.0.0.1:5433 (new_test_suite/sweep.py's `dql-sweep-pg`), the
//! `dql-fatboy-postgres` sibling, and a `psql` on PATH; DuckDB needs the
//! `dql-fatboy-duckdb` sibling and a `duckdb` CLI. Each PG test creates and
//! panic-safely drops its own UNIQUE scratch database. A test SKIPS with an
//! eprintln when its environment is absent.
//!
//! DuckDB note: the duckdb file takes an EXCLUSIVE file lock, so the mount
//! under test must be the ONLY opener — these tests use a throwaway SQLite
//! `--db` and reach the duckdb file solely through `mount!`/`mount_tree!`
//! (one fatboy child). PG has no such constraint.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query` with `stdin = query`, cwd = `dir`, against `db`.
fn run_dql(dir: &Path, db: &str, query: &str, sequential: bool) -> (bool, String, String) {
    let mut cmd = Command::new(dql_bin());
    cmd.arg("query").arg("--db").arg(db).arg("--to").arg("results");
    if sequential {
        cmd.arg("--sequential");
    }
    cmd.current_dir(dir)
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

fn fatboy_present(name: &str) -> bool {
    PathBuf::from(dql_bin())
        .parent()
        .map(|d| d.join(format!("{}{}", name, std::env::consts::EXE_SUFFIX)).is_file())
        .unwrap_or(false)
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

/// A throwaway SQLite `--db` file: a legal session backend that is NOT the
/// target under test (so the target's fatboy child is the sole opener).
fn dummy_sqlite(dir: &Path) -> String {
    let p = dir.join("dummy.db");
    rusqlite::Connection::open(&p)
        .unwrap()
        .execute_batch("CREATE TABLE z (a INTEGER)")
        .unwrap();
    p.to_str().unwrap().to_string()
}

// ── Postgres gating, psql door, panic-safe scratch databases ────────────

const PG_HOST: &str = "127.0.0.1:5433";

fn pg_uri(db: &str) -> String {
    format!("postgres://postgres@{}/{}", PG_HOST, db)
}

fn pg_env_or_skip(test: &str) -> bool {
    let reachable = std::net::TcpStream::connect_timeout(
        &PG_HOST.parse().unwrap(),
        std::time::Duration::from_millis(500),
    )
    .is_ok();
    if !reachable {
        eprintln!("SKIP {}: no PG at {}", test, PG_HOST);
        return false;
    }
    if !fatboy_present("dql-fatboy-postgres") {
        eprintln!("SKIP {}: no dql-fatboy-postgres next to {}", test, dql_bin());
        return false;
    }
    if !cli_present("psql") {
        eprintln!("SKIP {}: no `psql` on PATH", test);
        return false;
    }
    true
}

fn psql(db: &str, sql: &str) {
    let out = Command::new("psql")
        .arg(pg_uri(db))
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-tA")
        .arg("-c")
        .arg(sql)
        .output()
        .expect("run psql");
    assert!(
        out.status.success(),
        "psql failed for: {}\nstderr: {}",
        sql,
        String::from_utf8_lossy(&out.stderr)
    );
}

struct ScratchDb {
    name: String,
}

impl ScratchDb {
    fn create(name: &str) -> ScratchDb {
        psql("postgres", &format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", name));
        psql("postgres", &format!("CREATE DATABASE {}", name));
        ScratchDb { name: name.to_string() }
    }

    fn uri(&self) -> String {
        pg_uri(&self.name)
    }

    fn sql(&self, sql: &str) {
        psql(&self.name, sql);
    }
}

impl Drop for ScratchDb {
    fn drop(&mut self) {
        let _ = Command::new("psql")
            .arg(pg_uri("postgres"))
            .arg("-c")
            .arg(format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// public.t (one row) + sales.t (two rows) + sales.only_sales (one row):
/// the same table NAME lives in two schemas so a fragment/tree mount that
/// binds the WRONG schema is caught by the row values.
fn pg_two_schema_fixture(db: &ScratchDb) {
    db.sql(
        "CREATE TABLE t (id int, tag text); INSERT INTO t VALUES (1,'public-row'); \
         CREATE SCHEMA sales; \
         CREATE TABLE sales.t (id int, tag text); \
         INSERT INTO sales.t VALUES (10,'sales-a'),(11,'sales-b'); \
         CREATE TABLE sales.only_sales (x int); INSERT INTO sales.only_sales VALUES (99);",
    );
}

// ════════════════════════════════════════════════════════════════════════
// Phase B — fragment `mount!`
// ════════════════════════════════════════════════════════════════════════

/// `mount!("pg://…/db#sales", ns)` binds the sales schema: `ns.t` returns
/// SALES' rows and NOT public's (the fragment travels to source_ns and
/// qualifies the read).
#[test]
fn pg_mount_fragment_binds_named_schema() {
    if !pg_env_or_skip("pg_mount_fragment_binds_named_schema") {
        return;
    }
    let db = ScratchDb::create("probe_bc_frag");
    pg_two_schema_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    let q = format!(
        "mount!(\"{}#sales\", \"rep\")\n\nrep.t(*)",
        db.uri()
    );
    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), &q, true);
    assert!(ok, "fragment mount + read must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("sales-a") && stdout.contains("sales-b"),
        "rep.t must read the SALES schema.\nstdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("public-row"),
        "rep.t must NOT read public.t.\nstdout:\n{stdout}"
    );
}

/// A `#schema` that does not exist on the target refuses loudly (R-S4),
/// naming the missing schema — never binds an empty namespace.
#[test]
fn pg_mount_nonexistent_fragment_refuses() {
    if !pg_env_or_skip("pg_mount_nonexistent_fragment_refuses") {
        return;
    }
    let db = ScratchDb::create("probe_bc_ghost");
    pg_two_schema_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    let q = format!("mount!(\"{}#ghost\", \"g\")", db.uri());
    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), &q, false);
    assert!(!ok, "a nonexistent schema must refuse.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stderr.contains("schema 'ghost' does not exist"),
        "the refusal must name the missing schema.\nstderr:\n{stderr}"
    );
}

/// A bare `mount!` (no fragment) still binds public — behavior-identical to
/// Phase A: bare `t(*)` reads public.t.
#[test]
fn pg_bare_mount_binds_public() {
    if !pg_env_or_skip("pg_bare_mount_binds_public") {
        return;
    }
    let db = ScratchDb::create("probe_bc_bare");
    pg_two_schema_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), "t(*)", false);
    assert!(ok, "bare mount must read public.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("public-row") && !stdout.contains("sales-a"),
        "bare mount binds public.\nstdout:\n{stdout}"
    );
}

/// A `#schema` fragment on a SQLite target refuses (R-S5) — SQLite has no
/// schema concept. Driven through a `file://` URI (the fragment surface is
/// a URI feature).
#[test]
fn sqlite_fragment_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let dummy = dummy_sqlite(dir.path());
    // A second real SQLite file, addressed by a file:// URI with a fragment.
    let target = dir.path().join("target.db");
    rusqlite::Connection::open(&target)
        .unwrap()
        .execute_batch("CREATE TABLE q (a INTEGER)")
        .unwrap();
    let q = format!(
        "mount!(\"file://{}#sales\", \"s\")",
        target.to_str().unwrap()
    );
    let (ok, stdout, stderr) = run_dql(dir.path(), &dummy, &q, false);
    assert!(!ok, "a #schema on SQLite must refuse.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stderr.contains("SQLite has no schemas"),
        "the refusal must say SQLite has no schemas.\nstderr:\n{stderr}"
    );
}

// ════════════════════════════════════════════════════════════════════════
// Phase C — `mount_tree!`
// ════════════════════════════════════════════════════════════════════════

/// `mount_tree!("pg://…/db", ns)` binds one sub-namespace per PERSISTENT
/// schema: public, sales, information_schema, pg_catalog are present; the
/// transient pg_temp_*/pg_toast* are ABSENT; the receipt is ONE row whose
/// JSON-array column lists the created sub-namespaces (R-S2/R-S3); and a
/// table in a NON-public schema resolves via `ns::sales.<t>`.
#[test]
fn pg_mount_tree_creates_persistent_subnamespaces() {
    if !pg_env_or_skip("pg_mount_tree_creates_persistent_subnamespaces") {
        return;
    }
    let db = ScratchDb::create("probe_bc_tree");
    pg_two_schema_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    // The one-row receipt.
    let q = format!("mount_tree!(\"{}\", \"allpg\")", db.uri());
    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), &q, false);
    assert!(ok, "mount_tree! must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    for sub in [
        "allpg::public",
        "allpg::sales",
        "allpg::information_schema",
        "allpg::pg_catalog",
    ] {
        assert!(stdout.contains(sub), "receipt must list {sub}.\nstdout:\n{stdout}");
    }
    assert!(
        !stdout.contains("pg_temp") && !stdout.contains("pg_toast"),
        "transient schemas must be excluded (R-S2).\nstdout:\n{stdout}"
    );
    assert!(stdout.contains('['), "sub_namespaces is a JSON array.\nstdout:\n{stdout}");
    // ONE row: exactly one JSON array opener in the output.
    assert_eq!(
        stdout.matches('[').count(),
        1,
        "the receipt is a SINGLE row.\nstdout:\n{stdout}"
    );

    // A NON-public schema resolves via its sub-namespace.
    let q = format!(
        "mount_tree!(\"{}\", \"allpg\")\n\nallpg::sales.only_sales(*)",
        db.uri()
    );
    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), &q, true);
    assert!(ok, "sub-namespace read must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stdout.contains("99"), "allpg::sales.only_sales resolves.\nstdout:\n{stdout}");
}

/// The load-bearing R-S1 property: all sub-namespaces share ONE connection,
/// so a cross-schema read joining `ns::public.t` and `ns::sales.t` compiles
/// as a SINGLE-connection plan (no cross-connection refusal) and executes.
#[test]
fn pg_mount_tree_cross_schema_single_connection() {
    if !pg_env_or_skip("pg_mount_tree_cross_schema_single_connection") {
        return;
    }
    let db = ScratchDb::create("probe_bc_xconn");
    pg_two_schema_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    let q = format!(
        "mount_tree!(\"{}\", \"allpg\")\n\nallpg::public.t(*), allpg::sales.t(*)",
        db.uri()
    );
    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), &q, true);
    assert!(
        ok,
        "a cross-schema join must be a single-connection plan (a cross-connection \
         refusal here is the failure mode).\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("public-row") && stdout.contains("sales-a"),
        "the join joins across schemas on one connection.\nstdout:\n{stdout}"
    );
    assert!(
        !stderr.contains("cross-connection") && !stderr.contains("cross_connection"),
        "no cross-connection refusal.\nstderr:\n{stderr}"
    );
}

/// A SQLite target refuses `mount_tree!` cleanly (R-S5).
#[test]
fn sqlite_mount_tree_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let dummy = dummy_sqlite(dir.path());
    let target = dir.path().join("target.db");
    rusqlite::Connection::open(&target)
        .unwrap()
        .execute_batch("CREATE TABLE q (a INTEGER)")
        .unwrap();
    let q = format!("mount_tree!(\"{}\", \"tree\")", target.to_str().unwrap());
    let (ok, stdout, stderr) = run_dql(dir.path(), &dummy, &q, false);
    assert!(!ok, "mount_tree! on SQLite must refuse.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stderr.contains("SQLite has no schemas"),
        "the refusal must say SQLite has no schemas.\nstderr:\n{stderr}"
    );
}

// ════════════════════════════════════════════════════════════════════════
// DuckDB — fragment mount and mount_tree (throwaway SQLite --db)
// ════════════════════════════════════════════════════════════════════════

fn duckdb_env_or_skip(test: &str) -> bool {
    if !fatboy_present("dql-fatboy-duckdb") {
        eprintln!("SKIP {}: no dql-fatboy-duckdb next to {}", test, dql_bin());
        return false;
    }
    if !cli_present("duckdb") {
        eprintln!("SKIP {}: no `duckdb` CLI on PATH", test);
        return false;
    }
    true
}

fn duckdb_two_schema_fixture(dir: &Path) -> PathBuf {
    let db = dir.join("target.duckdb");
    let status = Command::new("duckdb")
        .arg(&db)
        .arg(
            "CREATE TABLE t (id int, tag text); INSERT INTO t VALUES (1,'main-row'); \
             CREATE SCHEMA sales; \
             CREATE TABLE sales.s (x int); INSERT INTO sales.s VALUES (42);",
        )
        .status()
        .expect("duckdb CLI");
    assert!(status.success());
    db
}

/// `mount!("file://…#sales", ns)` on a DuckDB file binds the sales schema:
/// `ns.s` reads sales.s.
#[test]
fn duckdb_mount_fragment_binds_named_schema() {
    if !duckdb_env_or_skip("duckdb_mount_fragment_binds_named_schema") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let dummy = dummy_sqlite(dir.path());
    let ddb = duckdb_two_schema_fixture(dir.path());
    let q = format!(
        "mount!(\"file://{}#sales\", \"ds\")\n\nds.s(*)",
        ddb.to_str().unwrap()
    );
    let (ok, stdout, stderr) = run_dql(dir.path(), &dummy, &q, true);
    assert!(ok, "duckdb fragment mount must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stdout.contains("42"), "ds.s reads sales.s.\nstdout:\n{stdout}");
}

/// `mount_tree!` over a DuckDB file with a user schema binds one
/// sub-namespace per persistent schema (main, sales, information_schema,
/// pg_catalog); a non-main schema resolves via its sub-namespace.
#[test]
fn duckdb_mount_tree_creates_subnamespaces() {
    if !duckdb_env_or_skip("duckdb_mount_tree_creates_subnamespaces") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let dummy = dummy_sqlite(dir.path());
    let ddb = duckdb_two_schema_fixture(dir.path());

    let q = format!("mount_tree!(\"{}\", \"duck\")", ddb.to_str().unwrap());
    let (ok, stdout, stderr) = run_dql(dir.path(), &dummy, &q, false);
    assert!(ok, "duckdb mount_tree! must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    for sub in ["duck::main", "duck::sales"] {
        assert!(stdout.contains(sub), "receipt must list {sub}.\nstdout:\n{stdout}");
    }
    assert_eq!(stdout.matches('[').count(), 1, "the receipt is ONE row.\nstdout:\n{stdout}");

    let q = format!(
        "mount_tree!(\"{}\", \"duck\")\n\nduck::sales.s(*)",
        ddb.to_str().unwrap()
    );
    let (ok, stdout, stderr) = run_dql(dir.path(), &dummy, &q, true);
    assert!(ok, "duckdb sub-namespace read.\nstdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stdout.contains("42"), "duck::sales.s resolves.\nstdout:\n{stdout}");
}
