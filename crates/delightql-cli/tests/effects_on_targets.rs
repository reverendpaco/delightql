// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `run!` and the query-position effect directives against LIVE Postgres
//! and DuckDB mounts.
//!
//! Why integration tests: honesty is a TOPOLOGY property — a real dql
//! process, a real fatboy child, a real engine, post-state verified
//! through the engine's OWN door (psql / the duckdb CLI), never through
//! dql's receipts alone.
//!
//! Environment gating: DuckDB tests need the
//! `dql-fatboy-duckdb` binary next to the dql under test AND a `duckdb`
//! CLI on PATH; Postgres tests need TCP 127.0.0.1:5433 (the sweep
//! lane's container `dql-sweep-pg`, new_test_suite/sweep.py), the
//! `dql-fatboy-postgres` sibling, and a `psql` on PATH. Each test SKIPS
//! with an eprintln when its environment is absent. Every PG test
//! creates its own scratch DATABASE (`probe_e5_<test>`) and drops it
//! panic-safely — nothing outside the
//! scratch databases is ever touched.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

/// Run `dql query` with `stdin = query`, cwd = `dir`, against `db`.
/// `sequential` splits multi-statement input client-side onto ONE
/// session (only the LAST statement's result prints — exec_ng's
/// contract), which is how the in-session-readability pins read what a
/// directive created moments earlier.
fn run_dql(dir: &Path, db: &str, query: &str, sequential: bool) -> (bool, String, String) {
    let mut cmd = Command::new(dql_bin());
    cmd.arg("query")
        .arg("--db")
        .arg(db)
        .arg("--to")
        .arg("results");
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
        .map(|d| {
            d.join(format!("{}{}", name, std::env::consts::EXE_SUFFIX))
                .is_file()
        })
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

// ── DuckDB gating and fixtures ──────────────────────────────────────────

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

/// Author a DuckDB file via the duckdb CLI (dql's fatboy holds the file
/// exclusively while a session runs, so authoring and
/// post-state checks bracket the dql invocations).
fn duckdb_exec(db: &Path, sql: &str) {
    let status = Command::new("duckdb")
        .arg(db)
        .arg(sql)
        .status()
        .expect("run duckdb CLI");
    assert!(status.success(), "duckdb CLI failed for: {}", sql);
}

fn duckdb_query(db: &Path, sql: &str) -> String {
    let out = Command::new("duckdb")
        .arg("-noheader")
        .arg("-list")
        .arg(db)
        .arg(sql)
        .output()
        .expect("run duckdb CLI");
    assert!(
        out.status.success(),
        "duckdb CLI query failed for: {}\nstderr: {}",
        sql,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn duckdb_orders_fixture(dir: &Path) -> PathBuf {
    let db = dir.join("target.duckdb");
    duckdb_exec(
        &db,
        "CREATE TABLE orders (order_id INTEGER, region TEXT, amount INTEGER); \
         INSERT INTO orders VALUES (101,'EU',250),(102,'US',80),(103,'EU',40); \
         CREATE TABLE orders_eu (order_id INTEGER, region TEXT, amount INTEGER);",
    );
    db
}

fn duckdb_durable_tables(db: &Path) -> String {
    duckdb_query(
        db,
        "SELECT table_name FROM information_schema.tables ORDER BY table_name;",
    )
}

// ── Postgres gating, psql door, panic-safe scratch databases ────────────

/// The sweep lane's conninfo (new_test_suite/sweep.py: container
/// `dql-sweep-pg`, trust auth, published on 127.0.0.1:5433).
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
        eprintln!(
            "SKIP {}: no PG at {} (start with new_test_suite/sweep.py postgres)",
            test, PG_HOST
        );
        return false;
    }
    if !fatboy_present("dql-fatboy-postgres") {
        eprintln!(
            "SKIP {}: no dql-fatboy-postgres next to {}",
            test,
            dql_bin()
        );
        return false;
    }
    if !cli_present("psql") {
        eprintln!("SKIP {}: no `psql` on PATH for fixtures/verification", test);
        return false;
    }
    true
}

/// Run SQL through psql (the engine's own door — never dql) and return
/// stdout, tuples-only unaligned.
fn psql(db: &str, sql: &str) -> String {
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
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A scratch DATABASE on the sweep container, dropped panic-safely on
/// Drop (`WITH (FORCE)` evicts any lingering backend). Rerun-safe:
/// pre-drops before creating.
struct ScratchDb {
    name: String,
}

impl ScratchDb {
    fn create(name: &str) -> ScratchDb {
        psql(
            "postgres",
            &format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", name),
        );
        psql("postgres", &format!("CREATE DATABASE {}", name));
        ScratchDb {
            name: name.to_string(),
        }
    }

    fn uri(&self) -> String {
        pg_uri(&self.name)
    }

    fn sql(&self, sql: &str) -> String {
        psql(&self.name, sql)
    }
}

impl Drop for ScratchDb {
    fn drop(&mut self) {
        let _ = Command::new("psql")
            .arg(pg_uri("postgres"))
            .arg("-c")
            .arg(format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.name
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// The standard PG orders fixture.
fn pg_orders_fixture(db: &ScratchDb) {
    db.sql(
        "CREATE TABLE orders (order_id INTEGER, region TEXT, amount INTEGER); \
         INSERT INTO orders VALUES (101,'EU',250),(102,'US',80),(103,'EU',40); \
         CREATE TABLE orders_eu (order_id INTEGER, region TEXT, amount INTEGER);",
    );
}

// ════════════════════════════════════════════════════════════════════════
// DuckDB — effect directives against a live mount
// ════════════════════════════════════════════════════════════════════════

/// Ad-hoc `temp_table!` with a RESOLVED source returns its receipt, the
/// object is readable IN-SESSION (the registration read-back round trip
/// over a real relay), and it is GONE after the session (session-temp,
/// no durable residue in the file).
#[test]
fn duckdb_resolved_source_temp_table_receipt_and_session_scratch() {
    if !duckdb_env_or_skip("duckdb_resolved_source_temp_table_receipt_and_session_scratch") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = duckdb_orders_fixture(dir.path());
    let db_str = db.to_str().unwrap();

    // The receipt.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        "orders(*) |> temp_table!(staged(*))(*)",
        false,
    );
    assert!(
        ok,
        "temp_table! on duckdb must execute.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("temp_table!") && stdout.contains("staged"),
        "expected the creation receipt.\nstdout:\n{stdout}"
    );

    // Readable in-session (one session, sequential).
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        "orders(*) |> temp_table!(staged(*))(*)\n\nstaged(*) ~> count:(*) as n",
        true,
    );
    assert!(
        ok,
        "in-session read of the created temp table.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('3'),
        "staged holds the 3 staged orders.\nstdout:\n{stdout}"
    );

    // Gone after the session: no durable residue, and a fresh session
    // cannot resolve the name.
    let tables = duckdb_durable_tables(&db);
    assert!(
        !tables.contains("staged"),
        "session temp must leave no durable residue.\ntables:\n{tables}"
    );
    let (ok, stdout, _stderr) = run_dql(dir.path(), db_str, "staged(*)", false);
    assert!(
        !ok,
        "a fresh session must not resolve the dead scratch.\nstdout:\n{stdout}"
    );
}

/// `_(x @ 1) |> temp_table!(t(*))(*)`'s anon-source object must land on the
/// MAIN mount's connection: the in-session read routes to the TARGET
/// (registration keys on the object's connection), so a hub landing
/// instead would surface the liar's shape — a success receipt
/// followed by "no such table" on the target session.
#[test]
fn duckdb_anon_source_temp_table_lands_on_the_target_session() {
    if !duckdb_env_or_skip("duckdb_anon_source_temp_table_lands_on_the_target_session") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = duckdb_orders_fixture(dir.path());
    let db_str = db.to_str().unwrap();
    let tables_before = duckdb_durable_tables(&db);

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        "_(x @ 1) |> temp_table!(t(*))(*)\n\nt(*)",
        true,
    );
    assert!(
        ok,
        "the anon-source object must be readable on the TARGET session \
         (a hub landing errors here — the liar's shape).\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('1'),
        "t holds the anon row.\nstdout:\n{stdout}"
    );

    // The receipt, on its own session.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        "_(x @ 1) |> temp_table!(t(*))(*)",
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("temp_table!") && stdout.contains('t'),
        "expected the creation receipt.\nstdout:\n{stdout}"
    );

    // Temp-only: the file's durable table list is unchanged.
    assert_eq!(
        tables_before,
        duckdb_durable_tables(&db),
        "an anon-source temp_table! must not mint durable objects"
    );
}

/// Ad-hoc DML lands on the target with YES receipts; a predicate that
/// matches nothing gives the EMPTY receipt (the pre-count gate). Post-
/// state verified through the duckdb CLI, never through dql.
#[test]
fn duckdb_adhoc_dml_receipts_and_post_state() {
    if !duckdb_env_or_skip("duckdb_adhoc_dml_receipts_and_post_state") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = duckdb_orders_fixture(dir.path());
    let db_str = db.to_str().unwrap();

    // insert! YES: 2 EU rows land.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        r#"orders(*), region = "EU" |> insert!(orders_eu(*))(*)"#,
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("insert!"),
        "YES receipt expected.\nstdout:\n{stdout}"
    );
    assert_eq!(duckdb_query(&db, "SELECT count(*) FROM orders_eu"), "2");

    // insert! NO: nothing matches, EMPTY receipt (zero rows — the
    // staged pre-count gates the receipt out), post-state unchanged.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        r#"orders(*), region = "XX" |> insert!(orders_eu(*))(*)"#,
        false,
    );
    assert!(
        ok,
        "a NO answer is not an error.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("insert!"),
        "NO must be the EMPTY receipt, not a success row.\nstdout:\n{stdout}"
    );
    assert_eq!(duckdb_query(&db, "SELECT count(*) FROM orders_eu"), "2");

    // update! YES: the US row's amount changes.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        r#"orders!!(*), region = "US" |> $$(999 as amount) |> update!(orders(*))(*)"#,
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("update!"),
        "YES receipt expected.\nstdout:\n{stdout}"
    );
    assert_eq!(
        duckdb_query(&db, "SELECT count(*) FROM orders WHERE amount = 999"),
        "1"
    );

    // delete! YES: the EU rows go.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        r#"orders!!(*), region = "EU" |> delete!(orders(*))(*)"#,
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("delete!"),
        "YES receipt expected.\nstdout:\n{stdout}"
    );
    assert_eq!(duckdb_query(&db, "SELECT count(*) FROM orders"), "1");
}

/// `table!` durable on a DuckDB file: the object persists across a full
/// reopen (verified through the duckdb CLI after the dql session exits)
/// and a SECOND dql session resolves it bare (mount introspection).
#[test]
fn duckdb_table_bang_durable_persists_across_reopen() {
    if !duckdb_env_or_skip("duckdb_table_bang_durable_persists_across_reopen") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = duckdb_orders_fixture(dir.path());
    let db_str = db.to_str().unwrap();

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db_str,
        "orders(*) |> table!(archive(*))(*)",
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("table!"),
        "creation receipt expected.\nstdout:\n{stdout}"
    );

    // Across a reopen, through the engine's own door.
    assert_eq!(duckdb_query(&db, "SELECT count(*) FROM archive"), "3");

    // A second dql session resolves it bare.
    let (ok, stdout, stderr) = run_dql(dir.path(), db_str, "archive(*) ~> count:(*) as n", false);
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains('3'),
        "the durable object survives sessions.\nstdout:\n{stdout}"
    );
}

/// An ABORTING plan on DuckDB: the constraint violation's TRUE error
/// surfaces (not a poisoned-transaction message), ROLLBACK undoes the
/// plan's earlier mutation, exit code 1.
#[test]
fn duckdb_aborting_plan_rolls_back_and_surfaces_the_true_error() {
    if !duckdb_env_or_skip("duckdb_aborting_plan_rolls_back_and_surfaces_the_true_error") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("abort.duckdb");
    duckdb_exec(
        &db,
        "CREATE TABLE src (x INTEGER); INSERT INTO src VALUES (5),(-1); \
         CREATE TABLE ok_t (x INTEGER); \
         CREATE TABLE guarded (x INTEGER CHECK (x > 0));",
    );
    std::fs::write(
        dir.path().join("abort.dql"),
        "main!(*) :-\n\
         \x20   src(*) |> insert!(ok_t(*))(*) : a!\n\
         \x20   src(*) |> insert!(guarded(*))(*) : b!\n\
         \x20   a!(*) ; b!(*)\n",
    )
    .unwrap();

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "run!(\"abort.dql\")(*)",
        false,
    );
    assert!(
        !ok,
        "the aborting plan must fail.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("CHECK constraint"),
        "the TRUE constraint error must surface.\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("transaction is aborted"),
        "never the poisoned-transaction mask (ROLLBACK-on-first-error).\nstderr:\n{stderr}"
    );
    // The rollback proof: the plan's FIRST insert (which succeeded) is gone.
    assert_eq!(
        duckdb_query(&db, "SELECT count(*) FROM ok_t"),
        "0",
        "the pre-error insert must be rolled back with the bracket"
    );
    assert_eq!(duckdb_query(&db, "SELECT count(*) FROM guarded"), "0");
}

// ════════════════════════════════════════════════════════════════════════
// Postgres — effect directives against a live mount, plus the capstone
// ════════════════════════════════════════════════════════════════════════

/// Ad-hoc DML on PG: the fused data-modifying-CTE receipts — YES
/// receipts with rows landing, the NO case's empty receipt,
/// update!/delete! post-states verified via psql.
#[test]
fn pg_adhoc_dml_receipts_and_post_state() {
    if !pg_env_or_skip("pg_adhoc_dml_receipts_and_post_state") {
        return;
    }
    let db = ScratchDb::create("probe_e5_dml");
    pg_orders_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    // insert! YES.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        r#"orders(*), region = "EU" |> insert!(orders_eu(*))(*)"#,
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("insert!"),
        "YES receipt expected.\nstdout:\n{stdout}"
    );
    assert_eq!(db.sql("SELECT count(*) FROM orders_eu"), "2");

    // insert! NO: empty receipt, nothing lands.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        r#"orders(*), region = "XX" |> insert!(orders_eu(*))(*)"#,
        false,
    );
    assert!(
        ok,
        "a NO answer is not an error.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("insert!"),
        "NO must be the EMPTY receipt (the fused gate working live).\nstdout:\n{stdout}"
    );
    assert_eq!(db.sql("SELECT count(*) FROM orders_eu"), "2");

    // update! YES.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        r#"orders!!(*), region = "US" |> $$(999 as amount) |> update!(orders(*))(*)"#,
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("update!"),
        "YES receipt expected.\nstdout:\n{stdout}"
    );
    assert_eq!(
        db.sql("SELECT count(*) FROM orders WHERE amount = 999"),
        "1"
    );

    // delete! YES.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        r#"orders!!(*), region = "EU" |> delete!(orders(*))(*)"#,
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("delete!"),
        "YES receipt expected.\nstdout:\n{stdout}"
    );
    assert_eq!(db.sql("SELECT count(*) FROM orders"), "1");
}

/// The anon-source object lands on the TARGET session (in-session read
/// routes to the mount's connection and answers), pg_class confirms it
/// never became durable.
#[test]
fn pg_anon_source_temp_table_lands_on_the_target() {
    if !pg_env_or_skip("pg_anon_source_temp_table_lands_on_the_target") {
        return;
    }
    let db = ScratchDb::create("probe_e5_anon");
    // A zero-table scratch database: the mount is legal, nothing resolves.
    let dir = tempfile::tempdir().unwrap();

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        "_(x @ 1) |> temp_table!(t(*))(*)\n\nt(*)",
        true,
    );
    assert!(
        ok,
        "the anon-source object must be readable on the TARGET session \
         (a hub landing errors here — the P2 liar's shape).\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('1'),
        "t holds the anon row.\nstdout:\n{stdout}"
    );

    // Never durable: the scratch database's catalog holds no relation `t`
    // after the session (its pg_temp schema died with the backend).
    assert_eq!(
        db.sql(
            "SELECT count(*) FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = 't' AND n.nspname NOT LIKE 'pg_%'"
        ),
        "0",
        "the anon temp object must not be durable"
    );
}

/// The registration read-back round trip over a REAL relay — PG's
/// information_schema lists the session's OWN temp table, so the
/// created object is queryable by a subsequent statement in the SAME
/// session — and `table!` spells `public.<name>` in a real bracket,
/// durable across sessions.
#[test]
fn pg_temp_readback_round_trip_and_table_bang_lands_in_public() {
    if !pg_env_or_skip("pg_temp_readback_round_trip_and_table_bang_lands_in_public") {
        return;
    }
    let db = ScratchDb::create("probe_e5_durable");
    pg_orders_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    // temp_table! + same-session read: the read-back registers the
    // object from information_schema's view of pg_temp.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        "orders(*) |> temp_table!(staged(*))(*)\n\nstaged(*) ~> count:(*) as n",
        true,
    );
    assert!(
        ok,
        "same-session read of the created temp table.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('3'),
        "staged holds the 3 orders.\nstdout:\n{stdout}"
    );

    // table! durable + same-session read, then public placement + rows
    // verified via psql after the session.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        "orders(*) |> table!(archive(*))(*)\n\narchive(*) ~> count:(*) as n",
        true,
    );
    assert!(
        ok,
        "same-session read of the durable creation.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains('3'),
        "archive holds the 3 orders.\nstdout:\n{stdout}"
    );
    assert_eq!(
        db.sql("SELECT table_schema FROM information_schema.tables WHERE table_name = 'archive'"),
        "public",
        "table! spells the mounted schema (R-T4): public.archive"
    );
    assert_eq!(db.sql("SELECT count(*) FROM public.archive"), "3");

    // A second dql session resolves the durable bare.
    let (ok, stdout, stderr) =
        run_dql(dir.path(), &db.uri(), "archive(*) ~> count:(*) as n", false);
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(stdout.contains('3'), "stdout:\n{stdout}");
}

/// THE CAPSTONE: `run!` of a torture-shaped multi-step script on live
/// Postgres — temp_table! staging with a mid-pipe stdout!, exit! (not
/// taken), temp_view!, an HO quarantine rule, the split route!, a
/// receipt-gated chain (route! gates mark_processed!), delete! cleanup,
/// the signed-witness ledger shipped mid-run, and a returning_other!
/// tail reading DURABLE post-state. Everything verified through psql:
/// the routed rows, the processed window, and ZERO residue (no durable
/// scratch, no `__` names; pg_temp died with the session).
#[test]
fn pg_run_torture_shaped_script_the_capstone() {
    if !pg_env_or_skip("pg_run_torture_shaped_script_the_capstone") {
        return;
    }
    let db = ScratchDb::create("probe_e5_torture");
    db.sql(
        "CREATE TABLE customers (customer_id INTEGER, region TEXT, name TEXT); \
         INSERT INTO customers VALUES (1,'EU','Ann'),(2,'US','Bob'); \
         CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, region TEXT, \
                              amount INTEGER, order_date TEXT, status TEXT); \
         INSERT INTO orders VALUES \
           (101,1,'EU',250,'2026-07-02','new'), \
           (102,2,'US',80,'2026-07-03','new'), \
           (103,9,'EU',40,'2026-07-02','new'), \
           (104,1,'EU',-5,'2026-07-04','new'), \
           (90,1,'EU',10,'2026-06-01','old'); \
         CREATE TABLE orders_eu (LIKE orders); \
         CREATE TABLE orders_us (LIKE orders); \
         CREATE TABLE orders_quarantine (LIKE orders);",
    );
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("capstone.dql"),
        r#"doc!("main", "E-T5 capstone: torture-shaped run on a live Postgres mount")

recent_orders(*) :- orders(*), order_date >= "2026-07-01"

final_summary(*) :-
    orders_eu(~> count:(*) as n, _(bucket @ "eu"))
  ; orders_us(~> count:(*) as n, _(bucket @ "us"))
  ; orders_quarantine(~> count:(*) as n, _(bucket @ "quarantine"))

quarantine!(Bad(*))(*) :-
    Bad(*) |> insert!(orders_quarantine(*))(*)

route!(*) :- valid(*), region = "EU" |> insert!(orders_eu(*))(*)
route!(*) :- valid(*), region = "US" |> insert!(orders_us(*))(*)

mark_processed!(*) :-
    orders!!(*), order_date >= "2026-07-01"
      |> $$("processed" as status)
      |> update!(orders(*))(*)

main!(*) :-
    recent_orders(*) |> stdout!(*) |> temp_table!(staged(*))(*) : s!
    staged(*) ~> count:(*) as n, n = 0, exit!(*) : x!
    staged(*), +customers(customer_id), amount > 0
      |> temp_view!(valid(*))(*) : v!
    staged(*), \+customers(customer_id) |> quarantine!(*) : q!
    route!(*), mark_processed!(*) : rm!
    staged!!(*), \+customers(customer_id) or amount <= 0
      |> delete!(staged(*))(*) : k!
    s!(+-) ; x!(+-) ; v!(+-) ; q!(+-) ; rm!(+-) ; k!(+-)
      |> stdout!(*)
      |> returning_other!(final_summary(*))(*)
"#,
    )
    .unwrap();

    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), "run!(\"capstone.dql\")(*)", false);
    assert!(
        ok,
        "the capstone run must succeed.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The mid-run ships arrived: the staged orders (stdout! #1) and the
    // total ledger (stdout! #2 — every arm's directive present).
    assert!(
        stdout.contains("101"),
        "the staging stdout! shipped.\nstdout:\n{stdout}"
    );
    for op in ["temp_table!", "temp_view!", "insert!", "update!", "delete!"] {
        assert!(
            stdout.contains(op),
            "the ledger carries the {op} receipt.\nstdout:\n{stdout}"
        );
    }
    // The return value: durable post-state buckets.
    for bucket in ["eu", "us", "quarantine"] {
        assert!(
            stdout.contains(bucket),
            "final_summary bucket {bucket}.\nstdout:\n{stdout}"
        );
    }

    // Durable post-state through psql (the oracle's shape: eu=1, us=1,
    // quarantine=1, processed=4 — order 90 predates the window).
    assert_eq!(db.sql("SELECT count(*) FROM orders_eu"), "1");
    assert_eq!(db.sql("SELECT count(*) FROM orders_us"), "1");
    assert_eq!(db.sql("SELECT count(*) FROM orders_quarantine"), "1");
    assert_eq!(
        db.sql("SELECT count(*) FROM orders WHERE status = 'processed'"),
        "4"
    );
    assert_eq!(db.sql("SELECT order_id FROM orders_quarantine"), "103");

    // ZERO residue: exactly the five fixture tables; nothing named like
    // plan scratch anywhere; pg_temp is empty post-session (the fatboy
    // child exited with the dql process).
    assert_eq!(
        db.sql(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = 'public'"
        ),
        "5",
        "no durable residue beyond the fixtures"
    );
    assert_eq!(
        db.sql("SELECT count(*) FROM pg_class WHERE relname LIKE '\\_\\_%'"),
        "0",
        "no scratch residue in any schema"
    );
    assert_eq!(
        db.sql(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname LIKE 'pg_temp%'"
        ),
        "0",
        "pg_temp empty post-session"
    );
}

/// An ABORTING plan on PG: the mid-plan constraint violation surfaces
/// its TRUE error (23514's message, never the 25P02 mask — the pump's
/// ROLLBACK-on-first-error), the bracket rolls back the plan's earlier
/// successful insert, exit code 1, zero residue.
#[test]
fn pg_aborting_plan_surfaces_the_true_error_and_rolls_back() {
    if !pg_env_or_skip("pg_aborting_plan_surfaces_the_true_error_and_rolls_back") {
        return;
    }
    let db = ScratchDb::create("probe_e5_abort");
    db.sql(
        "CREATE TABLE src (x INTEGER); INSERT INTO src VALUES (5),(-1); \
         CREATE TABLE ok_t (x INTEGER); \
         CREATE TABLE guarded (x INTEGER CHECK (x > 0));",
    );
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("abort.dql"),
        "main!(*) :-\n\
         \x20   src(*) |> insert!(ok_t(*))(*) : a!\n\
         \x20   src(*) |> insert!(guarded(*))(*) : b!\n\
         \x20   a!(*) ; b!(*)\n",
    )
    .unwrap();

    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), "run!(\"abort.dql\")(*)", false);
    assert!(
        !ok,
        "the aborting plan must fail.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("violates check constraint"),
        "the TRUE error surfaces (E-T3a + R-T3).\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("25P02") && !stderr.contains("current transaction is aborted"),
        "never the poisoned-transaction mask.\nstderr:\n{stderr}"
    );
    // The rollback proof: the FIRST insert succeeded mid-bracket and is gone.
    assert_eq!(
        db.sql("SELECT count(*) FROM ok_t"),
        "0",
        "the pre-error insert must be rolled back with the bracket"
    );
    assert_eq!(db.sql("SELECT count(*) FROM guarded"), "0");
    // Zero residue, durable and temp.
    assert_eq!(
        db.sql("SELECT count(*) FROM pg_class WHERE relname LIKE '\\_\\_%'"),
        "0",
        "no scratch residue after the abort"
    );
}

/// exit! taken AND not taken on PG — the dialected exit peek live: on
/// PG the exit shell sits INSIDE the bracket (ON COMMIT DROP), so a
/// pre-shell peek would poison the whole bracket; the plan only runs
/// because peeks start after the shell entry.
#[test]
fn pg_exit_taken_and_not_taken() {
    if !pg_env_or_skip("pg_exit_taken_and_not_taken") {
        return;
    }
    let db = ScratchDb::create("probe_e5_exit");
    db.sql(
        "CREATE TABLE src (x INTEGER); INSERT INTO src VALUES (5); \
         CREATE TABLE sink (x INTEGER);",
    );
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("exit.dql"),
        r#"final_count(*) :- sink(*) ~> count:(*) as n

main!(*) :-
    src(*), x >= 100 |> temp_table!(staged(*))(*) : s!

    staged(*) ~> count:(*) as n, n = 0, exit!(*) : x!

    staged(*) |> insert!(sink(*))(*) : i!

    s!(+-) ; x!(+-) ; i!(+-) |> stdout!(*) |> returning_other!(final_count(*))(*)
"#,
    )
    .unwrap();

    // TAKEN: nothing stages, exit! fires, the insert is skipped, the
    // wrap-guarded return is empty — a graceful exit, code 0.
    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), "run!(\"exit.dql\")(*)", false);
    assert!(
        ok,
        "exit! is graceful, not an abort.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !stdout.contains("insert!"),
        "post-exit arms must not run (their receipts never ship).\nstdout:\n{stdout}"
    );
    assert_eq!(
        db.sql("SELECT count(*) FROM sink"),
        "0",
        "exit! stopped the run before the insert"
    );

    // NOT TAKEN: a row stages, the run continues to the insert.
    db.sql("INSERT INTO src VALUES (150)");
    let (ok, stdout, stderr) = run_dql(dir.path(), &db.uri(), "run!(\"exit.dql\")(*)", false);
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains('1'),
        "the return reads sink's count = 1.\nstdout:\n{stdout}"
    );
    assert_eq!(
        db.sql("SELECT count(*) FROM sink"),
        "1",
        "the staged row landed"
    );
}

/// `stdout!` mid-pipe on PG: on_ship over a real relay — the mid-run
/// set prints live, then the receipt arrives as the return value.
#[test]
fn pg_stdout_mid_pipe_ships_live() {
    if !pg_env_or_skip("pg_stdout_mid_pipe_ships_live") {
        return;
    }
    let db = ScratchDb::create("probe_e5_ship");
    pg_orders_fixture(&db);
    let dir = tempfile::tempdir().unwrap();

    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        r#"orders(*), region = "EU" |> stdout!(*) |> temp_table!(staged(*))(*)"#,
        false,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("101") && stdout.contains("103"),
        "the mid-pipe set ships through on_ship.\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains("temp_table!"),
        "the receipt still arrives as the return.\nstdout:\n{stdout}"
    );
}

/// The RULED liminal-ledger answer: consult-then-run against a PG
/// mount records the ledger normally — ON THE HUB's internal SQLite
/// (the sys::ns bootstrap table), never on the customer's engine. The
/// ledger read works after the run, and the target database gained no
/// tables.
#[test]
fn pg_consult_plus_run_records_the_liminal_ledger_on_the_hub() {
    if !pg_env_or_skip("pg_consult_plus_run_records_the_liminal_ledger_on_the_hub") {
        return;
    }
    let db = ScratchDb::create("probe_e5_ledger");
    db.sql(
        "CREATE TABLE orders (order_id INTEGER, region TEXT); INSERT INTO orders VALUES (1,'EU');",
    );
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("ledger.dql"),
        r#"doc!("main", "the ledger stays on the hub")

main!(*) :-
    orders(*) |> temp_table!(staged(*))(*) : s!
    s!(*) |> returning!(*)
"#,
    )
    .unwrap();

    // One session: run! (consult half executes the liminal space, plan
    // half executes on PG), then read the namespace's ledger.
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        &db.uri(),
        "run!(\"ledger.dql\")(*)\n\nledger::(*).liminal(*)",
        true,
    );
    assert!(ok, "stdout:\n{stdout}\nstderr:\n{stderr}");
    assert!(
        stdout.contains("doc!"),
        "the liminal ledger holds the doc! receipt (hub bookkeeping).\nstdout:\n{stdout}"
    );

    // No sys tables — no tables at all — landed on the customer's engine.
    assert_eq!(
        db.sql("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'"),
        "1",
        "the target database holds exactly its fixture; the ledger never lands there"
    );
}

// ════════════════════════════════════════════════════════════════════════
// The non-firing control: all-SQLite effect statements are unaffected.
// ════════════════════════════════════════════════════════════════════════

#[test]
fn sqlite_effects_still_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("world.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE orders (order_id INTEGER, region TEXT, amount INTEGER);
         INSERT INTO orders VALUES (101, 'EU', 250), (102, 'US', 80), (103, 'EU', 40);",
    )
    .unwrap();
    drop(conn);
    let (ok, stdout, stderr) = run_dql(
        dir.path(),
        db.to_str().unwrap(),
        "orders(*) |> temp_table!(staged(*))(*)",
        false,
    );
    assert!(
        ok,
        "SQLite effect statements must keep working.\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("temp_table!"),
        "expected a temp_table! receipt.\nstdout:\n{stdout}"
    );
}
