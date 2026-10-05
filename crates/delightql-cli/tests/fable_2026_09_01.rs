// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! CLI-only discriminators from the blinded 2026-09-01 binary campaign.
//!
//! An outside observer can see each defect, but a corpus ball cannot: balls do
//! not feed CSV/JSON stdin or start the REPL, and the single-test runner
//! executes through the digest formats (`-f hash`), never through a `--to`
//! inspection.  These therefore belong at the real CLI boundary rather than
//! behind a compiler unit-test seam.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn dql_bin() -> &'static str {
    env!("CARGO_BIN_EXE_dql")
}

fn run(dir: &Path, args: &[&str], stdin: &str) -> (bool, String, String) {
    let mut child = Command::new(dql_bin())
        .args(args)
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
        .write_all(stdin.as_bytes())
        .expect("write stdin");
    let out = child.wait_with_output().expect("wait dql");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// F-009: the documented numeric filter must not become a TEXT-vs-INTEGER
/// comparison that admits every CSV row.
#[test]
fn csv_numeric_filter_obeys_the_values_not_sqlite_storage_precedence() {
    let tmp = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "tools",
            "csvstruct",
            "--has-headers",
            "--format",
            "csv",
            "c(*), age > 26",
        ],
        "name,age\nA,30\nB,7\nC,\n",
    );
    assert!(ok, "csvstruct failed: {stderr}");
    assert!(stdout.contains("A,30"), "qualifying row missing: {stdout}");
    assert!(!stdout.contains("B,7"), "7 compared as TEXT: {stdout}");
    assert!(
        !stdout.contains("C,"),
        "empty TEXT compared as numeric: {stdout}"
    );
}

/// F-022: installation of the binary's own `repl::*` definitions must obey
/// the same keyword law as authored code.  Stropping the wrapper declaration
/// is insufficient if its body still cites the reserved data name bare.
#[cfg(feature = "repl")]
#[test]
fn embedded_repl_namespace_installs_and_answers() {
    use std::sync::Arc;

    use delightql_cli::client::context::Mode;
    use delightql_cli::client::database::ClientDatabase;
    use delightql_cli::connection::{open_handle, SessionProfile};
    use delightql_cli::exec_ng::run_dql_query;

    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("open the client database"));
    let mut handle =
        open_handle(SessionProfile::Client(Some(db.clone()))).expect("open the client handle");
    let mut session = handle.session().expect("session");
    // `option` is a reserved word: bare, the admission law refuses it in
    // every position, which is the very defect this test pins.
    run_dql_query("repl::config.`option`(*)", &mut *session)
        .expect("the installed repl::config wrapper answers");
}

/// The observation law's identity, as the CLI reports it.
const OBSERVATION: &str = "semantic/effect/observation";

fn sink_db(dir: &Path) -> std::path::PathBuf {
    let db = dir.join("main.sqlite");
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TABLE sink(id INTEGER); INSERT INTO sink VALUES (1);")
        .unwrap();
    db
}

fn sink_rows(db: &Path) -> i64 {
    rusqlite::Connection::open(db)
        .unwrap()
        .query_row("SELECT count(*) FROM sink", [], |row| row.get(0))
        .unwrap()
}

fn is_one_hex_line(stdout: &str) -> bool {
    let trimmed = stdout.trim();
    !trimmed.is_empty()
        && trimmed.lines().count() == 1
        && trimmed.chars().all(|c| c.is_ascii_hexdigit())
}

const EFFECT: &str = "_(id @ 2) |> insert!(sink(*))(*)";

/// F-026: every `--to` inspection spelling is non-executing.  The refusal and
/// unchanged database are both required; either alone leaves a side-effect
/// door.
#[test]
fn hash_inspection_does_not_execute_an_effect() {
    let tmp = tempfile::tempdir().unwrap();
    let db = sink_db(tmp.path());

    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "query",
            "--db",
            "main.sqlite",
            "--to",
            "hash",
            "_(id @ 2) |> insert!(sink(*))(*)",
        ],
        "",
    );
    assert!(!ok, "inspection executed successfully: {stdout}{stderr}");
    assert!(stderr.contains(OBSERVATION), "wrong refusal: {stderr}");
    assert_eq!(sink_rows(&db), 1, "inspection inserted a row");
}

/// The same law under every digest inspection: the statement is judged
/// before any dispatcher, so the refusal names the observation and the
/// database is untouched.
#[test]
fn every_digest_inspection_refuses_the_effect() {
    let tmp = tempfile::tempdir().unwrap();
    let db = sink_db(tmp.path());
    for stage in ["totalhash", "fingerprint"] {
        let (ok, stdout, stderr) = run(
            tmp.path(),
            &["query", "--db", "main.sqlite", "--to", stage, EFFECT],
            "",
        );
        assert!(!ok, "--to {stage} executed: {stdout}{stderr}");
        assert!(stderr.contains(OBSERVATION), "--to {stage}: {stderr}");
        assert!(stdout.is_empty(), "--to {stage} printed: {stdout:?}");
    }
    assert_eq!(sink_rows(&db), 1);
}

/// A session directive is an effect the executor would run during
/// compilation; an inspection refuses it at that site, and a sequence
/// under an inspection is judged statement by statement, so the directive
/// standing BEFORE the pure statement still refuses the command.
#[test]
fn an_inspection_refuses_a_session_directive_anywhere_in_the_sequence() {
    let tmp = tempfile::tempdir().unwrap();
    sink_db(tmp.path());
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "query",
            "--db",
            "main.sqlite",
            "--to",
            "hash",
            "--sequential",
        ],
        "mount!(\"main.sqlite\", \"again\")(*)\n\nsink(*)\n",
    );
    assert!(!ok, "the directive executed: {stdout}{stderr}");
    assert!(stderr.contains(OBSERVATION), "{stderr}");
    assert!(stdout.is_empty(), "{stdout:?}");
}

/// The control: a pure statement answers under every digest inspection.
#[test]
fn a_pure_statement_answers_under_every_digest_inspection() {
    let tmp = tempfile::tempdir().unwrap();
    sink_db(tmp.path());
    for stage in ["hash", "totalhash"] {
        let (ok, stdout, stderr) = run(
            tmp.path(),
            &["query", "--db", "main.sqlite", "--to", stage, "sink(*)"],
            "",
        );
        assert!(ok, "--to {stage} refused a pure statement: {stderr}");
        assert!(is_one_hex_line(&stdout), "--to {stage}: {stdout:?}");
    }
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "query",
            "--db",
            "main.sqlite",
            "--to",
            "fingerprint",
            "sink(*)",
        ],
        "",
    );
    assert!(ok, "{stderr}");
    assert!(stdout.contains("\"datahash\""), "{stdout}");
}

/// The explicitly execution-capable road: `--to results` with a digest
/// format runs the statement — effect included — and digests its result.
#[test]
fn the_executing_road_digests_after_running_the_effect() {
    let tmp = tempfile::tempdir().unwrap();
    let db = sink_db(tmp.path());
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "query",
            "--db",
            "main.sqlite",
            "--to",
            "results",
            "-f",
            "hash",
            EFFECT,
        ],
        "",
    );
    assert!(ok, "the executing road refused: {stderr}");
    assert!(is_one_hex_line(&stdout), "{stdout:?}");
    assert_eq!(sink_rows(&db), 2, "the effect ran");

    // The default road is the executing one; the format alone selects the
    // digest rendering.
    let (ok, stdout, _) = run(
        tmp.path(),
        &["query", "--db", "main.sqlite", "-f", "totalhash", "sink(*)"],
        "",
    );
    assert!(ok);
    assert!(is_one_hex_line(&stdout), "{stdout:?}");
}

/// A digest renders an executed result, never a compilation.
#[test]
fn a_digest_format_under_a_compile_stage_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    sink_db(tmp.path());
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "query",
            "--db",
            "main.sqlite",
            "--to",
            "sql",
            "-f",
            "hash",
            "sink(*)",
        ],
        "",
    );
    assert!(!ok, "{stdout}");
    assert!(stderr.contains("renders an executed result"), "{stderr}");
}

/// F-039: a renderer must not begin a JSON document after execution has
/// already failed.  The structured error is on stderr; stdout stays empty.
#[test]
fn json_format_emits_no_partial_document_after_runtime_error() {
    let tmp = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "tools",
            "jstruct",
            "--error-prefix",
            "",
            "--error-format",
            "json",
            "--format",
            "json",
            "j(j) |> (j:{.name} as name)",
        ],
        "not json",
    );
    assert!(!ok, "malformed JSON unexpectedly succeeded");
    assert!(stderr.contains("malformed JSON"), "wrong error: {stderr}");
    assert!(
        stdout.is_empty(),
        "partial JSON document escaped: {stdout:?}"
    );
}

/// The control: a successful production commits the whole document, on
/// the query road and the tools road alike.
#[test]
fn json_format_commits_a_complete_document_on_success() {
    let tmp = tempfile::tempdir().unwrap();
    let (ok, stdout, stderr) = run(tmp.path(), &["query", "-f", "json", "_(a @ 1; 2)"], "");
    assert!(ok, "{stderr}");
    assert_eq!(stdout, "[\n  {\"a\": 1},\n  {\"a\": 2}\n]\n");

    let (ok, stdout, stderr) = run(
        tmp.path(),
        &[
            "tools",
            "jstruct",
            "--format",
            "json",
            "j(j) |> (j:{.name} as name)",
        ],
        "{\"name\": \"Ada\"}",
    );
    assert!(ok, "{stderr}");
    assert_eq!(stdout, "[\n  {\"name\": \"Ada\"}\n]\n");

    let (ok, stdout, stderr) = run(tmp.path(), &["query", "-f", "json", "_(a @ 1), a = 2"], "");
    assert!(ok, "{stderr}");
    assert_eq!(stdout, "[]\n", "the empty document has its own spelling");
}
