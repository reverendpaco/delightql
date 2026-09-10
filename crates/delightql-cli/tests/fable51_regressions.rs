// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Outside-observable CLI contracts that a result-hash ball cannot express:
//! bootstrap warnings, contradictory environment diagnostics, and the absence
//! of compiler-invariant reports without prescribing a new diagnostic leaf.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn run(dir: &Path, args: &[&str], dialect_env: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dql"));
    command
        .args(args)
        .current_dir(dir)
        .env("DQL_STATE_DIR", dir.join("state"))
        .env_remove("DQL_DIALECT")
        .env_remove("DQL_TEST_PANIC")
        .env_remove("RUST_BACKTRACE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(value) = dialect_env {
        command.env("DQL_DIALECT", value);
    }
    let mut child = command.spawn().expect("spawn dql");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let err_reader = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let start = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll dql") {
            break status;
        }
        if start.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            let stdout = out_reader.join().unwrap();
            let stderr = err_reader.join().unwrap();
            panic!(
                "dql exceeded ten seconds: {}{}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    };
    Output {
        status,
        stdout: out_reader.join().unwrap(),
        stderr: err_reader.join().unwrap(),
    }
}

/// A requested output dialect must not be executed against the SQLite host's
/// bootstrap. A ball sees the successful query answer, not this CLI warning.
#[test]
fn f25_postgres_rendering_keeps_the_host_bootstrap_in_its_own_dialect() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(
        tmp.path(),
        &[
            "query",
            "--dialect",
            "postgres",
            "--to",
            "sql",
            "-f",
            "raw",
            "_(n @ 1)",
        ],
        None,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{stderr}");
    assert!(stdout.contains("SELECT 1 AS n"), "{stdout}");
    assert!(
        !stderr.contains("repl::* namespace could not be installed"),
        "output dialect broke host bootstrap: {stderr}"
    );
}

/// The control pins the same observable bootstrap contract on the native
/// dialect; it does not require a remote PostgreSQL server.
#[test]
fn f25_sqlite_bootstrap_control() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(
        tmp.path(),
        &[
            "query",
            "--dialect",
            "sqlite",
            "--to",
            "sql",
            "-f",
            "raw",
            "_(n @ 1)",
        ],
        None,
    );
    assert!(out.status.success(), "{out:?}");
    assert!(String::from_utf8_lossy(&out.stdout).contains("SELECT 1 AS n"));
    assert!(!String::from_utf8_lossy(&out.stderr).contains("could not be installed"));
}

/// Failure and "ignoring" cannot both describe the handling of one invalid
/// environment value. The corpus cannot choose the CLI process environment.
#[test]
fn f34_rejected_dialect_is_not_also_reported_as_ignored() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(tmp.path(), &["query", "_(n @ 1)"], Some("no_such_dialect"));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "invalid dialect succeeded");
    assert!(stderr.contains("unknown DQL_DIALECT"), "{stderr}");
    assert!(
        !stderr
            .lines()
            .any(|line| line.contains("DQL_DIALECT") && line.contains("ignoring")),
        "the refused setting was also claimed to be ignored: {stderr}"
    );
}

/// run! consumes a definition file with main!, not a utility query file.
/// Wrong file kind must refuse without a compiler-invariant report; this
/// checks stderr without inventing a required new URI or accepting the file.
#[test]
fn f26_run_refuses_a_query_sequence_without_an_internal_error() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("utility.dql"),
        "#!dql query-sequence\n_(n @ 1)\n_(n @ 2)\n",
    )
    .unwrap();
    let out = run(tmp.path(), &["query", "run!(\"utility.dql\")(*)"], None);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "run! accepted a utility file");
    assert!(
        !stderr.contains("internal error") && !stderr.contains("delightql-error://internal/"),
        "wrong authored file kind became a compiler defect: {stderr}"
    );
}

/// The lawful definition-file form remains executable, independently of the
/// utility-file refusal. A run that merely refuses every file fails here.
#[test]
fn f26_run_definition_file_control() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(
        tmp.path().join("job.dql"),
        "main!(*) :- _(v @ 7) |> returning!(*)\n",
    )
    .unwrap();
    let out = run(
        tmp.path(),
        &["query", "-f", "json", "run!(\"job.dql\")(*)"],
        None,
    );
    assert!(out.status.success(), "{out:?}");
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0]["success"] == 1 || rows[0]["success"] == "1",
        "{rows:?}"
    );
    assert_eq!(rows[0]["operation"], "main!");
}

/// No success/refusal policy is inferred from a grammar-only spelling here.
/// Either lawful answer must avoid blaming a missing authored marker on a
/// compiler invariant; the precise admission/diagnostic remains to be judged.
#[test]
fn f28_sourceless_inner_form_never_reports_a_normalizer_invariant() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(
        tmp.path(),
        &[
            "query",
            "_(id @ 1; 2) |> (id, _:( ~> id * 2 as x) as twice)",
        ],
        None,
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("internal error") && !stderr.contains("delightql-error://internal/"),
        "authored inner form became a compiler defect: {stderr}"
    );
}

#[test]
fn f28_named_inner_relation_control() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(
        tmp.path(),
        &[
            "query",
            "-f",
            "csv",
            "_(id @ 1; 2) |> (id, _:(, _(v @ 2) ~> sum:(v)) as twice)",
        ],
        None,
    );
    assert!(out.status.success(), "{out:?}");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "id,twice\n1,2\n2,2"
    );
}

/// Ambiguity remains a refusal, but a compiler-generated wrapper is not an
/// authored candidate that a programmer can qualify to disambiguate.
#[test]
fn f11_ambiguity_does_not_offer_a_compiler_wrap_as_a_candidate() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(
        tmp.path(),
        &[
            "query",
            "sys::meta::(*), entities ~= ~> {name, type} |> (name, type)",
        ],
        None,
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "ambiguous name was accepted");
    assert!(stderr.contains("ambiguous"), "{stderr}");
    assert!(!stderr.contains("compiler wrap"), "{stderr}");
}

#[test]
fn f11_authored_ambiguity_control() {
    let tmp = tempfile::tempdir().unwrap();
    let out = run(
        tmp.path(),
        &["query", "left_rows(*) : _(id @ 1)\nright_rows(*) : _(id @ 1)\nleft_rows(*), right_rows(*), left_rows.id = right_rows.id |> %(id ~> count:(*) as n)"],
        None,
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!out.status.success(), "ambiguous name was accepted");
    assert!(stderr.contains("ambiguous"), "{stderr}");
    assert!(
        stderr.contains("left_rows") && stderr.contains("right_rows"),
        "{stderr}"
    );
    assert!(!stderr.contains("compiler wrap"), "{stderr}");
}

/// This tests agreement, not a requirement to preserve a retired catalog
/// face: either errors is no longer advertised or the advertised table reads.
#[test]
fn f12_an_advertised_errors_table_is_readable() {
    let tmp = tempfile::tempdir().unwrap();
    let catalog = run(
        tmp.path(),
        &["query", "-f", "json", "sys::(*) |> (entities), entities ~= ~> {name, type}, type = \"DBPermanentTable\" |> (name)"],
        None,
    );
    assert!(catalog.status.success(), "{catalog:?}");
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&catalog.stdout).unwrap();
    if rows.iter().any(|row| row["name"] == "errors") {
        let read = run(tmp.path(), &["query", "sys.errors(*), # < 0"], None);
        assert!(
            read.status.success(),
            "catalog advertises an unreadable table: {read:?}"
        );
    }
}
