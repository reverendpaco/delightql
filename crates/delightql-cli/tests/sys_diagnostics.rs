// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `sys::diagnostics.finding`: the session's own refusals and selftest
//! findings, queryable through an ordinary handle.

use delightql_cli::exec_ng::run_dql_query;

#[test]
fn a_refusal_is_a_finding_row_with_its_input() {
    let mut handle = delightql_cli::connection::open_handle().expect("handle");
    let mut session = handle.session().expect("session");

    let before = run_dql_query("sys::diagnostics.finding(*)", &mut *session).unwrap();
    assert!(before.rows.is_empty(), "a fresh session has no findings");

    let refused = run_dql_query("nosuch_relation(*)", &mut *session);
    assert!(refused.is_err(), "the refusal reaches the caller");

    let rows = run_dql_query(
        "sys::diagnostics.finding(*) |> (kind, uri, provider, input, message)",
        &mut *session,
    )
    .unwrap();
    assert_eq!(rows.rows.len(), 1, "{:?}", rows.rows);
    let col = |name: &str| rows.columns.iter().position(|c| c == name).unwrap();
    let row = &rows.rows[0];
    assert_eq!(row[col("kind")], "error");
    assert_eq!(row[col("uri")], "delightql-error://semantic/resolution/table");
    assert_eq!(row[col("provider")], "session", "recorded where the error crossed to the client");
    assert_eq!(row[col("input")], "nosuch_relation(*)", "the exact submission");
    assert!(row[col("message")].contains("nosuch_relation"));
}

#[test]
fn the_namespace_is_published_and_read_only() {
    let mut handle = delightql_cli::connection::open_handle().expect("handle");
    let mut session = handle.session().expect("session");
    let ns = run_dql_query(
        "sys::ns.namespace(*), fq_name = \"sys::diagnostics\" |> (kind, writable)",
        &mut *session,
    )
    .unwrap();
    assert_eq!(ns.rows.len(), 1);
    let col = |name: &str| ns.columns.iter().position(|c| c == name).unwrap();
    assert_eq!(ns.rows[0][col("kind")], "system");
    assert_eq!(ns.rows[0][col("writable")], "0");
}

/// Healthy selftest findings (`Ok`) are not rows; the relation stays a
/// record of problems.
#[test]
fn ok_selftest_findings_are_not_rows() {
    let mut handle = delightql_cli::connection::open_handle().expect("handle");
    let findings = handle.selftest();
    let mut session = handle.session().expect("session");
    let rows = run_dql_query(
        "sys::diagnostics.finding(*), provider != \"session\"",
        &mut *session,
    )
    .unwrap();
    let problems = findings
        .iter()
        .filter(|f| f.severity != delightql_core::diagnostics::Severity::Ok)
        .count();
    assert_eq!(rows.rows.len(), problems, "one row per non-Ok finding");
}
