// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE SESSION IS A LINEAR PROGRAM, witnessed where every CLI, C-ABI and
//! test-ball statement arrives: one statement per call, its leading blocks
//! admitted before it, its trailing blocks after its effects, and every
//! admitted block routing a qualifier through the alias it captured.

use delightql_protocol::{ClientTerm, Handler, Orientation, Projection, ServerTerm};

use super::pump_tests::{fresh_system, relay_over, shared_sqlite, statement, TestRelay};

/// One statement's answer: its rows as text, or its refusal's message.
fn answer(relay: &mut TestRelay<'_>, text: &str) -> Result<Vec<Vec<Option<String>>>, String> {
    let handle = match relay.handle(statement(text)) {
        ServerTerm::Header { handle, .. } => handle,
        ServerTerm::Error(error) => return Err(String::from_utf8_lossy(error.message()).into()),
        other => panic!("{text}: unexpected response {other:?}"),
    };
    let mut rows = Vec::new();
    loop {
        match relay.handle(ClientTerm::Fetch {
            handle: handle.clone(),
            projection: Projection::All,
            count: u64::MAX,
            orientation: Orientation::Rows,
        }) {
            ServerTerm::Data { cells } => rows.extend(cells.into_iter().map(|row| {
                row.into_iter()
                    .map(|cell| cell.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
                    .collect()
            })),
            ServerTerm::End => return Ok(rows),
            ServerTerm::Error(error) => return Err(String::from_utf8_lossy(error.message()).into()),
            other => panic!("{text}: unexpected fetch response {other:?}"),
        }
    }
}

fn ran(relay: &mut TestRelay<'_>, text: &str) {
    if let Err(why) = answer(relay, text) {
        panic!("{text}: {why}");
    }
}

fn one_column(relay: &mut TestRelay<'_>, text: &str) -> Vec<String> {
    answer(relay, text)
        .unwrap_or_else(|why| panic!("{text}: {why}"))
        .into_iter()
        .map(|row| row[0].clone().expect("a value"))
        .collect()
}

/// A session whose base directory holds two item libraries and one library
/// whose `hex` is not SQLite's.
fn sources() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    for (file, text) in [
        ("a.dql", "items(*) :- _(item @ \"from_lib_a\")\n"),
        ("b.dql", "items(*) :- _(item @ \"from_lib_b\")\n"),
        ("h.dql", "hex:(x) :- x + 1\n"),
    ] {
        std::fs::write(dir.path().join(file), text).expect("write a source");
    }
    dir
}

fn based(relay: &mut TestRelay<'_>, dir: &tempfile::TempDir) {
    use crate::api::ServerRelay;
    relay
        .set_session_setting(
            crate::settings::BASE_DIRECTORY,
            Some(dir.path().to_str().expect("utf-8 tempdir")),
        )
        .expect("session base");
}

#[test]
fn a_block_trailing_an_enlist_is_admitted_in_its_lexical_world() {
    let dir = sources();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    based(&mut relay, &dir);
    ran(&mut relay, "consult!(\"h.dql\", \"lib::h\")(*)");
    ran(
        &mut relay,
        "enlist!(\"lib::h\")(*) (~~ddl seven(*) :- _(v @ hex:(10)) ~~)",
    );
    assert_eq!(one_column(&mut relay, "seven(*)"), ["11"]);
}

#[test]
fn a_block_leading_an_alias_is_admitted_before_it() {
    let dir = sources();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    based(&mut relay, &dir);
    ran(&mut relay, "consult!(\"a.dql\", \"lib::a\")(*)");
    ran(
        &mut relay,
        "(~~ddl report(*) :- la.items(*) ~~) alias!(\"lib::a\", \"la\")(*)",
    );
    let why = answer(&mut relay, "report(*)").expect_err("the block was admitted before la");
    assert!(why.contains("la.items"), "{why}");
    // The prompt's own alias stands.
    assert_eq!(one_column(&mut relay, "la.items(*)"), ["from_lib_a"]);
}

#[test]
fn a_statement_does_not_read_the_block_trailing_it() {
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    let why = answer(&mut relay, "tr(*) (~~ddl tr(*) :- _(x @ 1) ~~)")
        .expect_err("the block stands after the statement");
    assert!(why.contains("tr"), "{why}");
    // The refused statement's program stopped there: nothing was admitted.
    answer(&mut relay, "tr(*)").expect_err("no block was admitted after a refusal");
}

#[test]
fn a_block_refused_after_its_statement_ran_is_the_calls_answer() {
    let dir = sources();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    based(&mut relay, &dir);
    ran(&mut relay, "consult!(\"a.dql\", \"lib::a\")(*)");
    answer(
        &mut relay,
        "enlist!(\"lib::a\")(*) (~~ddl:\"_reserved\" f(*) :- _(x @ 1) ~~)",
    )
    .expect_err("a reserved child refuses");
    // The statement's effect stands, as it would had the block been the
    // next call.
    assert_eq!(one_column(&mut relay, "items(*)"), ["from_lib_a"]);
}

#[test]
fn delist_removes_its_enlist_edge_and_nothing_else() {
    let dir = sources();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    based(&mut relay, &dir);
    ran(&mut relay, "consult!(\"a.dql\", \"lib::a\")(*)");
    ran(&mut relay, "enlist!(\"lib::a\")(*)");
    ran(&mut relay, "alias!(\"lib::a\", \"la\")(*)");
    ran(&mut relay, "_(x @ 1) (~~ddl report(*) :- la.items(*) ~~)");
    ran(&mut relay, "delist!(\"lib::a\")(*)");
    answer(&mut relay, "items(*)").expect_err("the bare name lost its enlistment");
    assert_eq!(one_column(&mut relay, "la.items(*)"), ["from_lib_a"]);
    assert_eq!(one_column(&mut relay, "report(*)"), ["from_lib_a"]);
}

/// Whatever a later session does to the shorthand, an admitted body never
/// answers from the namespace the shorthand names afterwards. Removing the
/// captured target may refuse or may succeed; either way the body does not
/// follow `la` to `lib::b`.
#[test]
fn a_captured_alias_never_follows_the_shorthand_to_another_namespace() {
    let dir = sources();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, shared_sqlite());
    based(&mut relay, &dir);
    ran(&mut relay, "consult!(\"a.dql\", \"lib::a\")(*)");
    ran(&mut relay, "consult!(\"b.dql\", \"lib::b\")(*)");
    ran(&mut relay, "alias!(\"lib::a\", \"la\")(*)");
    ran(&mut relay, "_(x @ 1) (~~ddl report(*) :- la.items(*) ~~)");
    if answer(&mut relay, "unconsult!(\"lib::a\")(*)").is_ok() {
        ran(&mut relay, "alias!(\"lib::b\", \"la\")(*)");
        assert_eq!(one_column(&mut relay, "la.items(*)"), ["from_lib_b"]);
    }
    match answer(&mut relay, "report(*)") {
        Ok(rows) => assert_eq!(
            rows,
            [[Some("from_lib_a".to_string())]],
            "the body reads the namespace it captured"
        ),
        Err(why) => assert!(why.contains("la.items"), "{why}"),
    }
}

/// What a run's `main!` read, from the run's own payload: the marker its
/// `returning!` shipped.
fn run_saw(relay: &mut TestRelay<'_>, statement: &str) -> String {
    let rows = answer(relay, statement).unwrap_or_else(|why| panic!("{statement}: {why}"));
    let payload = rows
        .last()
        .and_then(|row| row.last().cloned().flatten())
        .unwrap_or_else(|| panic!("{statement}: the run shipped no payload"));
    ["before", "after", "lead"]
        .into_iter()
        .find(|marker| payload.contains(&format!("\"m\":\"{marker}\"")))
        .unwrap_or_else(|| panic!("{statement}: no marker in {payload}"))
        .to_string()
}

const SCRIPT: &str = "main!(*) :- home::m.marker(*) |> returning!(*)";

/// A whole-statement `run!` or `run_namespace!` is a program step like any
/// other: its `main!` reads the marker while it is still "before", and the
/// block trailing the run replaces it only afterwards.
#[test]
fn a_run_statement_admits_its_trailing_blocks_after_the_run() {
    for (setup, run) in [
        (None, "run!(\"seen.dql\")(*)"),
        (
            Some("consult!(\"seen.dql\", \"sc\")(*)"),
            "run_namespace!(sc)(*)",
        ),
    ] {
        let dir = sources();
        std::fs::write(dir.path().join("seen.dql"), format!("{SCRIPT}\n"))
            .expect("write the script");
        let mut system = fresh_system();
        let mut relay = relay_over(&mut system, shared_sqlite());
        based(&mut relay, &dir);
        ran(
            &mut relay,
            "_(x @ 1) (~~ddl:\"m\" marker(*) :- _(m @ \"before\") ~~)",
        );
        if let Some(setup) = setup {
            ran(&mut relay, setup);
        }
        let statement = format!("{run} (~~ddl:\"m\" marker(*) :- _(m @ \"after\") ~~)");
        assert_eq!(run_saw(&mut relay, &statement), "before", "{run}");
        assert_eq!(
            one_column(&mut relay, "home::m.marker(*)"),
            ["after"],
            "{run}"
        );
    }
}

/// Blocks leading a run statement are admitted before the run: here they
/// supply the marker, and for `run_namespace!` the `main!` it demands.
#[test]
fn a_run_statement_admits_its_leading_blocks_before_the_run() {
    for statement in [
        "(~~ddl:\"m\" marker(*) :- _(m @ \"lead\") ~~) run!(\"seen.dql\")(*)".to_string(),
        format!(
            "(~~ddl:\"m\" marker(*) :- _(m @ \"lead\") ~~) (~~ddl:\"sc\" {SCRIPT} ~~) \
             run_namespace!(\"home::sc\")(*)"
        ),
    ] {
        let dir = sources();
        std::fs::write(dir.path().join("seen.dql"), format!("{SCRIPT}\n"))
            .expect("write the script");
        let mut system = fresh_system();
        let mut relay = relay_over(&mut system, shared_sqlite());
        based(&mut relay, &dir);
        assert_eq!(run_saw(&mut relay, &statement), "lead", "{statement}");
    }
}

/// A run that refuses admits no block trailing it, on either entrance.
#[test]
fn a_refused_run_statement_admits_no_trailing_block() {
    for (setup, run) in [
        (None, "run!(\"missing.dql\")(*)"),
        (
            Some("consult!(\"a.dql\", \"nm\")(*)"),
            "run_namespace!(nm)(*)",
        ),
    ] {
        let dir = sources();
        let mut system = fresh_system();
        let mut relay = relay_over(&mut system, shared_sqlite());
        based(&mut relay, &dir);
        if let Some(setup) = setup {
            ran(&mut relay, setup);
        }
        answer(
            &mut relay,
            &format!("{run} (~~ddl stray(*) :- _(x @ 1) ~~)"),
        )
        .expect_err("the run refuses");
        answer(&mut relay, "stray(*)").expect_err("no block was admitted after the refusal");
    }
}
