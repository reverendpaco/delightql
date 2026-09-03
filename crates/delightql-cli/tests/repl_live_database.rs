// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The live REPL client database through the real handle: one physical
//! `repl::data` mount, projection-only public relations, the sealed
//! read/write boundary, the moved dot-command authority, and survival
//! across a Core session reset.

#![cfg(feature = "repl")]

use std::sync::Arc;

use delightql_cli::exec_ng::run_dql_query;
use delightql_cli::client::context::Mode;
use delightql_cli::client::database::{ClientDatabase, InputKind, InputOutcome, WriteOutcome};
use delightql_cli::client::mount::{install_repl_namespace, open_client_handle};

fn live() -> (Arc<ClientDatabase>, Box<dyn delightql_core::api::DqlHandle>) {
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("open the live database"));
    let mut handle = open_client_handle(&db).expect("open the repl handle");
    install_repl_namespace(&mut *handle).expect("install repl::*");
    (db, handle)
}

/// The four public relations answer through the handle, and the surface
/// projection is exhaustive over the registry.
#[test]
fn the_public_relations_answer_and_the_surface_is_exhaustive() {
    let (db, mut handle) = live();

    // A host write lands before the reads so every relation has a row.
    assert!(matches!(
        db.set_option("output_format", Some("table".into()), "enum", None, "startup"),
        WriteOutcome::Applied
    ));
    let (id, _) = db.record_input(InputKind::Dql, "users(*)");
    db.close_input(id, InputOutcome::Succeeded, None, Some("SELECT 1".into()), Some(1.0));

    let mut session = handle.session().expect("session");
    let surface = run_dql_query("repl::surface.dot_command(*)", &mut *session).unwrap();
    let expected: std::collections::BTreeSet<String> =
        delightql_cli::repl::commands::dot_command_spellings()
            .map(String::from)
            .collect();
    let spelling_col = surface
        .columns
        .iter()
        .position(|c| c == "spelling")
        .expect("spelling column");
    let actual: std::collections::BTreeSet<String> = surface
        .rows
        .iter()
        .map(|r| r[spelling_col].clone())
        .collect();
    assert_eq!(actual, expected, "every registry spelling, nothing else");

    let options = run_dql_query("repl::config.`option`(*)", &mut *session).unwrap();
    assert_eq!(options.rows.len(), 1);
    let history = run_dql_query("repl::history.input(*)", &mut *session).unwrap();
    assert_eq!(history.rows.len(), 1);
    let incidents = run_dql_query("repl::errors.incident(*)", &mut *session).unwrap();
    assert!(incidents.rows.is_empty());
}

/// One physical connection: a join among public REPL relations plans and
/// answers — no cross-connection road involved.
#[test]
fn public_relations_join_on_the_one_connection() {
    let (db, mut handle) = live();
    let (id, _) = db.record_input(InputKind::DotCommand, ".help");
    db.close_input(id, InputOutcome::Succeeded, None, None, None);

    let mut session = handle.session().expect("session");
    let joined = run_dql_query(
        "repl::history.input(*), kind = \"dot_command\", \
         repl::surface.dot_command(*), spelling = input",
        &mut *session,
    )
    .expect("the join must plan on the one live connection");
    assert_eq!(joined.rows.len(), 1, "the .help row joins its surface row");
}

/// The sealed boundary holds against DQL: DML against the mounted data
/// namespace is refused, and the refusal is the authorizer's, not a maybe.
#[test]
fn dql_mutation_of_the_live_database_is_denied() {
    let (_db, mut handle) = live();
    let mut session = handle.session().expect("session");
    let attempt = run_dql_query(
        "_(spelling @ \".evil\", canonical_name @ \".evil\", is_alias @ 0, \
           args @ \"\", section @ \"x\", summary @ \"x\", example @ \"\") \
         |> insert!(repl::data.dot_command(*))(*)",
        &mut *session,
    );
    assert!(attempt.is_err(), "row DML against repl::data must refuse");

    // And the rows are untouched.
    let surface = run_dql_query("repl::surface.dot_command(*)", &mut *session).unwrap();
    assert!(!surface.rows.iter().any(|r| r.iter().any(|c| c == ".evil")));
}

/// The predecessor road is gone: `cli::surface` carries no dot_command
/// relation any more — the interactive surface has ONE authority.
#[test]
fn cli_surface_no_longer_carries_dot_commands() {
    let (_db, mut handle) = live();
    let mut session = handle.session().expect("session");
    run_dql_query(
        "mount!(\"delightql-bytes://surface\", \"cli::surface\")(*)",
        &mut *session,
    )
    .expect("mount cli::surface");
    let gone = run_dql_query("cli::surface.dot_command(*)", &mut *session);
    assert!(gone.is_err(), "cli::surface.dot_command must not resolve");
    // The remaining CLI surface still answers.
    let commands = run_dql_query("cli::surface.command(*)", &mut *session).unwrap();
    assert!(!commands.rows.is_empty());
}

/// Core session reset loses the catalog, not the client database: after
/// recovery and remount the same rows answer.
#[test]
fn a_session_reset_then_remount_retains_the_rows() {
    let (db, mut handle) = live();
    let (id, _) = db.record_input(InputKind::Dql, "users(*) |> (id)");
    db.close_input(id, InputOutcome::Failed, Some("no such table".into()), None, None);

    handle.recover_session().expect("reset");
    {
        let mut session = handle.session().expect("session");
        assert!(
            run_dql_query("repl::history.input(*)", &mut *session).is_err(),
            "the reset dropped the mount"
        );
    }
    install_repl_namespace(&mut *handle).expect("remount after recovery");
    let mut session = handle.session().expect("session");
    let history = run_dql_query("repl::history.input(*)", &mut *session).unwrap();
    assert_eq!(history.rows.len(), 1, "the ledger survived the reset");
    let input_col = history.columns.iter().position(|c| c == "input").unwrap();
    assert_eq!(history.rows[0][input_col], "users(*) |> (id)");
}

/// Configuration agreement: one typed operation changes the typed value,
/// the TUI snapshot, and the option row together; an invalid value changes
/// none of them.
#[test]
fn configuration_operations_agree_across_all_three_faces() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{handle_dot_command, ReplState};

    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::new(ClientDatabase::open_on(Mode::Other).unwrap()))).expect("repl state");
    state.set_output_format(OutputFormat::Json, ".format");
    state.set_zebra_mode(3, ".zebra").expect("3 is lawful");

    assert_eq!(state.config().output_format(), OutputFormat::Json);
    assert_eq!(state.config().zebra_mode(), Some(3));
    assert_eq!(state.shared_info.config_output_format, "json");
    assert_eq!(state.shared_info.config_zebra_mode, Some(3));

    let option_row = |state: &ReplState, name: &str| -> (String, String) {
        let mut handle = state.dql_handle.lock().unwrap();
        let mut session = handle.session().expect("session");
        let rows = run_dql_query(
            &format!("repl::config.`option`(*), name = \"{name}\""),
            &mut *session,
        )
        .expect("option row");
        let value = rows.columns.iter().position(|c| c == "value").unwrap();
        let source = rows.columns.iter().position(|c| c == "source").unwrap();
        assert_eq!(rows.rows.len(), 1, "one row per option");
        (rows.rows[0][value].clone(), rows.rows[0][source].clone())
    };
    assert_eq!(
        option_row(&state, "output_format"),
        ("json".to_string(), ".format".to_string())
    );
    assert_eq!(
        option_row(&state, "zebra_columns"),
        ("3".to_string(), ".zebra".to_string())
    );

    // Invalid value: refused, and NOTHING moved.
    assert!(state.set_zebra_mode(9, ".zebra").is_err());
    assert_eq!(state.config().zebra_mode(), Some(3));
    assert_eq!(state.shared_info.config_zebra_mode, Some(3));
    assert_eq!(
        option_row(&state, "zebra_columns"),
        ("3".to_string(), ".zebra".to_string())
    );

    // Every effective parser budget is projected as an option row.
    for operation in delightql_cli::repl::config::ReplParserOperation::ALL {
        let (value, _) = option_row(&state, operation.option_name());
        assert_eq!(
            value,
            state
                .config()
                .parser_budgets()
                .effective(operation)
                .as_millis()
                .to_string()
        );
    }

    // The breaker crosses the same three faces, driven by the dot command:
    // enabled by default with the startup source; `.repl helpers off/on`
    // moves the typed policy, the TUI snapshot, and the queryable row
    // together, stamping the manual source.
    assert_eq!(
        option_row(&state, "editor_parser_helpers"),
        ("true".to_string(), "startup".to_string())
    );
    assert!(state.shared_info.config_editor_helpers);
    handle_dot_command(".repl helpers off", &mut state).expect("known command");
    assert!(!state.config().editor_helpers_enabled());
    assert!(!state.shared_info.config_editor_helpers);
    assert_eq!(
        option_row(&state, "editor_parser_helpers"),
        ("false".to_string(), ".repl helpers".to_string())
    );
    handle_dot_command(".repl helpers status", &mut state).expect("status changes nothing");
    assert!(!state.config().editor_helpers_enabled());
    handle_dot_command(".repl helpers on", &mut state).expect("known command");
    assert!(state.config().editor_helpers_enabled());
    assert!(state.shared_info.config_editor_helpers);
    assert_eq!(
        option_row(&state, "editor_parser_helpers"),
        ("true".to_string(), ".repl helpers".to_string())
    );
}

/// History authority: dot commands and queries share the ONE ordered
/// ledger; started rows close with the dispatched outcome; unknown
/// spellings are refused.
#[test]
fn one_ordered_ledger_records_dot_commands_and_queries() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{handle_dot_command, process_query, ReplState};

    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::new(ClientDatabase::open_on(Mode::Other).unwrap()))).expect("repl state");
    // The preflight gate fails closed, so the DQL submission below needs a
    // SERVING containment worker — the real dql binary, not the test
    // harness the default controller would spawn as current_exe.
    state.parser_worker = std::sync::Arc::new(
        delightql_cli::repl::parser_worker::ParserWorkerController::new_with_executable(
            std::path::PathBuf::from(env!("CARGO_BIN_EXE_dql")),
            delightql_cli::repl::config::ReplParserBudgets::measured_defaults(),
            std::sync::Arc::clone(state.config().editor_helper_policy()),
            state.repl_db.clone(),
            None,
        ),
    );
    handle_dot_command(".format json", &mut state).expect("known command");
    handle_dot_command(".nonsense", &mut state).expect("unknown prints, continues");
    let flag = std::sync::atomic::AtomicBool::new(false);
    let _ = process_query("no_such_table(*)", &mut state, &flag);

    let mut handle = state.dql_handle.lock().unwrap();
    let mut session = handle.session().expect("session");
    let rows = run_dql_query("repl::history.input(*)", &mut *session).unwrap();
    let col = |name: &str| rows.columns.iter().position(|c| c == name).unwrap();
    let (id_c, kind_c, input_c, outcome_c, err_c) = (
        col("id"),
        col("kind"),
        col("input"),
        col("outcome"),
        col("error"),
    );
    let mut ledger: Vec<(i64, String, String, String, String)> = rows
        .rows
        .iter()
        .map(|r| {
            (
                r[id_c].parse::<i64>().unwrap(),
                r[kind_c].clone(),
                r[input_c].clone(),
                r[outcome_c].clone(),
                r[err_c].clone(),
            )
        })
        .collect();
    ledger.sort_by_key(|(id, ..)| *id);

    assert_eq!(ledger.len(), 3, "three inputs, one ledger");
    assert_eq!(
        (&ledger[0].1[..], &ledger[0].2[..], &ledger[0].3[..]),
        ("dot_command", ".format json", "succeeded")
    );
    assert_eq!(
        (&ledger[1].1[..], &ledger[1].2[..], &ledger[1].3[..]),
        ("dot_command", ".nonsense", "refused")
    );
    assert_eq!((&ledger[2].1[..], &ledger[2].2[..]), ("dql", "no_such_table(*)"));
    assert_eq!(ledger[2].3, "failed");
    assert!(!ledger[2].4.is_empty(), "the failure carries its error");
}

/// `.bug` ships the session files, the serialized client database, and
/// the session's database files in one archive beside the session files;
/// the description is an info row that reaches error.log.
#[test]
fn the_bug_tarball_carries_the_session_files_and_the_client_database() {
    use delightql_cli::client::bug::write_bug_report;
    use delightql_cli::client::database::{ClientDatabase, InputKind, InputOutcome};

    let state_dir = tempfile::tempdir().unwrap();
    std::env::set_var("DQL_STATE_DIR", state_dir.path());
    let user_db = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(user_db.path(), b"").unwrap();

    let db = Arc::new(ClientDatabase::open_on(Mode::Other).unwrap());
    let mut handle = open_client_handle(&db).expect("handle");
    install_repl_namespace(&mut *handle).expect("install repl::*");
    let (id, _) = db.record_input(InputKind::Dql, "users(*)");
    db.close_input(id, InputOutcome::Succeeded, None, Some("SELECT 1".into()), Some(1.0));

    let report = write_bug_report(&db, &mut *handle, Some("the join drops rows"), Some(user_db.path()))
        .expect("bug report");
    assert!(report.archive.starts_with(state_dir.path()));
    assert_eq!(report.databases.len(), 1, "the primary database ships: {:?}", report.databases);

    let file = std::fs::File::open(&report.archive).unwrap();
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(file));
    let names: Vec<String> = archive
        .entries()
        .unwrap()
        .map(|e| e.unwrap().path().unwrap().display().to_string())
        .collect();
    let stamp = report.files.stamp;
    for expected in [
        format!("bug-{stamp}/error.log.{stamp}"),
        format!("bug-{stamp}/context.{stamp}"),
        format!("bug-{stamp}/replay-script.{stamp}"),
        format!("bug-{stamp}/repl.sqlite"),
    ] {
        assert!(names.contains(&expected), "{expected} missing from {names:?}");
    }
    assert!(names.iter().any(|n| n.starts_with(&format!("bug-{stamp}/db/"))), "{names:?}");
    assert!(!names.iter().any(|n| n.ends_with("manifest.json")), "no manifest: the files are the record");

    let log = std::fs::read_to_string(&report.files.error_log).unwrap();
    assert!(log.contains("delightql-error://client/report/description"), "{log}");
    assert!(log.contains("the join drops rows"));
    assert!(log.contains("\"kind\": \"info\""));
    let script = std::fs::read_to_string(&report.files.replay_script).unwrap();
    assert!(script.contains("# 1 dql\nusers(*)\n"), "{script}");
}

/// One COMPLETE ordered ledger: direct-mode SQL, `.sql` one-offs and `.dql`
/// one-offs each get their own row beside their dot-command row.
#[test]
fn every_interactive_submission_road_crosses_the_one_ledger() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{handle_dot_command, process_query, ReplState};

    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::new(ClientDatabase::open_on(Mode::Other).unwrap()))).expect("repl state");
    state.parser_worker = std::sync::Arc::new(
        delightql_cli::repl::parser_worker::ParserWorkerController::new_with_executable(
            std::path::PathBuf::from(env!("CARGO_BIN_EXE_dql")),
            delightql_cli::repl::config::ReplParserBudgets::measured_defaults(),
            std::sync::Arc::clone(state.config().editor_helper_policy()),
            state.repl_db.clone(),
            None,
        ),
    );
    let flag = std::sync::atomic::AtomicBool::new(false);
    // The one-off propagates its query error (the REPL loop prints it and
    // continues); the dot row closes failed, the nested dql row too.
    assert!(handle_dot_command(".dql users(*)", &mut state).is_err());
    handle_dot_command(".sql SELECT 1", &mut state).unwrap();
    handle_dot_command(".sql", &mut state).unwrap(); // switch to SQL mode
    let _ = process_query("SELECT 2", &mut state, &flag);
    // A removed spelling is refused, and the refusal is a ledger row too.
    handle_dot_command(".file /tmp/anything.dql", &mut state).unwrap();

    let db = state.repl_db.as_ref().unwrap();
    let ledger: Vec<(String, String, String)> = db
        .history_rows()
        .unwrap()
        .into_iter()
        .map(|row| (row.kind, row.input, row.outcome))
        .collect();
    let expect = |kind: &str, input: &str, outcome: &str| {
        (kind.to_string(), input.to_string(), outcome.to_string())
    };
    assert_eq!(
        ledger,
        vec![
            expect("dot_command", ".dql users(*)", "failed"),
            expect("dql", "users(*)", "failed"), // no such table — but RECORDED
            expect("dot_command", ".sql SELECT 1", "succeeded"),
            expect("sql", "SELECT 1", "succeeded"),
            expect("dot_command", ".sql", "succeeded"),
            expect("sql", "SELECT 2", "succeeded"),
            expect("dot_command", ".file /tmp/anything.dql", "refused"),
        ],
        "one ordered authority over every interactive submission road"
    );
}

/// The book's REPL page lists exactly the registry's dot commands: a
/// command added, removed or renamed without the page is a red test,
/// not a stale manual.
#[test]
fn the_book_repl_page_names_exactly_the_registry() {
    let page = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../book/manual/repl.md"),
    )
    .expect("book/manual/repl.md");
    let table = page
        .split("## Dot commands")
        .nth(1)
        .and_then(|rest| rest.split("\n## ").next())
        .expect("the Dot commands section");
    // Every `.word` spelled in backticks inside the section's table rows.
    let mut in_page: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for line in table.lines().filter(|l| l.starts_with('|')) {
        let cell = line.trim_start_matches('|').split('|').next().unwrap_or("");
        for token in cell.split('`').skip(1).step_by(2) {
            if let Some(word) = token.split_whitespace().next() {
                if word.starts_with('.') && word.len() > 1 {
                    in_page.insert(word.trim_end_matches(',').to_string());
                }
            }
        }
    }
    let registry: std::collections::BTreeSet<String> =
        delightql_cli::repl::commands::dot_command_spellings()
            .map(String::from)
            .collect();
    assert_eq!(in_page, registry, "book/manual/repl.md drifted from DOT_COMMANDS");
}
