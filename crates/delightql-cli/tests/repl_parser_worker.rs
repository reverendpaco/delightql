// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The parser containment boundary against the REAL `dql` binary: the framed
//! wire, the cooperative and hard layers, generation discipline, timeout
//! capture and deduplication, worker cleanup, and the hidden entrance's
//! absence from the public surface.
//!
//! Wall-clock assertions here are containment bounds with generous outer
//! deadlines, never tight timing claims.

#![cfg(feature = "repl")]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use delightql_cli::repl::config::{ReplEditorHelperPolicy, ReplParserBudgets, ReplParserOperation};
use delightql_cli::client::context::Mode;
use delightql_cli::client::database::ClientDatabase;
use delightql_cli::repl::parser_worker::{ParserWorkerController, ProbeOutcome};
use delightql_cli::repl::worker::{read_frame, write_frame, WorkerRequest, WorkerResponse, WorkerResult};

/// The minimized deterministic freeze trigger from the diagnosis: the
/// c2rust runtime's error recovery loops on it below the cooperative
/// checkpoints, so only the process boundary contains it.
const TOXIC: &str = "(~~ddln(*)_(1):a a(),|1|<";

/// A generous outer deadline for any single contained operation.
const OUTER_DEADLINE: Duration = Duration::from_secs(20);

fn dql_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_dql"))
}

fn controller(db: Option<Arc<ClientDatabase>>) -> ParserWorkerController {
    controller_with_policy(db).0
}

fn controller_with_policy(
    db: Option<Arc<ClientDatabase>>,
) -> (ParserWorkerController, Arc<ReplEditorHelperPolicy>) {
    let policy = ReplEditorHelperPolicy::new_enabled();
    let worker = ParserWorkerController::new_with_executable(
        dql_exe(),
        ReplParserBudgets::measured_defaults(),
        Arc::clone(&policy),
        db,
        None,
    );
    (worker, policy)
}

/// The projected breaker option row `(value, source)`, read through a
/// serialized snapshot like the timeout evidence.
fn option_row(db: &ClientDatabase, name: &str) -> (String, String) {
    let image = db.serialize().expect("serialize");
    let mut conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.deserialize_read_exact("main", &image[..], image.len(), false)
        .unwrap();
    conn.query_row(
        "SELECT value, source FROM option WHERE name = ?1",
        [name],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .expect("the option row exists")
}

fn well_formed(worker: &ParserWorkerController, prefix: &str) -> Option<bool> {
    match worker.probe(ReplParserOperation::PromptWellFormed, prefix, None) {
        ProbeOutcome::Answer(WorkerResult::WellFormed { well_formed }) => Some(well_formed),
        _ => None,
    }
}

/// The prompt verdict through the real worker: non-monotonic prefix
/// well-formedness, exactly as the in-process probe answered it.
#[test]
fn prefix_wellformedness_is_not_monotonic_through_the_worker() {
    let worker = controller(None);
    let line = "users(*) |> (id)";
    let ladder: Vec<bool> = (0..=line.len())
        .filter(|i| line.is_char_boundary(*i))
        .map(|i| well_formed(&worker, &line[..i]).expect("the worker answers"))
        .collect();
    let flips = ladder.windows(2).filter(|w| w[0] != w[1]).count();
    assert!(flips >= 3, "expected several transitions, saw {flips}");
    assert!(well_formed(&worker, line).unwrap());
    assert_eq!(well_formed(&worker, ""), Some(false), "empty is not runnable");
    assert_eq!(well_formed(&worker, ".help"), Some(false), "dot-commands are not queries");
}

/// Unicode and multiline inputs round-trip the framed wire.
#[test]
fn multiline_unicode_requests_answer() {
    let worker = controller(None);
    let input = "users(*)\n  |> (naïve, δ→ε)\n";
    assert!(well_formed(&worker, input).is_some(), "the wire must answer");
    match worker.probe(ReplParserOperation::SyntaxHighlight, input, None) {
        ProbeOutcome::Answer(WorkerResult::Highlights { .. }) => {}
        other => panic!(
            "highlight must answer spans: {:?}",
            matches!(other, ProbeOutcome::TimedOut)
        ),
    }
}

/// The real minimized trigger, contained: the probe returns within the
/// outer deadline, the exact input lands in the timeout table, and the
/// controller keeps answering. Measured on this runtime, the cooperative
/// deadline reaches the trigger (the recovery loop crosses the runtime's
/// checkpoints under the progress-callback option), so containment is the
/// cooperative road and the worker survives; were it ever to stop polling,
/// the kill road below (`a_nonresponsive_worker_is_killed...`) is the
/// deterministic proof of the hard boundary.
#[test]
fn the_real_trigger_is_contained_captured_and_survivable() {
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("live database"));
    let (worker, policy) = controller_with_policy(Some(Arc::clone(&db)));

    // Warm the worker so the trigger meets a healthy generation-1 process.
    assert_eq!(well_formed(&worker, "users(*)"), Some(true));
    assert_eq!(worker.current_generation(), 1);

    let started = Instant::now();
    let outcome = worker.probe(ReplParserOperation::PromptWellFormed, TOXIC, None);
    assert!(
        started.elapsed() < OUTER_DEADLINE,
        "containment must bound the trigger"
    );
    assert!(matches!(outcome, ProbeOutcome::TimedOut));

    // The evidence: exact input, a ruled containment spelling, the
    // effective budget, and the build identities.
    let rows = incident_rows(&db);
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.input, TOXIC, "the EXACT input is the evidence");
    assert_eq!(row.operation, "prompt_well_formed");
    assert!(
        row.containment == "cooperative_cancel" || row.containment == "worker_kill",
        "a ruled containment spelling, got {}",
        row.containment
    );
    if row.containment == "cooperative_cancel" {
        assert_eq!(worker.current_generation(), 1, "a cooperative worker survives");
    } else {
        assert_eq!(worker.current_generation(), 2, "a killed worker is replaced");
    }
    assert_eq!(row.occurrence_count, 1);
    assert_eq!(row.worker_generation, 1, "the generation that served the request");
    assert_eq!(
        row.budget_ms as u128,
        ReplParserBudgets::measured_defaults()
            .effective(ReplParserOperation::PromptWellFormed)
            .as_millis()
    );
    assert_eq!(row.grammar_fingerprint, delightql_cst::GRAMMAR_FINGERPRINT);
    assert_eq!(row.parser_runtime, delightql_cst::PARSER_RUNTIME);

    // The first optional incident opened the breaker; containment is
    // survivable: re-arm (the manual road) and the controller answers again.
    assert!(
        !policy.helpers_enabled(),
        "the first optional incident trips the breaker"
    );
    policy.set_enabled(true);
    assert_eq!(well_formed(&worker, "users(*)"), Some(true));

    // Deduplication: the identical toxic input under the same containment
    // upserts one row. Containment is scheduler-dependent (a cooperative
    // reply that misses the grace window becomes a kill), so the pin is
    // key-shaped, not count-of-rows-shaped: total occurrences sum to 2,
    // and no two rows share a (operation, containment) key.
    assert!(matches!(
        worker.probe(ReplParserOperation::PromptWellFormed, TOXIC, None),
        ProbeOutcome::TimedOut
    ));
    let rows = incident_rows(&db);
    let prompt_rows: Vec<_> = rows
        .iter()
        .filter(|r| r.operation == "prompt_well_formed")
        .collect();
    assert_eq!(
        prompt_rows.iter().map(|r| r.occurrence_count).sum::<i64>(),
        2,
        "two encounters, deduplicated per specimen key"
    );
    let mut keys: Vec<_> = prompt_rows
        .iter()
        .map(|r| (r.operation.clone(), r.containment.clone()))
        .collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), prompt_rows.len(), "one row per specimen key");

    // A different operation over the same bytes is DIFFERENT evidence —
    // and a mandatory-preflight incident refuses without touching helper
    // state: re-arm first, and the helpers stay enabled through it.
    policy.set_enabled(true);
    assert!(matches!(
        worker.probe(ReplParserOperation::SubmissionPreflight, TOXIC, None),
        ProbeOutcome::TimedOut
    ));
    assert!(
        policy.helpers_enabled(),
        "preflight incidents never trip the optional breaker"
    );
    let rows = incident_rows(&db);
    assert!(
        rows.iter().any(|r| r.operation == "submission_preflight"),
        "operation participates in the specimen key"
    );
}

/// The HARD boundary, deterministically: a worker that never answers (the
/// hidden test hook makes it read the request and park) is killed and
/// reaped within the outer deadline, the incident records worker_kill, a
/// replacement is spawned, and the editor road gets control back.
#[test]
fn a_nonresponsive_worker_is_killed_reaped_and_replaced() {
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("live database"));
    let (worker, policy) = controller_with_policy(Some(Arc::clone(&db)));

    worker.hang_workers_for_tests();
    let started = Instant::now();
    let outcome = worker.probe(ReplParserOperation::PromptWellFormed, "users(*)", None);
    let contained_in = started.elapsed();

    assert!(matches!(outcome, ProbeOutcome::TimedOut));
    assert!(contained_in < OUTER_DEADLINE, "the kill road must bound the wait");
    assert_eq!(worker.current_generation(), 2, "killed and replaced");

    let rows = incident_rows(&db);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].containment, "worker_kill");
    assert_eq!(rows[0].worker_generation, 1);
    assert_eq!(rows[0].input, "users(*)", "the exact timed-out input");

    // The first optional incident opened the breaker: further optional
    // probes answer Disabled instantly — no worker contact, no new
    // incident, no third generation.
    assert!(!policy.helpers_enabled(), "the kill tripped the breaker");
    let started = Instant::now();
    assert!(matches!(
        worker.probe(ReplParserOperation::PromptWellFormed, "users(*) |> (id)", None),
        ProbeOutcome::Disabled
    ));
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "a disabled probe answers instantly"
    );
    assert_eq!(worker.current_generation(), 2, "no probe crossed, no spawn");
    assert_eq!(incident_rows(&db).len(), 1, "no incident while disabled");

    // The replacement hangs too (the seam is controller-wide), so prove
    // survivability structurally on the MANDATORY road, which the breaker
    // never gates: a second probe is contained the same bounded way — the
    // editor never waits unboundedly.
    let started = Instant::now();
    assert!(matches!(
        worker.probe(ReplParserOperation::SubmissionPreflight, "users(*) |> (id)", None),
        ProbeOutcome::TimedOut
    ));
    assert!(started.elapsed() < OUTER_DEADLINE);
    assert_eq!(worker.current_generation(), 3, "each kill spawns a replacement");
}

#[derive(Debug)]
struct IncidentRow {
    kind: String,
    road: String,
    operation: String,
    entrance: String,
    input: String,
    containment: String,
    occurrence_count: i64,
    worker_generation: i64,
    budget_ms: i64,
    parser_runtime: String,
    grammar_fingerprint: String,
}

fn incident_rows(db: &ClientDatabase) -> Vec<IncidentRow> {
    // Read through a serialized snapshot: the test needs no window and no
    // access to the private connection.
    let image = db.serialize().expect("serialize");
    let mut conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.deserialize_read_exact("main", &image[..], image.len(), false)
        .unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT kind, road, COALESCE(operation, ''), COALESCE(entrance, ''),
                    COALESCE(input, ''), COALESCE(containment, ''), occurrence_count,
                    COALESCE(worker_generation, -1), COALESCE(budget_ms, -1),
                    COALESCE(parser_runtime, ''), COALESCE(grammar_fingerprint, '')
             FROM incident ORDER BY id",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            Ok(IncidentRow {
                kind: row.get(0)?,
                road: row.get(1)?,
                operation: row.get(2)?,
                entrance: row.get(3)?,
                input: row.get(4)?,
                containment: row.get(5)?,
                occurrence_count: row.get(6)?,
                worker_generation: row.get(7)?,
                budget_ms: row.get(8)?,
                parser_runtime: row.get(9)?,
                grammar_fingerprint: row.get(10)?,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(stmt);
    rows
}

/// A submission preflight that times out refuses the submission before the
/// in-process compiler sees the bytes, closes the ledger row as refused,
/// and captures the incident.
#[test]
fn a_timed_out_preflight_refuses_the_submission() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{process_query, ReplState};

    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::new(ClientDatabase::open_on(Mode::Other).unwrap()))).expect("repl state");
    state.parser_worker = Arc::new(ParserWorkerController::new_with_executable(
        dql_exe(),
        ReplParserBudgets::measured_defaults(),
        Arc::clone(state.config().editor_helper_policy()),
        state.repl_db.clone(),
        None,
    ));

    let flag = std::sync::atomic::AtomicBool::new(false);
    let started = Instant::now();
    let result = process_query(TOXIC, &mut state, &flag);
    assert!(started.elapsed() < OUTER_DEADLINE, "preflight must bound the trigger");
    assert!(result.is_ok(), "a refused submission is not a REPL error");

    let db = state.repl_db.as_ref().expect("live database");
    let history = db.history_rows().expect("ledger");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].outcome, "refused");
    assert_eq!(history[0].input, TOXIC);

    // Two rows: the budget incident with its worker evidence, and the
    // refusal it caused — both carrying the exact input.
    let rows = incident_rows(db);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0].operation, "submission_preflight");
    assert_eq!(rows[0].input, TOXIC);
    assert_eq!((rows[1].kind.as_str(), rows[1].road.as_str()), ("error", "preflight"));
    assert_eq!(rows[1].input, TOXIC);

    // The mandatory incident left the optional breaker alone.
    assert!(
        state.config().editor_helpers_enabled(),
        "a preflight timeout never trips the optional breaker"
    );
    let (value, source) = option_row(db, "editor_parser_helpers");
    assert_eq!((value.as_str(), source.as_str()), ("true", "startup"));
}

/// Worker cleanup: dropping the controller kills and reaps its worker; no
/// process is left behind.
#[test]
fn dropping_the_controller_leaves_no_worker_behind() {
    let worker = controller(None);
    assert_eq!(well_formed(&worker, "users(*)"), Some(true));
    let pid = worker.current_worker_pid().expect("a live worker");
    assert!(
        std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "the worker runs while the controller lives"
    );
    drop(worker);
    // Reaped means GONE from the process table (not a zombie).
    let deadline = Instant::now() + OUTER_DEADLINE;
    loop {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"));
        match stat {
            Err(_) => break,
            Ok(contents) if contents.split_whitespace().nth(2) == Some("Z") => {
                panic!("the worker was left a zombie")
            }
            Ok(_) if Instant::now() > deadline => panic!("the worker outlived its controller"),
            Ok(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
}

/// Manual off: every optional callback takes its neutral fallback without
/// spawning or contacting a worker; manual on re-enables optional probing.
#[test]
fn manual_off_takes_neutral_fallbacks_without_a_worker() {
    let (worker, policy) = controller_with_policy(None);
    policy.set_enabled(false);
    for operation in [
        ReplParserOperation::PromptWellFormed,
        ReplParserOperation::SyntaxHighlight,
        ReplParserOperation::ContinuationNavigation,
    ] {
        assert!(matches!(
            worker.probe(operation, "users(*)", None),
            ProbeOutcome::Disabled
        ));
    }
    assert_eq!(worker.current_generation(), 0, "no worker was ever spawned");
    assert!(worker.current_worker_pid().is_none());

    policy.set_enabled(true);
    assert_eq!(well_formed(&worker, "users(*)"), Some(true));
    assert_eq!(worker.current_generation(), 1, "manual on probes again");
}

/// An optional worker failure (here: spawn refusal) trips the breaker
/// WITHOUT fabricating a timeout record, and projects the false option row
/// with the distinct worker-failure source.
#[test]
fn an_optional_worker_failure_trips_without_fabricating_evidence() {
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("live database"));
    let policy = ReplEditorHelperPolicy::new_enabled();
    let worker = ParserWorkerController::new_with_executable(
        PathBuf::from("/nonexistent/dql-worker-binary"),
        ReplParserBudgets::measured_defaults(),
        Arc::clone(&policy),
        Some(Arc::clone(&db)),
        None,
    );
    assert!(matches!(
        worker.probe(ReplParserOperation::SyntaxHighlight, "users(*)", None),
        ProbeOutcome::Unavailable
    ));
    assert!(!policy.helpers_enabled(), "the failure disables the helpers");
    assert!(incident_rows(&db).is_empty(), "no fabricated timeout evidence");
    let (value, source) = option_row(&db, "editor_parser_helpers");
    assert_eq!(value, "false");
    assert_eq!(source, "auto:syntax_highlight worker_failure");
}

/// A deterministic hanging optional probe: ONE incident, the breaker
/// disabled with the incident reference in the projected source, and
/// further redraw callbacks minting no more incidents while disabled.
#[test]
fn a_tripped_breaker_projects_the_incident_reference_and_goes_quiet() {
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("live database"));
    let (worker, policy) = controller_with_policy(Some(Arc::clone(&db)));
    worker.hang_workers_for_tests();

    assert!(matches!(
        worker.probe(ReplParserOperation::ContinuationNavigation, "users(*)", None),
        ProbeOutcome::TimedOut
    ));
    assert!(!policy.helpers_enabled());
    assert_eq!(incident_rows(&db).len(), 1);
    let (value, source) = option_row(&db, "editor_parser_helpers");
    assert_eq!(value, "false");
    assert!(
        source.starts_with("auto:continuation_navigation timeout incident="),
        "the automatic source names the operation and the incident: {source}"
    );

    // Redraw callbacks while disabled: quiet neutral fallbacks, no worker
    // contact, no further evidence.
    for _ in 0..5 {
        assert!(matches!(
            worker.probe(ReplParserOperation::SyntaxHighlight, "users(*)", None),
            ProbeOutcome::Disabled
        ));
    }
    assert_eq!(incident_rows(&db).len(), 1, "no incident while disabled");
    assert_eq!(worker.current_generation(), 2, "the kill's replacement stands unused");
}

/// Ordinary malformed/incomplete answers are parser results, not
/// operational incidents: the breaker stays closed.
#[test]
fn ordinary_malformed_answers_do_not_trip_the_breaker() {
    let (worker, policy) = controller_with_policy(None);
    assert_eq!(well_formed(&worker, "users(*) |>"), Some(false));
    assert!(policy.helpers_enabled());
    match worker.probe(ReplParserOperation::SubmissionPreflight, "users(*) |>", None) {
        ProbeOutcome::Answer(WorkerResult::Preflight { defects, .. }) => {
            assert!(defects, "the malformed submission reports defects")
        }
        _ => panic!("preflight answers ordinarily"),
    }
    assert!(policy.helpers_enabled(), "an ordinary answer never trips");
}

/// With helpers manually off, submission preflight still crosses the worker
/// and FAILS CLOSED: a hanging preflight refuses the submission and does
/// not change the helper option.
#[test]
fn preflight_still_crosses_and_fails_closed_with_helpers_off() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{process_query, ReplState};

    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::new(ClientDatabase::open_on(Mode::Other).unwrap()))).expect("repl state");
    let worker = Arc::new(ParserWorkerController::new_with_executable(
        dql_exe(),
        ReplParserBudgets::uniform(Duration::from_millis(100)),
        Arc::clone(state.config().editor_helper_policy()),
        state.repl_db.clone(),
        None,
    ));
    worker.hang_workers_for_tests();
    state.parser_worker = Arc::clone(&worker);
    state.set_editor_parser_helpers(false, ".repl helpers");

    let flag = std::sync::atomic::AtomicBool::new(false);
    let result = process_query("users(*)", &mut state, &flag);
    assert!(result.is_ok(), "a refused submission is not a REPL error");

    let db = state.repl_db.as_ref().expect("live database");
    let history = db.history_rows().expect("ledger");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].outcome, "refused");
    // The worker WAS contacted: the hanging preflight minted a kill
    // incident, and the refusal that followed is its own row.
    let rows = incident_rows(db);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0].operation, "submission_preflight");
    assert_eq!(rows[1].road, "preflight");
    // ... and the mandatory incident left the manual helper row alone.
    assert!(!state.config().editor_helpers_enabled());
    let (value, source) = option_row(db, "editor_parser_helpers");
    assert_eq!((value.as_str(), source.as_str()), ("false", ".repl helpers"));
}

/// Generation discipline at the worker itself: a mismatched request
/// generation is refused (the worker exits rather than serving it), and a
/// matched one echoes its generation.
#[test]
fn the_worker_refuses_a_stale_generation() {
    let spawn = |generation: u64| {
        Command::new(dql_exe())
            .arg("__repl-parser-worker")
            .arg("--generation")
            .arg(generation.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn worker")
    };
    let request = |request_id: u64, worker_generation: u64| WorkerRequest {
        request_id,
        worker_generation,
        operation: "prompt_well_formed".to_string(),
        entrance: "prompt".to_string(),
        input: "users(*)".to_string(),
        cursor_byte: None,
        cooperative_budget_ms: 1_000,
    };

    // Matched: answers, echoing id and generation.
    let mut child = spawn(7);
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    write_frame(&mut stdin, &serde_json::to_vec(&request(41, 7)).unwrap()).unwrap();
    let payload = read_frame(&mut stdout).unwrap().expect("an answer");
    let response: WorkerResponse = serde_json::from_slice(&payload).unwrap();
    assert_eq!(response.request_id, 41);
    assert_eq!(response.worker_generation, 7);
    assert!(matches!(
        response.result,
        WorkerResult::WellFormed { well_formed: true }
    ));
    drop(stdin);
    let status = child.wait().unwrap();
    assert!(status.success(), "EOF is a clean exit");

    // Stale: the worker refuses and dies rather than serving it.
    let mut child = spawn(7);
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    write_frame(&mut stdin, &serde_json::to_vec(&request(42, 6)).unwrap()).unwrap();
    assert!(
        read_frame(&mut stdout).unwrap().is_none(),
        "no answer crosses a stale generation"
    );
    let status = child.wait().unwrap();
    assert!(!status.success(), "a stale generation is fatal to the worker");

    // A garbage frame is equally fatal — protocol violations never get a
    // guessed answer.
    let mut child = spawn(7);
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    write_frame(&mut stdin, b"not json").unwrap();
    let _ = stdin.flush();
    assert!(read_frame(&mut stdout).unwrap().is_none());
    assert!(!child.wait().unwrap().success());
}

/// The hidden entrance is absent from ordinary help and from the queryable
/// `cli::surface` command inventory.
#[test]
fn the_hidden_entrance_is_absent_from_help_and_surface() {
    let help = Command::new(dql_exe()).arg("--help").output().expect("dql --help");
    let text = String::from_utf8_lossy(&help.stdout);
    assert!(
        !text.contains("__repl-parser-worker"),
        "the worker entrance must not appear in help"
    );

    // And not in the queryable surface either.
    let out = Command::new(dql_exe())
        .arg("query")
        .arg("--sequential")
        .arg("--file")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            child
                .stdin
                .as_mut()
                .unwrap()
                .write_all(
                    b"#!dql query-sequence\n\
                      mount!(\"delightql-bytes://surface\", \"cli::surface\")(*)\n\
                      cli::surface.command(*)\n",
                )
                .unwrap();
            child.wait_with_output()
        })
        .expect("query the surface");
    let rows = String::from_utf8_lossy(&out.stdout);
    assert!(
        rows.contains("query"),
        "the surface must answer commands: {rows} / {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !rows.contains("__repl-parser-worker"),
        "the hidden entrance must not be queryable surface"
    );
}

/// The preflight gate FAILS CLOSED: when the named worker executable cannot
/// serve, a DQL submission closes as refused — with a distinct
/// containment-unavailable reason, never a timeout record — and the bytes
/// never reach the in-process compiler.
#[test]
fn an_unavailable_worker_refuses_the_submission_without_execution() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{process_query, ReplState};

    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::new(ClientDatabase::open_on(Mode::Other).unwrap()))).expect("repl state");
    state.parser_worker = Arc::new(ParserWorkerController::new_with_executable(
        PathBuf::from("/nonexistent/dql-worker-binary"),
        ReplParserBudgets::measured_defaults(),
        Arc::clone(state.config().editor_helper_policy()),
        state.repl_db.clone(),
        None,
    ));

    let flag = std::sync::atomic::AtomicBool::new(false);
    // `users(*)` against an empty session would close `failed` (no such
    // table) if it EXECUTED; `refused` is the proof it never crossed.
    let result = process_query("users(*)", &mut state, &flag);
    assert!(result.is_ok(), "a refused submission is not a REPL error");

    let db = state.repl_db.as_ref().expect("live database");
    let history = db.history_rows().expect("ledger");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].outcome, "refused");
    assert!(
        history[0]
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("unavailable"),
        "the refusal names containment unavailability: {:?}",
        history[0].error
    );

    // Unavailability is NOT a parser timeout: no budget specimen was
    // minted. The refusal itself is recorded — the compiler never saw the
    // submission, so this row is the only trace of it being turned away.
    let rows = incident_rows(db);
    assert_eq!(rows.len(), 1, "one refusal row: {rows:?}");
    assert_eq!((rows[0].kind.as_str(), rows[0].road.as_str()), ("error", "preflight"));
    assert_eq!(rows[0].input, "users(*)");
    // And a mandatory-operation failure never touches the optional breaker.
    assert!(state.config().editor_helpers_enabled());
}

/// Record-before-respawn: evidence capture does not depend on replacement
/// startup. The worker binary is a COPY that vanishes after the first
/// spawn, so the replacement spawn fails — the killed worker's incident is
/// still recorded through the raw writer.
#[test]
fn a_kill_incident_is_recorded_even_when_the_replacement_cannot_spawn() {
    let dir = tempfile::tempdir().unwrap();
    let vanishing = dir.path().join("dql-vanishing");
    std::fs::copy(dql_exe(), &vanishing).unwrap();

    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("live database"));
    let worker = ParserWorkerController::new_with_executable(
        vanishing.clone(),
        ReplParserBudgets::measured_defaults(),
        ReplEditorHelperPolicy::new_enabled(),
        Some(Arc::clone(&db)),
        None,
    );
    worker.hang_workers_for_tests();

    // First probe spawns the worker from the copy; it hangs; delete the
    // binary UNDER the running worker so the replacement spawn must fail.
    // The unlink races the spawn only if it ran before the probe — it
    // cannot: spawn happens inside probe().
    let unlink = std::thread::spawn({
        let vanishing = vanishing.clone();
        move || {
            // After the initial spawn (~1 ms into probe), before the
            // replacement spawn (~75 ms in, once budget + grace expire).
            std::thread::sleep(Duration::from_millis(20));
            let _ = std::fs::remove_file(&vanishing);
        }
    });
    let outcome = worker.probe(ReplParserOperation::PromptWellFormed, "users(*)", None);
    unlink.join().unwrap();

    assert!(matches!(outcome, ProbeOutcome::TimedOut));
    let rows = incident_rows(&db);
    assert_eq!(rows.len(), 1, "the incident was recorded");
    assert_eq!(rows[0].containment, "worker_kill");
    assert_eq!(rows[0].worker_generation, 1);
    // Whether the replacement spawn succeeded is the OS's business here;
    // what the pin owns is that the record above exists regardless.
}

/// The ordinary prompt boundary flushes queued writes: a write queued
/// behind a busy connection lands via prompt_recovery_boundary on a
/// HEALTHY session — no Core recovery involved.
#[test]
fn the_ordinary_prompt_boundary_flushes_queued_writes() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{prompt_recovery_boundary, CommandResult, ReplState};
    use delightql_cli::client::database::InputKind;

    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::new(ClientDatabase::open_on(Mode::Other).unwrap()))).expect("repl state");
    let db = state.repl_db.clone().expect("live database");

    // Queue a ledger write behind a held connection, then release.
    {
        let conn = db.connection_arc();
        let held = conn.lock().unwrap();
        let (_, outcome) = db.record_input(InputKind::Dql, "queued-behind-busy");
        assert!(matches!(
            outcome,
            delightql_cli::client::database::WriteOutcome::Queued
        ));
        drop(held);
    }
    assert!(
        db.history_rows().unwrap().is_empty(),
        "still pending before the prompt boundary"
    );
    assert!(matches!(
        prompt_recovery_boundary(&mut state),
        CommandResult::Continue
    ));
    let rows = db.history_rows().unwrap();
    assert_eq!(rows.len(), 1, "the prompt boundary flushed the queue");
    assert_eq!(rows[0].input, "queued-behind-busy");
}

/// Entrance evidence is truthful for query-sequence submissions: a
/// cooperative cancel AND a hard kill over MARKED bytes both record
/// `entrance = query_sequence`, never a false `prompt`.
#[test]
fn marked_submission_timeouts_record_the_query_sequence_entrance() {
    // Cooperative: a zero-budget controller cancels a large marked
    // submission at the first checkpoint, on the utility road.
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("live database"));
    let worker = ParserWorkerController::new_with_executable(
        dql_exe(),
        ReplParserBudgets::uniform(Duration::ZERO),
        ReplEditorHelperPolicy::new_enabled(),
        Some(Arc::clone(&db)),
        None,
    );
    let marked = format!("#!dql query-sequence\n{}", "users(*)\n".repeat(30_000));
    assert!(matches!(
        worker.probe(ReplParserOperation::SubmissionPreflight, &marked, None),
        ProbeOutcome::TimedOut
    ));
    // Which containment road fired is scheduler-dependent under a zero
    // budget (a slow spawn turns the cooperative reply into a kill); the
    // finding's subject — the ENTRANCE — must be truthful on either road.
    // The deterministic cooperative-entrance pin lives at the worker's
    // serve() level (`a_cancelled_marked_submission_reports_its_entrance`).
    let rows = incident_rows(&db);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].entrance, "query_sequence");
    assert!(
        rows[0].containment == "cooperative_cancel" || rows[0].containment == "worker_kill",
        "a ruled containment spelling, got {}",
        rows[0].containment
    );

    // Hard kill: a hanging worker answers nothing, so the evidence records
    // the entrance the REQUEST selected — the same framing law.
    let db = Arc::new(ClientDatabase::open_on(Mode::Other).expect("live database"));
    let worker = ParserWorkerController::new_with_executable(
        dql_exe(),
        ReplParserBudgets::measured_defaults(),
        ReplEditorHelperPolicy::new_enabled(),
        Some(Arc::clone(&db)),
        None,
    );
    worker.hang_workers_for_tests();
    assert!(matches!(
        worker.probe(
            ReplParserOperation::SubmissionPreflight,
            "#!dql query-sequence\nusers(*)",
            None
        ),
        ProbeOutcome::TimedOut
    ));
    let rows = incident_rows(&db);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].entrance, "query_sequence");
    assert_eq!(rows[0].containment, "worker_kill");
}

/// Inputs whose tree-sitter recovery leaves the tree without a
/// `source_file` root, or otherwise shaped to trip a typed-CST accessor.
/// `((` is the minimized member: two unmatched parens at the prompt killed
/// the worker on every submission until the shaped accessor was used.
const UNSHAPED: &[&str] = &[
    "((",
    "((1,",
    "users(*), ((1,",
    "(((((((((((((((",
    ")",
    ")(",
    "|>",
    "~>",
    ":",
    "`",
    "\"",
    "_(",
    "?-",
    "",
    " \n\n ",
    "∂> ((",
    "_(x@-2.0) : xaxis\nxaxis(*), x < 1.2\n|> (x + 0.05 as x) : xaxis\n((1,",
    TOXIC,
];

/// One worker process serves EVERY operation over EVERY unshaped input and
/// is still alive at the end: a defective tree is an answer (defects,
/// no root branch, cancelled), never a dead worker.
#[test]
fn the_worker_answers_every_operation_over_unshaped_input_without_dying() {
    let mut child = Command::new(dql_exe())
        .arg("__repl-parser-worker")
        .arg("--generation")
        .arg("1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn worker");
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();

    let mut request_id = 0u64;
    for input in UNSHAPED {
        for op in ReplParserOperation::ALL {
            let entrances: &[&str] = if op == ReplParserOperation::SubmissionPreflight {
                &["prompt", "query_sequence"]
            } else {
                &["prompt"]
            };
            for entrance in entrances {
                request_id += 1;
                let request = WorkerRequest {
                    request_id,
                    worker_generation: 1,
                    operation: op.as_str().to_string(),
                    entrance: entrance.to_string(),
                    input: input.to_string(),
                    cursor_byte: None,
                    cooperative_budget_ms: 200,
                };
                write_frame(&mut stdin, &serde_json::to_vec(&request).unwrap()).unwrap();
                let payload = read_frame(&mut stdout)
                    .unwrap()
                    .unwrap_or_else(|| {
                        panic!(
                            "worker died on {} / {entrance} over {input:?}",
                            op.as_str()
                        )
                    });
                let response: WorkerResponse = serde_json::from_slice(&payload).unwrap();
                assert_eq!(response.request_id, request_id);
                // Whatever the verdict, it is one of the closed answers —
                // and never a panic: an unshaped tree is a verdict.
                match response.result {
                    WorkerResult::WellFormed { .. }
                    | WorkerResult::Highlights { .. }
                    | WorkerResult::Continuations { .. }
                    | WorkerResult::Preflight { .. }
                    | WorkerResult::Cancelled { .. } => {}
                    WorkerResult::Panicked { message, location } => panic!(
                        "worker panicked on {} / {entrance} over {input:?}: {message} at {location:?}",
                        op.as_str()
                    ),
                }
            }
        }
    }
    drop(stdin);
    let status = child.wait().unwrap();
    assert!(status.success(), "EOF after {request_id} answers is a clean exit");
}

/// A panic inside the worker is an ANSWER: the worker survives, the parent
/// records it with the operation's evidence, says it once, and the
/// mandatory preflight refuses the submission with the record named.
#[test]
fn a_worker_panic_is_forwarded_recorded_and_the_worker_survives() {
    use delightql_cli::output_format::OutputFormat;
    use delightql_cli::repl::commands::{process_query, ReplState};

    let db = Arc::new(ClientDatabase::open_on(Mode::Other).unwrap());
    let mut state = ReplState::new_over(None, OutputFormat::Table, None, Some(Arc::clone(&db)))
        .expect("repl state");
    let worker = Arc::new(ParserWorkerController::new_with_executable(
        dql_exe(),
        ReplParserBudgets::measured_defaults(),
        Arc::clone(state.config().editor_helper_policy()),
        Some(Arc::clone(&db)),
        None,
    ));
    worker.panic_workers_for_tests();
    state.parser_worker = Arc::clone(&worker);

    // An optional probe: the panic is recorded, the caller gets its
    // neutral fallback, and the breaker is untouched (the worker is fine).
    let outcome = worker.probe(ReplParserOperation::PromptWellFormed, "users(*)", None);
    assert!(matches!(outcome, ProbeOutcome::Panicked { .. }), "{outcome:?}");
    assert_eq!(worker.current_generation(), 1, "the worker was not replaced");
    assert!(state.config().editor_helpers_enabled());

    // The same worker answers the next request: it survived its panic.
    let again = worker.probe(ReplParserOperation::PromptWellFormed, "users(*)", None);
    assert!(matches!(again, ProbeOutcome::Panicked { .. }));
    assert_eq!(worker.current_generation(), 1);

    let rows = incident_rows(&db);
    assert_eq!(rows.len(), 1, "identical specimens are one row: {rows:?}");
    assert_eq!(rows[0].kind, "panic");
    assert_eq!(rows[0].road, "parser_worker");
    assert_eq!(rows[0].operation, "prompt_well_formed");
    assert_eq!(rows[0].containment, "worker_panic");
    assert_eq!(rows[0].input, "users(*)");
    assert_eq!(rows[0].occurrence_count, 2);
    assert_eq!(rows[0].grammar_fingerprint, delightql_cst::GRAMMAR_FINGERPRINT);

    // The mandatory preflight refuses rather than cross the in-process
    // parser with the bytes the worker panicked on.
    let flag = std::sync::atomic::AtomicBool::new(false);
    let result = process_query("users(*)", &mut state, &flag);
    assert!(result.is_ok(), "a refused submission is not a REPL error");
    let history = db.history_rows().expect("ledger");
    assert_eq!(history[0].outcome, "refused");
    let reason = history[0].error.clone().unwrap_or_default();
    assert!(reason.contains("panicked"), "{reason}");
    assert!(reason.contains("repl::errors.incident #"), "{reason}");
    let rows = incident_rows(&db);
    assert!(
        rows.iter().any(|r| r.road == "preflight" && r.kind == "error"),
        "the refusal is its own row: {rows:?}"
    );
    assert!(
        rows.iter()
            .any(|r| r.road == "parser_worker" && r.operation == "submission_preflight"),
        "the preflight panic is recorded with its operation: {rows:?}"
    );
}
