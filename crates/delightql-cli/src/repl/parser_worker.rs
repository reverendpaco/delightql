// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The parent side of the parser containment boundary.
//!
//! [`ParserWorkerController`] owns one persistent child worker, the
//! parent-minted generation counter, and the ruled containment order: when a
//! worker exceeds its budget the parent kills and reaps it FIRST, records the
//! timeout through the raw REPL-database writer (never through DQL, never
//! under the `DqlHandle` mutex), spawns the replacement, prints one
//! rate-limited warning, and returns control to the editor. A worker that
//! answers a cancellation cooperatively stays alive — cooperative
//! cancellation and worker kill are recorded as different containment.
//! Protocol violations (bad frame, mismatched id or generation) replace the
//! worker too, but are reported as protocol incidents, never misclassified
//! as parser timeouts.

use std::collections::HashMap;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use super::config::{
    ReplEditorHelperPolicy, ReplParserBudgets, ReplParserOperation, ReplParserOperationKind,
};
use crate::client::database::{ClientDatabase, IncidentRecordOutcome};
use crate::client::incident::{self, hierarchy, Incident, IncidentKind, WorkerEvidence};
use super::worker::{read_frame, write_frame, WorkerRequest, WorkerResponse, WorkerResult};

/// Grace beyond the cooperative budget for the reply to cross the wire.
/// Protocol latency, not a parser budget — the exhaustive budget mapping
/// stays in [`ReplParserBudgets`].
const REPLY_GRACE: Duration = Duration::from_millis(50);

/// Minimum quiet interval between repeated terminal warnings for the same
/// specimen key.
const WARN_INTERVAL: Duration = Duration::from_secs(30);

/// Floor between warnings per OPERATION, whatever the key: typing at the
/// end of a slow buffer mints a fresh specimen per keystroke, and a warning
/// per keystroke would bury the terminal.
const WARN_OPERATION_INTERVAL: Duration = Duration::from_secs(5);

/// How long the parent waits for a killed worker to be reaped.
const REAP_BUDGET: Duration = Duration::from_secs(2);

/// What a probe answered, after containment did its work.
#[derive(Debug)]
pub enum ProbeOutcome {
    Answer(WorkerResult),
    /// The operation exceeded its budget. The incident is recorded and the
    /// worker (if killed) already replaced; the caller falls back to its
    /// previous verdict / no-op.
    TimedOut,
    /// The worker could not serve (spawn failure or protocol incident).
    /// Never recorded as a timeout.
    Unavailable,
    /// The optional-helper breaker is open: the request was never sent and
    /// no worker was contacted or spawned. Callers take their neutral
    /// fallback quietly. Never answered for submission preflight.
    Disabled,
    /// The operation panicked inside the worker, which survived and
    /// answered. Recorded (or honestly lost) before this returns; the
    /// per-keystroke callers take their neutral fallback, preflight refuses.
    Panicked {
        message: String,
        recorded: IncidentRecordOutcome,
    },
}

/// What tripped the breaker — a recorded (or honestly lost) budget
/// incident, or a worker/protocol failure that created no timeout row.
enum HelperTrip {
    BudgetIncident { outcome: IncidentRecordOutcome },
    WorkerFailure { what: &'static str },
}

struct WorkerHandle {
    child: Child,
    stdin: ChildStdin,
    responses: mpsc::Receiver<std::io::Result<WorkerResponse>>,
    generation: u64,
}

impl WorkerHandle {
    /// Kill and REAP: the child never outlives this call, and no runaway
    /// process is ever detached.
    fn kill_and_reap(mut self) {
        let _ = self.child.kill();
        let deadline = Instant::now() + REAP_BUDGET;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                // Past the deadline a blocking wait is still owed: kill()
                // was delivered, so this terminates.
                _ => {
                    let _ = self.child.wait();
                    return;
                }
            }
        }
    }
}

pub struct ParserWorkerController {
    inner: Mutex<Option<WorkerHandle>>,
    budgets: ReplParserBudgets,
    /// The SHARED optional-helper breaker — the same instance `ReplConfig`
    /// mutates, never a copy of its value. Optional probes read it before
    /// touching the worker; incidents trip it through its atomic
    /// compare-and-disable.
    policy: Arc<ReplEditorHelperPolicy>,
    repl_db: Option<Arc<ClientDatabase>>,
    highlights: Option<std::path::PathBuf>,
    /// `None` = this process's own executable, resolved at spawn — parent
    /// and worker are the same binary. Tests name the built `dql` binary
    /// explicitly, because a test harness's `current_exe` is the harness.
    executable: Option<std::path::PathBuf>,
    /// Test seam: spawn workers with the hang hook armed, to prove the
    /// hard kill road deterministically.
    hang_workers: std::sync::atomic::AtomicBool,
    /// Test seam: spawn workers that panic on every request, to prove the
    /// forwarded-panic road deterministically.
    panic_workers: std::sync::atomic::AtomicBool,
    /// Monotonic spawn counter; increments on EVERY spawn, the initial
    /// worker included, and rides in every request and incident.
    generation: AtomicU64,
    next_request_id: AtomicU64,
    warn_gate: Mutex<HashMap<String, Instant>>,
    /// Incident ids already announced on the terminal: a deterministic
    /// panic on every keystroke of one line is one row and one line.
    announced_panics: Mutex<std::collections::HashSet<i64>>,
}

impl ParserWorkerController {
    pub fn new(
        budgets: ReplParserBudgets,
        policy: Arc<ReplEditorHelperPolicy>,
        repl_db: Option<Arc<ClientDatabase>>,
        highlights: Option<std::path::PathBuf>,
    ) -> ParserWorkerController {
        ParserWorkerController {
            inner: Mutex::new(None),
            budgets,
            policy,
            repl_db,
            highlights,
            executable: None,
            hang_workers: std::sync::atomic::AtomicBool::new(false),
            panic_workers: std::sync::atomic::AtomicBool::new(false),
            generation: AtomicU64::new(0),
            next_request_id: AtomicU64::new(1),
            warn_gate: Mutex::new(HashMap::new()),
            announced_panics: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// The test seam: the same controller over a NAMED `dql` binary.
    pub fn new_with_executable(
        executable: std::path::PathBuf,
        budgets: ReplParserBudgets,
        policy: Arc<ReplEditorHelperPolicy>,
        repl_db: Option<Arc<ClientDatabase>>,
        highlights: Option<std::path::PathBuf>,
    ) -> ParserWorkerController {
        let mut controller = ParserWorkerController::new(budgets, policy, repl_db, highlights);
        controller.executable = Some(executable);
        controller
    }

    pub fn budgets(&self) -> &ReplParserBudgets {
        &self.budgets
    }

    /// A read of the shared policy — never a cached copy. The prompt's
    /// neutral fallback asks this without minting a probe.
    pub fn helpers_enabled(&self) -> bool {
        self.policy.helpers_enabled()
    }

    /// The current worker generation (0 before the first spawn).
    pub fn current_generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Test seam: every worker spawned AFTER this call reads its request
    /// and never answers — the deterministic non-responsive worker.
    pub fn panic_workers_for_tests(&self) {
        self.panic_workers.store(true, Ordering::SeqCst);
    }

    pub fn hang_workers_for_tests(&self) {
        self.hang_workers.store(true, Ordering::SeqCst);
    }

    /// The live worker's OS pid, when one is running.
    pub fn current_worker_pid(&self) -> Option<u32> {
        self.inner
            .lock()
            .ok()?
            .as_ref()
            .map(|worker| worker.child.id())
    }

    fn spawn_worker(&self) -> Option<WorkerHandle> {
        let exe = match &self.executable {
            Some(exe) => exe.clone(),
            None => std::env::current_exe().ok()?,
        };
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let mut command = Command::new(exe);
        command
            .arg("__repl-parser-worker")
            .arg("--generation")
            .arg(generation.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if let Some(path) = &self.highlights {
            command.arg("--highlights").arg(path);
        }
        if self.hang_workers.load(Ordering::SeqCst) {
            command.env("DQL_TEST_WORKER_HANG", "1");
        }
        if self.panic_workers.load(Ordering::SeqCst) {
            command.env("DQL_TEST_WORKER_PANIC", "1");
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(e) => {
                incident::warning(
                    "parser_worker",
                    hierarchy::WORKER_UNAVAILABLE,
                    format!("could not spawn the REPL parser worker: {e}"),
                );
                return None;
            }
        };
        let stdin = child.stdin.take()?;
        let mut stdout = child.stdout.take()?;
        let (tx, rx) = mpsc::channel();
        // The reader thread blocks on the pipe and exits at EOF — which the
        // kill above guarantees — so it can never outlive its worker as a
        // runaway.
        std::thread::spawn(move || loop {
            let message = match read_frame(&mut stdout) {
                Ok(Some(payload)) => serde_json::from_slice::<WorkerResponse>(&payload)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                Ok(None) => break,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    break;
                }
            };
            let failed = message.is_err();
            if tx.send(message).is_err() || failed {
                break;
            }
        });
        Some(WorkerHandle {
            child,
            stdin,
            responses: rx,
            generation,
        })
    }

    /// One probe: send, wait the operation's budget (plus wire grace), and
    /// contain whatever happens.
    pub fn probe(
        &self,
        operation: ReplParserOperation,
        input: &str,
        cursor_byte: Option<u64>,
    ) -> ProbeOutcome {
        // The breaker's short circuit: a disabled OPTIONAL operation answers
        // before any worker is contacted or spawned. Submission preflight is
        // a mandatory safety operation and bypasses the check unconditionally
        // — no option can route submitted bytes around containment.
        match operation.kind() {
            ReplParserOperationKind::OptionalEditorHelper => {
                if !self.policy.helpers_enabled() {
                    return ProbeOutcome::Disabled;
                }
            }
            ReplParserOperationKind::MandatorySafety => {}
        }
        let mut slot = match self.inner.lock() {
            Ok(slot) => slot,
            Err(poisoned) => poisoned.into_inner(),
        };
        if slot.is_none() {
            *slot = self.spawn_worker();
        }
        let Some(worker) = slot.as_mut() else {
            drop(slot);
            self.trip_optional_helpers(
                operation,
                HelperTrip::WorkerFailure {
                    what: "the parser worker could not be spawned",
                },
            );
            return ProbeOutcome::Unavailable;
        };

        let budget = self.budgets.effective(operation);
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        // The entrance is the parent's selection, decided by the same
        // framing law the parse will apply: preflight takes the road the
        // submission's own bytes name; every per-keystroke probe is the
        // prompt road. Hard-kill evidence records THIS copy, because a
        // killed worker answered nothing.
        let entrance = selected_entrance(operation, input);
        let request = WorkerRequest {
            request_id,
            worker_generation: worker.generation,
            operation: operation.as_str().to_string(),
            entrance: entrance.to_string(),
            input: input.to_string(),
            cursor_byte,
            cooperative_budget_ms: budget.as_millis() as u64,
        };
        let payload = match serde_json::to_vec(&request) {
            Ok(payload) => payload,
            Err(_) => return ProbeOutcome::Unavailable,
        };
        let started = Instant::now();
        if write_frame(&mut worker.stdin, &payload).is_err() {
            self.replace_after_protocol_incident(&mut slot, "the worker pipe closed");
            drop(slot);
            self.trip_optional_helpers(
                operation,
                HelperTrip::WorkerFailure {
                    what: "the worker pipe closed",
                },
            );
            return ProbeOutcome::Unavailable;
        }

        match worker.responses.recv_timeout(budget + REPLY_GRACE) {
            Ok(Ok(response)) => {
                if response.request_id != request_id
                    || response.worker_generation != worker.generation
                {
                    self.replace_after_protocol_incident(
                        &mut slot,
                        "stale or mismatched response id/generation",
                    );
                    drop(slot);
                    self.trip_optional_helpers(
                        operation,
                        HelperTrip::WorkerFailure {
                            what: "stale or mismatched response id/generation",
                        },
                    );
                    return ProbeOutcome::Unavailable;
                }
                if let WorkerResult::Cancelled {
                    elapsed_ms,
                    last_progress_byte,
                    entrance,
                } = &response.result
                {
                    if entrance == "unknown_operation" {
                        self.replace_after_protocol_incident(&mut slot, "unknown operation");
                        drop(slot);
                        self.trip_optional_helpers(
                            operation,
                            HelperTrip::WorkerFailure {
                                what: "unknown operation",
                            },
                        );
                        return ProbeOutcome::Unavailable;
                    }
                    // Cooperative cancellation: the worker is healthy and
                    // stays; the evidence records the cooperative road.
                    let generation = worker.generation;
                    let entrance: &'static str = match entrance.as_str() {
                        "query_sequence" => "query_sequence",
                        "companion_cell" => "companion_cell",
                        _ => "prompt",
                    };
                    drop(slot);
                    let specimen = budget_incident(
                        input,
                        cursor_byte,
                        WorkerEvidence {
                            operation: operation.as_str(),
                            entrance,
                            budget_ms: budget.as_millis() as u64,
                            elapsed_ms: *elapsed_ms,
                            last_progress_byte: *last_progress_byte,
                            containment: "cooperative_cancel",
                            worker_generation: generation,
                        },
                    );
                    let key = specimen.specimen_key();
                    let input_bytes = input.len();
                    let recorded = self.record(specimen);
                    self.warn(
                        recorded.clone(),
                        &key,
                        operation.as_str(),
                        input_bytes,
                        "cooperative_cancel",
                    );
                    self.trip_optional_helpers(
                        operation,
                        HelperTrip::BudgetIncident { outcome: recorded },
                    );
                    return ProbeOutcome::TimedOut;
                }
                if let WorkerResult::Panicked { message, location } = &response.result {
                    // The worker is healthy and stays: a panic is a fact
                    // about these bytes on this operation, recorded with
                    // the same evidence a budget incident carries.
                    let generation = worker.generation;
                    drop(slot);
                    let mut incident = Incident::plain(
                        IncidentKind::Panic,
                        "parser_worker",
                        "internal/panic",
                        message.clone(),
                    );
                    incident.location = location.clone();
                    incident.input = Some(input.to_string());
                    incident.cursor_byte = cursor_byte;
                    incident.worker = Some(WorkerEvidence {
                        operation: operation.as_str(),
                        entrance,
                        budget_ms: budget.as_millis() as u64,
                        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
                        last_progress_byte: None,
                        containment: "worker_panic",
                        worker_generation: generation,
                    });
                    let recorded = self.record(incident);
                    self.announce_panic(&recorded, message, location.as_deref());
                    return ProbeOutcome::Panicked {
                        message: message.clone(),
                        recorded,
                    };
                }
                if !variant_matches(operation, &response.result) {
                    self.replace_after_protocol_incident(
                        &mut slot,
                        "response variant does not match the requested operation",
                    );
                    drop(slot);
                    self.trip_optional_helpers(
                        operation,
                        HelperTrip::WorkerFailure {
                            what: "response variant does not match the requested operation",
                        },
                    );
                    return ProbeOutcome::Unavailable;
                }
                ProbeOutcome::Answer(response.result)
            }
            Ok(Err(_)) => {
                self.replace_after_protocol_incident(&mut slot, "unreadable frame");
                drop(slot);
                self.trip_optional_helpers(
                    operation,
                    HelperTrip::WorkerFailure {
                        what: "unreadable frame",
                    },
                );
                ProbeOutcome::Unavailable
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // The worker died before answering: a protocol/crash
                // incident, never misclassified as a parser timeout.
                self.replace_after_protocol_incident(
                    &mut slot,
                    "the worker exited before answering",
                );
                drop(slot);
                self.trip_optional_helpers(
                    operation,
                    HelperTrip::WorkerFailure {
                        what: "the worker exited before answering",
                    },
                );
                ProbeOutcome::Unavailable
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // The ruled order, exactly: kill and reap FIRST, then record
                // through the raw writer, then the replacement, then the
                // warning, then control back to the editor. Recording sits
                // BEFORE the respawn so evidence capture never depends on
                // replacement startup.
                let generation = worker.generation;
                if let Some(worker) = slot.take() {
                    worker.kill_and_reap();
                }
                let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                let specimen = budget_incident(
                    input,
                    cursor_byte,
                    WorkerEvidence {
                        operation: operation.as_str(),
                        entrance,
                        budget_ms: budget.as_millis() as u64,
                        elapsed_ms,
                        last_progress_byte: None,
                        containment: "worker_kill",
                        worker_generation: generation,
                    },
                );
                let key = specimen.specimen_key();
                let input_bytes = input.len();
                let recorded = self.record(specimen);
                *slot = self.spawn_worker();
                drop(slot);
                self.warn(
                    recorded.clone(),
                    &key,
                    operation.as_str(),
                    input_bytes,
                    "worker_kill",
                );
                self.trip_optional_helpers(
                    operation,
                    HelperTrip::BudgetIncident { outcome: recorded },
                );
                ProbeOutcome::TimedOut
            }
        }
    }

    fn replace_after_protocol_incident(&self, slot: &mut Option<WorkerHandle>, what: &str) {
        incident::warning(
            "parser_worker",
            hierarchy::WORKER_UNAVAILABLE,
            format!("REPL parser worker protocol violation ({what}); worker replaced"),
        );
        if let Some(worker) = slot.take() {
            worker.kill_and_reap();
        }
        *slot = self.spawn_worker();
    }

    /// The automatic trip: on the FIRST incident from an OPTIONAL operation,
    /// atomically disable all three optional helpers, project the
    /// authoritative value through the retained raw writer, and print one
    /// actionable message. A mandatory-preflight incident never reaches the
    /// policy — the guard is structural here, not a caller convention. After
    /// the first transition the breaker trips quietly: disabled callbacks
    /// short-circuit at the probe entrance and mint no further incidents.
    /// Containment already finished before this runs — the trip never
    /// prevents worker replacement.
    fn trip_optional_helpers(&self, operation: ReplParserOperation, cause: HelperTrip) {
        match operation.kind() {
            ReplParserOperationKind::OptionalEditorHelper => {}
            ReplParserOperationKind::MandatorySafety => return,
        }
        if !self.policy.trip() {
            return;
        }
        let op = operation.as_str();
        // The headline states the evidence honestly: a committed incident id,
        // a pending queue position, or an admitted loss — never an invented
        // committed id. A worker/protocol failure that created no timeout row
        // gets its own distinct source and message.
        let (headline, source) = match cause {
            HelperTrip::BudgetIncident { outcome } => match outcome {
                IncidentRecordOutcome::Recorded { incident_id } => (
                    format!("{op} timed out (incident={incident_id})."),
                    format!("auto:{op} timeout incident={incident_id}"),
                ),
                IncidentRecordOutcome::Queued { pending_id } => (
                    format!(
                        "{op} timed out (incident=pending-{pending_id}; the evidence \
                         is queued, not yet committed)."
                    ),
                    format!("auto:{op} timeout incident=pending-{pending_id}"),
                ),
                IncidentRecordOutcome::Lost(reason) => (
                    format!("{op} timed out (the incident evidence was lost: {reason})."),
                    format!("auto:{op} timeout evidence-lost"),
                ),
            },
            HelperTrip::WorkerFailure { what } => (
                format!("a parser worker failure during {op} ({what})."),
                format!("auto:{op} worker_failure"),
            ),
        };
        if let Some(db) = &self.repl_db {
            if let crate::client::database::WriteOutcome::Lost(reason) = db.set_option(
                "editor_parser_helpers",
                Some("false".to_string()),
                "boolean",
                Some("true".to_string()),
                &source,
            ) {
                incident::warning(
                    "ledger",
                    hierarchy::LEDGER_WRITE_LOST,
                    format!(
                        "repl::config.option 'editor_parser_helpers' projection \
                         failed ({reason}); the typed value stands"
                    ),
                );
            }
        }
        incident::warning(
            "parser_worker",
            hierarchy::ASSISTANCE_DISABLED,
            format!("optional REPL parser assistance was disabled after\n         {headline}"),
        );
        eprintln!();
        eprintln!("         Syntax coloring, parse-aware prompts, and continuation navigation");
        eprintln!("         are now off. Submission safety preflight remains enabled.");
        eprintln!();
        eprintln!("         Run `.repl helpers on` to re-enable them.");
    }

    /// Record through the raw writer alone. Recording failure never defeats
    /// containment: the caller warns and recovery continues regardless.
    fn record(&self, specimen: Incident) -> IncidentRecordOutcome {
        match &self.repl_db {
            Some(db) => db.record_incident(specimen),
            None => IncidentRecordOutcome::Lost("no live REPL database".to_string()),
        }
    }

    /// One line per recorded panic row, on the terminal, with where the
    /// record is; a lost record says so instead.
    fn announce_panic(&self, recorded: &IncidentRecordOutcome, message: &str, location: Option<&str>) {
        let at = location.map(|l| format!(" (at {l})")).unwrap_or_default();
        match recorded {
            IncidentRecordOutcome::Recorded { incident_id } => {
                let first = self
                    .announced_panics
                    .lock()
                    .map(|mut seen| seen.insert(*incident_id))
                    .unwrap_or(true);
                if first {
                    eprintln!(
                        "[{}] {message}{at} — recorded as repl::errors.incident #{incident_id}",
                        incident::PANIC_URI
                    );
                }
            }
            IncidentRecordOutcome::Queued { pending_id } => eprintln!(
                "[{}] {message}{at} — recorded as repl::errors.incident (pending-{pending_id})",
                incident::PANIC_URI
            ),
            IncidentRecordOutcome::Lost(reason) => eprintln!(
                "[{}] {message}{at} — NOT recorded: {reason}",
                incident::PANIC_URI
            ),
        }
    }

    /// Print the rate-limited warning for a recorded (or lost) incident.
    fn warn(
        &self,
        outcome: IncidentRecordOutcome,
        key: &str,
        operation: &str,
        input_bytes: usize,
        containment: &str,
    ) {
        // The warning states what actually happened: a cooperative cancel
        // leaves the worker alive; only the hard road restarted one.
        let action = if containment == "worker_kill" {
            "worker restarted"
        } else {
            "parse cancelled cooperatively"
        };
        let now = Instant::now();
        let should_warn = {
            let mut gate = match self.warn_gate.lock() {
                Ok(gate) => gate,
                Err(poisoned) => poisoned.into_inner(),
            };
            let key_quiet = !matches!(
                gate.get(key),
                Some(last) if now.duration_since(*last) < WARN_INTERVAL
            );
            let op_key = format!("op:{operation}");
            let op_quiet = !matches!(
                gate.get(&op_key),
                Some(last) if now.duration_since(*last) < WARN_OPERATION_INTERVAL
            );
            if key_quiet && op_quiet {
                gate.insert(key.to_string(), now);
                gate.insert(op_key, now);
                true
            } else {
                false
            }
        };
        if !should_warn {
            return;
        }
        match outcome {
            IncidentRecordOutcome::Recorded { incident_id } => {
                eprintln!("warning: prompt parser exceeded its budget; {action}");
                eprintln!(
                    "         incident={incident_id} operation={operation} input_bytes={input_bytes}"
                );
            }
            IncidentRecordOutcome::Queued { pending_id } => {
                eprintln!("warning: prompt parser exceeded its budget; {action}");
                eprintln!(
                    "         incident=pending-{pending_id} operation={operation} input_bytes={input_bytes}"
                );
            }
            IncidentRecordOutcome::Lost(reason) => {
                eprintln!(
                    "warning: prompt parser exceeded its budget; {action} \
                     (incident NOT recorded: {reason}; operation={operation} input_bytes={input_bytes})"
                );
            }
        }
    }
}

/// A budget incident: the exact input, the evidence, one identity.
fn budget_incident(input: &str, cursor_byte: Option<u64>, worker: WorkerEvidence) -> Incident {
    let mut incident = Incident::plain(
        IncidentKind::Error,
        "parser_worker",
        hierarchy::WORKER_BUDGET,
        format!(
            "prompt parser exceeded its {} ms budget ({})",
            worker.budget_ms, worker.containment
        ),
    );
    incident.input = Some(input.to_string());
    incident.cursor_byte = cursor_byte;
    incident.worker = Some(worker);
    incident
}

impl Drop for ParserWorkerController {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.inner.lock() {
            if let Some(worker) = slot.take() {
                worker.kill_and_reap();
            }
        }
    }
}

/// The response variant an operation's request is owed. Anything else is a
/// protocol incident, never an ordinary parser result.
fn variant_matches(operation: ReplParserOperation, result: &WorkerResult) -> bool {
    match operation {
        ReplParserOperation::PromptWellFormed => matches!(result, WorkerResult::WellFormed { .. }),
        ReplParserOperation::SyntaxHighlight => matches!(result, WorkerResult::Highlights { .. }),
        ReplParserOperation::ContinuationNavigation => {
            matches!(result, WorkerResult::Continuations { .. })
        }
        ReplParserOperation::SubmissionPreflight => {
            matches!(result, WorkerResult::Preflight { .. })
        }
    }
}

/// The parser entrance the PARENT selects for a request, by the same
/// framing law the parse applies: preflight follows the submission's own
/// bytes (marked text — misplaced header included — is the utility
/// entrance); every per-keystroke probe is the prompt road.
fn selected_entrance(operation: ReplParserOperation, input: &str) -> &'static str {
    match operation {
        ReplParserOperation::SubmissionPreflight => match delightql_cst::submission_road(input) {
            delightql_cst::Root::QuerySequence => "query_sequence",
            delightql_cst::Root::DefinitionFile | delightql_cst::Root::CompanionCell => "prompt",
        },
        ReplParserOperation::PromptWellFormed
        | ReplParserOperation::SyntaxHighlight
        | ReplParserOperation::ContinuationNavigation => "prompt",
    }
}
