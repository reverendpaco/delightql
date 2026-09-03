// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The client's incident record: every error, warning and panic the
//! PROCESS produces, as one row each in `repl::errors.incident`. Core's
//! `sys::diagnostics.finding` records what the compiler refused; this
//! records what happened around it — the worker, the prompt, the exit.
//!
//! `kind` shares its domain with core's finding so the exit projection's
//! union means something; `road` says which mechanism produced the row.
//! Rows deduplicate by specimen key with an occurrence count: a
//! highlighter that panics on every keystroke of one line is one row.

use std::sync::Mutex;

use super::context::process_database;

/// The one severity domain, shared with `sys::diagnostics.finding`.
/// `Panic` sits above `Error`: a defect in dql, not in the submission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IncidentKind {
    Error,
    Warning,
    Info,
    Panic,
}

impl IncidentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            IncidentKind::Error => "error",
            IncidentKind::Warning => "warning",
            IncidentKind::Info => "info",
            IncidentKind::Panic => "panic",
        }
    }
}

/// Evidence only a parser-worker incident carries: the operation, the
/// entrance, the budget that applied, and how containment ended it.
#[derive(Clone, Debug)]
pub struct WorkerEvidence {
    /// Closed operation vocabulary (`prompt_well_formed`, `syntax_highlight`,
    /// `continuation_navigation`, `submission_preflight`).
    pub operation: &'static str,
    /// The parser's named entrance (`prompt`, `definition_file`,
    /// `query_sequence`, `companion_cell`).
    pub entrance: &'static str,
    /// The effective budget that applied, so later analysis needs no current
    /// configuration to reconstruct it.
    pub budget_ms: u64,
    pub elapsed_ms: f64,
    pub last_progress_byte: Option<u64>,
    /// `cooperative_cancel`, `worker_kill` or `worker_panic` — materially
    /// different evidence.
    pub containment: &'static str,
    /// Parent-minted spawn counter of the worker that served this request.
    pub worker_generation: u64,
}

/// One incident, before it is a row.
#[derive(Clone, Debug)]
pub struct Incident {
    pub kind: IncidentKind,
    /// Which mechanism produced it: `parser_worker`, `preflight`, `query`
    /// (the query thread), `main`, `startup`, `history`, `terminal`,
    /// `ledger`, `namespace`, `format`, `argument`, `dot_command`, `exit`.
    pub road: &'static str,
    /// The identity: a full `delightql-error://…` badge, never empty. A
    /// panic is `internal/panic` on every road.
    pub uri: String,
    pub message: String,
    /// `file:line` for panics.
    pub location: Option<String>,
    pub thread: Option<String>,
    /// The EXACT input, when there is one. Reproduction is the table's
    /// purpose; it is never redacted, and it reaches disk only on an
    /// explicit dump or the exit files.
    pub input: Option<String>,
    pub cursor_byte: Option<u64>,
    pub worker: Option<WorkerEvidence>,
}

impl Incident {
    /// A warning or error with an identity and no input.
    pub fn plain(kind: IncidentKind, road: &'static str, hierarchy: &str, message: String) -> Self {
        Incident {
            kind,
            road,
            uri: badge(hierarchy),
            message,
            location: None,
            thread: None,
            input: None,
            cursor_byte: None,
            worker: None,
        }
    }

    /// The deduplication key. The message is not part of it: a repeated
    /// specimen keeps its latest message and counts. Nor is the input of a
    /// PANIC: the location identifies the defect, the first row keeps the
    /// input that reproduces it, and every later prefix of a line typed
    /// against a panicking probe is the same defect, not a new specimen.
    pub fn specimen_key(&self) -> String {
        let input = match self.kind {
            IncidentKind::Panic => String::new(),
            _ => self
                .input
                .as_deref()
                .map(super::database::input_sha256)
                .unwrap_or_default(),
        };
        let worker = match &self.worker {
            Some(w) => format!(
                "{}|{}|{}|{}|{}|{}",
                w.operation,
                w.entrance,
                w.budget_ms,
                w.containment,
                delightql_cst::PARSER_RUNTIME,
                delightql_cst::GRAMMAR_FINGERPRINT
            ),
            None => "|||||".to_string(),
        };
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.kind.as_str(),
            self.road,
            self.uri,
            self.location.as_deref().unwrap_or(""),
            input,
            worker
        )
    }
}

/// The full badge for a client hierarchy (`client/worker/budget` →
/// `delightql-error://client/worker/budget`). Every hierarchy used here
/// is a registered row; the registry test pins that.
pub fn badge(hierarchy: &str) -> String {
    format!("delightql-error://{hierarchy}")
}

pub const PANIC_URI: &str = "delightql-error://internal/panic";

/// Client identities. Constants, never literals at the sites — a typo
/// would mint a phantom no registry row explains.
pub mod hierarchy {
    pub const WORKER_UNAVAILABLE: &str = "client/worker/unavailable";
    pub const WORKER_BUDGET: &str = "client/worker/budget";
    pub const ASSISTANCE_DISABLED: &str = "client/assistance/disabled";
    pub const PREFLIGHT_REFUSED: &str = "client/preflight/refused";
    pub const LEDGER_WRITE_LOST: &str = "client/ledger/write_lost";
    pub const NAMESPACE_INSTALL: &str = "client/namespace/install";
    pub const CONFIG: &str = "client/config";
    pub const TERMINAL: &str = "client/terminal";
    pub const ARGUMENT: &str = "client/argument";
    pub const FORMAT: &str = "client/format";
    pub const SANITIZE_DISABLED: &str = "client/sanitize/disabled";
    pub const DATABASE_UNAVAILABLE: &str = "client/database/unavailable";
    pub const UNBADGED: &str = "client/unbadged";
    pub const REPORT_DESCRIPTION: &str = "client/report/description";

    /// Every hierarchy above, for the registry test.
    pub const ALL: &[&str] = &[
        WORKER_UNAVAILABLE,
        WORKER_BUDGET,
        ASSISTANCE_DISABLED,
        PREFLIGHT_REFUSED,
        LEDGER_WRITE_LOST,
        NAMESPACE_INSTALL,
        CONFIG,
        TERMINAL,
        ARGUMENT,
        FORMAT,
        SANITIZE_DISABLED,
        DATABASE_UNAVAILABLE,
        UNBADGED,
        REPORT_DESCRIPTION,
    ];
}

/// Say it on stderr AND record it. The one road for a client warning or
/// error that is not already a row somewhere (a submission's error is the
/// ledger's; a budget incident is recorded before it is announced).
/// Without a process database the words still reach the human.
pub fn report(kind: IncidentKind, road: &'static str, hierarchy: &str, message: String) {
    let label = match kind {
        IncidentKind::Warning => "warning",
        IncidentKind::Error => "error",
        IncidentKind::Info => "info",
        IncidentKind::Panic => "panic",
    };
    eprintln!("{label}: {message}");
    if let Some(db) = process_database() {
        db.record_incident(Incident::plain(kind, road, hierarchy, message));
    }
}

pub fn warning(road: &'static str, hierarchy: &str, message: String) {
    report(IncidentKind::Warning, road, hierarchy, message)
}

pub fn error(road: &'static str, hierarchy: &str, message: String) {
    report(IncidentKind::Error, road, hierarchy, message)
}

/// A panic as the hook saw it: message, location, and the thread it was
/// on. The hook cannot insert — it may fire while the panicking thread
/// holds the connection lock — so it queues, and the next boundary (a
/// prompt, the exit) drains into the table.
#[derive(Clone, Debug)]
pub struct PanicRecord {
    pub message: String,
    pub location: Option<String>,
    pub thread: String,
}

static PANIC_QUEUE: Mutex<Vec<PanicRecord>> = Mutex::new(Vec::new());

/// The queue is bounded like every client buffer; a runaway panic loop
/// keeps the first records, which are the informative ones.
const PANIC_QUEUE_CAPACITY: usize = 64;

pub fn queue_panic(record: PanicRecord) {
    if let Ok(mut queue) = PANIC_QUEUE.lock() {
        if queue.len() < PANIC_QUEUE_CAPACITY {
            queue.push(record);
        }
    }
}

/// Take everything queued so far.
pub fn drain_panics() -> Vec<PanicRecord> {
    PANIC_QUEUE
        .lock()
        .map(|mut queue| std::mem::take(&mut *queue))
        .unwrap_or_default()
}

impl PanicRecord {
    /// The incident row: kind `panic`, on the road named by the thread.
    pub fn into_incident(self) -> Incident {
        let road: &'static str = match self.thread.as_str() {
            "main" => "main",
            "query" => "query",
            _ => "thread",
        };
        Incident {
            kind: IncidentKind::Panic,
            road,
            uri: PANIC_URI.to_string(),
            message: self.message,
            location: self.location,
            thread: Some(self.thread),
            input: None,
            cursor_byte: None,
            worker: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every client hierarchy is a registered identifier row — reached the
    /// way a user reaches it, through `sys::identifiers.identifier` on an
    /// ordinary handle — and nothing registered under client/ lacks a
    /// constant here.
    #[test]
    fn every_client_hierarchy_is_registered() {
        let mut handle = crate::connection::open_handle().expect("handle");
        let mut session = handle.session().expect("session");
        let rows = crate::exec_ng::run_dql_query(
            "sys::identifiers.identifier(*), kind = \"error\" |> (hierarchy)",
            &mut *session,
        )
        .expect("the registry answers");
        let col = rows.columns.iter().position(|c| c == "hierarchy").unwrap();
        let registered: std::collections::BTreeSet<String> =
            rows.rows.iter().map(|r| r[col].clone()).collect();
        for hierarchy in hierarchy::ALL {
            assert!(
                registered.contains(*hierarchy),
                "client hierarchy '{hierarchy}' has no registry row"
            );
        }
        for h in registered.iter().filter(|h| h.starts_with("client/")) {
            assert!(
                hierarchy::ALL.contains(&h.as_str()),
                "registry row '{h}' has no client constant"
            );
        }
    }

    /// The dedup key ignores the message and honors every identity field.
    #[test]
    fn specimen_keys_distinguish_identity_not_wording() {
        let a = Incident::plain(IncidentKind::Warning, "ledger", hierarchy::LEDGER_WRITE_LOST, "x".into());
        let mut b = a.clone();
        b.message = "y".into();
        assert_eq!(a.specimen_key(), b.specimen_key(), "wording is not identity");
        let mut c = a.clone();
        c.road = "main";
        assert_ne!(a.specimen_key(), c.specimen_key());
        let mut d = a.clone();
        d.input = Some("users(*)".into());
        assert_ne!(a.specimen_key(), d.specimen_key());
        let mut e = a.clone();
        e.location = Some("x.rs:1".into());
        assert_ne!(a.specimen_key(), e.specimen_key());

        // A panic's input is reproduction, not identity.
        let mut p = Incident::plain(IncidentKind::Panic, "parser_worker", "internal/panic", "boom".into());
        p.location = Some("w.rs:9".into());
        p.input = Some("u".into());
        let mut q = p.clone();
        q.input = Some("us".into());
        assert_eq!(p.specimen_key(), q.specimen_key(), "prefixes of one line are one defect");
        let mut r = p.clone();
        r.location = Some("w.rs:10".into());
        assert_ne!(p.specimen_key(), r.specimen_key());
    }

    /// A panic on the query thread lands on the `query` road; the hook's
    /// queue is bounded and drains to empty.
    #[test]
    fn panic_records_route_by_thread_and_drain() {
        drain_panics();
        queue_panic(PanicRecord {
            message: "boom".into(),
            location: Some("a.rs:1".into()),
            thread: "query".into(),
        });
        queue_panic(PanicRecord {
            message: "bang".into(),
            location: None,
            thread: "main".into(),
        });
        let drained = drain_panics();
        assert_eq!(drained.len(), 2);
        let roads: Vec<&str> = drained
            .into_iter()
            .map(|r| r.into_incident().road)
            .collect();
        assert_eq!(roads, ["query", "main"]);
        assert!(drain_panics().is_empty());
    }
}
