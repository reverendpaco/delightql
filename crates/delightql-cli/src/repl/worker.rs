// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The REPL parser worker: the hard containment boundary's far side, and the
//! framed wire both sides speak.
//!
//! Every REPL-local speculative parse runs here, in a child process spawned
//! from `current_exe()` through the hidden `__repl-parser-worker` subcommand
//! — parent and worker are the same binary, so grammar fingerprint, parser
//! runtime, and protocol always agree. The cooperative deadline in each
//! operation catches every parse that keeps reaching the runtime's
//! checkpoints — the minimized freeze trigger measurably does under the
//! progress-callback option — and the parent's kill-and-reap catches
//! whatever loops below them. No claim is made that the callback stops
//! every recovery loop; the process boundary is the guarantee.
//!
//! Frames are four-byte big-endian length prefixes over UTF-8 JSON, so
//! multiline input and arbitrary UTF-8 never become record delimiters.
//! Stdout is protocol-only; diagnostics go to stderr. Only verdicts,
//! highlight spans, continuation offsets, preflight facts, and cancellation
//! timing cross the wire — trees and parser objects never do.

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::time::{Duration, Instant};

use delightql_cst::{cst, CancellableParse, Parser, Root, SyntaxTree, TypedNode};

use super::config::ReplParserOperation;

/// Upper bound on one frame. A length beyond this is a protocol violation,
/// not an allocation request.
pub const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerRequest {
    pub request_id: u64,
    /// Parent-minted spawn counter; the worker refuses a mismatch.
    pub worker_generation: u64,
    /// `ReplParserOperation::as_str` spelling.
    pub operation: String,
    /// The parser entrance the parent selected for this input — `prompt`
    /// for the per-keystroke probes, the framing road for preflight. The
    /// worker's answer must agree; hard-kill evidence uses this copy.
    pub entrance: String,
    pub input: String,
    pub cursor_byte: Option<u64>,
    pub cooperative_budget_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerResponse {
    pub request_id: u64,
    pub worker_generation: u64,
    pub result: WorkerResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HighlightSpan {
    pub start: usize,
    pub end: usize,
    /// A capture/class name (`relation_name`, `pipe_operator`, or a
    /// highlights.scm capture); the parent maps names to colors.
    pub class: String,
}

/// Exactly one closed result variant per response.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum WorkerResult {
    WellFormed {
        well_formed: bool,
    },
    Highlights {
        spans: Vec<HighlightSpan>,
    },
    Continuations {
        byte_offsets: Vec<usize>,
    },
    Preflight {
        defects: bool,
        has_root_branch: bool,
        entrance: String,
    },
    Cancelled {
        elapsed_ms: f64,
        last_progress_byte: Option<u64>,
        entrance: String,
    },
    /// The operation panicked inside the worker. The worker survives and
    /// answers; the parent owns the record. `location` is `file:line`.
    Panicked {
        message: String,
        location: Option<String>,
    },
}

/// What the panic hook saw, for `serve` to read back after the unwind is
/// caught: the hook has the location, the caught payload does not.
static LAST_PANIC: std::sync::Mutex<Option<(String, Option<String>)>> =
    std::sync::Mutex::new(None);

/// Called by the process panic hook when this process is the worker: keep
/// the facts for the answer and stay silent — the parent records and says
/// it once, on its own terminal.
pub fn stash_panic(message: String, location: Option<String>) {
    if let Ok(mut last) = LAST_PANIC.lock() {
        *last = Some((message, location));
    }
}

pub fn write_frame<W: Write>(writer: &mut W, payload: &[u8]) -> std::io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"))?;
    writer.write_all(&len.to_be_bytes())?;
    writer.write_all(payload)?;
    writer.flush()
}

pub fn read_frame<R: Read>(reader: &mut R) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_bytes = [0u8; 4];
    match reader.read_exact(&mut len_bytes) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_bytes);
    if len > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds the protocol bound"),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload)?;
    Ok(Some(payload))
}

/// The parser entrance name the evidence records for a road.
fn root_entrance_name(root: Root) -> &'static str {
    match root {
        // The prompt road parses at the definition-file root under the
        // prompt wrap; the recorded entrance is the road, not the root.
        Root::DefinitionFile => "prompt",
        Root::QuerySequence => "query_sequence",
        Root::CompanionCell => "companion_cell",
    }
}

/// The parser entrance name the evidence records for a tree.
fn entrance_name(tree: &SyntaxTree) -> &'static str {
    root_entrance_name(tree.entrance())
}

/// The worker process body: serve framed requests until stdin closes.
/// A protocol violation or generation mismatch is fatal on purpose — the
/// parent kills and replaces a worker it cannot trust, and this side never
/// guesses.
pub fn run_worker(generation: u64, highlights: Option<std::path::PathBuf>) -> anyhow::Result<()> {
    #[cfg(feature = "prettify")]
    {
        let config = super::syntax_highlighter::HighlightConfig::from_path(highlights.as_deref());
        super::syntax_highlighter::init_highlighter(config);
    }
    #[cfg(not(feature = "prettify"))]
    let _ = highlights;

    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = stdin.lock();
    let mut writer = stdout.lock();
    let mut parser = Parser::new();

    while let Some(payload) = read_frame(&mut reader)? {
        // Test hook for the parent's hard containment road: a worker that
        // reads its request and never answers. Hidden, test-only — the
        // production freeze class this simulates is a parse looping below
        // the cooperative checkpoints.
        if std::env::var_os("DQL_TEST_WORKER_HANG").is_some() {
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        // Test hook for the forwarded-panic road: the operation panics
        // inside the worker, the worker answers `Panicked` and lives.
        let panic_on_serve = std::env::var_os("DQL_TEST_WORKER_PANIC").is_some();
        let request: WorkerRequest = serde_json::from_slice(&payload)
            .map_err(|e| anyhow::anyhow!("protocol violation: unreadable request: {e}"))?;
        anyhow::ensure!(
            request.worker_generation == generation,
            "protocol violation: request generation {} against worker generation {generation}",
            request.worker_generation
        );
        // The panic road: the parser is a c2rust runtime and the typed CST
        // reads it; a panic in either is an ANSWER, never a dead worker.
        let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if panic_on_serve {
                panic!("deliberate worker panic (DQL_TEST_WORKER_PANIC)");
            }
            serve(&mut parser, &request)
        })) {
            Ok(result) => result,
            Err(payload) => {
                let stashed = LAST_PANIC.lock().ok().and_then(|mut l| l.take());
                let (message, location) = match stashed {
                    Some(facts) => facts,
                    None => (payload_message(&*payload), None),
                };
                // A panic may have left the parser mid-parse; a fresh one
                // costs nothing and carries no state.
                parser = Parser::new();
                WorkerResult::Panicked { message, location }
            }
        };
        let response = WorkerResponse {
            request_id: request.request_id,
            worker_generation: generation,
            result,
        };
        write_frame(&mut writer, &serde_json::to_vec(&response)?)?;
    }
    Ok(())
}

fn payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with non-string payload".to_string()
    }
}

fn serve(parser: &mut Parser, request: &WorkerRequest) -> WorkerResult {
    let budget = Duration::from_millis(request.cooperative_budget_ms);
    let started = Instant::now();
    let deadline = started + budget;
    let mut should_cancel = |_: usize| Instant::now() >= deadline;

    let operation = ReplParserOperation::ALL
        .into_iter()
        .find(|op| op.as_str() == request.operation);
    match operation {
        Some(ReplParserOperation::PromptWellFormed) => {
            match parser.parse_prompt_cancellable(&request.input, &mut should_cancel) {
                CancellableParse::Completed(tree) => WorkerResult::WellFormed {
                    well_formed: !tree.has_defects() && tree.root_branch_if_shaped().is_some(),
                },
                CancellableParse::Cancelled {
                    last_progress_byte,
                    entrance,
                } => cancelled(started, last_progress_byte, root_entrance_name(entrance)),
            }
        }
        Some(ReplParserOperation::ContinuationNavigation) => {
            match parser.parse_prompt_cancellable(&request.input, &mut should_cancel) {
                CancellableParse::Completed(tree) => WorkerResult::Continuations {
                    byte_offsets: continuation_offsets(&tree),
                },
                CancellableParse::Cancelled {
                    last_progress_byte,
                    entrance,
                } => cancelled(started, last_progress_byte, root_entrance_name(entrance)),
            }
        }
        Some(ReplParserOperation::SyntaxHighlight) => {
            highlight_result(parser, request, started, &mut should_cancel)
        }
        Some(ReplParserOperation::SubmissionPreflight) => {
            match parser.parse_submission_cancellable(&request.input, &mut should_cancel) {
                // Recovery can leave the root itself unrecognized (`((`):
                // the shaped accessor answers None there instead of
                // panicking the worker.
                CancellableParse::Completed(tree) => WorkerResult::Preflight {
                    defects: tree.has_defects(),
                    has_root_branch: tree.root_branch_if_shaped().is_some(),
                    entrance: entrance_name(&tree).to_string(),
                },
                CancellableParse::Cancelled {
                    last_progress_byte,
                    entrance,
                } => {
                    // The framing decision is the submission's own; the
                    // cancellation carries the road it was on.
                    cancelled(started, last_progress_byte, root_entrance_name(entrance))
                }
            }
        }
        None => {
            // An unknown operation is a protocol-shaped refusal the parent
            // treats as a violation; answering a guessed verdict would be
            // worse. Cancelled-with-zero is not lied about — use an empty
            // highlight answer? No: refuse loudly via stderr and echo an
            // impossible verdict the parent's validation rejects.
            eprintln!(
                "repl-parser-worker: unknown operation '{}'",
                request.operation
            );
            WorkerResult::Cancelled {
                elapsed_ms: 0.0,
                last_progress_byte: None,
                entrance: "unknown_operation".to_string(),
            }
        }
    }
}

fn cancelled(started: Instant, last_progress_byte: Option<usize>, entrance: &str) -> WorkerResult {
    WorkerResult::Cancelled {
        elapsed_ms: started.elapsed().as_secs_f64() * 1000.0,
        last_progress_byte: last_progress_byte.map(|b| b as u64),
        entrance: entrance.to_string(),
    }
}

/// Every continuation anchor in the line, as BYTE offsets: where the text to
/// the left already stands as a relational expression. The parent converts
/// to char positions against the line it holds.
fn continuation_offsets(tree: &SyntaxTree) -> Vec<usize> {
    let mut offsets: Vec<usize> = delightql_cst::walk(tree)
        .filter(|node| {
            cst::Continuation::cast(node.node()).is_some()
                || cst::Relex::cast(node.node()).is_some()
                || cst::Effrelex::cast(node.node()).is_some()
        })
        .filter_map(|node| tree.byte_range(node).map(|range| range.start))
        .collect();
    offsets.sort_unstable();
    offsets.dedup();
    offsets
}

#[cfg(feature = "prettify")]
fn highlight_result(
    parser: &mut Parser,
    request: &WorkerRequest,
    started: Instant,
    should_cancel: &mut dyn FnMut(usize) -> bool,
) -> WorkerResult {
    match super::syntax_highlighter::highlight_spans(parser, &request.input, should_cancel) {
        Some(spans) => WorkerResult::Highlights { spans },
        None => cancelled(started, None, "prompt"),
    }
}

#[cfg(not(feature = "prettify"))]
fn highlight_result(
    _parser: &mut Parser,
    _request: &WorkerRequest,
    _started: Instant,
    _should_cancel: &mut dyn FnMut(usize) -> bool,
) -> WorkerResult {
    WorkerResult::Highlights { spans: Vec::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Framed wire: multiline and non-ASCII payloads round-trip; the length
    /// prefix, not content, delimits records.
    #[test]
    fn frames_round_trip_multiline_and_unicode() {
        let request = WorkerRequest {
            request_id: 7,
            worker_generation: 3,
            operation: "prompt_well_formed".to_string(),
            entrance: "prompt".to_string(),
            input: "users(*)\n  |> (naïve, δ)\n\"line\nbreak\"".to_string(),
            cursor_byte: Some(5),
            cooperative_budget_ms: 25,
        };
        let mut wire = Vec::new();
        write_frame(&mut wire, &serde_json::to_vec(&request).unwrap()).unwrap();
        write_frame(&mut wire, br#"{"second":"frame"}"#).unwrap();

        let mut reader = &wire[..];
        let first = read_frame(&mut reader).unwrap().expect("first frame");
        let decoded: WorkerRequest = serde_json::from_slice(&first).unwrap();
        assert_eq!(decoded.input, request.input);
        assert_eq!(decoded.request_id, 7);
        let second = read_frame(&mut reader).unwrap().expect("second frame");
        assert_eq!(second, br#"{"second":"frame"}"#);
        assert!(read_frame(&mut reader).unwrap().is_none(), "clean EOF");
    }

    /// An oversized length prefix is a protocol violation, not an allocation.
    #[test]
    fn an_absurd_frame_length_refuses() {
        let mut wire = Vec::new();
        wire.extend_from_slice(&(MAX_FRAME_BYTES + 1).to_be_bytes());
        assert!(read_frame(&mut &wire[..]).is_err());
    }

    /// The served operations answer their closed variants.
    #[test]
    fn operations_answer_their_variants() {
        let mut parser = Parser::new();
        let request = |operation: &str, input: &str| WorkerRequest {
            request_id: 1,
            worker_generation: 1,
            operation: operation.to_string(),
            entrance: "prompt".to_string(),
            input: input.to_string(),
            cursor_byte: None,
            cooperative_budget_ms: 1_000,
        };
        match serve(&mut parser, &request("prompt_well_formed", "users(*)")) {
            WorkerResult::WellFormed { well_formed } => assert!(well_formed),
            other => panic!("wrong variant: {other:?}"),
        }
        match serve(&mut parser, &request("prompt_well_formed", "users(*) |>")) {
            WorkerResult::WellFormed { well_formed } => assert!(!well_formed),
            other => panic!("wrong variant: {other:?}"),
        }
        match serve(
            &mut parser,
            &request("continuation_navigation", "users(*) |> (id)"),
        ) {
            WorkerResult::Continuations { byte_offsets } => {
                assert!(!byte_offsets.is_empty())
            }
            other => panic!("wrong variant: {other:?}"),
        }
        match serve(&mut parser, &request("submission_preflight", "users(*)")) {
            WorkerResult::Preflight {
                defects,
                has_root_branch,
                entrance,
            } => {
                assert!(!defects);
                assert!(has_root_branch);
                assert_eq!(entrance, "prompt");
            }
            other => panic!("wrong variant: {other:?}"),
        }
        match serve(
            &mut parser,
            &request("submission_preflight", "#!dql query-sequence\nusers(*)"),
        ) {
            WorkerResult::Preflight { entrance, .. } => assert_eq!(entrance, "query_sequence"),
            other => panic!("wrong variant: {other:?}"),
        }
    }

    /// A cancelled MARKED submission reports the utility entrance — the
    /// deterministic cooperative-entrance pin, free of wire timing.
    #[test]
    fn a_cancelled_marked_submission_reports_its_entrance() {
        let mut parser = Parser::new();
        let marked = format!("#!dql query-sequence\n{}", "users(*)\n".repeat(30_000));
        let request = WorkerRequest {
            request_id: 1,
            worker_generation: 1,
            operation: "submission_preflight".to_string(),
            entrance: "query_sequence".to_string(),
            input: marked,
            cursor_byte: None,
            cooperative_budget_ms: 0,
        };
        match serve(&mut parser, &request) {
            WorkerResult::Cancelled { entrance, .. } => assert_eq!(entrance, "query_sequence"),
            other => panic!("a spent budget must cancel: {other:?}"),
        }
    }

    /// A zero cooperative budget cancels a large parse and reports timing.
    #[test]
    fn a_spent_budget_cancels() {
        let mut parser = Parser::new();
        let large = "users(*), ".repeat(20_000) + "users(*)";
        let request = WorkerRequest {
            request_id: 1,
            worker_generation: 1,
            operation: "prompt_well_formed".to_string(),
            entrance: "prompt".to_string(),
            input: large,
            cursor_byte: None,
            cooperative_budget_ms: 0,
        };
        match serve(&mut parser, &request) {
            WorkerResult::Cancelled { entrance, .. } => assert_eq!(entrance, "prompt"),
            other => panic!("a spent budget must cancel: {other:?}"),
        }
    }
}
