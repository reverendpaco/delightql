//! Verdict types for assertion and error hook outcomes.
//!
//! The pipeline produces verdicts; the runner (CLI, test harness, CI)
//! consumes them and applies a strategy (fail-early, collect-all, log-only).

/// Whether the assertion or error hook passed or failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictOutcome {
    Pass,
    Fail,
}

/// Identifies which assertion or error hook produced the verdict.
#[derive(Debug, Clone)]
pub struct VerdictIdentity {
    /// Author-supplied name (`(~~assert:"name" ... ~~)`), if any.
    pub _name: Option<String>,
    /// Source location (line, column) in the query file.
    pub _source_location: Option<(usize, usize)>,
    /// Display text for the assertion or error hook.
    pub body_text: String,
}

/// Author-declared disposition override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictIntent {
    /// Halt immediately on failure, regardless of runner strategy.
    #[allow(dead_code)]
    Exit,
}

/// A structured verdict produced by the pipeline for each assertion
/// or error hook it encounters.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub outcome: VerdictOutcome,
    pub identity: VerdictIdentity,
    /// Human-readable detail (failure reason, matched URI, etc.).
    pub detail: Option<String>,
    /// Author intent override, if declared.
    pub _intent: Option<VerdictIntent>,
}
