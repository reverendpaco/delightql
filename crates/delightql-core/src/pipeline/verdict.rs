// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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
    pub name: Option<String>,
    /// Display text for the assertion or error hook.
    pub body_text: String,
}

/// A structured verdict produced by the pipeline for each assertion
/// or error hook it encounters.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub outcome: VerdictOutcome,
    /// Read by the HOST through the verdict hook — the payload's purpose is
    /// to cross that boundary, so no compiler-visible reader stands inside
    /// this crate.
    #[allow(dead_code)]
    pub identity: VerdictIdentity,
    /// Human-readable detail (failure reason, matched URI, etc.).
    pub detail: Option<String>,
}

/// The error a query DECLARES it expects, from `(~~error://… ~~)`.
///
/// The declaration and its verdict belong together: this says what was asked
/// for, [`Verdict`] says what happened. The URI segments prefix-match, so
/// `["semantic"]` names a family and the empty list names any error at all.
#[derive(Debug, Clone)]
pub struct ExpectedError {
    /// URI segments for prefix matching, e.g. `["semantic", "arity"]`.
    /// Empty means "any error" (bare `(~error ~)`).
    pub uri_segments: Vec<String>,
}

impl ExpectedError {
    /// Check if an actual error URI matches this expected error via prefix matching.
    ///
    /// - Empty segments matches any URI (bare `(~error ~)`)
    /// - `["semantic"]` matches `"semantic"`, `"semantic/arity"`, `"semantic/arity/2"`
    /// - `["semantic", "arity"]` matches `"semantic/arity"`, `"semantic/arity/2"` but not `"semantic/type"`
    pub fn matches(&self, actual_uri: &str) -> bool {
        if self.uri_segments.is_empty() {
            return true;
        }
        // Annotations carry the bare hierarchy (the sigil declares the
        // kind); minted identities carry the badge scheme. Strip it so
        // `(~error://semantic ~)` matches `delightql-error://semantic/…`.
        let actual = actual_uri
            .strip_prefix(delightql_types::error::ERROR_URI_SCHEME)
            .unwrap_or(actual_uri);
        let expected = self.uri_segments.join("/");
        actual == expected || actual.starts_with(&format!("{}/", expected))
    }

    /// Format the expected URI for display.
    pub fn display_uri(&self) -> String {
        if self.uri_segments.is_empty() {
            "(any error)".to_string()
        } else {
            format!("error://{}", self.uri_segments.join("/"))
        }
    }
}
