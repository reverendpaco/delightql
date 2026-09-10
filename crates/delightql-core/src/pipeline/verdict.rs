// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Verdict types for assertion-effect and expected-error outcomes.
//!
//! The pipeline produces verdicts; the runner (CLI, test harness, CI)
//! consumes them and applies a strategy (fail-early, collect-all, log-only).
//!
//! What a query DECLARES it expects, from `(~~error://… ~~)`, is an
//! [`ErrorSelector`]: validated once against the declared hierarchy at
//! normalization, matched against typed identities at judgment.

/// Whether the assertion effect or error hook passed or failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerdictOutcome {
    Pass,
    Fail,
}

/// Identifies which assertion effect or error hook produced the verdict.
#[derive(Debug, Clone)]
pub struct VerdictIdentity {
    /// Author-supplied assertion label, if any.
    pub name: Option<String>,
    /// Display text for the assertion or error hook.
    pub body_text: String,
}

/// A structured verdict produced for an assertion effect or error hook.
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
