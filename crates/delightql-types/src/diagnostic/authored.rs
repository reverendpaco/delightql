// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `authored/…` — what a program says about itself.

use super::Taxon;

/// The authored family: one fixed identity, no authored descendants.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Authored))]
pub enum Authored {
    /// A reached `abort!` — written by the program, or reached by a failing
    /// `assert!` — stops later effects, rolls back the current run, and
    /// reports this one identity with its authored label. The label is
    /// occurrence prose: authors mint no descendants under `authored/` and
    /// impersonate no compiler identity. Runs committed before a `;`
    /// boundary remain committed, and the session remains usable. Hookable:
    /// (~~error://authored/abort ~~).
    #[leaf("abort", class = Permission, summary = "The program aborted the run.")]
    #[error("{label}\n  abort input was nonempty{}", observation_failure.as_ref().map(|f| format!("\n  assertion observation failed; session quarantined: {f}")).unwrap_or_default())]
    Abort {
        label: String,
        /// When the abort was an assertion's and recording its verdict
        /// failed, the incident that quarantined the session.
        observation_failure: Option<String>,
    },
}
