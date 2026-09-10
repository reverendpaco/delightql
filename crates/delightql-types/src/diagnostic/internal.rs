// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `internal/…` — DelightQL's own defects.

use super::{DelightQLError, Taxon};

/// The internal family: never a user error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Internal))]
pub enum Internal {
    /// An internal invariant failed (a Rust panic). The CLI catches it and
    /// emits this record instead of a raw backtrace; rerun with
    /// RUST_BACKTRACE=1 for the developer trace, and please report the
    /// message and the query that triggered it. Your query may be perfectly
    /// valid — do not rewrite it to dodge this error; the bug is ours.
    #[leaf("panic", class = Connection, summary = "dql itself crashed. This is a bug in dql, not in your query.")]
    #[error("{message}")]
    Panic {
        message: String,
        location: Option<String>,
    },

    /// Every error a session reports should carry a delightql-error://
    /// identity. One that reaches the client with none is recorded under
    /// this identity in sys::diagnostics.finding so the hole is visible in
    /// the log rather than invisible in the message text. The message is
    /// the error's own; the missing badge is dql's omission, not yours.
    #[leaf("unbadged", class = Connection, summary = "An error crossed the session boundary without an identity.")]
    #[error("{message}")]
    Unbadged { message: String },

    /// The compiler reached a state its own laws say cannot be reached: a
    /// phase received a node the previous phase should have refused, a
    /// lowering met a shape resolution should have settled, the generator
    /// was handed something it has no spelling for. Never a user error —
    /// please report the query that produced it. `site` names where the
    /// invariant stood.
    #[leaf("invariant", class = Connection, summary = "A compiler invariant did not hold. This is a bug in dql.")]
    #[error("internal error ({site}): {detail} — this is a bug in dql, please report the query that produced it")]
    Invariant { site: String, detail: String },
}

impl Internal {
    /// The one convenience: an invariant at a named site.
    pub fn invariant(site: impl Into<String>, detail: impl Into<String>) -> DelightQLError {
        Internal::Invariant {
            site: site.into(),
            detail: detail.into(),
        }
        .into()
    }
}
