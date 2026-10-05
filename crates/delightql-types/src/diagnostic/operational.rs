// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `operational/…` — the query is valid; this session refuses to run it.

use super::Taxon;

/// The operational family: policy, not meaning.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Operational))]
pub enum Operational {
    /// The query references namespaces served by different connections.
    /// DelightQL deliberately does not federate: split the query, or mount
    /// the data into one engine.
    #[leaf("federation-prohibited", class = Permission, summary = "One query may touch only one connection.")]
    #[error("Validation error: {message}")]
    FederationProhibited { message: String },

    /// A statement the compiler does not cover: outside its fragment, and
    /// never handed to another compiler.
    #[leaf("uncovered", class = Permission, summary = "This build's compiler does not cover the statement.")]
    #[error("{message}")]
    Uncovered { message: String },

    /// The compiler's resource budgets. Two, measuring different objects at
    /// different times: nesting (the authored tree, before any walk) and
    /// refinement-depth (active refiner frames, while it runs).
    #[family("resource", summary = "A compiler resource budget was exceeded.")]
    #[error(transparent)]
    Resource(Resource),
}

/// `operational/resource/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Operational, Operational::Resource))]
pub enum Resource {
    /// Depth is a RESOURCE policy, not a rule of the language (S11): nothing
    /// about DelightQL forbids a deeply nested query. The compiler's phase
    /// walks recurse, and a walk deeper than the stack it runs on aborts
    /// the process instead of answering — which, for a compiler embedded in
    /// another program through the C interface, takes the HOST down. So the
    /// depth is measured on the parse tree, before any recursive walk, and
    /// refused. Raise the budget with DELIGHTQL_MAX_NESTING (or the host's
    /// own setter), or flatten the query: a chain of pipe stages costs no
    /// depth where nested parentheses do.
    #[leaf("nesting", class = Permission, summary = "The query nests deeper than this session budgets for.")]
    #[error("Validation error: {message}")]
    Nesting { message: String },

    /// The refiner walks a chain recursively, and its walks carry stacksafe
    /// — so a walk that stops making progress does not overflow the stack
    /// and fail promptly; it grows stack segments and clones AST state until
    /// the MACHINE gives out. A compilation therefore spends a bounded
    /// number of active refiner frames (512 by default, measured against a
    /// corpus maximum of 101), and the frame that would exceed the budget
    /// is refused instead of entered. This is a RESOURCE policy, not a rule
    /// of the language, and it is a DIFFERENT budget from
    /// operational/resource/nesting: that one measures the authored parse
    /// tree before any walk, this one measures refinement while it runs,
    /// and raising one does not raise the other. Meeting this refusal
    /// usually means one of two things: an unusually deep query, which an
    /// operator may afford by raising DELIGHTQL_MAX_REFINEMENT_DEPTH up to
    /// the ceiling of 4096; or a cycle in the compiler, which is a bug worth
    /// reporting with the query that found it.
    /// sys::execution.compiler_limit(*) reports the default, the effective
    /// value, and the ceiling. The session stays usable: the refusal ends
    /// the one compilation and nothing else.
    #[leaf("refinement-depth", class = Permission, summary = "Refinement recursed deeper than this session budgets for.")]
    #[error("Validation error: {message}")]
    RefinementDepth { message: String },
}
