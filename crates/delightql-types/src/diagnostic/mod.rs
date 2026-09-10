// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The diagnostic hierarchy: DelightQL's error identities as nested Rust.
//!
//! Rust containment IS URI containment. `DelightQLError::Semantic(
//! Semantic::Resolution(Resolution::Table { .. }))` renders as
//! `delightql-error://semantic/resolution/table`, and a state such as
//! `parse/resolution/table` has no Rust representation. Each leaf owns the
//! only payload lawful for its identity; each family is an enum of its
//! members; the hand-authored declaration is the active mint authority for
//! every fixed identity, and the registry rows `dql explain` reads are a
//! projection of it (`inventory`).
//!
//! What a producer does: construct a leaf. What it cannot do: omit the
//! identity, spell one as a string, pair one leaf's identity with another's
//! payload, choose a wire class, or change identity by rewording prose.
//! Matching (`ErrorSelector`) reads the typed segment view, never a rendered
//! URI.
//!
//! Prose conventions: the `summary` attribute is the one-line registry
//! summary; the doc comment is the explanation `dql explain` shows; the
//! `#[error]` string is the human presentation, free to improve without
//! touching identity.
//!
//! What cannot be written, as compile-fail proofs. An identity is not a
//! string anywhere in production:
//!
//! ```compile_fail
//! use delightql_types::diagnostic::*;
//! let _: DelightQLError = DelightQLError::from_hierarchy("semantic/resolution/table");
//! ```
//!
//! A leaf takes only its own payload — a table-not-found identity cannot be
//! constructed with a column-not-found payload:
//!
//! ```compile_fail
//! use delightql_types::diagnostic::*;
//! let _ = Resolution::Table { column: "c".to_string(), context: String::new() };
//! ```
//!
//! An identity cannot be assembled from segments outside the taxonomy:
//!
//! ```compile_fail
//! use delightql_types::taxon::ErrorId;
//! let _ = ErrorId { fixed: vec!["semantic", "invented"], tail: vec![] };
//! ```
//!
//! A family cannot hold a foreign leaf — `parse/resolution/table` has no
//! representation:
//!
//! ```compile_fail
//! use delightql_types::diagnostic::*;
//! let _ = DelightQLError::Parse(Resolution::Table { table: String::new(), context: String::new() });
//! ```

pub mod authored;
pub mod client;
pub mod dml;
pub mod imprint;
pub mod internal;
pub mod namespace;
pub mod operational;
pub mod parse;
pub mod runtime;
pub mod semantic;
pub mod target;

#[cfg(test)]
mod tests;

pub use crate::taxon::{
    inventory as walk_inventory, Carried, DiagnosticClass, ErrorId, ErrorSelector, InventoryRow,
    LeafDescriptor, Received, Role, SelectorRefusal, Taxon, ERROR_URI_SCHEME,
};
pub use authored::Authored;
pub use client::Client;
pub use delightql_macros::Taxon;
pub use dml::{Dml, DmlMarker, DmlRoles, DmlShape, DmlSource};
pub use imprint::{Blueprint, Imprint, Manifest};
pub use internal::Internal;
pub use namespace::{Namespace, NamespaceName};
pub use operational::{Operational, Resource};
pub use parse::{Parse, ParseAnon};
pub use runtime::{Mount, Runtime, SessionHealth};
pub use semantic::*;
pub use target::{
    DuckDb, NativeClass, Postgres, PostgresNative, Siso, Sqlite, SqliteNative, Target,
};

/// The root of the hierarchy: one typed diagnostic occurrence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
pub enum DelightQLError {
    /// Parse errors mean the query text could not be read as DelightQL at
    /// all — the grammar rejected it before any meaning was assigned. Check
    /// delimiter balance, operator spelling, and clause order. Hook family:
    /// (~~error://parse ~~) matches every parse error.
    #[family("parse", summary = "The source text is structurally invalid.")]
    #[error(transparent)]
    Parse(Parse),

    /// The query parsed but does not mean what it must: a name did not
    /// resolve, an arity or shape rule was violated, a definition
    /// disagrees with itself, an effect broke the algebra. Hook family:
    /// (~~error://semantic ~~) matches every semantic error, including the
    /// effect-discipline refusals under semantic/effect/.
    #[family(
        "semantic",
        summary = "The query is well-formed but semantically invalid."
    )]
    #[error(transparent)]
    Semantic(Semantic),

    /// DML errors cover insert!/update!/delete! shape and marker rules:
    /// marker/ (the !! mutation marker — missing, multiple, forbidden,
    /// mismatch), shape/ (required or meaningless clauses), source/ (what
    /// may feed a mutation), roles/ (what each argument stands for).
    #[family("dml", summary = "A data-modification query violated DML shape rules.")]
    #[error(transparent)]
    Dml(Dml),

    /// The query is valid and this session refuses to run it: a resource
    /// budget, a federation policy. Operational refusals are policy, not
    /// meaning — the same text may be accepted by a differently configured
    /// host.
    #[family("operational", summary = "This session refuses to run a valid query.")]
    #[error(transparent)]
    Operational(Operational),

    /// Compilation succeeded (or was never the question) and something
    /// failed while running: the engine refused generated SQL, a check the
    /// statement may not run without did not hold, the catalog store
    /// failed, a lock was poisoned, an external effect could not be
    /// compensated. Hook family: (~~error://runtime ~~).
    #[family("runtime", summary = "An execution-time failure.")]
    #[error(transparent)]
    Runtime(Runtime),

    /// Target errors originate in the mounted engine, not in DelightQL:
    /// target/<engine>/<class>/<code> embeds the world's taxonomy as the
    /// leaf (PostgreSQL SQLSTATE, SQLite result code) beside the fixed
    /// adapter failures (connect, orientation, unimplemented, protocol
    /// text, unknown handle). Hook family: (~~error://target/postgres ~~)
    /// matches any PostgreSQL-side failure.
    #[family("target", summary = "The foreign engine rejected or failed the query.")]
    #[error(transparent)]
    Target(Target),

    /// dql's own defects: a Rust panic, a compiler invariant that did not
    /// hold, an error that crossed a boundary without an identity.
    /// internal/ means DelightQL failed, as distinct from runtime/ (the
    /// query failed) — report these.
    #[family("internal", summary = "A defect in DelightQL itself.")]
    #[error(transparent)]
    Internal(Internal),

    /// imprint!'s linear lifecycle refusals: an archived blueprint namespace
    /// is visible but inert, and a manifest that cannot be realized names
    /// which of its facts stands in the way. Its own top segment: a
    /// lifecycle-policy refusal, not a query semantic error.
    #[family("imprint", summary = "A blueprint or manifest lifecycle refusal.")]
    #[error(transparent)]
    Imprint(Imprint),

    /// The system name guard: user-facing namespace creation refuses the
    /// reserved system name pool (exact sys/std/home, sys*/std* prefixes,
    /// `_` machinery segments, the sys::/std:: subtree). Its own top
    /// segment: a creation-policy refusal, not a query semantic error.
    #[family("namespace", summary = "A namespace-creation policy refusal.")]
    #[error(transparent)]
    Namespace(Namespace),

    /// The client's own incidents (repl::errors.incident): the parser
    /// containment worker, the prompt, the client database, the exit —
    /// facts about the PROCESS, recorded by the CLI, never minted by the
    /// compiler.
    #[family("client", summary = "An incident of the interactive client itself.")]
    #[error(transparent)]
    Client(Client),

    /// What a program says about itself: the one identity an authored
    /// `abort!` reaches. The authored label is occurrence prose; authors
    /// mint no descendants and impersonate no compiler identity.
    #[family("authored", summary = "A termination the program itself demanded.")]
    #[error(transparent)]
    Authored(Authored),

    /// An occurrence received over the protocol from a party of this build,
    /// carried on as the typed fact it already was: its identity, class and
    /// descriptor are the declared tree's, decoded once from the wire bytes
    /// ([`Received::decode`]); only its message is the peer's. Nothing may
    /// re-badge it.
    #[carrier]
    #[error("{0}")]
    Received(Received),
}

impl DelightQLError {
    /// The payload-free identity of this occurrence.
    pub fn id(&self) -> ErrorId {
        ErrorId::of(self)
    }

    /// The transport-neutral class of this occurrence.
    pub fn class(&self) -> DiagnosticClass {
        Taxon::class(self)
    }

    /// The rendered badge URI. For display, storage, protocol, and external
    /// APIs — never for an internal family question.
    pub fn error_uri(&self) -> String {
        self.id().uri()
    }

    /// The registry descriptor of this occurrence's leaf.
    pub fn descriptor(&self) -> &'static LeafDescriptor {
        Taxon::leaf(self)
    }
}

pub type Result<T> = std::result::Result<T, DelightQLError>;

/// The registry projection of the whole hierarchy: every family, emitted
/// leaf, external root, and retired identity, with its declared role.
pub fn inventory() -> Vec<InventoryRow> {
    walk_inventory::<DelightQLError>()
}

/// Validate an authored expectation path against this hierarchy.
pub fn selector(segments: &[&str]) -> std::result::Result<ErrorSelector, SelectorRefusal> {
    ErrorSelector::parse::<DelightQLError>(segments)
}

/// Decode an identity received over the protocol against this hierarchy:
/// `None` when the peer named nothing this build emits, so that the caller
/// judges the protocol rather than inventing an identity.
pub fn received(identity: &[u8], message: &[u8]) -> Option<Received> {
    Received::decode::<DelightQLError>(identity, message)
}

impl From<std::io::Error> for DelightQLError {
    fn from(error: std::io::Error) -> Self {
        Runtime::Io {
            message: error.to_string(),
        }
        .into()
    }
}
