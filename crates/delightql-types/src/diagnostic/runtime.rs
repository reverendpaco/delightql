// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `runtime/…` — execution-time failures.

use super::{DelightQLError, Taxon};

/// The runtime family. Its own path is an emitted identity: the broad
/// operational failure no member describes more precisely.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Runtime))]
pub enum Runtime {
    /// Runtime errors happen after SQL generation: the database rejected the
    /// SQL, a connection dropped, or I/O failed. This is the family's own
    /// broad identity, reported where no member describes the failure more
    /// precisely; the message carries the operation and its cause.
    #[leaf("", class = Connection, summary = "Compilation succeeded; execution failed.")]
    #[error("Database operation failed: {message} - {details}")]
    General { message: String, details: String },

    /// The bootstrap store — the in-memory catalog of connections,
    /// namespaces, cartridges, entities and definitions — refused an
    /// operation. The message names the operation and the engine's own
    /// text. Not the user's database: the session's own bookkeeping.
    #[leaf("catalog", class = Connection, summary = "The session catalog store failed an operation.")]
    #[error("Catalog operation failed: {operation} - {cause}")]
    Catalog { operation: String, cause: String },

    /// The connection to a mounted or primary database was lost or unusable
    /// at execution time: a lock held across a panic is poisoned and every
    /// later holder finds it so.
    #[leaf("connection", class = Connection, summary = "A database connection failed or was poisoned.")]
    #[error("Connection lock poisoned: {message}")]
    Connection { message: String, recovery: String },

    /// A file or stream could not be read or written. The message is the
    /// operating system's own.
    #[leaf("io", class = Connection, summary = "An I/O operation failed.")]
    #[error("IO error: {message}")]
    Io { message: String },

    /// Compilation succeeded, but the engine rejected the statement while
    /// executing it — e.g. a JSON function received malformed text, a
    /// constraint fired, or a transaction statement failed. The message
    /// carries the engine's own error text. Hookable:
    /// (~~error://runtime/execution ~~).
    #[leaf("execution", class = Connection, summary = "The database engine refused the generated SQL at run time.")]
    #[error("{message}")]
    Execution { message: String },

    /// The protocol channel between the relay and its backend party failed:
    /// a frame could not be read or written, the peer went away.
    #[leaf("relay/transport", class = Connection, summary = "The relay's transport to its backend failed.")]
    #[error("{message}")]
    Transport { message: String },

    /// The peer answered an exchange with a term the protocol does not admit
    /// there — a Header where Ok or End was owed, a Data term to a Close.
    /// The channel works; the conversation does not.
    #[leaf("relay/protocol", class = Connection, summary = "The peer violated the relay protocol.")]
    #[error("{message}")]
    Protocol { message: String },

    /// An engine refused a statement and named nothing the system could
    /// classify. Reported as a bug in the statement rather than as an
    /// anonymous error — the name the hook road has always judged it under.
    #[leaf("bug", class = Connection, summary = "An engine refusal the system could not classify.")]
    #[error("{message}")]
    Bug { message: String },

    /// A compiler-written check the statement may not run without answered
    /// no, and the compiler attached no more specific refusal to it. The
    /// message quotes the check's SQL.
    #[leaf("obligation", class = Permission, summary = "A compiler obligation did not hold.")]
    #[error("Compiler obligation failed\n  SQL: {sql}")]
    Obligation { sql: String },

    /// A precondition the statement declared did not hold.
    #[leaf("precondition", class = Permission, summary = "A declared precondition did not hold.")]
    #[error("{message}")]
    Precondition { message: String },

    /// A session object was used after its lifecycle ended: a namespace
    /// delisted and then addressed, a handle closed and then read.
    #[leaf("useafterfree", class = Permission, summary = "A session object was used after its lifecycle ended.")]
    #[error("{message}")]
    UseAfterFree { message: String },

    /// The session's health latch. An external effect's recovery became
    /// uncertain, or a registration the catalog cannot represent was
    /// reached; the session is quarantined until reset.
    #[family("session_health", summary = "The session is quarantined until reset.")]
    #[error(transparent)]
    SessionHealth(SessionHealth),

    /// A mount's lifecycle failed after the engine had already acted: the
    /// attached schema could not be detached, or the catalog could not
    /// record what was attached.
    #[family("mount", summary = "A mount lifecycle step failed.")]
    #[error(transparent)]
    Mount(Mount),

    /// A statement declared the error it expects with `(~~error://… ~~)`
    /// and the outcome did not match: it succeeded, or it failed with an
    /// identity outside the declared family. The message names both.
    #[leaf("expectation", class = Constraint, summary = "The declared expected error did not come.")]
    #[error("{outcome}")]
    Expectation { declared: String, outcome: String },

    /// A value read from the engine could not be converted to the type the
    /// caller demanded: the storage class disagrees with the declared one.
    #[leaf("value", class = Connection, summary = "An engine value could not be converted to the demanded type.")]
    #[error("{message}")]
    Value { message: String },

    /// The connection kind serving this session does not support the
    /// operation asked of it: a coprocess or adapter connection cannot
    /// attach an in-memory schema, or answer a full result set. Not a
    /// query error — a capability of the road the data is reached by.
    #[leaf("unsupported", class = Permission, summary = "This connection kind does not support the operation.")]
    #[error("{message}")]
    Unsupported { message: String },
}

impl Runtime {
    /// A poisoned lock: the recurring shape of every `Mutex::lock` failure.
    pub fn poisoned(what: impl Into<String>, error: impl std::fmt::Display) -> DelightQLError {
        Runtime::Connection {
            message: what.into(),
            recovery: error.to_string(),
        }
        .into()
    }

    /// A bootstrap-store failure: the recurring shape of every catalog
    /// operation's `map_err`.
    pub fn catalog(operation: impl Into<String>, cause: impl std::fmt::Display) -> DelightQLError {
        Runtime::Catalog {
            operation: operation.into(),
            cause: cause.to_string(),
        }
        .into()
    }
}

/// `runtime/session_health/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Runtime, Runtime::SessionHealth))]
pub enum SessionHealth {
    /// An external effect — an attached database, a spawned process, a file
    /// mount_new! created — could not be compensated after a failure, so
    /// the session's state is uncertain. New queries are refused until a
    /// reset retries the compensation.
    #[leaf("external_effect", class = Connection, summary = "An external effect's recovery is uncertain; the session is quarantined.")]
    #[error("{message}")]
    ExternalEffect { message: String },

    /// A run created an object the session catalog cannot register, so the
    /// catalog and the target have diverged. Quarantined until reset.
    #[leaf("registration_unsupported", class = Connection, summary = "A created object cannot be registered in the catalog.")]
    #[error("{message}")]
    RegistrationUnsupported { message: String },
}

/// `runtime/mount/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Runtime, Runtime::Mount))]
pub enum Mount {
    /// The engine refused to detach a schema the session had attached.
    #[leaf("detach", class = Connection, summary = "A mounted schema could not be detached.")]
    #[error("{message}")]
    Detach { message: String },

    /// The engine attached the schema, but the session catalog could not
    /// record the mount; the attachment was reversed.
    #[leaf("registration", class = Connection, summary = "A mount could not be recorded in the catalog.")]
    #[error("{message}")]
    Registration { message: String },

    /// The locator names nothing this session can open: a malformed URL, an
    /// unsupported scheme or mechanism, a schema the target does not have,
    /// or a fragment the target kind cannot take.
    #[leaf("locator", class = Connection, summary = "The mount locator names nothing this session can open.")]
    #[error("{message}")]
    Locator { message: String },
}
