// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! `target/<engine>/…` — failures that originate in the mounted engine.
//!
//! Each engine family holds the fixed adapter failures (connect,
//! orientation, unimplemented, protocol text, unknown handle) beside the
//! provider-owned open tail: the world's own code space, validated by the
//! provider type that carries it. Only those types' constructors build a
//! native terminal, and they build nothing outside their engine's subtree.

use super::{DiagnosticClass, Taxon};
use crate::taxon::ExternalTerminal;

/// The target family.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Target))]
pub enum Target {
    /// PostgreSQL through the fatboy adapter. The open tail is
    /// `<class>/<sqlstate>` — the world's taxonomy as the leaf
    /// (`target/postgres/undefined-object/42883`). Hook family:
    /// (~~error://target/postgres ~~) matches any PostgreSQL-side failure.
    #[family("postgres", summary = "A PostgreSQL-side failure.")]
    #[error(transparent)]
    Postgres(Postgres),

    /// DuckDB through the fatboy adapter or the in-process backend.
    #[family("duckdb", summary = "A DuckDB-side failure.")]
    #[error(transparent)]
    DuckDb(DuckDb),

    /// SQLite, in process. The open tail is `<class>/<code>` — the
    /// extended result code under its class word
    /// (`target/sqlite/constraint/2067`).
    #[family("sqlite", summary = "A SQLite-side failure.")]
    #[error(transparent)]
    Sqlite(Sqlite),

    /// A serial-in/serial-out coprocess profile (sqlite3, psql, osquery…):
    /// an engine reached through its own command-line tool, which reports
    /// text and no code.
    #[family("siso", summary = "A coprocess-profile failure.")]
    #[error(transparent)]
    Siso(Siso),
}

// ----------------------------------------------------------------------------
// The class vocabulary every provider tail uses
// ----------------------------------------------------------------------------

/// The engine-neutral class word of a native code, the second-to-last
/// segment of every provider tail. Ratified vocabulary (URI-DESIGN §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NativeClass {
    Syntax,
    TypeMismatch,
    Permission,
    UndefinedObject,
    Constraint,
    Timeout,
    Connection,
    Error,
}

impl NativeClass {
    pub fn word(&self) -> &'static str {
        match self {
            NativeClass::Syntax => "syntax",
            NativeClass::TypeMismatch => "type-mismatch",
            NativeClass::Permission => "permission",
            NativeClass::UndefinedObject => "undefined-object",
            NativeClass::Constraint => "constraint",
            NativeClass::Timeout => "timeout",
            NativeClass::Connection => "connection",
            NativeClass::Error => "error",
        }
    }

    pub fn from_word(word: &str) -> Option<NativeClass> {
        [
            NativeClass::Syntax,
            NativeClass::TypeMismatch,
            NativeClass::Permission,
            NativeClass::UndefinedObject,
            NativeClass::Constraint,
            NativeClass::Timeout,
            NativeClass::Connection,
            NativeClass::Error,
        ]
        .into_iter()
        .find(|c| c.word() == word)
    }

    /// The transport-neutral class a native class determines.
    pub fn diagnostic_class(&self) -> DiagnosticClass {
        match self {
            NativeClass::Permission => DiagnosticClass::Permission,
            NativeClass::Constraint => DiagnosticClass::Constraint,
            NativeClass::Timeout => DiagnosticClass::Timeout,
            NativeClass::Connection => DiagnosticClass::Connection,
            NativeClass::Syntax
            | NativeClass::TypeMismatch
            | NativeClass::UndefinedObject
            | NativeClass::Error => DiagnosticClass::Syntax,
        }
    }
}

/// A selector tail under a provider root: `[class]` or `[class, code]`
/// where `code` is admitted by the provider's own validator.
/// A lawful tail under a provider root: a class word alone names a family
/// of codes; a complete `<class>/<code>` names one occurrence, and its class
/// word must be the class the provider's own code law determines — a code
/// paired with a class it does not have is not a terminal at all.
fn validate_class_tail(
    tail: &[&str],
    code_ok: fn(&str) -> bool,
    class_of_code: fn(&str) -> Option<NativeClass>,
) -> bool {
    match tail {
        [class] => NativeClass::from_word(class).is_some(),
        [class, code] => {
            code_ok(code)
                && matches!(
                    (NativeClass::from_word(class), class_of_code(code)),
                    (Some(word), Some(determined)) if word == determined
                )
        }
        _ => false,
    }
}

/// The class a complete `<class>/<code>` tail determines — the code's own,
/// once the class word agrees with it; a class alone names a family of
/// codes, not an occurrence.
fn class_of_class_tail(
    tail: &[&str],
    code_ok: fn(&str) -> bool,
    class_of_code: fn(&str) -> Option<NativeClass>,
) -> Option<DiagnosticClass> {
    match tail {
        [class, code] if code_ok(code) => {
            let determined = class_of_code(code)?;
            (NativeClass::from_word(class) == Some(determined))
                .then(|| determined.diagnostic_class())
        }
        _ => None,
    }
}

// ----------------------------------------------------------------------------
// PostgreSQL
// ----------------------------------------------------------------------------

/// `target/postgres/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Target, Target::Postgres))]
pub enum Postgres {
    /// The fatboy adapter failed at a step that carries no SQLSTATE — a
    /// protocol exchange with the adapter process, a fetch it could not
    /// serve — reported with the context and the adapter's own text, which
    /// carries the precise identity the adapter answered with.
    #[leaf("adapter", class = Connection, summary = "The PostgreSQL adapter failed.")]
    #[error("Database operation failed: {context} - {detail}")]
    Adapter { context: String, detail: String },

    /// The fatboy process could not be spawned, or it disappeared between
    /// sessions; every term is answered with this until it is restarted.
    #[leaf("connect", class = Connection, summary = "The PostgreSQL adapter process could not be reached.")]
    #[error("{message}")]
    Connect { message: String },

    /// libpq reported a failure without a database error: the connection
    /// itself dropped or could not be established.
    #[leaf("connection", class = Connection, summary = "The PostgreSQL connection failed.")]
    #[error("{message}")]
    Connection { message: String },

    /// The client asked for a result orientation the adapter did not agree
    /// to during version negotiation.
    #[leaf("orientation", class = Connection, summary = "The requested orientation was not agreed.")]
    #[error("{message}")]
    Orientation { message: String },

    /// A protocol operation the PostgreSQL adapter does not implement.
    #[leaf("unimplemented", class = Permission, summary = "The PostgreSQL adapter does not implement this operation.")]
    #[error("{message}")]
    Unimplemented { message: String },

    /// A query or prepare term whose text is not UTF-8.
    #[leaf("protocol/text", class = Syntax, summary = "Protocol text was not UTF-8.")]
    #[error("{message}")]
    ProtocolText { message: String },

    /// A fetch, stat, or close named a handle the adapter does not hold.
    #[leaf("handle/unknown", class = Connection, summary = "An unknown result handle.")]
    #[error("unknown handle")]
    UnknownHandle,

    /// The engine's own refusal: `<class>/<sqlstate>` embeds PostgreSQL's
    /// SQLSTATE under its class word, the message preserved verbatim.
    #[external(summary = "A PostgreSQL error, by SQLSTATE class and code.")]
    #[error("{0}")]
    Native(PostgresNative),
}

/// A PostgreSQL SQLSTATE occurrence: validated code, its class, and the
/// engine's message. Constructed only by [`PostgresNative::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresNative {
    sqlstate: String,
    class: NativeClass,
    message: String,
}

impl PostgresNative {
    /// Five SQLSTATE characters: digits and uppercase letters.
    pub fn valid_sqlstate(code: &str) -> bool {
        code.len() == 5
            && code
                .chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_uppercase())
    }

    /// SQLSTATE → class word. Maps the standard classes: 42501 is a
    /// permission refusal inside the syntax class; 42P01/42703/42883 are
    /// undefined objects; 22 is a datatype problem; 23 a constraint; 28
    /// authorization; 57 an operator intervention (timeouts); 08 the
    /// connection.
    pub fn class_of(code: &str) -> NativeClass {
        let class = &code[..2.min(code.len())];
        match (code, class) {
            ("42501", _) => NativeClass::Permission,
            ("42P01", _) | ("42703", _) | ("42883", _) | ("42704", _) => {
                NativeClass::UndefinedObject
            }
            (_, "42") => NativeClass::Syntax,
            (_, "22") => NativeClass::TypeMismatch,
            (_, "23") => NativeClass::Constraint,
            (_, "28") => NativeClass::Permission,
            (_, "57") => NativeClass::Timeout,
            (_, "08") => NativeClass::Connection,
            _ => NativeClass::Error,
        }
    }

    /// The validated constructor: `None` when the code is not a SQLSTATE.
    pub fn new(sqlstate: &str, message: impl Into<String>) -> Option<PostgresNative> {
        if !Self::valid_sqlstate(sqlstate) {
            return None;
        }
        Some(PostgresNative {
            sqlstate: sqlstate.to_string(),
            class: Self::class_of(sqlstate),
            message: message.into(),
        })
    }

    pub fn sqlstate(&self) -> &str {
        &self.sqlstate
    }

    pub fn native_class(&self) -> NativeClass {
        self.class
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for PostgresNative {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl ExternalTerminal for PostgresNative {
    fn validate_tail(tail: &[&str]) -> bool {
        validate_class_tail(tail, Self::valid_sqlstate, |code| {
            Some(Self::class_of(code))
        })
    }
    fn class_of_tail(tail: &[&str]) -> Option<DiagnosticClass> {
        class_of_class_tail(tail, Self::valid_sqlstate, |code| {
            Some(Self::class_of(code))
        })
    }
    fn tail(&self) -> Vec<String> {
        vec![self.class.word().to_string(), self.sqlstate.clone()]
    }
    fn class(&self) -> DiagnosticClass {
        self.class.diagnostic_class()
    }
}

// ----------------------------------------------------------------------------
// DuckDB
// ----------------------------------------------------------------------------

/// `target/duckdb/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Target, Target::DuckDb))]
pub enum DuckDb {
    /// The fatboy process could not be spawned or reached.
    #[leaf("connect", class = Connection, summary = "The DuckDB adapter process could not be reached.")]
    #[error("{message}")]
    Connect { message: String },

    /// The client asked for a result orientation the adapter did not agree
    /// to during version negotiation.
    #[leaf("orientation", class = Connection, summary = "The requested orientation was not agreed.")]
    #[error("{message}")]
    Orientation { message: String },

    /// A protocol operation the DuckDB adapter does not implement.
    #[leaf("unimplemented", class = Permission, summary = "The DuckDB adapter does not implement this operation.")]
    #[error("{message}")]
    Unimplemented { message: String },

    /// A query term whose text is not UTF-8.
    #[leaf("protocol/text", class = Syntax, summary = "Protocol text was not UTF-8.")]
    #[error("{message}")]
    ProtocolText { message: String },

    /// A fetch, stat, or close named a handle the adapter does not hold.
    #[leaf("handle/unknown", class = Connection, summary = "An unknown result handle.")]
    #[error("unknown handle")]
    UnknownHandle,

    /// The engine refused a statement while preparing or executing it, or
    /// answered with a shape the adapter cannot relay. DuckDB reports text
    /// and no stable code; the message is the engine's own.
    #[leaf("error", class = Syntax, summary = "DuckDB refused the statement.")]
    #[error("{message}")]
    Engine { message: String },
}

// ----------------------------------------------------------------------------
// SQLite
// ----------------------------------------------------------------------------

/// `target/sqlite/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Target, Target::Sqlite))]
pub enum Sqlite {
    /// A query term whose text is not UTF-8.
    #[leaf("protocol/text", class = Syntax, summary = "Protocol text was not UTF-8.")]
    #[error("{message}")]
    ProtocolText { message: String },

    /// A fetch, stat, or close named a handle the party does not hold.
    #[leaf("handle/unknown", class = Connection, summary = "An unknown result handle.")]
    #[error("unknown handle")]
    UnknownHandle,

    /// The client asked for a result orientation the party did not agree to
    /// during version negotiation.
    #[leaf("orientation", class = Connection, summary = "The requested orientation was not agreed.")]
    #[error("{message}")]
    Orientation { message: String },

    /// A protocol operation the SQLite party does not implement.
    #[leaf("unimplemented", class = Permission, summary = "The SQLite party does not implement this operation.")]
    #[error("{message}")]
    Unimplemented { message: String },

    /// The streaming worker that reads rows died before answering; the
    /// result is unreadable.
    #[leaf("worker", class = Connection, summary = "The SQLite streaming worker died.")]
    #[error("{message}")]
    Worker { message: String },

    /// The engine refused an operation and the host carries only its text
    /// (the WASM system and coprocess roads); the message is the engine's
    /// own.
    #[leaf("error", class = Connection, summary = "SQLite refused an operation.")]
    #[error("{message}")]
    Engine { message: String },

    /// The engine's own refusal: `<class>/<code>` embeds SQLite's extended
    /// result code under its class word, the message preserved verbatim.
    #[external(summary = "A SQLite error, by result-code class and extended code.")]
    #[error("{0}")]
    Native(SqliteNative),
}

/// A SQLite result-code occurrence: the extended code, its class, and the
/// engine's message. Constructed only by [`SqliteNative::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteNative {
    extended_code: i32,
    class: NativeClass,
    message: String,
}

impl SqliteNative {
    fn valid_code(code: &str) -> bool {
        code.parse::<i32>().map(|c| c > 0).unwrap_or(false)
    }

    /// Primary result code → class word. 1 SQLITE_ERROR (a statement the
    /// engine could not accept: syntax, no such table or function, misuse)
    /// is the syntax class; 19 CONSTRAINT; 20 MISMATCH; 3/8/23 AUTH,
    /// READONLY, PERM; 5/6/9 BUSY, LOCKED, INTERRUPT; the engine's own
    /// failures (I/O, corruption, memory, cannot open) are the connection
    /// class; anything else is the general error class.
    /// The class of a spelled extended result code, when it is one.
    fn class_of_code(code: &str) -> Option<NativeClass> {
        code.parse::<i32>()
            .ok()
            .filter(|c| *c > 0)
            .map(Self::class_of)
    }

    pub fn class_of(extended_code: i32) -> NativeClass {
        match extended_code & 0xff {
            1 => NativeClass::Syntax,
            19 => NativeClass::Constraint,
            20 => NativeClass::TypeMismatch,
            3 | 8 | 23 => NativeClass::Permission,
            5 | 6 | 9 => NativeClass::Timeout,
            7 | 10 | 11 | 13 | 14 | 26 => NativeClass::Connection,
            _ => NativeClass::Error,
        }
    }

    /// The validated constructor: a positive extended result code.
    pub fn new(extended_code: i32, message: impl Into<String>) -> Option<SqliteNative> {
        if extended_code <= 0 {
            return None;
        }
        Some(SqliteNative {
            extended_code,
            class: Self::class_of(extended_code),
            message: message.into(),
        })
    }

    pub fn extended_code(&self) -> i32 {
        self.extended_code
    }

    pub fn native_class(&self) -> NativeClass {
        self.class
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for SqliteNative {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl ExternalTerminal for SqliteNative {
    fn validate_tail(tail: &[&str]) -> bool {
        validate_class_tail(tail, Self::valid_code, Self::class_of_code)
    }
    fn class_of_tail(tail: &[&str]) -> Option<DiagnosticClass> {
        class_of_class_tail(tail, Self::valid_code, Self::class_of_code)
    }
    fn tail(&self) -> Vec<String> {
        vec![
            self.class.word().to_string(),
            self.extended_code.to_string(),
        ]
    }
    fn class(&self) -> DiagnosticClass {
        self.class.diagnostic_class()
    }
}

// ----------------------------------------------------------------------------
// Coprocess profiles
// ----------------------------------------------------------------------------

/// `target/siso/…`
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Taxon)]
#[taxon(lineage(super::DelightQLError::Target, Target::Siso))]
pub enum Siso {
    /// A query term whose text is not UTF-8.
    #[leaf("protocol/text", class = Syntax, summary = "Protocol text was not UTF-8.")]
    #[error("{message}")]
    ProtocolText { message: String },

    /// A fetch, stat, or close named a handle the party does not hold.
    #[leaf("handle/unknown", class = Connection, summary = "An unknown result handle.")]
    #[error("unknown handle")]
    UnknownHandle,

    /// The client asked for a result orientation the party did not agree to
    /// during version negotiation.
    #[leaf("orientation", class = Connection, summary = "The requested orientation was not agreed.")]
    #[error("{message}")]
    Orientation { message: String },

    /// A protocol operation the eager party does not implement.
    #[leaf("unimplemented", class = Permission, summary = "The eager party does not implement this operation.")]
    #[error("{message}")]
    Unimplemented { message: String },

    /// The coprocess could not be spawned, lost its pipes, exited, or
    /// stopped answering within the frame timeout.
    #[leaf("process", class = Connection, summary = "The coprocess could not serve.")]
    #[error("{message}")]
    Process { message: String },

    /// The tool refused the statement; it reports text and no code, so the
    /// message is the tool's own.
    #[leaf("query", class = Connection, summary = "The coprocess tool refused the statement.")]
    #[error("{message}")]
    Query { message: String },

    /// The tool's output could not be read as the rows the profile
    /// promised.
    #[leaf("output", class = Connection, summary = "The coprocess output could not be parsed.")]
    #[error("{message}")]
    Output { message: String },
}
