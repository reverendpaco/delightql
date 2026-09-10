// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The one road from a rusqlite failure to a typed diagnostic.

use delightql_types::diagnostic::{DelightQLError, Sqlite, SqliteNative};

/// The engine's refusal, as its own identity: a SQLite result code becomes
/// the provider-owned terminal `target/sqlite/<class>/<code>`; a failure of
/// the binding layer with no engine code is `target/sqlite/error`. The
/// engine's message is preserved verbatim after the operation's name.
/// The engine's refusal under `operation`, the operation named before the
/// engine's own text.
pub fn engine_error(operation: &str, error: rusqlite::Error) -> DelightQLError {
    mapped(error, Some(operation))
}

/// The engine's refusal as its own identity, with the engine's own text: a
/// SQLite result code becomes the provider-owned terminal
/// `target/sqlite/<class>/<code>`; a failure of the binding layer with no
/// engine code is `target/sqlite/error`.
pub fn engine_refusal(error: rusqlite::Error) -> DelightQLError {
    mapped(error, None)
}

fn mapped(error: rusqlite::Error, operation: Option<&str>) -> DelightQLError {
    let prose = |text: String| match operation {
        Some(operation) => format!("{operation}: {text}"),
        None => text,
    };
    match error {
        rusqlite::Error::SqliteFailure(code, message) => {
            let message = prose(message.unwrap_or_else(|| code.to_string()));
            match SqliteNative::new(code.extended_code, message.clone()) {
                Some(native) => Sqlite::Native(native).into(),
                None => Sqlite::Engine { message }.into(),
            }
        }
        // A statement the engine refused while preparing it carries the same
        // result code as any other failure; rusqlite only adds the offset.
        rusqlite::Error::SqlInputError {
            error: code,
            msg,
            sql,
            offset,
        } => {
            let message = prose(format!("{msg} in {sql} at offset {offset}"));
            match SqliteNative::new(code.extended_code, message.clone()) {
                Some(native) => Sqlite::Native(native).into(),
                None => Sqlite::Engine { message }.into(),
            }
        }
        // A DelightQL diagnostic smuggled through a row callback comes back
        // as itself.
        rusqlite::Error::ToSqlConversionFailure(boxed) => {
            match boxed.downcast::<DelightQLError>() {
                Ok(diagnostic) => *diagnostic,
                Err(other) => Sqlite::Engine {
                    message: prose(other.to_string()),
                }
                .into(),
            }
        }
        other => Sqlite::Engine {
            message: prose(other.to_string()),
        }
        .into(),
    }
}
