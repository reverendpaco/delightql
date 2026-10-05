// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! DelightQL Shared Types
//!
//! This crate contains shared types used by both delightql-core and delightql-backends,
//! breaking the circular dependency between them.
//!
//! ## Architecture
//!
//! ```text
//! delightql-types (shared types, NO dependencies)
//!     ↓               ↓
//! delightql-core  delightql-backends
//! ```
//!
//! By extracting error types and core traits to this crate:
//! - delightql-core can define AST resolution logic
//! - delightql-backends can implement schema traits without depending on core
//! - delightql-core can then use backends for execution

pub mod db_traits;
pub mod diagnostic;
pub mod error;
pub mod factory;
pub mod identifier;
pub mod introspect;
pub mod namespace;
pub mod schema;
pub mod taxon;

// Test utilities (mock implementations for testing without real databases)
pub mod test_utils;

// Re-export commonly used types
pub use db_traits::{
    DatabaseConnection, DatabaseConnectionExt, DbValue, FromDbValue, Row, ToDbValue,
};
pub use error::{DelightQLError, Result};
pub use factory::{ConnectionComponents, ConnectionFactory};
pub use identifier::SqlIdentifier;
pub use introspect::{
    DatabaseIntrospector, DiscoveredAttribute, DiscoveredEntity, DiscoveredRelation,
};
pub use namespace::{NamespaceItem, NamespacePath};
pub use schema::{ColumnInfo, DatabaseSchema};

/// What SQLite's document admission states when a REAL has no spelling
/// SQLite reads back as the same REAL. The admission raises it inside the
/// engine's own error text, and the teaching finds it there by this phrase,
/// which contains no single quote.
pub const INEXACT_DOCUMENT_REAL: &str = "a REAL cannot enter a document on SQLite exactly";

/// Backend runtime messages with a known DQL-side remedy get the
/// teaching appended — the backend speaks its own vocabulary; the
/// remedy is ours. (Runtime is the honest enforcement point for
/// value-shape failures: declared types are affinity, not truth.)
pub fn teach_runtime_message(msg: String) -> String {
    if msg.contains("JSON cannot hold BLOB") {
        return format!(
            "{msg} — tree groups and document operations carry values through \
             JSON, which cannot hold BLOBs; encode the value explicitly \
             (hex:(col)) or exclude the column"
        );
    }
    // The admission raises through a JSON path error whose own wording
    // (`bad JSON path: '…'`) is not the failure: the statement runs from
    // the phrase to the path's closing quote.
    if let Some(start) = msg.find(INEXACT_DOCUMENT_REAL) {
        let end = msg[start..]
            .find('\'')
            .map_or(msg.len(), |offset| start + offset);
        return format!(
            "{} — SQLite converts between a REAL and its decimal text exactly \
             only at moderate magnitudes; carry the value in a column of its \
             own beside the record, tuple or tree",
            &msg[start..end]
        );
    }
    msg
}

#[cfg(test)]
mod teaching_tests {
    use super::{teach_runtime_message, INEXACT_DOCUMENT_REAL};

    /// The admission's refusal arrives inside SQLite's JSON path error; the
    /// teaching states the refusal and its value without the path wording,
    /// then the remedy. Any other message is untouched.
    #[test]
    fn an_inexact_document_real_is_taught_without_the_path_wording() {
        let raw = format!(
            "bad JSON path: '{INEXACT_DOCUMENT_REAL}: no spelling of \
             2.493814866205522628e+224 reads back as the same REAL'"
        );
        let taught = teach_runtime_message(raw);
        assert!(
            taught.starts_with(&format!(
                "{INEXACT_DOCUMENT_REAL}: no spelling of 2.493814866205522628e+224 \
                 reads back as the same REAL — "
            )),
            "{taught}"
        );
        assert!(taught.contains("carry the value in a column of its own"));
        assert!(!taught.contains("bad JSON path"));
        assert_eq!(
            teach_runtime_message("bad JSON path: '$.a['".to_string()),
            "bad JSON path: '$.a['"
        );
    }
}
