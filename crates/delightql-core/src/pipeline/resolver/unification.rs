// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE VOCABULARY OF AN AUTHORED LOOKUP: the reference a form writes and
//! the judgment it receives.
//!
//! The judgment itself is made by the lexical authority
//! (`resolver::lexical`) over frames only it can see. Nothing here decides
//! anything; a module that could decide from these types alone would be a
//! second lookup.

use delightql_types::SqlIdentifier;

#[derive(Debug)]
pub enum UnificationResult {
    /// The frontier's terminal answer: the occurrence the reference
    /// addresses, minted by the judgment that found it.
    Resolved(crate::pipeline::asts::core::ColumnOccurrence),
    Unresolved(String),
    Ambiguous {
        column: String,
        tables: Vec<String>,
    },
    /// A relation in view publishes dimensions the target never described.
    /// The reference is neither resolved nor absent: it was not searched.
    Opaque,
    /// The addressing refused with a teaching of its own, which every
    /// consumer surfaces unchanged rather than restating as "column not
    /// found". Treated as an absence by the widening a narrowed search does:
    /// what is missing HERE may still be answered outside.
    Refused(Refusal),
}

/// A refusal decided by the addressing, carried whole to the caller that
/// turns it into an error.
#[derive(Debug, Clone)]
pub struct Refusal {
    pub subcategory: &'static str,
    pub message: String,
    pub context: &'static str,
}

impl Refusal {
    pub fn into_error(self) -> crate::error::DelightQLError {
        crate::error::DelightQLError::validation_error_categorized(
            self.subcategory,
            self.message,
            self.context,
        )
    }
}

#[derive(Debug, Clone)]
pub enum ColumnReference {
    Named {
        name: SqlIdentifier,
        qualifier: Option<SqlIdentifier>,
    },
    Ordinal {
        position: u16,
        reverse: bool,
        qualifier: Option<SqlIdentifier>,
    },
}
