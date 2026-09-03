// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Destructure vocabularies.
//!
//! The comma member's three kinds are `Continuation` variants (see `chain`);
//! what remains here is the shared data those kinds carry.

use crate::lispy::ToLispy;

/// Mapping from JSON key to output column name
/// Used in destructuring to support renaming: {"json_key": column_name}
#[derive(Debug, Clone, PartialEq)]
pub struct DestructureMapping {
    /// Key in the JSON object (used in json_extract path)
    pub json_key: String,
    /// Output column occurrence for this extracted value.
    pub column: crate::relation::PortId,
}

impl ToLispy for DestructureMapping {
    fn to_lispy(&self) -> String {
        format!("(mapping {} {:?})", self.json_key, self.column)
    }
}

/// Destructuring operation mode
#[derive(Debug, Clone, PartialEq)]
pub enum DestructureMode {
    /// Scalar: ~= {pattern}
    /// Generates: json_extract(col, '$.field') for each field
    /// No row explosion, NULL for missing keys
    Scalar,

    /// Aggregate: ~= ~> {pattern}
    /// Generates: LEFT JOIN json_each(col)
    /// Row explosion, preserves rows with NULLs for empty/null arrays
    Aggregate,
}

impl ToLispy for DestructureMode {
    fn to_lispy(&self) -> String {
        match self {
            DestructureMode::Scalar => "scalar".to_string(),
            DestructureMode::Aggregate => "aggregate".to_string(),
        }
    }
}
