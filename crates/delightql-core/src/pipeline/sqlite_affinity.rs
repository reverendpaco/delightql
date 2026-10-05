// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! SQLite's type affinity of a declared column type (datatype3 §3.1): the
//! one statement of SQLite's rule. A SQLite declaration is free text; the
//! engine reads it by substring, in this order, and the affinity is all it
//! keeps of it.

/// SQLite's five affinities. `Blob` is the documented name of "none".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
}

impl Affinity {
    pub(crate) const ALL: [Affinity; 5] =
        [Affinity::Integer, Affinity::Text, Affinity::Blob, Affinity::Real, Affinity::Numeric];

    /// The affinity's name, in lower case, as SQLite's documentation spells
    /// it.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Affinity::Integer => "integer",
            Affinity::Text => "text",
            Affinity::Blob => "blob",
            Affinity::Real => "real",
            Affinity::Numeric => "numeric",
        }
    }
}

/// The affinity SQLite gives a column of this declared type: substring
/// tests in the documented order; an absent or empty declaration has none
/// (`Blob`). `None` where the catalog cannot establish it: `ANY` is NUMERIC
/// in an ordinary table and no affinity at all in a STRICT one, which the
/// catalog does not record.
pub(crate) fn declared_affinity(declared: Option<&str>) -> Option<Affinity> {
    let Some(declared) = declared else {
        return Some(Affinity::Blob);
    };
    let upper = declared.trim().to_ascii_uppercase();
    Some(if upper == "ANY" {
        return None;
    } else if upper.contains("INT") {
        Affinity::Integer
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        Affinity::Text
    } else if upper.is_empty() || upper.contains("BLOB") {
        Affinity::Blob
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        Affinity::Real
    } else {
        Affinity::Numeric
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_types_follow_the_documented_substring_order() {
        for (declared, affinity) in [
            (None, Some(Affinity::Blob)),
            (Some(""), Some(Affinity::Blob)),
            (Some("BLOB"), Some(Affinity::Blob)),
            (Some("INTEGER"), Some(Affinity::Integer)),
            (Some("BIGINT"), Some(Affinity::Integer)),
            (Some("INT64"), Some(Affinity::Integer)),
            (Some("VARCHAR(20)"), Some(Affinity::Text)),
            (Some("text"), Some(Affinity::Text)),
            (Some("REAL"), Some(Affinity::Real)),
            (Some("DOUBLE PRECISION"), Some(Affinity::Real)),
            (Some("FLOATING POINT"), Some(Affinity::Integer)),
            (Some("NUMERIC"), Some(Affinity::Numeric)),
            (Some("BOOLEAN"), Some(Affinity::Numeric)),
            (Some("DATE"), Some(Affinity::Numeric)),
            (Some("TIMESTAMP_NS"), Some(Affinity::Numeric)),
            (Some("JSON"), Some(Affinity::Numeric)),
            (Some("ANY"), None),
            (Some(" any "), None),
        ] {
            assert_eq!(declared_affinity(declared), affinity, "{declared:?}");
        }
    }
}
