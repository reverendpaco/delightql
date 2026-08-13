// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! SQL Identifier newtype with case-insensitive semantics.
//!
//! SQL identifiers (column names, table names, schema names) are case-insensitive
//! unless quoted (stropped). This newtype preserves the original case for display
//! and SQL generation, but provides case-folding `PartialEq`, `Eq`, `Hash`, and
//! `Ord` for UNstropped identifiers, and case-sensitive comparison for stropped
//! ones. The stroppedness bit is what lets the type honor both regimes; the raw
//! text (spelling) is always preserved verbatim for the generator to re-quote.

use serde::de::{self, Deserializer, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// A SQL identifier that compares by canonical form.
///
/// Preserves the original case (for display, error messages, SQL generation)
/// while providing case-insensitive equality/hashing/ordering when unstropped
/// and case-sensitive when stropped.
#[derive(Clone, Debug)]
pub struct SqlIdentifier {
    text: String,
    stropped: bool,
}

impl SqlIdentifier {
    /// Create a new (unstropped) SqlIdentifier from a string.
    pub fn new(s: impl Into<String>) -> Self {
        SqlIdentifier {
            text: s.into(),
            stropped: false,
        }
    }

    /// Create a stropped (quoted) SqlIdentifier: case-sensitive semantics.
    pub fn stropped(s: impl Into<String>) -> Self {
        SqlIdentifier {
            text: s.into(),
            stropped: true,
        }
    }

    /// Whether this identifier was stropped (quoted) and thus compares case-sensitively.
    pub fn is_stropped(&self) -> bool {
        self.stropped
    }

    /// Get the inner string (original case/spelling preserved).
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// Consume self and return the inner text String (spelling, not re-quoted).
    pub fn into_inner(self) -> String {
        self.text
    }

    /// Identifier equality between raw spellings, both treated as
    /// UNSTROPPED (they fold). A call here marks a site whose operands are
    /// not `SqlIdentifier`s and therefore carry no stropping bit — the
    /// comparison cannot honour one it was never given.
    pub fn str_eq(a: &str, b: &str) -> bool {
        a.eq_ignore_ascii_case(b)
    }

    /// `str_eq` over optional qualifiers: both-absent is equal,
    /// one-absent is not.
    pub fn opt_str_eq(a: Option<&str>, b: Option<&str>) -> bool {
        match (a, b) {
            (Some(a), Some(b)) => Self::str_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }

    /// The canonical bytes used for equality/hash/ordering: ASCII-folded iff
    /// unstropped, verbatim iff stropped. The single source of truth so that
    /// Eq, Hash, and Ord cannot disagree.
    fn canonical_bytes(&self) -> impl Iterator<Item = u8> + '_ {
        let fold = !self.stropped;
        self.text
            .bytes()
            .map(move |b| if fold { b.to_ascii_lowercase() } else { b })
    }
}

// Equality compares canonical form (see canonical_bytes).
impl PartialEq for SqlIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.canonical_bytes().eq(other.canonical_bytes())
    }
}
impl Eq for SqlIdentifier {}

// Hash over canonical form (must be consistent with PartialEq).
impl Hash for SqlIdentifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for b in self.canonical_bytes() {
            b.hash(state);
        }
    }
}

// Ordering over canonical form (must agree with PartialEq).
impl PartialOrd for SqlIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SqlIdentifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.canonical_bytes().cmp(other.canonical_bytes())
    }
}

// Cross-type equality: the `str`/`String` side is treated as an UNstropped
// identifier (folds); the SqlIdentifier side uses its own canonical form.
// These mostly serve comparisons against known-lowercase constants.
impl PartialEq<str> for SqlIdentifier {
    fn eq(&self, other: &str) -> bool {
        self.canonical_bytes()
            .eq(other.bytes().map(|b| b.to_ascii_lowercase()))
    }
}

impl PartialEq<&str> for SqlIdentifier {
    fn eq(&self, other: &&str) -> bool {
        *self == **other
    }
}

impl PartialEq<String> for SqlIdentifier {
    fn eq(&self, other: &String) -> bool {
        *self == **other
    }
}

// Reverse: str == SqlIdentifier
impl PartialEq<SqlIdentifier> for str {
    fn eq(&self, other: &SqlIdentifier) -> bool {
        other == self
    }
}

impl PartialEq<SqlIdentifier> for &str {
    fn eq(&self, other: &SqlIdentifier) -> bool {
        *other == **self
    }
}

impl PartialEq<SqlIdentifier> for String {
    fn eq(&self, other: &SqlIdentifier) -> bool {
        other == self.as_str()
    }
}

// Display preserves original case/spelling.
impl fmt::Display for SqlIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

// Deref to str for seamless read access (exposes the raw spelling).
impl Deref for SqlIdentifier {
    type Target = str;

    fn deref(&self) -> &str {
        &self.text
    }
}

impl AsRef<str> for SqlIdentifier {
    fn as_ref(&self) -> &str {
        &self.text
    }
}

// NOTE: `impl Borrow<str>` was REMOVED — it is unsound: Borrow requires
// hash(k) == hash(k.borrow()), but this type's Hash folds case (or honors
// stroppedness) while str::hash hashes raw bytes, so any HashMap<SqlIdentifier,_>
// lookup via &str already missed. Construct a key (SqlIdentifier::from(s)) instead.

// From conversions (unstropped — all existing call sites keep exact behavior).
impl From<String> for SqlIdentifier {
    fn from(s: String) -> Self {
        SqlIdentifier::new(s)
    }
}

impl From<&str> for SqlIdentifier {
    fn from(s: &str) -> Self {
        SqlIdentifier::new(s.to_string())
    }
}

impl From<SqlIdentifier> for String {
    fn from(id: SqlIdentifier) -> Self {
        id.text
    }
}

// An identifier serializes as ONE string, never as a struct: unstropped is
// the plain text, stropped is the text wrapped in backticks, and deserialize
// reads the backticks back as the stropping bit. Backticks are not admitted
// inside an identifier's text, so the delimiter cannot be confused with
// content.
impl Serialize for SqlIdentifier {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.stropped {
            serializer.serialize_str(&format!("`{}`", self.text))
        } else {
            serializer.serialize_str(&self.text)
        }
    }
}

impl<'de> Deserialize<'de> for SqlIdentifier {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SqlIdentifierVisitor;

        impl Visitor<'_> for SqlIdentifierVisitor {
            type Value = SqlIdentifier;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a SQL identifier string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<SqlIdentifier, E> {
                Ok(from_serialized(v))
            }

            fn visit_string<E: de::Error>(self, v: String) -> Result<SqlIdentifier, E> {
                // Avoid re-allocating for the common unstropped path.
                if is_backtick_wrapped(&v) {
                    Ok(SqlIdentifier::stropped(v[1..v.len() - 1].to_string()))
                } else {
                    Ok(SqlIdentifier::new(v))
                }
            }
        }

        deserializer.deserialize_str(SqlIdentifierVisitor)
    }
}

fn is_backtick_wrapped(s: &str) -> bool {
    s.len() >= 2 && s.starts_with('`') && s.ends_with('`')
}

fn from_serialized(s: &str) -> SqlIdentifier {
    if is_backtick_wrapped(s) {
        SqlIdentifier::stropped(s[1..s.len() - 1].to_string())
    } else {
        SqlIdentifier::new(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_case_insensitive_equality() {
        let a = SqlIdentifier::new("first_name");
        let b = SqlIdentifier::new("FIRST_NAME");
        let c = SqlIdentifier::new("First_Name");
        assert_eq!(a, b);
        assert_eq!(b, c);
        assert_eq!(a, c);
    }

    #[test]
    fn test_case_insensitive_inequality() {
        let a = SqlIdentifier::new("first_name");
        let b = SqlIdentifier::new("last_name");
        assert_ne!(a, b);
    }

    #[test]
    fn test_preserves_original_case() {
        let id = SqlIdentifier::new("First_Name");
        assert_eq!(id.as_str(), "First_Name");
        assert_eq!(id.to_string(), "First_Name");
    }

    #[test]
    fn test_cross_type_equality_str() {
        let id = SqlIdentifier::new("first_name");
        assert!(id == "FIRST_NAME");
        assert!(id == "first_name");
        assert!(id == "First_Name");
        assert!(id != "last_name");
    }

    #[test]
    fn test_cross_type_equality_string() {
        let id = SqlIdentifier::new("first_name");
        assert!(id == String::from("FIRST_NAME"));
    }

    #[test]
    fn test_reverse_equality() {
        let id = SqlIdentifier::new("first_name");
        assert!("FIRST_NAME" == id);
        assert!(String::from("FIRST_NAME") == id);
    }

    #[test]
    fn test_hash_consistency() {
        let a = SqlIdentifier::new("first_name");
        let b = SqlIdentifier::new("FIRST_NAME");

        let mut set = HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&b));
    }

    #[test]
    fn test_hashmap_lookup() {
        let mut map = HashMap::new();
        map.insert(SqlIdentifier::new("age"), 42);

        assert_eq!(map.get(&SqlIdentifier::new("AGE")), Some(&42));
        assert_eq!(map.get(&SqlIdentifier::new("Age")), Some(&42));
        assert_eq!(map.get(&SqlIdentifier::new("age")), Some(&42));
    }

    #[test]
    fn test_ordering() {
        let a = SqlIdentifier::new("alpha");
        let b = SqlIdentifier::new("BETA");
        let c = SqlIdentifier::new("Alpha");
        assert!(a < b);
        assert_eq!(a.cmp(&c), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_deref_to_str() {
        let id = SqlIdentifier::new("users");
        let s: &str = &id;
        assert_eq!(s, "users");
        assert!(id.starts_with("user"));
    }

    #[test]
    fn test_from_conversions() {
        let a: SqlIdentifier = "hello".into();
        let b: SqlIdentifier = String::from("hello").into();
        let c: String = a.clone().into();
        assert_eq!(a, b);
        assert_eq!(c, "hello");
    }

    // --- Canonical-form matrix (the 3a ruling encoded) ---

    #[test]
    fn test_stropped_is_case_sensitive() {
        let a = SqlIdentifier::stropped("Daniel");
        let b = SqlIdentifier::stropped("daniel");
        assert_ne!(a, b);
        assert_eq!(a, SqlIdentifier::stropped("Daniel"));
        assert!(a.is_stropped());
        assert!(!SqlIdentifier::new("Daniel").is_stropped());
    }

    #[test]
    fn test_mixed_stropped_unstropped_equal_when_canonical_matches() {
        // stropped `daniel` (canonical "daniel") == unstropped DANIEL (folds to "daniel").
        assert_eq!(SqlIdentifier::stropped("daniel"), SqlIdentifier::new("DANIEL"));
        assert_eq!(SqlIdentifier::new("DANIEL"), SqlIdentifier::stropped("daniel"));
    }

    #[test]
    fn test_mixed_stropped_unstropped_unequal_on_case() {
        // stropped `Daniel` (canonical "Daniel") != unstropped daniel (folds to "daniel").
        assert_ne!(SqlIdentifier::stropped("Daniel"), SqlIdentifier::new("daniel"));
        assert_ne!(SqlIdentifier::new("daniel"), SqlIdentifier::stropped("Daniel"));
    }

    #[test]
    fn test_hash_and_ord_agree_with_eq_stropped() {
        let strop_lower = SqlIdentifier::stropped("daniel");
        let unstrop_upper = SqlIdentifier::new("DANIEL");
        assert_eq!(strop_lower, unstrop_upper);
        assert_eq!(strop_lower.cmp(&unstrop_upper), std::cmp::Ordering::Equal);

        // Equal canonical => equal hash => found in a set/map keyed either way.
        let mut set = HashSet::new();
        set.insert(strop_lower.clone());
        assert!(set.contains(&unstrop_upper));

        let mut map = HashMap::new();
        map.insert(SqlIdentifier::stropped("daniel"), 7);
        assert_eq!(map.get(&SqlIdentifier::new("DANIEL")), Some(&7));

        // Case-sensitive stropped keys stay distinct.
        map.insert(SqlIdentifier::stropped("Daniel"), 9);
        assert_eq!(map.get(&SqlIdentifier::stropped("Daniel")), Some(&9));
        assert_eq!(map.get(&SqlIdentifier::stropped("daniel")), Some(&7));
    }

    #[test]
    fn test_ord_stropped_sensitive() {
        // 'D' (0x44) < 'd' (0x64) as raw bytes; stropped comparison is verbatim.
        let big = SqlIdentifier::stropped("Daniel");
        let small = SqlIdentifier::stropped("daniel");
        assert!(big < small);
    }

    // --- Serde (string format preserved) ---

    #[test]
    fn test_serde_unstropped_roundtrip_plain_string() {
        let id = SqlIdentifier::new("first_name");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"first_name\"");
        let back: SqlIdentifier = serde_json::from_str(&json).unwrap();
        assert!(!back.is_stropped());
        assert_eq!(back.as_str(), "first_name");
    }

    #[test]
    fn test_serde_stropped_roundtrip_backtick_wrapped() {
        let id = SqlIdentifier::stropped("Weird Name");
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"`Weird Name`\"");
        let back: SqlIdentifier = serde_json::from_str(&json).unwrap();
        assert!(back.is_stropped());
        assert_eq!(back.as_str(), "Weird Name");
    }

    // --- Authority functions: str_eq / opt_str_eq ---

    #[test]
    fn test_str_eq_folds_case() {
        assert!(SqlIdentifier::str_eq("first_name", "FIRST_NAME"));
        assert!(SqlIdentifier::str_eq("First_Name", "first_name"));
        assert!(SqlIdentifier::str_eq("id", "id"));
        assert!(!SqlIdentifier::str_eq("id", "id_2"));
        assert!(!SqlIdentifier::str_eq("first", "last"));
    }

    #[test]
    fn test_opt_str_eq_some_folds() {
        assert!(SqlIdentifier::opt_str_eq(Some("users"), Some("USERS")));
        assert!(!SqlIdentifier::opt_str_eq(Some("users"), Some("orders")));
    }

    #[test]
    fn test_opt_str_eq_none_cases() {
        assert!(SqlIdentifier::opt_str_eq(None, None));
        assert!(!SqlIdentifier::opt_str_eq(Some("users"), None));
        assert!(!SqlIdentifier::opt_str_eq(None, Some("users")));
    }

    #[test]
    fn test_serde_plain_string_deserializes_unstropped() {
        let back: SqlIdentifier = serde_json::from_str("\"users\"").unwrap();
        assert!(!back.is_stropped());
        assert_eq!(back.as_str(), "users");
    }
}
