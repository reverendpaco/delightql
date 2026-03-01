//! SQL Identifier newtype with case-insensitive semantics.
//!
//! SQL identifiers (column names, table names, schema names) are case-insensitive
//! unless quoted. This newtype preserves the original case for display and SQL
//! generation, but provides case-insensitive `PartialEq`, `Eq`, `Hash`, and `Ord`.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// A SQL identifier that compares case-insensitively.
///
/// Preserves the original case (for display, error messages, SQL generation)
/// while providing case-insensitive equality, hashing, and ordering.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SqlIdentifier(String);

impl SqlIdentifier {
    /// Create a new SqlIdentifier from a string.
    pub fn new(s: impl Into<String>) -> Self {
        SqlIdentifier(s.into())
    }

    /// Get the inner string (original case preserved).
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume self and return the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }
}

// Case-insensitive equality
impl PartialEq for SqlIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}
impl Eq for SqlIdentifier {}

// Case-insensitive hash (must be consistent with PartialEq)
impl Hash for SqlIdentifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for b in self.0.bytes() {
            b.to_ascii_lowercase().hash(state);
        }
    }
}

// Case-insensitive ordering
impl PartialOrd for SqlIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SqlIdentifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let a = self.0.as_bytes();
        let b = other.0.as_bytes();
        for (x, y) in a.iter().zip(b.iter()) {
            let ord = x.to_ascii_lowercase().cmp(&y.to_ascii_lowercase());
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        a.len().cmp(&b.len())
    }
}

// Cross-type equality: SqlIdentifier == str
impl PartialEq<str> for SqlIdentifier {
    fn eq(&self, other: &str) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<&str> for SqlIdentifier {
    fn eq(&self, other: &&str) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<String> for SqlIdentifier {
    fn eq(&self, other: &String) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

// Reverse: str == SqlIdentifier
impl PartialEq<SqlIdentifier> for str {
    fn eq(&self, other: &SqlIdentifier) -> bool {
        self.eq_ignore_ascii_case(&other.0)
    }
}

impl PartialEq<SqlIdentifier> for &str {
    fn eq(&self, other: &SqlIdentifier) -> bool {
        self.eq_ignore_ascii_case(&other.0)
    }
}

impl PartialEq<SqlIdentifier> for String {
    fn eq(&self, other: &SqlIdentifier) -> bool {
        self.eq_ignore_ascii_case(&other.0)
    }
}

// Display preserves original case
impl fmt::Display for SqlIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// Deref to str for seamless read access
impl Deref for SqlIdentifier {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SqlIdentifier {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for SqlIdentifier {
    fn borrow(&self) -> &str {
        &self.0
    }
}

// From conversions
impl From<String> for SqlIdentifier {
    fn from(s: String) -> Self {
        SqlIdentifier(s)
    }
}

impl From<&str> for SqlIdentifier {
    fn from(s: &str) -> Self {
        SqlIdentifier(s.to_string())
    }
}

impl From<SqlIdentifier> for String {
    fn from(id: SqlIdentifier) -> Self {
        id.0
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

    // Serde roundtrip tested at higher crate levels where serde_json is available.
}
