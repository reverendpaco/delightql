/// Minimal namespace path type for delightql-types
///
/// This is a simplified version without dependencies on core's lispy traits.
/// The full version with ToLispy support lives in delightql-core.

use crate::identifier::SqlIdentifier;
use serde::{Deserialize, Serialize};

/// Simple namespace item (just a name)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespaceItem {
    pub name: SqlIdentifier,
}

/// Simplified namespace path for type definitions
///
/// This uses Vec instead of SmallVec to avoid extra dependencies.
/// The core crate has a more optimized version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespacePath {
    items: Vec<NamespaceItem>,
}

impl NamespacePath {
    /// Empty path (unqualified reference)
    pub fn empty() -> Self {
        NamespacePath { items: vec![] }
    }

    /// Single-level path
    pub fn single(name: impl Into<String>) -> Self {
        NamespacePath {
            items: vec![NamespaceItem { name: SqlIdentifier::new(name) }],
        }
    }

    /// Multi-level path from parts (innermost → outermost)
    pub fn from_parts(parts: Vec<String>) -> Self {
        NamespacePath {
            items: parts.into_iter().map(|name| NamespaceItem { name: SqlIdentifier::new(name) }).collect(),
        }
    }

    /// Get items as slice
    pub fn items(&self) -> &[NamespaceItem] {
        &self.items
    }

    /// Check if path is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get depth
    pub fn depth(&self) -> usize {
        self.items.len()
    }

    /// Iterator from innermost → outermost
    pub fn iter(&self) -> impl Iterator<Item = &NamespaceItem> {
        self.items.iter()
    }

    /// Iterator from outermost → innermost
    pub fn iter_reversed(&self) -> impl Iterator<Item = &NamespaceItem> {
        self.items.iter().rev()
    }
}
