// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Metadata structures for resolved and refined phases

use crate::lispy::ToLispy;
use delightql_types::SqlIdentifier;
use smallvec::{smallvec, SmallVec};

// ============================================================================
// Namespace Path Infrastructure
// ============================================================================

/// Variable-length namespace path for multi-level hierarchies
///
/// Items ordered innermost → outermost (schema, database, server, catalog, etc.)
///
/// # Design Rationale
///
/// - **SmallVec optimization:** 90%+ of paths are 0-2 items (empty, or schema.table)
///   - No heap allocation for common case
///   - Automatic fallback to heap for deeper paths (rare)
///
/// - **Private fields:** Enforces invariants via constructors
///   - No empty identifiers allowed
///   - No direct Vec manipulation
///
/// - **Backend-agnostic:** Core AST doesn't interpret meaning of levels
///   - SQLite: items[0] = database (max 2 levels)
///   - SQL Server: items[0] = schema, items[1] = database, items[2] = server (max 4)
///   - PostgreSQL: items[0] = schema, items[1] = database (max 3)
///
/// # Examples
///
/// ```ignore
/// use delightql_core::pipeline::asts::core::metadata::NamespacePath;
///
/// // Empty path (unqualified reference)
/// let path = NamespacePath::empty();
///
/// // Single-level (schema only)
/// let path = NamespacePath::single("public");
///
/// // Multi-level (catalog.schema.table → ["schema", "catalog"])
/// let path = NamespacePath::from_parts(vec!["schema".into(), "catalog".into()]).unwrap();
///
/// // Display for errors
/// println!("Table not found: {}", path.with_table("users"));  // "catalog.schema.users"
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct NamespacePath {
    // Private: enforce invariants via constructors
    // SmallVec[2]: inline storage for 0-2 items (no allocation)
    items: SmallVec<[NamespaceItem; 2]>,
    /// How the path reaches its namespace: an exact path, or the
    /// self-relative child route `.::child` whose base is the primary
    /// definition context of the world the mention stands in. Only a
    /// world resolves the relative route; an exact spelling is answered
    /// by `qualifier().exact_fq()` alone.
    route: crate::pipeline::asts::vocabulary::QualifierRoute,
}

impl Default for NamespacePath {
    fn default() -> Self {
        NamespacePath::empty()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NamespaceItem {
    pub name: SqlIdentifier,
    // Future fields for late binding:
    // pub backend_hint: Option<NamespaceKind>,
    // pub link_info: Option<DatabaseLink>,
}

/// Errors that can occur when constructing or using namespace paths
#[derive(Debug, Clone, PartialEq)]
pub enum NamespaceError {
    /// Empty identifier in path (e.g., "schema..table")
    EmptyIdentifier,

    /// Path exceeds reasonable depth (suggests bug in parser)
    PathTooDeep { depth: usize, max: usize },
}

impl std::fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NamespaceError::EmptyIdentifier => {
                write!(f, "Namespace path cannot contain empty identifiers")
            }
            NamespaceError::PathTooDeep { depth, max } => {
                write!(f, "Namespace path too deep: {} levels (max {})", depth, max)
            }
        }
    }
}

impl std::error::Error for NamespaceError {}

impl NamespacePath {
    /// Maximum reasonable depth (sanity check during construction)
    /// SQL Server supports 4 levels, so 10 is conservative upper bound
    const MAX_REASONABLE_DEPTH: usize = 10;

    /// Empty path (unqualified reference)
    ///
    /// Examples: `users(*)`, `id`, `count:(*)`
    pub fn empty() -> Self {
        NamespacePath {
            items: smallvec![],
            route: crate::pipeline::asts::vocabulary::QualifierRoute::Exact,
        }
    }

    /// The written qualifier this path is: its route and its segments in
    /// path order. Only a lexical world turns it into a namespace; an
    /// exact route's `::`-joined spelling is `Qualifier::exact_fq`.
    pub fn qualifier(&self) -> crate::pipeline::asts::vocabulary::Qualifier {
        let segments: Vec<String> = self.items.iter().map(|i| i.name.to_string()).collect();
        match self.route {
            crate::pipeline::asts::vocabulary::QualifierRoute::Exact => {
                crate::pipeline::asts::vocabulary::Qualifier::exact(segments)
            }
            crate::pipeline::asts::vocabulary::QualifierRoute::SelfRelative => {
                crate::pipeline::asts::vocabulary::Qualifier::self_relative(segments)
            }
        }
    }

    /// The path a written qualifier is, route and all.
    pub fn from_qualifier(
        qualifier: &crate::pipeline::asts::vocabulary::Qualifier,
    ) -> Result<Self, NamespaceError> {
        let parts = qualifier.segments().to_vec();
        match qualifier.route() {
            crate::pipeline::asts::vocabulary::QualifierRoute::Exact => Self::from_parts(parts),
            crate::pipeline::asts::vocabulary::QualifierRoute::SelfRelative => {
                Self::self_relative(parts)
            }
        }
    }

    /// The self-relative child route `.::child`: the same segments, based
    /// at the primary definition context of whichever world reads it.
    pub fn self_relative(parts: Vec<String>) -> Result<Self, NamespaceError> {
        let mut path = Self::from_parts(parts)?;
        path.route = crate::pipeline::asts::vocabulary::QualifierRoute::SelfRelative;
        Ok(path)
    }

    /// Single-level path (e.g., just schema or just database)
    ///
    /// # Panics
    /// Panics if name is empty (debug builds only)
    pub fn single(name: impl Into<String>) -> Self {
        let name = name.into();
        debug_assert!(
            !name.is_empty(),
            "NamespacePath cannot contain empty identifier"
        );

        NamespacePath {
            items: smallvec![NamespaceItem {
                name: SqlIdentifier::new(name)
            }],
            route: crate::pipeline::asts::vocabulary::QualifierRoute::Exact,
        }
    }

    /// Multi-level path from parts (innermost → outermost)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use delightql_core::pipeline::asts::core::metadata::NamespacePath;
    ///
    /// // For "catalog.schema.table" in column ref "catalog.schema.table.column"
    /// let path = NamespacePath::from_parts(vec!["schema".into(), "catalog".into()]).unwrap();
    /// assert_eq!(path.depth(), 2);
    /// assert_eq!(path.first(), Some("schema"));
    /// assert_eq!(path.last(), Some("catalog"));
    /// ```
    ///
    /// # Errors
    ///
    /// - `EmptyIdentifier`: Any part is an empty string
    /// - `PathTooDeep`: Path exceeds MAX_REASONABLE_DEPTH (10 levels)
    pub fn from_parts(parts: Vec<String>) -> Result<Self, NamespaceError> {
        if parts.is_empty() {
            return Ok(Self::empty());
        }

        // Validate: no empty strings
        for part in &parts {
            if part.is_empty() {
                return Err(NamespaceError::EmptyIdentifier);
            }
        }

        // Validate: reasonable depth
        if parts.len() > Self::MAX_REASONABLE_DEPTH {
            return Err(NamespaceError::PathTooDeep {
                depth: parts.len(),
                max: Self::MAX_REASONABLE_DEPTH,
            });
        }

        Ok(NamespacePath {
            items: parts
                .into_iter()
                .map(|name| NamespaceItem {
                    name: SqlIdentifier::new(name),
                })
                .collect(),
            route: crate::pipeline::asts::vocabulary::QualifierRoute::Exact,
        })
    }

    /// Rehydrate a persisted fully-qualified namespace through the same
    /// validated path authority used by authored paths. Database metadata is
    /// the only caller of this spelling-level boundary; compiler consumers
    /// receive a typed path afterward.
    pub fn from_fq_string(fq: &str) -> Result<Self, NamespaceError> {
        Self::from_parts(fq.split("::").map(str::to_owned).collect())
    }

    /// Get items as slice (read-only access)
    pub fn items(&self) -> &[NamespaceItem] {
        &self.items
    }

    /// Get first item (innermost level - typically schema)
    pub fn first(&self) -> Option<&str> {
        self.items.first().map(|i| i.name.as_str())
    }

    /// Get last item (outermost level - typically catalog/server)
    pub fn last(&self) -> Option<&str> {
        self.items.last().map(|i| i.name.as_str())
    }

    /// Check if path is empty (unqualified reference)
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get depth (number of namespace levels)
    pub fn depth(&self) -> usize {
        self.items.len()
    }

    /// Iterator from innermost → outermost
    pub fn iter(&self) -> impl Iterator<Item = &NamespaceItem> {
        self.items.iter()
    }

    /// Iterator from outermost → innermost (for display)
    pub fn iter_reversed(&self) -> impl Iterator<Item = &NamespaceItem> {
        self.items.iter().rev()
    }

    /// Combine namespace path with table name for display
    ///
    /// Output format: outermost.inner.table (standard SQL order)
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use delightql_core::pipeline::asts::core::metadata::NamespacePath;
    ///
    /// let path = NamespacePath::from_parts(vec!["dbo".into(), "prod".into()]).unwrap();
    /// assert_eq!(path.with_table("users"), "prod.dbo.users");
    ///
    /// let empty = NamespacePath::empty();
    /// assert_eq!(empty.with_table("users"), "users");
    /// ```
    pub fn with_table(&self, table_name: &str) -> String {
        if self.is_empty() {
            table_name.to_string()
        } else {
            let mut parts: Vec<_> = self.iter_reversed().map(|i| i.name.as_str()).collect();
            parts.push(table_name);
            parts.join(".")
        }
    }

    /// Convert to delightql_types::NamespacePath for use with DatabaseSchema trait
    ///
    /// Core's rich NamespacePath needs to convert to the simplified
    /// types version when calling DatabaseSchema methods.
    ///
    /// Only an EXACT path names a backend namespace; a self-relative route
    /// answers `None`, because no schema provider stands in a primary
    /// definition context — the world resolves it first.
    pub fn to_types_namespace_path(&self) -> Option<delightql_types::namespace::NamespacePath> {
        match self.route {
            crate::pipeline::asts::vocabulary::QualifierRoute::SelfRelative => None,
            crate::pipeline::asts::vocabulary::QualifierRoute::Exact => {
                let parts: Vec<String> = self
                    .items
                    .iter()
                    .map(|item| item.name.to_string())
                    .collect();
                Some(delightql_types::namespace::NamespacePath::from_parts(parts))
            }
        }
    }

    /// Create from delightql_types::NamespacePath
    ///
    /// Converts the simplified types version to core's rich NamespacePath.
    pub fn from_types_namespace_path(
        types_path: &delightql_types::namespace::NamespacePath,
    ) -> Self {
        let items: SmallVec<[NamespaceItem; 2]> = types_path
            .items()
            .iter()
            .map(|item| NamespaceItem {
                name: SqlIdentifier::new(item.name.as_str()),
            })
            .collect();
        NamespacePath {
            items,
            route: crate::pipeline::asts::vocabulary::QualifierRoute::Exact,
        }
    }
}

impl std::fmt::Display for NamespacePath {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "(empty)")
        } else {
            let parts: Vec<_> = self.iter_reversed().map(|i| i.name.as_str()).collect();
            match self.route {
                crate::pipeline::asts::vocabulary::QualifierRoute::Exact => {
                    write!(f, "{}", parts.join("."))
                }
                crate::pipeline::asts::vocabulary::QualifierRoute::SelfRelative => {
                    write!(f, ".::{}", parts.join("."))
                }
            }
        }
    }
}

impl ToLispy for NamespacePath {
    fn to_lispy(&self) -> String {
        if self.items.is_empty() {
            "()".to_string()
        } else {
            let parts: Vec<_> = self.items.iter().map(|item| item.name.as_str()).collect();
            format!("({})", parts.join("."))
        }
    }
}

impl ToLispy for NamespaceItem {
    fn to_lispy(&self) -> String {
        self.name.to_string()
    }
}

// ============================================================================
// Grounding Infrastructure
// ============================================================================

// ============================================================================
// Metadata Structures (from resolver phase onward)
// ============================================================================

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_path_empty() {
        let path = NamespacePath::empty();
        assert!(path.is_empty());
        assert_eq!(path.depth(), 0);
        assert_eq!(path.first(), None);
        assert_eq!(path.last(), None);
        assert_eq!(path.to_string(), "(empty)");
        assert_eq!(path.to_lispy(), "()");
    }

    #[test]
    fn test_namespace_path_single_level() {
        let path = NamespacePath::single("public");
        assert!(!path.is_empty());
        assert_eq!(path.depth(), 1);
        assert_eq!(path.first(), Some("public"));
        assert_eq!(path.last(), Some("public"));
        assert_eq!(path.to_string(), "public");
        assert_eq!(path.to_lispy(), "(public)");
    }

    #[test]
    fn test_namespace_path_multi_level() {
        let path = NamespacePath::from_parts(vec![
            "dbo".into(),
            "AdventureWorks".into(),
            "SQLSERVER01".into(),
        ])
        .unwrap();

        assert_eq!(path.depth(), 3);
        assert_eq!(path.first(), Some("dbo")); // innermost
        assert_eq!(path.last(), Some("SQLSERVER01")); // outermost
        assert_eq!(path.to_string(), "SQLSERVER01.AdventureWorks.dbo");
        assert_eq!(path.to_lispy(), "(dbo.AdventureWorks.SQLSERVER01)");
    }

    #[test]
    fn test_namespace_path_with_table() {
        let path = NamespacePath::from_parts(vec!["dbo".into(), "prod".into()]).unwrap();
        assert_eq!(path.with_table("users"), "prod.dbo.users");

        let empty = NamespacePath::empty();
        assert_eq!(empty.with_table("users"), "users");
    }

    #[test]
    fn test_namespace_path_iteration() {
        let path = NamespacePath::from_parts(vec!["a".into(), "b".into(), "c".into()]).unwrap();

        // Forward: innermost → outermost
        let forward: Vec<_> = path.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(forward, vec!["a", "b", "c"]);

        // Reverse: outermost → innermost (for display)
        let reverse: Vec<_> = path.iter_reversed().map(|i| i.name.as_str()).collect();
        assert_eq!(reverse, vec!["c", "b", "a"]);
    }

    #[test]
    fn test_namespace_path_empty_identifier_rejected() {
        let result = NamespacePath::from_parts(vec!["schema".into(), "".into()]);
        assert!(matches!(result, Err(NamespaceError::EmptyIdentifier)));
    }

    #[test]
    fn test_namespace_path_too_deep_rejected() {
        let parts: Vec<_> = (0..20).map(|i| format!("level{}", i)).collect();
        let result = NamespacePath::from_parts(parts);
        assert!(matches!(
            result,
            Err(NamespaceError::PathTooDeep { depth: 20, max: 10 })
        ));
    }

    #[test]
    #[should_panic]
    #[cfg(debug_assertions)]
    fn test_namespace_path_single_empty_panics_debug() {
        let _ = NamespacePath::single("");
    }

    #[test]
    fn test_namespace_path_items_access() {
        let path = NamespacePath::from_parts(vec!["a".into(), "b".into()]).unwrap();
        let items = path.items();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].name, "a");
        assert_eq!(items[1].name, "b");
    }
}
