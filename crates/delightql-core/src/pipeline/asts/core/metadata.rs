//! Metadata structures for resolved and refined phases

use super::ColumnProvenance;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;
use serde::{Deserialize, Serialize};
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
/// ```
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamespacePath {
    // Private: enforce invariants via constructors
    // SmallVec[2]: inline storage for 0-2 items (no allocation)
    items: SmallVec<[NamespaceItem; 2]>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        NamespacePath { items: smallvec![] }
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
            items: smallvec![NamespaceItem { name: SqlIdentifier::new(name) }],
        }
    }

    /// Multi-level path from parts (innermost → outermost)
    ///
    /// # Examples
    ///
    /// ```
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
                .map(|name| NamespaceItem { name: SqlIdentifier::new(name) })
                .collect(),
        })
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
    /// ```
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
    /// Phase 2: Core's rich NamespacePath needs to convert to the simplified
    /// types version when calling DatabaseSchema methods.
    pub fn to_types_namespace_path(&self) -> delightql_types::namespace::NamespacePath {
        let parts: Vec<String> = self.items.iter().map(|item| item.name.to_string()).collect();
        delightql_types::namespace::NamespacePath::from_parts(parts)
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
        NamespacePath { items }
    }
}

impl std::fmt::Display for NamespacePath {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.is_empty() {
            write!(f, "(empty)")
        } else {
            let parts: Vec<_> = self.iter_reversed().map(|i| i.name.as_str()).collect();
            write!(f, "{}", parts.join("."))
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

/// Grounded namespace path: data_ns^lib_ns for binding data to groundable definitions
///
/// In `data::test^lib::math.users(*)`:
/// - `data_ns` = NamespacePath for "data::test" (where tables live)
/// - `grounded_ns` = vec of NamespacePaths for libraries being grounded (e.g., "lib::math")
///
/// Multiple groundings are supported: `data::test^lib::math^lib::extra`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroundedPath {
    /// The data namespace (provides concrete tables)
    pub data_ns: NamespacePath,
    /// The grounded namespaces (provide definitions with unresolved table references)
    pub grounded_ns: Vec<NamespacePath>,
}

impl ToLispy for GroundedPath {
    fn to_lispy(&self) -> String {
        let ns_parts: Vec<String> = self.grounded_ns.iter().map(|ns| ns.to_lispy()).collect();
        format!(
            "(grounding :data {} :libs [{}])",
            self.data_ns.to_lispy(),
            ns_parts.join(" ")
        )
    }
}

// ============================================================================
// Metadata Structures (from resolver phase onward)
// ============================================================================

/// Table name - either named or anonymous (fresh)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum TableName {
    Named(SqlIdentifier),
    #[lispy("fresh")]
    Fresh,
}

/// Fully qualified table reference
///
/// For table identity (not aliases): namespace_path identifies location, name is the table
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
#[lispy("fq_table")]
pub struct FqTable {
    /// Namespace path (WHERE to find table) - logical namespace
    /// This is what the user wrote (e.g., "c", "sys", "main")
    pub parents_path: NamespacePath,

    /// Table name
    pub name: TableName,

    /// Backend database schema name - physical namespace
    /// This is what SQL generation needs (e.g., "_c", "sys", "main")
    /// Only available after resolution phase - populated by querying sys.namespaces
    pub backend_schema: super::PhaseBox<Option<String>, super::Resolved>,
}

/// Column metadata with schema information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
#[lispy("c")]
pub struct ColumnMetadata {
    /// Core column identity and reference information
    pub info: ColumnProvenance,
    /// Source table information
    pub fq_table: FqTable,
    /// Position in the output
    pub table_position: Option<usize>,
    /// Whether this column has a user-provided name (vs generated)
    pub has_user_name: bool,
    /// Whether this column needs hygienic aliasing (for literal/expression constraints)
    /// When true, the transformer should use __dql_literal_N alias and hide from output
    #[serde(skip_serializing_if = "is_false", default)]
    pub needs_hygienic_alias: bool,
    /// Whether this column was renamed by a call-site positional pattern (e.g. employee(eid, ename, dept))
    /// and needs an explicit SELECT wrapper in the transformer. Distinguished from body-internal
    /// renames which are already baked into the body SQL.
    #[serde(skip_serializing_if = "is_false", default)]
    pub needs_sql_rename: bool,
    /// Interior relation schema for tree group columns.
    /// When this column holds a JSON array produced by `~> {}`, this field
    /// describes the columns of the interior relation for drill-down support.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub interior_schema: Option<Vec<crate::pipeline::asts::core::operators::InteriorColumnDef>>,
}

// Helper for serde skip_serializing_if
fn is_false(b: &bool) -> bool {
    !b
}

impl ColumnMetadata {
    /// Create new metadata from components
    pub fn new(info: ColumnProvenance, fq_table: FqTable, table_position: Option<usize>) -> Self {
        Self {
            info,
            fq_table,
            table_position,
            has_user_name: true, // Default to true for backward compatibility
            needs_hygienic_alias: false,
            needs_sql_rename: false,
            interior_schema: None,
        }
    }

    /// Create new metadata with explicit user name flag
    pub fn new_with_name_flag(
        info: ColumnProvenance,
        fq_table: FqTable,
        table_position: Option<usize>,
        has_user_name: bool,
    ) -> Self {
        Self {
            info,
            fq_table,
            table_position,
            has_user_name,
            needs_hygienic_alias: false,
            needs_sql_rename: false,
            interior_schema: None,
        }
    }

    pub fn from_existing(other: &ColumnMetadata) -> Self {
        other.clone()
    }

    pub fn has_alias(&self) -> bool {
        self.info.has_alias()
    }

    pub fn was_qualified(&self) -> Option<bool> {
        self.info.is_qualified()
    }

    pub fn name(&self) -> &str {
        self.info.name().unwrap_or("<unnamed>")
    }

    pub fn original_name(&self) -> &str {
        self.info.original_name().unwrap_or("<unnamed>")
    }

    pub fn set_alias(&mut self, alias: String) {
        self.info = self.info.clone().with_alias(alias);
    }
}

/// A CprSchema bound to its SQL alias. Private fields enforce that
/// the alias is reflected in the schema's provenance stacks via SubqueryAlias.
///
/// Invariant: `schema` always has SubqueryAlias pushed for `alias`.
/// Use `bind()` to construct (pushes provenance), or `from_parts()` when
/// the schema is already scoped (e.g., rebuilder after flatten/rebuild cycle).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScopedSchema {
    alias: SqlIdentifier,
    schema: CprSchema,
}

impl ScopedSchema {
    /// Construct a ScopedSchema, pushing SubqueryAlias onto every column.
    /// This is the primary constructor — enforces the alias-schema invariant.
    pub fn bind(schema: CprSchema, alias: SqlIdentifier) -> Self {
        let scoped = crate::pipeline::resolver::helpers::extraction::scope_schema_to_alias(
            schema,
            &alias,
        );
        ScopedSchema { alias, schema: scoped }
    }

    /// Reconstruct from already-scoped parts (for rebuilder after flatten/rebuild).
    /// The caller guarantees that `schema` already has the alias in its provenance.
    pub fn from_parts(alias: SqlIdentifier, schema: CprSchema) -> Self {
        ScopedSchema { alias, schema }
    }

    pub fn alias(&self) -> &SqlIdentifier {
        &self.alias
    }

    pub fn schema(&self) -> &CprSchema {
        &self.schema
    }
}

impl ToLispy for ScopedSchema {
    fn to_lispy(&self) -> String {
        format!("(scoped-schema :alias {} {})", self.alias, self.schema.to_lispy())
    }
}

/// Schema information for Current Piped Relation
/// Used in resolved and refined phases to track column resolution state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CprSchema {
    /// Successfully resolved with available columns
    Resolved(Vec<ColumnMetadata>),
    /// Failed to resolve some columns
    Failed {
        resolved_columns: Vec<ColumnMetadata>,
        unresolved_columns: Vec<ColumnMetadata>,
    },
    /// Needs resolution (bubbled up from operators)
    Unresolved(Vec<ColumnMetadata>),
    /// Unknown schema - for passthrough TVFs and external functions
    Unknown,
}

impl ToLispy for CprSchema {
    fn to_lispy(&self) -> String {
        match self {
            CprSchema::Resolved(cols) => {
                let col_list = cols
                    .iter()
                    .map(|c| c.to_lispy())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("(cpr_schema:resolved [{}])", col_list)
            }
            CprSchema::Failed {
                resolved_columns,
                unresolved_columns,
                ..
            } => {
                let resolved_list = resolved_columns
                    .iter()
                    .map(|c| c.to_lispy())
                    .collect::<Vec<_>>()
                    .join(" ");
                let unresolved_list = unresolved_columns
                    .iter()
                    .map(|c| c.to_lispy())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!(
                    "(cpr_schema:failed :resolved [{}] :unresolved [{}])",
                    resolved_list, unresolved_list
                )
            }
            CprSchema::Unresolved(cols) => {
                let col_list = cols
                    .iter()
                    .map(|c| c.to_lispy())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("(cpr_schema:unresolved [{}])", col_list)
            }
            CprSchema::Unknown => "(cpr_schema:unknown)".to_string(),
        }
    }
}

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
