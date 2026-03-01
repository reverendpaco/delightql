/// Schema context for expression transformation.
///
/// Provides access to column metadata with identity stacks during SQL generation.
/// This allows the transformer to query CTE contexts, pipe barriers, and other
/// column identity information that was tracked during resolution.
use crate::pipeline::asts::addressed::{ColumnMetadata, CprSchema};
use delightql_types::SqlIdentifier;
use std::collections::HashMap;

/// Context containing schema information for expression transformation.
///
/// Lazily builds an index on first lookup for efficient column metadata access.
/// Uses SqlIdentifier keys for case-insensitive column name lookup.
pub struct SchemaContext {
    /// The full schema with ColumnMetadata and identity stacks
    schema: CprSchema,

    /// Lazy index: column name -> index in schema vector
    /// Built on first call to lookup_column()
    /// Uses SqlIdentifier for case-insensitive lookup.
    column_index: Option<HashMap<SqlIdentifier, usize>>,
}

impl SchemaContext {
    /// Create a new SchemaContext from a CprSchema
    pub fn new(schema: CprSchema) -> Self {
        Self {
            schema,
            column_index: None,
        }
    }

    /// Create an empty/unknown SchemaContext (for cases where schema isn't available)
    ///
    /// This is a temporary measure during the migration. Column lookups will return None.
    pub fn unknown() -> Self {
        Self {
            schema: CprSchema::Unknown,
            column_index: None,
        }
    }

    /// Find ColumnMetadata by column name (case-insensitive).
    ///
    /// Builds an index on the first call for O(1) subsequent lookups.
    /// Returns None if:
    /// - The schema is not Resolved
    /// - The column name doesn't exist in the schema
    pub fn lookup_column(&mut self, name: &str) -> Option<&ColumnMetadata> {
        // Lazy index building
        if self.column_index.is_none() {
            if let CprSchema::Resolved(cols) = &self.schema {
                let mut index = HashMap::new();
                for (i, col) in cols.iter().enumerate() {
                    if let Some(col_name) = col.info.name() {
                        index.insert(SqlIdentifier::from(col_name), i);
                    }
                }
                self.column_index = Some(index);
            }
        }

        // Lookup (case-insensitive via SqlIdentifier)
        if let Some(ref index) = self.column_index {
            let key = SqlIdentifier::from(name);
            if let Some(&idx) = index.get(&key) {
                if let CprSchema::Resolved(cols) = &self.schema {
                    return cols.get(idx);
                }
            }
        }
        None
    }

    /// Get the number of columns in the schema (for debugging)
    pub fn column_count(&self) -> usize {
        match &self.schema {
            CprSchema::Resolved(cols) => cols.len(),
            other => panic!(
                "catch-all hit in schema_context.rs column_count: {:?}",
                other
            ),
        }
    }
}
