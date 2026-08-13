// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
/// Database Schema Abstraction
///
/// This module provides the core database schema trait and types needed by
/// both delightql-core (for resolution) and delightql-backends (for implementations).

use crate::identifier::SqlIdentifier;
use crate::error::Result;

/// Information about a database column (simple version for trait interface)
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: SqlIdentifier,
    pub nullable: bool,
    pub position: usize,
    /// The column's DECLARED type, verbatim from the catalog (sqlite
    /// decltype / entity_attribute.data_type). None when the source has no
    /// declaration (expression columns, typeless historical tables).
    /// Declarations lie about sqlite STORAGE (dynamic typing) — this is
    /// intent metadata, used where any consistent type beats none (e.g.
    /// typing corresponding-union NULL pads for strict targets).
    pub declared_type: Option<String>,
}

/// Core database schema trait used by the resolver
///
/// This trait must be implemented by database backends to provide
/// schema information to the resolution phase.
///
/// Note: Namespace resolution is NOT part of this trait - it's handled
/// internally by DelightQLSystem using the engine's bootstrap metadata.
pub trait DatabaseSchema: Send + Sync {
    /// Get columns for a table, querying the database dynamically
    /// This should handle:
    /// - Regular tables (None, "users")
    /// - Schema-qualified tables (Some("temp"), "sqlite_master")
    /// - Attached databases (Some("nba"), "player")
    fn get_table_columns(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<Vec<ColumnInfo>>>;

    /// Check if a table exists in the given schema
    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> Result<bool>;
}
