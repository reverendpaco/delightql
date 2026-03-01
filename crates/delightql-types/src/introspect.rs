//! Database Introspection Types
//!
//! This module provides the core introspection trait and types used by both:
//! - `delightql-core` (for runtime/bootstrap database introspection)
//! - `delightql-backends` (for user-facing target database introspection)
//!
//! These are intentionally kept in `delightql-types` to avoid circular dependencies
//! between core and backends.

use crate::error::Result;
use crate::identifier::SqlIdentifier;

/// Backend-agnostic database introspection trait
///
/// This trait allows different database backends (SQLite, DuckDB, Postgres) to provide
/// their own introspection implementations while returning a common format.
///
/// Each backend implements this trait to query its own system catalogs:
/// - SQLite: queries `sqlite_master` and `PRAGMA table_info`
/// - DuckDB: queries `information_schema.tables` and `information_schema.columns`
/// - Postgres: queries `pg_catalog` and `information_schema`
///
/// The discovered entities can be stored in metadata tables regardless
/// of the user database backend.
pub trait DatabaseIntrospector: Send {
    /// Introspect the database and return discovered entities
    ///
    /// # Returns
    /// * `Ok(Vec<DiscoveredEntity>)` - List of discovered tables/views with columns
    /// * `Err(DelightQLError)` - If introspection fails
    fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>>;

    /// Introspect a specific schema/database and return discovered entities
    ///
    /// This is used for introspecting attached databases (e.g., after ATTACH DATABASE in SQLite).
    /// For databases that don't support schemas, this should behave the same as introspect_entities().
    ///
    /// # Arguments
    /// * `schema` - Schema/database name to introspect
    ///
    /// # Returns
    /// * `Ok(Vec<DiscoveredEntity>)` - List of discovered tables/views with columns in that schema
    /// * `Err(DelightQLError)` - If introspection fails
    fn introspect_entities_in_schema(&self, schema: &str) -> Result<Vec<DiscoveredEntity>>;
}

/// Discovered entity from database introspection
///
/// Represents a table or view discovered by querying database metadata.
#[derive(Debug, Clone)]
pub struct DiscoveredEntity {
    /// Entity name (table or view name)
    pub name: SqlIdentifier,
    /// Entity type ID from entity_type_enum
    /// - 10 = DBPermanentTable
    /// - 11 = DBPermanentView
    /// - 12 = DBTemporaryTable
    /// - 13 = DBTemporaryView
    pub entity_type_id: i32,
    /// Columns/attributes discovered for this entity
    pub attributes: Vec<DiscoveredAttribute>,
}

/// Discovered column/attribute from database introspection
///
/// Represents a column in a table or view.
#[derive(Debug, Clone)]
pub struct DiscoveredAttribute {
    /// Column name
    pub name: SqlIdentifier,
    /// SQL data type (e.g., "INTEGER", "TEXT", "BOOLEAN")
    pub data_type: String,
    /// Column position (0-indexed)
    pub position: i32,
    /// Whether column accepts NULL values
    pub is_nullable: bool,
}
