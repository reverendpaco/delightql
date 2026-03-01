//! Core entity model for DelightQL resolution
//!
//! These types represent what things exist in DelightQL's world.
//! They are NOT AST nodes, but domain concepts that the AST references.

use crate::pipeline::ast_resolved::{CprSchema, NamespacePath};
use delightql_types::SqlIdentifier;

/// Information about an entity discovered by the resolver
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct EntityInfo {
    pub name: SqlIdentifier,
    /// Canonical entity name from bootstrap (what the DB stores).
    /// None for CTEs, built-ins, and entities where bootstrap doesn't provide a canonical name.
    pub canonical_name: Option<SqlIdentifier>,
    /// The namespace where this entity was found during resolution.
    /// For engaged tables resolved as unqualified names, this carries the
    /// discovered namespace so the transformer can emit schema-qualified SQL.
    pub resolved_namespace: Option<NamespacePath>,
    pub entity_type: EntityType,
    pub registry_source: RegistrySource,
    pub schema_source: SchemaSource,
    pub definition: EntityDefinition,
}

/// What kind of thing the entity is
#[derive(Debug, Clone, PartialEq)]
pub enum EntityType {
    /// Tables, views, CTEs, TVFs - has rows and columns
    Relation,
}

/// Where we discovered that an entity exists
#[derive(Debug, Clone, PartialEq)]
pub enum RegistrySource {
    /// Found in database catalog
    Database,
    /// Defined in current query (CTEs)
    QueryLocal,
}

/// Where we get the schema/structure for a specific entity
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaSource {
    /// Schema from information_schema, pg_proc, etc.
    DatabaseCatalog,
    /// Schema from CTE's SELECT clause
    SelectClause,
}

/// The actual definition/schema of an entity
#[derive(Debug, Clone)]
pub enum EntityDefinition {
    /// For relations (tables, CTEs, etc.)
    RelationSchema(CprSchema),
}
