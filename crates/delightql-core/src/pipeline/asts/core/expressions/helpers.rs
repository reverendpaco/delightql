//! Helper types used across expressions
//! QualifiedName, UsingColumn

use super::super::metadata::{GroundedPath, NamespacePath};
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;
use serde::{Deserialize, Serialize};

/// Identifier for tables, columns, etc with namespace path
///
/// Used for table references in FROM clauses, subqueries, etc.
/// Unlike Lvar (which has separate qualifier for aliases), this represents
/// the actual table identity.
///
/// Grounding: When `grounding` is Some, this identifier was written with the `^` operator.
/// For example, `data::test^lib::math.users(*)` has grounding that binds data namespace
/// to a library namespace with unresolved table references.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
#[lispy("identifier")]
pub struct QualifiedName {
    /// Namespace path (WHERE to find table: schema, database, catalog)
    pub namespace_path: NamespacePath,
    /// Table name itself
    pub name: SqlIdentifier,
    /// Grounding context: binds data namespace to groundable library namespaces
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub grounding: Option<GroundedPath>,
}

/// Column in USING clause
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToLispy)]
pub enum UsingColumn {
    /// Regular column
    #[lispy("identifier:using")]
    Regular(QualifiedName),
    /// Negated column (!column)
    #[lispy("identifier:using:negated")]
    Negated(QualifiedName),
}
