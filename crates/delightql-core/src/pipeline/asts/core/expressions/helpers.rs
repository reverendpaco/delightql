// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Helper types used across expressions
//! QualifiedName

use super::super::metadata::NamespacePath;
use crate::{lispy::ToLispy, ToLispy};
use delightql_types::SqlIdentifier;

/// Identifier for tables, columns, etc with namespace path
///
/// Used for table references in FROM clauses, subqueries, etc.
/// Unlike Lvar (which has separate qualifier for aliases), this represents
/// the actual table identity.
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("identifier")]
pub struct QualifiedName {
    /// Namespace path (WHERE to find table: schema, database, catalog)
    pub namespace_path: NamespacePath,
    /// Table name itself
    pub name: SqlIdentifier,
}
