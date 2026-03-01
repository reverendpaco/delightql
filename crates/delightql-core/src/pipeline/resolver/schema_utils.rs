//! Schema utility functions
//!
//! This module contains utilities for validating and working with column schemas
//! during resolution.

use super::unification::{unify_columns, ColumnReference};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;

/// Validate that column references can be resolved and return the resolved columns
pub(super) fn validate_and_get_resolved(
    references: Vec<ColumnReference>,
    available_columns: &[ast_resolved::ColumnMetadata],
    error_context: &str,
) -> Result<Vec<ast_resolved::ColumnMetadata>> {
    if references.is_empty() {
        return Ok(Vec::new());
    }

    let mut resolved_columns = Vec::new();
    let results = unify_columns(references, available_columns);

    for result in results {
        match result {
            super::unification::UnificationResult::Resolved(col) => {
                resolved_columns.push(col);
            }
            super::unification::UnificationResult::Unresolved(name) => {
                return Err(DelightQLError::column_not_found_error(
                    name,
                    error_context,
                ));
            }
            super::unification::UnificationResult::Ambiguous { column, tables } => {
                return Err(DelightQLError::ValidationError {
                    message: format!(
                        "Column '{}' {} is ambiguous. Could refer to: {}",
                        column,
                        error_context,
                        tables.join(", ")
                    ),
                    context: error_context.to_string(),
                    subcategory: None,
                });
            }
        }
    }
    Ok(resolved_columns)
}
