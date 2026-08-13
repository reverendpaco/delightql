// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Schema utility functions
//!
//! This module contains utilities for validating and working with column schemas
//! during resolution.

use super::unification::{unify_columns, ColumnReference};
use crate::error::{DelightQLError, Result};

/// Validate that column references can be resolved and return the resolved columns
pub(super) fn validate_and_get_resolved(
    references: Vec<ColumnReference>,
    available_columns: &[crate::names::ColId],
    visible: &[crate::names::ScopeId],
    registry: &crate::names::Registry,
    error_context: &str,
) -> Result<Vec<crate::names::ColId>> {
    if references.is_empty() {
        return Ok(Vec::new());
    }

    let mut resolved_columns = Vec::new();
    let results = unify_columns(references, available_columns, visible, registry);

    for result in results {
        match result {
            super::unification::UnificationResult::Resolved(col) => {
                resolved_columns.push(col);
            }
            super::unification::UnificationResult::Unresolved(name) => {
                // A name cannot be reported absent from an enumeration that
                // never happened.
                if registry.any_heading_opaque(visible) {
                    return Err(
                        super::resolving::domain_expressions::simple::opaque_heading_refusal(),
                    );
                }
                return Err(DelightQLError::column_not_found_error(name, error_context));
            }
            super::unification::UnificationResult::Opaque => {
                return Err(super::opaque_reference_refusal());
            }
            super::unification::UnificationResult::Refused(refusal) => {
                return Err(refusal.into_error());
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
