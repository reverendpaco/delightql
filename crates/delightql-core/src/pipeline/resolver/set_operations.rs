//! Set operation validation and schema building
//!
//! This module handles validation and schema construction for set operations
//! like UNION ALL (positional and CORRESPONDING), INTERSECT, etc.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use std::collections::HashSet;

/// Validate that two schemas are compatible for UNION ALL
pub(super) fn validate_union_compatible_schemas(
    s1: &ast_resolved::CprSchema,
    s2: &ast_resolved::CprSchema,
) -> Result<()> {
    let cols1 = match s1 {
        ast_resolved::CprSchema::Resolved(cols) => cols,
        _ => {
            return Err(DelightQLError::ParseError {
                message: "Cannot create UNION ALL with unresolved schema".to_string(),
                source: None,
                subcategory: None,
            });
        }
    };

    let cols2 = match s2 {
        ast_resolved::CprSchema::Resolved(cols) => cols,
        _ => {
            return Err(DelightQLError::ParseError {
                message: "Cannot create UNION ALL with unresolved schema".to_string(),
                source: None,
                subcategory: None,
            });
        }
    };

    if cols1.len() != cols2.len() {
        return Err(DelightQLError::ParseError {
            message: format!(
                "UNION ALL requires same column count: {} vs {}",
                cols1.len(),
                cols2.len()
            ),
            source: None,
            subcategory: None,
        });
    }

    // Check column names match (not just count).
    // If names differ, schemas are NOT the same -> caller uses CORRESPONDING.
    for (c1, c2) in cols1.iter().zip(cols2.iter()) {
        let n1 = c1.info.original_name().or_else(|| c1.info.alias_name());
        let n2 = c2.info.original_name().or_else(|| c2.info.alias_name());
        if n1 != n2 {
            return Err(DelightQLError::ParseError {
                message: format!(
                    "UNION ALL column name mismatch at position: {:?} vs {:?}",
                    n1, n2
                ),
                source: None,
                subcategory: None,
            });
        }
    }

    Ok(())
}

/// Build a unified schema for CORRESPONDING operations
/// Takes multiple schemas and creates a unified schema with all unique columns
/// Preserves the order from the first schema, appending new columns from subsequent schemas
pub(super) fn build_corresponding_schema(
    schemas: &[ast_resolved::CprSchema],
) -> Result<ast_resolved::CprSchema> {
    if schemas.is_empty() {
        return Err(DelightQLError::ParseError {
            message: "Cannot build corresponding schema from empty list".to_string(),
            source: None,
            subcategory: None,
        });
    }

    // Start with the first schema as base
    let mut unified_columns: Vec<ast_resolved::ColumnMetadata> = match &schemas[0] {
        ast_resolved::CprSchema::Resolved(cols) => cols.clone(),
        _ => {
            return Err(DelightQLError::ParseError {
                message: "Cannot build corresponding schema from unresolved schema".to_string(),
                source: None,
                subcategory: None,
            });
        }
    };

    // Track which column names we've already seen
    let mut seen_names: HashSet<String> = unified_columns
        .iter()
        .filter_map(|col| {
            col.info
                .original_name()
                .or_else(|| col.info.alias_name())
                .map(|s| s.to_string())
        })
        .collect();

    // Process each subsequent schema
    for schema in &schemas[1..] {
        let cols = match schema {
            ast_resolved::CprSchema::Resolved(cols) => cols,
            _ => {
                return Err(DelightQLError::ParseError {
                    message: "Cannot build corresponding schema from unresolved schema".to_string(),
                    source: None,
                    subcategory: None,
                });
            }
        };

        // Add any columns that we haven't seen yet
        for col in cols {
            let col_name = col
                .info
                .original_name()
                .or_else(|| col.info.alias_name())
                .unwrap_or("unknown")
                .to_string();

            if !seen_names.contains(&col_name) {
                seen_names.insert(col_name);
                unified_columns.push(col.clone());
            }
        }
    }

    Ok(ast_resolved::CprSchema::Resolved(unified_columns))
}

/// Validate schemas based on the set operator type
pub(super) fn validate_set_operation_schemas(
    operator: &ast_resolved::SetOperator,
    _s1: &ast_resolved::CprSchema,
    _s2: &ast_resolved::CprSchema,
) -> Result<()> {
    match operator {
        ast_resolved::SetOperator::UnionAllPositional
        | ast_resolved::SetOperator::SmartUnionAll => {
            // Positional and smart union require same column count
            if let (
                ast_resolved::CprSchema::Resolved(cols1),
                ast_resolved::CprSchema::Resolved(cols2),
            ) = (_s1, _s2)
            {
                if cols1.len() != cols2.len() {
                    return Err(DelightQLError::validation_error_categorized(
                        "set_operation/column_count_mismatch",
                        format!(
                            "Set operation requires both sides to have the same number of columns, \
                             but left has {} and right has {}",
                            cols1.len(),
                            cols2.len(),
                        ),
                        "Positional union column count mismatch",
                    ));
                }
            }
            Ok(())
        }
        ast_resolved::SetOperator::UnionCorresponding => {
            // No validation needed - all schemas are compatible for CORRESPONDING
            Ok(())
        }
        ast_resolved::SetOperator::MinusCorresponding => {
            // Minus requires same column names (by name match)
            // The output is always the left side's schema
            Ok(())
        }
    }
}
