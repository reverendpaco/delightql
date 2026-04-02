// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Unification Engine for Column Resolution
//!
//! This module handles matching unresolved column references against available columns,
//! detecting ambiguities, and building the resolved column metadata.

use crate::pipeline::ast_resolved::{ColumnMetadata, TableName};

/// Result of unifying a column reference
#[derive(Debug)]
pub enum UnificationResult {
    /// Successfully matched to exactly one column
    Resolved(ColumnMetadata),
    /// Could not find any matching column
    Unresolved(String),
    /// Found multiple matching columns (ambiguous)
    Ambiguous { column: String, tables: Vec<String> },
}

/// Unify a set of column references against available columns
pub fn unify_columns(
    references: Vec<ColumnReference>,
    available: &[ColumnMetadata],
) -> Vec<UnificationResult> {
    references
        .into_iter()
        .map(|ref_col| unify_single_column(ref_col, available))
        .collect()
}

/// A column reference that needs to be resolved
#[derive(Debug, Clone)]
pub enum ColumnReference {
    /// Named column reference (e.g., "user_id" or "users.id")
    Named {
        name: String,
        qualifier: Option<String>,
        schema: Option<String>,
    },
    /// Ordinal column reference (e.g., |3| or |-1|)
    Ordinal {
        position: u16,
        reverse: bool,
        qualifier: Option<String>,
        alias: Option<String>,
    },
}

/// Unify a single column reference against available columns
fn unify_single_column(
    reference: ColumnReference,
    available: &[ColumnMetadata],
) -> UnificationResult {
    match reference {
        ColumnReference::Named {
            name,
            qualifier,
            schema: _,
        } => {
            let matches: Vec<&ColumnMetadata> = available
                .iter()
                .filter(|col| {
                    matches_column(
                        col,
                        &ColumnReference::Named {
                            name: name.clone(),
                            qualifier: qualifier.clone(),
                            schema: None,
                        },
                    )
                })
                .collect();

            match matches.len() {
                0 => UnificationResult::Unresolved(name),
                1 => {
                    // Check if this column allows name-based access
                    if !matches[0].has_user_name {
                        // Find the position of this column for the error message
                        let position = available
                            .iter()
                            .position(|c| c == matches[0])
                            .map(|p| p + 1) // Convert to 1-based
                            .unwrap_or(0);
                        return UnificationResult::Unresolved(format!(
                            "{} (unnamed column, use |{}| instead)",
                            name, position
                        ));
                    }

                    // Clone the matched column and update qualification based on the reference
                    let mut resolved = matches[0].clone();
                    // If the reference had a qualifier, mark as USER-qualified
                    if qualifier.is_some() {
                        resolved.info = resolved.info.with_updated_qualification(
                            crate::pipeline::asts::core::QualificationSource::User,
                        );
                    }
                    UnificationResult::Resolved(resolved)
                }
                n => {
                    // Multiple matches - check for precedence rules
                    debug_assert!(n > 1, "Unexpected match count: {}", n);

                    // PRECEDENCE RULE: For unqualified references, prefer anonymous tables (Fresh)
                    // over named tables to handle pipe result scoping correctly
                    if qualifier.is_none() {
                        // Find Fresh (anonymous) table matches
                        let fresh_matches: Vec<&ColumnMetadata> = matches
                            .iter()
                            .filter(|col| matches!(col.qualifier(), TableName::Fresh))
                            .copied()
                            .collect();

                        // If we have exactly one Fresh match, prefer it over named tables
                        if fresh_matches.len() == 1 {
                            // Check if this column allows name-based access
                            if !fresh_matches[0].has_user_name {
                                let position = available
                                    .iter()
                                    .position(|c| c == fresh_matches[0])
                                    .map(|p| p + 1)
                                    .unwrap_or(0);
                                return UnificationResult::Unresolved(format!(
                                    "{} (unnamed column, use |{}| instead)",
                                    name, position
                                ));
                            }
                            let mut resolved = fresh_matches[0].clone();
                            resolved.info = resolved.info.with_updated_qualification(
                                crate::pipeline::asts::core::QualificationSource::None,
                            ); // Unqualified reference
                            return UnificationResult::Resolved(resolved);
                        }
                    }

                    // No precedence rule applies - report ambiguity
                    let tables: Vec<String> = matches
                        .iter()
                        .map(|col| match col.qualifier() {
                            TableName::Named(table_name) => table_name.to_string(),
                            TableName::Fresh => "_".to_string(),
                        })
                        .collect();
                    UnificationResult::Ambiguous {
                        column: name,
                        tables,
                    }
                }
            }
        }
        ColumnReference::Ordinal {
            position,
            reverse,
            qualifier,
            alias,
        } => {
            // Resolve ordinals by position!
            // Filter available columns by qualifier if present
            let candidates = if let Some(qual) = &qualifier {
                available
                    .iter()
                    .filter(|col| matches!(col.qualifier(), TableName::Named(t) if t == qual))
                    .collect::<Vec<_>>()
            } else {
                available.iter().collect::<Vec<_>>()
            };

            if candidates.is_empty() {
                let name = if reverse {
                    format!("|-{}|", position)
                } else {
                    format!("|{}|", position)
                };
                return UnificationResult::Unresolved(name);
            }

            // Calculate actual index
            let idx = if reverse {
                // Negative indexing from end
                if position as usize > candidates.len() {
                    let name = format!("|-{}|", position);
                    return UnificationResult::Unresolved(name);
                }
                candidates.len() - position as usize
            } else {
                // Positive indexing from start (1-based)
                if position == 0 || position as usize > candidates.len() {
                    let name = format!("|{}|", position);
                    return UnificationResult::Unresolved(name);
                }
                position as usize - 1
            };

            // Get the column at that position
            let mut resolved = candidates[idx].clone();

            // Apply alias if present
            if let Some(alias_name) = alias {
                resolved.info = resolved.info.with_alias(alias_name);
            }

            // Mark as USER-qualified if it had a qualifier
            if qualifier.is_some() {
                resolved.info = resolved.info.with_updated_qualification(
                    crate::pipeline::asts::core::QualificationSource::User,
                );
            }

            UnificationResult::Resolved(resolved)
        }
    }
}

/// Check if a column metadata matches a column reference
fn matches_column(col: &ColumnMetadata, reference: &ColumnReference) -> bool {
    match reference {
        ColumnReference::Named {
            name,
            qualifier,
            schema: _,
        } => {
            // Check column name (using effective name for matching)
            if !super::col_name_eq(col.name(), name) {
                return false;
            }

            // If reference has a qualifier, it must match the table name
            if let Some(ref qual) = qualifier {
                if qual == "_" {
                    // Special CPR reference - only matches Fresh (anonymous) tables
                    if !matches!(col.qualifier(), TableName::Fresh) {
                        return false;
                    }
                } else {
                    // Regular qualifier - must match table name
                    match col.qualifier() {
                        TableName::Named(table_name) => {
                            if table_name != qual {
                                return false;
                            }
                        }
                        TableName::Fresh => {
                            // Anonymous tables can't be qualified with regular names
                            return false;
                        }
                    }
                }
            }

            true
        }
        ColumnReference::Ordinal { .. } => {
            // Ordinals don't match by name, will be handled separately
            false
        }
    }
}
