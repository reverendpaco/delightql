// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;

/// Helper to emit validation warnings
pub(super) fn emit_validation_warning(warning: &str) {
    log::warn!("Column validation: {}", warning);
}

/// Check that programmer-authored column names are unique in an output schema.
/// Engine-managed names (has_user_name == false) are allowed to collide.
pub(super) fn check_duplicate_user_names(output: &[ast_resolved::ColumnMetadata]) -> Result<()> {
    let mut seen = std::collections::HashMap::new();
    for col in output {
        if !col.has_user_name {
            continue;
        }
        let name = col.name();
        if seen.contains_key(name) {
            return Err(DelightQLError::validation_error_categorized(
                "constraint",
                format!(
                    "Duplicate column '{}': programmer-authored names must be unique. \
                     Rename one with 'as' to disambiguate",
                    name,
                ),
                "in output schema",
            ));
        }
        seen.insert(name, ());
    }
    Ok(())
}

/// Apply the sanitization protocol to engine-managed columns in an output schema.
///
/// When engine-managed columns (from glob, pattern, range expansion) share a name,
/// every instance of the colliding name gets the disambiguated form:
///   `<scope>.<column>|<N>|`
/// where `<scope>` is the table name or alias, and `<N>` is the 1-based global ordinal.
///
/// Non-colliding engine-managed columns keep their bare names.
/// Programmer-authored columns (has_user_name == true) are never touched.
pub(in crate::pipeline::resolver) fn sanitize_engine_managed_columns(
    output: &mut Vec<ast_resolved::ColumnMetadata>,
    is_engine_col: &[bool],
) {
    // Find which bare names collide among engine-managed columns
    let mut name_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for (idx, col) in output.iter().enumerate() {
        if is_engine_col[idx] {
            *name_counts.entry(col.name().to_string()).or_insert(0) += 1;
        }
    }

    // Apply sanitized names to every instance of a colliding name
    for (idx, col) in output.iter_mut().enumerate() {
        if !is_engine_col[idx] {
            continue;
        }
        let bare_name = col.name().to_string();
        if name_counts.get(&bare_name).copied().unwrap_or(0) <= 1 {
            continue; // No collision, keep bare name
        }

        // Build sanitized name: <scope>.<column>|<N>|
        let ordinal = idx + 1; // 1-based
        let scope = match col.qualifier() {
            ast_resolved::TableName::Named(t) if !t.is_empty() => t.to_string(),
            _ => "_".to_string(),
        };
        let sanitized = format!("{}.{}|{}|", scope, bare_name, ordinal);

        col.info = ast_resolved::ColumnProvenance::from_column(sanitized);
    }
}

/// Restructure tree groups for proper grouping when nested reductions are present
///
/// When a tree group in `reducing_on` contains both non-nested and nested members:
/// - Non-nested members (simple columns/shorthands) are promoted to `reducing_by`
/// - Nested reduction members (with ~>) stay in `reducing_on`
///
/// Example transformation:
/// ```
/// reducing_by: []
/// reducing_on: [{country, "people": ~> {...}}]
/// ```
/// becomes:
/// ```
/// reducing_by: [country]
/// reducing_on: [{"people": ~> {...}}]
/// ```
pub(super) fn restructure_tree_groups_for_grouping(
    reducing_by: &mut Vec<ast_resolved::OutputDomainExpression>,
    reducing_on: &mut Vec<ast_resolved::OutputDomainExpression>,
) -> Result<()> {
    use ast_resolved::{DomainExpression, FunctionExpression};

    let mut new_reducing_on: Vec<ast_resolved::OutputDomainExpression> = Vec::new();

    for ode in reducing_on.drain(..) {
        // The phantom output stamp rides along untouched — restructuring runs
        // before the resolver stamps the output decision (Batch 13, slice 4).
        let ast_resolved::OutputDomainExpression { expr, output } = ode;
        match expr {
            DomainExpression::Function(FunctionExpression::Curly {
                members,
                inner_grouping_keys: _,
                cte_requirements: _,
                alias,
            }) => {
                // Check if this tree group has any nested reductions
                let has_nested_reduction = members.iter().any(|m| {
                    matches!(
                        m,
                        ast_resolved::CurlyMember::KeyValue {
                            nested_reduction: true,
                            ..
                        }
                    )
                });

                if has_nested_reduction {
                    // Split members into grouping columns and nested reductions
                    let mut nested_members = Vec::new();
                    let mut inner_grouping_keys = Vec::new(); // NEW: collect promoted columns here

                    for member in members {
                        match member {
                            ast_resolved::CurlyMember::Shorthand {
                                column,
                                qualifier,
                                schema,
                            } => {
                                // Add to inner_grouping_keys for analysis
                                inner_grouping_keys.push(DomainExpression::Lvar {
                                    name: column.clone(),
                                    qualifier: qualifier.clone(),
                                    namespace_path: schema
                                        .as_ref()
                                        .map(|s| {
                                            crate::pipeline::asts::resolved::NamespacePath::single(
                                                s.clone(),
                                            )
                                        })
                                        .unwrap_or_else(|| {
                                            crate::pipeline::asts::resolved::NamespacePath::empty()
                                        }),
                                    alias: None,
                                    provenance: ast_resolved::PhaseBox::phantom(),
                                });
                                // KEEP in members - resolver should only annotate!
                                nested_members.push(ast_resolved::CurlyMember::Shorthand {
                                    column,
                                    qualifier,
                                    schema,
                                });
                            }
                            ast_resolved::CurlyMember::KeyValue {
                                key,
                                nested_reduction: false,
                                value,
                            } => {
                                // Add to inner_grouping_keys for analysis
                                inner_grouping_keys.push(*value.clone());
                                // KEEP in members - resolver should only annotate, not transform!
                                nested_members.push(ast_resolved::CurlyMember::KeyValue {
                                    key,
                                    nested_reduction: false,
                                    value,
                                });
                            }

                            ast_resolved::CurlyMember::KeyValue {
                                key,
                                nested_reduction: true,
                                value,
                            } => {
                                // Nested reduction: keep in the tree group
                                nested_members.push(ast_resolved::CurlyMember::KeyValue {
                                    key,
                                    nested_reduction: true,
                                    value,
                                });
                            }

                            ast_resolved::CurlyMember::Comparison { .. } => {
                                // Comparisons stay as filters (not implemented yet)
                                // For now, just keep them
                                nested_members.push(member);
                            }
                            // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
                            ast_resolved::CurlyMember::PathLiteral { path, alias } => {
                                // PathLiterals don't contribute to grouping keys, just pass through
                                nested_members
                                    .push(ast_resolved::CurlyMember::PathLiteral { path, alias });
                            }
                            // TG-ERGONOMIC-INDUCTOR: These should have been expanded by earlier resolver
                            ast_resolved::CurlyMember::Glob
                            | ast_resolved::CurlyMember::Pattern { .. }
                            | ast_resolved::CurlyMember::OrdinalRange { .. } => {
                                return Err(crate::error::DelightQLError::ParseError {
                                    message: "Glob/Pattern/OrdinalRange in curly member should have been expanded by resolver".to_string(),
                                    source: None,
                                    subcategory: None,
                                });
                            }
                            // Placeholder is only valid in destructuring, not in construction
                            ast_resolved::CurlyMember::Placeholder => {
                                return Err(crate::error::DelightQLError::ParseError {
                                    message: "Placeholder in curly member should only appear in destructuring context".to_string(),
                                    source: None,
                                    subcategory: None,
                                });
                            }
                        }
                    }

                    // Create tree group with inner_grouping_keys and only nested members
                    new_reducing_on.push(ast_resolved::OutputDomainExpression {
                        expr: DomainExpression::Function(FunctionExpression::Curly {
                            members: nested_members,
                            inner_grouping_keys,    // Store promoted columns here!
                            cte_requirements: None, // Phase R2+ will populate this
                            alias,
                        }),
                        output,
                    });
                } else {
                    // No nested reductions - keep the tree group as-is in reducing_on
                    new_reducing_on.push(ast_resolved::OutputDomainExpression {
                        expr: DomainExpression::Function(FunctionExpression::Curly {
                            members,
                            inner_grouping_keys: vec![], // No promotions needed
                            cte_requirements: None,      // Phase R2+ will populate this
                            alias,
                        }),
                        output,
                    });
                }
            }
            // Non-tree-group expressions stay in reducing_on
            other => new_reducing_on.push(ast_resolved::OutputDomainExpression {
                expr: other,
                output,
            }),
        }
    }

    *reducing_on = new_reducing_on;

    // Also process tree groups in reducing_by to populate their inner_grouping_keys.
    // reducing_by keys now carry their phantom output stamp (slice 4); it rides
    // untouched — restructuring runs before the resolver stamps.
    let mut new_reducing_by: Vec<ast_resolved::OutputDomainExpression> = Vec::new();

    for ode in reducing_by.drain(..) {
        let ast_resolved::OutputDomainExpression { expr, output } = ode;
        match expr {
            DomainExpression::Function(FunctionExpression::Curly {
                members,
                inner_grouping_keys: _,
                cte_requirements: _,
                alias,
            }) => {
                // Check if this tree group has any nested reductions
                let has_nested_reduction = members.iter().any(|m| {
                    matches!(
                        m,
                        ast_resolved::CurlyMember::KeyValue {
                            nested_reduction: true,
                            ..
                        }
                    )
                });

                if has_nested_reduction {
                    // Split members into grouping columns and nested reductions
                    let mut nested_members = Vec::new();
                    let mut inner_grouping_keys = Vec::new();

                    for member in members {
                        match member {
                            ast_resolved::CurlyMember::Shorthand {
                                column,
                                qualifier,
                                schema,
                            } => {
                                // Add to inner_grouping_keys (stays in tree group, not promoted)
                                inner_grouping_keys.push(DomainExpression::Lvar {
                                    name: column.clone(),
                                    qualifier: qualifier.clone(),
                                    namespace_path: schema
                                        .as_ref()
                                        .map(|s| {
                                            crate::pipeline::asts::resolved::NamespacePath::single(
                                                s.clone(),
                                            )
                                        })
                                        .unwrap_or_else(|| {
                                            crate::pipeline::asts::resolved::NamespacePath::empty()
                                        }),
                                    alias: None,
                                    provenance: ast_resolved::PhaseBox::phantom(),
                                });
                                // Keep in nested_members for the tree group
                                nested_members.push(ast_resolved::CurlyMember::Shorthand {
                                    column,
                                    qualifier,
                                    schema,
                                });
                            }
                            ast_resolved::CurlyMember::KeyValue {
                                key,
                                nested_reduction: false,
                                value,
                            } => {
                                // Add to inner_grouping_keys (stays in tree group, not promoted)
                                inner_grouping_keys.push(*value.clone());
                                // Keep in nested_members for the tree group
                                nested_members.push(ast_resolved::CurlyMember::KeyValue {
                                    key,
                                    nested_reduction: false,
                                    value,
                                });
                            }

                            ast_resolved::CurlyMember::KeyValue {
                                key,
                                nested_reduction: true,
                                value,
                            } => {
                                // Nested reduction: keep in the tree group
                                nested_members.push(ast_resolved::CurlyMember::KeyValue {
                                    key,
                                    nested_reduction: true,
                                    value,
                                });
                            }

                            ast_resolved::CurlyMember::Comparison { .. } => {
                                // Comparisons stay as filters
                                nested_members.push(member);
                            }
                            // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
                            ast_resolved::CurlyMember::PathLiteral { path, alias } => {
                                // PathLiterals don't contribute to grouping keys, just pass through
                                nested_members
                                    .push(ast_resolved::CurlyMember::PathLiteral { path, alias });
                            }
                            // TG-ERGONOMIC-INDUCTOR: These should have been expanded by earlier resolver
                            ast_resolved::CurlyMember::Glob
                            | ast_resolved::CurlyMember::Pattern { .. }
                            | ast_resolved::CurlyMember::OrdinalRange { .. } => {
                                return Err(crate::error::DelightQLError::ParseError {
                                    message: "Glob/Pattern/OrdinalRange in curly member should have been expanded by resolver".to_string(),
                                    source: None,
                                    subcategory: None,
                                });
                            }
                            // Placeholder is only valid in destructuring, not in construction
                            ast_resolved::CurlyMember::Placeholder => {
                                return Err(crate::error::DelightQLError::ParseError {
                                    message: "Placeholder in curly member should only appear in destructuring context".to_string(),
                                    source: None,
                                    subcategory: None,
                                });
                            }
                        }
                    }

                    // Create tree group with inner_grouping_keys and all members
                    new_reducing_by.push(ast_resolved::OutputDomainExpression {
                        expr: DomainExpression::Function(FunctionExpression::Curly {
                            members: nested_members,
                            inner_grouping_keys,
                            cte_requirements: None, // Phase R2+ will populate this
                            alias,
                        }),
                        output,
                    });
                } else {
                    // No nested reductions - keep the tree group as-is
                    new_reducing_by.push(ast_resolved::OutputDomainExpression {
                        expr: DomainExpression::Function(FunctionExpression::Curly {
                            members,
                            inner_grouping_keys: vec![],
                            cte_requirements: None,
                            alias,
                        }),
                        output,
                    });
                }
            }
            // Non-tree-group expressions stay in reducing_by
            other => new_reducing_by.push(ast_resolved::OutputDomainExpression {
                expr: other,
                output,
            }),
        }
    }

    *reducing_by = new_reducing_by;
    Ok(())
}

/// Expand column name template with {@} and {#} placeholders
///
/// - `{@}` is replaced with the column name
/// - `{#}` is replaced with the absolute table position (1-indexed)
///
/// Returns an error if `{#}` is used but `table_position` is `None`
pub(super) fn expand_column_template(
    template: &str,
    column_name: &str,
    table_position: Option<usize>,
) -> Result<String> {
    let mut result = template.to_string();

    // Replace {#} with position
    if result.contains("{#}") {
        match table_position {
            Some(pos) => {
                result = result.replace("{#}", &pos.to_string());
            }
            None => {
                return Err(DelightQLError::parse_error(
                    "Cannot use {#} placeholder - column position unknown",
                ));
            }
        }
    }

    // Replace {@} with column name
    result = result.replace("{@}", column_name);

    Ok(result)
}
