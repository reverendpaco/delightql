use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;

/// Helper to emit validation warnings
pub(super) fn emit_validation_warning(warning: &str) {
    log::warn!("Column validation: {}", warning);
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
    reducing_by: &mut Vec<ast_resolved::DomainExpression>,
    reducing_on: &mut Vec<ast_resolved::DomainExpression>,
) -> Result<()> {
    use ast_resolved::{DomainExpression, FunctionExpression};

    let mut new_reducing_on = Vec::new();

    for expr in reducing_on.drain(..) {
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
                    new_reducing_on.push(DomainExpression::Function(FunctionExpression::Curly {
                        members: nested_members,
                        inner_grouping_keys,    // Store promoted columns here!
                        cte_requirements: None, // Phase R2+ will populate this
                        alias,
                    }));
                } else {
                    // No nested reductions - keep the tree group as-is in reducing_on
                    new_reducing_on.push(DomainExpression::Function(FunctionExpression::Curly {
                        members,
                        inner_grouping_keys: vec![], // No promotions needed
                        cte_requirements: None,      // Phase R2+ will populate this
                        alias,
                    }));
                }
            }
            // Non-tree-group expressions stay in reducing_on
            other => new_reducing_on.push(other),
        }
    }

    *reducing_on = new_reducing_on;

    // Also process tree groups in reducing_by to populate their inner_grouping_keys
    let mut new_reducing_by = Vec::new();

    for expr in reducing_by.drain(..) {
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
                    new_reducing_by.push(DomainExpression::Function(FunctionExpression::Curly {
                        members: nested_members,
                        inner_grouping_keys,
                        cte_requirements: None, // Phase R2+ will populate this
                        alias,
                    }));
                } else {
                    // No nested reductions - keep the tree group as-is
                    new_reducing_by.push(DomainExpression::Function(FunctionExpression::Curly {
                        members,
                        inner_grouping_keys: vec![],
                        cte_requirements: None,
                        alias,
                    }));
                }
            }
            // Non-tree-group expressions stay in reducing_by
            other => new_reducing_by.push(other),
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
