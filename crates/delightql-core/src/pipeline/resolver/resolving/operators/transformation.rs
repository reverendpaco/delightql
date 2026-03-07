use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::resolved::NamespacePath;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

use super::super::helpers::{
    build_concat_chain_with_placeholders, convert_column_alias, extract_column_name_from_expr,
};
use super::helpers::{emit_validation_warning, expand_column_template};

/// Check if a column's table provenance matches a qualifier string
fn matches_table_qualifier(col: &ast_resolved::ColumnMetadata, qualifier: &str) -> bool {
    match &col.fq_table.name {
        ast_resolved::TableName::Named(name) => name.as_ref() == qualifier,
        ast_resolved::TableName::Fresh => false,
    }
}

/// Resolve the MapCover operator via fold-based dispatch
///
/// Same semantics as `resolve_map_cover`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(super) fn resolve_map_cover_via_fold(
    fold: &mut ResolverFold,
    function: ast_unresolved::FunctionExpression,
    columns: Vec<ast_unresolved::DomainExpression>,
    containment_semantic: ast_unresolved::ContainmentSemantic,
    conditioned_on: Option<Box<ast_unresolved::BooleanExpression>>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Check if function is a StringTemplate and expand it to a Lambda
    let resolved_function =
        if let ast_unresolved::FunctionExpression::StringTemplate { parts, alias } = function {
            // Build the concat expression from the template parts
            // This time, we DON'T resolve @ placeholders - they stay as ValuePlaceholder
            let concat_expr = build_concat_chain_with_placeholders(parts)?;

            // Wrap in a Lambda since this is for MapCover
            ast_resolved::FunctionExpression::Lambda {
                body: Box::new(concat_expr),
                alias,
            }
        } else {
            // Regular function resolution — use fold's transform_function
            fold.transform_function(function)?
        };

    // Resolve columns - allow zero matches for patterns (Transform is safe as no-op)
    let resolved_columns =
        super::super::domain_expressions::projection::resolve_expressions_via_fold(
            fold, columns, available, true,
        )?;

    // Check if pattern matched zero columns (warning)
    if resolved_columns.is_empty() && !available.is_empty() {
        emit_validation_warning("MapCover pattern matched no columns - no transformation applied");
    }

    let resolved_condition = conditioned_on
        .map(|cond| fold.transform_boolean(*cond).map(Box::new))
        .transpose()?;

    let resolved_op = ast_resolved::UnaryRelationalOperator::MapCover {
        function: resolved_function,
        columns: resolved_columns,
        containment_semantic:
            super::super::super::helpers::converters::convert_containment_semantic(
                containment_semantic,
            ),
        conditioned_on: resolved_condition,
    };

    // MapCover applies a function to columns - for now just preserve input
    // TODO: Properly compute transformed columns
    Ok((resolved_op, available.to_vec()))
}

/// Resolve the Transform operator via fold-based dispatch
///
/// Same semantics as `resolve_transform`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(super) fn resolve_transform_via_fold(
    fold: &mut ResolverFold,
    transformations: Vec<(ast_unresolved::DomainExpression, String, Option<String>)>,
    conditioned_on: Option<Box<ast_unresolved::BooleanExpression>>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve each transformation expression
    let mut resolved_transformations = Vec::new();
    for (expr, alias, qualifier) in transformations {
        let resolved_expr =
            super::super::domain_expressions::projection::resolve_expressions_via_fold(
                fold,
                vec![expr],
                available,
                false,
            )?
            .into_iter()
            .next()
            .expect("resolve_expressions_via_fold returns same count as input");
        resolved_transformations.push((resolved_expr, alias.clone(), qualifier));
    }

    // Validate: all aliases must match existing column names (with optional qualifier)
    for (_, alias, qualifier) in &resolved_transformations {
        let matches = available.iter().any(|col| {
            if col.name() != alias {
                return false;
            }
            if let Some(ref q) = qualifier {
                matches_table_qualifier(col, q)
            } else {
                true
            }
        });
        if !matches {
            let display = match qualifier {
                Some(q) => format!("{}.{}", q, alias),
                None => alias.clone(),
            };
            return Err(DelightQLError::ParseError {
                message: format!(
                    "Transform alias '{}' does not match any existing column",
                    display
                ),
                source: None,
                subcategory: None,
            });
        }
    }

    // Check for duplicate aliases (qualifier-aware)
    let mut seen_aliases: std::collections::HashSet<(String, Option<String>)> =
        std::collections::HashSet::new();
    for (_, alias, qualifier) in &resolved_transformations {
        if !seen_aliases.insert((alias.clone(), qualifier.clone())) {
            let display = match qualifier {
                Some(q) => format!("{}.{}", q, alias),
                None => alias.clone(),
            };
            return Err(DelightQLError::ParseError {
                message: format!("Duplicate transform alias '{}'", display),
                source: None,
                subcategory: None,
            });
        }
    }

    let resolved_condition = conditioned_on
        .map(|cond| fold.transform_boolean(*cond).map(Box::new))
        .transpose()?;

    let resolved_op = ast_resolved::UnaryRelationalOperator::Transform {
        transformations: resolved_transformations,
        conditioned_on: resolved_condition,
    };

    // Output schema is same as input (transformations are in-place)
    Ok((resolved_op, available.to_vec()))
}

/// Resolve the EmbedMapCover operator via fold-based dispatch
///
/// Same semantics as `resolve_embed_map_cover`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(super) fn resolve_embed_map_cover_via_fold(
    fold: &mut ResolverFold,
    function: ast_unresolved::FunctionExpression,
    selector: ast_unresolved::ColumnSelector,
    alias_template: Option<ast_unresolved::ColumnAlias>,
    containment_semantic: ast_unresolved::ContainmentSemantic,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve the function (similar to MapCover)
    let resolved_function =
        if let ast_unresolved::FunctionExpression::StringTemplate { parts, alias } = function {
            // Build the concat expression from the template parts
            let concat_expr = build_concat_chain_with_placeholders(parts)?;
            // Wrap in a Lambda since this is for EmbedMapCover
            ast_resolved::FunctionExpression::Lambda {
                body: Box::new(concat_expr),
                alias,
            }
        } else {
            fold.transform_function(function)?
        };

    // For EmbedMapCover, we need to:
    // 1. Keep all original columns
    // 2. Add new columns for each transformation
    let mut output_columns = available.to_vec();

    // Resolve the column selector to actual column names AND create a Resolved variant
    let (resolved_selector, selected_columns) = match &selector {
        ast_unresolved::ColumnSelector::Explicit(exprs) => {
            // For explicit columns, keep as explicit (no pattern resolution needed)
            let resolved_exprs =
                super::super::domain_expressions::projection::resolve_expressions_via_fold(
                    fold,
                    exprs.clone(),
                    available,
                    false,
                )?;
            let column_names = resolved_exprs
                .iter()
                .filter_map(extract_column_name_from_expr)
                .collect::<Vec<_>>();
            (
                ast_resolved::ColumnSelector::Explicit(resolved_exprs),
                column_names,
            )
        }
        ast_unresolved::ColumnSelector::Regex(pattern) => {
            // Convert BRE pattern to Rust regex and resolve to column list
            use crate::pipeline::pattern::bre_to_rust_regex;
            let regex_pattern = bre_to_rust_regex(pattern)?;
            let regex = regex::Regex::new(&regex_pattern).map_err(|e| {
                DelightQLError::parse_error(format!("Invalid regex pattern: {}", e))
            })?;
            let matched_columns: Vec<String> = available
                .iter()
                .filter(|col| regex.is_match(col.name()))
                .map(|col| col.name().to_string())
                .collect();
            let original_selector =
                Box::new(ast_unresolved::ColumnSelector::Regex(pattern.clone()));
            (
                ast_resolved::ColumnSelector::Resolved {
                    columns: matched_columns.clone(),
                    original_selector,
                },
                matched_columns,
            )
        }
        ast_unresolved::ColumnSelector::All => {
            // For All, resolve to all available columns
            let all_columns: Vec<String> =
                available.iter().map(|col| col.name().to_string()).collect();
            let original_selector = Box::new(ast_unresolved::ColumnSelector::All);
            (
                ast_resolved::ColumnSelector::Resolved {
                    columns: all_columns.clone(),
                    original_selector,
                },
                all_columns,
            )
        }
        ast_unresolved::ColumnSelector::Positional { start, end } => {
            // For positional, resolve to specific columns
            let positional_columns: Vec<String> = available
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx >= (*start - 1) && *idx < *end)
                .map(|(_, col)| col.name().to_string())
                .collect();
            let original_selector = Box::new(ast_unresolved::ColumnSelector::Positional {
                start: *start,
                end: *end,
            });
            (
                ast_resolved::ColumnSelector::Resolved {
                    columns: positional_columns.clone(),
                    original_selector,
                },
                positional_columns,
            )
        }
        ast_unresolved::ColumnSelector::MultipleRegex(patterns) => {
            // Multiple regex patterns - union of matches, convert to Resolved
            use crate::pipeline::pattern::bre_to_rust_regex;
            let mut matched = Vec::new();
            for pattern in patterns {
                let regex_pattern = bre_to_rust_regex(pattern)?;
                let regex = regex::Regex::new(&regex_pattern).map_err(|e| {
                    DelightQLError::parse_error(format!("Invalid regex pattern: {}", e))
                })?;
                for col in available {
                    if regex.is_match(col.name()) && !matched.contains(&col.name().to_string()) {
                        matched.push(col.name().to_string());
                    }
                }
            }
            let original_selector = Box::new(ast_unresolved::ColumnSelector::MultipleRegex(
                patterns.clone(),
            ));
            (
                ast_resolved::ColumnSelector::Resolved {
                    columns: matched.clone(),
                    original_selector,
                },
                matched,
            )
        }
        ast_unresolved::ColumnSelector::Resolved { .. } => {
            // This should never happen in unresolved phase
            unreachable!("Resolved selector should not exist in unresolved phase")
        }
    };

    // Add new columns based on the transformation
    for column_name in &selected_columns {
        // Calculate the position for the NEW column being added
        let new_column_position = output_columns.len() + 1;

        // Expand the alias template if present
        let new_column_name =
            if let Some(ast_unresolved::ColumnAlias::Template(template)) = &alias_template {
                // Use expand_column_template to handle both {@} and {#}
                // For {#}, use the NEW column's position in the output, not the source column's position
                expand_column_template(&template.template, column_name, Some(new_column_position))?
            } else if let Some(ast_unresolved::ColumnAlias::Literal(name)) = &alias_template {
                // Use literal alias
                name.clone()
            } else {
                // Default: append "_transformed" or similar
                format!("{}_transformed", column_name)
            };

        // Add the new column to the output
        output_columns.push(ast_resolved::ColumnMetadata::new_with_name_flag(
            ast_resolved::ColumnProvenance::from_table_column(
                new_column_name.clone(),
                ast_resolved::TableName::Fresh,
                false,
            ),
            ast_resolved::FqTable {
                parents_path: NamespacePath::empty(),
                name: ast_resolved::TableName::Fresh,
                backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
            },
            Some(new_column_position),
            true,
        ));
    }

    // Warn if no columns matched (Embed is safe as no-op - originals preserved)
    if selected_columns.is_empty() && !available.is_empty() {
        emit_validation_warning("EmbedMapCover pattern matched no columns - no columns added");
    }

    let resolved_op = ast_resolved::UnaryRelationalOperator::EmbedMapCover {
        function: resolved_function,
        selector: resolved_selector,
        alias_template: convert_column_alias(alias_template),
        containment_semantic:
            super::super::super::helpers::converters::convert_containment_semantic(
                containment_semantic,
            ),
    };

    Ok((resolved_op, output_columns))
}
