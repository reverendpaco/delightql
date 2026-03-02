use crate::error::Result;
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::{ast_resolved, ast_unresolved};

use super::super::column_extraction::extract_provided_column_from_domain_expr;
use super::super::domain_expressions::resolve_expressions_with_schema;
use super::helpers::restructure_tree_groups_for_grouping;

/// Resolve the Modulo operator (GROUP BY / DISTINCT)
///
/// This handles grouping and aggregation operations:
/// - Simple DISTINCT (columns only)
/// - Complex GROUP BY with aggregations (reducing_by + reducing_on)
/// - Pivot expressions (PivotOf in reducing_on)
pub(super) fn resolve_modulo(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    spec: ast_unresolved::ModuloSpec,
    available: &[ast_resolved::ColumnMetadata],
    pivot_in_values: &std::collections::HashMap<String, Vec<String>>,
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve ModuloSpec (GROUP BY/DISTINCT)
    let (resolved_spec, output_columns) = match spec {
        ast_unresolved::ModuloSpec::Columns(cols) => {
            // Simple distinct/group on columns
            let resolved_cols =
                resolve_expressions_with_schema(cols, available, None, None, None, false)?;

            // Compute output columns - only the distinct columns
            let mut output = Vec::new();
            for (idx, expr) in resolved_cols.iter().enumerate() {
                if let Some(col) = extract_provided_column_from_domain_expr(expr, available, idx) {
                    output.push(col);
                }
            }

            (ast_resolved::ModuloSpec::Columns(resolved_cols), output)
        }
        ast_unresolved::ModuloSpec::GroupBy {
            reducing_by,
            reducing_on,
            arbitrary,
        } => {
            // Complex GROUP BY with aggregations
            let mut resolved_reducing_by =
                resolve_expressions_with_schema(reducing_by, available, None, None, None, false)?;
            let mut resolved_reducing_on =
                resolve_expressions_with_schema(reducing_on, available, None, None, None, false)?;

            // Populate pivot_values for PivotOf expressions from IN predicates
            for expr in resolved_reducing_on.iter_mut() {
                if let ast_resolved::DomainExpression::PivotOf {
                    pivot_key,
                    pivot_values,
                    ..
                } = expr
                {
                    match pivot_key.as_ref() {
                        ast_resolved::DomainExpression::Lvar { name, .. } => {
                            // Simple column reference: look up IN values directly
                            if let Some(values) = pivot_in_values.get(name.as_str()) {
                                *pivot_values = values.clone();
                            } else {
                                return Err(crate::error::DelightQLError::validation_error(
                                    format!(
                                        "Pivot 'of' on column '{}' requires a matching IN predicate with literal values",
                                        name
                                    ),
                                    "Add a filter like: column in (\"value1\"; \"value2\")".to_string(),
                                ));
                            }
                        }
                        _ => {
                            // Format-string case: pivot_key is a concat chain from StringTemplate
                            // e.g. :"{subject}_grade" → Infix("concat", Lvar("subject"), Literal("_grade"))
                            let lvar_names = extract_lvar_names_from_expr(pivot_key.as_ref());
                            if lvar_names.is_empty() {
                                return Err(crate::error::DelightQLError::validation_error(
                                    "Pivot 'of' key must reference a column (directly or via format string)".to_string(),
                                    "Use a column name or format string like: value of column, value of :\"{column}_suffix\"".to_string(),
                                ));
                            }

                            // Find the first referenced column that has IN values
                            let found = lvar_names.iter().find_map(|name| {
                                pivot_in_values.get(name).map(|v| (name.clone(), v.clone()))
                            });

                            if let Some((ref_col, in_values)) = found {
                                // Expand the concat template for each IN value
                                let mut expanded = Vec::new();
                                for value in &in_values {
                                    let mut subs = std::collections::HashMap::new();
                                    subs.insert(ref_col.clone(), value.clone());
                                    expanded
                                        .push(expand_concat_template(pivot_key.as_ref(), &subs));
                                }
                                *pivot_values = expanded;
                            } else {
                                let col_list = lvar_names.join(", ");
                                return Err(crate::error::DelightQLError::validation_error(
                                    format!(
                                        "Pivot 'of' format string references column(s) '{}' but none have a matching IN predicate",
                                        col_list
                                    ),
                                    "Add a filter like: column in (\"value1\"; \"value2\")".to_string(),
                                ));
                            }
                        }
                    }
                }
            }

            // Tree group restructuring: detect nested reductions and promote non-nested columns
            // If reducing_on contains a tree group with both non-nested and nested members,
            // move non-nested members to reducing_by
            restructure_tree_groups_for_grouping(
                &mut resolved_reducing_by,
                &mut resolved_reducing_on,
            )?;

            // Phase R3: Analyze tree groups and populate CTE requirements
            // This metadata will guide the transformer to generate independent CTEs
            super::super::tree_group_analysis::analyze_tree_groups_for_ctes(
                &mut resolved_reducing_by,
                &mut resolved_reducing_on,
            )?;

            // Compute output columns - GROUP BY columns plus aggregates
            let mut output = Vec::new();

            // First add the GROUP BY columns
            for (idx, expr) in resolved_reducing_by.iter().enumerate() {
                if let Some(col) = extract_provided_column_from_domain_expr(expr, available, idx) {
                    output.push(col);
                }
            }

            // Then add aggregate/pivot columns
            let base_idx = resolved_reducing_by.len();
            for (idx, expr) in resolved_reducing_on.iter().enumerate() {
                match expr {
                    ast_resolved::DomainExpression::PivotOf { pivot_values, .. } => {
                        // Add one output column per pivot value
                        for value in pivot_values {
                            let col_name = value.to_lowercase();
                            output.push(ast_resolved::ColumnMetadata::new_with_name_flag(
                                ast_resolved::ColumnProvenance::from_column(col_name),
                                ast_resolved::FqTable {
                                    parents_path: NamespacePath::empty(),
                                    name: ast_resolved::TableName::Fresh,
                                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(
                                        None,
                                    ),
                                },
                                None,
                                true,
                            ));
                        }
                    }
                    _ => {
                        if let Some(col) = extract_provided_column_from_domain_expr(
                            expr,
                            available,
                            base_idx + idx,
                        ) {
                            output.push(col);
                        }
                    }
                }
            }

            // Check for duplicate pivot column names.
            // The book requires each `of` expression to produce distinct column names.
            // e.g. `score of subject, grade of subject` is invalid — use format strings
            // like `grade of :"{subject}_grade"` to disambiguate.
            {
                let pivot_col_names: Vec<String> = output
                    .iter()
                    .skip(resolved_reducing_by.len())
                    .map(|col| col.name().to_string())
                    .collect();
                let mut seen = std::collections::HashSet::new();
                for name in &pivot_col_names {
                    if !seen.insert(name.as_str()) {
                        return Err(crate::error::DelightQLError::validation_error(
                            format!(
                                "Duplicate pivot column name '{}'. Each 'of' expression must produce distinct column names",
                                name
                            ),
                            "Use a format string to disambiguate, e.g.: grade of :\"{subject}_grade\"".to_string(),
                        ));
                    }
                }
            }

            // Resolve arbitrary columns
            let resolved_arbitrary =
                resolve_expressions_with_schema(arbitrary, available, None, None, None, false)?;

            // Add arbitrary columns to output
            let base_idx = resolved_reducing_by.len() + resolved_reducing_on.len();
            for (idx, expr) in resolved_arbitrary.iter().enumerate() {
                if let Some(col) =
                    extract_provided_column_from_domain_expr(expr, available, base_idx + idx)
                {
                    output.push(col);
                }
            }

            // Capture interior schemas for tree group columns
            // When reducing_on contains a Curly function with an alias (e.g., ~> {name, type} as entities),
            // attach the interior schema to the corresponding output column so drill-down can use it.
            capture_interior_schemas(&resolved_reducing_on, &mut output);

            let spec = ast_resolved::ModuloSpec::GroupBy {
                reducing_by: resolved_reducing_by,
                reducing_on: resolved_reducing_on,
                arbitrary: resolved_arbitrary,
            };

            (spec, output)
        }
    };

    let resolved_op = ast_resolved::UnaryRelationalOperator::Modulo {
        containment_semantic:
            super::super::super::helpers::converters::convert_containment_semantic(
                containment_semantic,
            ),
        spec: resolved_spec,
    };

    Ok((resolved_op, output_columns))
}

/// Resolve the AggregatePipe operator
///
/// This handles aggregation-only operations (no grouping).
/// Output schema contains only the aggregated expressions.
pub(super) fn resolve_aggregate_pipe(
    aggregations: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve aggregation expressions
    let resolved_aggregations =
        resolve_expressions_with_schema(aggregations, available, None, None, None, false)?;

    // Compute output columns for aggregate pipe - only the aggregations
    let mut output_columns = Vec::new();
    for (idx, expr) in resolved_aggregations.iter().enumerate() {
        if let Some(col) = extract_provided_column_from_domain_expr(expr, available, idx) {
            output_columns.push(col);
        }
    }

    let resolved_op = ast_resolved::UnaryRelationalOperator::AggregatePipe {
        aggregations: resolved_aggregations,
    };

    Ok((resolved_op, output_columns))
}

/// Extract all Lvar names from a resolved expression (recursing into concat chains).
fn extract_lvar_names_from_expr(expr: &ast_resolved::DomainExpression) -> Vec<String> {
    match expr {
        ast_resolved::DomainExpression::Lvar { name, .. } => vec![name.to_string()],
        ast_resolved::DomainExpression::Function(func) => match func {
            ast_resolved::FunctionExpression::Infix { left, right, .. } => {
                let mut names = extract_lvar_names_from_expr(left);
                names.extend(extract_lvar_names_from_expr(right));
                names
            }
            ast_resolved::FunctionExpression::Regular { arguments, .. }
            | ast_resolved::FunctionExpression::Curried { arguments, .. }
            | ast_resolved::FunctionExpression::Bracket { arguments, .. } => arguments
                .iter()
                .flat_map(extract_lvar_names_from_expr)
                .collect(),
            ast_resolved::FunctionExpression::StringTemplate { parts, .. } => parts
                .iter()
                .filter_map(|part| {
                    if let ast_resolved::StringTemplatePart::Interpolation(expr) = part {
                        Some(extract_lvar_names_from_expr(expr))
                    } else {
                        None
                    }
                })
                .flatten()
                .collect(),
            ast_resolved::FunctionExpression::HigherOrder {
                curried_arguments,
                regular_arguments,
                ..
            } => {
                let mut names: Vec<String> = curried_arguments
                    .iter()
                    .flat_map(extract_lvar_names_from_expr)
                    .collect();
                names.extend(
                    regular_arguments
                        .iter()
                        .flat_map(extract_lvar_names_from_expr),
                );
                names
            }
            ast_resolved::FunctionExpression::Lambda { body, .. } => {
                extract_lvar_names_from_expr(body)
            }
            ast_resolved::FunctionExpression::CaseExpression { arms, .. } => arms
                .iter()
                .flat_map(|arm| match arm {
                    ast_resolved::CaseArm::Simple {
                        test_expr, result, ..
                    } => {
                        let mut names = extract_lvar_names_from_expr(test_expr);
                        names.extend(extract_lvar_names_from_expr(result));
                        names
                    }
                    ast_resolved::CaseArm::CurriedSimple { result, .. } => {
                        extract_lvar_names_from_expr(result)
                    }
                    ast_resolved::CaseArm::Searched { result, .. } => {
                        extract_lvar_names_from_expr(result)
                    }
                    ast_resolved::CaseArm::Default { result } => {
                        extract_lvar_names_from_expr(result)
                    }
                })
                .collect(),
            ast_resolved::FunctionExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } => {
                let mut names: Vec<String> = arguments
                    .iter()
                    .flat_map(extract_lvar_names_from_expr)
                    .collect();
                names.extend(partition_by.iter().flat_map(extract_lvar_names_from_expr));
                names.extend(
                    order_by
                        .iter()
                        .flat_map(|spec| extract_lvar_names_from_expr(&spec.column)),
                );
                names
            }
            ast_resolved::FunctionExpression::JsonPath { source, .. } => {
                extract_lvar_names_from_expr(source)
            }
            ast_resolved::FunctionExpression::Curly { .. }
            | ast_resolved::FunctionExpression::MetadataTreeGroup { .. }
            | ast_resolved::FunctionExpression::Array { .. } => vec![],
        },
        ast_resolved::DomainExpression::Parenthesized { inner, .. } => {
            extract_lvar_names_from_expr(inner)
        }
        ast_resolved::DomainExpression::PipedExpression { value, .. } => {
            extract_lvar_names_from_expr(value)
        }
        ast_resolved::DomainExpression::Tuple { elements, .. } => elements
            .iter()
            .flat_map(extract_lvar_names_from_expr)
            .collect(),
        ast_resolved::DomainExpression::Literal { .. }
        | ast_resolved::DomainExpression::Projection(_)
        | ast_resolved::DomainExpression::NonUnifiyingUnderscore
        | ast_resolved::DomainExpression::ValuePlaceholder { .. }
        | ast_resolved::DomainExpression::Substitution(_)
        | ast_resolved::DomainExpression::ColumnOrdinal(_)
        | ast_resolved::DomainExpression::PivotOf { .. }
        | ast_resolved::DomainExpression::ScalarSubquery { .. }
        | ast_resolved::DomainExpression::Predicate { .. } => vec![],
    }
}

/// Evaluate a concat-chain expression by substituting Lvar references with literal values.
/// Used to expand format-string pivot keys like :"{subject}_grade" into concrete names.
fn expand_concat_template(
    expr: &ast_resolved::DomainExpression,
    substitutions: &std::collections::HashMap<String, String>,
) -> String {
    match expr {
        ast_resolved::DomainExpression::Lvar { name, .. } => substitutions
            .get(name.as_str())
            .cloned()
            .unwrap_or_else(|| name.to_string()),
        ast_resolved::DomainExpression::Literal { value, .. } => match value {
            ast_resolved::LiteralValue::String(s) => s.clone(),
            other => other.to_string(),
        },
        ast_resolved::DomainExpression::Function(func) => match func {
            ast_resolved::FunctionExpression::Infix {
                operator,
                left,
                right,
                ..
            } if operator == "concat" => {
                let l = expand_concat_template(left, substitutions);
                let r = expand_concat_template(right, substitutions);
                format!("{}{}", l, r)
            }
            other => panic!(
                "catch-all hit in aggregation.rs expand_concat_template (FunctionExpression): {:?}",
                other
            ),
        },
        other => panic!(
            "catch-all hit in aggregation.rs expand_concat_template (DomainExpression): {:?}",
            other
        ),
    }
}

/// Capture interior schemas for tree group columns in modulo output.
///
/// Scans reducing_on expressions for Curly functions (tree groups) with aliases,
/// extracts their member schemas, and attaches them to the corresponding output columns.
/// This enables InteriorDrillDown to know the schema of interior relations.
fn capture_interior_schemas(
    reducing_on: &[ast_resolved::DomainExpression],
    output: &mut [ast_resolved::ColumnMetadata],
) {
    for expr in reducing_on {
        if let Some((alias, schema)) = extract_interior_schema_from_expr(expr) {
            // Find the output column with this alias and attach the schema
            for col in output.iter_mut() {
                if crate::pipeline::resolver::col_name_eq(col.name(), &alias) {
                    col.interior_schema = Some(schema.clone());
                    break;
                }
            }
        }
    }
}

/// Extract interior schema from a tree group expression.
/// Returns (alias, schema) if the expression is a Curly function with members.
fn extract_interior_schema_from_expr(
    expr: &ast_resolved::DomainExpression,
) -> Option<(
    String,
    Vec<crate::pipeline::asts::core::operators::InteriorColumnDef>,
)> {
    match expr {
        ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Curly {
            alias,
            ..
        }) => {
            let alias = alias.as_ref()?;
            let schema = extract_curly_members_schema(expr)?;
            Some((alias.to_string(), schema))
        }
        // Non-tree-group expressions: regular functions (count, sum, etc.), columns,
        // literals, etc. These don't have interior schemas.
        ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Regular {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Curried {
            ..
        })
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::HigherOrder { .. },
        )
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Bracket {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Infix {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Lambda {
            ..
        })
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::StringTemplate { .. },
        )
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::CaseExpression { .. },
        )
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Window {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Array {
            ..
        })
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::MetadataTreeGroup { .. },
        )
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::JsonPath {
            ..
        }) => None,
        ast_resolved::DomainExpression::Lvar { .. }
        | ast_resolved::DomainExpression::Literal { .. }
        | ast_resolved::DomainExpression::Projection(_)
        | ast_resolved::DomainExpression::NonUnifiyingUnderscore
        | ast_resolved::DomainExpression::ValuePlaceholder { .. }
        | ast_resolved::DomainExpression::Substitution(_)
        | ast_resolved::DomainExpression::Predicate { .. }
        | ast_resolved::DomainExpression::PipedExpression { .. }
        | ast_resolved::DomainExpression::Parenthesized { .. }
        | ast_resolved::DomainExpression::Tuple { .. }
        | ast_resolved::DomainExpression::ColumnOrdinal(_)
        | ast_resolved::DomainExpression::ScalarSubquery { .. }
        | ast_resolved::DomainExpression::PivotOf { .. } => None,
    }
}

/// Extract the member schema from a Curly expression without requiring an alias.
/// Used both for top-level (via extract_interior_schema_from_expr) and for
/// nested tree groups where the inner ~> {} has no alias.
fn extract_curly_members_schema(
    expr: &ast_resolved::DomainExpression,
) -> Option<Vec<crate::pipeline::asts::core::operators::InteriorColumnDef>> {
    use crate::pipeline::asts::core::operators::InteriorColumnDef;

    match expr {
        ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Curly {
            members,
            ..
        }) => {
            let mut schema = Vec::new();
            for member in members {
                match member {
                    ast_resolved::CurlyMember::Shorthand { column, .. } => {
                        schema.push(InteriorColumnDef {
                            name: column.to_string(),
                            child_interior: None,
                        });
                    }
                    ast_resolved::CurlyMember::KeyValue {
                        key,
                        value,
                        nested_reduction,
                        ..
                    } => {
                        if *nested_reduction {
                            let child = extract_curly_members_schema(value);
                            schema.push(InteriorColumnDef {
                                name: key.clone(),
                                child_interior: child,
                            });
                        } else {
                            schema.push(InteriorColumnDef {
                                name: key.clone(),
                                child_interior: None,
                            });
                        }
                    }
                    ast_resolved::CurlyMember::Placeholder => {}
                    ast_resolved::CurlyMember::PathLiteral {
                        alias: path_alias, ..
                    } => {
                        if let Some(a) = path_alias {
                            schema.push(InteriorColumnDef {
                                name: a.to_string(),
                                child_interior: None,
                            });
                        }
                    }
                    // Comparison: {country="USA"} — filter predicate, not a column definition.
                    // Glob/Pattern/OrdinalRange: ergonomic inductors, resolved before here.
                    ast_resolved::CurlyMember::Comparison { .. }
                    | ast_resolved::CurlyMember::Glob
                    | ast_resolved::CurlyMember::Pattern { .. }
                    | ast_resolved::CurlyMember::OrdinalRange { .. } => {}
                }
            }
            Some(schema)
        }
        // Non-Curly expressions: the nested reduction value isn't another tree group.
        // No child interior schema to extract. Scalar reductions (count, sum, etc.)
        // and column references produce leaf values, not nested JSON objects.
        ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Regular {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Curried {
            ..
        })
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::HigherOrder { .. },
        )
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Bracket {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Infix {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Lambda {
            ..
        })
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::StringTemplate { .. },
        )
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::CaseExpression { .. },
        )
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Window {
            ..
        })
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::Array {
            ..
        })
        | ast_resolved::DomainExpression::Function(
            ast_resolved::FunctionExpression::MetadataTreeGroup { .. },
        )
        | ast_resolved::DomainExpression::Function(ast_resolved::FunctionExpression::JsonPath {
            ..
        }) => None,
        ast_resolved::DomainExpression::Lvar { .. }
        | ast_resolved::DomainExpression::Literal { .. }
        | ast_resolved::DomainExpression::Projection(_)
        | ast_resolved::DomainExpression::NonUnifiyingUnderscore
        | ast_resolved::DomainExpression::ValuePlaceholder { .. }
        | ast_resolved::DomainExpression::Substitution(_)
        | ast_resolved::DomainExpression::Predicate { .. }
        | ast_resolved::DomainExpression::PipedExpression { .. }
        | ast_resolved::DomainExpression::Parenthesized { .. }
        | ast_resolved::DomainExpression::Tuple { .. }
        | ast_resolved::DomainExpression::ColumnOrdinal(_)
        | ast_resolved::DomainExpression::ScalarSubquery { .. }
        | ast_resolved::DomainExpression::PivotOf { .. } => None,
    }
}
