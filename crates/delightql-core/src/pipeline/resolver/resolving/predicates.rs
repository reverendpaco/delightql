use crate::ddl::ddl_builder;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::ProjectionExpr;
use crate::pipeline::asts::ddl::DdlHead;
use crate::pipeline::resolver::grounding::substitute_in_domain_expr;
use std::collections::HashMap;

/// Desugar IN operator to InnerExists with anonymous table
/// EPOCH 5: Supports both single-column and tuple IN
/// Transforms: value in (val1; val2) → +_(value @ val1; val2) as InnerExists
/// Transforms: (c1, c2) in (v1, v2; v3, v4) → +_(c1, c2 @ v1, v2; v3, v4) as InnerExists
pub(in crate::pipeline::resolver) fn desugar_in_to_anonymous(
    resolved_value: ast_resolved::DomainExpression,
    resolved_set: Vec<ast_resolved::DomainExpression>,
    negated: bool,
) -> ast_resolved::BooleanExpression {
    // Unwrap tuple if needed - extract header columns
    let header_columns: Vec<ast_resolved::DomainExpression> = match resolved_value {
        ast_resolved::DomainExpression::Tuple { elements, .. } => elements,
        single_expr => vec![single_expr],
    };

    // Create rows from the set values
    // Each set element can be either a single value or a tuple
    let rows: Vec<ast_resolved::Row> = resolved_set
        .into_iter()
        .map(|expr| {
            // Unwrap tuple if needed - extract row values
            let row_values = match expr {
                ast_resolved::DomainExpression::Tuple { elements, .. } => elements,
                single_expr => vec![single_expr],
            };
            ast_resolved::Row { values: row_values }
        })
        .collect();

    // Create the anonymous table relation
    let anon_table =
        ast_resolved::RelationalExpression::Relation(ast_resolved::Relation::Anonymous {
            column_headers: Some(header_columns),
            rows,
            alias: None,
            outer: false,
            exists_mode: true,
            qua_target: None,
            cpr_schema: ast_resolved::PhaseBox::new(ast_resolved::CprSchema::Unknown),
        });

    ast_resolved::BooleanExpression::InnerExists {
        exists: !negated,
        identifier: ast_resolved::QualifiedName {
            namespace_path: crate::pipeline::asts::resolved::NamespacePath::empty(),
            name: "_".into(),
            grounding: None,
        },
        subquery: Box::new(anon_table),
        alias: None,
        using_columns: vec![],
    }
}

// =============================================================================
// USING correlation synthesis for semi-joins
// =============================================================================

/// Wrap a resolved subquery with correlation predicates derived from USING columns.
/// For `+orders(*.(status))`, this produces:
///   Filter(subquery, outer.status IS NOT DISTINCT FROM orders.status)
pub(in crate::pipeline::resolver) fn synthesize_using_correlation(
    subquery: ast_resolved::RelationalExpression,
    using_columns: &[String],
    inner_identifier: &ast_resolved::QualifiedName,
    outer_available: &[ast_resolved::ColumnMetadata],
) -> ast_resolved::RelationalExpression {
    use crate::pipeline::asts::core::metadata::TableName;
    use crate::pipeline::asts::core::FilterOrigin;

    if using_columns.is_empty() {
        return subquery;
    }

    let inner_table: delightql_types::SqlIdentifier = inner_identifier.name.clone();

    // Build one comparison per USING column
    let mut comparisons: Vec<ast_resolved::BooleanExpression> = Vec::new();
    for col_name in using_columns {
        let col_id: delightql_types::SqlIdentifier = col_name.as_str().into();

        // Find outer qualifier from the available schema
        let outer_qualifier: Option<delightql_types::SqlIdentifier> = outer_available
            .iter()
            .find(|cm| cm.info.name().map_or(false, |n| n == col_name))
            .and_then(|cm| match &cm.fq_table.name {
                TableName::Named(id) => Some(id.clone()),
                TableName::Fresh => None,
            });

        let lhs = ast_resolved::DomainExpression::Lvar {
            name: col_id.clone(),
            qualifier: outer_qualifier,
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: None,
            provenance: ast_resolved::PhaseBox::phantom(),
        };
        let rhs = ast_resolved::DomainExpression::Lvar {
            name: col_id,
            qualifier: Some(inner_table.clone()),
            namespace_path: ast_resolved::NamespacePath::empty(),
            alias: None,
            provenance: ast_resolved::PhaseBox::phantom(),
        };

        comparisons.push(ast_resolved::BooleanExpression::Comparison {
            operator: "null_safe_eq".to_string(),
            left: Box::new(lhs),
            right: Box::new(rhs),
        });
    }

    // Combine with AND
    let combined = comparisons
        .into_iter()
        .reduce(|acc, next| ast_resolved::BooleanExpression::And {
            left: Box::new(acc),
            right: Box::new(next),
        })
        .unwrap(); // safe: using_columns is non-empty

    // Wrap subquery in Filter
    ast_resolved::RelationalExpression::Filter {
        source: Box::new(subquery),
        condition: ast_resolved::SigmaCondition::Predicate(combined),
        origin: FilterOrigin::Generated,
        cpr_schema: ast_resolved::PhaseBox::phantom(),
    }
}

/// Build individual correlation SigmaConditions from USING columns.
/// Returns one SigmaCondition per column (not combined with AND), so that
/// `insert_filter_at_base` can wrap them as separate Filter nodes.
/// This matches the structure the explicit comma path produces, which the
/// CDT-SJ classifier and hygienic injection mechanism expect.
pub(in crate::pipeline::resolver) fn build_using_correlation_filters(
    using_columns: &[String],
    inner_identifier: &ast_resolved::QualifiedName,
    outer_available: &[ast_resolved::ColumnMetadata],
) -> Vec<ast_resolved::SigmaCondition> {
    use crate::pipeline::asts::core::metadata::TableName;

    let inner_table: delightql_types::SqlIdentifier = inner_identifier.name.clone();

    using_columns
        .iter()
        .map(|col_name| {
            let col_id: delightql_types::SqlIdentifier = col_name.as_str().into();

            let outer_qualifier: Option<delightql_types::SqlIdentifier> = outer_available
                .iter()
                .find(|cm| cm.info.name().map_or(false, |n| n == col_name))
                .and_then(|cm| match &cm.fq_table.name {
                    TableName::Named(id) => Some(id.clone()),
                    TableName::Fresh => None,
                });

            let lhs = ast_resolved::DomainExpression::Lvar {
                name: col_id.clone(),
                qualifier: outer_qualifier,
                namespace_path: ast_resolved::NamespacePath::empty(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            };
            let rhs = ast_resolved::DomainExpression::Lvar {
                name: col_id,
                qualifier: Some(inner_table.clone()),
                namespace_path: ast_resolved::NamespacePath::empty(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            };

            ast_resolved::SigmaCondition::Predicate(ast_resolved::BooleanExpression::Comparison {
                operator: "null_safe_eq".to_string(),
                left: Box::new(lhs),
                right: Box::new(rhs),
            })
        })
        .collect()
}

// =============================================================================
// Destructuring Pattern Helpers (Epoch 2)
// =============================================================================

/// Extract JSON key → column name mappings from an UNRESOLVED destructuring pattern
/// This doesn't resolve identifiers - it treats them as literal output column names
pub(in crate::pipeline::resolver) fn extract_key_mappings_from_unresolved_pattern(
    pattern: &ast_unresolved::FunctionExpression,
) -> Result<Vec<ast_resolved::DestructureMapping>> {
    match pattern {
        // METADATA TG: country:~> {first_name, last_name}
        // Creates column from JSON keys + recursively extract from constructor
        ast_unresolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            constructor,
            ..
        } => {
            let mut mappings = Vec::new();

            // The key_column captures JSON keys as data
            mappings.push(ast_resolved::DestructureMapping {
                json_key: key_column.to_string(),
                column_name: key_column.to_string(),
            });

            // Recursively extract mappings from nested constructor
            let nested_mappings =
                extract_key_mappings_from_unresolved_pattern(constructor.as_ref())?;
            mappings.extend(nested_mappings);

            Ok(mappings)
        }

        ast_unresolved::FunctionExpression::Curly { members, .. } => {
            let mut mappings = Vec::new();
            for member in members {
                match member {
                    ast_unresolved::CurlyMember::Shorthand { column, .. } => {
                        // Shorthand: {first_name}
                        // JSON key = column name
                        mappings.push(ast_resolved::DestructureMapping {
                            json_key: column.to_string(),
                            column_name: column.to_string(),
                        });
                    }
                    ast_unresolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => {
                        if *nested_reduction {
                            // Nested: "key": ~> {pattern} OR Aggregate TVar: "key": ~> identifier
                            match &**value {
                                // Aggregate TVar: "users": ~> sub_users
                                ast_unresolved::DomainExpression::Lvar { name, .. } => {
                                    mappings.push(ast_resolved::DestructureMapping {
                                        json_key: key.clone(),
                                        column_name: name.to_string(),
                                    });
                                }

                                // Nested explosion: "users": ~> {first_name}
                                ast_unresolved::DomainExpression::Function(nested_func) => {
                                    mappings.extend(extract_key_mappings_from_unresolved_pattern(
                                        nested_func,
                                    )?);
                                }
                                other => {
                                    panic!("catch-all hit in predicates.rs extract_key_mappings_from_unresolved_pattern (nested_reduction value): {:?}", other);
                                }
                            }
                        } else {
                            // KeyValue without ~>: Either simple mapping OR nested object
                            match &**value {
                                // Simple mapping: "first_name": fname
                                ast_unresolved::DomainExpression::Lvar { name, .. } => {
                                    mappings.push(ast_resolved::DestructureMapping {
                                        json_key: key.clone(),
                                        column_name: name.to_string(),
                                    });
                                }

                                // Nested object: "location": {country, city}
                                // RECURSE into nested pattern
                                ast_unresolved::DomainExpression::Function(
                                    ast_unresolved::FunctionExpression::Curly { .. },
                                ) => {
                                    if let ast_unresolved::DomainExpression::Function(nested_func) =
                                        &**value
                                    {
                                        let nested_mappings =
                                            extract_key_mappings_from_unresolved_pattern(
                                                nested_func,
                                            )?;
                                        mappings.extend(nested_mappings);
                                    }
                                }

                                _ => {
                                    return Err(DelightQLError::validation_error(
                                        format!(
                                            "Explicit key mapping requires simple identifier or nested object pattern as value.\n\
                                             Found: {{\"{}\":  <complex expression>}}\n\
                                             Expected: {{\"{}\":  column_name}} or {{\"{}\":  {{nested_pattern}}}}",
                                            key, key, key
                                        ),
                                        "destructuring_pattern"
                                    ));
                                }
                            }
                        }
                    }
                    // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
                    ast_unresolved::CurlyMember::PathLiteral { path, alias } => {
                        // Path literals in destructuring extract to the alias or inferred name
                        let column_name: String = if let Some(alias_name) = alias {
                            alias_name.to_string()
                        } else {
                            extract_column_name_from_path_literal(path.as_ref())?
                        };

                        mappings.push(ast_resolved::DestructureMapping {
                            json_key: column_name.clone(),
                            column_name,
                        });
                    }
                    ast_unresolved::CurlyMember::Glob
                    | ast_unresolved::CurlyMember::Pattern { .. }
                    | ast_unresolved::CurlyMember::OrdinalRange { .. } => {
                        return Err(DelightQLError::parse_error(
                            "Ergonomic inductors (*,  /pattern/, |range|) not supported in destructuring patterns"
                        ));
                    }
                    ast_unresolved::CurlyMember::Comparison { .. } => {
                        return Err(DelightQLError::parse_error(
                            "Comparison shorthand not supported in destructuring patterns",
                        ));
                    }
                    // Placeholder {_} in destructuring means "explode but don't extract fields"
                    // No mapping is created - just skip it
                    ast_unresolved::CurlyMember::Placeholder => {
                        // Skip - no mapping extracted for placeholder
                    }
                }
            }
            Ok(mappings)
        }
        ast_unresolved::FunctionExpression::Array { members, .. } => {
            // ARRAY DESTRUCTURING: Epoch 4 - Extract mappings from array pattern
            let mut mappings = Vec::new();
            for member in members {
                match member {
                    ast_unresolved::ArrayMember::Index { path, alias } => {
                        let (json_key, column_name) = match path.as_ref() {
                            ast_unresolved::DomainExpression::Projection(
                                ProjectionExpr::JsonPathLiteral { segments, .. },
                            ) => {
                                if segments.is_empty() {
                                    return Err(DelightQLError::parse_error(
                                        "Array destructuring path cannot be empty",
                                    ));
                                }

                                if !matches!(segments.first(), Some(crate::pipeline::asts::core::expressions::functions::PathSegment::ArrayIndex(_))) {
                                    return Err(DelightQLError::parse_error(
                                        "Array destructuring requires path starting with numeric index: [.0, .1, .2]"
                                    ));
                                }

                                let json_key = segments.iter()
                                    .map(|seg| match seg {
                                        crate::pipeline::asts::core::expressions::functions::PathSegment::ObjectKey(key) => key.clone(),
                                        crate::pipeline::asts::core::expressions::functions::PathSegment::ArrayIndex(idx) => idx.to_string(),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(".");

                                let column_name: String = alias.as_ref().map(|s| s.to_string()).unwrap_or_else(|| {
                                    segments.iter()
                                        .map(|seg| match seg {
                                            crate::pipeline::asts::core::expressions::functions::PathSegment::ObjectKey(key) => key.clone(),
                                            crate::pipeline::asts::core::expressions::functions::PathSegment::ArrayIndex(idx) => idx.to_string(),
                                        })
                                        .collect::<Vec<_>>()
                                        .join("_")
                                });

                                (json_key, column_name)
                            }
                            _ => {
                                return Err(DelightQLError::parse_error(
                                    "Array destructuring members must be path literals",
                                ));
                            }
                        };

                        mappings.push(ast_resolved::DestructureMapping {
                            json_key,
                            column_name: column_name.to_string(),
                        });
                    }
                }
            }
            Ok(mappings)
        }
        _ => Err(DelightQLError::parse_error(
            "Pattern must be a Curly function or Array pattern",
        )),
    }
}

/// Validate UNRESOLVED pattern is appropriate for the destructuring mode
pub(in crate::pipeline::resolver) fn validate_unresolved_pattern_for_mode(
    _pattern: &ast_unresolved::FunctionExpression,
    mode: &ast_unresolved::DestructureMode,
) -> Result<()> {
    use ast_unresolved::DestructureMode;

    match mode {
        DestructureMode::Scalar => {
            // Scalar mode WITH nested explosions is allowed
        }
        DestructureMode::Aggregate => {
            // Aggregate mode - nested explosions are allowed
        }
    }
    Ok(())
}

/// EPOCH 5: Validate no sibling explosions (multiple ~> at same pattern level)
/// Sibling explosions create ambiguous cartesian products
pub(in crate::pipeline::resolver) fn validate_no_sibling_explosions(pattern: &ast_unresolved::FunctionExpression) -> Result<()> {
    match pattern {
        ast_unresolved::FunctionExpression::MetadataTreeGroup { constructor, .. } => {
            // Recurse into the nested constructor
            validate_no_sibling_explosions(constructor.as_ref())?;
            Ok(())
        }

        ast_unresolved::FunctionExpression::Curly { members, .. } => {
            // Count how many members have nested_reduction: true at THIS level
            let explosion_count = members
                .iter()
                .filter(|m| {
                    matches!(
                        m,
                        ast_unresolved::CurlyMember::KeyValue {
                            nested_reduction: true,
                            ..
                        }
                    )
                })
                .count();

            if explosion_count > 1 {
                return Err(DelightQLError::validation_error(
                    "Multiple array explosions (~>) at the same pattern level create ambiguous cartesian product.\n\
                     Use sequential steps instead:\n\
                     Example:\n\
                     - Step 1: data ~= ~> {{\"users\": users_data, \"orders\": orders_data}}\n\
                     - Step 2: users_data ~= ~> {{first_name}}",
                    "destructuring"
                ));
            }

            // Recurse into nested patterns to check all depths
            for member in members {
                match member {
                    ast_unresolved::CurlyMember::KeyValue {
                        value,
                        nested_reduction,
                        ..
                    } => {
                        if *nested_reduction {
                            if let ast_unresolved::DomainExpression::Function(nested_func) =
                                &**value
                            {
                                validate_no_sibling_explosions(nested_func)?;
                            }
                        } else {
                            if let ast_unresolved::DomainExpression::Function(nested_func) =
                                &**value
                            {
                                validate_no_sibling_explosions(nested_func)?;
                            }
                        }
                    }
                    ast_unresolved::CurlyMember::Shorthand { .. }
                    | ast_unresolved::CurlyMember::Comparison { .. }
                    | ast_unresolved::CurlyMember::Glob { .. }
                    | ast_unresolved::CurlyMember::Pattern { .. }
                    | ast_unresolved::CurlyMember::OrdinalRange { .. }
                    | ast_unresolved::CurlyMember::Placeholder { .. }
                    | ast_unresolved::CurlyMember::PathLiteral { .. } => {}
                }
            }

            Ok(())
        }

        // Array destructuring: positional, no sibling explosion concept
        ast_unresolved::FunctionExpression::Array { .. } => Ok(()),
        // All other function types: not destructuring patterns, no explosion validation needed
        ast_unresolved::FunctionExpression::Regular { .. }
        | ast_unresolved::FunctionExpression::Curried { .. }
        | ast_unresolved::FunctionExpression::Bracket { .. }
        | ast_unresolved::FunctionExpression::Infix { .. }
        | ast_unresolved::FunctionExpression::HigherOrder { .. }
        | ast_unresolved::FunctionExpression::Lambda { .. }
        | ast_unresolved::FunctionExpression::StringTemplate { .. }
        | ast_unresolved::FunctionExpression::CaseExpression { .. }
        | ast_unresolved::FunctionExpression::Window { .. }
        | ast_unresolved::FunctionExpression::JsonPath { .. } => Ok(()),
    }
}

/// Convert unresolved destructuring pattern to resolved WITHOUT actually resolving
/// This is just a structural type conversion for destructuring patterns
pub(in crate::pipeline::resolver) fn convert_destructure_pattern_to_resolved(
    pattern: ast_unresolved::FunctionExpression,
) -> Result<ast_resolved::FunctionExpression> {
    match pattern {
        ast_unresolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            keys_only,
            cte_requirements: _cte_requirements,
            alias,
        } => {
            // Convert the nested constructor pattern
            let resolved_constructor = convert_destructure_pattern_to_resolved(*constructor)?;

            Ok(ast_resolved::FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier,
                key_schema,
                constructor: Box::new(resolved_constructor),
                keys_only,
                cte_requirements: None, // None for destructuring
                alias,
            })
        }

        ast_unresolved::FunctionExpression::Curly {
            members,
            inner_grouping_keys: _,
            cte_requirements: _,
            alias,
        } => {
            let resolved_members: Result<Vec<_>> = members
                .into_iter()
                .map(|member| match member {
                    ast_unresolved::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    } => Ok(ast_resolved::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    }),
                    ast_unresolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => {
                        let resolved_value = convert_unresolved_domain_to_resolved(*value)?;
                        Ok(ast_resolved::CurlyMember::KeyValue {
                            key,
                            nested_reduction,
                            value: Box::new(resolved_value),
                        })
                    }

                    // Placeholder {_} in destructuring means "explode but don't extract fields"
                    ast_unresolved::CurlyMember::Placeholder => {
                        Ok(ast_resolved::CurlyMember::Placeholder)
                    }

                    // PATH FIRST-CLASS: Epoch 5 - PathLiteral in destructuring
                    ast_unresolved::CurlyMember::PathLiteral { path, alias } => {
                        let resolved_path = convert_unresolved_domain_to_resolved(*path)?;
                        Ok(ast_resolved::CurlyMember::PathLiteral {
                            path: Box::new(resolved_path),
                            alias,
                        })
                    }

                    _ => Err(DelightQLError::parse_error(
                        "Only Shorthand, KeyValue, PathLiteral, and Placeholder allowed in destructuring patterns",
                    )),
                })
                .collect();

            Ok(ast_resolved::FunctionExpression::Curly {
                members: resolved_members?,
                inner_grouping_keys: vec![], // Empty for destructuring
                cte_requirements: None,      // None for destructuring
                alias,
            })
        }

        ast_unresolved::FunctionExpression::Array { members, alias } => {
            // ARRAY DESTRUCTURING: Epoch 4 - Convert array pattern to resolved
            let resolved_members: Result<Vec<_>> = members
                .into_iter()
                .map(|member| match member {
                    ast_unresolved::ArrayMember::Index { path, alias } => {
                        let resolved_path = convert_unresolved_domain_to_resolved(*path)?;
                        Ok(ast_resolved::ArrayMember::Index {
                            path: Box::new(resolved_path),
                            alias,
                        })
                    }
                })
                .collect();

            Ok(ast_resolved::FunctionExpression::Array {
                members: resolved_members?,
                alias,
            })
        }

        _ => Err(DelightQLError::parse_error(
            "Destructuring pattern must be Curly or Array",
        )),
    }
}

/// Convert unresolved domain expression to resolved WITHOUT actually resolving
/// This is just a structural type conversion for destructuring patterns
fn convert_unresolved_domain_to_resolved(
    expr: ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance,
        } => {
            // In destructuring, this is just an output column name
            Ok(ast_resolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: provenance.into(), // Convert Unresolved PhaseBox to Resolved
            })
        }
        ast_unresolved::DomainExpression::Function(f) => {
            let resolved_func = convert_destructure_pattern_to_resolved(f)?;
            Ok(ast_resolved::DomainExpression::Function(resolved_func))
        }
        // PATH FIRST-CLASS: Epoch 5 - JsonPathLiteral in destructuring
        ast_unresolved::DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
            segments,
            root_is_array,
            alias,
        }) => Ok(ast_resolved::DomainExpression::Projection(
            ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array,
                alias,
            },
        )),
        _ => Err(DelightQLError::parse_error(
            "Only Lvar, Function, and JsonPathLiteral allowed in destructuring pattern values",
        )),
    }
}

/// Extract column name from a path literal for destructuring
fn extract_column_name_from_path_literal(
    path_expr: &ast_unresolved::DomainExpression,
) -> Result<String> {
    match path_expr {
        ast_unresolved::DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
            segments,
            ..
        }) => {
            use crate::pipeline::asts::core::expressions::functions::PathSegment;

            let column_name = segments
                .iter()
                .map(|seg| match seg {
                    PathSegment::ObjectKey(key) => key.clone(),
                    PathSegment::ArrayIndex(idx) => idx.to_string(),
                })
                .collect::<Vec<_>>()
                .join("_");

            if column_name.is_empty() {
                return Err(DelightQLError::parse_error(
                    "Path literal must have at least one segment",
                ));
            }

            Ok(column_name)
        }
        _ => Err(DelightQLError::parse_error(
            "PathLiteral in destructuring must contain a JsonPathLiteral expression",
        )),
    }
}

/// Expand a consulted sigma predicate into an OR'd boolean expression.
pub(in crate::pipeline::resolver) fn expand_consulted_sigma(
    definition: &str,
    functor: &str,
    arguments: Vec<ast_unresolved::DomainExpression>,
    exists: bool,
) -> Result<ast_unresolved::BooleanExpression> {
    let ddl_defs = ddl_builder::build_ddl_file(definition)?;
    if ddl_defs.is_empty() {
        return Err(DelightQLError::parse_error(format!(
            "No definitions found for sigma predicate '{}'",
            functor
        )));
    }

    let mut clause_booleans: Vec<ast_unresolved::BooleanExpression> = Vec::new();

    for clause in &ddl_defs {
        let params = match &clause.head {
            DdlHead::SigmaPredicate { params } => params,
            _ => {
                return Err(DelightQLError::parse_error(format!(
                    "Expected sigma predicate definition for '{}', got {:?}",
                    functor, clause.head
                )));
            }
        };

        // Validate arity
        if params.len() != arguments.len() {
            return Err(DelightQLError::validation_error(
                format!(
                    "Sigma predicate '{}' expects {} arguments, got {}",
                    functor,
                    params.len(),
                    arguments.len()
                ),
                "Arity mismatch",
            ));
        }

        // Get body as DomainExpression::Predicate
        let body = clause.as_domain_expr().ok_or_else(|| {
            DelightQLError::parse_error(format!(
                "Sigma predicate '{}' clause has non-scalar body",
                functor
            ))
        })?;

        // Build param → argument substitution map
        let param_map: HashMap<&str, &ast_unresolved::DomainExpression> = params
            .iter()
            .map(|p| p.as_str())
            .zip(arguments.iter())
            .collect();

        // Substitute parameters in body
        let substituted = substitute_in_domain_expr(body.clone(), &param_map);

        // Extract the BooleanExpression from DomainExpression::Predicate
        let bool_expr = match substituted {
            ast_unresolved::DomainExpression::Predicate { expr, .. } => *expr,
            other => {
                return Err(DelightQLError::parse_error(format!(
                    "Sigma predicate '{}' body must be a boolean expression, got: {:?}",
                    functor, other
                )));
            }
        };

        clause_booleans.push(bool_expr);
    }

    // Combine all clause booleans with OR
    let combined = clause_booleans
        .into_iter()
        .reduce(|acc, next| ast_unresolved::BooleanExpression::Or {
            left: Box::new(acc),
            right: Box::new(next),
        })
        .unwrap(); // Safe: we checked ddl_defs is non-empty

    // Apply NOT for anti-join (\+)
    if exists {
        Ok(combined)
    } else {
        Ok(ast_unresolved::BooleanExpression::Not {
            expr: Box::new(combined),
        })
    }
}
