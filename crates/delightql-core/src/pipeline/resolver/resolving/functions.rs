use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};
use crate::pipeline::resolver::DatabaseSchema;
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Resolve function arguments, preserving globs inside functions
/// Passes through in_correlation flag for proper validation handling
fn resolve_function_arguments_with_context(
    arguments: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
    schema: Option<&dyn DatabaseSchema>,
    mut cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let mut resolved = Vec::new();

    for arg in arguments {
        match arg {
            ast_unresolved::DomainExpression::Projection(ProjectionExpr::Glob {
                qualifier,
                namespace_path,
                ..
            }) => {
                // Preserve Glob as-is inside functions - don't expand!
                resolved.push(ast_resolved::DomainExpression::Projection(
                    ProjectionExpr::Glob {
                        qualifier,
                        namespace_path,
                    },
                ));
            }
            other => {
                // Resolve argument with proper context
                // Manually reborrow the mutable reference
                let resolved_arg = match &mut cte_context {
                    Some(ctx) => {
                        super::domain_expressions::resolve_domain_expr_with_schema_and_context(
                            other,
                            available,
                            schema,
                            Some(*ctx),
                            in_correlation,
                            None, // TODO: Thread CFE definitions through function arguments
                            None,
                        )?
                    }
                    None => super::domain_expressions::resolve_domain_expr_with_schema_and_context(
                        other,
                        available,
                        schema,
                        None,
                        in_correlation,
                        None, // TODO: Thread CFE definitions through function arguments
                        None,
                    )?,
                };
                resolved.push(resolved_arg);
            }
        }
    }

    Ok(resolved)
}

/// Resolve function arguments, preserving globs inside functions
pub(in crate::pipeline::resolver) fn resolve_function_arguments(
    arguments: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let mut resolved = Vec::new();

    for arg in arguments {
        match arg {
            ast_unresolved::DomainExpression::Projection(ProjectionExpr::Glob {
                qualifier,
                namespace_path,
                ..
            }) => {
                // Preserve Glob as-is inside functions - don't expand!
                resolved.push(ast_resolved::DomainExpression::Projection(
                    ProjectionExpr::Glob {
                        qualifier,
                        namespace_path,
                    },
                ));
            }
            other => {
                // For non-glob arguments, use normal resolution (which will handle Lvar, Function, etc.)
                let resolved_args = super::domain_expressions::resolve_expressions_with_schema(
                    vec![other],
                    available,
                    None,
                    None,
                    None,
                    false,
                )?;
                resolved.extend(resolved_args);
            }
        }
    }

    Ok(resolved)
}

/// Resolve a function expression with available schema
pub(in crate::pipeline::resolver) fn resolve_function_with_schema(
    function: ast_unresolved::FunctionExpression,
    available: &[ast_resolved::ColumnMetadata],
    cfe_defs: Option<
        &std::collections::HashMap<
            String,
            crate::pipeline::ast_unresolved::PrecompiledCfeDefinition,
        >,
    >,
) -> Result<ast_resolved::FunctionExpression> {
    match function {
        ast_unresolved::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => {
            // Resolve arguments, but preserve Globs inside functions
            let resolved_args = resolve_function_arguments(arguments, available)?;

            // CCAFE validation: Check if this is a context-aware call and validate context params
            if let Some(cfe_definitions) = cfe_defs {
                if !resolved_args.is_empty() {
                    if let ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::ContextMarker,
                    ) = resolved_args[0]
                    {
                        // This is a context-aware call - validate context params exist
                        if let Some(cfe_def) = cfe_definitions.get(name.as_ref()) {
                            if !cfe_def.context_params.is_empty() {
                                // Extract available column names for validation
                                let available_names: std::collections::HashSet<String> =
                                    available.iter().map(|col| col.name().to_string()).collect();

                                // Check each context param
                                let mut missing_params = Vec::new();
                                for ctx_param in &cfe_def.context_params {
                                    if !available_names.contains(ctx_param) {
                                        missing_params.push(ctx_param.clone());
                                    }
                                }

                                if !missing_params.is_empty() {
                                    let context_mode = if cfe_def.allows_positional_context_call {
                                        "explicit"
                                    } else {
                                        "implicit (auto-discovered)"
                                    };

                                    return Err(crate::error::DelightQLError::ParseError {
                                        message: format!(
                                            "CFE '{}' requires context columns that don't exist in current scope.\n\
                                             \n\
                                             Missing columns: {}\n\
                                             Available columns: {}\n\
                                             \n\
                                             Context mode: {}\n\
                                             Context parameters: {}",
                                            name,
                                            missing_params.join(", "),
                                            available.iter().map(|c| c.name()).collect::<Vec<_>>().join(", "),
                                            context_mode,
                                            cfe_def.context_params.join(", ")
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // Resolve filter condition if present
            let resolved_condition = if let Some(cond) = conditioned_on {
                Some(Box::new(super::predicates::resolve_boolean_expression(
                    *cond, available,
                )?))
            } else {
                None
            };

            Ok(ast_resolved::FunctionExpression::Regular {
                name,
                namespace,
                arguments: resolved_args,
                alias,
                conditioned_on: resolved_condition,
            })
        }
        ast_unresolved::FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => {
            // Resolve arguments, but preserve Globs inside functions
            let resolved_args = resolve_function_arguments(arguments, available)?;

            // Resolve filter condition if present
            let resolved_condition = if let Some(cond) = conditioned_on {
                Some(Box::new(super::predicates::resolve_boolean_expression(
                    *cond, available,
                )?))
            } else {
                None
            };

            Ok(ast_resolved::FunctionExpression::Curried {
                name,
                namespace,
                arguments: resolved_args,
                conditioned_on: resolved_condition,
            })
        }
        ast_unresolved::FunctionExpression::Bracket { arguments, alias } => {
            // TG-ERGONOMIC-INDUCTOR: Bracket tree groups - expand globs, patterns, and ranges with de-duplication
            use crate::pipeline::asts::{resolved, unresolved};
            use std::collections::HashSet;

            // Track seen columns for de-duplication (first occurrence wins)
            let mut seen_columns = HashSet::new();
            let mut resolved_args: Vec<resolved::DomainExpression> = Vec::new();

            for arg in arguments {
                match arg {
                    unresolved::DomainExpression::Projection(ProjectionExpr::Glob {
                        qualifier,
                        namespace_path,
                    }) => {
                        // Expand to all available columns
                        for col in available {
                            let column_name = col.name().to_string();
                            if seen_columns.insert(column_name.clone()) {
                                let col_qualifier = match &col.fq_table.name {
                                    ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                        Some(name.to_string())
                                    }
                                    ast_resolved::TableName::Named(_)
                                    | ast_resolved::TableName::Fresh => None,
                                };
                                resolved_args.push(resolved::DomainExpression::Lvar {
                                    name: column_name.into(),
                                    qualifier: qualifier
                                        .clone()
                                        .or(col_qualifier.map(|s| s.into())),
                                    namespace_path: if namespace_path.is_empty() {
                                        col.fq_table.parents_path.clone()
                                    } else {
                                        namespace_path.clone()
                                    },
                                    alias: None,
                                    provenance: ast_resolved::PhaseBox::phantom(),
                                });
                            }
                        }
                    }
                    unresolved::DomainExpression::Projection(ProjectionExpr::Pattern {
                        pattern,
                        alias: _,
                    }) => {
                        // Expand to pattern-matched columns
                        use crate::pipeline::pattern::bre_to_rust_regex;
                        let regex_pattern = bre_to_rust_regex(&pattern)?;
                        let re = regex::Regex::new(&regex_pattern).map_err(|e| {
                            DelightQLError::parse_error(format!("Invalid column pattern: {}", e))
                        })?;

                        for col in available.iter().filter(|col| re.is_match(col.name())) {
                            let column_name = col.name().to_string();
                            if seen_columns.insert(column_name.clone()) {
                                let qualifier = match &col.fq_table.name {
                                    ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                        Some(name.to_string())
                                    }
                                    ast_resolved::TableName::Named(_)
                                    | ast_resolved::TableName::Fresh => None,
                                };
                                resolved_args.push(resolved::DomainExpression::Lvar {
                                    name: column_name.into(),
                                    qualifier: qualifier.map(|s| s.into()),
                                    namespace_path: col.fq_table.parents_path.clone(),
                                    alias: None,
                                    provenance: ast_resolved::PhaseBox::phantom(),
                                });
                            }
                        }
                    }
                    unresolved::DomainExpression::Projection(ProjectionExpr::ColumnRange(
                        range_box,
                    )) => {
                        // Expand to columns in ordinal range
                        let range = range_box.get();
                        let candidates: Vec<_> = available.iter().collect();

                        if candidates.is_empty() {
                            return Err(DelightQLError::ColumnNotFoundError {
                                column: format!(
                                    "|{}:{}|",
                                    range
                                        .start
                                        .map(|(p, r)| if r {
                                            format!("-{}", p)
                                        } else {
                                            p.to_string()
                                        })
                                        .unwrap_or_default(),
                                    range
                                        .end
                                        .map(|(p, r)| if r {
                                            format!("-{}", p)
                                        } else {
                                            p.to_string()
                                        })
                                        .unwrap_or_default()
                                ),
                                context:
                                    "No columns available for range resolution in bracket function"
                                        .to_string(),
                            });
                        }

                        let start_idx = if let Some((pos, reverse)) = range.start {
                            if reverse {
                                candidates.len().saturating_sub(pos as usize)
                            } else {
                                (pos.saturating_sub(1)) as usize
                            }
                        } else {
                            0
                        };

                        let end_idx = if let Some((pos, reverse)) = range.end {
                            if reverse {
                                candidates.len().saturating_sub(pos as usize)
                            } else {
                                (pos.saturating_sub(1)) as usize
                            }
                        } else {
                            candidates.len().saturating_sub(1)
                        };

                        for idx in start_idx..=end_idx.min(candidates.len().saturating_sub(1)) {
                            let col = candidates[idx];
                            let column_name = col.name().to_string();
                            if seen_columns.insert(column_name.clone()) {
                                let qualifier = match &col.fq_table.name {
                                    ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                        Some(name.to_string())
                                    }
                                    ast_resolved::TableName::Named(_)
                                    | ast_resolved::TableName::Fresh => None,
                                };
                                resolved_args.push(resolved::DomainExpression::Lvar {
                                    name: column_name.into(),
                                    qualifier: qualifier.map(|s| s.into()),
                                    namespace_path: col.fq_table.parents_path.clone(),
                                    alias: None,
                                    provenance: ast_resolved::PhaseBox::phantom(),
                                });
                            }
                        }
                    }
                    unresolved::DomainExpression::Lvar {
                        name,
                        qualifier,
                        namespace_path,
                        alias,
                        provenance: _,
                    } => {
                        // De-duplicate explicit column references too
                        if seen_columns.insert(name.to_string()) {
                            resolved_args.push(resolved::DomainExpression::Lvar {
                                name,
                                qualifier,
                                namespace_path,
                                alias,
                                provenance: ast_resolved::PhaseBox::phantom(),
                            });
                        }
                    }
                    other => {
                        // Other expressions - resolve normally without de-duplication
                        let resolved = super::domain_expressions::resolve_domain_expr_with_schema(
                            other, available, None,
                        )?;
                        resolved_args.push(resolved);
                    }
                }
            }

            Ok(ast_resolved::FunctionExpression::Bracket {
                arguments: resolved_args,
                alias,
            })
        }
        ast_unresolved::FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => {
            // Resolve left and right expressions for infix function
            let resolved_left =
                super::domain_expressions::resolve_domain_expr_with_schema(*left, available, None)?;
            let resolved_right = super::domain_expressions::resolve_domain_expr_with_schema(
                *right, available, None,
            )?;

            Ok(ast_resolved::FunctionExpression::Infix {
                operator,
                left: Box::new(resolved_left),
                right: Box::new(resolved_right),
                alias,
            })
        }
        ast_unresolved::FunctionExpression::Lambda { body, alias } => {
            // Resolve the lambda body expression
            // The @ placeholder will be resolved at evaluation time
            let resolved_body =
                super::domain_expressions::resolve_domain_expr_with_schema(*body, available, None)?;

            Ok(ast_resolved::FunctionExpression::Lambda {
                body: Box::new(resolved_body),
                alias,
            })
        }
        ast_unresolved::FunctionExpression::StringTemplate { .. } => {
            // StringTemplate should be handled at the DomainExpression level
            // and converted to concat operations
            Err(DelightQLError::ParseError {
                message: "StringTemplate should be expanded in resolve_domain_expr_with_schema"
                    .to_string(),
                source: None,
                subcategory: None,
            })
        }
        ast_unresolved::FunctionExpression::CaseExpression { arms, alias } => {
            // Resolve each arm of the CASE expression
            let mut resolved_arms = Vec::new();

            for arm in arms {
                let resolved_arm = match arm {
                    ast_unresolved::CaseArm::Simple {
                        test_expr,
                        value,
                        result,
                    } => {
                        // Resolve the test expression
                        let resolved_test =
                            super::domain_expressions::resolve_domain_expr_with_schema(
                                *test_expr, available, None,
                            )?;
                        // Resolve the result expression
                        let resolved_result =
                            super::domain_expressions::resolve_domain_expr_with_schema(
                                *result, available, None,
                            )?;

                        ast_resolved::CaseArm::Simple {
                            test_expr: Box::new(resolved_test),
                            value,
                            result: Box::new(resolved_result),
                        }
                    }
                    ast_unresolved::CaseArm::CurriedSimple { value, result } => {
                        // Curried simple - no test expression to resolve (uses @)
                        let resolved_result =
                            super::domain_expressions::resolve_domain_expr_with_schema(
                                *result, available, None,
                            )?;

                        ast_resolved::CaseArm::CurriedSimple {
                            value,
                            result: Box::new(resolved_result),
                        }
                    }
                    ast_unresolved::CaseArm::Searched { condition, result } => {
                        // Resolve the boolean condition
                        let resolved_condition =
                            super::predicates::resolve_boolean_expression(*condition, available)?;
                        // Resolve the result expression
                        let resolved_result =
                            super::domain_expressions::resolve_domain_expr_with_schema(
                                *result, available, None,
                            )?;

                        ast_resolved::CaseArm::Searched {
                            condition: Box::new(resolved_condition),
                            result: Box::new(resolved_result),
                        }
                    }
                    ast_unresolved::CaseArm::Default { result } => {
                        // Resolve the result expression
                        let resolved_result =
                            super::domain_expressions::resolve_domain_expr_with_schema(
                                *result, available, None,
                            )?;

                        ast_resolved::CaseArm::Default {
                            result: Box::new(resolved_result),
                        }
                    }
                };

                resolved_arms.push(resolved_arm);
            }

            Ok(ast_resolved::FunctionExpression::CaseExpression {
                arms: resolved_arms,
                alias,
            })
        }
        ast_unresolved::FunctionExpression::HigherOrder {
            name,
            curried_arguments,
            regular_arguments,
            alias,
            conditioned_on,
        } => {
            // Process curried arguments (preserve Globs inside functions)
            let resolved_curried_args = resolve_function_arguments(curried_arguments, available)?;

            // Process regular arguments (preserve Globs inside functions)
            let resolved_regular_args = resolve_function_arguments(regular_arguments, available)?;

            // Resolve filter condition if present
            let resolved_condition = if let Some(cond) = conditioned_on {
                Some(Box::new(super::predicates::resolve_boolean_expression(
                    *cond, available,
                )?))
            } else {
                None
            };

            Ok(ast_resolved::FunctionExpression::HigherOrder {
                name,
                curried_arguments: resolved_curried_args,
                regular_arguments: resolved_regular_args,
                alias,
                conditioned_on: resolved_condition,
            })
        }
        ast_unresolved::FunctionExpression::Curly {
            members,
            inner_grouping_keys: _,
            cte_requirements: _,
            alias,
        } => {
            // Tree groups - resolve members with ergonomic inductor expansion (TG-ERGONOMIC-INDUCTOR)
            use crate::pipeline::asts::{resolved, unresolved};
            use std::collections::HashSet;

            // Track seen columns for de-duplication (first occurrence wins)
            let mut seen_columns = HashSet::new();
            let mut resolved_members: Vec<resolved::CurlyMember> = Vec::new();

            for member in members {
                match member {
                    unresolved::CurlyMember::Glob => {
                        // Expand to all available columns
                        for col in available {
                            let column_name = col.name().to_string();
                            if seen_columns.insert(column_name.clone()) {
                                let qualifier = match &col.fq_table.name {
                                    ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                        Some(name.to_string())
                                    }
                                    ast_resolved::TableName::Named(_)
                                    | ast_resolved::TableName::Fresh => None,
                                };
                                resolved_members.push(resolved::CurlyMember::Shorthand {
                                    column: column_name.into(),
                                    qualifier: qualifier.map(|s| s.into()),
                                    schema: col
                                        .fq_table
                                        .parents_path
                                        .first()
                                        .map(|s| s.to_string().into()),
                                });
                            }
                        }
                    }
                    unresolved::CurlyMember::Pattern { pattern } => {
                        // Expand to pattern-matched columns
                        use crate::pipeline::pattern::bre_to_rust_regex;
                        let regex_pattern = bre_to_rust_regex(&pattern)?;
                        let re = regex::Regex::new(&regex_pattern).map_err(|e| {
                            DelightQLError::parse_error(format!("Invalid column pattern: {}", e))
                        })?;

                        for col in available.iter().filter(|col| re.is_match(col.name())) {
                            let column_name = col.name().to_string();
                            if seen_columns.insert(column_name.clone()) {
                                let qualifier = match &col.fq_table.name {
                                    ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                        Some(name.to_string())
                                    }
                                    ast_resolved::TableName::Named(_)
                                    | ast_resolved::TableName::Fresh => None,
                                };
                                resolved_members.push(resolved::CurlyMember::Shorthand {
                                    column: column_name.into(),
                                    qualifier: qualifier.map(|s| s.into()),
                                    schema: col
                                        .fq_table
                                        .parents_path
                                        .first()
                                        .map(|s| s.to_string().into()),
                                });
                            }
                        }
                    }
                    unresolved::CurlyMember::OrdinalRange { start, end } => {
                        // Expand to columns in ordinal range
                        let candidates: Vec<_> = available.iter().collect();

                        if candidates.is_empty() {
                            return Err(DelightQLError::ColumnNotFoundError {
                                column: format!(
                                    "|{}:{}|",
                                    start
                                        .map(|(p, r)| if r {
                                            format!("-{}", p)
                                        } else {
                                            p.to_string()
                                        })
                                        .unwrap_or_default(),
                                    end.map(|(p, r)| if r {
                                        format!("-{}", p)
                                    } else {
                                        p.to_string()
                                    })
                                    .unwrap_or_default()
                                ),
                                context:
                                    "No columns available for range resolution in curly function"
                                        .to_string(),
                            });
                        }

                        let start_idx = if let Some((pos, reverse)) = start {
                            if reverse {
                                candidates.len().saturating_sub(pos as usize)
                            } else {
                                (pos.saturating_sub(1)) as usize
                            }
                        } else {
                            0
                        };

                        let end_idx = if let Some((pos, reverse)) = end {
                            if reverse {
                                candidates.len().saturating_sub(pos as usize)
                            } else {
                                (pos.saturating_sub(1)) as usize
                            }
                        } else {
                            candidates.len().saturating_sub(1)
                        };

                        for idx in start_idx..=end_idx.min(candidates.len().saturating_sub(1)) {
                            let col = candidates[idx];
                            let column_name = col.name().to_string();
                            if seen_columns.insert(column_name.clone()) {
                                let qualifier = match &col.fq_table.name {
                                    ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                        Some(name.to_string())
                                    }
                                    ast_resolved::TableName::Named(_)
                                    | ast_resolved::TableName::Fresh => None,
                                };
                                resolved_members.push(resolved::CurlyMember::Shorthand {
                                    column: column_name.into(),
                                    qualifier: qualifier.map(|s| s.into()),
                                    schema: col
                                        .fq_table
                                        .parents_path
                                        .first()
                                        .map(|s| s.to_string().into()),
                                });
                            }
                        }
                    }
                    unresolved::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    } => {
                        // De-duplicate explicit columns too
                        if seen_columns.insert(column.to_string()) {
                            resolved_members.push(resolved::CurlyMember::Shorthand {
                                column,
                                qualifier,
                                schema,
                            });
                        }
                    }
                    unresolved::CurlyMember::Comparison { condition } => {
                        // Pass through - not subject to de-duplication
                        resolved_members.push(resolved::CurlyMember::Comparison {
                            condition: Box::new(super::predicates::resolve_boolean_expression(
                                *condition, available,
                            )?),
                        });
                    }
                    unresolved::CurlyMember::KeyValue {
                        key,
                        nested_reduction,
                        value,
                    } => {
                        // Pass through - not subject to de-duplication
                        resolved_members.push(resolved::CurlyMember::KeyValue {
                            key,
                            nested_reduction,
                            value: Box::new(
                                super::domain_expressions::resolve_domain_expr_with_schema(
                                    *value, available, None,
                                )?,
                            ),
                        });
                    }
                    // PATH FIRST-CLASS: Epoch 5 - PathLiteral handling
                    unresolved::CurlyMember::PathLiteral { path, alias } => {
                        // Pass through - recursively resolve the path expression
                        resolved_members.push(resolved::CurlyMember::PathLiteral {
                            path: Box::new(
                                super::domain_expressions::resolve_domain_expr_with_schema(
                                    *path, available, None,
                                )?,
                            ),
                            alias,
                        });
                    }
                    unresolved::CurlyMember::Placeholder => {
                        // Placeholder is only valid in destructuring context - pass through
                        resolved_members.push(resolved::CurlyMember::Placeholder);
                    }
                }
            }

            Ok(ast_resolved::FunctionExpression::Curly {
                members: resolved_members,
                inner_grouping_keys: vec![],
                cte_requirements: None, // Phase R2+ will populate this
                alias,
            })
        }
        ast_unresolved::FunctionExpression::Array { members, alias } => {
            // Array destructuring - resolve each member's path expression
            // ARRAY DESTRUCTURING: Epoch 4 - Resolver pass-through
            use crate::pipeline::asts::{resolved, unresolved};

            let mut resolved_members: Vec<resolved::ArrayMember> = Vec::new();

            for member in members {
                match member {
                    unresolved::ArrayMember::Index { path, alias } => {
                        // Resolve the path expression (should be JsonPathLiteral)
                        let resolved_path =
                            super::domain_expressions::resolve_domain_expr_with_schema(
                                *path, available, None,
                            )?;
                        resolved_members.push(resolved::ArrayMember::Index {
                            path: Box::new(resolved_path),
                            alias,
                        });
                    }
                }
            }

            Ok(ast_resolved::FunctionExpression::Array {
                members: resolved_members,
                alias,
            })
        }
        ast_unresolved::FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor,
            alias,
            keys_only,
            cte_requirements: _,
        } => {
            // Tree groups - resolve the constructor function
            Ok(ast_resolved::FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier,
                key_schema,
                constructor: Box::new(resolve_function_with_schema(*constructor, available, None)?),
                keys_only,
                cte_requirements: None,
                alias,
            })
        }
        ast_unresolved::FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            alias,
        } => {
            // Window functions: resolve arguments, partition_by, and order_by
            let resolved_arguments = resolve_function_arguments(arguments, available)?;

            let resolved_partition = partition_by
                .into_iter()
                .map(|expr| {
                    let resolved = super::domain_expressions::resolve_expressions_with_schema(
                        vec![expr],
                        available,
                        None,
                        None,
                        None,
                        false,
                    )?;
                    Ok(resolved
                        .into_iter()
                        .next()
                        .expect("Should have one expression"))
                })
                .collect::<Result<Vec<_>>>()?;

            let resolved_order = order_by
                .into_iter()
                .map(|spec| {
                    let resolved_col = super::domain_expressions::resolve_expressions_with_schema(
                        vec![spec.column],
                        available,
                        None,
                        None,
                        None,
                        false,
                    )?;
                    Ok(ast_resolved::OrderingSpec {
                        column: resolved_col
                            .into_iter()
                            .next()
                            .expect("Should have one expression"),
                        direction: spec.direction,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            // Resolve frame bounds if present
            let resolved_frame = frame
                .map(|f| resolve_window_frame(f, available))
                .transpose()?;

            Ok(ast_resolved::FunctionExpression::Window {
                name,
                arguments: resolved_arguments,
                partition_by: resolved_partition,
                order_by: resolved_order,
                frame: resolved_frame,
                alias,
            })
        }
        ast_unresolved::FunctionExpression::JsonPath {
            source,
            path,
            alias,
        } => {
            // PATH FIRST-CLASS: Epoch 4 - resolve both source and path as DomainExpressions
            let exprs_to_resolve = vec![*source, *path];
            let mut resolved = super::domain_expressions::resolve_expressions_with_schema(
                exprs_to_resolve,
                available,
                None,
                None,
                None,
                false,
            )?;

            let resolved_path = resolved.pop().expect("Should have path expression");
            let resolved_source = resolved.pop().expect("Should have source expression");

            Ok(ast_resolved::FunctionExpression::JsonPath {
                source: Box::new(resolved_source),
                path: Box::new(resolved_path),
                alias,
            })
        }
    }
}

/// Resolve a function expression with full context including in_correlation flag
/// This version passes through the in_correlation flag for proper validation
pub(super) fn resolve_function_expression_with_context(
    function: ast_unresolved::FunctionExpression,
    available: &[ast_resolved::ColumnMetadata],
    schema: Option<&dyn DatabaseSchema>,
    mut cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::FunctionExpression> {
    match function {
        ast_unresolved::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => {
            let resolved_args = match &mut cte_context {
                Some(ctx) => resolve_function_arguments_with_context(
                    arguments,
                    available,
                    schema,
                    Some(*ctx),
                    in_correlation,
                )?,
                None => resolve_function_arguments_with_context(
                    arguments,
                    available,
                    schema,
                    None,
                    in_correlation,
                )?,
            };

            // CCAFE validation: Check if this is a context-aware call and validate context params
            if let Some(cfe_definitions) = cfe_defs {
                log::debug!(
                    "CCAFE: Have {} CFE definitions available",
                    cfe_definitions.len()
                );
                if !resolved_args.is_empty() {
                    if let ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::ContextMarker,
                    ) = resolved_args[0]
                    {
                        log::debug!("CCAFE: Detected context-aware call to '{}'", name);
                        // This is a context-aware call - validate context params exist
                        if let Some(cfe_def) = cfe_definitions.get(name.as_ref()) {
                            log::debug!(
                                "CCAFE: Found CFE definition for '{}' with {} context params",
                                name,
                                cfe_def.context_params.len()
                            );
                            if !cfe_def.context_params.is_empty() {
                                // Extract available column names for validation
                                let available_names: std::collections::HashSet<String> =
                                    available.iter().map(|col| col.name().to_string()).collect();

                                // Check each context param
                                let mut missing_params = Vec::new();
                                for ctx_param in &cfe_def.context_params {
                                    if !available_names.contains(ctx_param) {
                                        missing_params.push(ctx_param.clone());
                                    }
                                }

                                if !missing_params.is_empty() {
                                    let context_mode = if cfe_def.allows_positional_context_call {
                                        "explicit"
                                    } else {
                                        "implicit (auto-discovered)"
                                    };

                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "CFE '{}' requires context columns that don't exist in current scope.\n\
                                             \n\
                                             Missing columns: {}\n\
                                             Available columns: {}\n\
                                             \n\
                                             Context mode: {}\n\
                                             Context parameters: {}",
                                            name,
                                            missing_params.join(", "),
                                            available.iter().map(|c| c.name()).collect::<Vec<_>>().join(", "),
                                            context_mode,
                                            cfe_def.context_params.join(", ")
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            let resolved_condition = if let Some(cond) = conditioned_on {
                Some(Box::new(super::predicates::resolve_boolean_expression(
                    *cond, available,
                )?))
            } else {
                None
            };
            Ok(ast_resolved::FunctionExpression::Regular {
                name,
                namespace,
                arguments: resolved_args,
                alias,
                conditioned_on: resolved_condition,
            })
        }
        ast_unresolved::FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => {
            let resolved_left = match &mut cte_context {
                Some(ctx) => {
                    super::domain_expressions::resolve_domain_expr_with_schema_and_context(
                        *left,
                        available,
                        schema,
                        Some(*ctx),
                        in_correlation,
                        cfe_defs,
                        None,
                    )?
                }
                None => super::domain_expressions::resolve_domain_expr_with_schema_and_context(
                    *left,
                    available,
                    schema,
                    None,
                    in_correlation,
                    cfe_defs,
                    None,
                )?,
            };
            let resolved_right = match &mut cte_context {
                Some(ctx) => {
                    super::domain_expressions::resolve_domain_expr_with_schema_and_context(
                        *right,
                        available,
                        schema,
                        Some(*ctx),
                        in_correlation,
                        cfe_defs,
                        None,
                    )?
                }
                None => super::domain_expressions::resolve_domain_expr_with_schema_and_context(
                    *right,
                    available,
                    schema,
                    None,
                    in_correlation,
                    cfe_defs,
                    None,
                )?,
            };
            Ok(ast_resolved::FunctionExpression::Infix {
                operator,
                left: Box::new(resolved_left),
                right: Box::new(resolved_right),
                alias,
            })
        }
        // For other cases, fall back to the standard resolution
        _ => resolve_function_with_schema(function, available, None),
    }
}

/// Resolve function arguments using the shared registry
fn resolve_function_arguments_with_registry(
    arguments: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let mut resolved = Vec::new();
    for arg in arguments {
        match arg {
            ast_unresolved::DomainExpression::Projection(ProjectionExpr::Glob {
                qualifier,
                namespace_path,
                ..
            }) => {
                resolved.push(ast_resolved::DomainExpression::Projection(
                    ProjectionExpr::Glob {
                        qualifier,
                        namespace_path,
                    },
                ));
            }
            other => {
                let resolved_arg = super::domain_expressions::resolve_domain_expr_with_registry(
                    other,
                    available,
                    registry,
                    in_correlation,
                )?;
                resolved.push(resolved_arg);
            }
        }
    }
    Ok(resolved)
}

/// Resolve a function expression using the shared registry
///
/// Handles Regular and Infix with full registry context (for scalar subqueries
/// in arguments). Other variants fall back to schema-only resolution.
pub(super) fn resolve_function_expression_with_registry(
    function: ast_unresolved::FunctionExpression,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
) -> Result<ast_resolved::FunctionExpression> {
    match function {
        ast_unresolved::FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => {
            let resolved_args = resolve_function_arguments_with_registry(
                arguments,
                available,
                registry,
                in_correlation,
            )?;

            // CCAFE validation
            {
                let cfe_defs = &registry.query_local.cfes;
                if !resolved_args.is_empty() {
                    if let ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::ContextMarker,
                    ) = resolved_args[0]
                    {
                        if let Some(cfe_def) = cfe_defs.get(name.as_ref()) {
                            if !cfe_def.context_params.is_empty() {
                                let available_names: std::collections::HashSet<String> =
                                    available.iter().map(|col| col.name().to_string()).collect();
                                let mut missing_params = Vec::new();
                                for ctx_param in &cfe_def.context_params {
                                    if !available_names.contains(ctx_param) {
                                        missing_params.push(ctx_param.clone());
                                    }
                                }
                                if !missing_params.is_empty() {
                                    let context_mode = if cfe_def.allows_positional_context_call {
                                        "explicit"
                                    } else {
                                        "implicit (auto-discovered)"
                                    };
                                    return Err(DelightQLError::ParseError {
                                        message: format!(
                                            "CFE '{}' requires context columns that don't exist in current scope.\n\
                                             \n\
                                             Missing columns: {}\n\
                                             Available columns: {}\n\
                                             \n\
                                             Context mode: {}\n\
                                             Context parameters: {}",
                                            name,
                                            missing_params.join(", "),
                                            available.iter().map(|c| c.name()).collect::<Vec<_>>().join(", "),
                                            context_mode,
                                            cfe_def.context_params.join(", ")
                                        ),
                                        source: None,
                                        subcategory: None,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            let resolved_condition = if let Some(cond) = conditioned_on {
                Some(Box::new(super::predicates::resolve_boolean_expression(
                    *cond, available,
                )?))
            } else {
                None
            };
            Ok(ast_resolved::FunctionExpression::Regular {
                name,
                namespace,
                arguments: resolved_args,
                alias,
                conditioned_on: resolved_condition,
            })
        }
        ast_unresolved::FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        } => {
            let resolved_left = super::domain_expressions::resolve_domain_expr_with_registry(
                *left,
                available,
                registry,
                in_correlation,
            )?;
            let resolved_right = super::domain_expressions::resolve_domain_expr_with_registry(
                *right,
                available,
                registry,
                in_correlation,
            )?;
            Ok(ast_resolved::FunctionExpression::Infix {
                operator,
                left: Box::new(resolved_left),
                right: Box::new(resolved_right),
                alias,
            })
        }
        // For other cases, fall back to the standard resolution
        _ => resolve_function_with_schema(function, available, None),
    }
}

/// Resolve window frame specification
fn resolve_window_frame(
    frame: ast_unresolved::WindowFrame,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<ast_resolved::WindowFrame> {
    use crate::pipeline::asts::{resolved, unresolved};

    let start = match frame.start {
        unresolved::FrameBound::Unbounded => resolved::FrameBound::Unbounded,
        unresolved::FrameBound::CurrentRow => resolved::FrameBound::CurrentRow,
        unresolved::FrameBound::Preceding(expr) => {
            let resolved_expr = super::domain_expressions::resolve_expressions_with_schema(
                vec![*expr],
                available,
                None,
                None,
                None,
                false,
            )?;
            resolved::FrameBound::Preceding(Box::new(
                resolved_expr
                    .into_iter()
                    .next()
                    .expect("Should have one expression"),
            ))
        }
        unresolved::FrameBound::Following(expr) => {
            let resolved_expr = super::domain_expressions::resolve_expressions_with_schema(
                vec![*expr],
                available,
                None,
                None,
                None,
                false,
            )?;
            resolved::FrameBound::Following(Box::new(
                resolved_expr
                    .into_iter()
                    .next()
                    .expect("Should have one expression"),
            ))
        }
    };

    let end = match frame.end {
        unresolved::FrameBound::Unbounded => resolved::FrameBound::Unbounded,
        unresolved::FrameBound::CurrentRow => resolved::FrameBound::CurrentRow,
        unresolved::FrameBound::Preceding(expr) => {
            let resolved_expr = super::domain_expressions::resolve_expressions_with_schema(
                vec![*expr],
                available,
                None,
                None,
                None,
                false,
            )?;
            resolved::FrameBound::Preceding(Box::new(
                resolved_expr
                    .into_iter()
                    .next()
                    .expect("Should have one expression"),
            ))
        }
        unresolved::FrameBound::Following(expr) => {
            let resolved_expr = super::domain_expressions::resolve_expressions_with_schema(
                vec![*expr],
                available,
                None,
                None,
                None,
                false,
            )?;
            resolved::FrameBound::Following(Box::new(
                resolved_expr
                    .into_iter()
                    .next()
                    .expect("Should have one expression"),
            ))
        }
    };

    Ok(resolved::WindowFrame {
        mode: frame.mode,
        start,
        end,
    })
}
