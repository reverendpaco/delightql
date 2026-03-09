use crate::pipeline::ast_resolved;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};
use crate::pipeline::asts::unresolved::NamespacePath;
use delightql_types::SqlIdentifier;

/// Check if a domain expression contains any qualified references
pub(in crate::pipeline::resolver) fn expr_has_qualified_ref(
    expr: &ast_resolved::DomainExpression,
) -> bool {
    match expr {
        ast_resolved::DomainExpression::Lvar { qualifier, .. } => qualifier.is_some(),
        ast_resolved::DomainExpression::Function(func) => {
            // Recursively check function arguments
            match func {
                ast_resolved::FunctionExpression::Regular { arguments, .. }
                | ast_resolved::FunctionExpression::Bracket { arguments, .. }
                | ast_resolved::FunctionExpression::Curried { arguments, .. } => {
                    arguments.iter().any(expr_has_qualified_ref)
                }
                ast_resolved::FunctionExpression::HigherOrder {
                    curried_arguments,
                    regular_arguments,
                    ..
                } => {
                    curried_arguments.iter().any(expr_has_qualified_ref)
                        || regular_arguments.iter().any(expr_has_qualified_ref)
                }
                ast_resolved::FunctionExpression::Infix { left, right, .. } => {
                    expr_has_qualified_ref(left) || expr_has_qualified_ref(right)
                }
                ast_resolved::FunctionExpression::Lambda { body, .. } => {
                    expr_has_qualified_ref(body)
                }
                ast_resolved::FunctionExpression::StringTemplate { .. } => {
                    // StringTemplate should have been expanded to concat by resolver
                    false
                }
                ast_resolved::FunctionExpression::CaseExpression { .. } => {
                    // CaseExpression not yet implemented in resolver
                    false
                }
                ast_resolved::FunctionExpression::Curly { .. } => false,
                ast_resolved::FunctionExpression::Array { .. } => false,
                ast_resolved::FunctionExpression::MetadataTreeGroup { .. } => false,
                ast_resolved::FunctionExpression::Window {
                    arguments,
                    partition_by,
                    order_by,
                    ..
                } => {
                    arguments.iter().any(expr_has_qualified_ref)
                        || partition_by.iter().any(expr_has_qualified_ref)
                        || order_by
                            .iter()
                            .any(|spec| expr_has_qualified_ref(&spec.column))
                }
                ast_resolved::FunctionExpression::JsonPath { source, .. } => {
                    // JsonPath: check if source has qualified references
                    expr_has_qualified_ref(source)
                }
            }
        }
        ast_resolved::DomainExpression::PipedExpression {
            value, transforms, ..
        } => {
            expr_has_qualified_ref(value)
                || transforms.iter().any(|t| match t {
                    ast_resolved::FunctionExpression::Regular { arguments, .. }
                    | ast_resolved::FunctionExpression::Curried { arguments, .. }
                    | ast_resolved::FunctionExpression::Bracket { arguments, .. } => {
                        arguments.iter().any(expr_has_qualified_ref)
                    }
                    ast_resolved::FunctionExpression::HigherOrder {
                        curried_arguments,
                        regular_arguments,
                        ..
                    } => {
                        curried_arguments.iter().any(expr_has_qualified_ref)
                            || regular_arguments.iter().any(expr_has_qualified_ref)
                    }
                    ast_resolved::FunctionExpression::Infix { left, right, .. } => {
                        expr_has_qualified_ref(left) || expr_has_qualified_ref(right)
                    }
                    ast_resolved::FunctionExpression::Lambda { body, .. } => {
                        expr_has_qualified_ref(body)
                    }
                    ast_resolved::FunctionExpression::StringTemplate { .. } => {
                        // StringTemplate should have been expanded to concat by resolver
                        false
                    }
                    ast_resolved::FunctionExpression::CaseExpression { .. } => {
                        // TODO: Check CASE arms for qualified refs
                        false
                    }
                    ast_resolved::FunctionExpression::Curly { .. } => false,
                    ast_resolved::FunctionExpression::MetadataTreeGroup { .. } => false,
                    ast_resolved::FunctionExpression::Window {
                        arguments,
                        partition_by,
                        order_by,
                        ..
                    } => {
                        arguments.iter().any(expr_has_qualified_ref)
                            || partition_by.iter().any(expr_has_qualified_ref)
                            || order_by
                                .iter()
                                .any(|spec| expr_has_qualified_ref(&spec.column))
                    }
                    _ => unimplemented!("JsonPath not yet implemented in this phase"),
                })
        }
        ast_resolved::DomainExpression::Parenthesized { inner, .. } => {
            expr_has_qualified_ref(inner)
        }
        // Projection expressions: Glob can be qualified (u.*), others are leaves
        ast_resolved::DomainExpression::Projection(proj) => match proj {
            ProjectionExpr::Glob { qualifier, .. } => qualifier.is_some(),
            _ => false,
        },
        // Tuple: recurse into elements (multi-column expressions can contain qualified refs)
        ast_resolved::DomainExpression::Tuple { elements, .. } => {
            elements.iter().any(expr_has_qualified_ref)
        }
        // PivotOf: recurse into value and key columns
        ast_resolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            ..
        } => expr_has_qualified_ref(value_column) || expr_has_qualified_ref(pivot_key),
        // Predicate: boolean expressions may contain qualified refs, but we'd need a
        // separate walker for BooleanExpression. Conservative false — rare in aggregates.
        ast_resolved::DomainExpression::Predicate { .. } => false,
        // Leaf expressions: no table qualifiers possible.
        ast_resolved::DomainExpression::Literal { .. }
        | ast_resolved::DomainExpression::NonUnifiyingUnderscore
        | ast_resolved::DomainExpression::ValuePlaceholder { .. }
        | ast_resolved::DomainExpression::Substitution(_)
        | ast_resolved::DomainExpression::ColumnOrdinal(_) => false,
        // ScalarSubquery: inner scope — qualified refs inside don't count as outer qualified.
        ast_resolved::DomainExpression::ScalarSubquery { .. } => false,
    }
}

/// Extract the column that a domain expression provides (if any).
/// This is the inductive solution - handles all domain expression types uniformly.
///
/// # Arguments
/// * `expr` - The expression to extract a column from
/// * `input_columns` - Available input columns
/// * `position` - The position of this expression in the projection (for generating unique names)
pub(in crate::pipeline::resolver) fn extract_provided_column_from_domain_expr(
    expr: &ast_resolved::DomainExpression,
    input_columns: &[ast_resolved::ColumnMetadata],
    position: usize,
) -> Option<ast_resolved::ColumnMetadata> {
    match expr {
        ast_resolved::DomainExpression::Lvar {
            name,
            alias,
            qualifier,
            ..
        } => {
            // An Lvar provides a column - either with its original name or with an alias
            if let Some(col) = input_columns
                .iter()
                .find(|c| crate::pipeline::resolver::col_name_eq(c.name(), name))
            {
                let mut output_col = col.clone();

                // Preserve the qualification status from the resolved expression.
                // If the expression had a qualifier (e.g., users.id), mark the column as qualified.
                if qualifier.is_some() {
                    output_col.info = output_col.info.with_updated_qualification(true);
                }

                if let Some(alias_name) = alias {
                    // If there's an alias, the expression provides a column with that alias name
                    output_col.info = output_col.info.with_alias(alias_name.clone());
                }
                // Projection establishes column identity: the name is now known,
                // even if the source was a passthrough table with unnamed columns.
                output_col.has_user_name = true;
                Some(output_col)
            } else {
                // Column not found in input — either passthrough table (no schema)
                // or a new computed column. The user explicitly wrote this name in a
                // projection, so the column is user-named.
                let final_name = alias.as_ref().unwrap_or(name);
                let mut info = ast_resolved::ColumnProvenance::from_column(final_name.clone());

                // Even for computed columns, preserve qualification status
                if qualifier.is_some() {
                    info = info.with_updated_qualification(true);
                }

                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    info,
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: ast_resolved::TableName::Fresh,
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    None,
                    true, // Lvar in projection = user explicitly named this column
                ))
            }
        }
        ast_resolved::DomainExpression::Function(func) => {
            // Functions with aliases provide new columns
            let (alias, has_qualified_args) = match func {
                ast_resolved::FunctionExpression::Regular {
                    alias, arguments, ..
                } => {
                    let qualified = arguments.iter().any(expr_has_qualified_ref);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Bracket {
                    alias, arguments, ..
                } => {
                    let qualified = arguments.iter().any(expr_has_qualified_ref);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Infix {
                    alias, left, right, ..
                } => {
                    let qualified = expr_has_qualified_ref(left) || expr_has_qualified_ref(right);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Curried { arguments, .. } => {
                    let qualified = arguments.iter().any(expr_has_qualified_ref);
                    (&None, qualified)
                }
                ast_resolved::FunctionExpression::HigherOrder {
                    alias,
                    curried_arguments,
                    regular_arguments,
                    ..
                } => {
                    let qualified = curried_arguments.iter().any(expr_has_qualified_ref)
                        || regular_arguments.iter().any(expr_has_qualified_ref);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::Lambda { body, alias, .. } => {
                    let qualified = expr_has_qualified_ref(body);
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::StringTemplate { .. } => {
                    // StringTemplate should have been expanded to concat by resolver
                    (&None, false)
                }
                ast_resolved::FunctionExpression::CaseExpression { alias, .. } => {
                    // CaseExpression - check if it has an alias
                    (alias, false)
                }
                ast_resolved::FunctionExpression::Curly { alias, .. } => (alias, false),
                ast_resolved::FunctionExpression::Array { alias, .. } => (alias, false),
                ast_resolved::FunctionExpression::MetadataTreeGroup { alias, .. } => (alias, false),
                ast_resolved::FunctionExpression::Window {
                    alias,
                    arguments,
                    partition_by,
                    order_by,
                    ..
                } => {
                    // Window function - check for qualified refs in all expressions
                    let qualified = arguments.iter().any(expr_has_qualified_ref)
                        || partition_by.iter().any(expr_has_qualified_ref)
                        || order_by
                            .iter()
                            .any(|spec| expr_has_qualified_ref(&spec.column));
                    (alias, qualified)
                }
                ast_resolved::FunctionExpression::JsonPath { alias, source, .. } => {
                    // JsonPath - check if source has qualified refs
                    let qualified = expr_has_qualified_ref(source);
                    (alias, qualified)
                }
            };

            if let Some(alias_name) = alias {
                // Function with alias creates a new column
                let mut info = ast_resolved::ColumnProvenance::from_column(alias_name.clone());

                // CRITICAL FIX: If function arguments contain qualified references, mark the output as qualified
                if has_qualified_args {
                    info = info.with_updated_qualification(true);
                }

                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    info,
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: ast_resolved::TableName::Fresh,
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    None,
                    alias.is_some(), // has_user_name true only if alias provided
                ))
            } else {
                // Function without alias still provides a column with a generated name
                // Use the naming utility to generate a unique name based on position
                let col_name =
                    crate::pipeline::naming::generate_function_column_name(func, position);

                let mut info = ast_resolved::ColumnProvenance::from_column(col_name);

                // CRITICAL: Preserve qualification status even without alias
                if has_qualified_args {
                    info = info.with_updated_qualification(true);
                }

                // ALWAYS create a column for function expressions, even without alias
                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    info,
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: ast_resolved::TableName::Fresh,
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    None,
                    false, // No alias in this branch, so has_user_name is false
                ))
            }
        }
        ast_resolved::DomainExpression::Literal { alias, value: _ } => {
            // Literals provide columns - use alias if provided, otherwise generate name
            let col_name: SqlIdentifier = if let Some(alias_name) = alias {
                alias_name.clone()
            } else {
                // Use naming utility for consistency (though literals usually have their value as name)
                format!("literal_{}", position + 1).into()
            };

            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::FqTable {
                    parents_path: NamespacePath::empty(),
                    name: ast_resolved::TableName::Fresh,
                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                },
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::Predicate { alias, .. } => {
            // Predicates provide boolean columns - use alias if provided
            let col_name = if let Some(alias_name) = alias {
                alias_name.clone()
            } else {
                // Generate a default name for the predicate column
                "predicate".into()
            };

            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::FqTable {
                    parents_path: NamespacePath::empty(),
                    name: ast_resolved::TableName::Fresh,
                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                },
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::Projection(proj) => match proj {
            // Globs don't provide individual columns - they expand to multiple columns
            // This needs to be handled separately by the operator
            ProjectionExpr::Glob { .. } => None,
            // These should have been resolved/expanded to Lvars by now
            ProjectionExpr::ColumnRange(_) | ProjectionExpr::Pattern { .. } => None,
            // PATH FIRST-CLASS: Epoch 5 - JsonPathLiteral handling
            // JsonPathLiteral provides a column like a literal value
            ProjectionExpr::JsonPathLiteral { alias, .. } => {
                let col_name: SqlIdentifier = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else {
                    // Generate a default name for path literal
                    format!("path_literal_{}", position + 1).into()
                };

                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    ast_resolved::ColumnProvenance::from_column(col_name),
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: ast_resolved::TableName::Fresh,
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    None,
                    alias.is_some(), // has_user_name true only if alias provided
                ))
            }
        },
        ast_resolved::DomainExpression::NonUnifiyingUnderscore => {
            // Placeholders don't provide columns
            None
        }
        ast_resolved::DomainExpression::ColumnOrdinal(_) => {
            // These should have been resolved/expanded to Lvars by now
            None
        }
        ast_resolved::DomainExpression::ScalarSubquery { alias, .. } => {
            // Scalar subquery returns a single value - treat like a function
            let col_name = alias
                .clone()
                .unwrap_or_else(|| format!("scalar_subquery_{}", position).into());

            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::FqTable {
                    parents_path: NamespacePath::empty(),
                    name: ast_resolved::TableName::Fresh,
                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                },
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::Substitution(sub) => match sub {
            SubstitutionExpr::Parameter { name, alias }
            | SubstitutionExpr::CurriedParameter { name, alias } => {
                // Parameters/curried parameters provide columns (for CFE/HOCFE bodies)
                let col_name = alias.as_ref().unwrap_or(name).clone();
                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    ast_resolved::ColumnProvenance::from_column(col_name),
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: ast_resolved::TableName::Fresh,
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    None,
                    alias.is_some(),
                ))
            }
            SubstitutionExpr::ContextParameter { .. } => {
                // ContextParameter should never exist in resolved phase - it's only created during
                // postprocessing in refined phase for CCAFE feature
                None
            }
            SubstitutionExpr::ContextMarker => {
                // ContextMarker (..) should only appear in function call arguments
                // It doesn't provide columns itself
                None
            }
        },
        ast_resolved::DomainExpression::ValuePlaceholder { alias } => {
            // @ placeholder provides a column for the value that will be substituted
            let col_name = if let Some(alias_name) = alias {
                alias_name.clone()
            } else {
                // Generate a default name for the placeholder
                "value".into()
            };

            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::FqTable {
                    parents_path: NamespacePath::empty(),
                    name: ast_resolved::TableName::Fresh,
                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                },
                None,
                alias.is_some(), // has_user_name true only if alias provided
            ))
        }
        ast_resolved::DomainExpression::PipedExpression { alias, .. } => {
            // Piped expression provides a column with the result of the pipeline
            let col_name = alias
                .as_ref()
                .cloned()
                .unwrap_or_else(|| "piped_result".into());

            Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                ast_resolved::ColumnProvenance::from_column(col_name),
                ast_resolved::FqTable {
                    parents_path: NamespacePath::empty(),
                    name: ast_resolved::TableName::Fresh,
                    backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                },
                None,
                alias.is_some(),
            ))
        }
        ast_resolved::DomainExpression::Parenthesized { inner, alias } => {
            // Parenthesized expression - check if inner expression provides a column
            // If it has an alias, use that; otherwise use the inner expression's column
            if alias.is_some() {
                Some(ast_resolved::ColumnMetadata::new_with_name_flag(
                    ast_resolved::ColumnProvenance::from_column(
                        alias.as_ref().expect("Checked is_some() above").clone(),
                    ),
                    ast_resolved::FqTable {
                        parents_path: NamespacePath::empty(),
                        name: ast_resolved::TableName::Fresh,
                        backend_schema: ast_resolved::PhaseBox::from_optional_schema(None),
                    },
                    None,
                    true,
                ))
            } else {
                extract_provided_column_from_domain_expr(inner, input_columns, position)
            }
        }
        ast_resolved::DomainExpression::Tuple { .. } => {
            // Tuples don't provide a single column - they should have been desugared
            None
        }

        // Pivot expressions expand to multiple columns, handled at modulo level
        ast_resolved::DomainExpression::PivotOf { .. } => None,
    }
}
