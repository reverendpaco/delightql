use super::utils::contains_value_placeholder;
/// Placeholder substitution logic for pipeline expressions
use crate::error::Result;
use crate::pipeline::asts::addressed::{
    CaseArm, DomainExpression as AstDomainExpression, FunctionExpression,
};
use crate::pipeline::sql_ast_v3::DomainExpression as SqlDomainExpression;

/// Substitute an AST expression into @ placeholders in a transform function
/// This keeps everything in AST domain to avoid SQL-to-AST conversion issues
pub fn substitute_ast_in_transform(
    value: AstDomainExpression,
    transform: FunctionExpression,
) -> Result<AstDomainExpression> {
    match transform {
        FunctionExpression::Lambda { body, alias: _ } => {
            // Recursively substitute @ with the value AST expression
            let substituted_body = substitute_ast_value_placeholder(*body, value)?;
            Ok(substituted_body)
        }
        FunctionExpression::Regular {
            name,
            namespace,
            arguments,
            alias,
            conditioned_on,
        } => {
            // Check if any arguments contain @
            let has_placeholder = arguments
                .iter()
                .any(super::utils::contains_value_placeholder);

            if has_placeholder {
                // Substitute @ in arguments
                let substituted_args = arguments
                    .into_iter()
                    .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                    .collect::<Result<Vec<_>>>()?;

                Ok(AstDomainExpression::Function(FunctionExpression::Regular {
                    name,
                    namespace,
                    arguments: substituted_args,
                    alias,
                    conditioned_on,
                }))
            } else {
                // No @ - value becomes first argument
                let mut new_args = vec![value];
                new_args.extend(arguments);

                Ok(AstDomainExpression::Function(FunctionExpression::Regular {
                    name,
                    namespace,
                    arguments: new_args,
                    alias,
                    conditioned_on,
                }))
            }
        }
        FunctionExpression::Curried {
            name,
            namespace,
            arguments,
            conditioned_on,
        } => {
            // Check if any arguments contain @
            let has_placeholder = arguments
                .iter()
                .any(super::utils::contains_value_placeholder);

            if has_placeholder {
                // Substitute @ in arguments
                let substituted_args = arguments
                    .into_iter()
                    .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                    .collect::<Result<Vec<_>>>()?;

                Ok(AstDomainExpression::Function(FunctionExpression::Curried {
                    name,
                    namespace,
                    arguments: substituted_args,
                    conditioned_on,
                }))
            } else {
                // No @ - value becomes first argument (currying)
                let mut new_args = vec![value];
                new_args.extend(arguments);

                Ok(AstDomainExpression::Function(FunctionExpression::Regular {
                    name,
                    namespace,
                    arguments: new_args,
                    alias: None,
                    conditioned_on, // Preserve the condition
                }))
            }
        }
        FunctionExpression::CaseExpression { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "Searched CASE expressions (with boolean conditions like '@ >= 65 -> ...') are not yet supported in pipe transforms. Only simple CASE (value matching like '@ \"active\" -> ...') is currently supported.".to_string(),
            source: None,
            subcategory: None,
        }),
        FunctionExpression::HigherOrder { name, .. } => Err(crate::error::DelightQLError::ParseError {
            message: format!("Higher-order CFE calls not yet implemented (Epoch 3): '{}'", name),
            source: None,
            subcategory: None,
        }),
        FunctionExpression::Infix { operator, .. } => Err(crate::error::DelightQLError::ParseError {
            message: format!("Infix operator '{}' not supported as direct pipe transform (during substitution). Use a lambda wrapper: /-> :(... {} ...)", operator, operator),
            source: None,
            subcategory: None,
        }),
        FunctionExpression::StringTemplate { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "String templates should have been expanded by the resolver. This is an internal error.".to_string(),
            source: None,
            subcategory: None,
        }),
        FunctionExpression::Bracket { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "Bracket expressions not yet supported as pipe transforms (during substitution).".to_string(),
            source: None,
            subcategory: None,
        }),
        FunctionExpression::Curly { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "Tree groups (curly functions) not yet supported as pipe transforms (Epoch 1).".to_string(),
            source: None,
            subcategory: None,
        }),
        FunctionExpression::MetadataTreeGroup { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "Metadata tree groups not yet supported as pipe transforms (Epoch 1).".to_string(),
            source: None,
            subcategory: None,
        }),
        FunctionExpression::Window {
            name,
            arguments,
            partition_by,
            order_by,
            frame,
            alias: _,
        } => {
            // Check if @ appears in arguments, partition_by, or order_by
            let has_placeholder_in_args = arguments.iter().any(|arg| contains_value_placeholder(arg));
            let has_placeholder_in_partition = partition_by.iter().any(|expr| contains_value_placeholder(expr));
            let has_placeholder_in_order = order_by.iter().any(|spec| contains_value_placeholder(&spec.column));

            if has_placeholder_in_args || has_placeholder_in_partition || has_placeholder_in_order {
                // @ was used - substitute it with the piped value
                let substituted_args = arguments
                    .into_iter()
                    .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                    .collect::<Result<Vec<_>>>()?;
                let substituted_partition = partition_by
                    .into_iter()
                    .map(|expr| substitute_ast_value_placeholder(expr, value.clone()))
                    .collect::<Result<Vec<_>>>()?;
                let substituted_order = order_by
                    .into_iter()
                    .map(|spec| {
                        Ok(crate::pipeline::asts::addressed::OrderingSpec {
                            column: substitute_ast_value_placeholder(spec.column, value.clone())?,
                            direction: spec.direction,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(AstDomainExpression::Function(FunctionExpression::Window {
                    name,
                    arguments: substituted_args,
                    partition_by: substituted_partition,
                    order_by: substituted_order,
                    frame, // TODO: substitute @ in frame bounds
                    alias: None,
                }))
            } else {
                // No @ - value becomes first argument
                let mut new_args = vec![value];
                new_args.extend(arguments);

                Ok(AstDomainExpression::Function(FunctionExpression::Window {
                    name,
                    arguments: new_args,
                    partition_by,
                    order_by,
                    frame,
                    alias: None,
                }))
            }
        }
        _ => unimplemented!("JsonPath not yet implemented in this phase"),
    }
}

/// Substitute @ placeholders in an AST expression with another AST expression
pub fn substitute_ast_value_placeholder(
    expr: AstDomainExpression,
    value: AstDomainExpression,
) -> Result<AstDomainExpression> {
    match expr {
        AstDomainExpression::ValuePlaceholder { .. } => {
            // Replace @ with the value
            Ok(value)
        }

        AstDomainExpression::Function(func) => {
            let substituted_func = match func {
                FunctionExpression::Regular {
                    name,
                    namespace,
                    arguments,
                    alias,
                    conditioned_on,
                } => {
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Regular {
                        name,
                        namespace,
                        arguments: substituted_args,
                        alias,
                        conditioned_on,
                    }
                }
                FunctionExpression::Curried {
                    name,
                    namespace,
                    arguments,
                    conditioned_on,
                } => {
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Curried {
                        name,
                        namespace,
                        arguments: substituted_args,
                        conditioned_on,
                    }
                }
                FunctionExpression::Bracket { arguments, alias } => {
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Bracket {
                        arguments: substituted_args,
                        alias,
                    }
                }
                FunctionExpression::Infix {
                    operator,
                    left,
                    right,
                    alias,
                } => {
                    let substituted_left =
                        Box::new(substitute_ast_value_placeholder(*left, value.clone())?);
                    let substituted_right =
                        Box::new(substitute_ast_value_placeholder(*right, value.clone())?);
                    FunctionExpression::Infix {
                        operator,
                        left: substituted_left,
                        right: substituted_right,
                        alias,
                    }
                }
                FunctionExpression::Lambda { body, alias } => {
                    let substituted_body =
                        Box::new(substitute_ast_value_placeholder(*body, value.clone())?);
                    FunctionExpression::Lambda {
                        body: substituted_body,
                        alias,
                    }
                }
                FunctionExpression::StringTemplate { .. } => {
                    return Err(crate::error::DelightQLError::validation_error(
                        "StringTemplate should have been expanded to concat by resolver",
                        "expression_transformer",
                    ));
                }
                FunctionExpression::CaseExpression { arms, alias } => {
                    // Substitute in all CASE arms
                    let substituted_arms = arms
                        .into_iter()
                        .map(|arm| match arm {
                            CaseArm::Simple {
                                test_expr,
                                value: arm_value,
                                result,
                            } => Ok(CaseArm::Simple {
                                test_expr: Box::new(substitute_ast_value_placeholder(
                                    *test_expr,
                                    value.clone(),
                                )?),
                                value: arm_value,
                                result: Box::new(substitute_ast_value_placeholder(
                                    *result,
                                    value.clone(),
                                )?),
                            }),
                            CaseArm::CurriedSimple {
                                value: arm_value,
                                result,
                            } => {
                                // For curried CASE, inject the value as the test expression
                                Ok(CaseArm::Simple {
                                    test_expr: Box::new(value.clone()),
                                    value: arm_value,
                                    result: Box::new(substitute_ast_value_placeholder(
                                        *result,
                                        value.clone(),
                                    )?),
                                })
                            }
                            CaseArm::Searched { condition, result } => {
                                // TODO: Substitute in boolean condition if needed
                                Ok(CaseArm::Searched {
                                    condition,
                                    result: Box::new(substitute_ast_value_placeholder(
                                        *result,
                                        value.clone(),
                                    )?),
                                })
                            }
                            CaseArm::Default { result } => Ok(CaseArm::Default {
                                result: Box::new(substitute_ast_value_placeholder(
                                    *result,
                                    value.clone(),
                                )?),
                            }),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    FunctionExpression::CaseExpression {
                        arms: substituted_arms,
                        alias,
                    }
                }
                FunctionExpression::HigherOrder {
                    name,
                    curried_arguments,
                    regular_arguments,
                    alias,
                    conditioned_on,
                } => {
                    let substituted_curried_args = curried_arguments
                        .into_iter()
                        .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    let substituted_regular_args = regular_arguments
                        .into_iter()
                        .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::HigherOrder {
                        name,
                        curried_arguments: substituted_curried_args,
                        regular_arguments: substituted_regular_args,
                        alias,
                        conditioned_on,
                    }
                }
                FunctionExpression::Curly {
                    members,
                    inner_grouping_keys,
                    cte_requirements,
                    alias,
                } => {
                    // Tree groups don't contain value placeholders (Epoch 1)
                    FunctionExpression::Curly {
                        members,
                        inner_grouping_keys,
                        cte_requirements,
                        alias,
                    }
                }
                FunctionExpression::MetadataTreeGroup {
                    key_column,
                    key_qualifier,
                    key_schema,
                    constructor,
                    keys_only,
                    cte_requirements,
                    alias,
                } => {
                    // Tree groups don't contain value placeholders (Epoch 1)
                    FunctionExpression::MetadataTreeGroup {
                        key_column,
                        key_qualifier,
                        key_schema,
                        constructor,
                        keys_only,
                        cte_requirements,
                        alias,
                    }
                }
                FunctionExpression::Window {
                    name,
                    arguments,
                    partition_by,
                    order_by,
                    frame,
                    alias,
                } => {
                    // Substitute @ placeholders in window function arguments
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    let substituted_partition = partition_by
                        .into_iter()
                        .map(|arg| substitute_ast_value_placeholder(arg, value.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    let substituted_order = order_by
                        .into_iter()
                        .map(|spec| {
                            Ok(crate::pipeline::asts::addressed::OrderingSpec {
                                column: substitute_ast_value_placeholder(
                                    spec.column,
                                    value.clone(),
                                )?,
                                direction: spec.direction,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Window {
                        name,
                        arguments: substituted_args,
                        partition_by: substituted_partition,
                        order_by: substituted_order,
                        frame,
                        alias,
                    }
                }
                _ => unimplemented!("JsonPath not yet implemented in this phase"),
            };
            Ok(AstDomainExpression::Function(substituted_func))
        }

        AstDomainExpression::Parenthesized { inner, alias } => {
            let substituted_inner = Box::new(substitute_ast_value_placeholder(*inner, value)?);
            Ok(AstDomainExpression::Parenthesized {
                inner: substituted_inner,
                alias,
            })
        }

        // Other expression types pass through unchanged
        other => Ok(other),
    }
}

/// Substitute @ placeholders in an expression with a column reference
/// This is used by MapCover to replace @ with the actual column being transformed
pub fn substitute_value_placeholder(
    expr: AstDomainExpression,
    column_ref: SqlDomainExpression,
) -> Result<AstDomainExpression> {
    match expr {
        // If we find a @, replace it with the column reference
        // But we need to convert SQL DomainExpression back to AST DomainExpression
        AstDomainExpression::ValuePlaceholder { .. } => {
            // Convert the SQL column reference back to AST format
            // For MapCover, this will be a simple column reference

            match column_ref {
                SqlDomainExpression::Column {
                    name, qualifier, ..
                } => {
                    // Convert ColumnQualifier to AST format
                    let namespace_path = match &qualifier {
                        Some(q) => match q.parts() {
                            crate::pipeline::sql_ast_v3::QualifierParts::Table(_) => {
                                crate::pipeline::asts::addressed::NamespacePath::empty()
                            }
                            crate::pipeline::sql_ast_v3::QualifierParts::SchemaTable {
                                schema,
                                ..
                            }
                            | crate::pipeline::sql_ast_v3::QualifierParts::DatabaseSchemaTable {
                                schema,
                                ..
                            } => crate::pipeline::asts::addressed::NamespacePath::single(schema),
                        },
                        None => crate::pipeline::asts::addressed::NamespacePath::empty(),
                    };
                    let ast_qualifier = qualifier.as_ref().map(|q| q.table_name().to_string());
                    Ok(AstDomainExpression::Lvar {
                        name: name.into(),
                        qualifier: ast_qualifier.map(|q| q.into()),
                        namespace_path,
                        alias: None,
                        provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                    })
                }
                _ => {
                    // For complex expressions from previous transforms, we can't convert back to AST
                    // This is a limitation - chained transforms with @ need special handling
                    log::error!(
                        "Cannot convert SQL expression to AST. SQL expression type: {:?}",
                        std::mem::discriminant(&column_ref)
                    );
                    log::error!("SQL expression debug: {:?}", column_ref);
                    Err(crate::error::DelightQLError::ParseError {
                        message: "Chained transforms with @ placeholders not yet fully supported"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    })
                }
            }
        }

        // Recursively substitute in function arguments
        AstDomainExpression::Function(func) => {
            let substituted_func = match func {
                FunctionExpression::Regular {
                    name,
                    namespace,
                    arguments,
                    alias,
                    conditioned_on,
                } => {
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_value_placeholder(arg, column_ref.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Regular {
                        name,
                        namespace,
                        arguments: substituted_args,
                        alias,
                        conditioned_on,
                    }
                }
                FunctionExpression::Curried {
                    name,
                    namespace,
                    arguments,
                    conditioned_on,
                } => {
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_value_placeholder(arg, column_ref.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Curried {
                        name,
                        namespace,
                        arguments: substituted_args,
                        conditioned_on,
                    }
                }
                FunctionExpression::Bracket { arguments, alias } => {
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_value_placeholder(arg, column_ref.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Bracket {
                        arguments: substituted_args,
                        alias,
                    }
                }
                FunctionExpression::Infix {
                    operator,
                    left,
                    right,
                    alias,
                } => {
                    let substituted_left =
                        Box::new(substitute_value_placeholder(*left, column_ref.clone())?);
                    let substituted_right =
                        Box::new(substitute_value_placeholder(*right, column_ref.clone())?);
                    FunctionExpression::Infix {
                        operator,
                        left: substituted_left,
                        right: substituted_right,
                        alias,
                    }
                }
                FunctionExpression::Lambda { body, alias } => {
                    // Recursively substitute in lambda body
                    let substituted_body =
                        Box::new(substitute_value_placeholder(*body, column_ref.clone())?);
                    FunctionExpression::Lambda {
                        body: substituted_body,
                        alias,
                    }
                }
                FunctionExpression::StringTemplate { .. } => {
                    return Err(crate::error::DelightQLError::validation_error(
                        "StringTemplate should have been expanded to concat by resolver",
                        "expression_transformer",
                    ));
                }
                FunctionExpression::CaseExpression { arms, alias } => {
                    // Substitute in all CASE arms
                    let substituted_arms = arms
                        .into_iter()
                        .map(|arm| match arm {
                            CaseArm::Simple {
                                test_expr,
                                value: arm_value,
                                result,
                            } => Ok(CaseArm::Simple {
                                test_expr: Box::new(substitute_value_placeholder(
                                    *test_expr,
                                    column_ref.clone(),
                                )?),
                                value: arm_value,
                                result: Box::new(substitute_value_placeholder(
                                    *result,
                                    column_ref.clone(),
                                )?),
                            }),
                            CaseArm::CurriedSimple {
                                value: arm_value,
                                result,
                            } => {
                                // For curried CASE, inject the column reference as the test expression
                                // First convert SQL column reference to AST format
                                let ast_test_expr = match column_ref.clone() {
                                    SqlDomainExpression::Column { name, qualifier, .. } => {
                                        let namespace_path = match &qualifier {
                                            Some(q) => match q.parts() {
                                                crate::pipeline::sql_ast_v3::QualifierParts::Table(_) => {
                                                    crate::pipeline::asts::addressed::NamespacePath::empty()
                                                }
                                                crate::pipeline::sql_ast_v3::QualifierParts::SchemaTable { schema, .. }
                                                | crate::pipeline::sql_ast_v3::QualifierParts::DatabaseSchemaTable { schema, .. } => {
                                                    crate::pipeline::asts::addressed::NamespacePath::single(schema)
                                                }
                                            },
                                            None => crate::pipeline::asts::addressed::NamespacePath::empty(),
                                        };
                                        let ast_qualifier = qualifier.as_ref().map(|q| q.table_name().to_string());
                                        AstDomainExpression::Lvar {
                                            name: name.into(),
                                            qualifier: ast_qualifier.map(|s| s.into()),
                                            namespace_path,
                                            alias: None,
                                            provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
                                        }
                                    }
                                    _ => {
                                        return Err(crate::error::DelightQLError::ParseError {
                                            message:
                                                "Complex expressions in curried CASE not yet supported"
                                                    .to_string(),
                                            source: None,
                                            subcategory: None,
                                        });
                                    }
                                };

                                Ok(CaseArm::Simple {
                                    test_expr: Box::new(ast_test_expr),
                                    value: arm_value,
                                    result: Box::new(substitute_value_placeholder(
                                        *result,
                                        column_ref.clone(),
                                    )?),
                                })
                            }
                            CaseArm::Searched { condition, result } => {
                                // TODO: Substitute in boolean condition if needed
                                Ok(CaseArm::Searched {
                                    condition,
                                    result: Box::new(substitute_value_placeholder(
                                        *result,
                                        column_ref.clone(),
                                    )?),
                                })
                            }
                            CaseArm::Default { result } => Ok(CaseArm::Default {
                                result: Box::new(substitute_value_placeholder(
                                    *result,
                                    column_ref.clone(),
                                )?),
                            }),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    FunctionExpression::CaseExpression {
                        arms: substituted_arms,
                        alias,
                    }
                }
                FunctionExpression::HigherOrder {
                    name,
                    curried_arguments,
                    regular_arguments,
                    alias,
                    conditioned_on,
                } => {
                    let substituted_curried_args = curried_arguments
                        .into_iter()
                        .map(|arg| substitute_value_placeholder(arg, column_ref.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    let substituted_regular_args = regular_arguments
                        .into_iter()
                        .map(|arg| substitute_value_placeholder(arg, column_ref.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::HigherOrder {
                        name,
                        curried_arguments: substituted_curried_args,
                        regular_arguments: substituted_regular_args,
                        alias,
                        conditioned_on,
                    }
                }
                FunctionExpression::Curly {
                    members,
                    inner_grouping_keys,
                    cte_requirements,
                    alias,
                } => {
                    // Tree groups don't contain value placeholders (Epoch 1)
                    FunctionExpression::Curly {
                        members,
                        inner_grouping_keys,
                        cte_requirements,
                        alias,
                    }
                }
                FunctionExpression::MetadataTreeGroup {
                    key_column,
                    key_qualifier,
                    key_schema,
                    constructor,
                    keys_only,
                    cte_requirements,
                    alias,
                } => {
                    // Tree groups don't contain value placeholders (Epoch 1)
                    FunctionExpression::MetadataTreeGroup {
                        key_column,
                        key_qualifier,
                        key_schema,
                        constructor,
                        keys_only,
                        cte_requirements,
                        alias,
                    }
                }
                FunctionExpression::Window {
                    name,
                    arguments,
                    partition_by,
                    order_by,
                    frame,
                    alias,
                } => {
                    // Substitute @ placeholders in window function arguments
                    let substituted_args = arguments
                        .into_iter()
                        .map(|arg| substitute_value_placeholder(arg, column_ref.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    let substituted_partition = partition_by
                        .into_iter()
                        .map(|arg| substitute_value_placeholder(arg, column_ref.clone()))
                        .collect::<Result<Vec<_>>>()?;
                    let substituted_order = order_by
                        .into_iter()
                        .map(|spec| {
                            Ok(crate::pipeline::asts::addressed::OrderingSpec {
                                column: substitute_value_placeholder(
                                    spec.column,
                                    column_ref.clone(),
                                )?,
                                direction: spec.direction,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    FunctionExpression::Window {
                        name,
                        arguments: substituted_args,
                        partition_by: substituted_partition,
                        order_by: substituted_order,
                        frame,
                        alias,
                    }
                }
                _ => unimplemented!("JsonPath not yet implemented in this phase"),
            };
            Ok(AstDomainExpression::Function(substituted_func))
        }

        // TODO: Handle boolean expressions separately, not wrapped in DomainExpression

        // Recursively substitute in parenthesized expressions
        AstDomainExpression::Parenthesized { inner, alias } => {
            let substituted_inner = Box::new(substitute_value_placeholder(*inner, column_ref)?);
            Ok(AstDomainExpression::Parenthesized {
                inner: substituted_inner,
                alias,
            })
        }

        // Other expression types pass through unchanged
        other => Ok(other),
    }
}
