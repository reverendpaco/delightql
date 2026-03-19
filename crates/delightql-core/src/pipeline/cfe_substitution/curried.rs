/// Curried parameter handling for HOCFEs
///
/// This module handles extraction and merging of function expressions from curried code arguments.
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed as ast;
use std::collections::HashMap;

use super::domain::substitute_in_domain_expression_with_curried;

/// Extract a function expression from curried code and apply additional arguments/conditions
/// When we have transform:() in the CFE body and "transform" maps to code like upper:(),
/// we extract the function from the code and merge in any additional arguments from the call site
pub fn extract_function_from_code(
    curried_code: &ast::DomainExpression,
    call_site_args: Vec<ast::DomainExpression>,
    call_site_alias: Option<String>,
    call_site_condition: Option<Box<ast::BooleanExpression>>,
    curried_substitutions: &HashMap<String, ast::DomainExpression>,
    regular_substitutions: &HashMap<String, ast::DomainExpression>,
) -> Result<ast::FunctionExpression> {
    // The curried code should be a Function expression (like upper:())
    match curried_code {
        ast::DomainExpression::Function(func) => {
            // Merge arguments: code's args + call site args (after substitution)
            let mut merged_func = func.clone();

            // If call site has arguments, we need to append them (or merge somehow)
            // For now, simple case: if code is upper:() and call site is transform:(arg1, arg2),
            // result is upper:(arg1, arg2)
            if !call_site_args.is_empty() {
                let substituted_call_args: Vec<ast::DomainExpression> = call_site_args
                    .into_iter()
                    .map(|arg| {
                        substitute_in_domain_expression_with_curried(
                            arg,
                            curried_substitutions,
                            regular_substitutions,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Replace the function's arguments with call site arguments
                merged_func = match merged_func {
                    ast::FunctionExpression::Regular {
                        name,
                        namespace,
                        alias,
                        conditioned_on,
                        ..
                    } => ast::FunctionExpression::Regular {
                        name,
                        namespace,
                        arguments: substituted_call_args,
                        alias: call_site_alias.map(|s| s.into()).or(alias),
                        conditioned_on: call_site_condition.or(conditioned_on),
                    },
                    ast::FunctionExpression::Curried {
                        name,
                        namespace,
                        conditioned_on,
                        ..
                    } => ast::FunctionExpression::Curried {
                        name,
                        namespace,
                        arguments: substituted_call_args,
                        conditioned_on: call_site_condition.or(conditioned_on),
                    },
                    ast::FunctionExpression::Window {
                        name,
                        arguments: _,
                        partition_by,
                        order_by,
                        frame,
                        alias,
                    } => ast::FunctionExpression::Window {
                        name,
                        arguments: substituted_call_args,
                        partition_by,
                        order_by,
                        frame,
                        alias: call_site_alias.map(|s| s.into()).or(alias),
                    },
                    other => other, // Other function types don't have arguments in the same way
                };
            }

            Ok(merged_func)
        }
        _ => Err(DelightQLError::ParseError {
            message: format!(
                "Curried parameter must hold a function expression, got: {:?}",
                std::mem::discriminant(curried_code)
            ),
            source: None,
            subcategory: None,
        }),
    }
}
