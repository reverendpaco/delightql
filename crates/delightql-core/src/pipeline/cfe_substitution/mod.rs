/// CFE (Common Function Expression) parameter substitution
///
/// This module handles substituting CFE parameters with argument expressions during transformation.
/// CFE bodies are already precompiled (resolved+refined) with Parameter nodes marking substitution holes.
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::addressed as ast;

// Submodules
mod curried;
mod domain;

// Re-export public functions
pub use domain::substitute_in_domain_expression_with_curried;

/// Check if a function name corresponds to a CFE invocation
pub fn is_cfe_invocation(func_name: &str, cfes: &[ast::PrecompiledCfeDefinition]) -> bool {
    cfes.iter().any(|cfe| cfe.name == func_name)
}

/// Lookup a CFE by name
pub fn lookup_cfe<'a>(
    func_name: &str,
    cfes: &'a [ast::PrecompiledCfeDefinition],
) -> Option<&'a ast::PrecompiledCfeDefinition> {
    cfes.iter().find(|cfe| cfe.name == func_name)
}

/// Check if arguments represent a context-aware call (first arg is ContextMarker)
pub fn is_context_aware_call(arguments: &[ast::DomainExpression]) -> bool {
    matches!(
        arguments.first(),
        Some(ast::DomainExpression::Substitution(
            crate::pipeline::asts::core::SubstitutionExpr::ContextMarker
        ))
    )
}

/// Substitute CFE with context-aware call: f:(.., args)
/// Grabs context parameters from scope by creating unqualified Lvar references
/// The existing resolution/transformation will handle finding these in the current scope
pub fn substitute_cfe_with_context(
    cfe: &ast::PrecompiledCfeDefinition,
    arguments: &[ast::DomainExpression], // Includes ContextMarker as first element
) -> Result<ast::DomainExpression> {
    // Skip the ContextMarker (first element)
    let regular_args = if arguments.is_empty() {
        &[]
    } else {
        &arguments[1..]
    };

    // Validate argument count (regular params only)
    if regular_args.len() != cfe.parameters.len() {
        return Err(DelightQLError::ParseError {
            message: format!(
                "CFE '{}' expects {} arguments, got {}",
                cfe.name,
                cfe.parameters.len(),
                regular_args.len()
            ),
            source: None,
            subcategory: None,
        });
    }

    let mut regular_substitutions = std::collections::HashMap::new();

    // Substitute context params with unqualified Lvar references
    // These will resolve to columns in the current scope during SQL generation
    for ctx_param in &cfe.context_params {
        regular_substitutions.insert(
            ctx_param.clone(),
            ast::DomainExpression::Lvar {
                name: ctx_param.clone().into(),
                qualifier: None, // Unqualified - will resolve to current scope
                namespace_path: ast::NamespacePath::empty(),
                alias: None,
                provenance: ast::PhaseBox::phantom(),
            },
        );
    }

    // Substitute regular params from arguments
    for (param, arg) in cfe.parameters.iter().zip(regular_args) {
        regular_substitutions.insert(param.clone(), arg.clone());
    }

    // No curried params for now (empty map)
    let curried_substitutions = std::collections::HashMap::new();

    // Perform substitution on CFE body
    domain::substitute_in_domain_expression_with_curried(
        cfe.body.clone().into(),
        &curried_substitutions,
        &regular_substitutions,
    )
}

/// Substitute CFE parameters with argument expressions (HOCFE-aware)
///
/// Takes a precompiled CFE body (with Parameter and CurriedParameter nodes) and argument lists,
/// and recursively replaces all parameter nodes with the corresponding arguments.
/// For HOCFEs, curried_arguments go to CurriedParameter nodes, regular_arguments go to Parameter nodes.
pub fn substitute_cfe_parameters_with_curried(
    cfe_body: ast::DomainExpression,
    curried_arguments: Vec<ast::DomainExpression>,
    regular_arguments: Vec<ast::DomainExpression>,
    curried_param_names: &[String],
    regular_param_names: &[String],
) -> Result<ast::DomainExpression> {
    log::debug!(
        "HOCFE substitution: curried_params={:?} (count={}), regular_params={:?} (count={})",
        curried_param_names,
        curried_arguments.len(),
        regular_param_names,
        regular_arguments.len()
    );

    if curried_arguments.len() != curried_param_names.len() {
        return Err(DelightQLError::ParseError {
            message: format!(
                "HOCFE expects {} curried arguments, got {}",
                curried_param_names.len(),
                curried_arguments.len()
            ),
            source: None,
            subcategory: None,
        });
    }

    if regular_arguments.len() != regular_param_names.len() {
        return Err(DelightQLError::ParseError {
            message: format!(
                "HOCFE expects {} regular arguments, got {}",
                regular_param_names.len(),
                regular_arguments.len()
            ),
            source: None,
            subcategory: None,
        });
    }

    // Build substitution maps for both types
    let mut curried_substitutions = std::collections::HashMap::new();
    for (param_name, arg_expr) in curried_param_names
        .iter()
        .zip(curried_arguments.into_iter())
    {
        log::debug!(
            "HOCFE curried substitution: mapping {} => {:?}",
            param_name,
            std::mem::discriminant(&arg_expr)
        );

        // Validate: if passing a function as HOCFE parameter, it must have no arguments
        if let ast::DomainExpression::Function(func_expr) = &arg_expr {
            log::debug!(
                "Checking curried parameter - function variant: {:?}",
                std::mem::discriminant(func_expr)
            );

            // Check all function types that could have arguments
            match func_expr {
                ast::FunctionExpression::Regular {
                    name, arguments, ..
                } => {
                    log::debug!(
                        "Found Regular function {} with {} args",
                        name,
                        arguments.len()
                    );
                    if !arguments.is_empty() {
                        return Err(DelightQLError::ParseError {
                            message: format!(
                                "HOCFE function parameters must have no arguments (like f:()), not partial applications.\n\
                                 Found {}:(...) with {} argument(s).\n\
                                 Higher-order CFEs can take functions (higher-order) or values (lower-order), but functions must be bare references.",
                                name,
                                arguments.len()
                            ),
                            source: None,
                            subcategory: None,
                        });
                    }
                }
                ast::FunctionExpression::Curried {
                    name, arguments, ..
                } => {
                    log::debug!(
                        "Found Curried function {} with {} args",
                        name,
                        arguments.len()
                    );
                    if !arguments.is_empty() {
                        return Err(DelightQLError::ParseError {
                            message: format!(
                                "HOCFE function parameters must have no arguments (like f:()), not partial applications.\n\
                                 Found {}:(...) with {} argument(s).\n\
                                 Higher-order CFEs can take functions (higher-order) or values (lower-order), but functions must be bare references.",
                                name,
                                arguments.len()
                            ),
                            source: None,
                            subcategory: None,
                        });
                    }
                }
                ast::FunctionExpression::Window {
                    name, arguments, ..
                } => {
                    log::debug!(
                        "Found Window function {} with {} args",
                        name,
                        arguments.len()
                    );
                    if !arguments.is_empty() {
                        return Err(DelightQLError::ParseError {
                            message: format!(
                                "HOCFE function parameters must have no arguments (like f:()), not partial applications.\n\
                                 Found {}:(...) with {} argument(s).\n\
                                 Higher-order CFEs can take functions (higher-order) or values (lower-order), but functions must be bare references.",
                                name,
                                arguments.len()
                            ),
                            source: None,
                            subcategory: None,
                        });
                    }
                }
                _ => {
                    log::debug!(
                        "Not a parameterized function type (Lambda, HigherOrder, etc.) - allowed"
                    );
                }
            }
        }

        curried_substitutions.insert(param_name.clone(), arg_expr);
    }

    let mut regular_substitutions = std::collections::HashMap::new();
    for (param_name, arg_expr) in regular_param_names
        .iter()
        .zip(regular_arguments.into_iter())
    {
        log::debug!(
            "HOCFE regular substitution: mapping {} => {:?}",
            param_name,
            std::mem::discriminant(&arg_expr)
        );
        regular_substitutions.insert(param_name.clone(), arg_expr);
    }

    // Recursively substitute both types of parameters
    substitute_in_domain_expression_with_curried(
        cfe_body,
        &curried_substitutions,
        &regular_substitutions,
    )
}

/// Substitute CFE parameters with argument expressions (regular CFEs only - backward compatible)
///
/// Takes a precompiled CFE body (with Parameter nodes) and a list of argument expressions,
/// and recursively replaces all Parameter nodes with the corresponding arguments.
pub fn substitute_cfe_parameters(
    cfe_body: ast::DomainExpression,
    arguments: Vec<ast::DomainExpression>,
    param_names: &[String],
) -> Result<ast::DomainExpression> {
    // Delegate to HOCFE version with empty curried params (backward compatibility)
    substitute_cfe_parameters_with_curried(cfe_body, vec![], arguments, &[], param_names)
}

/// Substitute CFE with positional context parameters
///
/// For explicit context CFEs called positionally: f:(ctx1, ctx2, regular)
/// Context params are ContextParameter nodes (looked up in regular_substitutions)
/// Regular params are Parameter nodes (also looked up in regular_substitutions)
pub fn substitute_cfe_positional_with_context(
    cfe_body: ast::DomainExpression,
    context_arguments: Vec<ast::DomainExpression>,
    regular_arguments: Vec<ast::DomainExpression>,
    context_param_names: &[String],
    regular_param_names: &[String],
) -> Result<ast::DomainExpression> {
    log::debug!(
        "Positional context substitution: context_params={:?} (count={}), regular_params={:?} (count={})",
        context_param_names,
        context_arguments.len(),
        regular_param_names,
        regular_arguments.len()
    );

    // Validate argument counts
    if context_arguments.len() != context_param_names.len() {
        return Err(DelightQLError::ParseError {
            message: format!(
                "Expected {} context arguments, got {}",
                context_param_names.len(),
                context_arguments.len()
            ),
            source: None,
            subcategory: None,
        });
    }

    if regular_arguments.len() != regular_param_names.len() {
        return Err(DelightQLError::ParseError {
            message: format!(
                "Expected {} regular arguments, got {}",
                regular_param_names.len(),
                regular_arguments.len()
            ),
            source: None,
            subcategory: None,
        });
    }

    // Build substitution maps - both context and regular params go in regular_substitutions
    // because ContextParameter and Parameter nodes both use regular_substitutions
    let mut regular_substitutions = std::collections::HashMap::new();

    // Add context param substitutions
    for (param_name, arg_expr) in context_param_names
        .iter()
        .zip(context_arguments.into_iter())
    {
        log::debug!(
            "Context substitution: mapping {} => {:?}",
            param_name,
            std::mem::discriminant(&arg_expr)
        );
        regular_substitutions.insert(param_name.clone(), arg_expr);
    }

    // Add regular param substitutions
    for (param_name, arg_expr) in regular_param_names
        .iter()
        .zip(regular_arguments.into_iter())
    {
        log::debug!(
            "Regular substitution: mapping {} => {:?}",
            param_name,
            std::mem::discriminant(&arg_expr)
        );
        regular_substitutions.insert(param_name.clone(), arg_expr);
    }

    // No curried params (empty map)
    let curried_substitutions = std::collections::HashMap::new();

    // Recursively substitute
    substitute_in_domain_expression_with_curried(
        cfe_body,
        &curried_substitutions,
        &regular_substitutions,
    )
}
