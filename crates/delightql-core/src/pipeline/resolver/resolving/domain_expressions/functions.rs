use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Resolve a function expression (including string templates)
pub(super) fn resolve_function_expr(
    func: ast_unresolved::FunctionExpression,
    available: &[ast_resolved::ColumnMetadata],
    schema: Option<&dyn super::super::super::DatabaseSchema>,
    mut cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    if let ast_unresolved::FunctionExpression::StringTemplate { parts, alias } = func {
        // String templates need special handling - they get converted to CONCAT chains
        let mut resolved_parts: Vec<ast_resolved::StringTemplatePart<ast_resolved::Resolved>> =
            Vec::new();
        for part in parts {
            match part {
                ast_unresolved::StringTemplatePart::Text(text) => {
                    resolved_parts.push(
                        ast_resolved::StringTemplatePart::<ast_resolved::Resolved>::Text(text),
                    );
                }
                ast_unresolved::StringTemplatePart::Interpolation(expr) => {
                    let resolved_expr = match &mut cte_context {
                        Some(ctx) => super::resolve_domain_expr_with_schema_and_context(
                            *expr,
                            available,
                            schema,
                            Some(*ctx),
                            in_correlation,
                            cfe_defs,
                            None,
                        )?,
                        None => super::resolve_domain_expr_with_schema_and_context(
                            *expr,
                            available,
                            schema,
                            None,
                            in_correlation,
                            cfe_defs,
                            None,
                        )?,
                    };
                    resolved_parts.push(
                        ast_resolved::StringTemplatePart::<ast_resolved::Resolved>::Interpolation(
                            Box::new(resolved_expr),
                        ),
                    );
                }
            }
        }

        Ok(super::super::super::string_templates::build_concat_chain(
            resolved_parts,
            alias,
        ))
    } else {
        // Need to pass in_correlation through function resolution
        let resolved_func = super::super::functions::resolve_function_expression_with_context(
            func,
            available,
            schema,
            cte_context,
            in_correlation,
            cfe_defs,
        )?;
        Ok(ast_resolved::DomainExpression::Function(resolved_func))
    }
}

/// Resolve a function expression using the shared registry
pub(super) fn resolve_function_expr_with_registry(
    func: ast_unresolved::FunctionExpression,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
) -> Result<ast_resolved::DomainExpression> {
    if let ast_unresolved::FunctionExpression::StringTemplate { parts, alias } = func {
        let mut resolved_parts: Vec<ast_resolved::StringTemplatePart<ast_resolved::Resolved>> =
            Vec::new();
        for part in parts {
            match part {
                ast_unresolved::StringTemplatePart::Text(text) => {
                    resolved_parts.push(
                        ast_resolved::StringTemplatePart::<ast_resolved::Resolved>::Text(text),
                    );
                }
                ast_unresolved::StringTemplatePart::Interpolation(expr) => {
                    let resolved_expr = super::resolve_domain_expr_with_registry(
                        *expr, available, registry, in_correlation,
                    )?;
                    resolved_parts.push(
                        ast_resolved::StringTemplatePart::<ast_resolved::Resolved>::Interpolation(
                            Box::new(resolved_expr),
                        ),
                    );
                }
            }
        }
        Ok(super::super::super::string_templates::build_concat_chain(
            resolved_parts,
            alias,
        ))
    } else {
        let resolved_func =
            super::super::functions::resolve_function_expression_with_registry(
                func, available, registry, in_correlation,
            )?;
        Ok(ast_resolved::DomainExpression::Function(resolved_func))
    }
}
