use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Resolve a piped expression (value |> transform1 |> transform2 ...)
pub(super) fn resolve_piped_expr(
    value: ast_unresolved::DomainExpression,
    transforms: Vec<ast_unresolved::FunctionExpression>,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    let resolved_value = super::resolve_domain_expr_with_schema(value, available, cfe_defs)?;
    let resolved_transforms = transforms
        .into_iter()
        .map(|t| {
            if let ast_unresolved::FunctionExpression::StringTemplate { parts, alias } = t {
                let concat_expr =
                    super::super::helpers::build_concat_chain_with_placeholders(parts)?;
                Ok(ast_resolved::FunctionExpression::Lambda {
                    body: Box::new(concat_expr),
                    alias,
                })
            } else {
                super::super::functions::resolve_function_with_schema(t, available, None)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ast_resolved::DomainExpression::PipedExpression {
        value: Box::new(resolved_value),
        transforms: resolved_transforms,
        alias: alias.map(|s| s.into()),
    })
}

/// Resolve a parenthesized expression
pub(super) fn resolve_parenthesized(
    inner: ast_unresolved::DomainExpression,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    schema: Option<&dyn super::super::super::DatabaseSchema>,
    mut cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    let resolved_inner = match &mut cte_context {
        Some(ctx) => super::resolve_domain_expr_with_schema_and_context(
            inner,
            available,
            schema,
            Some(*ctx),
            in_correlation,
            cfe_defs,
            None,
        )?,
        None => super::resolve_domain_expr_with_schema_and_context(
            inner,
            available,
            schema,
            None,
            in_correlation,
            cfe_defs,
            None,
        )?,
    };
    Ok(ast_resolved::DomainExpression::Parenthesized {
        inner: Box::new(resolved_inner),
        alias: alias.map(|s| s.into()),
    })
}

/// Resolve a parenthesized expression using the shared registry
pub(super) fn resolve_parenthesized_with_registry(
    inner: ast_unresolved::DomainExpression,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
) -> Result<ast_resolved::DomainExpression> {
    let resolved_inner =
        super::resolve_domain_expr_with_registry(inner, available, registry, in_correlation)?;
    Ok(ast_resolved::DomainExpression::Parenthesized {
        inner: Box::new(resolved_inner),
        alias: alias.map(|s| s.into()),
    })
}

/// Resolve a tuple expression
pub(super) fn resolve_tuple(
    elements: Vec<ast_unresolved::DomainExpression>,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    // EPOCH 5: Resolve each element in the tuple
    let mut resolved_elements = Vec::new();
    for elem in elements {
        resolved_elements.push(super::resolve_domain_expr_with_schema(
            elem, available, cfe_defs,
        )?);
    }
    Ok(ast_resolved::DomainExpression::Tuple {
        elements: resolved_elements,
        alias: alias.map(|s| s.into()),
    })
}
