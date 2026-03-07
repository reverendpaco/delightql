use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// Resolve a scalar subquery expression using the fold walker
///
/// Uses the fold's registry and config to resolve the subquery, preserving
/// namespace, CTE, CFE, and grounding context.
pub(in crate::pipeline::resolver) fn resolve_scalar_subquery_via_fold(
    fold: &mut ResolverFold,
    identifier: ast_unresolved::QualifiedName,
    subquery: ast_unresolved::RelationalExpression,
    alias: Option<String>,
) -> Result<ast_resolved::DomainExpression> {
    let config = fold.config.clone();
    let (resolved_subquery, _) = super::super::super::resolve_relational_expression_with_registry(
        subquery,
        fold.registry,
        Some(&fold.available),
        &config,
        None,
    )?;

    Ok(ast_resolved::DomainExpression::ScalarSubquery {
        identifier,
        subquery: Box::new(resolved_subquery),
        alias: alias.map(|s| s.into()),
    })
}
