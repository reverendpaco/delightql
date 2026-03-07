// Modularized resolver components
// These handle the actual resolution of AST nodes from unresolved to resolved state

pub(super) mod column_extraction;
pub(super) mod domain_expressions;
pub(super) mod functions;
pub(super) mod helpers;
mod operators;
pub(super) mod predicates;
pub(super) mod tree_group_analysis;

// Re-export the public interface functions for use by the resolver
pub(in crate::pipeline::resolver) use predicates::build_using_correlation_filters;
pub(in crate::pipeline::resolver) use predicates::synthesize_using_correlation;

pub(in crate::pipeline::resolver) use operators::resolve_operator_via_fold;

/// Resolve a domain expression via the fold path with a given EntityRegistry.
/// Entry point for CFE precompilation and DDL resolution — both use the same
/// fold-based walk as query resolution, just with a restricted registry.
pub(crate) fn resolve_domain_expr_via_registry(
    expr: crate::pipeline::ast_unresolved::DomainExpression,
    registry: &mut crate::resolution::EntityRegistry,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> crate::error::Result<crate::pipeline::ast_resolved::DomainExpression> {
    use crate::pipeline::ast_transform::AstTransform;
    let config = super::ResolutionConfig::default();
    let mut fold = super::resolver_fold::ResolverFold::new(registry, config, None, None);
    fold.available = available.to_vec();
    fold.in_correlation = in_correlation;
    fold.transform_domain(expr)
}
