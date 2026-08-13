// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Modularized resolver components
// These handle the actual resolution of AST nodes from unresolved to resolved state

pub(super) mod column_extraction;
pub(super) mod domain_expressions;
pub(super) mod functions;
pub(super) mod helpers;
pub(in crate::pipeline::resolver) mod operators;
pub(super) mod predicates;
pub(super) mod tree_group_analysis;

// Re-export the public interface functions for use by the resolver
pub(in crate::pipeline::resolver) use predicates::build_using_all_correlation_filters;
pub(in crate::pipeline::resolver) use predicates::build_using_correlation_filters;
pub(in crate::pipeline::resolver) use predicates::synthesize_using_correlation;

pub(in crate::pipeline::resolver) use operators::resolve_operator_via_fold;

/// Resolve a domain expression via the fold path with a given EntityRegistry.
/// Entry point for CFE instantiation and DDL resolution — both use the same
/// fold-based walk as query resolution, just with a restricted registry.
pub(crate) fn resolve_domain_expr_via_registry(
    expr: crate::pipeline::ast_unresolved::DomainExpression,
    registry: &mut crate::resolution::EntityRegistry,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> crate::error::Result<crate::pipeline::ast_resolved::DomainExpression> {
    use crate::pipeline::ast_transform::AstTransform;
    let config = super::ResolutionConfig::default();
    let available: Vec<_> = available.iter().map(|column| column.identity()).collect();
    let qualifier_scope = available
        .iter()
        .map(|column| registry.identities.scope_of(*column))
        .collect();
    let mut fold = super::resolver_fold::ResolverFold::new(registry, config, None, None);
    fold.available = available;
    fold.local_available = fold.available.clone();
    fold.qualifier_scope = qualifier_scope;
    fold.in_correlation = in_correlation;
    fold.transform_domain(expr)
}

/// The same door for a TRUTH. A DDL CHECK's body is a truth, so it resolves
/// through the truth walk rather than through a value walk that would have to
/// carry one.
pub(crate) fn resolve_truth_via_registry(
    expr: crate::pipeline::ast_unresolved::TruthExpression,
    registry: &mut crate::resolution::EntityRegistry,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
) -> crate::error::Result<crate::pipeline::ast_resolved::TruthExpression> {
    resolve_truth_in_correlation_via_registry(expr, registry, available, false)
}

/// The truth door, for a caller that knows whether it stands in a
/// correlation — a CFE body does, and a DDL constraint does not.
pub(crate) fn resolve_truth_in_correlation_via_registry(
    expr: crate::pipeline::ast_unresolved::TruthExpression,
    registry: &mut crate::resolution::EntityRegistry,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> crate::error::Result<crate::pipeline::ast_resolved::TruthExpression> {
    use crate::pipeline::ast_transform::AstTransform;
    let config = super::ResolutionConfig::default();
    let available: Vec<_> = available.iter().map(|column| column.identity()).collect();
    let qualifier_scope = available
        .iter()
        .map(|column| registry.identities.scope_of(*column))
        .collect();
    let mut fold = super::resolver_fold::ResolverFold::new(registry, config, None, None);
    fold.available = available;
    fold.local_available = fold.available.clone();
    fold.qualifier_scope = qualifier_scope;
    fold.in_correlation = in_correlation;
    fold.transform_boolean(expr)
}
