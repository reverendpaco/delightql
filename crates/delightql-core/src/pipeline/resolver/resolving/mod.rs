// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Modularized resolver components
// These handle the actual resolution of AST nodes from unresolved to resolved state

pub(super) mod domain_expressions;
pub(super) mod functions;
pub(super) mod helpers;
pub(crate) mod operators;
pub(crate) mod predicates;
pub(crate) mod tree_group_analysis;

// Re-export the public interface functions for use by the resolver
pub(in crate::pipeline::resolver) use predicates::build_using_all_correlation_filters;
pub(in crate::pipeline::resolver) use predicates::build_using_correlation_filters;
pub(in crate::pipeline::resolver) use predicates::synthesize_using_correlation;

pub(in crate::pipeline::resolver) use operators::resolve_operator_via_fold;

/// Resolve a domain expression via the fold path with a given ResolverCore.
/// Entry point for CFE instantiation and DDL resolution — both use the same
/// fold-based walk as query resolution, just with a restricted registry.
pub(crate) fn resolve_domain_expr_via_registry(
    expr: crate::pipeline::ast_unresolved::DomainExpression,
    registry: &mut crate::resolution::ResolverCore,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> crate::error::Result<crate::pipeline::ast_resolved::DomainExpression> {
    use crate::pipeline::ast_transform::AstTransform;
    let mut env = detached_env();
    declared_fold(registry, &mut env, available, in_correlation)?.transform_domain(expr)
}

/// The same door for a TRUTH. A DDL CHECK's body is a truth, so it resolves
/// through the truth walk rather than through a value walk that would have to
/// carry one.
pub(crate) fn resolve_truth_via_registry(
    expr: crate::pipeline::ast_unresolved::TruthExpression,
    registry: &mut crate::resolution::ResolverCore,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
) -> crate::error::Result<crate::pipeline::ast_resolved::TruthExpression> {
    resolve_truth_in_correlation_via_registry(expr, registry, available, false)
}

/// The truth door, for a caller that knows whether it stands in a
/// correlation — a CFE body does, and a DDL constraint does not.
pub(crate) fn resolve_truth_in_correlation_via_registry(
    expr: crate::pipeline::ast_unresolved::TruthExpression,
    registry: &mut crate::resolution::ResolverCore,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> crate::error::Result<crate::pipeline::ast_resolved::TruthExpression> {
    use crate::pipeline::ast_transform::AstTransform;
    let mut env = detached_env();
    declared_fold(registry, &mut env, available, in_correlation)?.transform_boolean(expr)
}

/// The DETACHED world both DDL doors stand in: no session, no reach — the
/// declared columns below are the entire environment.
fn detached_env() -> crate::defuse::environment::Environment {
    crate::defuse::environment::Environment::Use(
        crate::defuse::environment::UseEnvironment::detached(),
    )
}

/// One fold over the DECLARED environment: the door's columns derive the
/// relation the walk stands in, and nothing else is nameable.
fn declared_fold<'a, 'db>(
    registry: &'a mut crate::resolution::ResolverCore<'db>,
    env: &'a mut crate::defuse::environment::Environment,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> crate::error::Result<super::resolver_fold::ResolverFold<'a, 'db>> {
    // The door's columns are the one row the walk stands over, declared
    // and stood over by the lexical authority's own act; the row answers to
    // nothing, so no qualifier reaches anything here.
    let standing = declared_environment(registry, available)?;
    let mut fold =
        super::resolver_fold::ResolverFold::new(registry, env, super::ResolutionConfig::default());
    fold.lexical.enter(standing, super::Reach::Row);
    fold.in_correlation = in_correlation;
    Ok(fold)
}

fn declared_environment(
    registry: &crate::resolution::ResolverCore,
    available: &[crate::pipeline::ast_resolved::ColumnMetadata],
) -> crate::error::Result<super::ResolvedRelation> {
    let slots: Vec<_> = available
        .iter()
        .enumerate()
        .map(|(position, metadata)| {
            let column = metadata.identity();
            let named = registry.identities.published(column).ok_or_else(|| {
                crate::error::DelightQLError::database_error(
                    "a declared DDL column has no published name",
                    "DDL declaration environment",
                )
            })?;
            Ok(crate::relation::form::AnonymousSlot::Binder {
                position: position as u32,
                named,
                declared_type: registry.identities.facts(column).declared_type,
                shape: registry.identities.facts(column).shape,
            })
        })
        .collect::<crate::error::Result<_>>()?;
    super::ResolvedRelation::declared_row(
        crate::relation::form::AnonymousSpec {
            shape: crate::relation::form::AnonymousShape::Tabular,
            slots: &slots,
            answers_to: None,
        },
        &registry.identities,
    )
}
