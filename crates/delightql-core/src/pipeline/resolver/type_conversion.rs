// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Type conversion utilities for resolver
//!
//! This module contains pure conversion functions that transform unresolved AST nodes
//! to their resolved counterparts. These are used during the resolution process.
//!
//! The core conversion logic is provided by `PhaseConverter`, a no-op `AstTransform`
//! implementor that uses the default walk functions for Unresolved → Resolved phase
//! conversion, overriding only the variants that need special handling.

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_resolved::Resolved;
use crate::pipeline::ast_transform::{walk_transform_boolean, walk_transform_domain, AstTransform};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{DomainExpression, TruthExpression, Unresolved};
use crate::pipeline::asts::core::{Existence, RelationalMembership};

/// Unresolved → Resolved by the default walks, for a subtree whose names are
/// already bound. It looks nothing up: every payload the authored phase does
/// not carry is refused here, so a name this converter is handed unbound
/// leaves as an error rather than as a shape somebody else has to interpret.
struct PhaseConverter<'a> {
    identities: &'a crate::names::Registry,
}

impl AstTransform<Unresolved, Resolved> for PhaseConverter<'_> {
    crate::pipeline::ast_transform::position_is_resolved_against_a_heading!();
    fn fold_entity(
        &mut self,
        entity: crate::pipeline::asts::vocabulary::Ref,
    ) -> crate::error::Result<crate::names::CallableId> {
        Ok(entity.written_call_identity(self.identities))
    }
    crate::pipeline::ast_transform::column_is_bound_where_it_is_resolved!();
    crate::pipeline::ast_transform::binder_is_bound_where_the_pattern_is_resolved!();
    crate::pipeline::ast_transform::a_landing_is_consumed_where_the_pipe_is_applied!();
    crate::pipeline::ast_transform::a_context_marker_is_consumed_where_the_call_instantiates!();
    crate::pipeline::ast_transform::scope_is_minted_where_it_is_resolved!();
    crate::pipeline::ast_transform::minted_where_it_is_decided!(
        fold_output -> crate::relation::PortId: "an expression's output port",
        fold_scalar_output -> crate::relation::PortId: "a scalarized relation's column",
        fold_destructure -> Vec<crate::pipeline::asts::core::DestructureMapping>: "a destructuring pattern's columns",
    );
    fn fold_open_leaf(
        &mut self,
        _: crate::pipeline::asts::core::DomainHole,
    ) -> crate::error::Result<crate::pipeline::asts::core::FormalHole> {
        Err(crate::error::DelightQLError::validation_error_categorized(
            "value/open/unapplied",
            "a composition input stands outside any callable applying it",
            "the position that applies an open body spends its slot",
        ))
    }

    fn fold_cover_callable(
        &mut self,
        _: crate::pipeline::asts::core::Callable<crate::pipeline::asts::core::Unresolved>,
    ) -> crate::error::Result<()> {
        Err(crate::error::DelightQLError::transformation_error(
            "a cover's callable is applied where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn fold_rename_target(
        &mut self,
        _: crate::pipeline::asts::core::NameTarget,
    ) -> crate::error::Result<crate::names::Spelling> {
        Err(crate::error::DelightQLError::transformation_error(
            "a rename target is expanded where the rename resolves, and this fold is not that place",
            "phase_payload",
        ))
    }
    fn fold_drill(
        &mut self,
        _: crate::pipeline::asts::core::operators::AuthoredDrill,
    ) -> crate::error::Result<crate::pipeline::asts::core::operators::BoundDrill> {
        Err(crate::error::DelightQLError::transformation_error(
            "an interior drill binds where its operator resolves, and this fold is not that place",
            "phase_payload",
        ))
    }

    fn transform_domain(
        &mut self,
        expr: DomainExpression<Unresolved>,
    ) -> Result<DomainExpression<Resolved>> {
        match expr {
            other => walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: TruthExpression<Unresolved>,
    ) -> Result<TruthExpression<Resolved>> {
        match expr {
            // A relational membership test needs its subquery RESOLVED, and
            // this conversion resolves scalars. Answering with a ground
            // relation built from the written name would drop the subquery
            // it was handed — a relation nobody resolved standing in for one
            // somebody wrote — so it refuses instead.
            TruthExpression::Existence(Existence { addressing, .. })
            | TruthExpression::RelationalMembership(RelationalMembership { addressing, .. }) => {
                let identifier = &addressing.identifier;
                Err(DelightQLError::validation_error(
                    format!(
                        "a relational membership test over '{}' cannot be resolved here",
                        identifier.name
                    ),
                    "in a position that admits only scalar expressions",
                ))
            }
            other => walk_transform_boolean(self, other),
        }
    }
}

/// Helper function to convert unresolved DomainExpression to resolved
pub(super) fn convert_domain_expression(
    expr: &ast_unresolved::DomainExpression,
    identities: &crate::relation::Planning,
) -> Result<ast_resolved::DomainExpression> {
    PhaseConverter { identities }.transform_domain(expr.clone())
}

/// Convert unresolved QualifiedName to resolved QualifiedName
pub(super) fn convert_qualified_name(
    id: ast_unresolved::QualifiedName,
) -> ast_resolved::QualifiedName {
    ast_resolved::QualifiedName {
        namespace_path: id.namespace_path,
        name: id.name,
    }
}
