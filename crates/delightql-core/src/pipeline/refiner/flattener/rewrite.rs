// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structural rewrites needed while correlation predicates cross a boundary.

use crate::error::Result;
use crate::pipeline::ast_transform::{self, AstTransform};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::{Existence, RelationalMembership};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;

struct HygienicRewrite<'a> {
    carriers: &'a [(crate::relation::PortId, crate::relation::PortId)],
}

impl AstTransform<resolved::Resolved, resolved::Resolved> for HygienicRewrite<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(resolved::Resolved);

    fn transform_domain(
        &mut self,
        expr: resolved::DomainExpression,
    ) -> Result<resolved::DomainExpression> {
        match expr {
            resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                occurrence @ ColumnOccurrence { .. },
            ))) => {
                let column = occurrence.column;
                let mut carrying = self
                    .carriers
                    .iter()
                    .filter(|(source, _)| *source == column)
                    .map(|(_, output)| *output);
                // Two carriers standing for one value have equal claim, and
                // taking the first decides which one the condition meant.
                let column = match (carrying.next(), carrying.next()) {
                    (None, _) => column,
                    (Some(carrier), None) => carrier,
                    (Some(_), Some(_)) => {
                        return Err(crate::error::DelightQLError::parse_error(format!(
                            "{column:?} is carried more than once here, so a hoisted condition \
                             naming it stands for no single column"
                        )))
                    }
                };
                Ok(resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(occurrence.rebound(column)),
                )))
            }
            other => ast_transform::walk_transform_domain(self, other),
        }
    }

    fn transform_boolean(
        &mut self,
        expr: resolved::TruthExpression,
    ) -> Result<resolved::TruthExpression> {
        match expr {
            // Nested subqueries own separate scopes and separate injections.
            other @ resolved::TruthExpression::Existence(Existence { .. })
            | other @ resolved::TruthExpression::RelationalMembership(RelationalMembership {
                ..
            }) => Ok(other),
            other => ast_transform::walk_transform_boolean(self, other),
        }
    }
}

/// Re-point a hoisted condition at the carriers the subquery publishes.
///
/// A condition lifted out of a subquery names the occurrences it was resolved
/// against; where the projection stripped one, the subquery publishes a
/// hygienic carrier for it instead, and the condition has to name that. The
/// carriers come from the subquery itself — see
/// `pattern_classifier::correlation_carriers`.
pub(super) fn rewrite_with_hygienic_names(
    expr: resolved::TruthExpression,
    carriers: &[(crate::relation::PortId, crate::relation::PortId)],
) -> Result<resolved::TruthExpression> {
    let mut fold = HygienicRewrite { carriers };
    fold.transform_boolean(expr)
}
