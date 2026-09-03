// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// inner_cpr.rs - Shared helpers for Inner-CPR (Inner Column Preservation Rules) features
//
// Inner-CPR includes:
// - ScalarSubquery: orders:(~> count:(*))
// - InnerExists: +orders(, o.user_id = u.id)
// - Derived tables/Lateral joins: users(|> σ(age > 21))

use crate::error::Result;
use crate::pipeline::asts::unresolved as ast_unresolved;
use crate::pipeline::resolver::unification::ColumnReference;

/// Resolve an inner-CPR subquery during the bubbling phase, to learn what
/// it NEEDS.
///
/// This is the "double resolution": the subquery resolves during bubbling
/// only to extract its dependencies, and the resolved result is discarded.
/// It resolves in THE FOLD'S OWN WORLD — the same lexical environment the
/// real resolution will use — never in a transplanted copy of some other
/// world's bindings.
pub(in crate::pipeline::resolver) fn resolve_inner_cpr_during_bubbling(
    subquery: ast_unresolved::Chain,
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold<'_, '_>,
) -> Result<Vec<ColumnReference>> {
    Ok(fold.resolve_interior(subquery)?.into_needs())
}
