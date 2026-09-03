// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::Result;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

/// Resolve the TupleOrdering operator (ORDER BY) via fold-based dispatch
///
/// Same semantics as `resolve_tuple_ordering`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(in crate::pipeline::resolver) fn resolve_tuple_ordering_via_fold(
    fold: &mut ResolverFold,
    specs: Vec<ast_unresolved::OrderingSpec>,
    available: &[crate::relation::PortId],
) -> Result<(
    Vec<ast_resolved::OrderingSpec>,
    Vec<crate::relation::PortId>,
)> {
    // Resolve ORDER BY specs
    let resolved_specs = specs
        .into_iter()
        .map(|spec| {
            super::super::domain_expressions::projection::resolve_expressions_via_fold(
                fold,
                vec![spec.column],
                available,
            )
            .map(|mut exprs| ast_resolved::OrderingSpec {
                column: exprs
                    .pop()
                    .expect("resolve_expressions_via_fold returns same count as input"),
                direction: super::super::super::helpers::converters::convert_order_direction(
                    spec.direction,
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // ORDER BY doesn't change columns
    Ok((resolved_specs, available.to_vec()))
}
