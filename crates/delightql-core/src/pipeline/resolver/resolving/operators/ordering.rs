use crate::error::Result;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

/// Resolve the TupleOrdering operator (ORDER BY) via fold-based dispatch
///
/// Same semantics as `resolve_tuple_ordering`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(super) fn resolve_tuple_ordering_via_fold(
    fold: &mut ResolverFold,
    containment_semantic: ast_unresolved::ContainmentSemantic,
    specs: Vec<ast_unresolved::OrderingSpec>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Resolve ORDER BY specs
    let resolved_specs = specs
        .into_iter()
        .map(|spec| {
            super::super::domain_expressions::projection::resolve_expressions_via_fold(fold, vec![spec.column], available, false)
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

    let resolved_op = ast_resolved::UnaryRelationalOperator::TupleOrdering {
        containment_semantic:
            super::super::super::helpers::converters::convert_containment_semantic(
                containment_semantic,
            ),
        specs: resolved_specs,
    };

    // ORDER BY doesn't change columns
    Ok((resolved_op, available.to_vec()))
}
