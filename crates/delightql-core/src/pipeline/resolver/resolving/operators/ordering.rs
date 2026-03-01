use crate::error::Result;
use crate::pipeline::{ast_resolved, ast_unresolved};

use super::super::domain_expressions::resolve_expressions_with_schema;

/// Resolve the TupleOrdering operator (ORDER BY)
///
/// This handles sorting operations that specify how to order rows.
/// Does not change the schema - output columns are identical to input.
pub(super) fn resolve_tuple_ordering(
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
            resolve_expressions_with_schema(vec![spec.column], available, None, None, None, false)
                .map(|mut exprs| ast_resolved::OrderingSpec {
                    column: exprs
                        .pop()
                        .expect("resolve_expressions_with_schema returns same count as input"),
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
