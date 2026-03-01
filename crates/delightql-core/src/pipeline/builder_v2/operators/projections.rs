//! Projection, ordering, project-out, and reposition operators

use super::super::expressions::*;
use super::super::helpers::*;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;

/// Parse generalized projection ([...] or (...))
pub(in crate::pipeline::builder_v2) fn parse_generalized_projection(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let child = node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty projection"))?;

    let containment = ContainmentSemantic::Parenthesis;

    let expr_list = child
        .find_child("domain_expression_list")
        .ok_or_else(|| DelightQLError::parse_error("No expression list in projection"))?;

    let expressions = parse_domain_expression_list(expr_list, features)?;

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::General {
                containment_semantic: containment,
                expressions,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse ordering operation: #(field)
pub(in crate::pipeline::builder_v2) fn parse_ordering(
    node: CstNode,
    input: RelationalExpression,
) -> Result<RelationalExpression> {
    let order_list = node
        .find_child("order_list")
        .ok_or_else(|| DelightQLError::parse_error("No order_list in ordering"))?;

    let specs: Vec<_> = order_list
        .children()
        .filter(|child| child.kind() == "order_item")
        .map(|child| parse_order_item(child))
        .collect::<Result<Vec<_>>>()?;

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::TupleOrdering {
                containment_semantic: ContainmentSemantic::Parenthesis,
                specs,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse order item
pub(in crate::pipeline::builder_v2) fn parse_order_item(node: CstNode) -> Result<OrderingSpec> {
    let column_node = node
        .field("column")
        .ok_or_else(|| DelightQLError::parse_error("No column in order_item"))?;

    let column = match column_node.kind() {
        "column_ordinal" => parse_column_ordinal(column_node)?,
        _ => parse_lvar(column_node)?,
    };

    let direction = node
        .field_text("direction")
        .and_then(|text| match text.as_str() {
            "ascending" | "asc" => Some(OrderDirection::Ascending),
            "descending" | "desc" => Some(OrderDirection::Descending),
            other => panic!("catch-all hit in builder_v2/operators/projections.rs parse_order_item: unexpected direction {:?}", other),
        });

    Ok(OrderingSpec { column, direction })
}

/// Parse project-out operation: -(cols), -[cols]
pub(in crate::pipeline::builder_v2) fn parse_project_out(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let containment = ContainmentSemantic::Parenthesis;

    let list_node = node
        .find_child("domain_expression_list")
        .ok_or_else(|| DelightQLError::parse_error("No domain_expression_list in project_out"))?;

    let expressions = parse_domain_expression_list(list_node, features)?;

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::ProjectOut {
                containment_semantic: containment,
                expressions,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse reposition operation: *[col @ pos, ...]
pub(in crate::pipeline::builder_v2) fn parse_reposition(
    node: CstNode,
    input: RelationalExpression,
) -> Result<RelationalExpression> {
    let reposition_list = node
        .find_child("reposition_list")
        .ok_or_else(|| DelightQLError::parse_error("No reposition_list in reposition"))?;

    let mut moves = Vec::new();

    for child in reposition_list.children() {
        if child.kind() == "reposition_item" {
            let column_node = child
                .field("column")
                .ok_or_else(|| DelightQLError::parse_error("No column in reposition_item"))?;

            let column = if column_node.kind() == "integer_literal" {
                // Bare integer in *[...] — build ColumnOrdinal directly
                let text = column_node.text();
                let (position, reverse) = if text.starts_with('-') {
                    (
                        text[1..]
                            .parse::<u16>()
                            .map_err(|_| DelightQLError::parse_error("Invalid ordinal position"))?,
                        true,
                    )
                } else {
                    (
                        text.parse::<u16>()
                            .map_err(|_| DelightQLError::parse_error("Invalid ordinal position"))?,
                        false,
                    )
                };
                DomainExpression::ColumnOrdinal(PhaseBoxable::new(ColumnOrdinal {
                    position,
                    reverse,
                    qualifier: None,
                    namespace_path: NamespacePath::empty(),
                    alias: None,
                    glob: false,
                }))
            } else {
                parse_lvar(column_node)?
            };

            let position_text = child
                .field_text("position")
                .ok_or_else(|| DelightQLError::parse_error("No position in reposition_item"))?;

            let position: i32 = position_text.parse().map_err(|_| {
                DelightQLError::parse_error(format!("Invalid position number: {}", position_text))
            })?;

            moves.push(RepositionSpec { column, position });
        }
    }

    if moves.is_empty() {
        return Err(DelightQLError::parse_error("Empty reposition list"));
    }

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::Reposition { moves },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}
