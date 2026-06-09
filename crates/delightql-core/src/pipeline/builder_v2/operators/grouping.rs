// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Grouping, transform, and metadata tree group operators

use super::super::expressions::*;
use super::super::helpers::*;
use super::covers::parse_cover_filter_condition;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;
use crate::pipeline::query_features::FeatureCollector;

/// Parse transform operation: $$(expr as alias, ...)
pub(in crate::pipeline::builder_v2) fn parse_transform(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let transform_list = node
        .find_child("transform_list")
        .ok_or_else(|| DelightQLError::parse_error("No transform_list in transform"))?;

    let mut transformations = Vec::new();

    for child in transform_list.children() {
        if child.kind() == "transform_item" {
            // transform_item is now just a domain_expression
            let domain_expr_node = child.find_child("domain_expression").ok_or_else(|| {
                DelightQLError::parse_error("No domain_expression in transform_item")
            })?;

            // Parse the domain expression (which includes the alias)
            let domain_expr = parse_domain_expression_wrapper(domain_expr_node, features)?;

            // The alias field from the domain_expression CST node
            // With grammar change, alias is now an lvar (identifier or qualified_column)
            let alias_node = domain_expr_node.field("alias").ok_or_else(|| {
                DelightQLError::parse_error(
                    "Transform items must have 'as alias' - e.g., $$(upper:(name) as name)",
                )
            })?;

            let (alias, qualifier) = match alias_node.kind() {
                "lvar" => {
                    // lvar wraps either identifier or qualified_column
                    if let Some(qc) = alias_node.find_child("qualified_column") {
                        let table = qc.field_text("table");
                        let column = qc.field_text("column").ok_or_else(|| {
                            DelightQLError::parse_error("No column in qualified alias")
                        })?;
                        (column, table)
                    } else {
                        // Plain identifier inside lvar
                        (crate::pipeline::cst::unstrop(alias_node.text()), None)
                    }
                }
                "qualified_column" => {
                    let table = alias_node.field_text("table");
                    let column = alias_node.field_text("column").ok_or_else(|| {
                        DelightQLError::parse_error("No column in qualified alias")
                    })?;
                    (column, table)
                }
                other => panic!("catch-all hit in builder_v2/operators/grouping.rs parse_cover_transform: unexpected alias node kind {:?}", other),
            };

            transformations.push((domain_expr, alias, qualifier));
        }
    }

    let filter_condition = parse_cover_filter_condition(&transform_list, features)?;

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::Transform {
                transformations,
                conditioned_on: filter_condition,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse a `reduction_item_list` (the body after `~>`): separates ordinary
/// aggregate/expression items (→ `reducing_on`) from delegate selections
/// (`(cols) <~ [#(order)]` → `delegates`). Each delegate carries its payload
/// plus an optional ordering: a bare `<~` (empty ordering) is an arbitrary
/// delegate, while `<~ #(order)` is an ordered delegate (DISTINCT-ON-style).
/// Both are represented as `DelegateSpec { payload, order }` and lowered
/// downstream (empty order → bare columns; non-empty → `row_number()=1`).
fn parse_reduction_items(
    list_node: CstNode,
    features: &mut FeatureCollector,
) -> Result<(Vec<DomainExpression>, Vec<DelegateSpec>)> {
    let mut aggregates = Vec::new();
    let mut delegates = Vec::new();
    for item in list_node
        .children()
        .filter(|c| c.kind() == "reduction_item")
    {
        if let Some(delegate) = item.find_child("delegate_item") {
            let payload_node = delegate
                .field("payload")
                .ok_or_else(|| DelightQLError::parse_error("No payload in delegate_item"))?;
            let payload = parse_delegate_payload_columns(payload_node, features)?;
            // Empty ordering (bare `<~`) == arbitrary delegate.
            let order = if let Some(order_node) = delegate.field("order") {
                parse_delegate_order(order_node, features)?
            } else {
                Vec::new()
            };
            delegates.push(DelegateSpec { payload, order });
        } else if let Some(de) = item.find_child("domain_expression") {
            aggregates.push(parse_domain_expression_wrapper(de, features)?);
        } else {
            return Err(DelightQLError::parse_error("Empty reduction_item"));
        }
    }
    Ok((aggregates, delegates))
}

/// Parse a delegate `#(order)` slot (a `window_ordering` node) into ordering
/// specs, mirroring the window-function order parser.
fn parse_delegate_order(
    ordering_node: CstNode,
    features: &mut FeatureCollector,
) -> Result<Vec<OrderingSpec>> {
    let mut specs = Vec::new();
    for child in ordering_node.children() {
        if child.kind() == "window_order_item" {
            let column_node = child
                .field("column")
                .ok_or_else(|| DelightQLError::parse_error("No column in delegate order item"))?;
            let column = parse_domain_expression_wrapper(column_node, features)?;
            let direction = child.field_text("direction").and_then(|dir| match dir.as_str() {
                "asc" | "ascending" => Some(OrderDirection::Ascending),
                "desc" | "descending" => Some(OrderDirection::Descending),
                _ => None,
            });
            specs.push(OrderingSpec { column, direction });
        }
    }
    Ok(specs)
}

/// Extract the payload columns from a `delegate_payload` node. Parenthesized
/// multi-column payloads parse as `domain_expression → tuple_expression`; a
/// bare single column is a plain `domain_expression`. Whole-row `(*)` parses (in
/// practice) as a `tuple_expression` whose sole element is a `glob`, handled by
/// the tuple branch below and expanded to all columns by the resolver.
fn parse_delegate_payload_columns(
    payload: CstNode,
    features: &mut FeatureCollector,
) -> Result<Vec<DomainExpression>> {
    if let Some(de) = payload.find_child("domain_expression") {
        if let Some(tuple) = de.find_child("tuple_expression") {
            parse_domain_expression_list(tuple, features)
        } else {
            Ok(vec![parse_domain_expression_wrapper(de, features)?])
        }
    } else if payload.find_child("glob").is_some() {
        // `(*)` as a bare `glob` child (grammar alternative 1). The GLR parser
        // routes `(*)` through the `domain_expression → tuple_expression`
        // branch above, so this is defensive; handling it identically keeps the
        // two grammar alternatives equivalent regardless of GLR resolution.
        Ok(vec![DomainExpression::glob_builder().build()])
    } else {
        Err(DelightQLError::parse_error("Empty delegate_payload"))
    }
}

/// Parse grouping operation: %(city) or %[city]
pub(in crate::pipeline::builder_v2) fn parse_grouping(
    node: CstNode,
    input: RelationalExpression,
    features: &mut FeatureCollector,
) -> Result<RelationalExpression> {
    let (containment, grouping_node) = if let Some(paren) = node.find_child("grouping_paren") {
        (ContainmentSemantic::Parenthesis, paren)
    } else {
        return Err(DelightQLError::parse_error("No grouping content"));
    };

    let spec = {
        let reducing_on_node = grouping_node.field("reducing_on");
        let has_aggregate = reducing_on_node.is_some();

        if has_aggregate {
            let reducing_by_node = grouping_node.field("reducing_by");

            if let (Some(by_node), Some(on_node)) = (reducing_by_node, reducing_on_node) {
                let reducing_by = parse_domain_expression_list(by_node, features)?;
                let (reducing_on, delegates) = parse_reduction_items(on_node, features)?;
                ModuloSpec::GroupBy {
                    reducing_by,
                    reducing_on,
                    delegates,
                }
            } else if let Some(on_node) = reducing_on_node {
                let (reducing_on, delegates) = parse_reduction_items(on_node, features)?;
                ModuloSpec::GroupBy {
                    reducing_by: Vec::new(),
                    reducing_on,
                    delegates,
                }
            } else {
                return Err(DelightQLError::parse_error("Invalid grouping structure"));
            }
        } else {
            let reducing_by_node = grouping_node
                .field("reducing_by")
                .or_else(|| grouping_node.find_child("domain_expression_list"))
                .ok_or_else(|| DelightQLError::parse_error("No columns in grouping"))?;

            let columns = parse_domain_expression_list(reducing_by_node, features)?;

            // Delegate selections live in reduction place (after ~>), so a bare
            // %(cols) with no ~> is always a simple distinct/group.
            ModuloSpec::Columns(columns)
        }
    };

    Ok(RelationalExpression::Pipe(Box::new(
        stacksafe::StackSafe::new(PipeExpression {
            source: input,
            operator: UnaryRelationalOperator::Modulo {
                containment_semantic: containment,
                spec,
            },
            cpr_schema: PhaseBox::phantom(),
        }),
    )))
}

/// Parse metadata tree group: column:~> {constructor}
pub(in crate::pipeline::builder_v2) fn parse_metadata_tree_group(
    node: CstNode,
) -> Result<DomainExpression> {
    // Get the key column (lvar)
    let key_node = node
        .field("key")
        .ok_or_else(|| DelightQLError::parse_error("No key in metadata_tree_group"))?;

    let key_lvar = parse_lvar(key_node)?;
    let (key_column, key_qualifier, key_schema) = match key_lvar {
        DomainExpression::Lvar {
            name, qualifier, ..
        } => (name, qualifier, None), // We don't use namespace_path here - stays None for builder phase
        _ => {
            return Err(DelightQLError::parse_error(
                "Expected lvar as key in metadata_tree_group",
            ))
        }
    };

    // Get the constructor (curly_function, bracket_function, array_destructure_pattern, metadata_tree_group, or placeholder)
    let constructor_node = node
        .children()
        .find(|child| {
            child.kind() == "curly_function"
                || child.kind() == "bracket_function"
                || child.kind() == "array_destructure_pattern"
                || child.kind() == "metadata_tree_group"
                || child.kind() == "placeholder"
        })
        .ok_or_else(|| DelightQLError::parse_error("No constructor in metadata_tree_group"))?;

    // Handle placeholder specially - for bare `_`, set keys_only = true
    // For `{_}` or any other constructor, keys_only = false
    let (constructor, keys_only) = if constructor_node.kind() == "placeholder" {
        // For country:~> _, create an empty Curly with Placeholder marker
        // AND set keys_only = true to signal "extract keys only, no array explosion"
        let curly = FunctionExpression::Curly {
            members: vec![CurlyMember::Placeholder],
            inner_grouping_keys: vec![],
            cte_requirements: None,
            alias: None,
        };
        (curly, true) // keys_only = true for bare _
    } else {
        let constructor_expr = parse_expression(constructor_node, &mut FeatureCollector::new())?;
        match constructor_expr {
            DomainExpression::Function(func) => (func, false), // keys_only = false for {_} and other patterns
            _ => {
                return Err(DelightQLError::parse_error(
                    "Expected function expression as constructor in metadata_tree_group",
                ))
            }
        }
    };

    // Extract alias if present
    let alias = node.field_text("alias");

    Ok(DomainExpression::Function(
        FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            constructor: Box::new(constructor),
            keys_only,
            cte_requirements: None,
            alias: alias.map(|s| s.into()),
        },
    ))
}
