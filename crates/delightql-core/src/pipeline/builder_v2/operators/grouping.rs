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
                let reducing_on = parse_domain_expression_list(on_node, features)?;
                let arbitrary = if let Some(arb_node) = grouping_node.field("arbitrary") {
                    parse_domain_expression_list(arb_node, features)?
                } else {
                    Vec::new()
                };
                ModuloSpec::GroupBy {
                    reducing_by,
                    reducing_on,
                    arbitrary,
                }
            } else if let Some(on_node) = reducing_on_node {
                let reducing_on = parse_domain_expression_list(on_node, features)?;
                let arbitrary = if let Some(arb_node) = grouping_node.field("arbitrary") {
                    parse_domain_expression_list(arb_node, features)?
                } else {
                    Vec::new()
                };
                ModuloSpec::GroupBy {
                    reducing_by: Vec::new(),
                    reducing_on,
                    arbitrary,
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

            // Check for arbitrary columns without aggregates: %(country ~? last_name)
            if let Some(arb_node) = grouping_node.field("arbitrary") {
                let arbitrary = parse_domain_expression_list(arb_node, features)?;
                ModuloSpec::GroupBy {
                    reducing_by: columns,
                    reducing_on: Vec::new(),
                    arbitrary,
                }
            } else {
                ModuloSpec::Columns(columns)
            }
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
