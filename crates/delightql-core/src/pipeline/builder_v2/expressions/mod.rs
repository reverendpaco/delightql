//! Domain expressions, functions, literals parsing
//!
//! This module contains the main expression parsing logic, organized into:
//! - literals: Column references, literal values, ordinals, ranges
//! - functions: Function calls (regular, bracket, curly, higher-order)
//! - case_and_subqueries: CASE expressions and scalar subqueries
//! - pipes: Piped expressions (/-> operator)

use super::helpers::*;
use super::parse_predicate;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;

pub(super) mod case_and_subqueries;
pub(super) mod functions;
pub(super) mod literals;
pub(super) mod pipes;

// Re-export the main parsing functions for use by parent module
pub(super) use case_and_subqueries::{parse_case_expression, parse_scalar_subquery};
pub(super) use functions::parse_function_call;
pub(super) use literals::{parse_column_ordinal, parse_literal, parse_lvar};

/// Parse simple expression (for comparison operands)
pub(super) fn parse_simple_expression(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<DomainExpression> {
    let child = node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty simple expression"))?;

    // Use the common helper to avoid duplication
    parse_expression(child, features)
}

/// Parse binary expression - RECURSIVE
pub(super) fn parse_binary_expression(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<DomainExpression> {
    let left_node = node
        .field("left")
        .ok_or_else(|| DelightQLError::parse_error("No left in binary expression"))?;
    let right_node = node
        .field("right")
        .ok_or_else(|| DelightQLError::parse_error("No right in binary expression"))?;
    let operator_node = node
        .field("operator")
        .ok_or_else(|| DelightQLError::parse_error("No operator in binary expression"))?;
    // The operator field contains a binary_operator node, get its child
    let operator = operator_node
        .children()
        .next()
        .map(|child| child.kind().to_string())
        .unwrap_or_else(|| operator_node.kind().to_string());

    let left = parse_simple_expression(left_node, features)?;
    let right = parse_simple_expression(right_node, features)?;

    Ok(DomainExpression::Function(FunctionExpression::infix(
        operator, left, right,
    )))
}

/// Parse any expression node directly (literal, lvar, function_call, etc.)
/// This is the main entry point that handles all expression types
pub(super) fn parse_expression(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<DomainExpression> {
    if node.kind() == "domain_expression" || node.kind() == "non_binary_domain_expression" {
        return parse_domain_expression_wrapper(node, features);
    }

    match node.kind() {
        "lvar" => {
            // HO scalar substitution: if the lvar is a simple identifier matching a scalar param,
            // return the bound DomainExpression instead of building an Lvar
            if let Some(ref bindings) = features.ho_bindings {
                if !bindings.scalar_params.is_empty() {
                    // Check if the lvar's first child is a plain identifier
                    if let Some(child) = node.children().next() {
                        if child.kind() == "identifier" {
                            let text: &str = child.text();
                            if let Some(bound_expr) = bindings.scalar_params.get(text) {
                                return Ok(bound_expr.clone());
                            }
                        }
                    }
                }
            }
            literals::parse_lvar(node)
        }
        "literal" => literals::parse_literal(node),
        "boolean_literal" => {
            // Boolean literals can appear as domain values (e.g., true=(3>2))
            let value = node.text() == "true";
            Ok(DomainExpression::Literal {
                value: LiteralValue::Boolean(value),
                alias: None,
            })
        }
        "glob" => {
            let qualifier = node.field_text("qualifier");
            // NOTE: grammar's glob rule only supports qualifier (single identifier like o.*),
            // not namespace_path. Namespace-qualified globs would need a grammar change.
            let mut builder = DomainExpression::glob_builder();
            if let Some(q) = qualifier {
                builder = builder.with_qualifier(q);
            }
            Ok(builder.build())
        }
        "binary_expression" => parse_binary_expression(node, features),
        "predicate" => parse_predicate(node, features),
        "parenthesized_expression" => {
            let inner = node
                .find_child("domain_expression")
                .ok_or_else(|| DelightQLError::parse_error("No expression in parentheses"))?;
            let inner_expr = parse_domain_expression_wrapper(inner, features)?;
            Ok(DomainExpression::Parenthesized {
                inner: Box::new(inner_expr),
                alias: None,
            })
        }
        "tuple_expression" => {
            // EPOCH 5: Parse tuple for multi-column IN
            // Handles both (expr) -> Parenthesized and (expr, expr) -> Tuple
            let mut elements = Vec::new();
            for child in node.children() {
                if child.kind() == "domain_expression" {
                    let expr = parse_domain_expression_wrapper(child, features)?;
                    elements.push(expr);
                }
            }
            if elements.is_empty() {
                return Err(DelightQLError::parse_error("Empty tuple expression"));
            }
            // Single element: parenthesized expression, not a tuple
            if elements.len() == 1 {
                Ok(DomainExpression::Parenthesized {
                    inner: Box::new(elements.into_iter().next().unwrap()),
                    alias: None,
                })
            } else {
                // Multiple elements: actual tuple
                Ok(DomainExpression::Tuple {
                    elements,
                    alias: None,
                })
            }
        }
        "function_call" => functions::parse_function_call(node),
        "bracket_function" => functions::parse_bracket_function(node),
        "curly_function" => functions::parse_curly_function(node),
        "column_ordinal" => literals::parse_column_ordinal(node),
        "column_range" => literals::parse_column_range(node),
        "qualified_column" => {
            let table = node.field_text("table");
            let column = node
                .field_text("column")
                .ok_or_else(|| DelightQLError::parse_error("Missing column in qualified_column"))?;

            Ok(DomainExpression::lvar_builder(column)
                .with_qualifier(table)
                .build())
        }
        "value_placeholder" => Ok(DomainExpression::ValuePlaceholder { alias: None }),
        "pattern_literal" => {
            let full_text = node.text();
            let pattern_text = if full_text.starts_with('/') && full_text.ends_with('/') {
                full_text[1..full_text.len() - 1].to_string()
            } else {
                return Err(DelightQLError::parse_error(format!(
                    "Invalid pattern literal format: '{}' (expected /pattern/)",
                    full_text
                )));
            };
            Ok(DomainExpression::Projection(ProjectionExpr::Pattern {
                pattern: pattern_text,
                alias: None,
            }))
        }
        "string_template" => {
            let mut parts = Vec::new();

            for child in node.children() {
                match child.kind() {
                    "template_text" | "triple_template_text" => {
                        let text = child.text();
                        let processed = literals::process_template_escapes(text)?;
                        parts.push(StringTemplatePart::Text(processed));
                    }
                    "template_interpolation" => {
                        if let Some(expr_node) = child.field("expression") {
                            let expr = parse_domain_expression_wrapper(expr_node, features)?;
                            parts.push(StringTemplatePart::Interpolation(Box::new(expr)));
                        }
                    }
                    other => panic!("catch-all hit in builder_v2/expressions/mod.rs parse_expression string_template: unexpected node kind {:?}", other),
                }
            }

            Ok(DomainExpression::Function(
                FunctionExpression::StringTemplate { parts, alias: None },
            ))
        }
        "citation" => {
            // Citation: :name → name:() (zero-arity call via :name syntax)
            let name = node.field_text("name")
                .ok_or_else(|| DelightQLError::parse_error("No name in citation"))?;
            Ok(DomainExpression::Function(
                FunctionExpression::function_builder(name).build()
            ))
        }
        "piped_expression" => pipes::parse_piped_expression(node),
        "case_expression" => case_and_subqueries::parse_case_expression(node),
        "scalar_subquery" => case_and_subqueries::parse_scalar_subquery(node, features),
        "metadata_tree_group" => super::operators::parse_metadata_tree_group(node),
        "path_literal" => functions::parse_path_literal(node),
        "array_destructure_pattern" => functions::parse_array_destructure_pattern(node),
        "pivot_expression" => {
            let value_node = node
                .field("value_column")
                .ok_or_else(|| DelightQLError::parse_error("No value_column in pivot_expression"))?;
            let key_node = node
                .field("pivot_key")
                .ok_or_else(|| DelightQLError::parse_error("No pivot_key in pivot_expression"))?;
            let value_column = parse_expression(value_node, features)?;
            let pivot_key = parse_expression(key_node, features)?;
            Ok(DomainExpression::PivotOf {
                value_column: Box::new(value_column),
                pivot_key: Box::new(pivot_key),
                pivot_values: Vec::new(), // populated by resolver
            })
        }
        "sparse_fill" => Err(DelightQLError::parse_error_categorized(
            "anon",
            "Sparse fill _(col @ val) can only appear in anonymous tables with sparse (?) columns"
                .to_string(),
        )),
        _ => Err(DelightQLError::parse_error(format!(
            "Unknown expression type: '{}'. Expected one of: literal, lvar, function_call, \
             binary_expression, predicate, parenthesized_expression, bracket_function, \
             column_ordinal, column_range, value_placeholder (@), pattern_literal (/pattern/), \
             string_template, citation, case_expression, scalar_subquery, metadata_tree_group, or piped_expression",
            node.kind()
        ))),
    }
}

/// Parse a domain_expression wrapper node (which contains the actual expression + optional alias)
pub(super) fn parse_domain_expression_wrapper(
    node: CstNode,
    features: &mut crate::pipeline::query_features::FeatureCollector,
) -> Result<DomainExpression> {
    let alias = node.field_text("alias");

    let child = node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty domain expression"))?;

    let mut expr = parse_expression(child, features)?;

    if let Some(alias_str) = alias {
        apply_alias_to_expression(&mut expr, Some(alias_str));
    }

    Ok(expr)
}
