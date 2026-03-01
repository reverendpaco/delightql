//! Piped expression parsing (/-> operator)

use super::functions::parse_function_call;
use super::parse_expression;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;

/// Parse piped expression: (value /-> transform1 /-> transform2)
pub(super) fn parse_piped_expression(node: CstNode) -> Result<DomainExpression> {
    let value = node
        .field("value")
        .ok_or_else(|| DelightQLError::parse_error("Piped expression missing value"))?;

    let value_expr = parse_expression(
        value,
        &mut crate::pipeline::query_features::FeatureCollector::new(),
    )?;

    let mut transforms = Vec::new();

    let mut found_value = false;
    for child in node.children() {
        if child.kind() == "functional_pipe_operator" {
            continue;
        }

        if !found_value && child.kind() != "functional_pipe_operator" {
            found_value = true;
            continue;
        }

        if child.kind() == "function_call" {
            let func = parse_function_call(child)?;
            match func {
                DomainExpression::Function(f) => transforms.push(f),
                _ => {
                    return Err(DelightQLError::validation_error(
                        "Transform must be a function",
                        "Piped expressions can only transform through function calls",
                    ))
                }
            }
        } else if child.kind() == "string_template" {
            let expr = parse_expression(
                child,
                &mut crate::pipeline::query_features::FeatureCollector::new(),
            )?;
            match expr {
                DomainExpression::Function(f) => transforms.push(f),
                _ => {
                    return Err(DelightQLError::parse_error(
                        "String template must parse as a function",
                    ))
                }
            }
        } else if child.kind() == "case_expression" {
            // Need to reference case_expression parser from case_and_subqueries module
            let expr = super::case_and_subqueries::parse_case_expression(child)?;
            match expr {
                DomainExpression::Function(f) => transforms.push(f),
                _ => {
                    return Err(DelightQLError::parse_error(
                        "CASE expression must parse as a function",
                    ))
                }
            }
        }
    }

    if transforms.is_empty() {
        return Err(DelightQLError::parse_error(
            "Piped expression needs at least one transform after /->. \
             Example: age /-> :(@ * 2) or total /-> sum:()",
        ));
    }

    Ok(DomainExpression::PipedExpression {
        value: Box::new(value_expr),
        transforms,
        alias: None,
    })
}
