// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Piped expression parsing (/-> and /->> operators)

use super::functions::parse_function_call;
use super::parse_expression;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::expressions::pipes::PipeDirection;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;

/// Parse piped expression: (value /-> transform1 /->> transform2)
///
/// Each transform is paired with the operator that introduced it: `/->` ⇒
/// `PipeDirection::First` (current value becomes argument 0); `/->>` ⇒
/// `PipeDirection::Last` (current value becomes the final argument).
pub(super) fn parse_piped_expression(node: CstNode) -> Result<DomainExpression> {
    let value = node
        .field("value")
        .ok_or_else(|| DelightQLError::parse_error("Piped expression missing value"))?;

    let value_expr = parse_expression(
        value,
        &mut crate::pipeline::query_features::FeatureCollector::new(),
    )?;

    let mut transforms: Vec<(PipeDirection, FunctionExpression)> = Vec::new();

    // Walk children in order. Each pipe-operator child fixes the direction
    // for the next transform-yielding child.
    let mut pending_dir: Option<PipeDirection> = None;
    let mut found_value = false;

    for child in node.children() {
        match child.kind() {
            "functional_pipe_operator" => {
                pending_dir = Some(PipeDirection::First);
                continue;
            }
            "reverse_pipe_operator" => {
                pending_dir = Some(PipeDirection::Last);
                continue;
            }
            _ => {}
        }

        if !found_value {
            found_value = true;
            continue;
        }

        let dir = pending_dir.take().ok_or_else(|| {
            DelightQLError::parse_error("Piped expression: transform without preceding pipe operator")
        })?;

        let func = match child.kind() {
            "function_call" => parse_function_call(child)?,
            "string_template" => parse_expression(
                child,
                &mut crate::pipeline::query_features::FeatureCollector::new(),
            )?,
            "case_expression" => super::case_and_subqueries::parse_case_expression(child)?,
            _ => continue,
        };

        match func {
            DomainExpression::Function(f) => transforms.push((dir, f)),
            _ => {
                return Err(DelightQLError::validation_error(
                    "Transform must be a function",
                    "Piped expressions can only transform through function calls",
                ))
            }
        }
    }

    if transforms.is_empty() {
        return Err(DelightQLError::parse_error(
            "Piped expression needs at least one transform after /-> or /->>. \
             Example: age /-> :(@ * 2) or total /->> sum:()",
        ));
    }

    Ok(DomainExpression::PipedExpression {
        value: Box::new(value_expr),
        transforms,
        alias: None,
    })
}
