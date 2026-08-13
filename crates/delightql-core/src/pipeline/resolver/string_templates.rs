// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::ast_resolved::{
    DomainExpression, FunctionApplication, LiteralValue, ValueTemplatePart,
};

/// Build a concat chain from string template parts
///
/// This function takes a vector of string template parts (text and interpolations)
/// and builds a left-associative chain of concat operations.
///
/// For example: ["Hello ", {name}, "!"] becomes:
/// concat(concat("Hello ", name), "!")
///
/// Returns a DomainExpression that can be either:
/// - A single Literal (for templates with only text)
/// - A Function with nested Infix concat operations (for templates with interpolations)
pub fn build_concat_chain(parts: Vec<ValueTemplatePart>) -> DomainExpression {
    let mut parts_iter = parts.into_iter();

    // Start with first part
    let mut result = match parts_iter.next() {
        Some(ValueTemplatePart::Text(text)) => {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(text)))
        }
        Some(ValueTemplatePart::Interpolation(expr)) => *expr,
        None => {
            // Empty template - return empty string
            return DomainExpression::Application(FunctionApplication::Ground(
                LiteralValue::String(String::new()),
            ));
        }
    };

    // Chain rest with concat operations
    for part in parts_iter {
        let next_expr = match part {
            ValueTemplatePart::Text(text) => DomainExpression::Application(
                FunctionApplication::Ground(LiteralValue::String(text)),
            ),
            ValueTemplatePart::Interpolation(expr) => *expr,
        };

        result = DomainExpression::Application(FunctionApplication::Infix(
            crate::pipeline::asts::core::InfixApplication {
                operator: crate::pipeline::asts::vocabulary::BinOp::Concat,
                left: Box::new(result),
                right: Box::new(next_expr),
            },
        ));
    }

    result
}
