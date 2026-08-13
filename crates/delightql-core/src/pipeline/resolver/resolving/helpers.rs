// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// Helper to convert column alias from unresolved to resolved
pub(in crate::pipeline::resolver) fn convert_column_alias(
    alias: Option<ast_unresolved::ColumnAlias>,
) -> Option<ast_resolved::ColumnAlias> {
    alias.map(|a| match a {
        ast_unresolved::ColumnAlias::Literal(s) => ast_resolved::ColumnAlias::Literal(s),
        ast_unresolved::ColumnAlias::Template(t) => {
            ast_resolved::ColumnAlias::Template(ast_resolved::ColumnNameTemplate {
                template: t.template,
            })
        }
    })
}

/// Build concat chain from string template parts, preserving @ placeholders
/// This is used for MapCover and Transform contexts where @ will be replaced later
pub(in crate::pipeline::resolver) fn build_concat_chain_with_placeholders(
    fold: &mut ResolverFold<'_, '_>,
    parts: Vec<ast_unresolved::ValueTemplatePart>,
) -> Result<ast_resolved::DomainExpression> {
    use ast_resolved::{DomainExpression, FunctionApplication, LiteralValue};

    let mut parts_iter = parts.into_iter();

    // Start with first part
    let mut result = match parts_iter.next() {
        Some(ast_unresolved::ValueTemplatePart::Text(text)) => {
            DomainExpression::Application(FunctionApplication::Ground(LiteralValue::String(text)))
        }
        Some(ast_unresolved::ValueTemplatePart::Interpolation(expr)) => {
            fold.transform_domain(*expr)?
        }
        None => {
            // Empty template - return empty string
            return Ok(DomainExpression::Application(FunctionApplication::Ground(
                LiteralValue::String(String::new()),
            )));
        }
    };

    // Chain rest with concat
    for part in parts_iter {
        let next_expr = match part {
            ast_unresolved::ValueTemplatePart::Text(text) => DomainExpression::Application(
                FunctionApplication::Ground(LiteralValue::String(text)),
            ),
            ast_unresolved::ValueTemplatePart::Interpolation(expr) => {
                fold.transform_domain(*expr)?
            }
        };

        result = DomainExpression::Application(FunctionApplication::Infix(
            crate::pipeline::asts::core::InfixApplication {
                operator: crate::pipeline::asts::vocabulary::BinOp::Concat,
                left: Box::new(result),
                right: Box::new(next_expr),
            },
        ));
    }

    Ok(result)
}
