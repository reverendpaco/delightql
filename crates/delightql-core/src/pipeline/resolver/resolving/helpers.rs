use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;

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

/// Helper to extract column name from resolved expression
pub(in crate::pipeline::resolver) fn extract_column_name_from_expr(
    expr: &ast_resolved::DomainExpression,
) -> Option<String> {
    match expr {
        ast_resolved::DomainExpression::Lvar { name, .. } => Some(name.to_string()),
        // In resolved AST, qualified columns are still Lvar with qualifier field set
        other => panic!(
            "catch-all hit in resolving/helpers.rs extract_column_name_from_expr: {:?}",
            other
        ),
    }
}

/// Build concat chain from string template parts, preserving @ placeholders
/// This is used for MapCover and Transform contexts where @ will be replaced later
pub(in crate::pipeline::resolver) fn build_concat_chain_with_placeholders(
    parts: Vec<ast_unresolved::StringTemplatePart>,
) -> Result<ast_resolved::DomainExpression> {
    use ast_resolved::{DomainExpression, FunctionExpression, LiteralValue};

    let mut parts_iter = parts.into_iter();

    // Start with first part
    let mut result = match parts_iter.next() {
        Some(ast_unresolved::StringTemplatePart::Text(text)) => DomainExpression::Literal {
            value: LiteralValue::String(text),
            alias: None,
        },
        Some(ast_unresolved::StringTemplatePart::Interpolation(expr)) => {
            // Convert unresolved expression but preserve @ placeholders
            convert_domain_expression_preserving_placeholders(&expr)?
        }
        None => {
            // Empty template - return empty string
            return Ok(DomainExpression::Literal {
                value: LiteralValue::String(String::new()),
                alias: None,
            });
        }
    };

    // Chain rest with concat
    for part in parts_iter {
        let next_expr = match part {
            ast_unresolved::StringTemplatePart::Text(text) => DomainExpression::Literal {
                value: LiteralValue::String(text),
                alias: None,
            },
            ast_unresolved::StringTemplatePart::Interpolation(expr) => {
                convert_domain_expression_preserving_placeholders(&expr)?
            }
        };

        result = DomainExpression::Function(FunctionExpression::Infix {
            operator: "concat".to_string(),
            left: Box::new(result),
            right: Box::new(next_expr),
            alias: None,
        });
    }

    Ok(result)
}

/// Convert unresolved expression to resolved, preserving @ placeholders
fn convert_domain_expression_preserving_placeholders(
    expr: &ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::ValuePlaceholder { alias } => {
            Ok(ast_resolved::DomainExpression::ValuePlaceholder {
                alias: alias.clone(),
            })
        }
        ast_unresolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } => Ok(ast_resolved::DomainExpression::Lvar {
            name: name.clone(),
            qualifier: qualifier.clone(),
            namespace_path: namespace_path.clone(),
            alias: alias.clone(),
            provenance: ast_resolved::PhaseBox::phantom(),
        }),
        ast_unresolved::DomainExpression::Literal { value, alias } => {
            Ok(ast_resolved::DomainExpression::Literal {
                value: value.clone(),
                alias: alias.clone(),
            })
        }
        // For other expression types, use basic conversion
        _ => super::super::convert_domain_expression(expr),
    }
}
