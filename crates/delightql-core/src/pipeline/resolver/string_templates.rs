use crate::pipeline::ast_resolved::{
    DomainExpression, FunctionExpression, LiteralValue, Resolved, StringTemplatePart,
};
use delightql_types::SqlIdentifier;

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
pub fn build_concat_chain(
    parts: Vec<StringTemplatePart<Resolved>>,
    alias: Option<SqlIdentifier>,
) -> DomainExpression {
    let mut parts_iter = parts.into_iter();

    // Start with first part
    let mut result = match parts_iter.next() {
        Some(StringTemplatePart::Text(text)) => DomainExpression::Literal {
            value: LiteralValue::String(text),
            alias: None,
        },
        Some(StringTemplatePart::Interpolation(expr)) => *expr,
        None => {
            // Empty template - return empty string
            return DomainExpression::Literal {
                value: LiteralValue::String(String::new()),
                alias,
            };
        }
    };

    // Chain rest with concat operations
    for part in parts_iter {
        let next_expr = match part {
            StringTemplatePart::Text(text) => DomainExpression::Literal {
                value: LiteralValue::String(text),
                alias: None,
            },
            StringTemplatePart::Interpolation(expr) => *expr,
        };

        result = DomainExpression::Function(FunctionExpression::Infix {
            operator: "concat".to_string(), // DelightQL's concat operator (same as ++)
            left: Box::new(result),
            right: Box::new(next_expr),
            alias: None,
        });
    }

    // Add the final alias to the outermost expression
    apply_alias_to_expression(result, alias)
}

/// Build a concat chain and extract just the FunctionExpression
///
/// This is a convenience wrapper for callers that need a FunctionExpression
/// rather than a DomainExpression. Used by pattern_resolver and mod.rs.
pub fn build_concat_chain_as_function(
    parts: Vec<StringTemplatePart<Resolved>>,
    alias: Option<SqlIdentifier>,
) -> FunctionExpression {
    let expr = build_concat_chain(parts, alias.clone());

    match expr {
        DomainExpression::Function(func) => func,
        // For single literals or empty templates, wrap in a Regular function
        _ => FunctionExpression::Regular {
            name: "concat".into(),
            namespace: None,
            arguments: vec![expr],
            alias,
            conditioned_on: None,
        },
    }
}

/// Apply an alias to the outermost level of an expression
fn apply_alias_to_expression(expr: DomainExpression, alias: Option<SqlIdentifier>) -> DomainExpression {
    match expr {
        DomainExpression::Function(FunctionExpression::Infix {
            operator,
            left,
            right,
            alias: _,
        }) => DomainExpression::Function(FunctionExpression::Infix {
            operator,
            left,
            right,
            alias,
        }),
        DomainExpression::Literal { value, alias: _ } => DomainExpression::Literal { value, alias },
        // For other expression types, return as-is
        // In practice, build_concat_chain only produces Literal or Infix
        _ => expr,
    }
}
