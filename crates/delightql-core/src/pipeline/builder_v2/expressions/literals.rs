//! Literal values, column references, ordinals, and ranges parsing

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::unresolved::NamespacePath;
use crate::pipeline::asts::unresolved::*;
use crate::pipeline::cst::CstNode;

/// Parse lvar (column reference)
pub(in crate::pipeline::builder_v2) fn parse_lvar(node: CstNode) -> Result<DomainExpression> {
    let child = node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty lvar"))?;

    match child.kind() {
        "identifier" => {
            Ok(DomainExpression::lvar_builder(crate::pipeline::cst::unstrop(child.text())).build())
        }
        "qualified_column" => {
            let _schema = child.field_text("schema");

            let qualifier = if let Some(table_field) = child.field("table") {
                if table_field.kind() == "cpr_reference" {
                    // Special marker for CPR reference
                    Some("_".to_string())
                } else {
                    Some(crate::pipeline::cst::unstrop(table_field.text()))
                }
            } else {
                child.field_text("qualifier")
            };

            let name = child
                .field_text("column")
                .ok_or_else(|| DelightQLError::parse_error("No column in qualified_column"))?;

            // Note: schema parsing will be updated later, for now ignore it
            Ok(DomainExpression::lvar_builder(name)
                .with_qualifier(qualifier)
                .build())
        }
        _ => Err(DelightQLError::parse_error("Invalid lvar")),
    }
}

/// Strip enclosing quotes from a string literal CST node text.
/// Handles triple-quoted (""") and double-quoted (") forms.
pub(in crate::pipeline::builder_v2) fn strip_string_quotes(text: &str) -> &str {
    if text.starts_with("\"\"\"") {
        &text[3..text.len() - 3]
    } else {
        &text[1..text.len() - 1]
    }
}

/// Decode a string literal that may have a `b64:` prefix.
/// Returns the decoded content for b64 strings, or stripped-quotes content for regular strings.
pub(in crate::pipeline::builder_v2) fn decode_string_literal_text(text: &str) -> Option<String> {
    if text.starts_with("b64:") {
        let inner = &text[4..];
        let encoded = strip_string_quotes(inner);
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        String::from_utf8(bytes).ok()
    } else if text.starts_with('"') {
        Some(strip_string_quotes(text).to_string())
    } else {
        None
    }
}

/// Parse literal
pub(in crate::pipeline::builder_v2) fn parse_literal(node: CstNode) -> Result<DomainExpression> {
    let child = node
        .children()
        .next()
        .ok_or_else(|| DelightQLError::parse_error("Empty literal"))?;

    match child.kind() {
        "string_literal" => {
            let text = child.text();
            if text.starts_with("b64:") {
                let inner = &text[4..]; // skip "b64:"
                let encoded = strip_string_quotes(inner);
                use base64::Engine as _;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| {
                        DelightQLError::parse_error(format!(
                            "Invalid base64 in b64:\"...\" literal: {}",
                            e
                        ))
                    })?;
                let decoded = String::from_utf8(bytes).map_err(|e| {
                    DelightQLError::parse_error(format!(
                        "b64:\"...\" decoded to invalid UTF-8: {}",
                        e
                    ))
                })?;
                Ok(DomainExpression::literal_builder(LiteralValue::String(decoded)).build())
            } else {
                let value = strip_string_quotes(text);
                Ok(
                    DomainExpression::literal_builder(LiteralValue::String(value.to_string()))
                        .build(),
                )
            }
        }
        "hex_literal" => {
            // Parse hex literal: 0x0A or 0x0a
            let text = child.text();
            // Strip 0x or 0X prefix
            let hex_str = &text[2..];
            // Parse as hex and convert to decimal string for storage
            let value = u64::from_str_radix(hex_str, 16).map_err(|_| {
                DelightQLError::parse_error(format!("Invalid hex literal: {}", text))
            })?;
            Ok(DomainExpression::literal_builder(LiteralValue::Number(value.to_string())).build())
        }
        "octal_literal" => {
            // Parse octal literal: 0o12 or 0O12
            let text = child.text();
            // Strip 0o or 0O prefix
            let octal_str = &text[2..];
            // Parse as octal and convert to decimal string for storage
            let value = u64::from_str_radix(octal_str, 8).map_err(|_| {
                DelightQLError::parse_error(format!("Invalid octal literal: {}", text))
            })?;
            Ok(DomainExpression::literal_builder(LiteralValue::Number(value.to_string())).build())
        }
        "number_literal" | "integer_literal" => Ok(DomainExpression::literal_builder(
            LiteralValue::Number(child.text().to_string()),
        )
        .build()),
        "boolean_literal" => {
            let value = child.text() == "true";
            Ok(DomainExpression::literal_builder(LiteralValue::Boolean(value)).build())
        }
        "null_literal" => Ok(DomainExpression::literal_builder(LiteralValue::Null).build()),
        _ => Err(DelightQLError::parse_error("Unknown literal type")),
    }
}

pub(in crate::pipeline::builder_v2) fn parse_column_ordinal(
    node: CstNode,
) -> Result<DomainExpression> {
    let position_node = node
        .field("position")
        .ok_or_else(|| DelightQLError::parse_error("Missing position in column_ordinal"))?;

    // Reject identifiers in column ordinals — only integer literals allowed.
    // The grammar accepts identifiers to produce a clear semantic error here
    // instead of a cryptic parse error.
    if position_node.kind() == "identifier" {
        return Err(DelightQLError::validation_error_categorized(
            "constraint/column_ordinal",
            format!(
                "Column ordinal |{}| must be an integer literal, not an identifier. \
                 Column ordinals only accept integer positions like |1| or |-1|.",
                position_node.text()
            ),
            "Column ordinal constraint",
        ));
    }

    let text = position_node.text();

    // Glob ordinal: x|*| — represents all columns by position
    if text == "*" {
        let qualifier = node.field_text("qualifier");
        let ordinal = ColumnOrdinal {
            position: 0,
            reverse: false,
            qualifier,
            namespace_path: NamespacePath::empty(),
            alias: None,
            glob: true,
        };
        return Ok(DomainExpression::ColumnOrdinal(PhaseBoxable::new(ordinal)));
    }

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

    let qualifier = node.field_text("qualifier");

    let ordinal = ColumnOrdinal {
        position,
        reverse,
        qualifier,
        namespace_path: NamespacePath::empty(),
        alias: None,
        glob: false,
    };

    Ok(DomainExpression::ColumnOrdinal(PhaseBoxable::new(ordinal)))
}

/// Process escape sequences in smart-string template text.
///
/// Supported escapes:
///   \n  → newline
///   \t  → tab
///   \\  → backslash
///   \q  → single quote (')
///   \Q  → double quote (")
///
/// Unrecognized `\x` sequences produce an error.
pub fn process_template_escapes(text: &str) -> Result<String> {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('q') => result.push('\''),
                Some('Q') => result.push('"'),
                Some(other) => {
                    return Err(DelightQLError::parse_error(format!(
                        "Unrecognized escape sequence '\\{}' in smart string. \
                         Supported escapes: \\n \\t \\\\ \\q \\Q",
                        other
                    )));
                }
                None => {
                    return Err(DelightQLError::parse_error(
                        "Trailing backslash in smart string",
                    ));
                }
            }
        } else {
            result.push(ch);
        }
    }
    Ok(result)
}

pub(in crate::pipeline::builder_v2) fn parse_column_range(
    node: CstNode,
) -> Result<DomainExpression> {
    let parse_position = |n: CstNode| -> Result<(u16, bool)> {
        let text = n.text();
        if text.starts_with('-') {
            Ok((
                text[1..]
                    .parse()
                    .map_err(|_| DelightQLError::parse_error("Invalid range position"))?,
                true,
            ))
        } else {
            Ok((
                text.parse()
                    .map_err(|_| DelightQLError::parse_error("Invalid range position"))?,
                false,
            ))
        }
    };

    let start = node.field("start").map(parse_position).transpose()?;
    let end = node.field("end").map(parse_position).transpose()?;

    if start.is_none() && end.is_none() {
        return Err(DelightQLError::parse_error(
            "Column range must have start or end",
        ));
    }

    let qualifier = node.field_text("qualifier");

    let range = ColumnRange {
        start,
        end,
        qualifier,
        namespace_path: NamespacePath::empty(),
    };

    Ok(DomainExpression::Projection(ProjectionExpr::ColumnRange(
        PhaseBoxable::new(range),
    )))
}
