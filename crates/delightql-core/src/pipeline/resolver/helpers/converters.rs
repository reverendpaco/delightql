use crate::pipeline::ast_resolved;

use crate::pipeline::ast_unresolved;
pub(in super::super) fn convert_containment_semantic(
    cs: ast_unresolved::ContainmentSemantic,
) -> ast_resolved::ContainmentSemantic {
    match cs {
        ast_unresolved::ContainmentSemantic::Bracket => ast_resolved::ContainmentSemantic::Bracket,
        ast_unresolved::ContainmentSemantic::Parenthesis => {
            ast_resolved::ContainmentSemantic::Parenthesis
        }
    }
}

pub(in super::super) fn convert_identifier(
    id: ast_unresolved::QualifiedName,
) -> ast_resolved::QualifiedName {
    ast_resolved::QualifiedName {
        namespace_path: id.namespace_path,
        name: id.name,
        grounding: None,
    }
}

pub(in super::super) fn convert_using_column(
    col: ast_unresolved::UsingColumn,
) -> ast_resolved::UsingColumn {
    match col {
        ast_unresolved::UsingColumn::Regular(id) => {
            ast_resolved::UsingColumn::Regular(convert_identifier(id))
        }
        ast_unresolved::UsingColumn::Negated(id) => {
            ast_resolved::UsingColumn::Negated(convert_identifier(id))
        }
    }
}

pub(in super::super) fn convert_literal_value(
    value: ast_unresolved::LiteralValue,
) -> ast_resolved::LiteralValue {
    match value {
        ast_unresolved::LiteralValue::String(s) => ast_resolved::LiteralValue::String(s),
        ast_unresolved::LiteralValue::Number(n) => ast_resolved::LiteralValue::Number(n),
        ast_unresolved::LiteralValue::Boolean(b) => ast_resolved::LiteralValue::Boolean(b),
        ast_unresolved::LiteralValue::Null => ast_resolved::LiteralValue::Null,
    }
}

pub(in super::super) fn convert_order_direction(
    dir: Option<ast_unresolved::OrderDirection>,
) -> Option<ast_resolved::OrderDirection> {
    dir.map(|d| match d {
        ast_unresolved::OrderDirection::Ascending => ast_resolved::OrderDirection::Ascending,
        ast_unresolved::OrderDirection::Descending => ast_resolved::OrderDirection::Descending,
    })
}
