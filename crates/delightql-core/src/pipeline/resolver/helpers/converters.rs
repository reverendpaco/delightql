// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::pipeline::ast_resolved;

use crate::pipeline::ast_unresolved;

pub(in super::super) fn convert_literal_value(
    value: ast_unresolved::LiteralValue,
) -> ast_resolved::LiteralValue {
    match value {
        ast_unresolved::LiteralValue::String(s) => ast_resolved::LiteralValue::String(s),
        ast_unresolved::LiteralValue::Number(n) => ast_resolved::LiteralValue::Number(n),
        ast_unresolved::LiteralValue::Boolean(b) => ast_resolved::LiteralValue::Boolean(b),
        ast_unresolved::LiteralValue::Null => ast_resolved::LiteralValue::Null,
        ast_unresolved::LiteralValue::Symbol(s) => ast_resolved::LiteralValue::Symbol(s),
        ast_unresolved::LiteralValue::Mention(s) => ast_resolved::LiteralValue::Mention(s),
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
