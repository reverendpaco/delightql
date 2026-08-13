// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Set-operation validation over arena headings.

use crate::error::{DelightQLError, Result};
use crate::names::{Registry, ScopeId};
use crate::pipeline::ast_resolved;

pub(super) fn validate_union_compatible_schemas(
    left: ScopeId,
    right: ScopeId,
    registry: &Registry,
) -> Result<()> {
    let left_len = registry.known_heading(left)?.len();
    let right_len = registry.known_heading(right)?.len();
    if left_len != right_len {
        return Err(DelightQLError::ParseError {
            message: format!("UNION ALL requires same column count: {left_len} vs {right_len}"),
            source: None,
            subcategory: None,
        });
    }
    if !registry.same_heading_names(left, right) {
        return Err(DelightQLError::parse_error(
            "UNION ALL column names differ; use CORRESPONDING",
        ));
    }
    Ok(())
}

pub(super) fn build_corresponding_schema(
    scopes: &[ScopeId],
    registry: &Registry,
) -> Result<ScopeId> {
    registry.merge_corresponding(scopes)?.ok_or_else(|| {
        DelightQLError::parse_error("Cannot build corresponding schema from empty list")
    })
}

pub(super) fn validate_set_operation_schemas(
    operator: &ast_resolved::SetOperator,
    left: ScopeId,
    right: ScopeId,
    registry: &Registry,
) -> Result<()> {
    match operator {
        ast_resolved::SetOperator::UnionAllPositional
        | ast_resolved::SetOperator::SmartUnionAll => {
            let left_len = registry.known_heading(left)?.len();
            let right_len = registry.known_heading(right)?.len();
            if left_len != right_len {
                return Err(DelightQLError::validation_error_categorized(
                    "set_operation/column_count_mismatch",
                    format!(
                        "Set operation requires both sides to have the same number of columns, \
                         but left has {left_len} and right has {right_len}"
                    ),
                    "Positional union column count mismatch",
                ));
            }
            Ok(())
        }
        ast_resolved::SetOperator::UnionCorresponding
        | ast_resolved::SetOperator::MinusCorresponding => Ok(()),
    }
}
