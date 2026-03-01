// Ordering and positioning operators: TupleOrdering, Reposition

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{SelectBuilder, SelectItem, SelectStatement};

use super::super::context::TransformContext;
use super::super::expression_transformer::transform_domain_expression;

/// Handle TupleOrdering operator: |> ^[col asc]
pub fn apply_tuple_ordering(
    builder: SelectBuilder,
    specs: Vec<ast_addressed::OrderingSpec>,
    ctx: &TransformContext,
    source_schema: &ast_addressed::CprSchema,
) -> Result<SelectStatement> {
    // TupleOrdering: ORDER BY specified columns
    let mut result_builder = builder;
    let mut schema_ctx = crate::pipeline::transformer_v3::SchemaContext::new(source_schema.clone());

    // If there are no select items yet (e.g., direct from table), add SELECT *
    if !result_builder.has_select_items() {
        result_builder = result_builder.select(SelectItem::star());
    }

    for spec in specs {
        let expr = transform_domain_expression(spec.column, ctx, &mut schema_ctx)?;
        let direction = match spec.direction {
            Some(ast_addressed::OrderDirection::Ascending) | None => {
                Some(crate::pipeline::sql_ast_v3::OrderDirection::Asc)
            }
            Some(ast_addressed::OrderDirection::Descending) => {
                Some(crate::pipeline::sql_ast_v3::OrderDirection::Desc)
            }
        };
        let order_term = crate::pipeline::sql_ast_v3::OrderTerm::new(expr, direction);
        result_builder = result_builder.order_by(order_term);
    }

    // TupleOrdering doesn't change the projection, just adds ORDER BY
    result_builder
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}

/// Handle Reposition operator: |> @[col: pos]
pub fn apply_reposition(
    builder: SelectBuilder,
    moves: Vec<ast_addressed::RepositionSpec>,
    cpr_schema: &ast_addressed::CprSchema,
    source_schema_updated: &ast_addressed::CprSchema,
) -> Result<SelectStatement> {
    // Reposition: Reorder columns based on position specifications
    // Get the available columns from CPR schema
    let columns = match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols.clone(),
        _ => {
            return Err(crate::error::DelightQLError::transformation_error(
                "Reposition requires resolved schema",
                "reposition",
            ));
        }
    };

    // Apply the reposition algorithm
    let num_columns = columns.len();
    let mut result = vec![None; num_columns];
    let mut moved_indices = std::collections::HashSet::new();

    // Process each move
    for spec in moves {
        // Find the column index
        let column_idx = match &spec.column {
            ast_addressed::DomainExpression::Lvar { name, .. } => columns
                .iter()
                .position(|col| col.name().eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    crate::error::DelightQLError::transformation_error(
                        format!("Column '{}' not found", name),
                        "reposition",
                    )
                })?,
            ast_addressed::DomainExpression::ColumnOrdinal(_ordinal) => {
                // In refined phase, ordinal should already be resolved to a value
                // For now, we'll reject ordinals in reposition
                return Err(crate::error::DelightQLError::transformation_error(
                    "Column ordinals not yet supported in reposition",
                    "reposition",
                ));
            }
            _ => {
                return Err(crate::error::DelightQLError::transformation_error(
                    "Reposition only supports column names and ordinals",
                    "reposition",
                ));
            }
        };

        // Normalize negative positions
        let mut target_pos = spec.position;
        if target_pos < 0 {
            target_pos = (num_columns as i32) + target_pos + 1;
        }

        // Validate position range
        if target_pos < 1 || target_pos > num_columns as i32 {
            return Err(crate::error::DelightQLError::transformation_error(
                format!(
                    "Position {} is out of range for {} columns",
                    spec.position, num_columns
                ),
                "reposition",
            ));
        }

        // Check if this column has already been moved
        if moved_indices.contains(&column_idx) {
            let col_name = columns[column_idx].name();
            return Err(crate::error::DelightQLError::transformation_error(
                format!("Column '{}' appears multiple times in reposition", col_name),
                "reposition",
            ));
        }

        let target_idx = (target_pos - 1) as usize;

        // Check for duplicate target position
        if result[target_idx].is_some() {
            return Err(crate::error::DelightQLError::transformation_error(
                format!("Multiple columns cannot target position {}", target_pos),
                "reposition",
            ));
        }

        result[target_idx] = Some(&columns[column_idx]);
        moved_indices.insert(column_idx);
    }

    // Fill remaining positions with unmoved columns in order
    let mut remaining: Vec<&ast_addressed::ColumnMetadata> = columns
        .iter()
        .enumerate()
        .filter(|(idx, _)| !moved_indices.contains(idx))
        .map(|(_, col)| col)
        .collect();

    for slot in result.iter_mut() {
        if slot.is_none() && !remaining.is_empty() {
            *slot = Some(remaining.remove(0));
        }
    }

    let source_cols = match source_schema_updated {
        ast_addressed::CprSchema::Resolved(cols) => Some(cols.as_slice()),
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => Some(resolved_columns.as_slice()),
        other => panic!(
            "catch-all hit in pipe_operators/ordering.rs apply_ordering (CprSchema): {:?}",
            other
        ),
    };

    // Build select items in the new order, using qualified refs when available
    let select_items: Vec<SelectItem> = result
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(_, col)| {
            let col_name = col.name();
            // Find the source column by name to get table provenance
            let source_col = source_cols
                .and_then(|src| src.iter().find(|s| s.info.name().unwrap_or("") == col_name));
            let expr = super::shared::source_column_ref(col_name, source_col);
            SelectItem::Expression {
                expr,
                alias: Some(col_name.to_string()),
            }
        })
        .collect();

    builder
        .set_select(select_items)
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}
