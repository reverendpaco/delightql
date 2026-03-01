// General projection operator: |> [...]

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{SelectBuilder, SelectItem, SelectStatement};

use super::super::super::context::TransformContext;
use super::super::super::domain_to_select_item_with_name_and_flag;
use super::super::super::schema_context::SchemaContext;

/// Handle General projection operator: |> [...]
pub fn apply_general_projection(
    builder: SelectBuilder,
    expressions: Vec<ast_addressed::DomainExpression>,
    source_schema: &ast_addressed::CprSchema,
    cpr_schema: &ast_addressed::CprSchema,
    ctx: &TransformContext,
) -> Result<SelectStatement> {
    let select_items = if expressions.is_empty() {
        vec![SelectItem::star()]
    } else {
        // Create schema context from source schema
        // After a pipe, we need to decide if qualifiers should be preserved:
        // - Single table source: unqualified (convert Named to Fresh)
        // - Multiple tables (join): qualified (keep Named for disambiguation)
        let source_schema_with_fresh = match source_schema {
            ast_addressed::CprSchema::Resolved(cols) => {
                // Check if all columns come from the same table by comparing table names
                let first_table = cols.first().map(|col| &col.fq_table.name);
                let is_single_table =
                    cols.iter()
                        .all(|col| match (first_table, &col.fq_table.name) {
                            (
                                Some(ast_addressed::TableName::Named(t1)),
                                ast_addressed::TableName::Named(t2),
                            ) => t1 == t2,
                            (
                                Some(ast_addressed::TableName::Fresh),
                                ast_addressed::TableName::Fresh,
                            ) => true,
                            _ => false,
                        });

                let fresh_cols: Vec<_> = cols
                    .iter()
                    .map(|col| {
                        let mut fresh_col = col.clone();
                        // If single table source, convert to Fresh for unqualified references
                        // If join (multiple tables), preserve qualifiers for disambiguation
                        if is_single_table {
                            match &col.fq_table.name {
                                ast_addressed::TableName::Named(_) => {
                                    fresh_col.fq_table.name = ast_addressed::TableName::Fresh;
                                }
                                // Already Fresh (e.g., anonymous table) — no conversion needed
                                ast_addressed::TableName::Fresh => {}
                            }
                        }
                        fresh_col
                    })
                    .collect();
                ast_addressed::CprSchema::Resolved(fresh_cols)
            }
            // Failed/Unresolved/Unknown: shouldn't reach transformer, but if they do,
            // treat as empty schema (no qualifier stripping)
            ast_addressed::CprSchema::Failed { .. }
            | ast_addressed::CprSchema::Unresolved(_)
            | ast_addressed::CprSchema::Unknown => ast_addressed::CprSchema::Resolved(vec![]),
        };
        let mut schema_ctx = SchemaContext::new(source_schema_with_fresh);

        // Extract column metadata from CprSchema to get generated names
        let columns = match cpr_schema {
            ast_addressed::CprSchema::Resolved(cols) => cols,
            // Non-Resolved: use empty column list (names from expressions themselves)
            ast_addressed::CprSchema::Failed { .. }
            | ast_addressed::CprSchema::Unresolved(_)
            | ast_addressed::CprSchema::Unknown => &vec![],
        };

        expressions
            .into_iter()
            .enumerate()
            .map(|(idx, expr)| {
                // Try to get the column name from CprSchema for this position
                let col_metadata = columns.get(idx);
                let generated_name = col_metadata.map(|col| col.name().to_string());
                let has_user_name = col_metadata.map(|col| col.has_user_name).unwrap_or(false);
                domain_to_select_item_with_name_and_flag(
                    expr,
                    generated_name,
                    has_user_name,
                    ctx,
                    &mut schema_ctx,
                )
            })
            .collect::<Result<Vec<_>>>()?
    };

    builder
        .set_select(select_items)
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}
