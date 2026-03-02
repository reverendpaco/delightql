// Narrowing destructure operator: .column{.field1, .field2}
//
// Iterates a JSON array column via json_each, extracts named fields from
// each element via json_extract. No context carry-forward — the output
// schema contains only the named fields.
//
// Keeps FROM flat (like InteriorDrillDown) to avoid issues with builders
// that don't have select items set yet (e.g., CTE table references).
//
// Generates SQL like:
// ```sql
// SELECT json_extract(_narrow_0.value, '$.name') AS name,
//        json_extract(_narrow_0.value, '$.age') AS age
// FROM source_table, json_each(source_table."col") AS _narrow_0
// ```

use super::super::super::context::TransformContext;
use super::super::super::helpers::alias_generator::next_alias;
use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{
    DomainExpression as SqlExpr, SelectBuilder, SelectItem, SelectStatement, TableExpression,
    TvfArgument,
};

/// Extract the referenceable name from a TableExpression.
fn table_ref_name(table: &TableExpression) -> Option<String> {
    match table {
        TableExpression::Table { alias, name, .. } => {
            Some(alias.as_deref().unwrap_or(name).to_string())
        }
        TableExpression::Subquery { alias, .. } => Some(alias.clone()),
        TableExpression::TVF { alias, .. } => alias.clone(),
        _ => None,
    }
}

pub fn apply_narrowing_destructure(
    builder: SelectBuilder,
    column: String,
    fields: Vec<String>,
    cpr_schema: &ast_addressed::CprSchema,
    _ctx: &TransformContext,
) -> Result<SelectStatement> {
    let narrow_alias = format!("_narrow_{}", next_alias().replace("t", ""));

    // Extract output column names from CprSchema (resolver may have renamed)
    let output_names: Vec<String> = match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols
            .iter()
            .map(|col| {
                col.info
                    .alias_name()
                    .or_else(|| col.info.original_name())
                    .unwrap_or("?")
                    .to_string()
            })
            .collect(),
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns
            .iter()
            .map(|col| {
                col.info
                    .alias_name()
                    .or_else(|| col.info.original_name())
                    .unwrap_or("?")
                    .to_string()
            })
            .collect(),
        _ => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "NarrowingDestructure requires resolved schema".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };

    // Get source table reference from builder's FROM clause
    let source_tables = builder.get_from().cloned().unwrap_or_default();
    let source_ref = source_tables
        .first()
        .and_then(table_ref_name)
        .unwrap_or_else(|| next_alias());

    // Build SELECT items: json_extract(_narrow_N.value, '$.field') AS output_name
    let mut select_items = Vec::new();
    for (i, field) in fields.iter().enumerate() {
        let alias = output_names
            .get(i)
            .cloned()
            .unwrap_or_else(|| field.rsplit('.').next().unwrap_or(field).to_string());
        select_items.push(SelectItem::expression_with_alias(
            SqlExpr::RawSql(format!(
                "json_extract({}.value, '$.{}')",
                narrow_alias, field
            )),
            &alias,
        ));
    }

    // Build FROM: keep existing tables + add json_each TVF
    let json_each_tvf = TableExpression::TVF {
        schema: None,
        function: "json_each".to_string(),
        arguments: vec![TvfArgument::Identifier(format!(
            "\"{}\".\"{}\"",
            source_ref, column
        ))],
        alias: Some(narrow_alias),
    };

    let mut from_tables = source_tables;
    from_tables.push(json_each_tvf);

    let result = builder.set_select(select_items).from_tables(from_tables);

    result
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("NarrowingDestructure: failed to build result: {}", e),
            source: None,
            subcategory: None,
        })
}
