// MetaIze operator: ^ or ^^
// Reifies the input relation's schema as queryable data

use crate::error::Result;
use crate::pipeline::ast_addressed::{self, LiteralValue, TableName};
use crate::pipeline::sql_ast_v3::{
    DomainExpression, QueryExpression, SelectBuilder, SelectItem, SelectStatement,
};

/// Handle MetaIze operator: |> ^ or |> ^^
///
/// This synthesizes a relation from the input schema at compile time.
/// - `^` returns basic schema: scope, column_name, ordinal
/// - `^^` returns detailed schema: scope, column_name, ordinal, data_type, nullable
///
/// The `scope` column shows which table owns each column:
/// - Named table → table name (e.g. "users", "products")
/// - Fresh/unqualified → "_" (the FULL sigil)
///
/// The output uses a VALUES subquery with appropriate column aliases.
pub fn apply_meta_ize(
    _builder: SelectBuilder,
    detailed: bool,
    source_schema: &ast_addressed::CprSchema,
) -> Result<SelectStatement> {
    // Get the columns from the schema
    let columns = match source_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => resolved_columns,
        _ => {
            return Err(crate::error::DelightQLError::ParseError {
                message: "MetaIze requires resolved schema".to_string(),
                source: None,
                subcategory: None,
            })
        }
    };

    // Handle empty schema
    if columns.is_empty() {
        return Err(crate::error::DelightQLError::ParseError {
            message: "MetaIze: input relation has no columns".to_string(),
            source: None,
            subcategory: None,
        });
    }

    // Build VALUES rows from schema metadata
    let mut value_rows = Vec::new();

    for (idx, col) in columns.iter().enumerate() {
        let col_name = col
            .info
            .alias_name()
            .or_else(|| col.info.original_name())
            .unwrap_or("?")
            .to_string();

        // Scope: the owning table name, or "_" for unqualified/fresh columns
        let scope = match &col.fq_table.name {
            TableName::Named(name) => name.to_string(),
            TableName::Fresh => "_".to_string(),
        };

        let row = if detailed {
            // Detailed schema: scope, column_name, ordinal, data_type, nullable
            vec![
                DomainExpression::literal(LiteralValue::String(scope)),
                DomainExpression::literal(LiteralValue::String(col_name)),
                DomainExpression::literal(LiteralValue::Number((idx + 1).to_string())),
                DomainExpression::literal(LiteralValue::String("unknown".to_string())),
                DomainExpression::literal(LiteralValue::String("true".to_string())),
            ]
        } else {
            // Basic schema: scope, column_name, ordinal
            vec![
                DomainExpression::literal(LiteralValue::String(scope)),
                DomainExpression::literal(LiteralValue::String(col_name)),
                DomainExpression::literal(LiteralValue::Number((idx + 1).to_string())),
            ]
        };

        value_rows.push(row);
    }

    // Create VALUES expression as a subquery
    let values_query = QueryExpression::Values { rows: value_rows };

    // Select with literal column aliases for the first row (SQLite doesn't support column aliases on VALUES)
    // We'll select the values and use AS to name them
    let column_names: Vec<&str> = if detailed {
        vec!["scope", "column_name", "ordinal", "data_type", "nullable"]
    } else {
        vec!["scope", "column_name", "ordinal"]
    };

    // Select all columns from the VALUES subquery
    // Note: Column referencing depends on database - for now use positional
    let select_items: Vec<SelectItem> = column_names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            SelectItem::expression_with_alias(
                DomainExpression::column(&format!("column{}", i + 1)),
                *name,
            )
        })
        .collect();

    // Build the final SELECT statement
    SelectBuilder::new()
        .set_select(select_items)
        .from_subquery(values_query, "_meta")
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: e,
            source: None,
            subcategory: None,
        })
}
