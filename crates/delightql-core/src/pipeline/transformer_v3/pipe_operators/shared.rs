// Shared utilities for pipe operators

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{DomainExpression, QueryExpression, SelectItem, SelectStatement};

use crate::pipeline::transformer_v3::QualifierScope;

/// Check if an operation requires schema knowledge and provide helpful error messages
pub fn check_schema_dependent_operation(
    cpr_schema: &ast_addressed::CprSchema,
    operation_name: &str,
) -> Result<()> {
    match cpr_schema {
        ast_addressed::CprSchema::Unknown => match operation_name {
            "ProjectOut" => Err(crate::error::DelightQLError::ParseError {
                message: "ProjectOut operation |> -[...] cannot work with unknown TVF schema.\n\
                        ProjectOut requires knowing all columns to compute which ones to exclude.\n\
                        Try using simple projection |> [...] instead."
                    .to_string(),
                source: None,
                subcategory: None,
            }),
            "RenameCover" => Err(crate::error::DelightQLError::ParseError {
                message:
                    "RenameCover operation |> *[old→new] cannot work with unknown TVF schema.\n\
                        RenameCover requires validating that 'old' column names exist.\n\
                        The database will validate column names at runtime."
                        .to_string(),
                source: None,
                subcategory: None,
            }),
            "MapCover" => Err(crate::error::DelightQLError::ParseError {
                message:
                    "MapCover operation |> $(func)([...]) cannot work with unknown TVF schema.\n\
                        MapCover requires validating function application to columns.\n\
                        Use simpler operations that don't require schema validation."
                        .to_string(),
                source: None,
                subcategory: None,
            }),
            _ => Err(crate::error::DelightQLError::ParseError {
                message: format!(
                    "Operation {} requires known schema but TVF schema is unknown",
                    operation_name
                ),
                source: None,
                subcategory: None,
            }),
        },
        _ => Ok(()), // Schema is known (Resolved, Failed, or Unresolved), operation can proceed
    }
}

/// Ensure all columns in a query expression have names (add aliases for unnamed columns)
/// This is needed when the expression will be used as a subquery
pub fn ensure_all_columns_have_names(
    expr: QueryExpression,
    cpr_schema: &ast_addressed::CprSchema,
) -> Result<QueryExpression> {
    // Check if we have schema information
    let columns = match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => cols,
        _ => {
            // No schema information, return as-is
            return Ok(expr);
        }
    };

    // Only process SELECT expressions
    match expr {
        QueryExpression::Select(select) => {
            let items = select.select_list();

            // Check if any columns need aliases
            let needs_aliases = columns.iter().enumerate().any(|(i, col)| {
                if !col.has_user_name {
                    // Check if the corresponding select item lacks an alias
                    if let Some(item) = items.get(i) {
                        matches!(item, SelectItem::Expression { alias, .. } if alias.is_none())
                    } else {
                        false
                    }
                } else {
                    false
                }
            });

            if !needs_aliases {
                // No changes needed
                return Ok(QueryExpression::Select(select));
            }

            // Rebuild with aliases for unnamed columns
            let mut builder = SelectStatement::builder();

            // Copy FROM clause
            if let Some(tables) = select.from() {
                builder = builder.from_tables(tables.to_vec());
            }

            // Copy other clauses
            if let Some(where_clause) = select.where_clause() {
                builder = builder.where_clause(where_clause.clone());
            }
            if let Some(group_by) = select.group_by() {
                builder = builder.group_by(group_by.to_vec());
            }
            if let Some(having) = select.having() {
                builder = builder.having(having.clone());
            }
            if let Some(order_by) = select.order_by() {
                for term in order_by {
                    builder = builder.order_by(term.clone());
                }
            }
            if let Some(limit) = select.limit() {
                if let Some(offset) = limit.offset() {
                    builder = builder.limit_offset(limit.count(), offset);
                } else {
                    builder = builder.limit(limit.count());
                }
            }

            // Add SELECT items with aliases where needed
            for (i, item) in items.iter().enumerate() {
                let new_item = if let Some(col) = columns.get(i) {
                    if !col.has_user_name {
                        // This column needs an alias
                        match item {
                            SelectItem::Expression { expr, alias } if alias.is_none() => {
                                // Add the generated name as an alias
                                SelectItem::expression_with_alias(
                                    expr.clone(),
                                    col.name().to_string(),
                                )
                            }
                            _ => item.clone(), // Already has an alias or is not a simple expression
                        }
                    } else {
                        item.clone()
                    }
                } else {
                    item.clone()
                };
                builder = builder.select(new_item);
            }

            builder
                .build()
                .map(|select| QueryExpression::Select(Box::new(select)))
                .map_err(|e| {
                    DelightQLError::transformation_error(e, "ensure_all_columns_have_names")
                })
        }
        _ => Ok(expr), // Not a SELECT, return as-is
    }
}

/// Column reference using qualified form when source provenance is Named.
/// When the source column comes from a Named table (LAW1 kept joins flat),
/// emit a qualified reference. When Fresh (subquery-wrapped), emit unqualified.
pub fn source_column_ref(
    col_name: &str,
    source_col: Option<&ast_addressed::ColumnMetadata>,
) -> DomainExpression {
    if let Some(src) = source_col {
        if let ast_addressed::TableName::Named(table) = &src.fq_table.name {
            return DomainExpression::with_qualifier(
                QualifierScope::structural(table.as_str()),
                col_name,
            );
        }
    }
    DomainExpression::column(col_name)
}
