//! TVF (Table-Valued Function) schema resolution
//!
//! TECHNICAL DEBT: This module hardcodes column schemas for known TVFs.
//! The correct fix is making the resolver permissive about column references
//! from Unknown-schema tables, then deleting this file entirely.
//! See memory/tvf-passthrough-fix.md for the full plan.

use crate::pipeline::ast_resolved;

/// Create column metadata for a TVF column
pub(super) fn create_tvf_column_metadata(
    name: &str,
    _data_type: &str,
    table_name: &str,
    position: usize,
) -> ast_resolved::ColumnMetadata {
    let table = ast_resolved::TableName::Named(table_name.into());
    ast_resolved::ColumnMetadata {
        info: ast_resolved::ColumnProvenance::from_table_column(
            name.to_string(),
            table.clone(),
            false, // TVF columns are not qualified in source
        ),
        fq_table: ast_resolved::FqTable {
            parents_path: crate::pipeline::asts::resolved::NamespacePath::empty(),
            name: table,
            backend_schema: ast_resolved::PhaseBox::from_optional_schema(None), // TVFs don't have backend schemas
        },
        table_position: Some(position),
        has_user_name: true, // Table columns have user names
        needs_hygienic_alias: false,
        needs_sql_rename: false,
        interior_schema: None,
    }
}

/// Hardcoded TVF schemas for known functions.
///
/// TECHNICAL DEBT: This should be replaced by runtime introspection.
/// TVF columns should be discovered by the backend at execution time,
/// with the resolver allowing Unknown-schema column references through.
pub(super) fn get_tvf_schema(function: &str, alias: Option<&str>) -> ast_resolved::CprSchema {
    let table_name = alias.unwrap_or(function);

    match function {
        "json_each" => {
            let columns = vec![
                create_tvf_column_metadata("key", "TEXT", table_name, 0),
                create_tvf_column_metadata("value", "TEXT", table_name, 1),
                create_tvf_column_metadata("type", "TEXT", table_name, 2),
                create_tvf_column_metadata("atom", "TEXT", table_name, 3),
                create_tvf_column_metadata("id", "INTEGER", table_name, 4),
                create_tvf_column_metadata("parent", "INTEGER", table_name, 5),
                create_tvf_column_metadata("fullkey", "TEXT", table_name, 6),
                create_tvf_column_metadata("path", "TEXT", table_name, 7),
            ];
            ast_resolved::CprSchema::Resolved(columns)
        }
        "pragma_table_info" => {
            let columns = vec![
                create_tvf_column_metadata("cid", "INTEGER", table_name, 0),
                create_tvf_column_metadata("name", "TEXT", table_name, 1),
                create_tvf_column_metadata("type", "TEXT", table_name, 2),
                create_tvf_column_metadata("notnull", "INTEGER", table_name, 3),
                create_tvf_column_metadata("dflt_value", "TEXT", table_name, 4),
                create_tvf_column_metadata("pk", "INTEGER", table_name, 5),
            ];
            ast_resolved::CprSchema::Resolved(columns)
        }
        _other => ast_resolved::CprSchema::Unknown,
    }
}
