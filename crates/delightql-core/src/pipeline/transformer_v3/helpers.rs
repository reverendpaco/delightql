/// Helper functions and utilities for the transformer_v3 module.
/// These are pure functions with no dependencies on complex state or side effects.
use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::generator_v3::SqlDialect;
use crate::pipeline::sql_ast_v3::{JoinType, TableExpression};

/// Convert refined AST join type to SQL AST join type
/// FULL OUTER JOIN for SQLite will be expanded to UNION ALL in segment_handler
pub fn convert_join_type(
    join_type: ast_addressed::JoinType,
    _dialect: SqlDialect,
) -> Result<JoinType> {
    match join_type {
        ast_addressed::JoinType::Inner => Ok(JoinType::Inner),
        ast_addressed::JoinType::LeftOuter => Ok(JoinType::Left),
        ast_addressed::JoinType::RightOuter => Ok(JoinType::Right),
        ast_addressed::JoinType::FullOuter => Ok(JoinType::Full),
    }
}

/// Extract the alias from a TableExpression if it has one
pub fn extract_table_alias(table: &TableExpression) -> Option<String> {
    match table {
        TableExpression::Table { alias, name, .. } => {
            // Use alias if present, otherwise use table name
            alias.clone().or_else(|| Some(name.clone()))
        }
        TableExpression::Subquery { alias, .. } => Some(alias.clone()),
        TableExpression::UnionTable { alias, .. } => Some(alias.clone()),
        TableExpression::Join { .. } => None, // Joins don't have a single alias
        TableExpression::Values { .. } => None, // Values don't have an alias
        TableExpression::TVF {
            alias, function, ..
        } => {
            // Use alias if present, otherwise use function name
            alias.clone().or_else(|| Some(function.clone()))
        }
    }
}

/// Helper to extract column name from a DomainExpression
pub fn extract_column_name(expr: &ast_addressed::DomainExpression) -> Option<String> {
    match expr {
        ast_addressed::DomainExpression::Lvar { name, .. } => Some(name.to_string()),
        other => panic!(
            "catch-all hit in helpers.rs extract_column_name: {:?}",
            other
        ),
    }
}

/// Create a simple Lvar AST node for a column name (unqualified, used for @ substitution)
///
/// This helper eliminates duplication of the Lvar creation pattern used across
/// transformation operators when creating AST column references for placeholder substitution.
pub fn create_column_lvar(col_name: &str) -> ast_addressed::DomainExpression {
    ast_addressed::DomainExpression::Lvar {
        name: col_name.into(),
        qualifier: None, // Unqualified - column is already in scope
        namespace_path: ast_addressed::NamespacePath::empty(),
        alias: None,
        provenance: crate::pipeline::asts::addressed::PhaseBox::phantom(),
    }
}

/// Extract function name and curried arguments for MapCover operations
pub fn extract_function_with_args(
    func: &ast_addressed::FunctionExpression,
) -> Result<(String, Vec<ast_addressed::DomainExpression>)> {
    match func {
        ast_addressed::FunctionExpression::Regular {
            name, arguments, ..
        } => {
            // Regular functions in MapCover: column is implicit first arg
            // Any arguments here would be curried and come after the column
            Ok((name.to_string(), arguments.clone()))
        }
        ast_addressed::FunctionExpression::Curried {
            name, arguments, ..
        } => {
            // Curried functions: these arguments come AFTER the column
            Ok((name.to_string(), arguments.clone()))
        }
        ast_addressed::FunctionExpression::Window {
            name, arguments, ..
        } => {
            // Window functions: column is implicit first arg (before window context)
            // Any arguments here would be curried and come after the column
            Ok((name.to_string(), arguments.clone()))
        }
        ast_addressed::FunctionExpression::HigherOrder {
            name,
            regular_arguments,
            ..
        } => {
            // Higher-order functions: use regular arguments (curried args already bound)
            Ok((name.to_string(), regular_arguments.clone()))
        }
        _ => Err(crate::error::DelightQLError::ParseError {
            message: "Unsupported function type for MapCover".to_string(),
            source: None,
            subcategory: None,
        }),
    }
}

/// Compute the SQL qualifier for a column based on its identity stack and transform context
/// Helper to get resolved columns from CprSchema
pub fn get_resolved_columns(
    cpr_schema: &ast_addressed::CprSchema,
) -> Result<&[ast_addressed::ColumnMetadata]> {
    match cpr_schema {
        ast_addressed::CprSchema::Resolved(cols) => Ok(cols),
        ast_addressed::CprSchema::Failed {
            resolved_columns, ..
        } => Ok(resolved_columns),
        ast_addressed::CprSchema::Unresolved(_) => {
            // If unresolved, we can't get the full schema
            Err(crate::error::DelightQLError::ParseError {
                message: "Cover operators require resolved schema".to_string(),
                source: None,
                subcategory: None,
            })
        }
        ast_addressed::CprSchema::Unknown => {
            // If unknown schema, we can't get column information
            Err(crate::error::DelightQLError::ParseError {
                message: "Cover operators require known schema (unknown TVF schema)".to_string(),
                source: None,
                subcategory: None,
            })
        }
    }
}

/// Alias generation module - manages unique alias generation for subqueries
pub mod alias_generator {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    pub fn next_alias() -> String {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("t{}", n)
    }
}
