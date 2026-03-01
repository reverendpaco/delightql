// CTE Extraction Module
// Handles extraction of CTEs from query expressions

use crate::error::Result;
use crate::pipeline::sql_ast_v3::{QueryExpression, SqlStatement};

/// CTE extraction - separate pass after transformation
/// This is a tree-rewriting operation
pub fn extract_ctes(query: QueryExpression) -> Result<SqlStatement> {
    // Wrap query expression in a SqlStatement
    Ok(SqlStatement::simple(query))
}
