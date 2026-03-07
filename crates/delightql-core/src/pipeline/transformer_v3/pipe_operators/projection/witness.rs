// Witness operator: + or \+
// Reifies existence of the input relation as a 1-row, 1-column relation

use crate::error::Result;
use crate::pipeline::sql_ast_v3::{
    DomainExpression, QueryExpression, SelectBuilder, SelectItem, SelectStatement,
};

/// Handle Witness operator: |> + or |> \+
///
/// Generates:
///   `+`  → SELECT EXISTS(SELECT 1 FROM (<source>)) AS "met"
///   `\+` → SELECT NOT EXISTS(SELECT 1 FROM (<source>)) AS "met"
///
/// The source builder is finalized into a subquery that becomes
/// the argument to EXISTS/NOT EXISTS.
pub fn apply_witness(
    builder: SelectBuilder,
    exists: bool,
    _source_schema: &crate::pipeline::ast_addressed::CprSchema,
) -> Result<SelectStatement> {
    // The builder has FROM tables but may not have SELECT items.
    // We need SELECT 1 FROM <source_tables> [WHERE ...] for the EXISTS subquery.
    // Reuse the builder's FROM and WHERE, replace SELECT with literal 1.
    let inner_query = builder
        .set_select(vec![SelectItem::expression(DomainExpression::literal(
            crate::pipeline::ast_addressed::LiteralValue::Number("1".to_string()),
        ))])
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("Witness: failed to build inner query: {}", e),
            source: None,
            subcategory: None,
        })?;

    let inner_expr = QueryExpression::Select(Box::new(inner_query));

    // Build: EXISTS(<inner>) or NOT EXISTS(<inner>)
    let exists_expr = if exists {
        DomainExpression::exists(inner_expr)
    } else {
        DomainExpression::not_exists(inner_expr)
    };

    // Build: SELECT <exists_expr> AS "met"
    SelectBuilder::new()
        .select(SelectItem::expression_with_alias(exists_expr, "met"))
        .build()
        .map_err(|e| crate::error::DelightQLError::ParseError {
            message: format!("Witness: failed to build outer query: {}", e),
            source: None,
            subcategory: None,
        })
}
