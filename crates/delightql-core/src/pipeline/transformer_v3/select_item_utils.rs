// SelectItem Utilities
//
// Helper functions for converting DomainExpressions to SelectItems with alias support

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::SelectItem;

use super::context::TransformContext;
use super::expression_transformer::transform_domain_expression;
use super::schema_context::SchemaContext;

pub(crate) fn domain_to_select_item_with_name_and_flag(
    expr: ast_addressed::DomainExpression,
    generated_name: Option<String>,
    _has_user_name: bool,
    ctx: &TransformContext,
    schema_ctx: &mut SchemaContext,
) -> Result<SelectItem> {
    use ast_addressed::{DomainExpression, FunctionExpression};

    // Extract alias before transforming
    let alias = match &expr {
        DomainExpression::Lvar { alias, .. } => alias.clone(),
        DomainExpression::Literal { alias, .. } => alias.clone(),
        DomainExpression::Function(func) => match func {
            FunctionExpression::Regular { alias, .. } => alias.clone(),
            FunctionExpression::Infix { alias, .. } => alias.clone(),
            FunctionExpression::Bracket { alias, .. } => alias.clone(),
            FunctionExpression::Curried { .. } => None,
            FunctionExpression::Lambda { alias, .. } => alias.clone(),
            FunctionExpression::StringTemplate { .. } => None,
            FunctionExpression::CaseExpression { alias, .. } => alias.clone(),
            FunctionExpression::HigherOrder { alias, .. } => alias.clone(),
            FunctionExpression::Curly { alias, .. } => alias.clone(),
            FunctionExpression::Array { alias, .. } => alias.clone(),
            FunctionExpression::MetadataTreeGroup { alias, .. } => alias.clone(),
            FunctionExpression::Window { alias, .. } => alias.clone(),
            FunctionExpression::JsonPath { alias, .. } => alias.clone(),
        },
        DomainExpression::Predicate { alias, .. } => alias.clone(),
        DomainExpression::Projection(ref proj) => {
            use ast_addressed::ProjectionExpr;
            match proj {
                ProjectionExpr::Glob { .. } => None,
                ProjectionExpr::ColumnRange(_) => None,
                ProjectionExpr::Pattern { alias, .. } => alias.clone(),
                ProjectionExpr::JsonPathLiteral { alias, .. } => alias.clone(),
            }
        }
        DomainExpression::NonUnifiyingUnderscore => None,
        DomainExpression::ValuePlaceholder { alias, .. } => alias.clone(),
        DomainExpression::Substitution(_) => {
            unreachable!("SubstitutionExpr should not survive to Addressed phase")
        }
        DomainExpression::PipedExpression { alias, .. } => alias.clone(),
        DomainExpression::Parenthesized { alias, .. } => alias.clone(),
        DomainExpression::ColumnOrdinal(_) => {
            unreachable!("ColumnOrdinal should not survive to Addressed phase")
        }
        DomainExpression::Tuple { alias, .. } => alias.clone(),
        DomainExpression::ScalarSubquery { alias, .. } => alias.clone(),
        // Pivot handled at modulo level, but extract alias for completeness
        DomainExpression::PivotOf { .. } => None,
    };

    // Transform to SQL expression
    let sql_expr = transform_domain_expression(expr, ctx, schema_ctx)?;

    // Create SelectItem with alias (explicit or generated)
    // NOTE: We emit ALL generated names in SQL, not just user-provided ones.
    // System-generated names (sum_1, avg_2, etc.) are needed for ordinal projections
    // and other references. The has_user_name flag is for display/error messages only.
    Ok(match alias {
        Some(alias_str) => SelectItem::expression_with_alias(sql_expr, alias_str),
        None => match generated_name {
            Some(name) => SelectItem::expression_with_alias(sql_expr, name),
            None => SelectItem::expression(sql_expr),
        },
    })
}
