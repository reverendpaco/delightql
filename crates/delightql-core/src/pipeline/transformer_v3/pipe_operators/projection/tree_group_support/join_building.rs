// JOIN building for tree group CTEs

use crate::error::Result;
use crate::pipeline::ast_addressed;
use crate::pipeline::sql_ast_v3::{DomainExpression, TableExpression};
use crate::pipeline::transformer_v3::context::TransformContext;
use crate::pipeline::transformer_v3::expression_transformer::transform_domain_expression;
use crate::pipeline::transformer_v3::QualifierScope;

use super::CteJoinInfo;

/// Build FROM clause with JOINs for independent CTEs
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn build_from_with_joins(
    base_table: Option<Vec<TableExpression>>,
    cte_joins: &[CteJoinInfo],
    needs_base_table: bool,
    ctx: &TransformContext,
    source_schema: &ast_addressed::CprSchema,
) -> Result<TableExpression> {
    if !needs_base_table {
        let last_cte =
            cte_joins
                .last()
                .ok_or_else(|| crate::error::DelightQLError::ParseError {
                    message: "No CTEs available but needs_base_table is false".to_string(),
                    source: None,
                    subcategory: None,
                })?;
        return Ok(TableExpression::table(&last_cte.cte_name));
    }

    let mut current_table = base_table
        .and_then(|tables| tables.into_iter().next())
        .ok_or_else(|| crate::error::DelightQLError::ParseError {
            message: "Cannot build JOINs without base table".to_string(),
            source: None,
            subcategory: None,
        })?;

    let base_qualifier = match &current_table {
        TableExpression::Table { alias: Some(a), .. } => a.clone(),
        TableExpression::Table { name, .. } => name.clone(),
        TableExpression::Subquery { alias, .. } => alias.clone(),
        _ => "base".to_string(),
    };

    for cte_info in cte_joins {
        let join_conditions: Vec<DomainExpression> = cte_info
            .join_keys
            .iter()
            .map(|key_expr| {
                let sql_key = transform_domain_expression(
                    key_expr.clone(),
                    ctx,
                    &mut crate::pipeline::transformer_v3::SchemaContext::new(source_schema.clone()),
                )?;
                let base_key = if let DomainExpression::Column { name, .. } = &sql_key {
                    DomainExpression::Column {
                        name: name.clone(),
                        qualifier: Some(QualifierScope::structural(base_qualifier.clone())),
                    }
                } else {
                    sql_key.clone()
                };
                let cte_key = if let DomainExpression::Column { name, .. } = &sql_key {
                    DomainExpression::Column {
                        name: name.clone(),
                        qualifier: Some(QualifierScope::structural(cte_info.cte_name.clone())),
                    }
                } else {
                    sql_key.clone()
                };
                Ok(DomainExpression::eq(base_key, cte_key))
            })
            .collect::<Result<Vec<_>>>()?;

        let on_condition = if join_conditions.is_empty() {
            DomainExpression::literal(ast_addressed::LiteralValue::Boolean(true))
        } else if join_conditions.len() == 1 {
            join_conditions.into_iter().next().unwrap()
        } else {
            DomainExpression::and(join_conditions)
        };

        current_table = TableExpression::left_join(
            current_table,
            TableExpression::table(&cte_info.cte_name),
            on_condition,
        );
    }

    Ok(current_table)
}
