use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::ProjectionExpr;
use crate::pipeline::{ast_resolved, ast_unresolved};
use crate::resolution::EntityRegistry;

use super::super::column_extraction::extract_provided_column_from_domain_expr;
use super::super::domain_expressions;

/// Resolve the General projection operator
///
/// This handles simple projections (SELECT-like operations) that specify which columns
/// to include in the output. Supports:
/// Resolve the General projection operator using the shared registry
///
/// ScalarSubquery expressions use `resolve_domain_expr_with_registry` (shared context).
/// All other expressions (globs, patterns, lvars) use the existing expansion path
/// with split borrows from the registry.
pub(super) fn resolve_general_with_registry(
    containment_semantic: ast_unresolved::ContainmentSemantic,
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    let mut resolved_expressions = Vec::new();
    for expr in expressions {
        if matches!(
            expr,
            ast_unresolved::DomainExpression::ScalarSubquery { .. }
        ) {
            // ScalarSubquery: use shared registry (preserves all context)
            let resolved = domain_expressions::resolve_domain_expr_with_registry(
                expr, available, registry, false,
            )?;
            resolved_expressions.push(resolved);
        } else {
            // Normal expressions: split borrows for expansion (globs, patterns, etc.)
            let cfe_defs = Some(&registry.query_local.cfes);
            let schema = registry.database.schema();
            let cte_context = &mut registry.query_local.ctes;
            let resolved_exprs = domain_expressions::resolve_expressions_with_schema(
                vec![expr],
                available,
                cfe_defs,
                Some(schema),
                Some(cte_context),
                false,
            )?;
            resolved_expressions.extend(resolved_exprs);
        };
    }

    // Compute output columns
    let mut output_columns = Vec::new();
    for (idx, expr) in resolved_expressions.iter().enumerate() {
        if let Some(col) = extract_provided_column_from_domain_expr(expr, available, idx) {
            output_columns.push(col);
        } else if let ast_resolved::DomainExpression::Projection(ProjectionExpr::Glob {
            qualifier,
            ..
        }) = expr
        {
            if let Some(qual) = qualifier {
                let count_before = output_columns.len();
                for col in available {
                    if let ast_resolved::TableName::Named(table_name) = &col.fq_table.name {
                        if table_name == qual {
                            output_columns.push(col.clone());
                        }
                    }
                }
                if output_columns.len() == count_before {
                    return Err(DelightQLError::validation_error(
                        format!(
                            "Qualified glob '{}.*' matched no columns - table or alias not in scope",
                            qual
                        ),
                        "Check that the qualifier matches a table name or alias in the query",
                    ));
                }
            } else {
                output_columns.extend_from_slice(available);
            }
        }
    }

    if output_columns.is_empty() {
        return Err(DelightQLError::parse_error(
            "Projection matched no columns - would create empty table",
        ));
    }

    let resolved_op = ast_resolved::UnaryRelationalOperator::General {
        containment_semantic:
            super::super::super::helpers::converters::convert_containment_semantic(
                containment_semantic,
            ),
        expressions: resolved_expressions,
    };

    Ok((resolved_op, output_columns))
}
