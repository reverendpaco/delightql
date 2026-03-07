use super::super::column_extraction::extract_provided_column_from_domain_expr;
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::ProjectionExpr;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

/// Resolve the General projection operator via fold-based dispatch
///
/// Same semantics as `resolve_general_with_registry`, but expression resolution
/// goes through the fold's transform hooks instead of free functions + registry.
pub(super) fn resolve_general_via_fold(
    fold: &mut ResolverFold,
    containment_semantic: ast_unresolved::ContainmentSemantic,
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<(
    ast_resolved::UnaryRelationalOperator,
    Vec<ast_resolved::ColumnMetadata>,
)> {
    // Detect embed duplicate: when a glob is present alongside explicit expressions
    // whose alias matches an existing column, reject early. This catches +(expr as col)
    // where col already exists — user should use $$(expr as col) to replace instead.
    // Only check explicit non-glob expression aliases, NOT glob-on-glob overlap
    // (which is valid for multi-table joins like (u.*, o.*)).
    let has_glob = expressions.iter().any(|e| {
        matches!(
            e,
            ast_unresolved::DomainExpression::Projection(ProjectionExpr::Glob { .. })
        )
    });
    if has_glob {
        for expr in &expressions {
            // Skip glob/projection expressions — only check explicit value expressions
            if matches!(expr, ast_unresolved::DomainExpression::Projection(_)) {
                continue;
            }
            let alias = match expr {
                ast_unresolved::DomainExpression::Literal { alias, .. } => alias.as_ref(),
                ast_unresolved::DomainExpression::Lvar { alias, .. } => alias.as_ref(),
                ast_unresolved::DomainExpression::Function(func) => {
                    use ast_unresolved::FunctionExpression as FE;
                    match func {
                        FE::Regular { alias, .. }
                        | FE::Bracket { alias, .. }
                        | FE::Infix { alias, .. }
                        | FE::Lambda { alias, .. }
                        | FE::CaseExpression { alias, .. }
                        | FE::Window { alias, .. }
                        | FE::Curly { alias, .. }
                        | FE::Array { alias, .. }
                        | FE::MetadataTreeGroup { alias, .. }
                        | FE::JsonPath { alias, .. }
                        | FE::HigherOrder { alias, .. } => alias.as_ref(),
                        _ => None,
                    }
                }
                _ => None,
            };
            if let Some(alias_name) = alias {
                if available
                    .iter()
                    .any(|col| col.name() == alias_name.as_str())
                {
                    return Err(DelightQLError::validation_error_categorized(
                        "constraint",
                        format!(
                            "Duplicate column '{}' in embed projection: column already exists in source schema. \
                             Use $$(expr as {}) to replace the existing column instead",
                            alias_name, alias_name,
                        ),
                        "in embed projection",
                    ));
                }
            }
        }
    }

    let mut resolved_expressions = Vec::new();
    for expr in expressions {
        if matches!(
            expr,
            ast_unresolved::DomainExpression::ScalarSubquery { .. }
        ) {
            // ScalarSubquery: use fold's transform_domain (preserves all context)
            let resolved = fold.transform_domain(expr)?;
            resolved_expressions.push(resolved);
        } else {
            // Normal expressions: use fold-based expansion (globs, patterns, etc.)
            let resolved_exprs =
                super::super::domain_expressions::projection::resolve_expressions_via_fold(
                    fold,
                    vec![expr],
                    available,
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
