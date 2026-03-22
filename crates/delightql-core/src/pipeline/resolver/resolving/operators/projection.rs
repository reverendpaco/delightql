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

    // Resolve expressions, tracking which ones are engine-managed (glob, pattern, range).
    // Engine-managed expressions are allowed to produce duplicate output names;
    // programmer-authored names must be unique.
    let mut resolved_expressions = Vec::new();
    let mut engine_managed = Vec::new(); // parallel to resolved_expressions

    for expr in expressions {
        let is_engine = matches!(
            expr,
            ast_unresolved::DomainExpression::Projection(
                ProjectionExpr::Glob { .. }
                    | ProjectionExpr::Pattern { .. }
                    | ProjectionExpr::ColumnRange(_)
            )
        );
        if matches!(
            expr,
            ast_unresolved::DomainExpression::ScalarSubquery { .. }
        ) {
            // ScalarSubquery: use fold's transform_domain (preserves all context)
            let resolved = fold.transform_domain(expr)?;
            engine_managed.push(false);
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
            for _ in 0..resolved_exprs.len() {
                engine_managed.push(is_engine);
            }
            resolved_expressions.extend(resolved_exprs);
        };
    }

    // Compute output columns; build a parallel vector tracking which are engine-managed.
    // We don't modify has_user_name on the columns themselves — that flag is used
    // by downstream resolution. The is_engine_col vector is only for our duplicate check.
    let mut output_columns = Vec::new();
    let mut is_engine_col = Vec::new();
    for (idx, expr) in resolved_expressions.iter().enumerate() {
        if let Some(col) = extract_provided_column_from_domain_expr(expr, available, idx) {
            is_engine_col.push(engine_managed[idx]);
            output_columns.push(col);
        } else if let ast_resolved::DomainExpression::Projection(ProjectionExpr::Glob {
            qualifier,
            ..
        }) = expr
        {
            // Globs should already be expanded by resolve_expressions_via_fold,
            // but handle the fallback path just in case.
            if let Some(qual) = qualifier {
                let count_before = output_columns.len();
                for col in available {
                    if let ast_resolved::TableName::Named(table_name) = &col.table_name {
                        if table_name == qual {
                            is_engine_col.push(true);
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
                for col in available {
                    is_engine_col.push(true);
                    output_columns.push(col.clone());
                }
            }
        }
    }

    if output_columns.is_empty() {
        return Err(DelightQLError::parse_error(
            "Projection matched no columns - would create empty table",
        ));
    }

    // Duplicate name check (two rules from the duplicate-column protocol):
    //
    // 1. Programmer-authored names must be unique among themselves.
    //    (age as x, id as x) → error; (u.id, o.id) → error
    //
    // 2. A programmer-authored name must not collide with an engine-managed
    //    name from a wildcard/pattern/range in the same projection.
    //    (*, age) → error if `age` exists in the wildcard expansion
    //
    // Engine-managed names are allowed to collide with each other:
    //    (u.*, o.*) → permitted, engine disambiguates
    {
        let mut seen_user: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        let mut seen_engine: std::collections::HashSet<&str> = std::collections::HashSet::new();
        // First pass: collect engine-managed names
        for (idx, col) in output_columns.iter().enumerate() {
            if is_engine_col[idx] {
                seen_engine.insert(col.name());
            }
        }
        // Second pass: check programmer-authored names
        for (idx, col) in output_columns.iter().enumerate() {
            if is_engine_col[idx] {
                continue;
            }
            let name = col.name();
            // Rule 1: programmer-authored vs programmer-authored
            if let Some(_first_idx) = seen_user.get(name) {
                return Err(DelightQLError::validation_error_categorized(
                    "constraint",
                    format!(
                        "Duplicate column '{}' in projection: programmer-authored names must be unique. \
                         Rename one with 'as' to disambiguate",
                        name,
                    ),
                    "in projection",
                ));
            }
            // Rule 2: programmer-authored vs engine-managed (from glob/pattern)
            if seen_engine.contains(name) {
                return Err(DelightQLError::validation_error_categorized(
                    "constraint",
                    format!(
                        "Duplicate column '{}' in projection: explicit column collides with wildcard expansion. \
                         Rename with 'as' or remove the explicit reference",
                        name,
                    ),
                    "in projection",
                ));
            }
            seen_user.insert(name, idx);
        }
    }

    // Sanitize engine-managed columns that collide
    super::helpers::sanitize_engine_managed_columns(&mut output_columns, &is_engine_col);

    let resolved_op = ast_resolved::UnaryRelationalOperator::General {
        containment_semantic:
            super::super::super::helpers::converters::convert_containment_semantic(
                containment_semantic,
            ),
        expressions: resolved_expressions,
    };

    Ok((resolved_op, output_columns))
}
