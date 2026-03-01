use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Resolve a scalar subquery expression using the shared registry
///
/// This is the preferred path — namespace, CTE, CFE, and grounding context
/// are all preserved from the outer query.
pub(super) fn resolve_scalar_subquery(
    identifier: ast_unresolved::QualifiedName,
    subquery: ast_unresolved::RelationalExpression,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
) -> Result<ast_resolved::DomainExpression> {
    let (resolved_subquery, _) = super::super::super::resolve_relational_expression_with_registry(
        subquery,
        registry,
        Some(available),
        &super::super::super::ResolutionConfig::default(),
        None,
    )?;

    Ok(ast_resolved::DomainExpression::ScalarSubquery {
        identifier,
        subquery: Box::new(resolved_subquery),
        alias: alias.map(|s| s.into()),
    })
}

/// Resolve a scalar subquery with schema (backward-compat path)
///
/// Creates a fresh registry from the provided schema/system. This loses
/// grounding and borrow context — callers should prefer `resolve_scalar_subquery`
/// with a shared registry when possible.
pub(super) fn resolve_scalar_subquery_with_schema(
    identifier: ast_unresolved::QualifiedName,
    subquery: ast_unresolved::RelationalExpression,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    schema: Option<&dyn super::super::super::DatabaseSchema>,
    mut cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    system: Option<&crate::system::DelightQLSystem>,
) -> Result<ast_resolved::DomainExpression> {
    let db_schema = schema.ok_or_else(|| {
        DelightQLError::ParseError {
            message: "Scalar subquery requires database schema for resolution".to_string(),
            source: None,
            subcategory: None,
        }
    })?;

    let mut temp_registry = if let Some(sys) = system {
        EntityRegistry::new_with_system(db_schema, sys)
    } else {
        EntityRegistry::new(db_schema)
    };

    // Copy CTE context if available
    if let Some(ref ctx) = cte_context {
        temp_registry.query_local.ctes = (*ctx).clone();
    }

    let (resolved_subquery, _) = super::super::super::resolve_relational_expression_with_registry(
        subquery,
        &mut temp_registry,
        Some(available),
        &super::super::super::ResolutionConfig::default(),
        None,
    )?;

    // Copy CTE changes back
    if let Some(ref mut ctx) = cte_context {
        **ctx = temp_registry.query_local.ctes;
    }

    Ok(ast_resolved::DomainExpression::ScalarSubquery {
        identifier,
        subquery: Box::new(resolved_subquery),
        alias: alias.map(|s| s.into()),
    })
}
