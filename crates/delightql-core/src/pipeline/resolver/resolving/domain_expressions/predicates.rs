use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Resolve a predicate expression wrapped in DomainExpression::Predicate
pub(super) fn resolve_predicate_expr(
    expr: ast_unresolved::BooleanExpression,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    schema: Option<&dyn super::super::super::DatabaseSchema>,
    cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    // Use the appropriate predicate resolver based on available context
    let resolved_pred = if let (Some(db_schema), Some(ctx)) = (schema, cte_context) {
        // Have full context - can handle EXISTS, IN, scalar subqueries, etc.
        super::super::predicates::resolve_predicate_with_schema(
            expr,
            available,
            db_schema,
            ctx,
            in_correlation,
            cfe_defs,
        )?
    } else {
        // Limited context - can only handle simple predicates (Comparison, And, Or, Not)
        super::super::predicates::resolve_boolean_expression(expr, available)?
    };
    Ok(ast_resolved::DomainExpression::Predicate {
        expr: Box::new(resolved_pred),
        alias: alias.map(|s| s.into()),
    })
}

/// Resolve a predicate expression using the shared registry
pub(super) fn resolve_predicate_expr_with_registry(
    expr: ast_unresolved::BooleanExpression,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
) -> Result<ast_resolved::DomainExpression> {
    let resolved_pred = super::super::predicates::resolve_predicate_with_registry(
        expr, available, registry, in_correlation,
        &crate::pipeline::resolver::ResolutionConfig::default(),
    )?;
    Ok(ast_resolved::DomainExpression::Predicate {
        expr: Box::new(resolved_pred),
        alias: alias.map(|s| s.into()),
    })
}
