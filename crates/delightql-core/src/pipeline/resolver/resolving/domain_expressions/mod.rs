// Modularized domain expression resolution components
// Each module handles a specific category of DomainExpression variants

mod compound;
mod functions;
mod predicates;
mod projection;
mod simple;
mod subqueries;

use crate::error::Result;
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Resolve a domain expression with available schema
///
/// This is the standard resolution path for simple expressions (lvars, literals, functions)
/// that only need to know what columns are available. For expressions that need database
/// schema access (like ScalarSubquery), use `resolve_domain_expr_with_full_context` instead.
pub(in crate::pipeline::resolver) fn resolve_domain_expr_with_schema(
    expr: ast_unresolved::DomainExpression,
    available: &[ast_resolved::ColumnMetadata],
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    resolve_domain_expr_with_schema_and_context(expr, available, None, None, false, cfe_defs, None)
}

/// Resolve a domain expression with full context (schema + CTE)
///
/// This is required for expressions that reference tables or CTEs (like ScalarSubquery)
/// that need to resolve nested queries. For simple expressions (lvars, literals),
/// use `resolve_domain_expr_with_schema` instead.
///
/// # Parameters
/// - `expr`: The expression to resolve
/// - `available`: Columns available in the current scope
/// - `schema`: Database schema for table lookups
/// - `cte_context`: CTE definitions for subquery resolution
/// - `in_correlation`: True if resolving in a correlation context (EXISTS, union correlations)
pub(crate) fn resolve_domain_expr_with_full_context(
    expr: ast_unresolved::DomainExpression,
    available: &[ast_resolved::ColumnMetadata],
    schema: &dyn super::super::DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    resolve_domain_expr_with_schema_and_context(
        expr,
        available,
        Some(schema),
        Some(cte_context),
        in_correlation,
        cfe_defs,
        None,
    )
}

/// Like `resolve_domain_expr_with_full_context` but with optional system reference
/// for namespace-aware resolution inside scalar subqueries.
pub(crate) fn resolve_domain_expr_with_full_context_and_system(
    expr: ast_unresolved::DomainExpression,
    available: &[ast_resolved::ColumnMetadata],
    schema: &dyn super::super::DatabaseSchema,
    cte_context: &mut HashMap<String, ast_resolved::CprSchema>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
    system: Option<&crate::system::DelightQLSystem>,
) -> Result<ast_resolved::DomainExpression> {
    resolve_domain_expr_with_schema_and_context(
        expr,
        available,
        Some(schema),
        Some(cte_context),
        in_correlation,
        cfe_defs,
        system,
    )
}

/// Internal resolution function with optional context parameters
///
/// All resolution paths funnel through this function which handles all expression types.
/// The schema and cte_context parameters are optional to support different resolution contexts:
/// - None: Simple expressions only (lvars, literals, functions)
/// - Some: Full expressions including table/CTE references (ScalarSubquery, InnerExists)
pub(super) fn resolve_domain_expr_with_schema_and_context(
    expr: ast_unresolved::DomainExpression,
    available: &[ast_resolved::ColumnMetadata],
    schema: Option<&dyn super::super::DatabaseSchema>,
    cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
    system: Option<&crate::system::DelightQLSystem>,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        // Simple expressions (lvars, literals, parameters)
        ast_unresolved::DomainExpression::Lvar { .. }
        | ast_unresolved::DomainExpression::ColumnOrdinal(_)
        | ast_unresolved::DomainExpression::Literal { .. }
        | ast_unresolved::DomainExpression::ValuePlaceholder { .. }
        | ast_unresolved::DomainExpression::Substitution(_)
        | ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
            simple::resolve_simple_expr(expr, available, in_correlation)
        }

        // Projection-only expressions (error out in single-value contexts)
        ast_unresolved::DomainExpression::Projection(_) => {
            simple::resolve_projection_only_expr(expr)
        }

        // Functions
        ast_unresolved::DomainExpression::Function(func) => functions::resolve_function_expr(
            func,
            available,
            schema,
            cte_context,
            in_correlation,
            cfe_defs,
        ),

        // Predicates
        ast_unresolved::DomainExpression::Predicate { expr, alias } => {
            predicates::resolve_predicate_expr(
                *expr,
                alias.map(|s| s.to_string()),
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )
        }

        // Subqueries (backward-compat path — creates fresh registry)
        ast_unresolved::DomainExpression::ScalarSubquery {
            identifier,
            subquery,
            alias,
        } => subqueries::resolve_scalar_subquery_with_schema(
            identifier,
            *subquery,
            alias.map(|s| s.to_string()),
            available,
            schema,
            cte_context,
            system,
        ),

        // Compound expressions
        ast_unresolved::DomainExpression::PipedExpression {
            value,
            transforms,
            alias,
        } => compound::resolve_piped_expr(
            *value,
            transforms,
            alias.map(|s| s.to_string()),
            available,
            cfe_defs,
        ),
        ast_unresolved::DomainExpression::Parenthesized { inner, alias } => {
            compound::resolve_parenthesized(
                *inner,
                alias.map(|s| s.to_string()),
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
            )
        }
        ast_unresolved::DomainExpression::Tuple { elements, alias } => {
            compound::resolve_tuple(elements, alias.map(|s| s.to_string()), available, cfe_defs)
        }
        // Pivot expressions: resolve children, pivot_values populated later in resolve_modulo
        ast_unresolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            pivot_values,
        } => {
            let resolved_value = resolve_domain_expr_with_schema_and_context(
                *value_column,
                available,
                schema,
                cte_context,
                in_correlation,
                cfe_defs,
                None,
            )?;
            let resolved_key = resolve_domain_expr_with_schema(*pivot_key, available, cfe_defs)?;
            Ok(ast_resolved::DomainExpression::PivotOf {
                value_column: Box::new(resolved_value),
                pivot_key: Box::new(resolved_key),
                pivot_values,
            })
        }
    }
}

/// Resolve a domain expression using the shared registry
///
/// This is the new resolution path that preserves all namespace, CTE, CFE, and grounding
/// context by passing the EntityRegistry directly. Scalar subqueries and EXISTS use
/// the same registry instead of creating fresh copies.
pub(in crate::pipeline::resolver) fn resolve_domain_expr_with_registry(
    expr: ast_unresolved::DomainExpression,
    available: &[ast_resolved::ColumnMetadata],
    registry: &mut EntityRegistry,
    in_correlation: bool,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        // Simple expressions — no registry needed
        ast_unresolved::DomainExpression::Lvar { .. }
        | ast_unresolved::DomainExpression::ColumnOrdinal(_)
        | ast_unresolved::DomainExpression::Literal { .. }
        | ast_unresolved::DomainExpression::ValuePlaceholder { .. }
        | ast_unresolved::DomainExpression::Substitution(_)
        | ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
            simple::resolve_simple_expr(expr, available, in_correlation)
        }

        // Projection-only expressions
        ast_unresolved::DomainExpression::Projection(_) => {
            simple::resolve_projection_only_expr(expr)
        }

        // Functions — pass registry for potential subquery arguments
        ast_unresolved::DomainExpression::Function(func) => {
            functions::resolve_function_expr_with_registry(
                func,
                available,
                registry,
                in_correlation,
            )
        }

        // Predicates — pass registry for EXISTS/IN
        ast_unresolved::DomainExpression::Predicate { expr, alias } => {
            predicates::resolve_predicate_expr_with_registry(
                *expr,
                alias.map(|s| s.to_string()),
                available,
                registry,
                in_correlation,
            )
        }

        // Scalar subquery — uses registry directly, no fresh creation
        ast_unresolved::DomainExpression::ScalarSubquery {
            identifier,
            subquery,
            alias,
        } => subqueries::resolve_scalar_subquery(
            identifier,
            *subquery,
            alias.map(|s| s.to_string()),
            available,
            registry,
        ),

        // Piped — extract cfe_defs from registry, use simple path
        ast_unresolved::DomainExpression::PipedExpression {
            value,
            transforms,
            alias,
        } => {
            let cfe_defs = Some(&registry.query_local.cfes);
            compound::resolve_piped_expr(
                *value,
                transforms,
                alias.map(|s| s.to_string()),
                available,
                cfe_defs,
            )
        }

        // Parenthesized — pass registry (inner might be subquery)
        ast_unresolved::DomainExpression::Parenthesized { inner, alias } => {
            compound::resolve_parenthesized_with_registry(
                *inner,
                alias.map(|s| s.to_string()),
                available,
                registry,
                in_correlation,
            )
        }

        // Tuple — extract cfe_defs from registry, use simple path
        ast_unresolved::DomainExpression::Tuple { elements, alias } => {
            let cfe_defs = Some(&registry.query_local.cfes);
            compound::resolve_tuple(elements, alias.map(|s| s.to_string()), available, cfe_defs)
        }

        // PivotOf — recursive
        ast_unresolved::DomainExpression::PivotOf {
            value_column,
            pivot_key,
            pivot_values,
        } => {
            let resolved_value = resolve_domain_expr_with_registry(
                *value_column,
                available,
                registry,
                in_correlation,
            )?;
            let cfe_defs = Some(&registry.query_local.cfes);
            let resolved_key = resolve_domain_expr_with_schema(*pivot_key, available, cfe_defs)?;
            Ok(ast_resolved::DomainExpression::PivotOf {
                value_column: Box::new(resolved_value),
                pivot_key: Box::new(resolved_key),
                pivot_values,
            })
        }
    }
}

/// Resolve a list of domain expressions with available schema
/// This handles expansion of globs, patterns, ranges, and ordinals
/// Resolve expressions with schema, optionally allowing patterns to match zero columns
pub(in crate::pipeline::resolver) fn resolve_expressions_with_schema(
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
    schema: Option<&dyn super::super::DatabaseSchema>,
    cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    projection::resolve_expressions_with_schema_internal(
        expressions,
        available,
        false,
        cfe_defs,
        schema,
        cte_context,
        in_correlation,
    )
}

/// Internal version that allows control over zero-match behavior for patterns
/// Used by operators that need to allow empty pattern matches
pub(in crate::pipeline::resolver) fn resolve_expressions_with_schema_internal(
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
    allow_zero_pattern_matches: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
    schema: Option<&dyn super::super::DatabaseSchema>,
    cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    projection::resolve_expressions_with_schema_internal(
        expressions,
        available,
        allow_zero_pattern_matches,
        cfe_defs,
        schema,
        cte_context,
        in_correlation,
    )
}
