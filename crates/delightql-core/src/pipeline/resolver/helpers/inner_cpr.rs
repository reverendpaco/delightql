// inner_cpr.rs - Shared helpers for Inner-CPR (Inner Column Preservation Rules) features
//
// Inner-CPR includes:
// - ScalarSubquery: orders:(~> count:(*))
// - InnerExists: +orders(, o.user_id = u.id)
// - Derived tables/Lateral joins: users(|> σ(age > 21))
//
// This module provides shared functionality to avoid code duplication across these features.

use crate::error::Result;
use crate::pipeline::asts::{resolved as ast_resolved, unresolved as ast_unresolved};
use crate::pipeline::resolver::unification::ColumnReference;
use crate::pipeline::resolver::{DatabaseSchema, ResolutionConfig};
use crate::resolution::EntityRegistry;
use std::collections::HashMap;

/// Information returned from bubbling an inner-CPR subquery
pub struct InnerCprBubbleResult {
    /// The dependencies that the subquery needs (i_need)
    pub dependencies: Vec<ColumnReference>,
    /// Updated CTE context after resolving the subquery
    pub updated_cte_context: HashMap<String, ast_resolved::CprSchema>,
}

/// Resolve an inner-CPR subquery during the bubbling phase
///
/// This is the "double resolution hack" - we resolve the subquery during bubbling
/// just to extract its dependencies (i_need), then throw away the resolved result.
/// The same subquery will be resolved again during the actual resolving phase.
///
/// # Why This Exists
///
/// The type system separates bubbling (returns Unresolved) from resolving (returns Resolved).
/// But inner-CPR contains full relational expressions that need resolution to determine
/// their dependencies. This hack bridges that gap.
///
/// # Parameters
///
/// - `subquery`: The unresolved subquery to analyze
/// - `schema`: Database schema for table lookups
/// - `system`: Optional system reference for consulted namespace resolution
/// - `cte_context`: Current CTE definitions (will be updated with any changes)
/// - `outer_context`: Optional outer query columns for correlation support
///
/// # Returns
///
/// Returns the dependencies and updated CTE context. The resolved subquery itself
/// is discarded by the caller.
pub(in crate::pipeline::resolver) fn resolve_inner_cpr_during_bubbling(
    subquery: ast_unresolved::RelationalExpression,
    schema: &dyn DatabaseSchema,
    system: Option<&crate::system::DelightQLSystem>,
    cte_context: &HashMap<String, ast_resolved::CprSchema>,
    outer_context: Option<&[ast_resolved::ColumnMetadata]>,
) -> Result<InnerCprBubbleResult> {
    // Create temporary registry for resolution, preserving system reference
    // so consulted namespaces (std::math, etc.) remain visible.
    let mut temp_registry = match system {
        Some(sys) => EntityRegistry::new_with_system(schema, sys),
        None => EntityRegistry::new(schema),
    };

    // Copy CTE context to temporary registry
    temp_registry.query_local.ctes = cte_context.clone();

    // Resolve the subquery to extract dependencies
    // NOTE: We throw away the resolved result (_resolved_subquery)
    let (_resolved_subquery, subquery_bubbled) =
        super::super::resolve_relational_expression_with_registry(
            subquery,
            &mut temp_registry,
            outer_context,
            &ResolutionConfig::default(),
            None,
        )?;

    // Return dependencies and updated CTE context
    Ok(InnerCprBubbleResult {
        dependencies: subquery_bubbled.i_need,
        updated_cte_context: temp_registry.query_local.ctes,
    })
}
