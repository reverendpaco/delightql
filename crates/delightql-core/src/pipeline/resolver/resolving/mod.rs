// Modularized resolver components
// These handle the actual resolution of AST nodes from unresolved to resolved state

pub(super) mod column_extraction;
pub(super) mod domain_expressions;
pub(super) mod functions;
pub(super) mod helpers;
mod operators;
pub(super) mod predicates;
pub(super) mod tree_group_analysis;

// Re-export the public interface functions for use by the resolver
pub(in crate::pipeline::resolver) use predicates::resolve_sigma_condition_with_registry;
pub(in crate::pipeline::resolver) use predicates::synthesize_using_correlation;
pub(in crate::pipeline::resolver) use predicates::build_using_correlation_filters;

pub(in crate::pipeline::resolver) use operators::resolve_operator_with_registry;

// Re-export for CFE precompilation (crate-wide access)
pub(crate) use domain_expressions::resolve_domain_expr_with_full_context_and_system;
