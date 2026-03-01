// Transformer V3 - Pure Functional Design with Explicit Context
//
// Core Philosophy:
// 1. Explicit TransformContext for correlation tracking
// 2. Each function does recursive induction with local logic
// 3. Natural nesting through recursion (no CTE accumulation)
// 4. CPR Laws encoded in nesting patterns
// 5. CTE extraction is a separate tree-rewriting pass
//
// Key Insight: Each operation naturally wraps its source in a subquery,
// creating the deep nesting that encodes CPR semantics.

// Module declarations
mod context;
mod cpr_laws;
mod cte_extractor;
mod cte_handling;
mod destructuring_recursive;
mod expression_transformer;
mod filter_transformer;
mod finalization;
mod full_outer_expansion;
mod helpers;
mod join_handler;
mod orchestrator;
mod pipe_operators;
mod predicate_utils;
mod relation_transformer;
mod schema_context;
mod schema_utils;
mod segment_handler;
mod select_item_utils;
mod set_operations;
mod tree_group_ctes;
mod types;

// Qualifier scope — unified qualification policy for data source scoping
mod qualifier_scope;
pub(in crate::pipeline) use qualifier_scope::QualifierMint;
pub(in crate::pipeline::transformer_v3) use qualifier_scope::QualifierScope;

// Query wrapper - enforces provenance updates when wrapping queries
mod query_wrapper;

// CFE substitution submodule
mod cfe_substitution;

// Public API re-exports - these are the entry points used by the rest of the codebase
pub use orchestrator::{transform, transform_query_with_options};

// Internal re-exports for use within transformer_v3 submodules
pub(crate) use context::TransformContext;
pub(crate) use expression_transformer::transform_domain_expression;
pub(crate) use helpers::alias_generator::next_alias;
pub(crate) use helpers::convert_join_type;
pub(crate) use orchestrator::{transform_pipe, transform_relational};
pub(crate) use schema_context::SchemaContext;
pub(crate) use segment_handler::{finalize_to_query, JoinSpec, SegmentSource};
pub(crate) use select_item_utils::domain_to_select_item_with_name_and_flag;
pub(crate) use types::QueryBuildState;
