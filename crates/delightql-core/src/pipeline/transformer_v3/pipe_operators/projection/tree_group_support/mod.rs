// Tree group support for projection operators
// Handles CTE generation, expression rewriting, and join building for tree groups

pub(super) mod cte_collection;
pub(super) mod cte_generation;
pub(super) mod expression_rewriting;
pub(super) mod join_building;
pub(super) mod qualification;

// Re-export commonly used items for internal projection module use
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) use cte_collection::collect_cte_requirements;
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) use cte_generation::{
    generate_all_independent_ctes, generate_wrapper_ctes_for_aggregates,
};
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) use expression_rewriting::modify_expressions_for_ctes;
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) use join_building::build_from_with_joins;
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) use qualification::{
    qualify_base_table_references, qualify_expression_with_table, qualify_sql_expression,
};

// Shared types used across tree_group_support modules
use crate::pipeline::ast_addressed;

/// Information about a tree group's CTE requirements extracted from AST
#[derive(Debug, Clone)]
pub(super) struct TreeGroupCteInfo {
    /// Index in reducing_by or reducing_on
    pub index: usize,
    /// Location (reducing_by or reducing_on)
    pub location: ast_addressed::TreeGroupLocation,
    /// CTE requirements from resolver analysis
    pub requirements: ast_addressed::CteRequirements,
}

/// Result of CTE generation for all tree groups
pub(super) struct CteGenerationResult {
    /// All generated CTEs (in order they should appear in WITH clause)
    pub ctes: Vec<crate::pipeline::sql_ast_v3::Cte>,
    /// Information for JOINing each CTE to base table
    pub cte_joins: Vec<CteJoinInfo>,
}

/// Information needed to JOIN a CTE to the base table
#[derive(Debug, Clone)]
pub(super) struct CteJoinInfo {
    /// Name of the CTE to join
    pub cte_name: String,
    /// Keys to join on (from cte_requirements.join_keys)
    pub join_keys: Vec<ast_addressed::DomainExpression>,
    /// All grouping keys used by the CTE (for detecting promoted columns)
    /// When this is larger than join_keys, the CTE has promoted columns
    #[allow(dead_code)]
    pub accumulated_grouping_keys: Vec<ast_addressed::DomainExpression>,
    /// Index in original reducing_on (for mapping expressions to CTEs)
    pub original_index: usize,
    /// Column aliases in the CTE (for expression modification)
    pub column_aliases: Vec<String>,
    /// Location of the tree group
    pub location: ast_addressed::TreeGroupLocation,
    /// GROUPING DRESS keys: maps column index to original key name
    /// For scalar nested objects like {"key-foo": {country}} that became CTE columns
    pub grouping_dress_keys: Vec<(usize, String)>,
}
