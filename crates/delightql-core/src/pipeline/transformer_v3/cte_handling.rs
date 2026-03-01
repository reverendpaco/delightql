// CTE (Common Table Expression) handling utilities for transformer_v3
//
// This module provides utilities for collecting and managing CFE (Compiled Function Expression)
// definitions from query trees. CFEs are special precompiled expressions that can be
// referenced within queries.

use crate::pipeline::ast_addressed;

/// Collect all CFE definitions from a query tree
///
/// This function recursively walks through a query tree and collects all
/// PrecompiledCfeDefinition nodes. It handles:
/// - WithPrecompiledCfes: Queries that contain CFE definitions
/// - ReplTempTable/ReplTempView: REPL commands that may wrap queries with CFEs
/// - Other query types: Passed through without collecting CFEs
///
/// # Arguments
/// * `query` - The query tree to search for CFE definitions
///
/// # Returns
/// A vector of all CFE definitions found in the query tree
pub(crate) fn collect_cfes(
    query: &ast_addressed::Query,
) -> Vec<ast_addressed::PrecompiledCfeDefinition> {
    let mut cfes = Vec::new();
    match query {
        ast_addressed::Query::WithPrecompiledCfes {
            cfes: query_cfes,
            query: inner,
        } => {
            cfes.extend(query_cfes.clone());
            cfes.extend(collect_cfes(inner));
        }
        ast_addressed::Query::WithCtes { .. } => {
            // No CFEs at this level
        }
        ast_addressed::Query::ReplTempTable { query: inner, .. } => {
            cfes.extend(collect_cfes(inner));
        }
        ast_addressed::Query::ReplTempView { query: inner, .. } => {
            cfes.extend(collect_cfes(inner));
        }
        ast_addressed::Query::Relational(_) | ast_addressed::Query::WithCfes { .. } => {
            // No CFEs or they need precompilation
        }
        ast_addressed::Query::WithErContext { .. } => {
            unreachable!("ER-context consumed by resolver")
        }
    }
    cfes
}
