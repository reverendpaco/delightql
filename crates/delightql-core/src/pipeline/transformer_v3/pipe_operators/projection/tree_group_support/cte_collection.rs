// CTE requirement collection from tree groups

use super::TreeGroupCteInfo;
use crate::pipeline::ast_addressed;

/// Collect CTE requirements from tree groups in reducing_by and reducing_on
///
/// This function reads the `cte_requirements` field populated by the resolver
/// and extracts the information needed for CTE generation.
///
/// Phase R4: Function created but not yet called
/// Phase R5+: Will be called when feature flag is enabled
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn collect_cte_requirements(
    reducing_by: &[ast_addressed::DomainExpression],
    reducing_on: &[ast_addressed::DomainExpression],
) -> Vec<TreeGroupCteInfo> {
    let mut result = Vec::new();

    // Collect from reducing_by (scalar context)
    for (idx, expr) in reducing_by.iter().enumerate() {
        match expr {
            ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::Curly {
                    cte_requirements: Some(req),
                    ..
                },
            ) => {
                result.push(TreeGroupCteInfo {
                    index: idx,
                    location: req.location,
                    requirements: req.clone(),
                });
            }
            ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::MetadataTreeGroup {
                    keys_only: _keys_only,
                    cte_requirements: Some(req),
                    ..
                },
            ) => {
                result.push(TreeGroupCteInfo {
                    index: idx,
                    location: req.location,
                    requirements: req.clone(),
                });
                // Note: Nested MetadataTreeGroups are handled recursively by generate_nested_reduction_cte
            }
            // Non-tree-group expressions (Lvar, Literal, etc.) or tree groups without
            // cte_requirements: no CTEs to collect, skip
            _ => {}
        }
    }

    // Collect from reducing_on (aggregate context)
    for (idx, expr) in reducing_on.iter().enumerate() {
        match expr {
            ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::Curly {
                    cte_requirements: Some(req),
                    ..
                },
            ) => {
                result.push(TreeGroupCteInfo {
                    index: idx,
                    location: req.location,
                    requirements: req.clone(),
                });
            }
            ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::MetadataTreeGroup {
                    keys_only: _keys_only,
                    cte_requirements: Some(req),
                    ..
                },
            ) => {
                result.push(TreeGroupCteInfo {
                    index: idx,
                    location: req.location,
                    requirements: req.clone(),
                });
                // Note: Nested MetadataTreeGroups are handled recursively by generate_nested_reduction_cte
            }
            // Non-tree-group aggregate expressions: no CTEs to collect, skip
            _ => {}
        }
    }

    result
}
