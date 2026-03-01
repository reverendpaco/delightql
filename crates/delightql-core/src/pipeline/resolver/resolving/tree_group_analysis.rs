//! Tree group CTE requirements analysis (Phase R2+)
//!
//! This module analyzes tree groups in modulo reductions to determine which ones
//! need CTEs and populates the `cte_requirements` metadata in the AST.
//!
//! The transformer (Phase R4+) will read this metadata to generate independent CTEs
//! for each tree group with nested reductions.

use crate::error::Result;
use crate::pipeline::asts::core::phase_box::PhaseBox;
use crate::pipeline::asts::resolved::{
    self as ast, CteRequirements, FunctionExpression, NestedMemberCteInfo, TreeGroupLocation,
};

/// Check if a domain expression is a tree group with nested reductions
fn has_nested_reductions(expr: &ast::DomainExpression) -> bool {
    match expr {
        ast::DomainExpression::Function(FunctionExpression::Curly { members, .. }) => {
            members.iter().any(|m| {
                matches!(
                    m,
                    ast::CurlyMember::KeyValue {
                        nested_reduction: true,
                        ..
                    }
                )
            })
        }
        ast::DomainExpression::Function(FunctionExpression::MetadataTreeGroup { .. }) => true,
        // Non-tree-group function expressions: Regular, Curried, HigherOrder, Bracket,
        // Infix, Lambda, StringTemplate, CaseExpression, Window, Array, JsonPath.
        // None of these are tree groups — no nested reductions.
        ast::DomainExpression::Function(FunctionExpression::Regular { .. })
        | ast::DomainExpression::Function(FunctionExpression::Curried { .. })
        | ast::DomainExpression::Function(FunctionExpression::HigherOrder { .. })
        | ast::DomainExpression::Function(FunctionExpression::Bracket { .. })
        | ast::DomainExpression::Function(FunctionExpression::Infix { .. })
        | ast::DomainExpression::Function(FunctionExpression::Lambda { .. })
        | ast::DomainExpression::Function(FunctionExpression::StringTemplate { .. })
        | ast::DomainExpression::Function(FunctionExpression::CaseExpression { .. })
        | ast::DomainExpression::Function(FunctionExpression::Window { .. })
        | ast::DomainExpression::Function(FunctionExpression::Array { .. })
        | ast::DomainExpression::Function(FunctionExpression::JsonPath { .. }) => false,
        // Non-function domain expressions: columns, literals, placeholders, etc.
        // None of these are tree groups.
        ast::DomainExpression::Lvar { .. }
        | ast::DomainExpression::Literal { .. }
        | ast::DomainExpression::Projection(_)
        | ast::DomainExpression::NonUnifiyingUnderscore
        | ast::DomainExpression::ValuePlaceholder { .. }
        | ast::DomainExpression::Substitution(_)
        | ast::DomainExpression::Predicate { .. }
        | ast::DomainExpression::PipedExpression { .. }
        | ast::DomainExpression::Parenthesized { .. }
        | ast::DomainExpression::Tuple { .. }
        | ast::DomainExpression::ColumnOrdinal(_)
        | ast::DomainExpression::ScalarSubquery { .. }
        | ast::DomainExpression::PivotOf { .. } => false,
    }
}

/// Extract inner grouping keys WITH KEY NAMES from a tree group
/// Returns (key_name, expression) where key_name is Some for GROUPING DRESS, None for simple fields
fn extract_inner_grouping_keys_with_names(
    expr: &ast::DomainExpression,
) -> Vec<(Option<String>, ast::DomainExpression)> {
    match expr {
        ast::DomainExpression::Function(FunctionExpression::Curly { members, .. }) => {
            // Extract non-nested members with their key names
            members
                .iter()
                .filter_map(|m| match m {
                    ast::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    } => {
                        // Simple scalar field - no key name
                        Some((
                            None,
                            ast::DomainExpression::Lvar {
                                name: column.clone(),
                                qualifier: qualifier.clone(),
                                namespace_path: schema
                                    .as_ref()
                                    .map(|s| ast::NamespacePath::single(s.clone()))
                                    .unwrap_or_else(|| ast::NamespacePath::empty()),
                                alias: None,
                                provenance: ast::PhaseBox::phantom(),
                            },
                        ))
                    }
                    ast::CurlyMember::KeyValue {
                        key,
                        nested_reduction: false,
                        value,
                    } => {
                        // Check if this is GROUPING DRESS (nested object) or simple renamed field
                        if matches!(
                            value.as_ref(),
                            ast::DomainExpression::Function(FunctionExpression::Curly { .. })
                        ) {
                            // GROUPING DRESS: nested object like {"key": {country, age}}
                            // Include key name for reconstruction
                            Some((Some(key.clone()), *value.clone()))
                        } else {
                            // Simple renamed field like {"key": column_name}
                            // No key name (not GROUPING DRESS)
                            Some((None, *value.clone()))
                        }
                    }
                    // KeyValue with nested_reduction (explosion ~>): NOT a grouping key
                    // These are aggregation targets, not dimensions
                    ast::CurlyMember::KeyValue {
                        nested_reduction: true,
                        ..
                    } => None,
                    // Comparison, Glob, Pattern, OrdinalRange, Placeholder, PathLiteral:
                    // not grouping keys in tree group context — skip
                    ast::CurlyMember::Comparison { .. }
                    | ast::CurlyMember::Glob { .. }
                    | ast::CurlyMember::Pattern { .. }
                    | ast::CurlyMember::OrdinalRange { .. }
                    | ast::CurlyMember::Placeholder { .. }
                    | ast::CurlyMember::PathLiteral { .. } => None,
                })
                .collect()
        }
        ast::DomainExpression::Function(FunctionExpression::MetadataTreeGroup {
            key_column,
            key_qualifier,
            key_schema,
            ..
        }) => {
            // For metadata tree groups, the key column has no key name (not GROUPING DRESS)
            vec![(
                None,
                ast::DomainExpression::Lvar {
                    name: key_column.clone(),
                    qualifier: key_qualifier.clone(),
                    namespace_path: key_schema
                        .as_ref()
                        .map(|s| ast::NamespacePath::single(s.clone()))
                        .unwrap_or_else(|| ast::NamespacePath::empty()),
                    alias: None,
                    provenance: ast::PhaseBox::phantom(),
                },
            )]
        }
        other => panic!("catch-all hit in tree_group_analysis.rs extract_inner_grouping_keys_with_names (DomainExpression): {:?}", other),
    }
}

/// Information about a tree group that needs CTE analysis
struct TreeGroupToAnalyze<'a> {
    /// Mutable reference to the expression (so we can update cte_requirements)
    expr: &'a mut ast::DomainExpression,
    /// Location in the query (reducing_by or reducing_on)
    location: TreeGroupLocation,
    /// Index in the original slice (for debugging)
    #[allow(dead_code)]
    index: usize,
}

/// Collect all tree groups that need CTE analysis
///
/// Searches through reducing_by and reducing_on for tree groups with nested reductions.
/// Returns mutable references so we can populate their cte_requirements field.
fn collect_tree_groups_needing_ctes<'a>(
    reducing_by: &'a mut [ast::DomainExpression],
    reducing_on: &'a mut [ast::DomainExpression],
) -> Vec<TreeGroupToAnalyze<'a>> {
    let mut result = Vec::new();

    // Collect from reducing_by (scalar context)
    for (idx, expr) in reducing_by.iter_mut().enumerate() {
        if has_nested_reductions(expr) {
            result.push(TreeGroupToAnalyze {
                expr,
                location: TreeGroupLocation::InReducingBy,
                index: idx,
            });
        }
    }

    // Collect from reducing_on (aggregate context)
    for (idx, expr) in reducing_on.iter_mut().enumerate() {
        if has_nested_reductions(expr) {
            result.push(TreeGroupToAnalyze {
                expr,
                location: TreeGroupLocation::InReducingOn,
                index: idx,
            });
        }
    }

    result
}

/// Recursively populate cte_requirements for nested MetadataTreeGroups
///
/// For chained metadata tree groups like `country:~> status:~> name:~>`:
/// - Each level needs its own cte_requirements
/// - Each level's accumulated_keys = parent's accumulated_keys + this level's key
///
/// This function walks the constructor chain and populates each MetadataTreeGroup.
fn populate_nested_metadata_cte_requirements(
    expr: &mut ast::DomainExpression,
    location: TreeGroupLocation,
    accumulated_keys: Vec<(Option<String>, ast::DomainExpression)>,
) -> Result<()> {
    if let ast::DomainExpression::Function(FunctionExpression::MetadataTreeGroup {
        key_column,
        key_qualifier,
        key_schema,
        constructor,
        keys_only: _,
        cte_requirements,
        ..
    }) = expr
    {
        // This level's key
        let this_key = ast::DomainExpression::Lvar {
            name: key_column.clone(),
            qualifier: key_qualifier.clone(),
            namespace_path: key_schema
                .as_ref()
                .map(|s| ast::NamespacePath::single(s.clone()))
                .unwrap_or_else(|| ast::NamespacePath::empty()),
            alias: None,
            provenance: ast::PhaseBox::phantom(),
        };

        // Accumulated keys for this level = parent's keys + this key (no key name for metadata TG)
        let mut my_accumulated_keys = accumulated_keys.clone();
        my_accumulated_keys.push((None, this_key));

        log::debug!(
            "MetadataTreeGroup key={}, parent_keys={}, my_keys={}",
            key_column,
            accumulated_keys.len(),
            my_accumulated_keys.len()
        );

        // Populate this level's cte_requirements
        // For metadata tree groups, nested_members_info contains info about the constructor
        let nested_members_info = vec![NestedMemberCteInfo {
            key: "constructor".to_string(),
            cte_column_name: "constructor".to_string(),
        }];

        *cte_requirements = Some(CteRequirements {
            needs_cte: true,
            accumulated_grouping_keys: my_accumulated_keys.clone(),
            join_keys: accumulated_keys.iter().map(|(_, e)| e.clone()).collect(), // JOIN on parent's keys (just expressions)
            location,
            nested_members_info,
            cte_name: PhaseBox::phantom(),
        });

        // Recursively process the constructor if it's another MetadataTreeGroup
        if let FunctionExpression::MetadataTreeGroup { .. } = constructor.as_ref() {
            let mut constructor_as_domain = ast::DomainExpression::Function(*constructor.clone());
            populate_nested_metadata_cte_requirements(
                &mut constructor_as_domain,
                location,
                my_accumulated_keys,
            )?;
            // Extract the mutated MetadataTreeGroup back
            if let ast::DomainExpression::Function(func) = constructor_as_domain {
                *constructor = Box::new(func);
            }
        }
    }

    Ok(())
}

/// Compute CTE requirements for a single tree group
///
/// Given:
/// - The tree group expression
/// - Location (reducing_by or reducing_on)
/// - Outer grouping keys (from the modulo reducing_by)
///
/// Returns CteRequirements with:
/// - accumulated_grouping_keys = outer + inner
/// - join_keys = outer
/// - location
/// - nested_members_info (placeholder for now, Phase R4+ will use this)
fn compute_cte_requirements(
    expr: &mut ast::DomainExpression,
    location: TreeGroupLocation,
    outer_grouping_keys: &[(Option<String>, ast::DomainExpression)],
) -> Result<CteRequirements> {
    // For MetadataTreeGroups, recursively populate nested ones first
    if matches!(
        expr,
        ast::DomainExpression::Function(FunctionExpression::MetadataTreeGroup { .. })
    ) {
        populate_nested_metadata_cte_requirements(expr, location, outer_grouping_keys.to_vec())?;
    }

    // Extract inner grouping keys WITH KEY NAMES from this tree group
    let inner_keys_with_names = extract_inner_grouping_keys_with_names(expr);
    log::debug!(
        "Tree group inner_keys: {:?}, location: {:?}",
        inner_keys_with_names.len(),
        location
    );

    // Extract just the expressions for join_keys (no key names needed for joins)
    let inner_keys_exprs: Vec<_> = inner_keys_with_names
        .iter()
        .map(|(_, e)| e.clone())
        .collect();

    // Logic differs based on location (scalar vs aggregate context)
    let (accumulated_grouping_keys, join_keys) = match location {
        TreeGroupLocation::InReducingBy => {
            // Scalar context: CTE groups by inner keys only, joins on inner keys
            (inner_keys_with_names.clone(), inner_keys_exprs.clone())
        }
        TreeGroupLocation::InReducingOn => {
            // Aggregate context: CTE groups by outer + inner, joins on outer keys (just expressions)
            let mut accumulated = outer_grouping_keys.to_vec();
            accumulated.extend(inner_keys_with_names);
            let join_exprs: Vec<_> = outer_grouping_keys.iter().map(|(_, e)| e.clone()).collect();
            (accumulated, join_exprs)
        }
    };

    // Extract nested member info (keys that will become CTE columns)
    let nested_members_info = extract_nested_member_info(expr);

    Ok(CteRequirements {
        needs_cte: true,
        accumulated_grouping_keys,
        join_keys,
        location,
        nested_members_info,
        cte_name: PhaseBox::phantom(),
    })
}

/// Extract nested member information from a tree group
///
/// For each nested reduction ("key": ~> {...}), we need to track:
/// - The key name
/// - The CTE column name that will hold the aggregated result
///
/// Phase R4+ will use this to generate CTE columns and modify the tree group
/// to reference them with JSON(cte_column).
fn extract_nested_member_info(expr: &ast::DomainExpression) -> Vec<NestedMemberCteInfo> {
    match expr {
        ast::DomainExpression::Function(FunctionExpression::Curly { members, .. }) => {
            members
                .iter()
                .filter_map(|m| match m {
                    ast::CurlyMember::KeyValue {
                        key,
                        nested_reduction: true,
                        ..
                    } => Some(NestedMemberCteInfo {
                        key: key.clone(),
                        // CTE column name will be same as key (Phase R4+ may need to make unique)
                        cte_column_name: key.clone(),
                    }),
                    // Non-nested-reduction members: Shorthand (plain column), KeyValue(reduction=false),
                    // Comparison, Glob, Pattern, OrdinalRange, Placeholder, PathLiteral
                    // These don't generate CTEs — filter them out
                    ast::CurlyMember::Shorthand { .. }
                    | ast::CurlyMember::KeyValue {
                        nested_reduction: false,
                        ..
                    }
                    | ast::CurlyMember::Comparison { .. }
                    | ast::CurlyMember::Glob { .. }
                    | ast::CurlyMember::Pattern { .. }
                    | ast::CurlyMember::OrdinalRange { .. }
                    | ast::CurlyMember::Placeholder { .. }
                    | ast::CurlyMember::PathLiteral { .. } => None,
                })
                .collect()
        }
        // MetadataTreeGroup: future phase
        ast::DomainExpression::Function(FunctionExpression::MetadataTreeGroup { .. }) => vec![],
        // All other DomainExpressions: not tree groups, no nested members
        _ => vec![],
    }
}

/// Main entry point: Analyze all tree groups and populate cte_requirements
///
/// This function is called by the resolver after basic resolution is complete.
/// It finds all tree groups with nested reductions in the modulo specification
/// and populates their cte_requirements field with the metadata needed for
/// independent CTE generation.
///
/// Parameters:
/// - reducing_by: The grouping keys (may contain tree groups in scalar context)
/// - reducing_on: The aggregate expressions (may contain tree groups in aggregate context)
///
/// Side effects:
/// - Mutates tree groups in-place to set their cte_requirements field
pub fn analyze_tree_groups_for_ctes(
    reducing_by: &mut [ast::DomainExpression],
    reducing_on: &mut [ast::DomainExpression],
) -> Result<()> {
    // Build outer grouping keys WITH KEY NAMES, expanding tree groups to their inner keys
    // This ensures we GROUP BY the actual identifiers, not the JSON construction
    let mut outer_grouping_keys: Vec<(Option<String>, ast::DomainExpression)> = Vec::new();
    for expr in reducing_by.iter() {
        if has_nested_reductions(expr) {
            // For tree groups, use their inner grouping keys with key names
            outer_grouping_keys.extend(extract_inner_grouping_keys_with_names(expr));
        } else {
            // For non-tree groups, use as-is (no key name)
            outer_grouping_keys.push((None, expr.clone()));
        }
    }

    // Phase 1: Collect all tree groups needing CTE analysis
    let tree_groups = collect_tree_groups_needing_ctes(reducing_by, reducing_on);

    if tree_groups.is_empty() {
        // No tree groups with nested reductions - nothing to do
        return Ok(());
    }

    // Phase 2: Compute CTE requirements for each tree group
    for tree_group in tree_groups {
        // Compute requirements based on location and outer grouping keys
        let cte_req =
            compute_cte_requirements(tree_group.expr, tree_group.location, &outer_grouping_keys)?;

        // Phase 3: Annotate the AST node with requirements
        match tree_group.expr {
            ast::DomainExpression::Function(FunctionExpression::Curly {
                cte_requirements, ..
            }) => {
                // Populate the cte_requirements field
                *cte_requirements = Some(cte_req);
            }
            ast::DomainExpression::Function(FunctionExpression::MetadataTreeGroup {
                keys_only: _,
                cte_requirements,
                ..
            }) => {
                // Populate the cte_requirements field for metadata tree groups
                *cte_requirements = Some(cte_req);
            }
            other => {
                panic!(
                    "catch-all hit in tree_group_analysis.rs populate_cte_requirements: {:?}",
                    other
                );
            }
        }
    }

    Ok(())
}
