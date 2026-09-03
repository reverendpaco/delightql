// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Tree group CTE requirements analysis (Phase R2+)
//!
//! This module analyzes tree groups in group reductions to determine which ones
//! need CTEs and populates the `cte_requirements` metadata in the AST.
//!
//! The transformer (Phase R4+) will read this metadata to generate independent CTEs
//! for each tree group with nested reductions.

use crate::error::Result;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::{Enclyph, MetadataTarget};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved::{
    self as ast, CteRequirements, FunctionApplication, NestedMemberCteInfo, ReductionPlan,
    TreeGroupLocation, TreeGroupPlan,
};

/// WHAT A TREE GROUP IS, borrowed for annotation.
///
/// A record with an induced member and a metadata level both re-enter
/// reduction under the parent's group, so both owe a CTE. Nothing else does,
/// and the position says which one it is holding rather than a reader asking
/// a value what it happens to be.
enum TreeGroup<'a> {
    Record(&'a mut ast::Record),
    Metadata(&'a mut ast::MetadataGroup),
}

/// Does this published value build a tree group with nested reductions?
fn has_nested_reductions(expr: &ast::DomainExpression) -> bool {
    match expr {
        ast::DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(
            record,
        ))) => record.members.iter().any(|member| {
            matches!(
                member,
                ast::RecordMember::Induced { .. } | ast::RecordMember::Metadata { .. }
            )
        }),
        _ => false,
    }
}

/// The tree group a reduction item carries, when it carries one.
fn tree_group_of(item: &mut ast::ReductionItem) -> Option<TreeGroup<'_>> {
    match item {
        // A metadata level always reduces.
        ast::ReductionItem::Metadata(metadata) => Some(TreeGroup::Metadata(&mut metadata.group)),
        // A pivot rotates values into columns; a delegate selects a
        // representative row: neither builds a tree.
        ast::ReductionItem::Pivot(_) | ast::ReductionItem::Delegate(_) => None,
        ast::ReductionItem::Out(item) => match item.value_mut()? {
            ast::DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(
                record,
            ))) if record.members.iter().any(|member| {
                matches!(
                    member,
                    ast::RecordMember::Induced { .. } | ast::RecordMember::Metadata { .. }
                )
            }) =>
            {
                Some(TreeGroup::Record(record))
            }
            _ => None,
        },
    }
}

/// The inner grouping keys a tree group contributes, with the key name a
/// GROUPING DRESS reconstructs under (`None` for a plain field).
fn extract_inner_grouping_keys_with_names(
    expr: &ast::DomainExpression,
) -> Vec<(Option<String>, ast::DomainExpression)> {
    match expr {
        ast::DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(record))) => {
            record
                .members
                .iter()
                .filter_map(|member| match member {
                    ast::RecordMember::SelfKeyed(NamedReference(occurrence)) => Some((
                        None,
                        ast::DomainExpression::Reference(Reference::Named(NamedReference(
                            ColumnOccurrence::engine(occurrence.column),
                        ))),
                    )),
                    ast::RecordMember::Keyed { key, value } => {
                        // GROUPING DRESS: a nested record standing as a key
                        // is reconstructed under its own name; a plain renamed
                        // field is just the value.
                        if matches!(
                            value.as_ref(),
                            ast::DomainExpression::Application(FunctionApplication::Enclyph(
                                Enclyph::Record(_)
                            ))
                        ) {
                            Some((Some(key.clone()), *value.clone()))
                        } else {
                            Some((None, *value.clone()))
                        }
                    }
                    // An induced member is an aggregation target, not a
                    // dimension; so is a metadata group.
                    ast::RecordMember::Induced { .. } | ast::RecordMember::Metadata { .. } => None,
                    ast::RecordMember::Spread(spread) => spread.expanded(),
                })
                .collect()
        }
        other => panic!("catch-all hit in tree_group_analysis.rs extract_inner_grouping_keys_with_names (DomainExpression): {:?}", other),
    }
}

/// A metadata level's key column has no key name (not GROUPING DRESS).
fn metadata_key(group: &ast::MetadataGroup) -> (Option<String>, ast::DomainExpression) {
    (
        None,
        ast::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence::engine(group.key.column),
        ))),
    )
}

/// A tree group awaiting its CTE requirements, and where it stands.
struct TreeGroupToAnalyze<'a> {
    group: TreeGroup<'a>,
    /// Location in the query (keys or reductions)
    location: TreeGroupLocation,
    item_index: usize,
}

/// Collect every tree group that needs CTE analysis.
///
/// Returns mutable borrows so the requirements can be written back where
/// they belong: on the record, or on the metadata level.
fn collect_tree_groups_needing_ctes<'a>(
    keys: &'a mut [ast::OutItem],
    reductions: &'a mut [ast::ReductionItem],
) -> Vec<TreeGroupToAnalyze<'a>> {
    let mut result = Vec::new();

    // From keys (scalar context). A key is a publication item; the CTE
    // analysis reaches the value it publishes and leaves the item's naming and
    // output stamp alone. A spread publishes no value to analyze, and a
    // metadata level has no derivation as a group key.
    for (item_index, item) in keys.iter_mut().enumerate() {
        let Some(expr) = item.value_mut() else {
            continue;
        };
        let ast::DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(
            record,
        ))) = expr
        else {
            continue;
        };
        if record.members.iter().any(|member| {
            matches!(
                member,
                ast::RecordMember::Induced { .. } | ast::RecordMember::Metadata { .. }
            )
        }) {
            result.push(TreeGroupToAnalyze {
                group: TreeGroup::Record(record),
                location: TreeGroupLocation::InKeys,
                item_index,
            });
        }
    }

    // From reductions (aggregate context), where a metadata level also stands.
    for (item_index, item) in reductions.iter_mut().enumerate() {
        if let Some(group) = tree_group_of(item) {
            result.push(TreeGroupToAnalyze {
                group,
                location: TreeGroupLocation::InReductions,
                item_index,
            });
        }
    }

    result
}

/// Populate cte_requirements down a metadata CHAIN.
///
/// For chained levels like `country:~> status:~> name:~>`:
/// - each level needs its own cte_requirements;
/// - each level's accumulated keys are its parent's plus its own key.
fn populate_nested_metadata_cte_requirements(
    group: &mut ast::MetadataGroup,
    location: TreeGroupLocation,
    accumulated_keys: Vec<(Option<String>, ast::DomainExpression)>,
) -> Result<()> {
    let this_key = ast::DomainExpression::Reference(Reference::Named(NamedReference(
        ColumnOccurrence::engine(group.key.column),
    )));

    // Accumulated keys for this level = parent's keys + this key (a metadata
    // level's key has no key name).
    let mut my_accumulated_keys = accumulated_keys.clone();
    my_accumulated_keys.push((None, this_key));

    // For metadata levels, nested_members_info describes the target.
    group.cte_requirements = Some(CteRequirements {
        needs_cte: true,
        accumulated_grouping_keys: my_accumulated_keys.clone(),
        // JOIN on parent's keys (just expressions)
        join_keys: accumulated_keys.iter().map(|(_, e)| e.clone()).collect(),
        location,
        nested_members_info: vec![NestedMemberCteInfo {
            key: "constructor".to_string(),
        }],
    });

    if let MetadataTarget::Group(nested) = &mut group.target {
        populate_nested_metadata_cte_requirements(nested, location, my_accumulated_keys)?;
    }

    Ok(())
}

/// Compute CTE requirements for a single tree group
///
/// Given:
/// - The tree group expression
/// - Location (keys or reductions)
/// - Outer grouping keys (from the group keys)
///
/// Returns CteRequirements with:
/// - accumulated_grouping_keys = outer + inner
/// - join_keys = outer
/// - location
/// - nested_members_info (placeholder for now, Phase R4+ will use this)
fn compute_cte_requirements(
    group: &mut TreeGroup<'_>,
    location: TreeGroupLocation,
    outer_grouping_keys: &[(Option<String>, ast::DomainExpression)],
) -> Result<CteRequirements> {
    // The inner grouping keys this group contributes, and the nested members
    // that will become CTE columns. A metadata chain populates every level of
    // itself first, and contributes its own key.
    let (inner_keys_with_names, nested_members_info) = match group {
        TreeGroup::Record(record) => {
            let expr = ast::DomainExpression::Application(FunctionApplication::Enclyph(
                Enclyph::Record((**record).clone()),
            ));
            (
                extract_inner_grouping_keys_with_names(&expr),
                extract_nested_member_info(&expr),
            )
        }
        TreeGroup::Metadata(group) => {
            populate_nested_metadata_cte_requirements(
                group,
                location,
                outer_grouping_keys.to_vec(),
            )?;
            (vec![metadata_key(group)], Vec::new())
        }
    };
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
        TreeGroupLocation::InKeys => {
            // Scalar context: CTE groups by inner keys only, joins on inner keys
            (inner_keys_with_names, inner_keys_exprs)
        }
        TreeGroupLocation::InReductions => {
            // Aggregate context: CTE groups by outer + inner, joins on outer keys
            let mut accumulated = outer_grouping_keys.to_vec();
            accumulated.extend(inner_keys_with_names);
            let join_exprs: Vec<_> = outer_grouping_keys.iter().map(|(_, e)| e.clone()).collect();
            (accumulated, join_exprs)
        }
    };

    Ok(CteRequirements {
        needs_cte: true,
        accumulated_grouping_keys,
        join_keys,
        location,
        nested_members_info,
    })
}

/// Extract nested member information from a tree group
///
/// For each nested reduction (`"key": ~> {...}`), record the key name.
fn extract_nested_member_info(expr: &ast::DomainExpression) -> Vec<NestedMemberCteInfo> {
    match expr {
        ast::DomainExpression::Application(FunctionApplication::Enclyph(Enclyph::Record(
            record,
        ))) => {
            record
                .members
                .iter()
                .filter_map(|member| match member {
                    ast::RecordMember::Induced { key, .. }
                    | ast::RecordMember::Metadata { key, .. } => {
                        Some(NestedMemberCteInfo { key: key.clone() })
                    }
                    // No other member re-enters reduction, so none needs a CTE.
                    ast::RecordMember::Keyed { .. }
                    | ast::RecordMember::SelfKeyed(_)
                    | ast::RecordMember::Spread(_) => None,
                })
                .collect()
        }
        // A metadata level's own requirements are populated by the chain walk.
        _ => vec![],
    }
}

/// Main entry point: Analyze all tree groups and populate cte_requirements
///
/// This function is called by the resolver after basic resolution is complete.
/// It finds all tree groups with nested reductions in the group specification
/// and populates their cte_requirements field with the metadata needed for
/// independent CTE generation.
///
/// Parameters:
/// - keys: The grouping keys (may contain tree groups in scalar context)
/// - reductions: The aggregate expressions (may contain tree groups in aggregate context)
///
/// Side effects:
/// - Mutates tree groups in-place to set their cte_requirements field
pub fn analyze_tree_groups_for_ctes(
    keys: &mut [ast::OutItem],
    reductions: &mut [ast::ReductionItem],
) -> Result<ast::ReductionPlan> {
    // Build outer grouping keys WITH KEY NAMES, expanding tree groups to their
    // inner keys. This ensures we GROUP BY the actual identifiers, not the JSON
    // construction (a key is a publication item; reach the value it publishes).
    let mut outer_grouping_keys: Vec<(Option<String>, ast::DomainExpression)> = Vec::new();
    for item in keys.iter() {
        let Some(expr) = item.value() else {
            continue;
        };
        if has_nested_reductions(expr) {
            // For tree groups, use their inner grouping keys with key names
            outer_grouping_keys.extend(extract_inner_grouping_keys_with_names(expr));
        } else {
            // For non-tree groups, use as-is (no key name)
            outer_grouping_keys.push((None, expr.clone()));
        }
    }

    let mut plan = ReductionPlan::empty();
    for mut tree_group in collect_tree_groups_needing_ctes(keys, reductions) {
        let requirements = compute_cte_requirements(
            &mut tree_group.group,
            tree_group.location,
            &outer_grouping_keys,
        )?;
        match tree_group.group {
            TreeGroup::Record(_) => plan.tree_groups.push(TreeGroupPlan {
                location: tree_group.location,
                item_index: tree_group.item_index,
                requirements,
            }),
            TreeGroup::Metadata(group) => group.cte_requirements = Some(requirements),
        }
    }

    Ok(plan)
}
