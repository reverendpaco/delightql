// Expression rewriting for tree groups with CTE references

use super::CteJoinInfo;
use crate::error::Result;
use crate::pipeline::asts::addressed as ast_addressed;

/// Replace nested reduction members in a tree group with references to CTE columns
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn replace_nested_reductions_with_cte_columns(
    expr: ast_addressed::DomainExpression,
    cte_name: &str,
    column_aliases: &[String],
    grouping_dress_keys: &[(usize, String)],
) -> Result<ast_addressed::DomainExpression> {
    use ast_addressed::{DomainExpression, FunctionExpression};

    match expr {
        DomainExpression::Function(func_expr) => match func_expr {
            FunctionExpression::Curly {
                members,
                inner_grouping_keys: _inner_grouping_keys,
                cte_requirements,
                alias,
            } => {
                use ast_addressed::CurlyMember;
                let mut new_members = Vec::new();

                // Separate nested and non-nested members
                let (nested_members, non_nested_members) =
                    crate::pipeline::transformer_v3::tree_group_ctes::extract_nested_members(
                        members,
                    );

                // Process non_nested members (simple grouping keys + simple renamed fields)
                // GROUPING DRESS will be handled separately via grouping_dress_keys
                for member in non_nested_members {
                    match member {
                        CurlyMember::Shorthand { column, .. } => {
                            // Simple grouping key - qualify with CTE name
                            new_members.push(CurlyMember::Shorthand {
                                column,
                                qualifier: Some(cte_name.into()),
                                schema: None,
                            });
                        }
                        CurlyMember::KeyValue {
                            key,
                            nested_reduction: false,
                            value,
                        } if !matches!(
                            value.as_ref(),
                            DomainExpression::Function(FunctionExpression::Curly { .. })
                        ) =>
                        {
                            // Simple renamed field like {"product_name": p.name}
                            // Rewrite value to reference CTE column
                            if let DomainExpression::Lvar { name, .. } = value.as_ref() {
                                new_members.push(CurlyMember::KeyValue {
                                    key: key.clone(),
                                    nested_reduction: false,
                                    value: Box::new(DomainExpression::Lvar {
                                        name: name.clone(),
                                        qualifier: Some(cte_name.into()),
                                        namespace_path: ast_addressed::NamespacePath::empty(),
                                        alias: None,
                                        provenance: ast_addressed::PhaseBox::phantom(),
                                    }),
                                });
                            }
                        }
                        _ => {
                            // GROUPING DRESS members - skip here, will be added from grouping_dress_keys
                        }
                    }
                }

                // Add GROUPING DRESS members from grouping_dress_keys
                for (col_idx, key_name) in grouping_dress_keys {
                    if let Some(cte_col_name) = column_aliases.get(*col_idx) {
                        new_members.push(CurlyMember::KeyValue {
                            key: key_name.clone(),
                            nested_reduction: false,
                            value: Box::new(DomainExpression::Function(
                                FunctionExpression::Regular {
                                    name: "JSON".into(),
                                    namespace: None,
                                    arguments: vec![DomainExpression::Lvar {
                                        name: cte_col_name.clone().into(),
                                        qualifier: Some(cte_name.into()),
                                        namespace_path: ast_addressed::NamespacePath::empty(),
                                        alias: None,
                                        provenance: ast_addressed::PhaseBox::phantom(),
                                    }],
                                    alias: None,
                                    conditioned_on: None,
                                },
                            )),
                        });
                    }
                }

                // Add aggregate reductions (nested_members)
                // Column layout: [simple_grouping_keys, grouping_dress, aggregates]
                // Total grouping keys = all columns except aggregates
                let total_grouping_keys_count = column_aliases.len() - nested_members.len();
                for (idx, (key, _)) in nested_members.iter().enumerate() {
                    let cte_col_idx = total_grouping_keys_count + idx;
                    let cte_column = column_aliases
                        .get(cte_col_idx)
                        .cloned()
                        .or_else(|| {
                            cte_requirements.as_ref().and_then(|req| {
                                req.nested_members_info
                                    .iter()
                                    .find(|info| info.key == *key)
                                    .map(|info| info.cte_column_name.clone())
                            })
                        })
                        .unwrap_or_else(|| key.clone());

                    let col_ref = DomainExpression::Function(FunctionExpression::Regular {
                        name: "json".into(),
                        namespace: None,
                        arguments: vec![DomainExpression::Lvar {
                            name: cte_column.into(),
                            qualifier: Some(cte_name.into()),
                            namespace_path: ast_addressed::NamespacePath::empty(),
                            alias: None,
                            provenance: ast_addressed::PhaseBox::phantom(),
                        }],
                        alias: None,
                        conditioned_on: None,
                    });

                    new_members.push(CurlyMember::KeyValue {
                        key: key.clone(),
                        nested_reduction: false,
                        value: Box::new(col_ref),
                    });
                }

                Ok(DomainExpression::Function(FunctionExpression::Curly {
                    members: new_members,
                    inner_grouping_keys: Vec::new(),
                    cte_requirements,
                    alias,
                }))
            }
            FunctionExpression::MetadataTreeGroup {
                key_column,
                key_qualifier: _,
                key_schema,
                constructor: _,
                keys_only,
                cte_requirements,
                alias,
            } => {
                let transformed_constructor = Box::new(FunctionExpression::Regular {
                    name: "json".into(),
                    namespace: None,
                    arguments: vec![DomainExpression::Lvar {
                        name: "constructor".into(),
                        qualifier: Some(cte_name.into()),
                        namespace_path: ast_addressed::NamespacePath::empty(),
                        alias: None,
                        provenance: ast_addressed::PhaseBox::phantom(),
                    }],
                    alias: None,
                    conditioned_on: None,
                });

                Ok(DomainExpression::Function(
                    FunctionExpression::MetadataTreeGroup {
                        key_column,
                        key_qualifier: Some(cte_name.into()),
                        key_schema,
                        constructor: transformed_constructor,
                        keys_only,
                        cte_requirements,
                        alias,
                    },
                ))
            }
            _ => Ok(DomainExpression::Function(func_expr)),
        },
        other => Ok(other),
    }
}
/// Modify tree group expressions to reference CTE columns
pub(in crate::pipeline::transformer_v3::pipe_operators::projection) fn modify_expressions_for_ctes(
    reducing_by: &mut [ast_addressed::DomainExpression],
    reducing_on: &mut [ast_addressed::DomainExpression],
    cte_joins: &[CteJoinInfo],
) -> Result<()> {
    log::debug!(
        "modify_expressions_for_ctes: {} CTE joins to process",
        cte_joins.len()
    );

    for cte_join in cte_joins {
        log::debug!(
            "Processing CTE join: {}, location: {:?}, index: {}",
            cte_join.cte_name,
            cte_join.location,
            cte_join.original_index
        );

        let expr = match cte_join.location {
            ast_addressed::TreeGroupLocation::InReducingBy => {
                &mut reducing_by[cte_join.original_index]
            }
            ast_addressed::TreeGroupLocation::InReducingOn => {
                &mut reducing_on[cte_join.original_index]
            }
        };

        match expr {
            ast_addressed::DomainExpression::Function(ast_addressed::FunctionExpression::Curly {
                members,
                inner_grouping_keys,
                cte_requirements,
                ..
            }) => {
                let (nested_members, non_nested_members) =
                    crate::pipeline::transformer_v3::tree_group_ctes::extract_nested_members(
                        members.clone(),
                    );

                if !nested_members.is_empty() {
                    let mut new_members = Vec::new();
                    let outer_keys_count = if let Some(req) = cte_requirements {
                        req.accumulated_grouping_keys.len() - inner_grouping_keys.len()
                    } else {
                        0
                    };

                    if cte_join.location == ast_addressed::TreeGroupLocation::InReducingOn {
                        for (idx, grouping_expr) in inner_grouping_keys.iter().enumerate() {
                            let cte_col_idx = outer_keys_count + idx;
                            if let Some(_cte_col_name) = cte_join.column_aliases.get(cte_col_idx) {
                                if let ast_addressed::DomainExpression::Lvar { name, .. } =
                                    grouping_expr
                                {
                                    new_members.push(ast_addressed::CurlyMember::Shorthand {
                                        column: name.clone(),
                                        qualifier: Some(cte_join.cte_name.clone().into()),
                                        schema: None,
                                    });
                                }
                            }
                        }
                    }

                    // Process GROUPING DRESS (non_nested scalar nested objects)
                    // These need to reference CTE columns that were generated for them
                    let mut grouping_dress_col_idx = outer_keys_count + inner_grouping_keys.len();
                    for member in non_nested_members {
                        match &member {
                            ast_addressed::CurlyMember::KeyValue {
                                key,
                                nested_reduction: false,
                                value,
                            } if matches!(
                                value.as_ref(),
                                ast_addressed::DomainExpression::Function(
                                    ast_addressed::FunctionExpression::Curly { .. }
                                )
                            ) =>
                            {
                                // This is GROUPING DRESS - reference the CTE column
                                if let Some(cte_col_name) =
                                    cte_join.column_aliases.get(grouping_dress_col_idx)
                                {
                                    new_members.push(ast_addressed::CurlyMember::KeyValue {
                                        key: key.clone(),
                                        nested_reduction: false,
                                        value: Box::new(ast_addressed::DomainExpression::Function(
                                            ast_addressed::FunctionExpression::Regular {
                                                name: "JSON".into(),
                                                namespace: None,
                                                arguments: vec![
                                                    ast_addressed::DomainExpression::Lvar {
                                                        name: cte_col_name.clone().into(),
                                                        qualifier: Some(cte_join.cte_name.clone().into()),
                                                        namespace_path:
                                                            ast_addressed::NamespacePath::empty(),
                                                        alias: None,
                                                        provenance: ast_addressed::PhaseBox::new(
                                                            None,
                                                        ),
                                                    },
                                                ],
                                                alias: None,
                                                conditioned_on: None,
                                            },
                                        )),
                                    });
                                    grouping_dress_col_idx += 1;
                                }
                            }
                            _ => {
                                // Other non-nested members (simple scalar fields)
                                new_members.push(member);
                            }
                        }
                    }

                    for (idx, (key, _)) in nested_members.iter().enumerate() {
                        let cte_col_idx = grouping_dress_col_idx + idx;
                        if let Some(cte_col_name) = cte_join.column_aliases.get(cte_col_idx) {
                            new_members.push(ast_addressed::CurlyMember::KeyValue {
                                key: key.clone(),
                                nested_reduction: false,
                                value: Box::new(ast_addressed::DomainExpression::Function(
                                    ast_addressed::FunctionExpression::Regular {
                                        name: "JSON".into(),
                                        namespace: None,
                                        arguments: vec![ast_addressed::DomainExpression::Lvar {
                                            name: cte_col_name.clone().into(),
                                            qualifier: Some(cte_join.cte_name.clone().into()),
                                            namespace_path: ast_addressed::NamespacePath::empty(),
                                            alias: None,
                                            provenance: ast_addressed::PhaseBox::phantom(),
                                        }],
                                        alias: None,
                                        conditioned_on: None,
                                    },
                                )),
                            });
                        }
                    }

                    log::debug!(
                        "Modified tree group members: {} total, location: {:?}",
                        new_members.len(),
                        cte_join.location
                    );
                    *members = new_members;
                }
            }
            ast_addressed::DomainExpression::Function(
                ast_addressed::FunctionExpression::MetadataTreeGroup {
                    key_qualifier,
                    constructor,
                    keys_only: _keys_only,
                    cte_requirements,
                    ..
                },
            ) => {
                *key_qualifier = Some(cte_join.cte_name.clone().into());

                let outer_keys_count = if let Some(req) = cte_requirements {
                    req.accumulated_grouping_keys.len() - 1
                } else {
                    0
                };

                let cte_col_idx = outer_keys_count + 1;
                if let Some(cte_col_name) = cte_join.column_aliases.get(cte_col_idx) {
                    *constructor = Box::new(ast_addressed::FunctionExpression::Regular {
                        name: "JSON".into(),
                        namespace: None,
                        arguments: vec![ast_addressed::DomainExpression::Lvar {
                            name: cte_col_name.clone().into(),
                            qualifier: Some(cte_join.cte_name.clone().into()),
                            namespace_path: ast_addressed::NamespacePath::empty(),
                            alias: None,
                            provenance: ast_addressed::PhaseBox::phantom(),
                        }],
                        alias: None,
                        conditioned_on: None,
                    });
                }
            }
            other => panic!("catch-all hit in expression_rewriting.rs modify_expressions_for_ctes (DomainExpression): {:?}", other),
        }
    }

    Ok(())
}
