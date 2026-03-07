use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::ProjectionExpr;
use crate::pipeline::resolver::resolver_fold::ResolverFold;

/// Resolve function arguments via fold, preserving Glob projections
pub(in crate::pipeline::resolver) fn resolve_function_arguments_via_fold(
    fold: &mut ResolverFold,
    arguments: Vec<ast_unresolved::DomainExpression>,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let mut resolved = Vec::new();
    for arg in arguments {
        match arg {
            ast_unresolved::DomainExpression::Projection(ProjectionExpr::Glob {
                qualifier,
                namespace_path,
                ..
            }) => {
                resolved.push(ast_resolved::DomainExpression::Projection(
                    ProjectionExpr::Glob {
                        qualifier,
                        namespace_path,
                    },
                ));
            }
            ast_unresolved::DomainExpression::Projection(ProjectionExpr::JsonPathLiteral {
                segments,
                root_is_array,
                alias,
            }) => {
                // JsonPathLiteral is a simple value — pass through as-is
                resolved.push(ast_resolved::DomainExpression::Projection(
                    ProjectionExpr::JsonPathLiteral {
                        segments,
                        root_is_array,
                        alias,
                    },
                ));
            }
            other => {
                let resolved_arg = fold.transform_domain(other)?;
                resolved.push(resolved_arg);
            }
        }
    }
    Ok(resolved)
}

/// Resolve bracket tree group with ergonomic inductor expansion
pub(in crate::pipeline::resolver) fn resolve_bracket_via_fold(
    fold: &mut ResolverFold,
    func: ast_unresolved::FunctionExpression,
) -> Result<ast_resolved::FunctionExpression> {
    use crate::pipeline::asts::{resolved, unresolved};
    use std::collections::HashSet;

    let (arguments, alias) = match func {
        ast_unresolved::FunctionExpression::Bracket { arguments, alias } => (arguments, alias),
        _ => unreachable!("resolve_bracket_via_fold called with non-Bracket"),
    };

    let available = fold.available.clone();
    let mut seen_columns = HashSet::new();
    let mut resolved_args: Vec<resolved::DomainExpression> = Vec::new();

    for arg in arguments {
        match arg {
            unresolved::DomainExpression::Projection(ProjectionExpr::Glob {
                qualifier,
                namespace_path,
            }) => {
                for col in &available {
                    let column_name = col.name().to_string();
                    if seen_columns.insert(column_name.clone()) {
                        let col_qualifier = match &col.fq_table.name {
                            ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                Some(name.to_string())
                            }
                            ast_resolved::TableName::Named(_)
                            | ast_resolved::TableName::Fresh => None,
                        };
                        resolved_args.push(resolved::DomainExpression::Lvar {
                            name: column_name.into(),
                            qualifier: qualifier
                                .clone()
                                .or(col_qualifier.map(|s| s.into())),
                            namespace_path: if namespace_path.is_empty() {
                                col.fq_table.parents_path.clone()
                            } else {
                                namespace_path.clone()
                            },
                            alias: None,
                            provenance: ast_resolved::PhaseBox::phantom(),
                        });
                    }
                }
            }
            unresolved::DomainExpression::Projection(ProjectionExpr::Pattern {
                pattern,
                alias: _,
            }) => {
                use crate::pipeline::pattern::bre_to_rust_regex;
                let regex_pattern = bre_to_rust_regex(&pattern)?;
                let re = regex::Regex::new(&regex_pattern).map_err(|e| {
                    DelightQLError::parse_error(format!("Invalid column pattern: {}", e))
                })?;

                for col in available.iter().filter(|col| re.is_match(col.name())) {
                    let column_name = col.name().to_string();
                    if seen_columns.insert(column_name.clone()) {
                        let qualifier = match &col.fq_table.name {
                            ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                Some(name.to_string())
                            }
                            ast_resolved::TableName::Named(_)
                            | ast_resolved::TableName::Fresh => None,
                        };
                        resolved_args.push(resolved::DomainExpression::Lvar {
                            name: column_name.into(),
                            qualifier: qualifier.map(|s| s.into()),
                            namespace_path: col.fq_table.parents_path.clone(),
                            alias: None,
                            provenance: ast_resolved::PhaseBox::phantom(),
                        });
                    }
                }
            }
            unresolved::DomainExpression::Projection(ProjectionExpr::ColumnRange(
                range_box,
            )) => {
                let range = range_box.get();
                let candidates: Vec<_> = available.iter().collect();

                if candidates.is_empty() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: format!(
                            "|{}:{}|",
                            range
                                .start
                                .map(|(p, r)| if r {
                                    format!("-{}", p)
                                } else {
                                    p.to_string()
                                })
                                .unwrap_or_default(),
                            range
                                .end
                                .map(|(p, r)| if r {
                                    format!("-{}", p)
                                } else {
                                    p.to_string()
                                })
                                .unwrap_or_default()
                        ),
                        context:
                            "No columns available for range resolution in bracket function"
                                .to_string(),
                    });
                }

                let start_idx = if let Some((pos, reverse)) = range.start {
                    if reverse {
                        candidates.len().saturating_sub(pos as usize)
                    } else {
                        (pos.saturating_sub(1)) as usize
                    }
                } else {
                    0
                };

                let end_idx = if let Some((pos, reverse)) = range.end {
                    if reverse {
                        candidates.len().saturating_sub(pos as usize)
                    } else {
                        (pos.saturating_sub(1)) as usize
                    }
                } else {
                    candidates.len().saturating_sub(1)
                };

                for idx in start_idx..=end_idx.min(candidates.len().saturating_sub(1)) {
                    let col = candidates[idx];
                    let column_name = col.name().to_string();
                    if seen_columns.insert(column_name.clone()) {
                        let qualifier = match &col.fq_table.name {
                            ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                Some(name.to_string())
                            }
                            ast_resolved::TableName::Named(_)
                            | ast_resolved::TableName::Fresh => None,
                        };
                        resolved_args.push(resolved::DomainExpression::Lvar {
                            name: column_name.into(),
                            qualifier: qualifier.map(|s| s.into()),
                            namespace_path: col.fq_table.parents_path.clone(),
                            alias: None,
                            provenance: ast_resolved::PhaseBox::phantom(),
                        });
                    }
                }
            }
            unresolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: _,
            } => {
                if seen_columns.insert(name.to_string()) {
                    resolved_args.push(resolved::DomainExpression::Lvar {
                        name,
                        qualifier,
                        namespace_path,
                        alias,
                        provenance: ast_resolved::PhaseBox::phantom(),
                    });
                }
            }
            other => {
                let resolved = fold.transform_domain(other)?;
                resolved_args.push(resolved);
            }
        }
    }

    Ok(ast_resolved::FunctionExpression::Bracket {
        arguments: resolved_args,
        alias,
    })
}

/// Resolve curly tree group with ergonomic inductor expansion + column validation
pub(in crate::pipeline::resolver) fn resolve_curly_via_fold(
    fold: &mut ResolverFold,
    func: ast_unresolved::FunctionExpression,
) -> Result<ast_resolved::FunctionExpression> {
    use crate::pipeline::asts::{resolved, unresolved};
    use std::collections::HashSet;

    let (members, alias) = match func {
        ast_unresolved::FunctionExpression::Curly {
            members,
            inner_grouping_keys: _,
            cte_requirements: _,
            alias,
        } => (members, alias),
        _ => unreachable!("resolve_curly_via_fold called with non-Curly"),
    };

    let available = fold.available.clone();
    let mut seen_columns = HashSet::new();
    let mut resolved_members: Vec<resolved::CurlyMember> = Vec::new();

    for member in members {
        match member {
            unresolved::CurlyMember::Glob => {
                for col in &available {
                    let column_name = col.name().to_string();
                    if seen_columns.insert(column_name.clone()) {
                        let qualifier = match &col.fq_table.name {
                            ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                Some(name.to_string())
                            }
                            ast_resolved::TableName::Named(_)
                            | ast_resolved::TableName::Fresh => None,
                        };
                        resolved_members.push(resolved::CurlyMember::Shorthand {
                            column: column_name.into(),
                            qualifier: qualifier.map(|s| s.into()),
                            schema: col
                                .fq_table
                                .parents_path
                                .first()
                                .map(|s| s.to_string().into()),
                        });
                    }
                }
            }
            unresolved::CurlyMember::Pattern { pattern } => {
                use crate::pipeline::pattern::bre_to_rust_regex;
                let regex_pattern = bre_to_rust_regex(&pattern)?;
                let re = regex::Regex::new(&regex_pattern).map_err(|e| {
                    DelightQLError::parse_error(format!("Invalid column pattern: {}", e))
                })?;

                for col in available.iter().filter(|col| re.is_match(col.name())) {
                    let column_name = col.name().to_string();
                    if seen_columns.insert(column_name.clone()) {
                        let qualifier = match &col.fq_table.name {
                            ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                Some(name.to_string())
                            }
                            ast_resolved::TableName::Named(_)
                            | ast_resolved::TableName::Fresh => None,
                        };
                        resolved_members.push(resolved::CurlyMember::Shorthand {
                            column: column_name.into(),
                            qualifier: qualifier.map(|s| s.into()),
                            schema: col
                                .fq_table
                                .parents_path
                                .first()
                                .map(|s| s.to_string().into()),
                        });
                    }
                }
            }
            unresolved::CurlyMember::OrdinalRange { start, end } => {
                let candidates: Vec<_> = available.iter().collect();

                if candidates.is_empty() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: format!(
                            "|{}:{}|",
                            start
                                .map(|(p, r)| if r {
                                    format!("-{}", p)
                                } else {
                                    p.to_string()
                                })
                                .unwrap_or_default(),
                            end.map(|(p, r)| if r {
                                format!("-{}", p)
                            } else {
                                p.to_string()
                            })
                            .unwrap_or_default()
                        ),
                        context:
                            "No columns available for range resolution in curly function"
                                .to_string(),
                    });
                }

                let start_idx = if let Some((pos, reverse)) = start {
                    if reverse {
                        candidates.len().saturating_sub(pos as usize)
                    } else {
                        (pos.saturating_sub(1)) as usize
                    }
                } else {
                    0
                };

                let end_idx = if let Some((pos, reverse)) = end {
                    if reverse {
                        candidates.len().saturating_sub(pos as usize)
                    } else {
                        (pos.saturating_sub(1)) as usize
                    }
                } else {
                    candidates.len().saturating_sub(1)
                };

                for idx in start_idx..=end_idx.min(candidates.len().saturating_sub(1)) {
                    let col = candidates[idx];
                    let column_name = col.name().to_string();
                    if seen_columns.insert(column_name.clone()) {
                        let qualifier = match &col.fq_table.name {
                            ast_resolved::TableName::Named(name) if !name.is_empty() => {
                                Some(name.to_string())
                            }
                            ast_resolved::TableName::Named(_)
                            | ast_resolved::TableName::Fresh => None,
                        };
                        resolved_members.push(resolved::CurlyMember::Shorthand {
                            column: column_name.into(),
                            qualifier: qualifier.map(|s| s.into()),
                            schema: col
                                .fq_table
                                .parents_path
                                .first()
                                .map(|s| s.to_string().into()),
                        });
                    }
                }
            }
            unresolved::CurlyMember::Shorthand {
                column,
                qualifier,
                schema,
            } => {
                if !available.is_empty() {
                    use crate::pipeline::resolver::unification::{
                        unify_columns, ColumnReference, UnificationResult,
                    };
                    let col_ref = ColumnReference::Named {
                        name: column.to_string(),
                        qualifier: qualifier.as_ref().map(|q| q.to_string()),
                        schema: None,
                    };
                    let result = unify_columns(vec![col_ref], &available)
                        .into_iter()
                        .next()
                        .unwrap();
                    match result {
                        UnificationResult::Resolved(_) => {}
                        UnificationResult::Unresolved(col_name) => {
                            return Err(DelightQLError::column_not_found_error(
                                col_name,
                                "in tree group key",
                            ));
                        }
                        UnificationResult::Ambiguous {
                            column: col,
                            tables,
                        } => {
                            return Err(DelightQLError::validation_error_categorized(
                                "resolution/ambiguous",
                                format!(
                                    "Ambiguous column '{}' in tree group key: found in tables {}. Use a qualifier (e.g., {}.{})",
                                    col,
                                    tables.join(", "),
                                    tables[0],
                                    col,
                                ),
                                "in tree group key",
                            ));
                        }
                    }
                }
                if seen_columns.insert(column.to_string()) {
                    resolved_members.push(resolved::CurlyMember::Shorthand {
                        column,
                        qualifier,
                        schema,
                    });
                }
            }
            unresolved::CurlyMember::Comparison { condition } => {
                resolved_members.push(resolved::CurlyMember::Comparison {
                    condition: Box::new(fold.transform_boolean(*condition)?),
                });
            }
            unresolved::CurlyMember::KeyValue {
                key,
                nested_reduction,
                value,
            } => {
                resolved_members.push(resolved::CurlyMember::KeyValue {
                    key,
                    nested_reduction,
                    value: Box::new(fold.transform_domain(*value)?),
                });
            }
            unresolved::CurlyMember::PathLiteral { path, alias } => {
                resolved_members.push(resolved::CurlyMember::PathLiteral {
                    path: Box::new(fold.transform_domain(*path)?),
                    alias,
                });
            }
            unresolved::CurlyMember::Placeholder => {
                resolved_members.push(resolved::CurlyMember::Placeholder);
            }
        }
    }

    Ok(ast_resolved::FunctionExpression::Curly {
        members: resolved_members,
        inner_grouping_keys: vec![],
        cte_requirements: None,
        alias,
    })
}

/// Resolve window frame specification via fold
pub(in crate::pipeline::resolver) fn resolve_window_frame_via_fold(
    fold: &mut ResolverFold,
    frame: ast_unresolved::WindowFrame,
) -> Result<ast_resolved::WindowFrame> {
    use crate::pipeline::asts::{resolved, unresolved};

    let resolve_bound =
        |fold: &mut ResolverFold,
         bound: unresolved::FrameBound|
         -> Result<resolved::FrameBound> {
            match bound {
                unresolved::FrameBound::Unbounded => Ok(resolved::FrameBound::Unbounded),
                unresolved::FrameBound::CurrentRow => Ok(resolved::FrameBound::CurrentRow),
                unresolved::FrameBound::Preceding(expr) => {
                    let resolved_expr = fold.transform_domain(*expr)?;
                    Ok(resolved::FrameBound::Preceding(Box::new(resolved_expr)))
                }
                unresolved::FrameBound::Following(expr) => {
                    let resolved_expr = fold.transform_domain(*expr)?;
                    Ok(resolved::FrameBound::Following(Box::new(resolved_expr)))
                }
            }
        };

    let start = resolve_bound(fold, frame.start)?;
    let end = resolve_bound(fold, frame.end)?;

    Ok(resolved::WindowFrame {
        mode: frame.mode,
        start,
        end,
    })
}
