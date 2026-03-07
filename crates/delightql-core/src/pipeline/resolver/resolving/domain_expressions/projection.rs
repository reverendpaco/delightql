use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};

fn expand_glob(
    qualifier: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
) -> Result<Vec<ast_resolved::DomainExpression>> {
    if let Some(qual) = qualifier {
        Ok(available
            .iter()
            .filter(
                |col| matches!(&col.fq_table.name, ast_resolved::TableName::Named(t) if t == &qual),
            )
            .map(|col| ast_resolved::DomainExpression::Lvar {
                name: col.name().into(),
                qualifier: Some(qual.clone().into()),
                namespace_path: col.fq_table.parents_path.clone(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            })
            .collect())
    } else {
        Ok(available
            .iter()
            .map(|col| {
                let qualifier = match &col.fq_table.name {
                    ast_resolved::TableName::Named(name) if !name.is_empty() => {
                        Some(name.to_string())
                    }
                    ast_resolved::TableName::Named(_) | ast_resolved::TableName::Fresh => None,
                };
                ast_resolved::DomainExpression::Lvar {
                    name: col.name().into(),
                    qualifier: qualifier.map(|s| s.into()),
                    namespace_path: col.fq_table.parents_path.clone(),
                    alias: None,
                    provenance: ast_resolved::PhaseBox::phantom(),
                }
            })
            .collect())
    }
}

fn expand_pattern(
    pattern: &str,
    available: &[ast_resolved::ColumnMetadata],
    allow_zero_matches: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    use crate::pipeline::pattern::bre_to_rust_regex;
    let regex_pattern = bre_to_rust_regex(pattern)?;

    // Create regex for matching
    let re = regex::Regex::new(&regex_pattern)
        .map_err(|e| DelightQLError::parse_error(format!("Invalid column pattern: {}", e)))?;

    let columns: Vec<_> = available
        .iter()
        .filter(|col| re.is_match(col.name()))
        .map(|col| {
            let qualifier = match &col.fq_table.name {
                ast_resolved::TableName::Named(name) if !name.is_empty() => Some(name.to_string()),
                ast_resolved::TableName::Named(_) | ast_resolved::TableName::Fresh => None,
            };
            ast_resolved::DomainExpression::Lvar {
                name: col.name().into(),
                qualifier: qualifier.map(|s| s.into()),
                namespace_path: col.fq_table.parents_path.clone(),
                alias: None,
                provenance: ast_resolved::PhaseBox::phantom(),
            }
        })
        .collect();

    if columns.is_empty() {
        if !allow_zero_matches {
            return Err(DelightQLError::parse_error(format!(
                "Pattern '{}' does not match any columns",
                pattern
            )));
        }
    }

    Ok(columns)
}

fn calculate_ordinal_index(
    ordinal: &ast_unresolved::ColumnOrdinal,
    total_cols: usize,
) -> Result<usize> {
    if ordinal.reverse {
        if ordinal.position as usize > total_cols {
            return Err(DelightQLError::ColumnNotFoundError {
                column: format!("|-{}|", ordinal.position),
                context: format!(
                    "Position {} from end exceeds {} available columns",
                    ordinal.position, total_cols
                ),
            });
        }
        Ok(total_cols - ordinal.position as usize)
    } else {
        if ordinal.position == 0 {
            return Err(DelightQLError::ColumnNotFoundError {
                column: "|0|".to_string(),
                context: "Column positions start at 1".to_string(),
            });
        }
        let pos = (ordinal.position - 1) as usize;
        if pos >= total_cols {
            return Err(DelightQLError::ColumnNotFoundError {
                column: format!("|{}|", ordinal.position),
                context: format!(
                    "Position {} exceeds {} available columns",
                    ordinal.position, total_cols
                ),
            });
        }
        Ok(pos)
    }
}

fn calculate_range_start(range: &ast_unresolved::ColumnRange, total_cols: usize) -> Result<usize> {
    if let Some((pos, reverse)) = range.start {
        if reverse {
            if pos as usize > total_cols {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: format!("|-{}:|", pos),
                    context: format!(
                        "Start position {} from end exceeds {} available columns",
                        pos, total_cols
                    ),
                });
            }
            Ok(total_cols - pos as usize)
        } else {
            if pos == 0 {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: "|0:|".to_string(),
                    context: "Column positions start at 1".to_string(),
                });
            }
            let idx = (pos - 1) as usize;
            if idx >= total_cols {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: format!("|{}:|", pos),
                    context: format!(
                        "Start position {} exceeds {} available columns",
                        pos, total_cols
                    ),
                });
            }
            Ok(idx)
        }
    } else {
        Ok(0)
    }
}

fn calculate_range_end(range: &ast_unresolved::ColumnRange, total_cols: usize) -> Result<usize> {
    if let Some((pos, reverse)) = range.end {
        if reverse {
            if pos as usize > total_cols {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: format!("|:{}|", pos),
                    context: format!(
                        "End position {} from end exceeds {} available columns",
                        pos, total_cols
                    ),
                });
            }
            Ok(total_cols - pos as usize)
        } else {
            if pos == 0 {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: "|:0|".to_string(),
                    context: "Column positions start at 1".to_string(),
                });
            }
            let idx = (pos - 1) as usize;
            if idx >= total_cols {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: format!("|:{}|", pos),
                    context: format!(
                        "End position {} exceeds {} available columns",
                        pos, total_cols
                    ),
                });
            }
            Ok(idx)
        }
    } else {
        Ok(total_cols - 1)
    }
}

fn format_range_string(range: &ast_unresolved::ColumnRange) -> String {
    format!(
        "|{}:{}|",
        range
            .start
            .map(|(p, r)| if r { format!("-{}", p) } else { p.to_string() })
            .unwrap_or_else(|| "".to_string()),
        range
            .end
            .map(|(p, r)| if r { format!("-{}", p) } else { p.to_string() })
            .unwrap_or_else(|| "".to_string())
    )
}

/// Resolve a list of domain expressions via the fold walker, expanding globs/patterns/ranges/ordinals
/// structurally but using `fold.transform_domain()` for actual expression resolution.
pub(in crate::pipeline::resolver) fn resolve_expressions_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
    allow_zero_pattern_matches: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let mut resolved = Vec::new();

    for expr in expressions {
        match expr {
            ast_unresolved::DomainExpression::Projection(ref proj) => match proj {
                ProjectionExpr::Glob { qualifier, .. } => {
                    let columns = expand_glob(qualifier.clone().map(|s| s.to_string()), available)?;
                    if columns.is_empty() && qualifier.is_some() {
                        return Err(DelightQLError::validation_error(
                            format!(
                                "Qualified glob '{}.*' matched no columns - table or alias not in scope",
                                qualifier.as_ref().unwrap()
                            ),
                            "Check that the qualifier matches a table name or alias in the query"
                                .to_string(),
                        ));
                    }
                    resolved.extend(columns);
                }
                ProjectionExpr::Pattern { pattern, .. } => {
                    let columns = expand_pattern(pattern, available, allow_zero_pattern_matches)?;
                    resolved.extend(columns);
                }
                ProjectionExpr::ColumnRange(range_box) => {
                    let range = range_box.get();

                    let candidates = if let Some(qual) = &range.qualifier {
                        available.iter()
                            .filter(|col| matches!(&col.fq_table.name, ast_resolved::TableName::Named(t) if t == qual))
                            .collect::<Vec<_>>()
                    } else {
                        available.iter().collect::<Vec<_>>()
                    };

                    if candidates.is_empty() {
                        return Err(DelightQLError::ColumnNotFoundError {
                            column: format_range_string(range),
                            context: "No columns available for range resolution".to_string(),
                        });
                    }

                    let start_idx = calculate_range_start(range, candidates.len())?;
                    let end_idx = calculate_range_end(range, candidates.len())?;

                    if start_idx > end_idx {
                        return Err(DelightQLError::ColumnNotFoundError {
                            column: format_range_string(range),
                            context: format!(
                                "Invalid range: start position {} is after end position {}",
                                start_idx + 1,
                                end_idx + 1
                            ),
                        });
                    }

                    for idx in start_idx..=end_idx {
                        let column = candidates[idx];
                        let qualifier =
                            if let ast_resolved::TableName::Named(name) = &column.fq_table.name {
                                if !name.is_empty() {
                                    Some(name.to_string())
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                        // Use the actual column name (alias if present, otherwise original)
                        let name = column.name().to_string();
                        resolved.push(ast_resolved::DomainExpression::Lvar {
                            name: name.into(),
                            qualifier: qualifier.map(|s| s.into()),
                            namespace_path: column.fq_table.parents_path.clone(),
                            alias: None,
                            provenance: ast_resolved::PhaseBox::phantom(),
                        });
                    }
                }
                // PATH FIRST-CLASS: Epoch 5 - JsonPathLiteral handling
                // JsonPathLiteral is a simple value like Literal - just pass through
                ProjectionExpr::JsonPathLiteral {
                    segments,
                    root_is_array,
                    alias,
                } => {
                    resolved.push(ast_resolved::DomainExpression::Projection(
                        ProjectionExpr::JsonPathLiteral {
                            segments: segments.clone(),
                            root_is_array: *root_is_array,
                            alias: alias.clone(),
                        },
                    ));
                }
            },
            ast_unresolved::DomainExpression::ColumnOrdinal(ordinal_box) => {
                let ordinal = ordinal_box.get();

                let candidates = if let Some(qual) = &ordinal.qualifier {
                    available.iter()
                        .filter(|col| matches!(&col.fq_table.name, ast_resolved::TableName::Named(t) if t == qual))
                        .collect::<Vec<_>>()
                } else {
                    available.iter().collect::<Vec<_>>()
                };

                if candidates.is_empty() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: format!("|{}|", ordinal.position),
                        context: "No columns available for ordinal resolution".to_string(),
                    });
                }

                let idx = calculate_ordinal_index(&ordinal, candidates.len())?;
                let column = candidates[idx];

                // Use the actual column name (alias if present, otherwise original)
                // The ordinal's own alias takes precedence if specified
                let name = column.name().to_string();
                resolved.push(ast_resolved::DomainExpression::Lvar {
                    name: name.into(),
                    qualifier: ordinal.qualifier.clone().map(|s| s.into()).or_else(|| {
                        match &column.fq_table.name {
                            ast_resolved::TableName::Named(t) => Some(t.clone().into()),
                            _ => None,
                        }
                    }),
                    namespace_path: column.fq_table.parents_path.clone(),
                    alias: ordinal.alias.clone().map(|s| s.into()),
                    provenance: ast_resolved::PhaseBox::phantom(),
                });
            }
            ast_unresolved::DomainExpression::Lvar {
                name,
                qualifier,
                namespace_path,
                alias,
                provenance: _,
            } => {
                // Simple lvar resolution — no registry needed, same as existing
                let available_clone = available.to_vec();
                let lvar_expr = ast_unresolved::DomainExpression::Lvar {
                    name,
                    qualifier,
                    namespace_path,
                    alias,
                    provenance: ast_unresolved::PhaseBox::phantom(),
                };
                resolved.push(super::simple::resolve_simple_expr(
                    lvar_expr,
                    &available_clone,
                    fold.in_correlation,
                )?);
            }
            ast_unresolved::DomainExpression::Function(func) => {
                // Delegate through transform_domain which handles StringTemplate→concat
                resolved.push(fold.transform_domain(ast_unresolved::DomainExpression::Function(func))?);
            }
            ast_unresolved::DomainExpression::Literal { value, alias } => {
                resolved.push(ast_resolved::DomainExpression::Literal { value, alias });
            }
            ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
                resolved.push(ast_resolved::DomainExpression::NonUnifiyingUnderscore);
            }
            ast_unresolved::DomainExpression::Predicate { expr: pred, alias } => {
                let resolved_bool = fold.transform_boolean(*pred)?;
                resolved.push(ast_resolved::DomainExpression::Predicate {
                    expr: Box::new(resolved_bool),
                    alias,
                });
            }
            ast_unresolved::DomainExpression::ValuePlaceholder { alias } => {
                resolved.push(ast_resolved::DomainExpression::ValuePlaceholder { alias });
            }
            ast_unresolved::DomainExpression::Substitution(sub) => match sub {
                SubstitutionExpr::Parameter { name, alias } => {
                    resolved.push(ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::Parameter { name, alias },
                    ));
                }
                SubstitutionExpr::CurriedParameter { name, alias } => {
                    resolved.push(ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::CurriedParameter { name, alias },
                    ));
                }
                SubstitutionExpr::ContextParameter { .. } => {
                    return Err(DelightQLError::ParseError {
                        message: "ContextParameter should not appear in unresolved phase"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    });
                }
                SubstitutionExpr::ContextMarker => {
                    resolved.push(ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::ContextMarker,
                    ));
                }
            },
            ast_unresolved::DomainExpression::PipedExpression { .. } => {
                let resolved_expr = fold.transform_domain(expr)?;
                resolved.push(resolved_expr);
            }
            ast_unresolved::DomainExpression::Parenthesized { .. } => {
                let resolved_expr = fold.transform_domain(expr)?;
                resolved.push(resolved_expr);
            }
            ast_unresolved::DomainExpression::Tuple { .. } => {
                let resolved_expr = fold.transform_domain(expr)?;
                resolved.push(resolved_expr);
            }
            ast_unresolved::DomainExpression::ScalarSubquery { .. } => {
                let resolved_expr = fold.transform_domain(expr)?;
                resolved.push(resolved_expr);
            }
            ast_unresolved::DomainExpression::PivotOf {
                value_column,
                pivot_key,
                pivot_values,
            } => {
                let resolved_value = fold.transform_domain(*value_column)?;
                let resolved_key = fold.transform_domain(*pivot_key)?;
                resolved.push(ast_resolved::DomainExpression::PivotOf {
                    value_column: Box::new(resolved_value),
                    pivot_key: Box::new(resolved_key),
                    pivot_values,
                });
            }
        }
    }

    Ok(resolved)
}
