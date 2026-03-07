use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};
use std::collections::HashMap;

/// Resolve a list of domain expressions with available schema
/// This handles expansion of globs, patterns, ranges, and ordinals
/// Resolve expressions with schema, optionally allowing patterns to match zero columns
pub(super) fn resolve_expressions_with_schema_internal(
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[ast_resolved::ColumnMetadata],
    allow_zero_pattern_matches: bool,
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
    schema: Option<&dyn super::super::super::DatabaseSchema>,
    mut cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
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
                resolved.push(ast_resolved::DomainExpression::Lvar {
                    name,
                    qualifier,
                    namespace_path,
                    alias,
                    provenance: ast_resolved::PhaseBox::phantom(),
                });
            }
            ast_unresolved::DomainExpression::Function(func) => {
                if let ast_unresolved::FunctionExpression::StringTemplate { parts, alias } = func {
                    let concat_expr = resolve_string_template_for_projection(
                        parts,
                        alias.map(|s| s.to_string()),
                        available,
                        cfe_defs,
                    )?;
                    resolved.push(concat_expr);
                } else {
                    let resolved_func = super::super::functions::resolve_function_with_schema(
                        func, available, cfe_defs,
                    )?;
                    resolved.push(ast_resolved::DomainExpression::Function(resolved_func));
                }
            }
            ast_unresolved::DomainExpression::Literal { value, alias } => {
                resolved.push(ast_resolved::DomainExpression::Literal { value, alias });
            }
            ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
                resolved.push(ast_resolved::DomainExpression::NonUnifiyingUnderscore);
            }
            ast_unresolved::DomainExpression::Predicate { expr, alias } => {
                let resolved_bool = resolve_predicate_for_projection(
                    *expr,
                    available,
                    cfe_defs,
                    schema,
                    cte_context.as_deref_mut(),
                    in_correlation,
                )?;
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
                    // Parameters should only appear in CFE bodies
                    resolved.push(ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::Parameter { name, alias },
                    ));
                }
                SubstitutionExpr::CurriedParameter { name, alias } => {
                    // Curried parameters should only appear in HOCFE bodies
                    resolved.push(ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::CurriedParameter { name, alias },
                    ));
                }
                SubstitutionExpr::ContextParameter { .. } => {
                    // ContextParameter should never exist in unresolved phase - it's only created during
                    // postprocessing in refined phase for CCAFE feature
                    return Err(DelightQLError::ParseError {
                        message: "ContextParameter should not appear in unresolved phase"
                            .to_string(),
                        source: None,
                        subcategory: None,
                    });
                }
                SubstitutionExpr::ContextMarker => {
                    // ContextMarker (..) passes through unchanged
                    resolved.push(ast_resolved::DomainExpression::Substitution(
                        SubstitutionExpr::ContextMarker,
                    ));
                }
            },
            ast_unresolved::DomainExpression::PipedExpression {
                value,
                transforms,
                alias,
            } => {
                let resolved_value =
                    super::resolve_domain_expr_with_schema(*value, available, cfe_defs)?;
                let resolved_transforms = transforms
                    .into_iter()
                    .map(|t| {
                        if let ast_unresolved::FunctionExpression::StringTemplate { parts, alias } =
                            t
                        {
                            let concat_expr =
                                super::super::helpers::build_concat_chain_with_placeholders(parts)?;
                            Ok(ast_resolved::FunctionExpression::Lambda {
                                body: Box::new(concat_expr),
                                alias,
                            })
                        } else {
                            super::super::functions::resolve_function_with_schema(
                                t, available, None,
                            )
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                resolved.push(ast_resolved::DomainExpression::PipedExpression {
                    value: Box::new(resolved_value),
                    transforms: resolved_transforms,
                    alias,
                });
            }
            ast_unresolved::DomainExpression::Parenthesized { inner, alias } => {
                let resolved_inner =
                    super::resolve_domain_expr_with_schema(*inner, available, cfe_defs)?;
                resolved.push(ast_resolved::DomainExpression::Parenthesized {
                    inner: Box::new(resolved_inner),
                    alias,
                });
            }
            ast_unresolved::DomainExpression::Tuple { elements, alias } => {
                // EPOCH 5: Resolve each element in the tuple
                let mut resolved_elements = Vec::new();
                for elem in elements {
                    resolved_elements.push(super::resolve_domain_expr_with_schema(
                        elem, available, cfe_defs,
                    )?);
                }
                resolved.push(ast_resolved::DomainExpression::Tuple {
                    elements: resolved_elements,
                    alias,
                });
            }
            ast_unresolved::DomainExpression::ScalarSubquery { .. } => {
                // ScalarSubquery requires schema and CTE context for resolution
                // This function (resolve_expressions_with_schema_internal) doesn't have that context
                // ScalarSubquery resolution should happen in resolve_domain_expr_with_full_context instead
                return Err(DelightQLError::ParseError {
                    message: "Scalar subqueries require full resolution context and cannot be used in this context".to_string(),
                    source: None,
                    subcategory: None,
                });
            }

            // Pivot: resolve both children, pass through pivot_values
            ast_unresolved::DomainExpression::PivotOf {
                value_column,
                pivot_key,
                pivot_values,
            } => {
                let resolved_value = super::resolve_domain_expr_with_schema_and_context(
                    *value_column,
                    available,
                    schema,
                    cte_context.as_deref_mut(),
                    in_correlation,
                    cfe_defs,
                    None,
                )?;
                let resolved_key =
                    super::resolve_domain_expr_with_schema(*pivot_key, available, cfe_defs)?;
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

fn resolve_string_template_for_projection(
    parts: Vec<ast_unresolved::StringTemplatePart>,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
) -> Result<ast_resolved::DomainExpression> {
    let mut resolved_parts: Vec<ast_resolved::StringTemplatePart<ast_resolved::Resolved>> =
        Vec::new();
    for part in parts {
        match part {
            ast_unresolved::StringTemplatePart::Text(text) => {
                resolved_parts
                    .push(ast_resolved::StringTemplatePart::<ast_resolved::Resolved>::Text(text));
            }
            ast_unresolved::StringTemplatePart::Interpolation(expr) => {
                let resolved_expr =
                    super::resolve_domain_expr_with_schema(*expr, available, cfe_defs)?;
                resolved_parts.push(
                    ast_resolved::StringTemplatePart::<ast_resolved::Resolved>::Interpolation(
                        Box::new(resolved_expr),
                    ),
                );
            }
        }
    }

    Ok(super::super::super::string_templates::build_concat_chain(
        resolved_parts,
        alias.map(|s| s.into()),
    ))
}

fn resolve_predicate_for_projection(
    expr: ast_unresolved::BooleanExpression,
    available: &[ast_resolved::ColumnMetadata],
    cfe_defs: Option<&HashMap<String, ast_unresolved::PrecompiledCfeDefinition>>,
    schema: Option<&dyn super::super::super::DatabaseSchema>,
    mut cte_context: Option<&mut HashMap<String, ast_resolved::CprSchema>>,
    in_correlation: bool,
) -> Result<ast_resolved::BooleanExpression> {
    match expr {
        ast_unresolved::BooleanExpression::Comparison {
            left,
            operator,
            right,
        } => {
            let resolved_left = super::resolve_domain_expr_with_schema(*left, available, cfe_defs)?;
            let resolved_right =
                super::resolve_domain_expr_with_schema(*right, available, cfe_defs)?;
            Ok(ast_resolved::BooleanExpression::Comparison {
                left: Box::new(resolved_left),
                operator,
                right: Box::new(resolved_right),
            })
        }
        ast_unresolved::BooleanExpression::GlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::GlobCorrelation { left, right })
        }
        ast_unresolved::BooleanExpression::OrdinalGlobCorrelation { left, right } => {
            Ok(ast_resolved::BooleanExpression::OrdinalGlobCorrelation { left, right })
        }
        ast_unresolved::BooleanExpression::Using { .. } => Err(DelightQLError::ParseError {
            message: "USING clause not supported in projections".to_string(),
            source: None,
            subcategory: None,
        }),
        ast_unresolved::BooleanExpression::In { .. }
        | ast_unresolved::BooleanExpression::InRelational { .. }
        | ast_unresolved::BooleanExpression::InnerExists { .. } => {
            // IN and EXISTS require schema and CTE context for resolution
            // Delegate to the full predicate resolver
            match (schema, cte_context.as_deref_mut()) {
                (Some(db_schema), Some(ctx)) => {
                    super::super::predicates::resolve_predicate_with_schema(
                        expr,
                        available,
                        db_schema,
                        ctx,
                        in_correlation,
                        cfe_defs,
                    )
                }
                _ => {
                    Err(DelightQLError::ParseError {
                        message: "IN/EXISTS predicates require full resolution context (schema and CTE access)".to_string(),
                        source: None,
                        subcategory: None,
                    })
                }
            }
        }
        ast_unresolved::BooleanExpression::And { left, right } => {
            // Recursively resolve both sides by wrapping in Predicate and resolving
            let left_pred = ast_unresolved::DomainExpression::Predicate {
                expr: left,
                alias: None,
            };
            let right_pred = ast_unresolved::DomainExpression::Predicate {
                expr: right,
                alias: None,
            };

            let resolved_left =
                super::resolve_domain_expr_with_schema(left_pred, available, cfe_defs)?;
            let resolved_right =
                super::resolve_domain_expr_with_schema(right_pred, available, cfe_defs)?;

            // Extract the boolean expressions back out
            let left_bool = match resolved_left {
                ast_resolved::DomainExpression::Predicate { expr, .. } => *expr,
                _ => unreachable!("Expected predicate after resolution"),
            };
            let right_bool = match resolved_right {
                ast_resolved::DomainExpression::Predicate { expr, .. } => *expr,
                _ => unreachable!("Expected predicate after resolution"),
            };

            Ok(ast_resolved::BooleanExpression::And {
                left: Box::new(left_bool),
                right: Box::new(right_bool),
            })
        }
        ast_unresolved::BooleanExpression::Or { left, right } => {
            let left_pred = ast_unresolved::DomainExpression::Predicate {
                expr: left,
                alias: None,
            };
            let right_pred = ast_unresolved::DomainExpression::Predicate {
                expr: right,
                alias: None,
            };

            let resolved_left =
                super::resolve_domain_expr_with_schema(left_pred, available, cfe_defs)?;
            let resolved_right =
                super::resolve_domain_expr_with_schema(right_pred, available, cfe_defs)?;

            // Extract the boolean expressions back out
            let left_bool = match resolved_left {
                ast_resolved::DomainExpression::Predicate { expr, .. } => *expr,
                _ => unreachable!("Expected predicate after resolution"),
            };
            let right_bool = match resolved_right {
                ast_resolved::DomainExpression::Predicate { expr, .. } => *expr,
                _ => unreachable!("Expected predicate after resolution"),
            };

            Ok(ast_resolved::BooleanExpression::Or {
                left: Box::new(left_bool),
                right: Box::new(right_bool),
            })
        }
        ast_unresolved::BooleanExpression::Not { expr } => {
            let inner_pred = ast_unresolved::DomainExpression::Predicate { expr, alias: None };

            let resolved_inner =
                super::resolve_domain_expr_with_schema(inner_pred, available, cfe_defs)?;

            // Extract the boolean expression back out
            let inner_bool = match resolved_inner {
                ast_resolved::DomainExpression::Predicate { expr, .. } => *expr,
                _ => unreachable!("Expected predicate after resolution"),
            };

            Ok(ast_resolved::BooleanExpression::Not {
                expr: Box::new(inner_bool),
            })
        }
        ast_unresolved::BooleanExpression::BooleanLiteral { value } => {
            Ok(ast_resolved::BooleanExpression::BooleanLiteral { value })
        }
        ast_unresolved::BooleanExpression::Sigma { .. } => {
            Err(crate::error::DelightQLError::not_implemented(
                "Sigma predicates in projections not yet supported",
            ))
        }
    }
}
