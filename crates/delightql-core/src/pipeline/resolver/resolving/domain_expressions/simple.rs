use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{ProjectionExpr, SubstitutionExpr};

/// Resolve simple expressions (lvars, literals, parameters, etc.)
pub(in crate::pipeline::resolver) fn resolve_simple_expr(
    expr: ast_unresolved::DomainExpression,
    available: &[ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::Lvar {
            name,
            qualifier,
            namespace_path,
            alias,
            provenance: _,
        } => resolve_lvar(
            name.to_string(),
            qualifier.map(|s| s.to_string()),
            namespace_path,
            alias.map(|s| s.to_string()),
            available,
            in_correlation,
        ),

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
                    context: "No columns available for ordinal resolution in predicate".to_string(),
                });
            }

            let idx = if ordinal.reverse {
                if ordinal.position as usize > candidates.len() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: format!("|-{}|", ordinal.position),
                        context: format!(
                            "Position {} from end exceeds {} available columns",
                            ordinal.position,
                            candidates.len()
                        ),
                    });
                }
                candidates.len() - ordinal.position as usize
            } else {
                if ordinal.position == 0 {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: "|0|".to_string(),
                        context: "Column positions start at 1".to_string(),
                    });
                }
                let pos = (ordinal.position - 1) as usize;
                if pos >= candidates.len() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: format!("|{}|", ordinal.position),
                        context: format!(
                            "Position {} exceeds {} available columns",
                            ordinal.position,
                            candidates.len()
                        ),
                    });
                }
                pos
            };

            let column = candidates[idx];
            // Use the actual column name (alias if present, otherwise original)
            // The ordinal's own alias takes precedence if specified
            let name = column.name().to_string();
            Ok(ast_resolved::DomainExpression::Lvar {
                name: name.into(),
                qualifier: ordinal.qualifier.clone().map(|s| s.into()).or_else(|| {
                    match &column.fq_table.name {
                        ast_resolved::TableName::Named(t) => Some(t.clone().into()),
                        _ => None,
                    }
                }),
                namespace_path: crate::pipeline::asts::resolved::NamespacePath::empty(),
                alias: ordinal.alias.clone().map(|s| s.into()),
                provenance: ast_resolved::PhaseBox::phantom(),
            })
        }

        ast_unresolved::DomainExpression::Literal { value, alias } => {
            Ok(ast_resolved::DomainExpression::Literal {
                value: super::super::super::helpers::converters::convert_literal_value(value),
                alias,
            })
        }

        ast_unresolved::DomainExpression::ValuePlaceholder { alias } => {
            Ok(ast_resolved::DomainExpression::ValuePlaceholder { alias })
        }

        ast_unresolved::DomainExpression::Substitution(sub) => match sub {
            SubstitutionExpr::Parameter { name, alias } => {
                // Parameters should only appear in CFE bodies
                Ok(ast_resolved::DomainExpression::Substitution(
                    SubstitutionExpr::Parameter { name, alias },
                ))
            }
            SubstitutionExpr::CurriedParameter { name, alias } => {
                // Curried parameters should only appear in HOCFE bodies
                Ok(ast_resolved::DomainExpression::Substitution(
                    SubstitutionExpr::CurriedParameter { name, alias },
                ))
            }
            SubstitutionExpr::ContextParameter { .. } => {
                // ContextParameter should never exist in unresolved phase - it's only created during
                // postprocessing in refined phase for CCAFE feature
                Err(DelightQLError::ParseError {
                    message: "ContextParameter should not appear in unresolved phase".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            SubstitutionExpr::ContextMarker => {
                // ContextMarker (..) passes through to resolved phase unchanged
                // It's used in function call arguments for CCAFE
                Ok(ast_resolved::DomainExpression::Substitution(
                    SubstitutionExpr::ContextMarker,
                ))
            }
        },

        ast_unresolved::DomainExpression::NonUnifiyingUnderscore => {
            Ok(ast_resolved::DomainExpression::NonUnifiyingUnderscore)
        }

        _ => unreachable!("resolve_simple_expr called with non-simple expression"),
    }
}

/// Resolve expressions that are only valid in projection contexts
pub(in crate::pipeline::resolver) fn resolve_projection_only_expr(
    expr: ast_unresolved::DomainExpression,
) -> Result<ast_resolved::DomainExpression> {
    match expr {
        ast_unresolved::DomainExpression::Projection(ref proj) => match proj {
            ProjectionExpr::ColumnRange(_) => {
                Err(DelightQLError::ParseError {
                    message: "Column ranges can only be used in projections, not in predicates or single-value contexts".to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            ProjectionExpr::Glob { .. } => {
                Err(DelightQLError::ParseError {
                    message:
                        "Glob expressions can only be used in projections, not in single-value contexts"
                            .to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            ProjectionExpr::Pattern { .. } => {
                Err(DelightQLError::ParseError {
                    message:
                        "Pattern expressions can only be used in projections, not in single-value contexts"
                            .to_string(),
                    source: None,
                    subcategory: None,
                })
            }
            ProjectionExpr::JsonPathLiteral { .. } => {
                Err(DelightQLError::ParseError {
                    message:
                        "JsonPathLiteral expressions can only be used in projections, not in single-value contexts"
                            .to_string(),
                    source: None,
                    subcategory: None,
                })
            }
        },
        _ => unreachable!("resolve_projection_only_expr called with non-projection-only expression"),
    }
}

fn resolve_lvar(
    name: String,
    qualifier: Option<String>,
    namespace_path: crate::pipeline::asts::unresolved::NamespacePath,
    alias: Option<String>,
    available: &[ast_resolved::ColumnMetadata],
    in_correlation: bool,
) -> Result<ast_resolved::DomainExpression> {
    // Validate column references based on context:
    // - In regular contexts: always validate
    // - In correlation contexts:
    //   - Skip unqualified refs (might be correlation params)
    //   - For qualified refs: validate ONLY if qualifier matches a table in available
    //     (if qualifier is unknown, might be from interdependent EXISTS)
    //
    // We validate by checking if the column NAME exists, stripping the qualifier.
    log::debug!(
        "Resolving Lvar '{}', qualifier={:?}, in_correlation={}, available_count={}",
        name,
        qualifier,
        in_correlation,
        available.len()
    );
    let should_validate = if available.is_empty() {
        // No columns available — source has unknown schema (TVFs, passthrough).
        // Skip validation; the backend validates at runtime.
        // Real tables always have at least one column, so empty available
        // can only come from CprSchema::Unknown. See memory/semantic-onion.md.
        log::debug!("  -> should_validate=false (unknown schema, no available columns)");
        false
    } else if !in_correlation {
        // Not in correlation - validate everything
        log::debug!("  -> should_validate=true (not in correlation)");
        true
    } else if qualifier.is_none() {
        // In correlation, unqualified - skip (might be correlation param)
        log::debug!("  -> should_validate=false (in correlation + unqualified)");
        false
    } else {
        // In correlation, qualified - validate only if qualifier is in available
        let qual_name = qualifier.as_ref().unwrap();
        let qualifier_known = available.iter().any(|col| match &col.fq_table.name {
            ast_resolved::TableName::Named(t) => t == qual_name,
            ast_resolved::TableName::Fresh => false,
        });
        if !qualifier_known {
            // Qualifier doesn't match any table in scope. Check if the column
            // name exists unqualified under a *Named* table — if so, the
            // qualifier is definitely bogus. If the column only exists under
            // Fresh tables, the qualifier might be the original table name that
            // the resolver doesn't track; let it through for SQL-level resolution.
            let col_under_named = available.iter().any(|col| {
                col.info.original_name() == Some(&name)
                    && matches!(col.fq_table.name, ast_resolved::TableName::Named(_))
            });
            if col_under_named {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: format!("{}.{}", qual_name, name),
                    context: format!(
                        "Qualifier '{}' does not match any table in scope. \
                         Column '{}' exists but not under that qualifier",
                        qual_name, name
                    ),
                });
            }
        }
        qualifier_known
    };

    if should_validate {
        use super::super::super::unification::{unify_columns, ColumnReference, UnificationResult};

        let schema = namespace_path.first().map(|s| s.to_string());

        // Detect post-pipe context: all available columns are Fresh.
        // After a pipe boundary, qualifiers referring to the original table are stale
        // and must be rejected. In other contexts (joins, subqueries), qualifiers may
        // not match the resolver's internal table names but are valid at SQL level,
        // so we preserve the lenient strip-and-validate behavior.
        let all_fresh = qualifier.is_some()
            && qualifier.as_ref().unwrap() != "_"
            && !available.is_empty()
            && available
                .iter()
                .all(|col| matches!(col.fq_table.name, ast_resolved::TableName::Fresh));

        if all_fresh {
            // Post-pipe context: qualifier is definitely stale.
            let unqual_ref = ColumnReference::Named {
                name: name.clone(),
                qualifier: None,
                schema,
            };
            let result = unify_columns(vec![unqual_ref], available)
                .into_iter()
                .next()
                .unwrap();

            match result {
                UnificationResult::Resolved(_) | UnificationResult::Ambiguous { .. } => {
                    // Column exists but qualifier is stale after pipe boundary
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: format!("{}.{}", qualifier.as_ref().unwrap(), name),
                        context: format!(
                            "Qualifier '{}' is not in scope after pipe boundary. Use unqualified '{}'",
                            qualifier.as_ref().unwrap(),
                            name
                        ),
                    });
                }
                UnificationResult::Unresolved(col_name) => {
                    return Err(DelightQLError::column_not_found_error(
                        col_name,
                        "in domain expression",
                    ));
                }
            }
        } else {
            // Non-pipe context or unqualified: validate column name exists,
            // stripping qualifier (existing lenient behavior).
            let col_ref = ColumnReference::Named {
                name: name.clone(),
                qualifier: None,
                schema,
            };

            let results = unify_columns(vec![col_ref], available);

            for result in results {
                match result {
                    UnificationResult::Resolved(_) => {
                        break;
                    }
                    UnificationResult::Unresolved(col_name) => {
                        return Err(DelightQLError::column_not_found_error(
                            col_name,
                            "in domain expression",
                        ));
                    }
                    UnificationResult::Ambiguous { column, tables } => {
                        if qualifier.is_some() {
                            // Qualified reference disambiguates at SQL level
                            break;
                        }
                        return Err(DelightQLError::validation_error_categorized(
                            "resolution/ambiguous",
                            format!(
                                "Ambiguous column '{}' exists in tables: {}. Use a qualifier (e.g., {}.{})",
                                column,
                                tables.join(", "),
                                tables[0],
                                column,
                            ),
                            "in domain expression",
                        ));
                    }
                }
            }
        }
    }

    Ok(ast_resolved::DomainExpression::Lvar {
        name: name.into(),
        qualifier: qualifier.map(|s| s.into()),
        namespace_path,
        alias: alias.map(|s| s.into()),
        provenance: ast_resolved::PhaseBox::phantom(),
    })
}
