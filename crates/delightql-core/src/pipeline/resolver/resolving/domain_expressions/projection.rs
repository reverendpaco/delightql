// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::literals::{column_ordinal_text, column_range_text};
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use crate::pipeline::asts::core::{Glob, RegexSelector, Spread};
use crate::pipeline::asts::core::{NamedReference, Reference};

fn expand_glob(
    qualifier: Option<delightql_types::SqlIdentifier>,
    available: &[crate::names::ColId],
    registry: &crate::names::Registry,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let explicit_qualifier = qualifier.is_some();
    let columns = qualifier.map_or_else(
        || available.to_vec(),
        |qualifier| {
            // As written: a stropped qualifier names a case-sensitive scope,
            // and folding it here would look for a scope nobody named.
            let spelling = registry.intern(qualifier.as_str(), qualifier.is_stropped());
            registry
                .qualified_glob(registry.canonical(spelling), available)
                .to_vec()
        },
    );
    Ok(columns
        .into_iter()
        .map(|column| {
            ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence {
                    column,
                    explicit_qualifier,
                },
            )))
        })
        .collect())
}

fn expand_pattern(
    pattern: &str,
    available: &[crate::names::ColId],
    allow_zero_matches: bool,
    registry: &crate::names::Registry,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    use crate::pipeline::pattern::bre_to_rust_regex;
    let regex_pattern = bre_to_rust_regex(pattern)?;

    // Create regex for matching
    let re = regex::Regex::new(&regex_pattern)
        .map_err(|e| DelightQLError::parse_error(format!("Invalid column pattern: {}", e)))?;

    let columns: Vec<_> = registry
        .pattern_columns(&re, available)
        .into_iter()
        .map(|column| {
            ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence {
                    column,
                    explicit_qualifier: false,
                },
            )))
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
                column: column_ordinal_text(ordinal.position, true),
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
                column: column_ordinal_text(0, false),
                context: "Column positions start at 1".to_string(),
            });
        }
        let pos = (ordinal.position - 1) as usize;
        if pos >= total_cols {
            return Err(DelightQLError::ColumnNotFoundError {
                column: column_ordinal_text(ordinal.position, false),
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
                    column: column_range_text(Some((pos, true)), None),
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
                    column: column_range_text(Some((0, false)), None),
                    context: "Column positions start at 1".to_string(),
                });
            }
            let idx = (pos - 1) as usize;
            if idx >= total_cols {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: column_range_text(Some((pos, false)), None),
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
                    column: column_range_text(None, Some((pos, true))),
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
                    column: column_range_text(None, Some((0, false))),
                    context: "Column positions start at 1".to_string(),
                });
            }
            let idx = (pos - 1) as usize;
            if idx >= total_cols {
                return Err(DelightQLError::ColumnNotFoundError {
                    column: column_range_text(None, Some((pos, false))),
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
    column_range_text(range.start, range.end)
}

/// The columns a spread covers, as resolved references.
///
/// One authority for what enumeration means, whether the spread stands in a
/// publication position or in a selector.
pub(in crate::pipeline::resolver) fn expand_spread(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    spread: &Spread<crate::pipeline::asts::core::Unresolved>,
    available: &[crate::names::ColId],
    allow_zero_pattern_matches: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    match spread {
        Spread::Glob(Glob { qualifier, .. }) => {
            let columns = expand_glob(qualifier.clone(), available, &fold.registry.identities)?;
            if columns.is_empty() && qualifier.is_some() {
                return Err(DelightQLError::validation_error(
                    format!(
                        "Qualified glob '{}.*' matched no columns - table or alias not in scope",
                        qualifier.as_ref().expect("a qualifier was just observed")
                    ),
                    "Check that the qualifier matches a table name or alias in the query"
                        .to_string(),
                ));
            }
            Ok(columns)
        }
        Spread::Regex(RegexSelector { pattern, .. }) => expand_pattern(
            pattern,
            available,
            allow_zero_pattern_matches,
            &fold.registry.identities,
        ),
        Spread::PositionalSpan(range) => expand_range(fold, range, available),
    }
}

/// The columns a positional span covers.
fn expand_range(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    range: &ast_unresolved::ColumnRange,
    available: &[crate::names::ColId],
) -> Result<Vec<ast_resolved::DomainExpression>> {
    // A qualified range is a qualified glob narrowed by position — the same
    // tiers, or `u|1..2|` and `u.*` reach different columns one character
    // apart.
    let candidates = if let Some(qual) = &range.qualifier {
        let spelling = fold
            .registry
            .identities
            .intern(qual.as_str(), qual.is_stropped());
        let qualifier = fold.registry.identities.canonical(spelling);
        fold.registry
            .identities
            .qualified_glob(qualifier, available)
    } else {
        crate::names::Candidates::from_vec(available.to_vec())
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

    Ok((start_idx..=end_idx)
        .map(|idx| {
            ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                ColumnOccurrence {
                    column: *candidates
                        .in_order()
                        .nth(idx)
                        .expect("validated range index is in candidate order"),
                    explicit_qualifier: range.qualifier.is_some(),
                },
            )))
        })
        .collect())
}

/// THE ONE SELECTOR EXPANSION. Every enumerating operator — project-out,
/// map cover, embed map cover — asks the same question of the same
/// authority, so a spread reaches the same columns in each and a resolved
/// selector holds nothing but references.
///
/// The same occurrence twice is one selection: two overlapping regexes name
/// the same column once, and the first position it was named at survives.
pub(in crate::pipeline::resolver) fn resolve_selector_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    selector: Vec<ast_unresolved::SelectorItem>,
    available: &[crate::names::ColId],
    allow_zero_pattern_matches: bool,
) -> Result<(Vec<ast_resolved::SelectorItem>, Vec<crate::names::ColId>)> {
    let mut items = Vec::new();
    let mut columns = Vec::new();
    for item in selector {
        let expanded = match item {
            ast_unresolved::SelectorItem::Spread(spread) => {
                expand_spread(fold, &spread, available, allow_zero_pattern_matches)?
            }
            ast_unresolved::SelectorItem::Reference(reference) => resolve_expressions_via_fold(
                fold,
                vec![ast_unresolved::DomainExpression::Reference(reference)],
                available,
            )?,
        };
        for expression in expanded {
            let ast_resolved::DomainExpression::Reference(reference) = expression else {
                return Err(DelightQLError::transformation_error(
                    "a selector addresses columns, and this item resolved to a value",
                    "selector",
                ));
            };
            let Some(column) = reference_column(&reference) else {
                return Err(DelightQLError::transformation_error(
                    "a selector item resolved to an address with no occurrence",
                    "selector",
                ));
            };
            if columns.contains(&column) {
                continue;
            }
            columns.push(column);
            items.push(ast_resolved::SelectorItem::Reference(reference));
        }
    }
    Ok((items, columns))
}

/// The occurrence a resolved reference names.
pub(in crate::pipeline::resolver) fn reference_column(
    reference: &crate::pipeline::asts::core::Reference<crate::pipeline::asts::core::Resolved>,
) -> Option<crate::names::ColId> {
    match reference {
        crate::pipeline::asts::core::Reference::Named(
            crate::pipeline::asts::core::NamedReference(ColumnOccurrence { column, .. }),
        ) => Some(*column),
        crate::pipeline::asts::core::Reference::Ordinal(ordinal) => match *ordinal {},
    }
}

/// Resolve a list of PUBLICATION ITEMS. A spread expands into the several
/// one-value items it covers, and each keeps no naming — there was none to
/// keep, because the type has no field for one.
/// Resolve a REDUCTION list. A metadata group resolves its key against the
/// enclosing relation and its target as an ordinary construction; every
/// other item is an out item, resolved the way every publication position
/// resolves one.
pub(in crate::pipeline::resolver) fn resolve_reduction_items_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    items: Vec<ast_unresolved::ReductionItem>,
    available: &[crate::names::ColId],
) -> Result<Vec<ast_resolved::ReductionItem>> {
    let mut resolved = Vec::new();
    for item in items {
        match item {
            ast_unresolved::ReductionItem::Out(item) => {
                for out in resolve_out_items_via_fold(fold, vec![item], available, false)? {
                    resolved.push(ast_resolved::ReductionItem::Out(out));
                }
            }
            ast_unresolved::ReductionItem::Metadata(metadata) => resolved.push(
                ast_resolved::ReductionItem::Metadata(ast_resolved::MetadataOut {
                    group: fold.resolve_metadata_group(metadata.group)?,
                    naming: metadata.naming,
                    output: None,
                }),
            ),
            // THE IN IS THE HEADING WITNESS, and it is read after this: the
            // pivot's two expressions resolve here, its values where the
            // group's membership predicates are in scope.
            ast_unresolved::ReductionItem::Pivot(pivot) => resolved.push(
                ast_resolved::ReductionItem::Pivot(crate::pipeline::asts::core::PivotSpec {
                    value_column: Box::new(fold.transform_domain(*pivot.value_column)?),
                    pivot_key: Box::new(fold.transform_domain(*pivot.pivot_key)?),
                    values: pivot.values,
                }),
            ),
            // A delegate resolves at the group boundary that owns it, where
            // its payload's outputs publish AFTER every other reduction;
            // this general road resolves the members it carries.
            ast_unresolved::ReductionItem::Delegate(delegate) => resolved.push(
                ast_resolved::ReductionItem::Delegate(ast_resolved::DelegateSpec {
                    payload: resolve_out_items_via_fold(
                        fold,
                        delegate.payload,
                        available,
                        false,
                    )?,
                    order: delegate
                        .order
                        .into_iter()
                        .map(|ordering| {
                            resolve_expressions_via_fold(fold, vec![ordering.column], available)
                                .map(|mut expressions| ast_resolved::OrderingSpec {
                                    column: expressions.pop().expect(
                                        "one ordering expression resolves to one expression",
                                    ),
                                    direction: crate::pipeline::resolver::helpers::converters::convert_order_direction(
                                        ordering.direction,
                                    ),
                                })
                        })
                        .collect::<Result<Vec<_>>>()?,
                }),
            ),
        }
    }
    Ok(resolved)
}

pub(in crate::pipeline::resolver) fn resolve_out_items_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    items: Vec<ast_unresolved::OutItem>,
    available: &[crate::names::ColId],
    allow_zero_pattern_matches: bool,
) -> Result<Vec<ast_resolved::OutItem>> {
    let mut resolved = Vec::new();
    for item in items {
        match item {
            ast_unresolved::OutItem::Many(spread) => {
                for expr in expand_spread(fold, &spread, available, allow_zero_pattern_matches)? {
                    resolved.push(ast_resolved::OutItem::One(ast_resolved::OneOut {
                        expr: OutValue::Domain(expr),
                        naming: None,
                        output: None,
                    }));
                }
            }
            // The compiler's own whole-operand item passes through: there
            // is no heading question in it for resolution to answer.
            ast_unresolved::OutItem::Whole => resolved.push(ast_resolved::OutItem::Whole),
            ast_unresolved::OutItem::One(one) => {
                let ast_unresolved::OneOut {
                    expr,
                    naming,
                    output: (),
                } = one;
                // ONE ITEM, ONE VALUE — by type. Neither a domain value nor
                // a crossing admits an enumerating form, so this road cannot
                // fan out and no name is published across more than one
                // column.
                resolved.push(ast_resolved::OutItem::One(ast_resolved::OneOut {
                    expr: resolve_out_value_via_fold(fold, expr, available)?,
                    naming,
                    output: None,
                }));
            }
        }
    }
    Ok(resolved)
}

/// Resolve a list of domain expressions via the fold walker, expanding globs/patterns/ranges/ordinals
/// structurally but using `fold.transform_domain()` for actual expression resolution.
/// Resolve one PUBLISHED value. A domain value takes the ordinary road; a
/// crossing resolves its truth through the same fold, so both sides of the
/// adapter see the same scope.
pub(in crate::pipeline::resolver) fn resolve_out_value_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    value: ast_unresolved::OutValue,
    available: &[crate::names::ColId],
) -> Result<ast_resolved::OutValue> {
    use crate::pipeline::ast_transform::AstTransform;
    Ok(match value {
        ast_unresolved::OutValue::Domain(domain) => {
            let mut values = resolve_expressions_via_fold(fold, vec![domain], available)?;
            ast_resolved::OutValue::Domain(
                values
                    .pop()
                    .expect("one value resolves to exactly one value"),
            )
        }
        ast_unresolved::OutValue::Truth(crossing) => {
            ast_resolved::OutValue::Truth(crate::pipeline::asts::core::TruthAsValue(
                fold.transform_boolean(crossing.into_truth())?,
            ))
        }
    })
}

pub(in crate::pipeline::resolver) fn resolve_expressions_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[crate::names::ColId],
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let mut resolved = Vec::new();

    for expr in expressions {
        match expr {
            ast_unresolved::DomainExpression::Reference(Reference::Ordinal(ordinal)) => {
                // A qualified ordinal is a qualified glob narrowed by
                // position — the same tiers, or `u|1|` and `u.*` reach
                // different columns one character apart.
                let candidates = if let Some(qual) = &ordinal.qualifier {
                    let spelling = fold
                        .registry
                        .identities
                        .intern(qual.as_str(), qual.is_stropped());
                    let qualifier = fold.registry.identities.canonical(spelling);
                    fold.registry
                        .identities
                        .qualified_glob(qualifier, available)
                } else {
                    crate::names::Candidates::from_vec(available.to_vec())
                };

                if candidates.is_empty() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: column_ordinal_text(ordinal.position, false),
                        context: "No columns available for ordinal resolution".to_string(),
                    });
                }

                let idx = calculate_ordinal_index(&ordinal, candidates.len())?;
                resolved.push(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(ColumnOccurrence {
                        column: *candidates
                            .in_order()
                            .nth(idx)
                            .expect("validated ordinal index is in candidate order"),
                        explicit_qualifier: ordinal.qualifier.is_some(),
                        // As written, exactly as the other ordinal road: one
                        // publication cannot be case-sensitive on one and folded
                        // on the other.
                    }),
                )));
            }
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier,
                    namespace_path,
                },
            ))) => {
                // Simple lvar resolution — no registry needed, same as existing
                let available_clone = available.to_vec();
                let lvar_expr = ast_unresolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(AuthoredColumn {
                        name,
                        qualifier,
                        namespace_path,
                    }),
                ));
                let local = fold.local_available.clone();
                let qualifiers = fold.qualifier_scope.clone();
                let in_correlation = fold.in_correlation;
                resolved.push(super::simple::resolve_simple_expr(
                    lvar_expr,
                    &available_clone,
                    &local,
                    &qualifiers,
                    in_correlation,
                    &mut fold.correlation_witness,
                    &fold.registry.identities,
                )?);
            }
            // An open body's leaf travels to instantiation: the position
            // The applying position spends an open leaf during resolution;
            // transform_domain is where a standing cover cell substitutes,
            // and where a leaf outside any applying position refuses.
            expr @ ast_unresolved::DomainExpression::Application(
                ast_unresolved::FunctionApplication::Open(_),
            ) => {
                resolved.push(fold.transform_domain(expr)?);
            }
            ast_unresolved::DomainExpression::Application(func) => {
                // Delegate through transform_domain which handles StringTemplate→concat
                resolved.push(
                    fold.transform_domain(ast_unresolved::DomainExpression::Application(func))?,
                );
            }
        }
    }

    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_range_end_refusal_echoes_the_authored_minus() {
        let range = ast_unresolved::ColumnRange {
            start: None,
            end: Some((99, true)),
            qualifier: None,
            namespace_path: Default::default(),
        };

        let error = calculate_range_end(&range, 3).unwrap_err();
        let DelightQLError::ColumnNotFoundError { column, .. } = error else {
            panic!("expected an out-of-range column refusal");
        };
        assert_eq!(column, "|:-99|");
    }
}
