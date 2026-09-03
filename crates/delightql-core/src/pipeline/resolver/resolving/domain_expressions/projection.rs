// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_resolved;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::literals::{column_ordinal_text, column_range_text};
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence};
use crate::pipeline::asts::core::{Glob, RegexSelector, Spread};
use crate::pipeline::asts::core::{NamedReference, Reference};

pub(in crate::pipeline::resolver) type PendingOutItem = crate::relation::pending::Position;

fn expand_glob(
    qualifier: Option<delightql_types::SqlIdentifier>,
    position: &crate::pipeline::resolver::Position<'_>,
    registry: &crate::relation::Planning,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    // THE FRONTIER ANSWERS THE GLOB: its own heading bare, or what the
    // qualifier reaches, each as the occurrence the glob addresses.
    let columns = match qualifier {
        None => position.heading(registry)?,
        Some(qualifier) => position.qualified_glob(&qualifier, registry)?,
    };
    Ok(columns
        .into_iter()
        .map(|occurrence| {
            ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(occurrence)))
        })
        .collect())
}

fn expand_pattern(
    pattern: &str,
    position: &crate::pipeline::resolver::Position<'_>,
    allow_zero_matches: bool,
    registry: &crate::relation::Planning,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    use crate::pipeline::pattern::bre_to_rust_regex;
    let regex_pattern = bre_to_rust_regex(pattern)?;

    let re = regex::Regex::new(&regex_pattern)
        .map_err(|e| DelightQLError::parse_error(format!("Invalid column pattern: {}", e)))?;

    // THE FRONTIER ANSWERS THE SPREAD: the positions of the heading in
    // view whose published names the pattern matches.
    let columns: Vec<_> = position
        .spread(|name| re.is_match(name), registry)?
        .into_iter()
        .map(|occurrence| {
            ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(occurrence)))
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
    available: &[crate::relation::PortId],
    allow_zero_pattern_matches: bool,
) -> Result<Vec<ast_resolved::DomainExpression>> {
    match spread {
        Spread::Glob(Glob { qualifier, .. }) => {
            let columns = expand_glob(qualifier.clone(), &fold.lexical, &fold.core.identities)?;
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
            &fold.lexical,
            allow_zero_pattern_matches,
            &fold.core.identities,
        ),
        Spread::PositionalSpan(range) => expand_range(fold, range, available),
    }
}

/// The columns a positional span covers.
fn expand_range(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    range: &ast_unresolved::ColumnRange,
    available: &[crate::relation::PortId],
) -> Result<Vec<ast_resolved::DomainExpression>> {
    // A qualified range is a qualified glob narrowed by position — the same
    // tiers, or `u|1..2|` and `u.*` reach different columns one character
    // apart.
    let _ = available;
    let candidates = fold
        .lexical
        .in_order(range.qualifier.as_ref(), &fold.core.identities)?;

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
                candidates[idx].clone(),
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
    available: &[crate::relation::PortId],
    allow_zero_pattern_matches: bool,
) -> Result<(
    Vec<ast_resolved::SelectorItem>,
    Vec<crate::relation::PortId>,
)> {
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
            // A SELECTOR ADDRESSES THE HEADING STANDING HERE. A qualified
            // item reaches the lexical binding, whose position a step above
            // it republished; the construction record says which position
            // that became, and it is the one a removal removes.
            let Some(column) =
                crate::relation::landed_in(&fold.core.identities, available, column)?
            else {
                return Err(DelightQLError::transformation_error(
                    "a selector item addresses a position this heading does not stand on",
                    "selector",
                ));
            };
            if columns.contains(&column) {
                continue;
            }
            columns.push(column);
            items.push(ast_resolved::SelectorItem::Reference(
                crate::pipeline::asts::core::Reference::Named(
                    // THE SAME REFERENCE, standing on the heading's own
                    // position: an occurrence the item addressed follows
                    // its position; a spread's match is the engine's own.
                    crate::pipeline::asts::core::NamedReference(match &reference {
                        crate::pipeline::asts::core::Reference::Named(
                            crate::pipeline::asts::core::NamedReference(occurrence),
                        ) => occurrence.rebound(column),
                        _ => ColumnOccurrence::engine(column),
                    }),
                ),
            ));
        }
    }
    Ok((items, columns))
}

/// The occurrence a resolved reference names.
pub(in crate::pipeline::resolver) fn reference_column(
    reference: &crate::pipeline::asts::core::Reference<crate::pipeline::asts::core::Resolved>,
) -> Option<crate::relation::PortId> {
    match reference {
        crate::pipeline::asts::core::Reference::Named(
            crate::pipeline::asts::core::NamedReference(ColumnOccurrence { column, .. }),
        ) => Some(*column),
        crate::pipeline::asts::core::Reference::Ordinal(ordinal) => match *ordinal {},
        crate::pipeline::asts::core::Reference::Physical(_) => None,
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
    available: &[crate::relation::PortId],
) -> Result<Vec<crate::relation::pending::Reduction>> {
    let mut resolved = Vec::new();
    for item in items {
        match item {
            ast_unresolved::ReductionItem::Out(item) => {
                for out in resolve_out_items_via_fold(fold, vec![item], available, false)? {
                    resolved.push(crate::relation::pending::Reduction::Out(out));
                }
            }
            ast_unresolved::ReductionItem::Metadata(metadata) => {
                resolved.push(crate::relation::pending::Reduction::Metadata {
                    group: fold.resolve_metadata_group(metadata.group)?,
                    naming: metadata.naming,
                })
            }
            // THE IN IS THE HEADING WITNESS, and it is read after this: the
            // pivot's two expressions resolve here, its values where the
            // group's membership predicates are in scope.
            ast_unresolved::ReductionItem::Pivot(pivot) => {
                resolved.push(crate::relation::pending::Reduction::Pivot(
                    crate::pipeline::asts::core::PivotSpec {
                        value_column: Box::new(fold.transform_domain(*pivot.value_column)?),
                        pivot_key: Box::new(fold.transform_domain(*pivot.pivot_key)?),
                        values: pivot.values,
                    },
                ))
            }
            // A delegate resolves at the group boundary that owns it, where
            // its payload's outputs publish AFTER every other reduction;
            // this general road resolves the members it carries.
            ast_unresolved::ReductionItem::Delegate(_) => {
                unreachable!("group resolution partitions delegates before reduction events")
            }
        }
    }
    Ok(resolved)
}

pub(in crate::pipeline::resolver) fn resolve_out_items_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    items: Vec<ast_unresolved::OutItem>,
    available: &[crate::relation::PortId],
    allow_zero_pattern_matches: bool,
) -> Result<Vec<PendingOutItem>> {
    let mut resolved = Vec::new();
    for item in items {
        match item {
            ast_unresolved::OutItem::Many(spread) => {
                for expr in expand_spread(fold, &spread, available, allow_zero_pattern_matches)? {
                    resolved.push(PendingOutItem::Expanded { expr, naming: None });
                }
            }
            // The compiler's own whole-operand item passes through: there
            // is no heading question in it for resolution to answer.
            ast_unresolved::OutItem::Whole => resolved.push(PendingOutItem::Whole),
            ast_unresolved::OutItem::One(one) => {
                let (expr, naming) = (one.expr, one.naming);
                // ONE ITEM, ONE VALUE — by type. Neither a domain value nor
                // a crossing admits an enumerating form, so this road cannot
                // fan out and no name is published across more than one
                // column.
                resolved.push(PendingOutItem::Authored {
                    expr: resolve_out_value_via_fold(fold, expr, available)?,
                    naming,
                });
            }
        }
    }
    Ok(resolved)
}

/// Resolve a list of domain expressions via the fold walker, expanding globs/patterns/ranges/ordinals
/// structurally but using `fold.transform_domain()` for actual expression resolution.
/// Resolve one PUBLISHED value.
pub(in crate::pipeline::resolver) fn resolve_out_value_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    value: ast_unresolved::DomainExpression,
    available: &[crate::relation::PortId],
) -> Result<ast_resolved::DomainExpression> {
    let mut values = resolve_expressions_via_fold(fold, vec![value], available)?;
    Ok(values
        .pop()
        .expect("one value resolves to exactly one value"))
}

pub(in crate::pipeline::resolver) fn resolve_expressions_via_fold(
    fold: &mut crate::pipeline::resolver::resolver_fold::ResolverFold,
    expressions: Vec<ast_unresolved::DomainExpression>,
    available: &[crate::relation::PortId],
) -> Result<Vec<ast_resolved::DomainExpression>> {
    let mut resolved = Vec::new();

    for expr in expressions {
        match expr {
            ast_unresolved::DomainExpression::Reference(Reference::Ordinal(ordinal)) => {
                // A qualified ordinal is a qualified glob narrowed by
                // position — the same tiers, or `u|1|` and `u.*` reach
                // different columns one character apart.
                let candidates = fold
                    .lexical
                    .in_order(ordinal.qualifier.as_ref(), &fold.core.identities)?;

                if candidates.is_empty() {
                    return Err(DelightQLError::ColumnNotFoundError {
                        column: column_ordinal_text(ordinal.position, false),
                        context: "No columns available for ordinal resolution".to_string(),
                    });
                }

                let idx = calculate_ordinal_index(&ordinal, candidates.len())?;
                resolved.push(ast_resolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(candidates[idx].clone()),
                )));
            }
            ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
                AuthoredColumn {
                    name,
                    qualifier,
                    namespace_path,
                },
            ))) => {
                // THE ACTIVE FORMAL FRAME ANSWERS FIRST, exactly as it does
                // on the fold's own domain road: an unqualified name a
                // definition body declares as a parameter is the
                // caller-resolved actual, never a column of the source.
                if qualifier.is_none() && namespace_path.is_empty() {
                    if let Some(bound) = fold.env.formal_value(&name) {
                        resolved.push(crate::pipeline::resolver::resolver_fold::anchor_formal(
                            &fold.core.identities,
                            available,
                            bound,
                        )?);
                        continue;
                    }
                }
                // Simple lvar resolution — no registry needed, same as existing
                let lvar_expr = ast_unresolved::DomainExpression::Reference(Reference::Named(
                    NamedReference(AuthoredColumn {
                        name,
                        qualifier,
                        namespace_path,
                    }),
                ));
                let in_correlation = fold.in_correlation;
                resolved.push(super::simple::resolve_simple_expr(
                    lvar_expr,
                    &fold.lexical,
                    in_correlation,
                    &mut fold.correlation_witness,
                    &fold.core.identities,
                )?);
            }
            ast_unresolved::DomainExpression::Reference(Reference::Physical(_)) => {
                unreachable!("physical references cannot enter resolution")
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
