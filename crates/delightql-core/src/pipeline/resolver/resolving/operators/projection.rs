// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use super::super::column_extraction::{extract_provided_column_for_item, mint_projection_scope};
use crate::error::{DelightQLError, Result};
use crate::names::ColId;
use crate::pipeline::asts::core::literals::column_ordinal_text;
use crate::pipeline::asts::core::AuthoredColumn;
use crate::pipeline::asts::core::{Glob, Spread};
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};
use delightql_types::SqlIdentifier;

/// The name a publication item asks its output to answer to, for the
/// duplicate-name laws. A named item says it outright; an unnamed reference
/// says its own spelling, and an unnamed ordinal says its written position.
///
/// Only the authored label is wanted here — what the output is finally called
/// is the registry's answer, read back from the minted occurrence.
fn authored_label(item: &ast_unresolved::OutItem) -> Option<&SqlIdentifier> {
    let ast_unresolved::OutItem::One(one) = item else {
        return None;
    };
    one.naming
        .as_ref()
        .or_else(|| one.expr.domain().and_then(bare_name))
}

fn bare_name(expr: &ast_unresolved::DomainExpression) -> Option<&SqlIdentifier> {
    match expr {
        ast_unresolved::DomainExpression::Reference(Reference::Named(NamedReference(
            AuthoredColumn { name, .. },
        ))) => Some(name),
        _ => None,
    }
}

/// An unnamed ordinal's label is the position as WRITTEN — `|2|`, `u|-1|` —
/// because that is what a duplicate refusal has to echo back.
fn ordinal_label(item: &ast_unresolved::OutItem) -> Option<String> {
    let ast_unresolved::OutItem::One(one) = item else {
        return None;
    };
    if one.naming.is_some() {
        return None;
    }
    one.expr.domain().and_then(written_ordinal)
}

fn written_ordinal(expr: &ast_unresolved::DomainExpression) -> Option<String> {
    match expr {
        ast_unresolved::DomainExpression::Reference(Reference::Ordinal(ordinal)) => {
            let position = column_ordinal_text(ordinal.position, ordinal.reverse);
            Some(
                ordinal
                    .qualifier
                    .as_ref()
                    .map_or(position.clone(), |qualifier| {
                        format!("{qualifier}{position}")
                    }),
            )
        }
        _ => None,
    }
}

/// Whether the ENGINE decides this item's names rather than the programmer.
/// A spread's expansion answers to the names its sources already publish.
fn engine_managed(item: &ast_unresolved::OutItem) -> bool {
    matches!(item, ast_unresolved::OutItem::Many(_))
}

/// Resolve the General projection operator via fold-based dispatch.
/// EMBED IS EXTENSION: the operand's whole heading rides in front of the
/// added items, prefixed HERE — the one shared projection algorithm — so
/// the two spellings cannot drift and the resolved carrier still says
/// which was authored.
pub(super) fn resolve_embed_via_fold(
    fold: &mut ResolverFold,
    items: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::OutItem>,
    available: &[ColId],
) -> Result<(ast_resolved::PipeOp, Vec<ColId>)> {
    let mut prefixed = Vec::with_capacity(items.len() + 1);
    prefixed.push(ast_unresolved::OutItem::Many(Spread::Glob(Glob::whole())));
    prefixed.extend(items.into_vec());
    let prefixed = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(prefixed)
        .expect("the prefix glob makes the embed items nonempty");
    let (resolved, output) = resolve_general_via_fold(fold, prefixed, available)?;
    let ast_resolved::PipeOp::Project(items) = resolved else {
        unreachable!("the projection algorithm answers with a projection");
    };
    Ok((
        ast_resolved::PipeOp::Embed(items),
        output,
    ))
}

pub(super) fn resolve_general_via_fold(
    fold: &mut ResolverFold,
    items: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::OutItem>,
    available: &[ColId],
) -> Result<(ast_resolved::PipeOp, Vec<ColId>)> {
    let items = items.into_vec();
    let has_glob = items.iter().any(|item| {
        matches!(
            item,
            ast_unresolved::OutItem::Many(Spread::Glob(Glob {
                qualifier: None,
                ..
            }))
        )
    });

    if has_glob {
        for item in &items {
            let ast_unresolved::OutItem::One(one) = item else {
                continue;
            };
            let Some(naming) = one.naming.as_ref() else {
                continue;
            };
            let spelling = fold
                .registry
                .identities
                .intern(naming.as_str(), naming.is_stropped());
            let name = fold.registry.identities.canonical(spelling);
            if available
                .iter()
                .any(|column| fold.registry.identities.published_sym(*column) == Some(name))
            {
                return Err(DelightQLError::validation_error_categorized(
                    "constraint",
                    format!(
                        "Duplicate column '{}' in embed projection: column already exists in source schema. \
                         Use $$(expr as {}) to replace the existing column instead",
                        naming, naming,
                    ),
                    "in embed projection",
                ));
            }
        }
    }

    // The authored labels are read while the items are still authored, and
    // each expansion of one item inherits that item's label — a spread's
    // several are all engine-named, so one label per item is enough.
    let mut resolved_items = Vec::new();
    let mut output_intents = Vec::new();
    for item in items {
        let label = authored_label(&item).cloned();
        let ordinal = ordinal_label(&item);
        let is_engine = engine_managed(&item);
        // A scalar subquery resolves as one value; the spread road would
        // mistake its interior for something to enumerate.
        let resolved = match &item {
            ast_unresolved::OutItem::One(one)
                if matches!(
                    one.expr.domain(),
                    Some(ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Scalarized(_)
                    ))
                ) =>
            {
                let ast_unresolved::OutItem::One(one) = item else {
                    unreachable!("the guard just matched a one-value item")
                };
                vec![ast_resolved::OutItem::One(ast_resolved::OneOut {
                    expr: crate::pipeline::ast_transform::transform_out_value(fold, one.expr)?,
                    naming: one.naming,
                    output: None,
                })]
            }
            _ => super::super::domain_expressions::projection::resolve_out_items_via_fold(
                fold,
                vec![item],
                available,
                false,
            )?,
        };
        for resolved_item in resolved {
            output_intents.push((label.clone(), ordinal.clone(), is_engine));
            resolved_items.push(resolved_item);
        }
    }

    let output_scope = mint_projection_scope(&fold.registry.identities, available);
    let mut output_columns = Vec::new();
    let mut output_metadata = Vec::new();
    for (position, item) in resolved_items.iter_mut().enumerate() {
        let Some(column) = extract_provided_column_for_item(
            item,
            position,
            &fold.registry.identities,
            output_scope,
        ) else {
            continue;
        };
        output_columns.push(column);
        output_metadata.push(output_intents[position].clone());
        if let ast_resolved::OutItem::One(one) = item {
            one.output = Some(column);
        }
    }

    if output_columns.is_empty() {
        return Err(DelightQLError::parse_error(
            "Projection matched no columns - would create empty table",
        ));
    }

    let engine_names: Vec<_> = output_columns
        .iter()
        .zip(&output_metadata)
        .filter_map(|(column, (_, _, engine_managed))| {
            engine_managed
                .then(|| fold.registry.identities.published_sym(*column))
                .flatten()
        })
        .collect();
    let mut seen_user = Vec::new();
    for (column, (authored_name, ordinal_label, engine_managed)) in
        output_columns.iter().zip(&output_metadata)
    {
        if *engine_managed {
            continue;
        }
        let authored_label = authored_name
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| ordinal_label.clone());
        let Some(authored_label) = authored_label else {
            continue;
        };
        let canonical = match fold.registry.identities.published_sym(*column) {
            Some(canonical) => canonical,
            None => {
                let Some(authored_name) = authored_name else {
                    continue;
                };
                let spelling = fold
                    .registry
                    .identities
                    .intern(authored_name.as_str(), authored_name.is_stropped());
                fold.registry.identities.canonical(spelling)
            }
        };
        if seen_user.contains(&canonical) {
            return Err(DelightQLError::validation_error_categorized(
                "constraint",
                format!(
                    "Duplicate column '{}' in projection: programmer-authored names must be unique. \
                     Rename one with 'as' to disambiguate",
                    authored_label,
                ),
                "in projection",
            ));
        }
        if engine_names.contains(&canonical) {
            return Err(DelightQLError::validation_error_categorized(
                "constraint",
                format!(
                    "Duplicate column '{}' in projection: explicit column collides with wildcard expansion. \
                     Rename with 'as' or remove the explicit reference",
                    authored_label,
                ),
                "in projection",
            ));
        }
        seen_user.push(canonical);
    }

    Ok((
        ast_resolved::PipeOp::Project(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(resolved_items)
                .expect("the empty projection refused above"),
        ),
        output_columns,
    ))
}
