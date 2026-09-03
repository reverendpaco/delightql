// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
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
    one.naming.as_ref().or_else(|| bare_name(&one.expr))
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
/// because that is what a refusal has to echo back. It is the SPELLING of a
/// position, never a name: nothing is called `|2|`.
fn ordinal_label(item: &ast_unresolved::OutItem) -> Option<String> {
    let ast_unresolved::OutItem::One(one) = item else {
        return None;
    };
    if one.naming.is_some() {
        return None;
    }
    written_ordinal(&one.expr)
}

fn written_ordinal(expr: &ast_unresolved::DomainExpression) -> Option<String> {
    match expr {
        ast_unresolved::DomainExpression::Reference(Reference::Ordinal(ordinal)) => {
            let position = crate::pipeline::asts::core::literals::column_ordinal_text(
                ordinal.position,
                ordinal.reverse,
            );
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

/// Resolve the General projection operator via fold-based dispatch.
/// EMBED IS EXTENSION: the operand's whole heading rides in front of the
/// added items, prefixed HERE — the one shared projection algorithm — so
/// the two spellings cannot drift and the resolved carrier still says
/// which was authored.
pub(super) fn resolve_embed_via_fold(
    fold: &mut ResolverFold,
    items: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::OutItem>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    let mut prefixed = Vec::with_capacity(items.len() + 1);
    prefixed.push(ast_unresolved::OutItem::Many(Spread::Glob(Glob::whole())));
    prefixed.extend(items.into_vec());
    let prefixed = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(prefixed)
        .expect("the prefix glob makes the embed items nonempty");
    resolve_projection_via_fold(fold, prefixed, available, input, Projection::Embed)
}

pub(super) fn resolve_general_via_fold(
    fold: &mut ResolverFold,
    items: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::OutItem>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    resolve_projection_via_fold(fold, items, available, input, Projection::Project)
}

/// WHICH PROJECTION THIS IS, and there is no third.
///
/// ONE description of the operation: the variant decides the output law the
/// authority derives under AND the operator the tree stores, so a
/// projection cannot be stored as an embed or derived as one.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Projection {
    /// `|> (a, b)` — the heading is exactly the items.
    Project,
    /// `|> $$(x)` — the operand's whole heading, then the items.
    Embed,
}

fn resolve_projection_via_fold(
    fold: &mut ResolverFold,
    items: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::OutItem>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
    kind: Projection,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
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
                .core
                .identities
                .intern(naming.as_str(), naming.is_stropped());
            let name = fold.core.identities.canonical(spelling);
            if available
                .iter()
                .any(|column| fold.core.identities.published_sym(column.column()) == Some(name))
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
    let mut positions = Vec::new();
    let mut output_metadata = Vec::new();
    for item in items {
        let label = authored_label(&item).cloned();
        let ordinal = ordinal_label(&item);
        // A scalar subquery resolves as one value; the spread road would
        // mistake its interior for something to enumerate.
        let resolved = match &item {
            ast_unresolved::OutItem::One(one)
                if matches!(
                    one.expr,
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Scalarized(_)
                    )
                ) =>
            {
                let ast_unresolved::OutItem::One(one) = item else {
                    unreachable!("the guard just matched a one-value item")
                };
                vec![crate::relation::pending::Position::Authored {
                    expr: fold.transform_domain(one.expr)?,
                    naming: one.naming,
                }]
            }
            _ => super::super::domain_expressions::projection::resolve_out_items_via_fold(
                fold,
                vec![item],
                available,
                false,
            )?,
        };
        for position in resolved {
            if position.value().is_some() {
                output_metadata.push((
                    label.clone(),
                    ordinal.clone(),
                    position.is_engine_expansion(),
                ));
            }
            positions.push(position);
        }
    }

    // ONE DESCRIPTION. The publication states its operand, which act it is,
    // and the positions it publishes; the authority reads the slots off
    // those positions, mints the ports, and writes the items over them, so
    // the operator the tree stores and the law the interface came from are
    // the same statement rather than two a call site pairs.
    let (step, output_ports) =
        fold.core
            .identities
            .authority()
            .bind(crate::relation::pending::Pending::Publication {
                input,
                publishes: match kind {
                    Projection::Embed => crate::relation::pending::Publishes::Edited,
                    Projection::Project => crate::relation::pending::Publishes::Anew,
                },
                // The authored `|>` stage: the one road every surface
                // publication takes.
                why: crate::relation::form::ProjectWhy::Stage,
                positions,
            })?;
    let output_columns: Vec<_> = output_ports.iter().map(|port| port.column()).collect();

    let engine_names: Vec<_> = output_columns
        .iter()
        .zip(&output_metadata)
        .filter_map(|(column, (_, _, engine_managed))| {
            engine_managed
                .then(|| fold.core.identities.published_sym(*column))
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
        // AN ORDINAL IS NOT A NAME. `|2|` authors a POSITION: it says which
        // output port stands here and nothing about what the port is
        // called. So an item with no authored name is judged on the name it
        // INHERITS from the source heading — which is a name the author did
        // write, upstream — and the two cases teach differently.
        //
        // A name already lost to a repetition is not a name being authored
        // again. Re-projecting an already-poisoned heading (`|> (|2|,
        // |1|)`) reorders two ports that publish nothing; refusing it as a
        // duplicate authored name reported a name the author never wrote.
        let inherited = fold.core.identities.published_sym(*column);
        let (canonical, authored) = match (authored_name, inherited) {
            (Some(_), Some(canonical)) => (canonical, true),
            (Some(authored_name), None) => {
                let spelling = fold
                    .core
                    .identities
                    .intern(authored_name.as_str(), authored_name.is_stropped());
                (fold.core.identities.canonical(spelling), true)
            }
            (None, Some(canonical)) if !fold.core.identities.name_lost_to_ambiguity(*column) => {
                (canonical, false)
            }
            (None, _) => continue,
        };
        let authored_label = authored_name
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| ordinal_label.clone())
            .unwrap_or_default();
        if seen_user.contains(&canonical) {
            return Err(DelightQLError::validation_error_categorized(
                "constraint",
                if authored {
                    format!(
                        "Duplicate column '{}' in projection: programmer-authored names must be \
                         unique. Rename one with 'as' to disambiguate",
                        authored_label,
                    )
                } else {
                    // What happened is not that a name was written twice:
                    // two addressed positions publish ONE name, and a
                    // projection cannot carry it twice.
                    format!(
                        "'{}' selects a position already published by this projection under the \
                         same name, and one heading cannot carry one name twice. Name one of them \
                         with 'as'",
                        authored_label,
                    )
                },
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

    // The heading is finished: seal its ambiguities before anything
    // carries a position out of it.
    fold.core
        .identities
        .seal_heading_ambiguities(&output_columns);

    Ok((step, output_ports))
}
