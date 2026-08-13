// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::names::{Addressing, ColId, ColumnOrigin, Computation, Hint, Registry, ValueFacts};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

use super::super::column_extraction::{
    extract_provided_column_for_item, extract_provided_column_for_reduction, mint_projection_scope,
};
use crate::pipeline::asts::core::{NamedReference, Reference};

/// Resolve publication items and keep, per resolved item, the authored label
/// the duplicate-name law judges: the name the author wrote, and whether the
/// engine rather than the programmer chose it.
fn resolve_published_items(
    fold: &mut ResolverFold,
    items: Vec<ast_unresolved::OutItem>,
    available: &[ColId],
) -> Result<(
    Vec<ast_resolved::OutItem>,
    Vec<(Option<delightql_types::SqlIdentifier>, bool)>,
)> {
    let mut resolved = Vec::new();
    let mut intents = Vec::new();
    for item in items {
        let naming = match &item {
            ast_unresolved::OutItem::One(one) => one.naming.clone(),
            ast_unresolved::OutItem::Many(_) | ast_unresolved::OutItem::Whole => None,
        };
        let engine_managed = matches!(
            item,
            ast_unresolved::OutItem::Many(_) | ast_unresolved::OutItem::Whole
        );
        for resolved_item in
            super::super::domain_expressions::projection::resolve_out_items_via_fold(
                fold,
                vec![item],
                available,
                false,
            )?
        {
            resolved.push(resolved_item);
            intents.push((naming.clone(), engine_managed));
        }
    }
    Ok((resolved, intents))
}

fn check_duplicate_user_names(
    identities: &Registry,
    output: &[ColId],
    intents: &[(Option<delightql_types::SqlIdentifier>, bool)],
) -> Result<()> {
    let mut seen = Vec::new();
    for (column, (authored_name, engine_managed)) in output.iter().zip(intents) {
        if *engine_managed {
            continue;
        }
        let Some(authored_name) = authored_name else {
            continue;
        };
        let canonical = identities.published_sym(*column).unwrap_or_else(|| {
            identities
                .canonical(identities.intern(authored_name.as_str(), authored_name.is_stropped()))
        });
        if seen.contains(&canonical) {
            return Err(DelightQLError::validation_error_categorized(
                "constraint",
                format!(
                    "Duplicate column '{}': programmer-authored names must be unique. \
                     Rename one with 'as' to disambiguate",
                    authored_name,
                ),
                "in output schema",
            ));
        }
        seen.push(canonical);
    }
    Ok(())
}

/// The same, over what an arm COMPUTES.
fn collect_result_lvars(
    result: &crate::pipeline::asts::core::OutValue<crate::pipeline::asts::core::Resolved>,
    output: &mut Vec<ColId>,
) {
    use crate::pipeline::asts::core::OutValue;
    match result {
        OutValue::Domain(value) => collect_lvars(value, output),
        // A crossing carries a TRUTH, whose own columns are reached by the
        // truth walk beside this one.
        OutValue::Truth(_) => {}
    }
}

fn collect_lvars(expression: &ast_resolved::DomainExpression, output: &mut Vec<ColId>) {
    match expression {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => output.push(*column),
        ast_resolved::DomainExpression::Application(function) => match function {
            ast_resolved::FunctionApplication::Ground(_)
            | ast_resolved::FunctionApplication::Open(_)
            // A scalarized relation names its own scope's columns.
            | ast_resolved::FunctionApplication::Scalarized(_) => {}
            ast_resolved::FunctionApplication::Standard(application) => {
                for argument in application.call().arguments.value_domains() {
                    collect_lvars(argument, output);
                }
            }
            // The arms are the callee's constants; the lvars a pick reads
            // are its arguments'.
            ast_resolved::FunctionApplication::FieldSelect(select) => {
                for argument in select.application.call().arguments.value_domains() {
                    collect_lvars(argument, output);
                }
            }
            ast_resolved::FunctionApplication::Infix(infix) => {
                collect_lvars(&infix.left, output);
                collect_lvars(&infix.right, output);
            }
            ast_resolved::FunctionApplication::Template(template) => {
                for part in template.parts() {
                    if let ast_resolved::ValueTemplatePart::Interpolation(inner) = part {
                        collect_lvars(inner, output);
                    }
                }
            }
            ast_resolved::FunctionApplication::ClauseSelection(selection) => {
                for arm in &selection.arms {
                    collect_result_lvars(&arm.result, output);
                }
            }
            ast_resolved::FunctionApplication::Case(case) => {
                let default = match case {
                    ast_resolved::CaseExpression::Anchored {
                        anchor,
                        arms,
                        default,
                    } => {
                        collect_lvars(anchor, output);
                        for arm in arms.iter() {
                            collect_lvars(&arm.result, output);
                        }
                        default
                    }
                    // A condition names its own columns through the truth
                    // walk; this collector reads results.
                    ast_resolved::CaseExpression::Searched { arms, default } => {
                        for arm in arms.iter() {
                            collect_lvars(&arm.result, output);
                        }
                        default
                    }
                };
                if let Some(result) = default {
                    collect_lvars(result, output);
                }
            }
            ast_resolved::FunctionApplication::JsonAccess(access) => {
                collect_lvars(&access.source, output);
            }
            // A TUPLE'S ELEMENTS ARE ORDINARY VALUES, so their addresses are
            // the enclosing reduction's. A record's keys and a metadata
            // level's contents publish into an INTERIOR heading, which is not
            // the outer group's to collect.
            ast_resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Tuple(tuple),
            ) => {
                for element in tuple.elements.iter() {
                    collect_lvars(element, output);
                }
            }
            ast_resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::Record(_),
            )
            | ast_resolved::FunctionApplication::Enclyph(
                crate::pipeline::asts::core::Enclyph::EmptyRecord(_),
            ) => {}
        },
        // Uninhabited after resolution, and still written: a match on a
        // REFERENCE cannot omit an uninhabited variant's arm.
        ast_resolved::DomainExpression::Reference(Reference::Ordinal(_)) => {}
    }
}

fn pivot_values_for(
    expression: &ast_resolved::DomainExpression,
    pivot_in_values: &std::collections::HashMap<crate::names::Sym, Vec<String>>,
    identities: &Registry,
) -> Option<(ColId, Vec<String>)> {
    let mut columns = Vec::new();
    collect_lvars(expression, &mut columns);
    // Both halves of the match, because refusing tells them apart and the
    // refusal does not: no values collected at all reads the same as values
    // collected under a name the key does not publish.
    crate::probe::probe!(
        pivot,
        "key candidates={:?} published={:?} in-values keyed by={:?}",
        columns,
        columns
            .iter()
            .map(|column| identities.published_sym(*column))
            .collect::<Vec<_>>(),
        pivot_in_values.keys().collect::<Vec<_>>()
    );
    for column in columns {
        // A candidate publishing nothing is skipped, not fatal. It cannot be
        // the key — the map is keyed by published name — and abandoning the
        // search there would let an unnameable candidate hide a later one
        // that matches, which is the treatment a merely non-matching
        // candidate already gets.
        let Some(published) = identities.published_sym(column) else {
            continue;
        };
        if let Some(values) = pivot_in_values.get(&published) {
            return Some((column, values.clone()));
        }
    }
    None
}

fn expand_pivot_template(
    expression: &ast_resolved::DomainExpression,
    source: ColId,
    value: &str,
) -> Option<String> {
    match expression {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => (*column == source).then(|| value.to_string()),
        ast_resolved::DomainExpression::Application(ast_resolved::FunctionApplication::Ground(
            value,
        )) => match value {
            ast_resolved::LiteralValue::String(value) => Some(value.clone()),
            other => Some(other.to_string()),
        },
        ast_resolved::DomainExpression::Application(ast_resolved::FunctionApplication::Infix(
            infix,
        )) if infix.operator == crate::pipeline::asts::vocabulary::BinOp::Concat => Some(format!(
            "{}{}",
            expand_pivot_template(&infix.left, source, value)?,
            expand_pivot_template(&infix.right, source, value)?
        )),
        _ => None,
    }
}

/// Attach the interior heading a RECORD construction publishes.
///
/// The record's own members name the interior's columns, in written order;
/// an induced member's target is the level beneath it. A published value
/// that is not a record has no interior heading to attach.
fn attach_record_interior(
    identities: &Registry,
    owner: ColId,
    expression: &ast_resolved::DomainExpression,
) -> bool {
    use crate::pipeline::asts::core::Enclyph;

    let ast_resolved::DomainExpression::Application(ast_resolved::FunctionApplication::Enclyph(
        Enclyph::Record(record),
    )) = expression
    else {
        return false;
    };
    attach_record_columns(identities, owner, record);
    true
}

fn attach_record_columns(identities: &Registry, owner: ColId, record: &ast_resolved::Record) {
    use crate::pipeline::asts::core::{Enclyph, NamedReference, RecordMember};

    let scope = identities.mint_interior_scope(owner, Hint::None);
    let mut position = 0_u32;
    for member in record.members.iter() {
        let (published, nested) = match member {
            RecordMember::SelfKeyed(NamedReference(occurrence)) => {
                (identities.published(occurrence.column), None)
            }
            RecordMember::Keyed { key, .. } => (Some(identities.intern(key, false)), None),
            RecordMember::Induced { key, value } => (
                Some(identities.intern(key, false)),
                match value.as_ref() {
                    // A tuple publishes by position and names nothing, so it
                    // contributes no interior heading.
                    Enclyph::Record(nested) => Some(nested),
                    Enclyph::EmptyRecord(_) => None,
                    Enclyph::Tuple(_) => None,
                },
            ),
            RecordMember::Spread(spread) => spread.expanded(),
        };
        let child = identities.mint_column(
            scope,
            ColumnOrigin::Bound { position },
            published,
            Addressing::Published,
            ValueFacts::default(),
        );
        position += 1;
        if let Some(nested) = nested {
            attach_record_columns(identities, child, nested);
        }
    }
}

fn duplicate_published(identities: &Registry, columns: &[ColId]) -> Option<crate::names::Sym> {
    let mut seen = Vec::new();
    for column in columns {
        if let Some(name) = identities.published_sym(*column) {
            if seen.iter().any(|seen_name| *seen_name == name) {
                return Some(name);
            }
            seen.push(name);
        }
    }
    None
}

pub(super) fn resolve_group_via_fold(
    fold: &mut ResolverFold,
    spec: ast_unresolved::GroupSpec,
    available: &[ColId],
    pivot_in_values: &std::collections::HashMap<crate::names::Sym, Vec<String>>,
) -> Result<(ast_resolved::PipeOp, Vec<ColId>)> {
    let output_scope = mint_projection_scope(&fold.registry.identities, available);
    let (spec, output) = match spec {
        ast_unresolved::GroupSpec::Distinct { keys } => {
            let (resolved, intents) = resolve_published_items(fold, keys.into_vec(), available)?;
            let mut resolved = resolved;
            let mut output = Vec::new();
            for (position, item) in resolved.iter_mut().enumerate() {
                if let Some(column) = extract_provided_column_for_item(
                    item,
                    position,
                    &fold.registry.identities,
                    output_scope,
                ) {
                    output.push(column);
                    if let ast_resolved::OutItem::One(one) = item {
                        one.output = Some(column);
                    }
                }
            }
            check_duplicate_user_names(&fold.registry.identities, &output, &intents)?;
            (
                ast_resolved::GroupSpec::Distinct {
                    keys: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(resolved)
                        .expect("the authored distinct keys were nonempty"),
                },
                output,
            )
        }
        ast_unresolved::GroupSpec::Reduce {
            keys,
            reductions,
            plan: _,
        } => {
            // Delegates and the other reduction members take two resolution
            // roads (the delegate's outputs publish LAST, after every other
            // reduction), so the one nonempty family splits here and is
            // reassembled below in that published order.
            let (delegates, reductions): (Vec<_>, Vec<_>) = reductions
                .into_vec()
                .into_iter()
                .partition(|item| {
                    matches!(item, ast_unresolved::ReductionItem::Delegate(_))
                });
            let delegates: Vec<ast_unresolved::DelegateSpec> = delegates
                .into_iter()
                .map(|item| match item {
                    ast_unresolved::ReductionItem::Delegate(delegate) => delegate,
                    _ => unreachable!("the partition selected delegates"),
                })
                .collect();
            let mut by = super::super::domain_expressions::projection::resolve_out_items_via_fold(
                fold,
                keys,
                available,
                false,
            )?;
            let mut on =
                super::super::domain_expressions::projection::resolve_reduction_items_via_fold(
                    fold,
                    reductions,
                    available,
                )?;

            // THE IN IS THE HEADING WITNESS, read here where the group's
            // membership predicates are in scope.
            for item in on.iter_mut() {
                if let ast_resolved::ReductionItem::Pivot(pivot) = item {
                    let (source, values) = pivot_values_for(
                        &pivot.pivot_key,
                        pivot_in_values,
                        &fold.registry.identities,
                    )
                    .ok_or_else(|| {
                        DelightQLError::validation_error(
                            "Pivot key requires a matching IN predicate",
                            "Add an IN predicate with literal values for a referenced column",
                        )
                    })?;
                    let mut expanded = Vec::new();
                    for value in values {
                        expanded.push(
                            expand_pivot_template(&pivot.pivot_key, source, &value)
                                .unwrap_or(value),
                        );
                    }
                    pivot.values = expanded;
                }
            }

            let plan =
                super::super::tree_group_analysis::analyze_tree_groups_for_ctes(&mut by, &mut on)?;

            let mut output = Vec::new();
            for (position, item) in by.iter_mut().enumerate() {
                let column = extract_provided_column_for_item(
                    item,
                    position,
                    &fold.registry.identities,
                    output_scope,
                );
                if let Some(column) = column {
                    output.push(column);
                }
                if let ast_resolved::OutItem::One(one) = item {
                    one.output = column;
                }
            }

            let base = by.len();
            let mut pivot_outputs = Vec::new();
            for (position, item) in on.iter_mut().enumerate() {
                // A pivot publishes ONE column per value its key's membership
                // predicate named, so its item publishes none of its own.
                let pivot_values = match item {
                    ast_resolved::ReductionItem::Pivot(pivot) => Some(pivot.values.clone()),
                    ast_resolved::ReductionItem::Out(_)
                    | ast_resolved::ReductionItem::Metadata(_)
                    | ast_resolved::ReductionItem::Delegate(_) => None,
                };
                if let Some(pivot_values) = pivot_values {
                    for value in &pivot_values {
                        let spelling = fold.registry.identities.intern(value, false);
                        let column = fold.registry.identities.mint_column(
                            output_scope,
                            ColumnOrigin::Computed {
                                via: Computation::Aggregate,
                            },
                            Some(spelling),
                            Addressing::Published,
                            ValueFacts::default(),
                        );
                        output.push(column);
                        pivot_outputs.push(column);
                    }
                    if let Some(ast_resolved::OutItem::One(one)) = item.out_item_mut() {
                        one.output = None;
                    }
                    continue;
                }
                let column = extract_provided_column_for_reduction(
                    item,
                    base + position,
                    &fold.registry.identities,
                    output_scope,
                );
                if let Some(column) = column {
                    if let Some(expression) = item.domain_value() {
                        attach_record_interior(&fold.registry.identities, column, expression);
                    }
                    output.push(column);
                }
                match item {
                    ast_resolved::ReductionItem::Out(ast_resolved::OutItem::One(one)) => {
                        one.output = column
                    }
                    ast_resolved::ReductionItem::Metadata(metadata) => metadata.output = column,
                    // A spread, the whole, a pivot and a delegate each
                    // publish something other than one output of their own.
                    ast_resolved::ReductionItem::Out(_)
                    | ast_resolved::ReductionItem::Pivot(_)
                    | ast_resolved::ReductionItem::Delegate(_) => {
                    }
                }
            }

            if let Some(duplicate) = duplicate_published(&fold.registry.identities, &output) {
                // Pivot values become output identifiers. When their
                // normalized spellings collide, the refusal must identify
                // the pivot road so a format string is an actionable repair;
                // ordinary grouped-output collisions keep the general
                // projection diagnosis below.
                if pivot_outputs.iter().any(|column| {
                    fold.registry.identities.published_sym(*column) == Some(duplicate)
                }) {
                    return Err(DelightQLError::validation_error_categorized(
                        "constraint/pivot",
                        "Duplicate pivot column name",
                        "Disambiguate pivot values with a format string",
                    ));
                }
                return Err(DelightQLError::validation_error(
                    "Duplicate output name in grouped projection",
                    "Rename one output or disambiguate pivot values with a format string",
                ));
            }

            let mut resolved_delegates = Vec::with_capacity(delegates.len());
            for delegate in delegates {
                let payload =
                    super::super::domain_expressions::projection::resolve_out_items_via_fold(
                        fold,
                        delegate.payload,
                        available,
                        false,
                    )?;
                let order = delegate
                    .order
                    .into_iter()
                    .map(|ordering| {
                        super::super::domain_expressions::projection::resolve_expressions_via_fold(
                            fold,
                            vec![ordering.column],
                            available,
                        )
                        .map(|mut expressions| {
                            ast_resolved::OrderingSpec {
                            column: expressions
                                .pop()
                                .expect("one ordering expression resolves to one expression"),
                            direction:
                                super::super::super::helpers::converters::convert_order_direction(
                                    ordering.direction,
                                ),
                        }
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                resolved_delegates.push(ast_resolved::DelegateSpec { payload, order });
            }

            let mut seen: Vec<_> = output
                .iter()
                .filter_map(|column| fold.registry.identities.published_sym(*column))
                .collect();
            let mut position = by.len() + on.len();
            for delegate in &mut resolved_delegates {
                for item in &mut delegate.payload {
                    let column = extract_provided_column_for_item(
                        item,
                        position,
                        &fold.registry.identities,
                        output_scope,
                    );
                    let column = column.filter(|column| {
                        fold.registry
                            .identities
                            .published_sym(*column)
                            .is_none_or(|name| {
                                if seen.contains(&name) {
                                    false
                                } else {
                                    seen.push(name);
                                    true
                                }
                            })
                    });
                    if let Some(column) = column {
                        output.push(column);
                    }
                    if let ast_resolved::OutItem::One(one) = item {
                        one.output = column;
                    }
                    position += 1;
                }
            }

            let mut reductions = on;
            reductions.extend(
                resolved_delegates
                    .into_iter()
                    .map(ast_resolved::ReductionItem::Delegate),
            );
            (
                ast_resolved::GroupSpec::Reduce {
                    keys: by,
                    reductions: crate::pipeline::asts::vocabulary::Vec1::try_from_vec(reductions)
                        .expect("the authored reduction was nonempty"),
                    plan,
                },
                output,
            )
        }
    };

    Ok((
        ast_resolved::PipeOp::Group(spec),
        output,
    ))
}

