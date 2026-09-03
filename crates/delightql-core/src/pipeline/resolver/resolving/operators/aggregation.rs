// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::resolver::{PivotInWitness, PivotInWitnesses};
use crate::pipeline::{ast_resolved, ast_unresolved};

use crate::pipeline::asts::core::{NamedReference, Reference};

/// Resolve publication items and keep, per resolved item, the authored label
/// the duplicate-name law judges: the name the author wrote, and whether the
/// engine rather than the programmer chose it.
fn resolve_published_items(
    fold: &mut ResolverFold,
    items: Vec<ast_unresolved::OutItem>,
    available: &[crate::relation::PortId],
) -> Result<(
    Vec<super::super::domain_expressions::projection::PendingOutItem>,
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
    identities: &crate::relation::Planning,
    output: &[crate::relation::PortId],
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
        let canonical = identities
            .published_sym(column.column())
            .unwrap_or_else(|| {
                identities.canonical(
                    identities.intern(authored_name.as_str(), authored_name.is_stropped()),
                )
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
    result: &crate::pipeline::asts::core::DomainExpression<crate::pipeline::asts::core::Resolved>,
    output: &mut Vec<crate::relation::PortId>,
) {
    collect_lvars(result, output)
}

fn collect_lvars(
    expression: &ast_resolved::DomainExpression,
    output: &mut Vec<crate::relation::PortId>,
) {
    match expression {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => output.push(*column),
        ast_resolved::DomainExpression::Reference(Reference::Physical(_)) => {}
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
            // A crossed truth reads the values its truth reads at this
            // scope, exactly as an arithmetic operand reads its operands.
            ast_resolved::FunctionApplication::Crossed(crossing) => {
                for operand in crossing.truth().scalar_operands() {
                    collect_lvars(operand, output);
                }
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
                    // A condition reads its values the way a result does.
                    ast_resolved::CaseExpression::Searched { arms, default } => {
                        for arm in arms.iter() {
                            for operand in arm.condition.scalar_operands() {
                                collect_lvars(operand, output);
                            }
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
                    collect_lvars(element.value(), output);
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
    pivot_in_values: &PivotInWitnesses,
    identities: &crate::relation::Planning,
) -> PivotValueJudgment {
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
            .map(|column| identities.published_sym(column.column()))
            .collect::<Vec<_>>(),
        pivot_in_values.keys().collect::<Vec<_>>()
    );
    for column in columns {
        // A candidate publishing nothing is skipped, not fatal. It cannot be
        // the key — the map is keyed by published name — and abandoning the
        // search there would let an unnameable candidate hide a later one
        // that matches, which is the treatment a merely non-matching
        // candidate already gets.
        let Some(published) = identities.published_sym(column.column()) else {
            continue;
        };
        if let Some(witness) = pivot_in_values.get(&published) {
            return match witness {
                PivotInWitness::ColumnNames(values) => {
                    PivotValueJudgment::Ready(column, values.clone())
                }
                PivotInWitness::UnnameableValues => PivotValueJudgment::Unnameable(column),
            };
        }
    }
    PivotValueJudgment::Missing
}

enum PivotValueJudgment {
    Ready(crate::relation::PortId, Vec<String>),
    Unnameable(crate::relation::PortId),
    Missing,
}

fn pivot_key_teaching(
    key: crate::relation::PortId,
    identities: &crate::relation::Planning,
) -> String {
    let mut teaching = String::new();
    let mut sink = crate::names::Teaching(&mut teaching);
    if let Some(spelling) = identities.published(key.column()) {
        identities.write(spelling, &mut sink);
    } else {
        identities.write_ordinal_report(key.column(), &mut sink);
    }
    teaching
}

fn expand_pivot_template(
    expression: &ast_resolved::DomainExpression,
    source: crate::relation::PortId,
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
pub(crate) fn attach_record_interior(
    authority: &crate::relation::SemanticBuilder<'_>,
    owner: crate::relation::PortId,
    expression: &ast_resolved::DomainExpression,
) -> Result<bool> {
    use crate::pipeline::asts::core::Enclyph;

    let ast_resolved::DomainExpression::Application(ast_resolved::FunctionApplication::Enclyph(
        Enclyph::Record(record),
    )) = expression
    else {
        return Ok(false);
    };
    let body = record_relation(authority, record)?;
    authority.derive(crate::relation::RelForm::Interior(
        crate::relation::form::InteriorSpec { owner, body },
    ))?;
    Ok(true)
}

fn record_relation(
    authority: &crate::relation::SemanticBuilder<'_>,
    record: &ast_resolved::Record,
) -> Result<crate::relation::SemanticRelation> {
    use crate::pipeline::asts::core::{Enclyph, NamedReference, RecordMember};

    let mut slots = Vec::new();
    let mut nested = Vec::new();
    for (position, member) in record.members.iter().enumerate() {
        let (published, child) = match member {
            RecordMember::SelfKeyed(NamedReference(occurrence)) => (
                authority.names().published(occurrence.column.column()),
                None,
            ),
            RecordMember::Keyed { key, .. } | RecordMember::Metadata { key, .. } => {
                (Some(authority.names().intern(key, false)), None)
            }
            RecordMember::Induced { key, value } => (
                Some(authority.names().intern(key, false)),
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
        slots.push(crate::relation::form::AnonymousSlot::Declared {
            position: position as u32,
            named: published,
        });
        nested.push(child);
    }
    let relation = authority.derive(crate::relation::RelForm::Anonymous(
        crate::relation::form::AnonymousSpec {
            shape: crate::relation::form::AnonymousShape::Tabular,
            slots: &slots,
            answers_to: None,
        },
    ))?;
    let ports = authority.interface(&relation)?.ports().to_vec();
    for (owner, child) in ports.into_iter().zip(nested) {
        if let Some(child) = child {
            let body = record_relation(authority, child)?;
            authority.derive(crate::relation::RelForm::Interior(
                crate::relation::form::InteriorSpec { owner, body },
            ))?;
        }
    }
    Ok(relation)
}

fn duplicate_published(
    identities: &crate::relation::Planning,
    columns: &[crate::relation::PortId],
) -> Option<crate::names::Sym> {
    let mut seen = Vec::new();
    for column in columns {
        if let Some(name) = identities.published_sym(column.column()) {
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
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
    pivot_in_values: &PivotInWitnesses,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    use crate::relation::pending::{Delegate, GroupShape, Pending, Reduction};

    let (step, pivot_names, output, distinct, intents) = match spec {
        ast_unresolved::GroupSpec::Distinct { keys } => {
            let (pending, intents) = resolve_published_items(fold, keys.into_vec(), available)?;
            let (step, output) = fold.core.identities.authority().bind(Pending::Group {
                input,
                keys: pending,
                shape: GroupShape::Distinct,
            })?;
            (step, Vec::new(), output, true, intents)
        }
        ast_unresolved::GroupSpec::Reduce {
            keys,
            reductions,
            plan: _,
        } => {
            // Delegates and the other reduction members take two resolution
            // roads (the delegate's outputs publish LAST, after every other
            // reduction), so the one nonempty family splits here and is
            // reassembled by the authority in that published order.
            let (delegates, reductions): (Vec<_>, Vec<_>) = reductions
                .into_vec()
                .into_iter()
                .partition(|item| matches!(item, ast_unresolved::ReductionItem::Delegate(_)));
            let delegates: Vec<ast_unresolved::DelegateSpec> = delegates
                .into_iter()
                .map(|item| match item {
                    ast_unresolved::ReductionItem::Delegate(delegate) => delegate,
                    _ => unreachable!("the partition selected delegates"),
                })
                .collect();
            let by = super::super::domain_expressions::projection::resolve_out_items_via_fold(
                fold, keys, available, false,
            )?;
            // REDUCTION-SLOT MEMBERS RESOLVE UNDER THE REDUCING GRADE:
            // the position's expectation is present while each member's
            // value — and any consulted definition it opens — resolves.
            let prior_grade = std::mem::replace(
                &mut fold.position_grade,
                crate::defuse::bound_use::CallableGrade::Reducing,
            );
            let on = super::super::domain_expressions::projection::resolve_reduction_items_via_fold(
                fold, reductions, available,
            );
            fold.position_grade = prior_grade;
            let mut on = on?;

            // GRADE AGREEMENT AT THE REDUCTION SLOT (the implicit-
            // aggregation clause), judged with the GROUP KEYS in hand: a
            // bare key column is constant per group and licenses itself;
            // every other row-column occurrence must stand under a
            // reducing absorber. Judged from the VALUE THAT RESOLVED,
            // never the authored spelling — an enlisted DQL definition
            // named `sum` is its own per-row body, and an absorber
            // somewhere does not license per-row reads elsewhere.
            {
                use crate::pipeline::asts::core::Reference;
                let group_keys: std::collections::HashSet<crate::relation::PortId> = by
                    .iter()
                    .filter_map(|key| match key {
                        crate::relation::pending::Position::Authored { expr, .. }
                        | crate::relation::pending::Position::Expanded { expr, .. } => match expr {
                            crate::pipeline::asts::resolved::DomainExpression::Reference(
                                Reference::Named(named),
                            ) => Some(named.column().column),
                            _ => None,
                        },
                        crate::relation::pending::Position::Whole => None,
                    })
                    .collect();
                for item in on.iter() {
                    let Reduction::Out(
                        crate::relation::pending::Position::Authored { expr, naming }
                        | crate::relation::pending::Position::Expanded { expr, naming },
                    ) = item
                    else {
                        continue;
                    };
                    let value = expr;
                    match crate::defuse::bound_use::judge_grade(
                        fold.core,
                        crate::defuse::bound_use::CallableGrade::Reducing,
                        &group_keys,
                        value,
                    ) {
                        crate::defuse::bound_use::ReductionStanding::Lawful => {}
                        crate::defuse::bound_use::ReductionStanding::PerRow => {
                            let label = naming
                                .as_ref()
                                .map(|name| format!("'{name}'"))
                                .unwrap_or_else(|| "this member".to_string());
                            return Err(DelightQLError::validation_error_categorized(
                                "constraint/implicit_aggregation",
                                format!(
                                    "the group has many rows and the member {label} has one \
                                     slot: a value with one answer per row cannot stand alone \
                                     in a reduction, and there is no implicit aggregation, ever"
                                ),
                                "write the reduction, e.g. `sum:(expr)`",
                            ));
                        }
                    }
                }
            }

            // THE IN IS THE HEADING WITNESS, read here where the group's
            // membership predicates are in scope.
            for item in on.iter_mut() {
                if let Reduction::Pivot(pivot) = item {
                    let (source, values) = match pivot_values_for(
                        &pivot.pivot_key,
                        pivot_in_values,
                        &fold.core.identities,
                    ) {
                        PivotValueJudgment::Ready(source, values) => (source, values),
                        PivotValueJudgment::Unnameable(source) => {
                            let key = pivot_key_teaching(source, &fold.core.identities);
                            return Err(DelightQLError::validation_error(
                                format!(
                                    "Pivot key '{key}' has a matching IN predicate whose values cannot name output columns"
                                ),
                                "Use string values for the pivot heading until the numeric-key ruling is settled",
                            ));
                        }
                        PivotValueJudgment::Missing => {
                            return Err(DelightQLError::validation_error(
                                "Pivot key requires a matching IN predicate",
                                "Add an IN predicate with literal values for a referenced column",
                            ))
                        }
                    };
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
                resolved_delegates.push(Delegate { payload, order });
            }

            // OUTWARD-ACTING: a metadata group summarizes the group of
            // rows its record stands for. With no keys written, `~> {`
            // makes one record PER ROW, and a single row is not a group.
            if by.is_empty() {
                for item in &on {
                    let Reduction::Out(out) = item else { continue };
                    let Some(crate::pipeline::asts::core::DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Enclyph(
                            crate::pipeline::asts::core::Enclyph::Record(record),
                        ),
                    )) = out.value()
                    else {
                        continue;
                    };
                    if let Some(key) = record.members.iter().find_map(|member| match member {
                        ast_resolved::RecordMember::Metadata { key, .. } => Some(key.clone()),
                        _ => None,
                    }) {
                        return Err(DelightQLError::validation_error_categorized(
                            "constraint/metadata_per_row",
                            format!(
                                "`~> {{` makes one record PER ROW, and the metadata group \
                                 '{key}' inside it has no group of rows to summarize"
                            ),
                            "write the grouping keys, so the record stands for a group: \
                             `%(keys ~> {{ … }})`",
                        ));
                    }
                }
            }

            // What the duplicate check below needs to know about pivots,
            // read off the description before the authority consumes it.
            let pivot_names: Vec<crate::names::Sym> = on
                .iter()
                .filter_map(|item| match item {
                    Reduction::Pivot(pivot) => Some(&pivot.values),
                    Reduction::Out(_) | Reduction::Metadata { .. } => None,
                })
                .flatten()
                .map(|value| {
                    fold.core
                        .identities
                        .canonical(fold.core.identities.intern(value, false))
                })
                .collect();

            let (step, output) = fold.core.identities.authority().bind(Pending::Group {
                input,
                keys: by,
                shape: GroupShape::Reduce {
                    reductions: on,
                    delegates: resolved_delegates,
                },
            })?;
            (step, pivot_names, output, false, Vec::new())
        }
    };
    if distinct {
        check_duplicate_user_names(&fold.core.identities, &output, &intents)?;
    }
    if let Some(duplicate) = duplicate_published(&fold.core.identities, &output) {
        if pivot_names.contains(&duplicate) {
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

    Ok((step, output))
}
