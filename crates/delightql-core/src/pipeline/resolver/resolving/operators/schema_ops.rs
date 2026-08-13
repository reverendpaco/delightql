// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::names::{
    Addressing, ColId, ColumnOrigin, Computation, Hint, Registry, Republish, ScopeId, ScopeOrigin,
    ValueFacts, WrapReason,
};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};
use delightql_types::SqlIdentifier;

use super::super::domain_expressions::projection::resolve_expressions_via_fold;
use super::helpers::emit_validation_warning;
use crate::pipeline::asts::core::{NamedReference, Reference};

fn output_scope(identities: &Registry, available: &[ColId], why: WrapReason) -> ScopeId {
    match identities.common_scope(available) {
        Some(input) => identities.mint_derived_scope(ScopeOrigin::Wrap { input, why }, Hint::None),
        None => identities.mint_scope(ScopeOrigin::AnonRelation, Hint::None, None),
    }
}

fn minted_column(identities: &Registry, scope: ScopeId, name: &str, origin: ColumnOrigin) -> ColId {
    let spelling = identities.intern(name, false);
    identities.mint_column(
        scope,
        origin,
        Some(spelling),
        Addressing::Published,
        ValueFacts::default(),
    )
}

fn resolved_column(expression: &ast_resolved::DomainExpression) -> Option<ColId> {
    match expression {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Some(*column),
        _ => None,
    }
}

fn current_occurrence(identities: &Registry, available: &[ColId], source: ColId) -> Result<ColId> {
    let matches: Vec<_> = available
        .iter()
        .copied()
        .filter(|candidate| identities.republishes(*candidate, source))
        .collect();
    match matches.as_slice() {
        [column] => Ok(*column),
        [] => Err(DelightQLError::parse_error(
            "A resolved rename source has no occurrence in the pipe heading",
        )),
        _ => Err(DelightQLError::validation_error_categorized(
            "constraint",
            "A rename source reaches more than one column in the pipe heading",
            "in rename-cover operator",
        )),
    }
}

fn unique_named(identities: &Registry, available: &[ColId], name: &SqlIdentifier) -> Option<ColId> {
    // As written: the strop is what makes the name case-sensitive, and a
    // dequalifying step reaches the lvar the author spelled.
    let spelling = identities.intern(name.as_str(), name.is_stropped());
    let name = identities.canonical(spelling);
    let matches: Vec<_> = available
        .iter()
        .copied()
        .filter(|column| identities.published_sym(*column) == Some(name))
        .collect();
    match matches.as_slice() {
        [column] => Some(*column),
        _ => None,
    }
}

fn republish_all(identities: &Registry, available: &[ColId], scope: ScopeId) -> Vec<ColId> {
    available
        .iter()
        .map(|column| {
            identities.republish_column(
                *column,
                scope,
                Republish::Passthrough,
                identities.published(*column),
                identities.addressing(*column),
                |_| {},
            )
        })
        .collect()
}

pub(super) fn resolve_project_out(
    fold: &mut ResolverFold,
    selector: Vec<ast_unresolved::SelectorItem>,
    available: &[ColId],
) -> Result<(ast_resolved::PipeOp, Vec<ColId>)> {
    let (resolved_selector, removed) =
        super::super::domain_expressions::projection::resolve_selector_via_fold(
            fold, selector, available, true,
        )?;
    let kept: Vec<_> = available
        .iter()
        .copied()
        .filter(|candidate| {
            !removed
                .iter()
                .any(|removed| fold.registry.identities.same_value(*candidate, *removed))
        })
        .collect();

    if kept.is_empty() {
        return Err(DelightQLError::parse_error(
            "Cannot remove all columns - would create empty table",
        ));
    }
    if removed.is_empty() && !available.is_empty() {
        emit_validation_warning("ProjectOut pattern matched no columns - no changes made");
    }

    let scope = output_scope(&fold.registry.identities, available, WrapReason::Projection);
    let output = republish_all(&fold.registry.identities, &kept, scope);
    Ok((
        ast_resolved::PipeOp::ProjectOut(resolved_selector),
        output,
    ))
}

pub(super) fn resolve_rename_cover(
    fold: &mut ResolverFold,
    specs: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::RenameSpec>,
    available: &[ColId],
) -> Result<(ast_resolved::PipeOp, Vec<ColId>)> {
    let mut renames: Vec<(ColId, crate::names::Spelling)> = Vec::new();
    for spec in specs.into_vec() {
        // A RENAME SOURCE ADDRESSES COLUMNS: one reference, or the several a
        // regex or glob covers. It reaches them through the one expansion
        // authority every selector uses.
        let resolved = match spec.from {
            ast_unresolved::RenameSource::Reference(reference) => resolve_expressions_via_fold(
                fold,
                vec![ast_unresolved::DomainExpression::Reference(reference)],
                available,
            )?,
            ast_unresolved::RenameSource::Regex(regex) => {
                super::super::domain_expressions::projection::expand_spread(
                    fold,
                    &ast_unresolved::Spread::Regex(regex),
                    available,
                    false,
                )?
            }
            ast_unresolved::RenameSource::Glob(glob) => {
                super::super::domain_expressions::projection::expand_spread(
                    fold,
                    &ast_unresolved::Spread::Glob(glob),
                    available,
                    false,
                )?
            }
        };
        for expression in resolved {
            let Some(source) = resolved_column(&expression) else {
                continue;
            };
            // A qualified reference names the occurrence at the relation it
            // addresses, while a pipe operates on the occurrence exported by
            // the current heading. Rename the unique descendant in that
            // heading so an unrelated column with the same spelling remains
            // untouched.
            let column = current_occurrence(&fold.registry.identities, available, source)?;
            let position = available
                .iter()
                .position(|candidate| *candidate == column)
                .map(|position| position + 1);
            let new_name = match &spec.to {
                ast_unresolved::RenameTarget::Identifier(literal) => {
                    fold.registry.identities.intern(literal, false)
                }
                ast_unresolved::RenameTarget::Template(alias) => match alias {
                    ast_unresolved::ColumnAlias::Template(template) => fold
                        .registry
                        .identities
                        .expand_template(
                            column,
                            &template.template,
                            position.expect("resolved column has an input position"),
                        )
                        .ok_or_else(|| {
                            DelightQLError::parse_error("Cannot expand {@} for an anonymous column")
                        })?,
                    ast_unresolved::ColumnAlias::Literal(literal) => {
                        fold.registry.identities.intern(literal, false)
                    }
                },
            };
            renames.push((column, new_name));
        }
    }

    let target_names: Vec<_> = renames
        .iter()
        .map(|(_, name)| fold.registry.identities.canonical(*name))
        .collect();
    let duplicate_source = renames.iter().enumerate().any(|(index, (source, _))| {
        renames
            .iter()
            .enumerate()
            .any(|(other, (candidate, _))| other != index && candidate == source)
    });
    if duplicate_source {
        return Err(DelightQLError::validation_error_categorized(
            "constraint",
            "Each rename source must be named once",
            "in rename-cover operator",
        ));
    }
    for (index, target) in target_names.iter().enumerate() {
        let duplicate_target = target_names
            .iter()
            .enumerate()
            .any(|(other, candidate)| other != index && candidate == target);
        let passthrough_collision = available.iter().any(|column| {
            !renames.iter().any(|(renamed, _)| renamed == column)
                && fold.registry.identities.published_sym(*column) == Some(*target)
        });
        if duplicate_target || passthrough_collision {
            return Err(DelightQLError::validation_error_categorized(
                "constraint",
                "Rename targets must be unique and must not collide with passthrough columns",
                "in rename-cover operator",
            ));
        }
    }

    let scope = output_scope(&fold.registry.identities, available, WrapReason::Projection);
    let mut output = Vec::with_capacity(available.len());
    for column in available {
        let replacement = renames
            .iter()
            .filter(|(source, _)| source == column)
            .map(|(_, spelling)| *spelling)
            .collect::<Vec<_>>();
        if let [spelling] = replacement.as_slice() {
            output.push(fold.registry.identities.republish_column(
                *column,
                scope,
                Republish::Rename,
                Some(*spelling),
                Addressing::Published,
                |_| {},
            ));
        } else {
            output.push(fold.registry.identities.republish_column(
                *column,
                scope,
                Republish::Passthrough,
                fold.registry.identities.published(*column),
                fold.registry.identities.addressing(*column),
                |_| {},
            ));
        }
    }

    let resolved_specs: Vec<_> = renames
        .into_iter()
        .map(|(column, to)| ast_resolved::RenameSpec {
            from: crate::pipeline::asts::core::RenameSource::Reference(Reference::Named(
                NamedReference(ColumnOccurrence {
                    column,
                    explicit_qualifier: false,
                }),
            )),
            to,
        })
        .collect();
    let resolved_specs = crate::pipeline::asts::vocabulary::Vec1::try_from_vec(resolved_specs)
        .expect("a rename source that matches nothing refused during resolution");
    Ok((
        ast_resolved::PipeOp::Rename(resolved_specs),
        output,
    ))
}

pub(in crate::pipeline::resolver) fn resolve_reposition(
    fold: &mut ResolverFold,
    moves: Vec<ast_unresolved::RepositionSpec>,
    available: &[ColId],
) -> Result<(Vec<ast_resolved::RepositionSpec>, Vec<ColId>)> {
    let count = available.len();
    let mut result = vec![None; count];
    let mut moved = Vec::new();
    let mut resolved_moves = Vec::new();

    for spec in moves {
        let resolved = resolve_expressions_via_fold(
            fold,
            vec![ast_unresolved::DomainExpression::Reference(spec.column)],
            available,
        )?
        .into_iter()
        .next()
        .expect("one reposition reference resolves to one expression");
        let column = resolved_column(&resolved).ok_or_else(|| {
            DelightQLError::parse_error("Reposition only supports columns and ordinals")
        })?;
        let ast_resolved::DomainExpression::Reference(reference) = resolved else {
            return Err(DelightQLError::parse_error(
                "Reposition only supports columns and ordinals",
            ));
        };
        let source = available
            .iter()
            .position(|candidate| *candidate == column)
            .ok_or_else(|| DelightQLError::parse_error("Reposition column is not in the input"))?;
        if moved.contains(&source) {
            return Err(DelightQLError::parse_error(
                "A column appears multiple times in reposition",
            ));
        }
        let target = if spec.position < 0 {
            count as i32 + spec.position
        } else {
            spec.position - 1
        };
        if target < 0 || target >= count as i32 {
            return Err(DelightQLError::parse_error(format!(
                "Position {} is out of range for {} columns",
                spec.position, count
            )));
        }
        let target = target as usize;
        if result[target].is_some() {
            return Err(DelightQLError::parse_error(
                "Multiple columns cannot target the same position",
            ));
        }
        result[target] = Some(column);
        moved.push(source);
        resolved_moves.push(ast_resolved::RepositionSpec {
            column: reference,
            position: spec.position,
        });
    }

    let remaining: Vec<_> = available
        .iter()
        .enumerate()
        .filter(|(index, _)| !moved.contains(index))
        .map(|(_, column)| *column)
        .collect();
    let mut remaining = remaining.into_iter();
    for slot in &mut result {
        if slot.is_none() {
            *slot = remaining.next();
        }
    }
    let reordered: Vec<_> = result.into_iter().flatten().collect();
    let scope = output_scope(&fold.registry.identities, available, WrapReason::Projection);
    Ok((
        resolved_moves,
        republish_all(&fold.registry.identities, &reordered, scope),
    ))
}

pub(in crate::pipeline::resolver) fn resolve_witness(
    available: &[ColId],
    identities: &Registry,
) -> Result<Vec<ColId>> {
    let scope = output_scope(identities, available, WrapReason::Witness);
    let output = vec![minted_column(
        identities,
        scope,
        "met",
        ColumnOrigin::Computed {
            via: Computation::Operator,
        },
    )];
    Ok(output)
}

pub(in crate::pipeline::resolver) fn resolve_signed_witness(
    available: &[ColId],
    identities: &Registry,
) -> Result<Vec<ColId>> {
    let mut ordinal = 1;
    let met_name = loop {
        let candidate = if ordinal == 1 {
            "met".to_string()
        } else {
            format!("met_{ordinal}")
        };
        let spelling = identities.intern(&candidate, false);
        let name = identities.canonical(spelling);
        if !available
            .iter()
            .any(|column| identities.published_sym(*column) == Some(name))
        {
            break candidate;
        }
        ordinal += 1;
    };
    let scope = output_scope(identities, available, WrapReason::Witness);
    let mut output = republish_all(identities, available, scope);
    output.push(minted_column(
        identities,
        scope,
        &met_name,
        ColumnOrigin::Computed {
            via: Computation::Operator,
        },
    ));
    Ok(output)
}

pub(in crate::pipeline::resolver) fn resolve_meta_ize(
    available: &[ColId],
    identities: &Registry,
) -> Result<Vec<ColId>> {
    let scope = output_scope(identities, available, WrapReason::Meta);
    let output = ["scope", "column_name", "ordinal"]
        .into_iter()
        .map(|name| {
            minted_column(
                identities,
                scope,
                name,
                ColumnOrigin::Computed {
                    via: Computation::Operator,
                },
            )
        })
        .collect();
    Ok(output)
}

/// The one access resolver: `*`, `.(a, b)`, and `.*`.
///
/// Every access is heading-PRESERVING here — it qualifies, dequalifies, or
/// activates the heading it was handed, and publishes exactly those columns.
/// Only the named form has anything to check, and it checks the one thing a
/// name can be wrong about: that the heading answers to it, once.
pub(in crate::pipeline::resolver) fn resolve_access(
    access: ast_resolved::Access,
    available: &[ColId],
    identities: &Registry,
) -> Result<(ast_resolved::Access, Vec<ColId>)> {
    if let ast_resolved::Access::Dequalify(columns) = &access {
        for name in columns {
            if unique_named(identities, available, name).is_none() {
                return Err(DelightQLError::column_not_found_error(
                    name.as_str(),
                    "in USING operator",
                ));
            }
        }
    }
    Ok((access, available.to_vec()))
}

pub(in crate::pipeline::resolver) fn resolve_interior_drill_down(
    column: String,
    glob: bool,
    columns: Vec<String>,
    unresolved_groundings: Vec<(String, String)>,
    available: &[ColId],
    identities: &Registry,
) -> Result<(crate::pipeline::asts::core::operators::BoundDrill, Vec<ColId>)> {
    // The drill carrier still holds characters rather than a written name;
    // reading it unstropped is what it has always meant.
    let drilled = unique_named(identities, available, &SqlIdentifier::new(column.clone()))
        .ok_or_else(|| {
            DelightQLError::validation_error(
                format!("Interior drill-down: column '{column}' not found in input relation"),
                "Check that the column names a single tree-group column",
            )
        })?;
    let facts = identities.facts(drilled);
    if facts.interior_conflict {
        return Err(DelightQLError::validation_error_categorized(
            "effect/ledger/mixed_release",
            format!(
                "releasing '{column}' across ledger arms whose declared interior headings differ"
            ),
            "narrow the ledger to arms with one interior heading before releasing",
        ));
    }
    let interior = facts.interior.ok_or_else(|| {
        DelightQLError::validation_error(
            format!("Interior drill-down: column '{column}' has no known interior heading"),
            "Use ~= destructuring for values without a statically known heading",
        )
    })?;
    let interior_columns = identities.known_heading(interior)?;
    let selected = if glob {
        interior_columns
            .iter()
            .copied()
            .map(|column| (column, None))
            .collect::<Vec<_>>()
    } else {
        // An argumentative drill-down is an ARITY BINDER, never a projection
        // list: it binds the interior's columns left to right, one name per
        // column, exactly as `employees(id, name)` binds a base relation. A
        // name here is the name being GIVEN to a position, so whether the
        // interior already publishes that name decides nothing — to keep a
        // subset of a wider interior, expand with `(*)` and project, or narrow
        // with braces.
        if columns.len() != interior_columns.len() {
            return Err(DelightQLError::validation_error(
                format!(
                    "drill-down into '{column}' names {} columns; its interior has {}",
                    columns.len(),
                    interior_columns.len()
                ),
                "An argumentative drill-down binds the interior's columns by position, one \
                 name each. Expand with (*) and project to keep a subset.",
            ));
        }
        // The names bound here are programmer-authored and a heading, so they
        // obey the uniqueness a projection's do — without this the second of
        // two `d`s is silently published as `d_2`. `_` is exempt: it binds
        // nothing, so any number of them collide over nothing.
        let mut bound = Vec::new();
        for name in columns.iter().filter(|name| *name != "_") {
            let spelling = identities.canonical(identities.intern(name, false));
            if bound.contains(&spelling) {
                return Err(DelightQLError::validation_error(
                    format!(
                        "Duplicate column '{name}' in drill-down into '{column}': \
                         programmer-authored names must be unique. Rename one to disambiguate"
                    ),
                    "in output schema",
                ));
            }
            bound.push(spelling);
        }
        interior_columns
            .iter()
            .copied()
            .zip(columns.iter())
            .filter(|(_, alias)| *alias != "_")
            .map(|(column, alias)| (column, Some(alias.as_str())))
            .collect()
    };
    let selected_columns = selected
        .iter()
        .map(|(column, _)| *column)
        .collect::<Vec<_>>();

    let scope = output_scope(identities, available, WrapReason::Projection);
    let mut output = available
        .iter()
        .copied()
        .filter(|candidate| *candidate != drilled)
        .map(|source| {
            identities.republish_column(
                source,
                scope,
                Republish::Passthrough,
                identities.published(source),
                identities.addressing(source),
                |_| {},
            )
        })
        .collect::<Vec<_>>();
    // The interior is a relation the row carries, and it answers to the name
    // of the column it was drilled out of: `t(*).items(*)` puts `items` in
    // scope as a qualifier, so `items.x` names the interior's `x` and not an
    // `x` the enclosing level also publishes. Without it an interior sharing
    // a name with the level above it is unreachable, and `R |> .items(*)`
    // does not resolve at all — that form normalizes to a qualified glob the
    // compiler writes for itself.
    //
    // The qualifier rides the COLUMN, not a scope of its own: this operator
    // republishes both halves into one output and the pipe stage republishes
    // again, and a scope survives neither step where addressing survives
    // both. The published name is untouched either way — a glob keeps the
    // interior's own spelling, a binder keeps the one it bound.
    let answers_under = identities.published_sym(drilled);
    for (source, alias) in selected {
        output.push(
            identities.republish_column(
                source,
                scope,
                if alias.is_some() {
                    Republish::Rename
                } else {
                    Republish::Passthrough
                },
                alias
                    .map(|name| identities.intern(name, false))
                    .or_else(|| identities.published(source)),
                answers_under.map_or(
                    if alias.is_some() {
                        Addressing::Bare
                    } else {
                        Addressing::Published
                    },
                    Addressing::BareAnswering,
                ),
                |_| {},
            ),
        );
    }

    let resolved_groundings = unresolved_groundings
        .iter()
        .filter_map(|(position, value)| {
            let position: usize = position.parse().ok()?;
            interior_columns.in_order().nth(position).map(|column| {
                crate::pipeline::asts::core::operators::ResolvedInteriorGrounding {
                    column: *column,
                    value: value.clone(),
                }
            })
        })
        .collect();
    Ok((
        crate::pipeline::asts::core::operators::BoundDrill {
            column: drilled,
            columns: selected_columns,
            groundings: resolved_groundings,
        },
        output,
    ))
}

/// A narrowing, resolved: the bound nest, the pattern read through the
/// shared path authorities, and the columns it publishes.
pub(in crate::pipeline::resolver) struct ResolvedNarrowing {
    pub nest: crate::pipeline::asts::core::Reference<crate::pipeline::asts::core::Resolved>,
    pub pattern:
        crate::pipeline::asts::core::RecordPattern<crate::pipeline::asts::core::Resolved>,
    pub schema: Vec<crate::pipeline::asts::core::DestructureMapping>,
}

pub(in crate::pipeline::resolver) fn resolve_narrowing_destructure(
    nest: ast_unresolved::Reference,
    pattern: ast_unresolved::RecordPattern,
    available: &[ColId],
    identities: &Registry,
) -> Result<(ResolvedNarrowing, Vec<ColId>)> {
    use crate::pipeline::asts::core::{NamedReference, Reference, TreePattern};

    let Reference::Named(NamedReference(authored)) = &nest else {
        return Err(DelightQLError::validation_error(
            "a narrowing addresses its nest by name".to_string(),
            "write the column's name",
        ));
    };
    let source = unique_named(identities, available, &authored.name).ok_or_else(|| {
        DelightQLError::validation_error(
            format!(
                "Narrowing destructure: column '{}' not found in input relation",
                authored.name
            ),
            "Check that the column name matches one input column",
        )
    })?;
    if identities
        .facts(source)
        .declared_type
        .as_deref()
        .is_some_and(crate::pipeline::asts::core::metadata::is_plainly_scalar_declaration)
    {
        return Err(DelightQLError::ValidationError {
            message: format!(
                "cannot narrow into column '{}': a plain scalar has no rows to iterate",
                authored.name
            ),
            context: "resolver::narrowing_destructure".to_string(),
            subcategory: Some(crate::uri_registry::subcat::COMPOUND_SCALAR_COLUMN),
        });
    }

    // ONE MAPPING AUTHORITY. The keys a pattern reads and the names it
    // publishes come from the pattern members themselves, exactly as an
    // ordinary destructure's do — so a numeric step, a multi-step reach and
    // a flattened name mean the same thing on both roads.
    let declared = TreePattern::Record(pattern.clone());
    let mappings =
        crate::pipeline::resolver::resolving::predicates::extract_key_mappings_from_unresolved_pattern(
            &declared,
        )?;
    let scope = output_scope(identities, available, WrapReason::Projection);
    let mut output = Vec::with_capacity(mappings.len());
    let mut columns = std::collections::HashMap::new();
    let mut schema = Vec::with_capacity(mappings.len());
    for (json_key, published) in mappings {
        let column = minted_column(
            identities,
            scope,
            &published,
            ColumnOrigin::Computed {
                via: Computation::Operator,
            },
        );
        columns.insert(
            identities.canonical(identities.intern(&published, false)),
            column,
        );
        output.push(column);
        schema.push(crate::pipeline::asts::core::DestructureMapping { json_key, column });
    }
    let resolved =
        crate::pipeline::resolver::resolving::predicates::convert_destructure_pattern_to_resolved(
            declared, &columns, identities,
        )?;
    let TreePattern::Record(pattern) = resolved else {
        unreachable!("a record pattern converts to a record pattern");
    };
    Ok((
        ResolvedNarrowing {
            nest: Reference::Named(NamedReference(
                crate::pipeline::asts::core::ColumnOccurrence {
                    column: source,
                    explicit_qualifier: authored.qualifier.is_some(),
                },
            )),
            pattern,
            schema,
        },
        output,
    ))
}
