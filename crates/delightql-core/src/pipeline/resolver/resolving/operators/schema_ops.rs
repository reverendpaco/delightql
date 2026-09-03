// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};
use delightql_types::SqlIdentifier;

use super::super::domain_expressions::projection::resolve_expressions_via_fold;
use super::helpers::emit_validation_warning;
use crate::pipeline::asts::core::{NamedReference, Reference};

fn resolved_column(expression: &ast_resolved::DomainExpression) -> Option<crate::relation::PortId> {
    match expression {
        ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
            ColumnOccurrence { column, .. },
        ))) => Some(*column),
        _ => None,
    }
}

/// The position of THIS heading a resolved rename source names.
///
/// A qualified source reaches the lexical binding — the alias's own position
/// — and the step standing over it republished that position into one of its
/// own. Construction recorded the carry, so the rename lands on the position
/// this heading actually publishes rather than refusing a source it can see.
fn current_occurrence(
    identities: &crate::relation::Planning,
    available: &[crate::relation::PortId],
    source: crate::relation::PortId,
) -> Result<crate::relation::PortId> {
    crate::relation::landed_in(identities, available, source)?.ok_or_else(|| {
        DelightQLError::parse_error(
            "A resolved rename source has no occurrence in the pipe heading",
        )
    })
}

fn unique_named(
    identities: &crate::relation::Planning,
    available: &[crate::relation::PortId],
    name: &SqlIdentifier,
) -> Option<crate::relation::PortId> {
    // As written: the strop is what makes the name case-sensitive, and a
    // dequalifying step reaches the lvar the author spelled.
    let spelling = identities.intern(name.as_str(), name.is_stropped());
    let name = identities.canonical(spelling);
    let matches: Vec<_> = available
        .iter()
        .copied()
        .filter(|port| identities.published_sym(port.column()) == Some(name))
        .collect();
    match matches.as_slice() {
        [column] => Some(*column),
        _ => None,
    }
}

pub(super) fn resolve_project_out(
    fold: &mut ResolverFold,
    selector: Vec<ast_unresolved::SelectorItem>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    let (resolved_selector, removed) =
        super::super::domain_expressions::projection::resolve_selector_via_fold(
            fold, selector, available, true,
        )?;
    // Removal is slot-exact: each removed target selects ONE slot through
    // the correspondence authority, and a sibling publication of the same
    // value stays.
    let kept: Vec<_> = available
        .iter()
        .copied()
        .filter(|candidate| !removed.contains(candidate))
        .collect();

    if kept.is_empty() {
        return Err(DelightQLError::parse_error(
            "Cannot remove all columns - would create empty table",
        ));
    }
    if removed.is_empty() && !available.is_empty() {
        emit_validation_warning("ProjectOut pattern matched no columns - no changes made");
    }

    fold.core
        .identities
        .authority()
        .bind(crate::relation::pending::Pending::ProjectOut {
            input,
            selector: resolved_selector,
            removed,
        })
}

pub(super) fn resolve_rename_cover(
    fold: &mut ResolverFold,
    specs: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::RenameSpec>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    let mut renames: Vec<(crate::relation::PortId, crate::names::Spelling)> = Vec::new();
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
            let column = current_occurrence(&fold.core.identities, available, source)?;
            let position = available
                .iter()
                .position(|candidate| *candidate == column)
                .map(|position| position + 1);
            let new_name = match &spec.to {
                ast_unresolved::RenameTarget::Identifier(literal) => {
                    fold.core.identities.intern(literal, false)
                }
                ast_unresolved::RenameTarget::Template(alias) => match alias {
                    ast_unresolved::ColumnAlias::Template(template) => fold
                        .core
                        .identities
                        .expand_template(
                            column.column(),
                            &template.template,
                            position.expect("resolved column has an input position"),
                        )
                        .ok_or_else(|| {
                            DelightQLError::parse_error("Cannot expand {@} for an anonymous column")
                        })?,
                    ast_unresolved::ColumnAlias::Literal(literal) => {
                        fold.core.identities.intern(literal, false)
                    }
                },
            };
            renames.push((column, new_name));
        }
    }

    let target_names: Vec<_> = renames
        .iter()
        .map(|(_, name)| fold.core.identities.canonical(*name))
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
                && fold.core.identities.published_sym(column.column()) == Some(*target)
        });
        if duplicate_target || passthrough_collision {
            return Err(DelightQLError::validation_error_categorized(
                "constraint",
                "Rename targets must be unique and must not collide with passthrough columns",
                "in rename-cover operator",
            ));
        }
    }

    fold.core
        .identities
        .authority()
        .bind(crate::relation::pending::Pending::Rename {
            input,
            renames: renames
                .into_iter()
                .map(|(source, to)| crate::relation::form::RenameSlot { source, to })
                .collect(),
        })
}

pub(in crate::pipeline::resolver) fn resolve_reposition(
    fold: &mut ResolverFold,
    moves: Vec<ast_unresolved::RepositionSpec>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    let mut pending_moves = Vec::new();
    for spec in moves {
        let resolved = resolve_expressions_via_fold(
            fold,
            vec![ast_unresolved::DomainExpression::Reference(spec.column)],
            available,
        )?
        .into_iter()
        .next()
        .expect("one reposition reference resolves to one expression");
        if resolved_column(&resolved).is_none() {
            return Err(DelightQLError::parse_error(
                "Reposition only supports columns and ordinals",
            ));
        }
        let ast_resolved::DomainExpression::Reference(reference) = resolved else {
            return Err(DelightQLError::parse_error(
                "Reposition only supports columns and ordinals",
            ));
        };
        pending_moves.push(crate::relation::pending::Move {
            reference,
            position: spec.position,
        });
    }
    fold.core
        .identities
        .authority()
        .bind(crate::relation::pending::Pending::Reposition {
            input,
            moves: pending_moves,
        })
}

pub(in crate::pipeline::resolver) fn resolve_witness(
    input: crate::relation::SemanticRelation,
    authored: crate::pipeline::asts::core::Polarity,
    identities: &crate::relation::Planning,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    identities
        .authority()
        .bind(crate::relation::pending::Pending::Witness {
            input,
            polarity: authored,
        })
}

pub(in crate::pipeline::resolver) fn resolve_signed_witness(
    input: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    identities
        .authority()
        .bind(crate::relation::pending::Pending::SignedWitness { input })
}

pub(in crate::pipeline::resolver) fn resolve_meta_ize(
    input: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    identities
        .authority()
        .bind(crate::relation::pending::Pending::Meta { input })
}

/// The one access resolver: `*`, `.(a, b)`, and `.*`.
///
/// Every access is heading-PRESERVING here — it qualifies, dequalifies, or
/// activates the heading it was handed, and publishes exactly those columns.
/// Only the named form has anything to check, and it checks the one thing a
/// name can be wrong about: that the heading answers to it, once.
pub(in crate::pipeline::resolver) fn resolve_access(
    access: ast_resolved::Access,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
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
    identities
        .authority()
        .bind(crate::relation::pending::Pending::Access { input, access })
}

pub(in crate::pipeline::resolver) fn resolve_interior_drill_down(
    column: String,
    drilled: crate::relation::PortId,
    glob: bool,
    columns: Vec<String>,
    unresolved_groundings: Vec<(String, String)>,
    input: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    // The drill carrier still holds characters rather than a written name;
    // reading it unstropped is what it has always meant.
    // The nest was addressed through the frontier where the step stands.
    if crate::relation::interior_conflict(identities, drilled) {
        return Err(DelightQLError::validation_error_categorized(
            "effect/ledger/mixed_release",
            format!(
                "releasing '{column}' across ledger arms whose declared interior headings differ"
            ),
            "narrow the ledger to arms with one interior heading before releasing",
        ));
    }
    let interior = crate::relation::interior(identities, drilled)?.ok_or_else(|| {
        DelightQLError::validation_error(
            format!("Interior drill-down: column '{column}' has no known interior heading"),
            "Use ~= destructuring for values without a statically known heading",
        )
    })?;
    let interior_columns = crate::relation::published_ports(identities, &interior)?;
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
    let renames: Vec<_> = selected
        .iter()
        .filter_map(|(source, alias)| {
            alias.map(|alias| crate::relation::form::RenameSlot {
                source: *source,
                to: identities.intern(alias, false),
            })
        })
        .collect();
    let selected_relation = if renames.is_empty() {
        interior
    } else {
        identities
            .authority()
            .derive(crate::relation::RelForm::Rename(
                crate::relation::form::RenameSpec {
                    input: interior,
                    why: crate::relation::form::ProjectWhy::Restate,
                    renames: &renames,
                },
            ))?
    };
    let renamed_ports = crate::relation::published_ports(identities, &selected_relation)?;
    let selected_columns: Vec<_> = selected
        .iter()
        .filter_map(|(source, alias)| {
            let position = crate::relation::published_ports(identities, &interior)
                .ok()?
                .iter()
                .position(|candidate| candidate == source)?;
            let port = renamed_ports[position];
            (alias.is_none() || *alias != Some("_")).then_some(port)
        })
        .collect();
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
    let resolved_groundings = unresolved_groundings
        .iter()
        .filter_map(|(position, value)| {
            let position: usize = position.parse().ok()?;
            interior_columns.iter().nth(position).map(|column| {
                crate::pipeline::asts::core::operators::ResolvedInteriorGrounding {
                    column: *column,
                    value: value.clone(),
                }
            })
        })
        .collect();
    identities
        .authority()
        .bind(crate::relation::pending::Pending::Drill {
            input,
            drill: crate::pipeline::asts::core::operators::BoundDrill {
                column: drilled,
                columns: selected_columns,
                selection: if glob {
                    crate::relation::form::DrillSelection::Whole
                } else {
                    crate::relation::form::DrillSelection::Bound
                },
                groundings: resolved_groundings,
            },
        })
}

pub(in crate::pipeline::resolver) fn resolve_narrowing_destructure(
    nest: crate::pipeline::asts::core::ColumnOccurrence,
    spelled: &str,
    pattern: ast_unresolved::RecordPattern,
    input: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    // The nest was addressed through the frontier where the step stands.
    let source = nest.column;
    if identities
        .facts(source.column())
        .declared_type
        .as_deref()
        .is_some_and(crate::pipeline::asts::core::metadata::is_plainly_scalar_declaration)
    {
        return Err(DelightQLError::ValidationError {
            message: format!(
                "cannot narrow into column '{}': a plain scalar has no rows to iterate",
                spelled
            ),
            context: "resolver::narrowing_destructure".to_string(),
            subcategory: Some(crate::uri_registry::subcat::COMPOUND_SCALAR_COLUMN),
        });
    }

    identities
        .authority()
        .bind(crate::relation::pending::Pending::Narrow {
            input,
            nest,
            pattern,
        })
}
