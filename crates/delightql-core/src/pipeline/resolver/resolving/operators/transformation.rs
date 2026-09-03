// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::{ast_resolved, ast_unresolved};

use super::super::helpers::{build_concat_chain_with_placeholders, convert_column_alias};
use super::helpers::emit_validation_warning;
use crate::pipeline::asts::core::{NamedReference, Reference};

pub(super) fn resolve_map_cover_via_fold(
    fold: &mut ResolverFold,
    function: ast_unresolved::Callable,
    columns: Vec<ast_unresolved::SelectorItem>,
    conditioned_on: Option<Box<ast_unresolved::TruthExpression>>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    let (columns, covered) =
        super::super::domain_expressions::projection::resolve_selector_via_fold(
            fold, columns, available, true,
        )?;
    if columns.is_empty() && !available.is_empty() {
        emit_validation_warning("MapCover pattern matched no columns - no transformation applied");
    }
    // THE COVER IS THE APPLYING POSITION, and it applies HERE: one closed
    // resolved expression per covered cell. The callable is spent by that
    // application — the resolved carrier holds cells, no callable at all.
    let cells = covered
        .iter()
        .map(|column| {
            Ok(crate::pipeline::asts::core::operators::AppliedCell {
                column: *column,
                expr: apply_callable_to_cell(fold, &function, *column)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let conditioned_on = conditioned_on
        .map(|condition| fold.transform_boolean(*condition).map(Box::new))
        .transpose()?;
    fold.core
        .identities
        .authority()
        .bind(crate::relation::pending::Pending::MapCover {
            input,
            selector: columns,
            guard: conditioned_on,
            cells,
        })
}

/// THE COVER'S APPLYING POSITION, at resolution: apply the authored
/// callable to one covered cell and answer the CLOSED resolved expression
/// it produces. The open leaf is spent here — while the body resolves, the
/// fold's cover cell IS the leaf's value — so no resolved tree carries an
/// unapplied slot.
fn apply_callable_to_cell(
    fold: &mut ResolverFold,
    callable: &ast_unresolved::Callable,
    column: crate::relation::PortId,
) -> Result<ast_resolved::DomainExpression> {
    let cell = ast_resolved::DomainExpression::Reference(Reference::Named(NamedReference(
        ColumnOccurrence::engine(column),
    )));
    let with_cell =
        |fold: &mut ResolverFold,
         resolve: &mut dyn FnMut(&mut ResolverFold) -> Result<ast_resolved::DomainExpression>|
         -> Result<ast_resolved::DomainExpression> {
            let prior = fold.cover_cell.replace(cell.clone());
            let resolved = resolve(fold);
            fold.cover_cell = prior;
            resolved
        };
    match callable {
        // The slot is spent in the body, at every depth — a selection arm,
        // a scalarized relation's interior.
        ast_unresolved::Callable::Lambda(lambda) => with_cell(fold, &mut |fold| {
            fold.transform_domain((*lambda.body).clone())
        }),
        // AN OPEN STRING IS THE CONCAT IT DENOTES; its interpolations
        // resolve with the cell standing in their slots.
        ast_unresolved::Callable::String(template) => with_cell(fold, &mut |fold| {
            build_concat_chain_with_placeholders(fold, template.clone().into_parts())
        }),
        ast_unresolved::Callable::Functor(application) => {
            // A mention of a value DEFINITION applies per cell: the cell
            // takes the definition's final parameter, exactly as an
            // authored lambda's slot takes the flowing value.
            if let Some(applied) =
                crate::defuse::callable::cover_functor_apply_cell(fold, application, cell.clone())?
            {
                return Ok(applied);
            }
            // `@` anywhere in the call — arguments or window — is the slot,
            // and the per-cell resolution spends it.
            if functor_mentions_slot(application) {
                return with_cell(fold, &mut |fold| {
                    fold.transform_domain(ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Standard(application.clone()),
                    ))
                });
            }
            // No slot written: the cell takes the DEFAULT LANDING, the
            // argument row's final place — for a window function only when
            // no argument was written, since its own arguments already say
            // what it reads. A cover applies a callable to a cell the way a
            // pipe applies one to a flowing value, so it spends the same
            // judgment; were it to choose its own position, `$(f:(a))(x)`
            // and `x /-> f:(a)` would be two different applications of one
            // spelling. The landing is written as the slot itself, placed
            // on the AUTHORED call, so the one signature authority judges
            // the rebuilt invocation exactly as it judges an authored
            // spelling.
            use crate::pipeline::asts::core::operators::{CallArguments, ScalarArgument};
            let mut rebuilt = application.clone();
            let lands = match (&rebuilt.window, &rebuilt.call.call().arguments) {
                (Some(_), CallArguments::Scalar(members)) => members.is_empty(),
                (Some(_), CallArguments::None) => true,
                (None, CallArguments::Scalar(_)) | (None, CallArguments::None) => true,
                (_, CallArguments::HigherOrder(_)) => false,
            };
            if lands {
                let slot = ScalarArgument::plain(ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Open(
                        crate::pipeline::asts::core::DomainHole::CompositionInput,
                    ),
                ));
                let arguments = &mut rebuilt.call.call_mut().arguments;
                match arguments {
                    CallArguments::Scalar(members) => {
                        let written = std::mem::take(members);
                        *members = crate::pipeline::normalize::land_final(slot, written);
                    }
                    CallArguments::None => *arguments = CallArguments::Scalar(vec![slot]),
                    CallArguments::HigherOrder(_) => {
                        unreachable!("the landing is not placed in a higher-order group")
                    }
                }
            }
            with_cell(fold, &mut |fold| {
                fold.transform_domain(ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Standard(rebuilt.clone()),
                ))
            })
        }
    }
}

/// Whether the authored functor writes the slot anywhere the application
/// reads — its argument row or its window.
fn functor_mentions_slot(application: &ast_unresolved::StandardApplication) -> bool {
    fn in_expr(expr: &ast_unresolved::DomainExpression) -> bool {
        use crate::pipeline::ast_visit::{AstVisit, Descent};
        struct Finder(bool);
        impl AstVisit<crate::pipeline::asts::core::Unresolved> for Finder {
            fn enter_domain(
                &mut self,
                e: &ast_unresolved::DomainExpression,
            ) -> crate::error::Result<Descent> {
                if matches!(
                    e,
                    ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Open(
                            crate::pipeline::asts::core::DomainHole::CompositionInput,
                        ),
                    )
                ) {
                    self.0 = true;
                    return Ok(Descent::Break);
                }
                Ok(Descent::Continue)
            }
        }
        let mut finder = Finder(false);
        let _ = crate::pipeline::ast_visit::walk_visit_domain(&mut finder, expr);
        finder.0
    }
    let args_mention = application.call().arguments.value_domains().any(in_expr);
    let window_mentions = application.window.as_ref().is_some_and(|window| {
        window.partition.iter().any(in_expr)
            || window.ordering.iter().any(|spec| in_expr(&spec.column))
    });
    args_mention || window_mentions
}

pub(super) fn resolve_transform_via_fold(
    fold: &mut ResolverFold,
    transformations: crate::pipeline::asts::vocabulary::Vec1<ast_unresolved::NamedOutItem>,
    conditioned_on: Option<Box<ast_unresolved::TruthExpression>>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    let mut resolved = Vec::new();
    let mut targets = Vec::new();
    for item in transformations.into_vec() {
        let (expr, naming, qualifier) = (item.expr, item.naming, item.qualifier);
        let expression = Some(
            super::super::domain_expressions::projection::resolve_out_value_via_fold(
                fold, expr, available,
            )?,
        )
        .into_iter()
        .next()
        .expect("one transform expression resolves to one expression");
        // AS WRITTEN, both halves: a strop is what makes an address
        // case-sensitive, and a folded target addresses a column nobody named.
        let alias_spelling = fold
            .core
            .identities
            .intern(naming.as_str(), naming.is_stropped());
        let alias_sym = fold.core.identities.canonical(alias_spelling);
        let qualifier_sym = qualifier.as_ref().map(|qualifier| {
            let spelling = fold
                .core
                .identities
                .intern(qualifier.as_str(), qualifier.is_stropped());
            fold.core.identities.canonical(spelling)
        });
        let mut witness = crate::pipeline::resolver::Witness::default();
        let matches = vec![fold.lexical.address(
            crate::pipeline::resolver::unification::ColumnReference::Named {
                name: naming.clone(),
                qualifier: qualifier.clone(),
            },
            false,
            &mut witness,
            &fold.core.identities,
        )?];
        // Two different failures wore one message, and neither of them was a
        // parse failure. A target that reaches nothing is an unresolved
        // column — under its WRITTEN spelling, qualifier included — and a
        // target that reaches several is the ordinary ambiguity. Saying
        // "does not name exactly one" reported the arithmetic instead of the
        // fact, and dropped the qualifier the user wrote.
        let spelled = match &qualifier {
            Some(qualifier) => format!("{qualifier}.{naming}"),
            None => naming.to_string(),
        };
        let covered = match matches.into_iter().next().expect("one target was asked") {
            crate::pipeline::resolver::unification::UnificationResult::Resolved(occurrence) => {
                occurrence.column
            }
            crate::pipeline::resolver::unification::UnificationResult::Unresolved(_) => {
                return Err(DelightQLError::column_not_found_error(
                    spelled,
                    "as a transform target",
                ));
            }
            crate::pipeline::resolver::unification::UnificationResult::Ambiguous { .. } => {
                return Err(DelightQLError::validation_error_categorized(
                    "resolution/ambiguous",
                    format!("Ambiguous transform target '{spelled}'"),
                    "as a transform target",
                ));
            }
            crate::pipeline::resolver::unification::UnificationResult::Opaque => {
                return Err(crate::pipeline::resolver::opaque_reference_refusal());
            }
            crate::pipeline::resolver::unification::UnificationResult::Refused(refusal) => {
                return Err(refusal.into_error());
            }
        };
        if targets.contains(&(alias_sym, qualifier_sym)) {
            return Err(DelightQLError::parse_error(format!(
                "Duplicate transform target '{naming}'"
            )));
        }
        targets.push((alias_sym, qualifier_sym));
        // THE TARGET IS THE OUTPUT. Resolution found the one column this item
        // writes; the lowering reads that decision instead of re-addressing
        // the same two words against a later heading.
        resolved.push(crate::relation::pending::TransformItem {
            expr: expression,
            naming,
            qualifier,
            covered,
        });
    }

    let conditioned_on = conditioned_on
        .map(|condition| fold.transform_boolean(*condition).map(Box::new))
        .transpose()?;
    fold.core
        .identities
        .authority()
        .bind(crate::relation::pending::Pending::Transform {
            input,
            items: resolved,
            guard: conditioned_on,
        })
}

pub(super) fn resolve_embed_map_cover_via_fold(
    fold: &mut ResolverFold,
    function: ast_unresolved::Callable,
    selector: Vec<ast_unresolved::SelectorItem>,
    alias_template: Option<ast_unresolved::ColumnAlias>,
    available: &[crate::relation::PortId],
    input: crate::relation::SemanticRelation,
) -> Result<(ast_resolved::Step, Vec<crate::relation::PortId>)> {
    let (resolved_selector, selected) =
        super::super::domain_expressions::projection::resolve_selector_via_fold(
            fold, selector, available, true,
        )?;

    if selected.is_empty() && !available.is_empty() {
        emit_validation_warning("EmbedMapCover pattern matched no columns - no columns added");
    }

    // THE COVER IS THE APPLYING POSITION for the embed spelling too: one
    // closed resolved expression per covered cell, appended beside the
    // operand's heading under the naming the authority expands.
    let cells = selected
        .iter()
        .map(|column| {
            Ok(crate::pipeline::asts::core::operators::AppliedCell {
                column: *column,
                expr: apply_callable_to_cell(fold, &function, *column)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    fold.core
        .identities
        .authority()
        .bind(crate::relation::pending::Pending::EmbedMapCover {
            input,
            naming: convert_column_alias(alias_template),
            selector: resolved_selector,
            cells,
        })
}
