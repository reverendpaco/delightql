// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE MODE IS THE COMPRESSION — the one boundary a declared functional
//! dependency is read at.
//!
//! Both spellings of the pick arrive here: `foo:(x).out1`, which names the
//! output, and `foo:(x)` standing in value position, which names none and is
//! admitted only where the declaration leaves exactly one to name. One
//! authority answers both, so a one-output mode and a wider one travel the
//! same road and no width has a second semantics.

use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::{AuthoredColumn, ColumnOccurrence, FieldSelect, ModeWitness};
use crate::pipeline::asts::{resolved as ast_resolved, unresolved as ast_unresolved};
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::resolution::registry::DeclaredMode;

/// What the author wrote after the call, if anything.
pub(in crate::pipeline::resolver) enum Picked {
    /// `foo:(x).out1`
    Named(AuthoredColumn),
    /// `foo:(x)` in value position — the row itself, admitted only at width
    /// one.
    Whole,
}

/// Resolve a call whose callee may declare a mode.
///
/// `Ok(None)` means the callee declares none AND the author named no field —
/// an ordinary call, which the ordinary road takes. Everything else is
/// answered here, refusals included: a pick at a callee with no declaration
/// is not an ordinary call that happens to have a suffix.
pub(in crate::pipeline::resolver) fn resolve_mode_call(
    fold: &mut ResolverFold,
    application: ast_unresolved::StandardApplication,
    picked: Picked,
) -> Result<Option<ast_resolved::FunctionApplication>> {
    let reference = &application.call().callee;
    let name = reference.name_text();
    let namespace = reference.namespace_fq();
    let declared = if matches!(picked, Picked::Whole) && !fold.core.consult.any_declared_mode()? {
        None
    } else {
        crate::defuse::bound_use::use_declared_mode(fold, &name, namespace.as_deref())?
    };

    let Some(mode_use) = declared else {
        return match picked {
            // An ordinary call. The pick is what makes a declaration
            // mandatory, and there is none here.
            Picked::Whole => Ok(None),
            Picked::Named(column) => Err(DelightQLError::validation_error_categorized(
                "mode/undeclared",
                format!(
                    "'{name}' declares no functional mode, so there is no row for '.{}' to pick from",
                    column.name
                ),
                "a bare `.name` after a call picks an output of a fact function's declared \
                 mode — `f(a -> b, c ---- …)`; an ordinary function returns its one value \
                 and is written without a suffix",
            )),
        };
    };

    let declaration = mode_use.declaration.clone();
    let entity_identity = mode_use.identity.clone();
    let arguments = application.call().arguments.scalar_members().len();
    if arguments != declaration.inputs.len() {
        return Err(DelightQLError::validation_error_categorized(
            "mode/arity",
            format!(
                "'{name}' declares {} input{}, and the call supplies {arguments}",
                declaration.inputs.len(),
                if declaration.inputs.len() == 1 {
                    ""
                } else {
                    "s"
                }
            ),
            "a mode-compressed call supplies exactly the declared inputs, in order",
        ));
    }

    let selected = match &picked {
        Picked::Named(column) => match declaration.output_position(&column.name) {
            Some(position) => position,
            None => {
                return Err(DelightQLError::validation_error_categorized(
                    "mode/unknown_output",
                    format!(
                        "'{name}' declares no output '{}' — its outputs are {}",
                        column.name,
                        declaration.output_spellings()
                    ),
                    "a pick names one of the declared outputs, by its exact spelling",
                ))
            }
        },
        // A WIDER RESULT IS NOT A SCALAR VALUE MERELY BECAUSE IT IS ONE ROW.
        // The declaration compressed the call to one row; a value position
        // wants one column, and only a one-output declaration has one to
        // give.
        Picked::Whole => {
            if declaration.outputs.len() != 1 {
                return Err(DelightQLError::validation_error_categorized(
                    "mode/degree",
                    format!(
                        "'{name}' is one ROW of {} outputs, and a value position holds one \
                         column — pick one: {}",
                        declaration.outputs.len(),
                        declaration.output_spellings()
                    ),
                    "a declared mode compresses a call to one row; a `.field` after it \
                     selects the column",
                ));
            }
            0
        }
    };

    let call = fold.resolve_standard_application(application)?;
    // THE DECLARED ROWS ARE RELATIONS. The input row is what an output
    // cell stands over and reads — declared and stood over in one lexical
    // act; the output row is what a pick selects from.
    let (input_row, inputs, outputs) = declare_rows(fold, &name, &declaration)?;
    let mode = mode_use.resolve_arms(fold, input_row)?;

    Ok(Some(ast_resolved::FunctionApplication::FieldSelect(
        FieldSelect {
            application: call,
            field: ColumnOccurrence::engine(outputs[selected]),
            dependency: Box::new(ModeWitness {
                entity: entity_identity,
                mode,
                inputs,
                selected,
            }),
        },
    )))
}

/// THE DECLARED ROWS, DECLARED ONCE EACH: the input row and the output
/// row, in declared order and under the declared spellings.
///
/// The input row is the relation an output cell stands over and reads —
/// born and stood over by the lexical authority's own act, from the
/// declaration's spellings; the output row is what a pick selects from.
/// Lowering reads the POSITION; nothing past here addresses a field by
/// characters.
fn declare_rows(
    fold: &mut ResolverFold,
    name: &str,
    declaration: &DeclaredMode,
) -> Result<(
    crate::pipeline::resolver::ResolvedRelation,
    Vec<crate::relation::PortId>,
    Vec<crate::relation::PortId>,
)> {
    let identities = &fold.core.identities;
    let hint = identities.intern(name, false);
    let slots = |declared: &[delightql_types::SqlIdentifier]| -> Vec<_> {
        declared
            .iter()
            .enumerate()
            .map(
                |(position, declared)| crate::relation::form::AnonymousSlot::Binder {
                    position: position as u32,
                    named: identities.intern(declared.as_str(), declared.is_stropped()),
                    declared_type: None,
                    shape: crate::names::ValueShape::Unknown,
                },
            )
            .collect()
    };
    let input_slots = slots(&declaration.inputs);
    let input_row = crate::pipeline::resolver::ResolvedRelation::declared_row(
        crate::relation::form::AnonymousSpec {
            shape: crate::relation::form::AnonymousShape::ArgumentRow,
            slots: &input_slots,
            answers_to: Some(hint),
        },
        identities,
    )?;
    let inputs = crate::relation::published_ports(identities, &input_row.semantic_relation())?;
    let output_slots = slots(&declaration.outputs);
    let output_row = identities
        .authority()
        .derive(crate::relation::RelForm::Anonymous(
            crate::relation::form::AnonymousSpec {
                shape: crate::relation::form::AnonymousShape::ArgumentRow,
                slots: &output_slots,
                answers_to: Some(hint),
            },
        ))?;
    let outputs = crate::relation::published_ports(identities, &output_row)?;
    Ok((input_row, inputs, outputs))
}
