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
use crate::names::{Addressing, ColumnOrigin, Hint, ScopeOrigin, ValueFacts};
use crate::pipeline::asts::core::{
    AuthoredColumn, ColumnOccurrence, FactFunctionMode, FieldSelect, ModeWitness, QualifiedName,
    Resolved, Unresolved,
};
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
    let scope = fold.config.resolution_namespace.clone();

    let declared = if matches!(picked, Picked::Whole) && !fold.registry.consult.any_declared_mode()?
    {
        None
    } else {
        fold.registry
            .consult
            .lookup_declared_mode(&name, namespace.as_deref(), scope.as_deref())?
    };

    let Some((entity, declaration)) = declared else {
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

    let arguments = application.call().arguments.scalar_members().len();
    if arguments != declaration.inputs.len() {
        return Err(DelightQLError::validation_error_categorized(
            "mode/arity",
            format!(
                "'{name}' declares {} input{}, and the call supplies {arguments}",
                declaration.inputs.len(),
                if declaration.inputs.len() == 1 { "" } else { "s" }
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

    // The DECLARATION comes from the catalog; the ARMS come from the clause
    // source, the same road every other body is read by. The two must agree
    // about width, and a catalog that disagrees with the definition it was
    // written from is a corruption, not a user error.
    let group = crate::ddl::reconstruct::group(&entity.definition)?;
    let Some(authored) = group.declared_mode() else {
        return Err(DelightQLError::database_error(
            "corrupt catalog: an entity declares a functional dependency and its stored \
             definition is not a fact function",
            name.to_string(),
        ));
    };
    // THE TWO READINGS ARE ONE DECLARATION. The catalog chose the selected
    // POSITION and the source supplies the expression at that position, so
    // they must agree about every name, its stropping, its role and its
    // order — equal widths under different names would select the wrong
    // output while looking consistent.
    if !declaration.agrees_with(
        &authored.inputs.iter().cloned().collect::<Vec<_>>(),
        &authored.outputs.iter().cloned().collect::<Vec<_>>(),
    ) {
        return Err(DelightQLError::database_error(
            "corrupt catalog: the stored mode and the stored definition are not the same              declaration",
            name.to_string(),
        ));
    }
    let authored = authored.clone();

    let call = fold.resolve_standard_application(application)?;
    // THE DECLARED HEADING IS A RELATION'S. Minting all of it — inputs then
    // outputs — gives the output cells the occurrences they read and the
    // pick the occurrence it publishes, from one heading rather than two.
    let (inputs, outputs) = mint_heading(fold, &name, &declaration);
    let mode = resolve_mode(fold, authored, &inputs)?;

    Ok(Some(ast_resolved::FunctionApplication::FieldSelect(
        FieldSelect {
            application: call,
            field: ColumnOccurrence {
                column: outputs[selected],
                explicit_qualifier: false,
            },
            dependency: Box::new(ModeWitness {
                entity: QualifiedName {
                    namespace_path: crate::pipeline::asts::core::NamespacePath::from_fq_string(
                        &entity.namespace,
                    )
                    .unwrap_or_else(|_| crate::pipeline::asts::core::NamespacePath::empty()),
                    name: entity.name.clone(),
                },
                mode,
                inputs,
                selected,
            }),
        },
    )))
}

/// The mode's output rows, resolved AGAINST THE DECLARED INPUTS AND NOTHING
/// ELSE.
///
/// An output cell's only binders are the inputs the head declared — there is
/// no enclosing row in either face — so the caller's heading is taken away
/// for the duration. Without that, a cell reading `a` would silently bind to
/// a column of the row the call stands in, which is a different relation's
/// value under the same name.
fn resolve_mode(
    fold: &mut ResolverFold,
    mode: FactFunctionMode<Unresolved>,
    inputs: &[crate::names::ColId],
) -> Result<FactFunctionMode<Resolved>> {
    use crate::pipeline::asts::core::FactFunctionArm;
    use crate::pipeline::ast_transform::AstTransform;

    let outer_available = std::mem::replace(&mut fold.available, inputs.to_vec());
    let outer_local = std::mem::replace(&mut fold.local_available, inputs.to_vec());
    let outer_qualifiers = std::mem::take(&mut fold.qualifier_scope);
    fold.push_declared_scope();

    let resolved = (|| {
        let arms = mode.arms.try_map(|arm| -> Result<FactFunctionArm<Resolved>> {
            Ok(FactFunctionArm {
                inputs: arm.inputs,
                outputs: arm.outputs.try_map(|value| fold.transform_domain(value))?,
            })
        })?;
        let default = match mode.default {
            Some(row) => Some(row.try_map(|value| fold.transform_domain(value))?),
            None => None,
        };
        Ok(FactFunctionMode {
            inputs: mode.inputs,
            outputs: mode.outputs,
            arms,
            default,
        })
    })();

    fold.pop_declared_scope();
    fold.qualifier_scope = outer_qualifiers;
    fold.local_available = outer_local;
    fold.available = outer_available;
    resolved
}

/// THE DECLARED RELATION, MINTED ONCE: inputs then outputs, in declared
/// order and under the declared spellings.
///
/// The inputs are what an output cell reads; the outputs are what a pick
/// selects from. Both are positions of ONE heading — the same heading the
/// relational face publishes — so neither is a second answer to what this
/// entity's columns are. Lowering reads the POSITION; nothing past here
/// addresses a field by characters.
fn mint_heading(
    fold: &mut ResolverFold,
    name: &str,
    declaration: &DeclaredMode,
) -> (Vec<crate::names::ColId>, Vec<crate::names::ColId>) {
    let identities = &fold.registry.identities;
    let hint = identities.intern(name, false);
    let scope = identities.mint_scope(ScopeOrigin::AnonRelation, Hint::User(hint), None);
    let mint = |position: usize, declared: &delightql_types::SqlIdentifier| {
        let published = identities.intern(declared.as_str(), declared.is_stropped());
        identities.mint_column(
            scope,
            ColumnOrigin::Bound {
                position: position as u32,
            },
            Some(published),
            Addressing::Published,
            ValueFacts::default(),
        )
    };
    let inputs: Vec<_> = declaration
        .inputs
        .iter()
        .enumerate()
        .map(|(position, declared)| mint(position, declared))
        .collect();
    let outputs: Vec<_> = declaration
        .outputs
        .iter()
        .enumerate()
        .map(|(position, declared)| mint(inputs.len() + position, declared))
        .collect();
    (inputs, outputs)
}
