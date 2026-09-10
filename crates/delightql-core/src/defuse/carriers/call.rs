// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A HIGHER-ORDER CALL'S RELATION FORMALS, bound as one operation of the
//! carrier authority: the admitted application row is read pair by pair,
//! each relation actual is resolved in the world its provenance names —
//! the piped source in the caller's run, an authored actual closed — the
//! receiving formal's FACE is applied to what resolved, and only then is
//! the carrier bound and the formal recorded. The relation and the
//! interface its formal appointed are never held apart.

use super::formal::{FormalBinding, RelationFormals};
use super::CarrierRecord;
use crate::defuse::application::{Actual, Application};
use crate::defuse::bound_use::ClosedRelationActual;
use crate::diagnostic::{Constraint, Ho, Resolution, Semantic};
use crate::error::{DelightQLError, Result};
use crate::pipeline::ast_unresolved;
use crate::pipeline::asts::core::{AuthoredColumn, NamedReference, Reference};
use crate::pipeline::asts::ddl::{HeadItems, HoParam};
use crate::pipeline::query_features::HoParamBindings;
use crate::pipeline::resolver::resolver_fold::ResolverFold;
use crate::pipeline::resolver::{PatternOperand, PatternOwner, ResolvedRelation};
use crate::relation::form::HoPart;
use crate::relation::StructuralRelation;

/// THE FACE A RELATION FORMAL DECLARES: open (`T(*)`), preserving the
/// actual's interface, or appointed (`T(k, v)`), replacing it by position.
enum Face<'a> {
    Open,
    Appointed(&'a [crate::pipeline::asts::core::definitions::HeadItem]),
}

impl<'a> Face<'a> {
    fn of(param: &'a HoParam) -> Option<Face<'a>> {
        match param {
            HoParam::Relation {
                cols: HeadItems::Glob,
                ..
            } => Some(Face::Open),
            HoParam::Relation {
                cols: HeadItems::Listed(items),
                ..
            } => Some(Face::Appointed(items)),
            HoParam::Scalar { .. } | HoParam::Rule { .. } | HoParam::Ground { .. } => None,
        }
    }

    fn names(items: &[crate::pipeline::asts::core::definitions::HeadItem]) -> Vec<String> {
        items.iter().map(|item| item.supply.spelling()).collect()
    }
}

/// BIND EVERY RELATION FORMAL OF ONE ADMITTED APPLICATION.
///
/// The application already paired each actual with its formal by position.
/// This act spends the relation pairs: the piped source resolves in the
/// caller's run and rides the record as the pipe source; an authored actual
/// is admitted as a closed relation value and resolves closed, with no
/// outer row, sibling members or qualifiers in view; a name-only actual is
/// the whole named relation; an inline lift under an appointed face stands
/// in the body as its literal. A whole read of a carrier the enclosing body
/// already holds is FORWARDED into an open formal — the bound relation and
/// its interface move whole — while an appointed formal appoints anew over
/// it like over any relation.
///
/// Every carrier is bound into the record of this act, which reserves its
/// landing as it binds; the formal is recorded in `bindings` from the row
/// the bind produced, never ahead of the carrier.
pub(in crate::defuse) fn bind_relation_formals(
    caller: &mut ResolverFold<'_, '_>,
    entity: &str,
    params: &[HoParam],
    application: &Application<'_>,
    bindings: &mut HoParamBindings,
) -> Result<CarrierRecord> {
    // THE FORMALS ARE ISSUED FIRST, over the declared row, before any pair
    // is spent: every binding below is made AT an issued identity by its
    // declared position, never under a spelling.
    bindings.formals = RelationFormals::issued(params);
    let mut record = CarrierRecord::default();
    for pair in application.pairs() {
        let param = pair.formal();
        let Some(face) = Face::of(param) else {
            continue;
        };
        let declared = param.name();
        let formal = declared.as_str();
        let position = pair.position();
        let actual = pair.actual();
        let landed = actual.is_landed();
        let bound = match actual {
            Actual::Rule(_) => {
                return Err(DelightQLError::from(Ho::RelationActualForm {
                    message: format!(
                        "parameter '{formal}' of '{entity}' requires a relation value, but \
                         position {position} is a rule designator"
                    ),
                }));
            }
            Actual::Relation { relation, .. } => {
                if let (Face::Appointed(items), false) = (&face, landed) {
                    // THE LIFT'S ROWS ARE THE ARGUMENT: an authored headerless
                    // literal binds inline under the declared names, so every
                    // reader that needs the VALUES sees the cells. A landed
                    // literal is the pipe's source and resolves in the
                    // caller's run like any landed relation — its cells may
                    // read the enclosing row.
                    if let Some(rows) =
                        crate::pipeline::resolver::grounding::lifted_rows_under_declared_names(
                            relation,
                            &Face::names(items),
                        )
                    {
                        bindings
                            .formals
                            .bind(position, FormalBinding::inline(rows)?, landed)?;
                        continue;
                    }
                }
                if let (Face::Open, Some(landing)) = (&face, whole_carrier_read(relation)) {
                    // FORWARDING: the enclosing body's carrier travels whole
                    // — its relation already publishes the interface that
                    // body reads it under, and an open formal declares
                    // nothing to appoint over it.
                    let holder = caller.env.carriers_holding(landing).ok_or_else(|| {
                        crate::diagnostic::Internal::invariant(
                            "higher-order relation forwarding",
                            "a forwarded relation lost its structural carrier",
                        )
                    })?;
                    record.inherit(holder, landing)?;
                    FormalBinding::carrier(landing)
                } else if landed {
                    // THE LANDED SOURCE STANDS IN THE CALLER'S RUN: it is
                    // evaluated where that run is — at the join enclosing a
                    // hoisted interior, if that is where the pipe was
                    // written — so a restriction inside it that reads the
                    // enclosing row is the same correlation act it would be
                    // without the call.
                    let mut world = caller.child_in_run();
                    let row = bind_faced(
                        &mut world,
                        &mut record,
                        HoPart::PipeSource,
                        declared,
                        &face,
                        (*relation).clone(),
                    )?;
                    FormalBinding::carrier(row)
                } else {
                    let admitted =
                        ClosedRelationActual::admit((*relation).clone(), entity, formal, position)?;
                    bind_closed(caller, &mut record, declared, &face, admitted)?
                }
            }
            Actual::Lifted(run) => {
                let Face::Appointed(items) = &face else {
                    unreachable!("the admission lifts literals only into a listed formal")
                };
                let rows = lifted_rows(entity, formal, position, &Face::names(items), run)?;
                FormalBinding::inline(rows)?
            }
            Actual::Value(_) | Actual::Opaque | Actual::Skip => {
                // A value standing at a relation formal: a carried name is
                // the whole named relation it denotes, resolved in the
                // caller's scope.
                let name = match actual.term() {
                    Some(ast_unresolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(AuthoredColumn { name, .. }),
                    ))) => name.to_string(),
                    Some(ast_unresolved::DomainExpression::Application(
                        ast_unresolved::FunctionApplication::Ground(
                            crate::pipeline::asts::core::LiteralValue::String(name),
                        ),
                    )) if matches!(face, Face::Open) => name,
                    Some(other) => {
                        return Err(match face {
                            Face::Open => DelightQLError::from(Ho::RelationalArgument {
                                message: format!(
                                    "Expected table name at position {position} for param \
                                     '{formal}', got {other:?}"
                                ),
                            }),
                            Face::Appointed(_) => DelightQLError::from(Constraint::General {
                                message: format!(
                                    "Argumentative param '{formal}' expects literal values for \
                                     scalar lift, got {other:?}"
                                ),
                            }),
                        });
                    }
                    None => {
                        return Err(DelightQLError::from(Ho::RelationalArgument {
                            message: format!(
                                "parameter '{formal}' of '{entity}' is supplied at position \
                                 {position} by a relation expression, which names nothing \
                                 this position can bind"
                            ),
                        }));
                    }
                };
                let admitted = ClosedRelationActual::admit(
                    bare_glob_reference(&name),
                    entity,
                    formal,
                    position,
                )?;
                bind_closed(caller, &mut record, declared, &face, admitted)?
            }
        };
        bindings.formals.bind(position, bound, landed)?;
    }
    Ok(record)
}

/// THE CALLER ROW BECOMES A CARRIER: the standing row the call absorbed is
/// spent into its structural binding, as the record's join input.
pub(in crate::defuse) fn bind_join_input(
    caller: &ResolverFold<'_, '_>,
    source: Option<ResolvedRelation>,
) -> Result<CarrierRecord> {
    let mut record = CarrierRecord::default();
    if let Some(source) = source {
        record.bind_join_input(source, &caller.core.identities)?;
    }
    Ok(record)
}

/// An authored actual, admitted closed, resolved in a child world with no
/// outer row, no sibling members and no qualifiers. A name the closed world
/// cannot answer but the caller's world could is caller capture, and
/// refuses as such.
fn bind_closed(
    caller: &mut ResolverFold<'_, '_>,
    record: &mut CarrierRecord,
    formal: &delightql_types::SqlIdentifier,
    face: &Face<'_>,
    actual: ClosedRelationActual,
) -> Result<FormalBinding> {
    let resolved = {
        let mut closed = caller.child_closed();
        bind_faced(
            &mut closed,
            record,
            HoPart::Argument,
            formal,
            face,
            actual.into_chain(),
        )
    };
    match resolved {
        Ok(landing) => Ok(FormalBinding::carrier(landing)),
        Err(DelightQLError::Semantic(Semantic::Resolution(Resolution::Column {
            column,
            context,
        }))) if caller_answers(caller, &column) => {
            // The closed resolution's own account stays attached as the
            // cause, so a reader can see which reference it was.
            let _ = context;
            Err(DelightQLError::from(Ho::RelationActualCapture {
                message: format!(
                    "a relation actual is a closed relation value: its interior may not \
                     read `{column}` from the calling row"
                ),
            }))
        }
        Err(error) => Err(error),
    }
}

/// ONE CARRIER, RESOLVED, FACED AND BOUND: the chain resolves like any
/// relation in the world it is handed, the residual carriers crossing that
/// world ride its body, the formal's face is applied — an open face keeps
/// what resolved, an appointed face spends the declared pattern over it by
/// position, exact width and repeated names judged by the one pattern
/// authority — and the bind spends the faced body, reserving the landing
/// and deriving the carrier as one act.
fn bind_faced(
    world: &mut ResolverFold<'_, '_>,
    record: &mut CarrierRecord,
    part: HoPart,
    formal: &delightql_types::SqlIdentifier,
    face: &Face<'_>,
    source_expr: ast_unresolved::Chain,
) -> Result<StructuralRelation> {
    let crossing = world.crossing_carriers.clone();
    let standing = world.resolve_relational(source_expr)?;
    let standing = if crossing.is_empty() {
        standing
    } else {
        let identities = world.core.identities;
        standing.republished(|chain| {
            super::crossing::inject_crossing_carriers(chain, &crossing, identities)
        })?
    };
    let faced = match face {
        Face::Open => standing,
        Face::Appointed(items) => ResolvedRelation::patterned(
            PatternOperand::Standing(standing),
            &super::formal::appointed_pattern(items),
            PatternOwner::Authored(formal.clone()),
            world,
        )?
        .restricted_by_its_own_constraints(&world.core.identities)?,
    };
    Ok(record.bind(part, faced, &world.core.identities)?.landing())
}

/// A residual's own carrier chain, resolved in the world it is handed and
/// bound into the record as a structural carrier — the residual road's
/// entrance to the same bind, with no face to apply.
pub(super) fn resolve_carrier(
    world: &mut ResolverFold<'_, '_>,
    record: &mut CarrierRecord,
    part: HoPart,
    source_expr: ast_unresolved::Chain,
) -> Result<crate::relation::CarrierRow> {
    let crossing = world.crossing_carriers.clone();
    let standing = world.resolve_relational(source_expr)?;
    let standing = if crossing.is_empty() {
        standing
    } else {
        let identities = world.core.identities;
        standing.republished(|chain| {
            super::crossing::inject_crossing_carriers(chain, &crossing, identities)
        })?
    };
    record.bind(part, standing, &world.core.identities)
}

/// A WHOLE READ OF A CARRIER the enclosing body holds: a structural mention
/// read whole — no steps, no dimension named, no alias, not outer. Only
/// such a read forwards a carrier; a read that patterns, projects or
/// renames it is a relation of its own and resolves as one.
fn whole_carrier_read(chain: &ast_unresolved::Chain) -> Option<StructuralRelation> {
    if chain.has_steps() {
        return None;
    }
    if let Some(access) = chain.head_access() {
        if !access.is_whole() {
            return None;
        }
    }
    match chain.head().form() {
        ast_unresolved::GroundForm::Reference(ast_unresolved::Relation::Ground {
            mention:
                ast_unresolved::GroundMention::Structural {
                    pending,
                    alias: None,
                    ..
                },
            outer: false,
        }) => Some(*pending),
        _ => None,
    }
}

/// THE SCALAR LIFT'S ROWS: the admission handed the formal the run of
/// literals standing at it; they are rows of N values each, N the declared
/// width. Multiple rows arise from `;`: `pivot_by("Maths"; "Music")`.
fn lifted_rows(
    entity: &str,
    formal: &str,
    position: usize,
    columns: &[String],
    run: &[&ast_unresolved::DomainExpression],
) -> Result<ast_unresolved::Chain> {
    let n_cols = columns.len();
    if n_cols == 0 || run.len() % n_cols != 0 {
        return Err(DelightQLError::from(Constraint::General {
            message: format!(
                "Argumentative param '{formal}' expects {n_cols} values per row, but only {} \
                 remain at position {position}",
                run.len() % n_cols.max(1),
            ),
        }));
    }
    let _ = entity;
    let mut all_rows = Vec::new();
    for row in run.chunks(n_cols) {
        let mut row_values = Vec::with_capacity(n_cols);
        for (col_idx, val_expr) in row.iter().enumerate() {
            let value = match val_expr {
                ast_unresolved::DomainExpression::Application(
                    ast_unresolved::FunctionApplication::Ground(
                        value @ (crate::pipeline::asts::core::LiteralValue::String(_)
                        | crate::pipeline::asts::core::LiteralValue::Number(_)),
                    ),
                ) => value.clone(),
                other => {
                    return Err(DelightQLError::from(Constraint::Unsupported {
                        message: format!(
                            "Unsupported expression in scalar lift for param '{formal}' column \
                             {col_idx}: {other:?}"
                        ),
                    }));
                }
            };
            row_values.push(value);
        }
        all_rows.push(row_values);
    }
    crate::pipeline::resolver::grounding::lift_scalars_to_anonymous_table(columns, &all_rows)
}

/// A `name(*)` reference for a name-only HO argument. The name is the
/// caller's text.
fn bare_glob_reference(name: &str) -> ast_unresolved::Chain {
    ast_unresolved::Chain::read(
        ast_unresolved::Relation::Ground {
            mention: ast_unresolved::GroundMention::Named {
                identifier: ast_unresolved::QualifiedName {
                    namespace_path: ast_unresolved::NamespacePath::empty(),
                    name: name.into(),
                },
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
        },
        ast_unresolved::Access::All,
    )
}

/// Whether the CALLER'S world would answer the reference the closed world
/// could not: a bare name published by the caller's live row or its outer
/// row, or a qualifier one of its scopes answers to. A diagnostic
/// question — it decides only which refusal to teach.
fn caller_answers(caller: &ResolverFold<'_, '_>, reference: &str) -> bool {
    caller
        .lexical
        .answers_spelling(reference, &caller.core.identities)
}
