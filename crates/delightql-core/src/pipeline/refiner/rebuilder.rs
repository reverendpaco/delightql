// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// rebuilder.rs - Phase 3 of FAR cycle: Rebuild AST with predicates in proper homes
//
// The rebuilder takes the analyzed segment and rebuilds it into a refined AST
// with predicates pushed down to their appropriate operators according to
// their classification.
//
// Submodules for organization
mod exists_handler;
mod join_builder;

use self::exists_handler::nest_interdependent_exists;
use self::join_builder::rebuild_join_segment;
use super::analyzer::AnalyzedSegment;
use super::flattener::FlatTable;
use super::types::*;
use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::{Comparison, Existence, GroundForm, RelationalMembership};
use crate::pipeline::asts::refined::{LiteralValue, Refined};
use crate::pipeline::asts::resolved::{InnerRelationPattern, Resolved};
use crate::pipeline::asts::{refined, resolved};
use std::collections::HashMap;

/// Main entry point - rebuild an analyzed segment into refined AST
pub(super) fn rebuild_internal(
    analyzed: AnalyzedSegment,
    is_top_level: bool,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    log::debug!(
        "rebuild: {} tables, {} operators, {} predicates, is_top_level={}",
        analyzed.tables.len(),
        analyzed.operators.len(),
        analyzed.predicates.len(),
        is_top_level
    );

    // Check for forbidden predicates first
    for pred in &analyzed.predicates {
        if let PredicateClass::Forbidden { reason } = &pred.class {
            return Err(Internal::invariant(
                "refiner::rebuilder",
                format!(
                    "Forbidden predicate: {:?} (reason: {:?})",
                    pred.expr, reason
                ),
            ));
        }
    }

    // Group predicates by their operator association
    let mut op_predicates = group_predicates_by_operator(&analyzed.predicates);

    // Handle interdependent EXISTS predicates by nesting them
    nest_interdependent_exists(
        &mut op_predicates,
        &analyzed.exists_dependencies,
        identities,
    )?;

    log::debug!("Calling rebuild_join_segment");
    rebuild_join_segment(
        analyzed,
        op_predicates,
        is_top_level,
        danger_gates,
        identities,
    )
}

/// Group predicates by which operator they modify
fn group_predicates_by_operator(
    predicates: &[AnalyzedPredicate],
) -> HashMap<OperatorRef, Vec<AnalyzedPredicate>> {
    let mut grouped = HashMap::new();

    for pred in predicates {
        grouped
            .entry(pred.operator_ref.clone())
            .or_insert_with(Vec::new)
            .push(pred.clone());
    }

    grouped
}

/// Apply top-level filters to the result
fn apply_top_level_filters(
    result: refined::Chain,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    apply_filter_predicates(result, op_predicates, OperatorRef::TopLevel, identities)
}

/// Apply filter predicates for a given operator reference
fn apply_filter_predicates(
    mut result: refined::Chain,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    op_ref: OperatorRef,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    if let Some(preds) = op_predicates.remove(&op_ref) {
        for pred in preds {
            match pred.class {
                PredicateClass::F { .. } | PredicateClass::Fx => {
                    result = wrap_with_filter(result, pred, identities)?;
                }
                other => panic!(
                    "catch-all hit in rebuilder.rs apply_filter_predicates: {:?}",
                    other
                ),
            }
        }
    }
    Ok(result)
}

/// Wrap an expression with a filter
fn wrap_with_filter(
    source: refined::Chain,
    pred: AnalyzedPredicate,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    Ok(source.transparently(refined::Transparent::Restrict {
        condition: refine_predicate_boolean(pred.expr.into_truth(), identities)?,
        origin: pred.origin,
    }))
}

/// Convert a flat table to a refined relation
fn table_to_refined(
    table: &FlatTable,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    // A CONSULTED EXPANSION CROSSES AS A NODE. The body is refined
    // independently — it is self-contained and takes no part in the outer
    // segment's FAR cycle — and the head it stands under keeps what it
    // published, because a crossing has no argument for a relation.
    if let Some(head) = table.consulted_head() {
        let result = refined::Chain::ground(head.clone().crossing(|form| {
            let GroundForm::Reference(resolved::Relation::ConsultedView { body, outer }) = form
            else {
                unreachable!("the head was just matched as a consulted expansion");
            };
            Ok(GroundForm::Reference(refined::Relation::ConsultedView {
                body: Box::new(crate::pipeline::refiner::refine_query(*body, identities)?),
                outer,
            }))
        })?);

        return apply_table_filters(result, table, identities);
    }

    // Check if this is a pipe expression
    if let Some(ref pipe_expr) = table.pipe_expr {
        // Recursively refine the pipe expression
        // Pass is_top_level=false to skip outer join validation (this is an inner context)
        // The gates travel with the recursion: a danger the writer armed
        // on the query is armed inside the relation it stands on.
        let refined = crate::pipeline::refiner::refine_internal(
            pipe_expr.as_ref().clone(),
            false,
            danger_gates.clone(),
            identities,
        )?;
        // A filter the flattener kept LOCAL belongs to the table it was
        // attached to, whatever kind that table is. Returning the pipe's
        // refinement without them dropped a higher-order ground
        // discriminator's whole constraint, so every clause of the callee
        // answered.
        return apply_table_filters(refined, table, identities);
    }

    let result = build_base_relation(table, danger_gates, identities)?;
    // A filter kept local by the flattener belongs to the table it was
    // attached to regardless of relation kind. Applying these only to
    // consulted views silently discarded the same constraint on a structural
    // higher-order carrier.
    let mut result = apply_table_filters(result, table, identities)?;

    log::debug!("table_to_refined: Processing {:?}", table.relation);
    log::debug!(
        "Available operator refs: {:?}",
        op_predicates.keys().collect::<Vec<_>>()
    );

    for (op_ref, preds) in op_predicates.iter() {
        for pred in preds {
            log::debug!(
                "  Op {:?} has predicate: class={:?}, origin={:?}",
                op_ref,
                pred.class,
                pred.origin
            );
        }
    }

    let mut filters_to_apply = Vec::new();

    for (op_ref, preds) in op_predicates.iter_mut() {
        let mut remaining = Vec::new();
        for pred in preds.drain(..) {
            if let PredicateClass::F { table: target } = &pred.class {
                if *target == table.relation.scope() {
                    if let resolved::FilterOrigin::PositionalLiteral { source } = &pred.origin {
                        log::debug!(
                            "      Checking PositionalLiteral: source={:?}, table={:?}, op_ref={:?}",
                            source,
                            table.relation,
                            op_ref
                        );
                        log::debug!(
                            "      Applying PositionalLiteral filter to table {:?}",
                            table.relation
                        );
                        filters_to_apply.push(pred);
                        continue;
                    }
                }
            }
            remaining.push(pred);
        }
        *preds = remaining;
    }

    for filter_pred in filters_to_apply {
        result = wrap_with_filter(result, filter_pred, identities)?;
    }

    Ok(result)
}

fn apply_table_filters(
    mut result: refined::Chain,
    table: &FlatTable,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    for (filter_expr, origin) in &table._table_filters {
        let refined_condition = refine_predicate_boolean(filter_expr.clone(), identities)?;
        result = result.transparently(refined::Transparent::Restrict {
            condition: refined_condition,
            origin: origin.clone(),
        });
    }
    Ok(result)
}

/// Build the base relation from a flat table
fn build_base_relation(
    table: &FlatTable,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    let schema_box = table.relation;

    if let Some(ref tvf_data) = table.tvf_data {
        return build_tvf_relation(tvf_data, schema_box, identities);
    }

    if let Some(ref anon_data) = table.anonymous_data {
        return build_anonymous_relation(anon_data, table, schema_box, identities);
    }

    // A DERIVED TABLE CROSSES AS A NODE, with the pattern read out of the
    // head it stands under rather than stored beside it, and its body is
    // rebuilt from the segment the flattener stored beside the head. There
    // is no road that copies the body across instead: a subquery crossing
    // unrebuilt keeps every member condition a filter over the join.
    if let Some((head, inner_pattern)) = table.inner_head() {
        let subquery_segment = table.subquery_segment.as_deref().ok_or_else(|| {
            Internal::invariant(
                "refiner::rebuilder",
                "a derived table is flattened together with its subquery segment",
            )
        })?;
        return build_inner_relation_from_flattened(
            inner_pattern,
            head,
            subquery_segment,
            danger_gates,
            identities,
        );
    }

    build_ground_relation(table, schema_box, identities)
}

/// Build a TVF relation
fn build_tvf_relation(
    tvf_data: &super::flattener::TvfData,
    schema_box: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    let mut fold = super::RefinerFold::inner(
        crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
        identities,
    );
    let ho_arguments = tvf_data
        .arguments
        .iter()
        .map(|argument| {
            Ok(match argument {
                Some(argument) => crate::pipeline::asts::core::operators::HoArgument::Value(
                    crate::pipeline::asts::core::ArgumentValue::plain(
                        fold.transform_domain(argument.clone())?,
                    ),
                ),
                // A valueless position rides back as the skip it is.
                None => crate::pipeline::asts::core::operators::HoArgument::Skip,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // A TVF is a READ like any other: the relation and what its parens asked
    // of it. Rebuilding the callable alone drops the access between the
    // resolved and refined trees, where nothing downstream can recover it.
    let authority = identities.authority();
    let head = authority.reading(crate::relation::builder::ReadHead::Call {
        call: refined::SealedCall::from_inner(
            refined::FunctorCall {
                callee: tvf_data.function,
                arguments: crate::pipeline::asts::core::operators::CallArguments::higher_order(
                    ho_arguments,
                ),
                marks: Default::default(),
            },
            false,
        ),
        alias: (),
        published: schema_box,
    })?;
    authority.read_asking(
        refined::Chain::ground(head),
        crate::pipeline::ast_transform::walk_transform_access(&mut fold, tvf_data.access.clone())?,
    )
}

/// Build an anonymous table relation
fn build_anonymous_relation(
    anon_data: &super::flattener::AnonymousTableData,
    flat: &FlatTable,
    schema_box: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    let table = crate::pipeline::ast_transform::walk_transform_anon_table(
        &mut super::RefinerFold::inner(
            crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
            identities,
        ),
        resolved::AnonTable {
            body: anon_data.body.clone(),
        },
    )?;
    let chain = refined::Chain::ground(identities.authority().reading(
        crate::relation::builder::ReadHead::Anonymous {
            relation: refined::AnonRelation {
                table,
                alias: None,
                outer: flat.outer,
            },
            published: schema_box,
        },
    )?);
    // An unasked access is the zero-width narrowing the resolver put on
    // this read: every header position was consumed. Restated here so the
    // rebuilt table publishes what the resolved one published — nothing —
    // while the grid's cells ride as dependencies for the constraints that
    // read them.
    if matches!(flat.access, resolved::Access::Unasked) {
        let ports = crate::relation::published_ports(identities, &schema_box)?;
        let narrowed = identities.authority().extend(
            chain,
            crate::relation::builder::StepOp::Access {
                shape: crate::relation::form::AccessShape::Empty,
                slots: &[],
                dependencies: &ports,
            },
        )?;
        // THE REBUILT NARROWING STANDS WHERE THE RESOLVER'S STOOD. The
        // segment's join stood over the resolver's zero-width narrowing of
        // this grid; the one derived here narrows the same grid to nothing,
        // and the authority records it as the replacement so the rebuild
        // may consume it in that occurrence's place.
        if let Some(was) = flat.narrowed {
            identities
                .authority()
                .narrowed_again(was, &narrowed.semantic_relation())?;
        }
        return Ok(narrowed);
    }
    Ok(chain)
}

/// Build INNER-RELATION from flattened subquery segment (PHASE 5: Recursive FAR)
/// This is the new code path that uses the pre-flattened subquery instead of re-processing AST
fn build_inner_relation_from_flattened(
    pattern: &InnerRelationPattern<Resolved>,
    head: &resolved::Grelex,
    subquery_segment: &super::flattener::FlatSegment,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    // The subquery segment has already been flattened
    // Correlation filters have already been hoisted
    // We need to: analyze it, then rebuild it

    // THE REBUILD RUNS THROUGH THE AUTHORITY. A nested subquery reaches the
    // FAR cycle inside its enclosing segment's flatten, so it never crosses
    // the refiner's own routing hub — and the rebuilt join is a NEW
    // relation. Reporting it here is what lets the boundary standing over
    // this subquery translate its recorded positions onto the ones the
    // rebuilt statement emits.
    // THE OPERAND TRAVELS WHOLE. What is being refined is the pattern's own
    // subquery node; the authority reads its relation out of that node, so
    // nothing here names one operand while rebuilding another. The rebuild
    // works from the FLATTENED segment rather than from the node, which is
    // why the node arrives and is spent without being read.
    let operand = match pattern {
        InnerRelationPattern::CorrelatedScalarJoin { subquery, .. }
        | InnerRelationPattern::CorrelatedGroupJoin { subquery, .. }
        | InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. }
        | InnerRelationPattern::Indeterminate { subquery, .. } => (**subquery).clone(),
    };
    let rebuilt_subquery = identities.authority().refine_relation(operand, |_spent| {
        // Analyze the flattened subquery segment, then rebuild it. Pass
        // is_top_level=false to skip outer join validation (inner context).
        let analyzed_subquery = super::analyzer::analyze(subquery_segment.clone(), identities)?;
        rebuild_internal(analyzed_subquery, false, danger_gates, identities)
    })?;
    // A NESTED SUBQUERY'S REBUILD IS A NEW RELATION, and that is the
    // point: the boundary standing over it translates its recorded
    // positions onto the ones the rebuilt statement emits, through the map
    // this act produced. Preservation is lawful too — a segment the FAR
    // cycle left alone rebuilds to itself.
    let (rebuilt_subquery, carried) = match rebuilt_subquery {
        crate::relation::Refinement::Preserved(chain) => (chain, None),
        crate::relation::Refinement::Rebuilt { chain, map } => (chain, Some(map)),
    };

    // Convert pattern from Resolved to Refined, replacing the subquery with
    // the rebuilt one. The hoisted conditions, aggregations and deferred
    // values cross through the refiner's own fold, so a relation nested in
    // any of them is refined where it stands.
    let mut fold = super::RefinerFold::inner(danger_gates.clone(), identities);
    let refined_pattern: InnerRelationPattern<Refined> = match pattern {
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            deferred,
            ..
        } => InnerRelationPattern::CorrelatedScalarJoin {
            identifier: identifier.clone(),
            correlation_filters: correlation_filters
                .iter()
                .map(|f| fold.transform_boolean(f.clone()))
                .collect::<Result<Vec<_>>>()?,
            deferred: carry_deferred(deferred, &mut fold)?,
            subquery: Box::new(rebuilt_subquery),
        },
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            deferred,
            ..
        } => InnerRelationPattern::CorrelatedGroupJoin {
            identifier: identifier.clone(),
            correlation_filters: correlation_filters
                .iter()
                .map(|f| fold.transform_boolean(f.clone()))
                .collect::<Result<Vec<_>>>()?,
            aggregations: aggregations
                .iter()
                .map(|a| fold.transform_domain(a.clone()))
                .collect::<Result<Vec<_>>>()?,
            deferred: carry_deferred(deferred, &mut fold)?,
            subquery: Box::new(rebuilt_subquery),
        },
        InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            is_consulted_view,
            ..
        } => InnerRelationPattern::UncorrelatedDerivedTable {
            identifier: identifier.clone(),
            subquery: Box::new(rebuilt_subquery),
            is_consulted_view: *is_consulted_view,
        },
        InnerRelationPattern::Indeterminate { identifier, .. } => {
            InnerRelationPattern::Indeterminate {
                identifier: identifier.clone(),
                subquery: Box::new(rebuilt_subquery),
            }
        }
    };

    // THE HEAD CROSSES, and the map its body's rebuild produced is what
    // makes keeping the old boundary true: the boundary standing over this
    // subquery translates its recorded positions onto the ones the rebuilt
    // statement emits.
    let _ = &carried;
    Ok(refined::Chain::ground(head.clone().crossing(|form| {
        let GroundForm::Reference(resolved::Relation::InnerRelation { outer, .. }) = form else {
            unreachable!("the head was just matched as a derived table");
        };
        Ok(GroundForm::Reference(refined::Relation::InnerRelation {
            pattern: refined_pattern,
            alias: None,
            outer,
        }))
    })?))
}
/// Remove correlation filters from a relational expression
/// Public wrapper for use by flattener when recursively flattening INNER-RELATIONs
fn build_ground_relation(
    table: &FlatTable,
    schema_box: crate::relation::SemanticRelation,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    let access = match &table.access {
        // Dequalify/DequalifyAll: USING columns already extracted into join
        // predicates by analyzer. Revert to plain Glob for SQL generation.
        resolved::Access::Dequalify(_) | resolved::Access::DequalifyAll => resolved::Access::All,
        // Glob/Positional/Bare: pass through unchanged.
        // Positional must survive — transformer uses it to generate column renames.
        resolved::Access::All => resolved::Access::All,
        resolved::Access::Slots(exprs) => resolved::Access::Slots(exprs.clone()),
        resolved::Access::Unasked => resolved::Access::Unasked,
    };

    let access = crate::pipeline::ast_transform::walk_transform_access(
        &mut super::RefinerFold::inner(
            crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
            identities,
        ),
        access,
    )?;
    identities
        .authority()
        .ground_read(access, table.outer, schema_box)
}

fn combine_predicates_with_and(
    predicates: Vec<refined::TruthExpression>,
) -> refined::TruthExpression {
    refined::TruthExpression::all(predicates).unwrap_or_else(create_true_literal)
}

fn create_true_literal() -> refined::TruthExpression {
    refined::TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
        left: Box::new(refined::DomainExpression::Application(
            refined::FunctionApplication::Ground(LiteralValue::Number("1".to_string())),
        )),
        right: Box::new(refined::DomainExpression::Application(
            refined::FunctionApplication::Ground(LiteralValue::Number("1".to_string())),
        )),
    })
}

/// Convert a resolved boolean expression to refined, refining InnerExists/InRelational
/// subqueries through the full refiner pipeline. Without this, InnerRelation patterns
/// inside InnerExists stay as Indeterminate and the transformer can't handle them.
pub(super) fn refine_predicate_boolean(
    expr: resolved::TruthExpression,
    identities: &crate::relation::Planning,
) -> Result<refined::TruthExpression> {
    match expr {
        resolved::TruthExpression::Existence(Existence {
            polarity,
            relation: subquery,
            ..
        }) => {
            // Refine the InnerExists subquery through the full refiner pipeline
            let refined_subquery = crate::pipeline::refiner::refine_internal(
                *subquery,
                false,
                crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
                identities,
            )?;
            Ok(refined::TruthExpression::Existence(Existence {
                polarity,
                relation: Box::new(refined_subquery),
                addressing: (),
            }))
        }
        resolved::TruthExpression::RelationalMembership(RelationalMembership {
            probe,
            relation: subquery,
            negated,
            ..
        }) => {
            let refined_subquery = crate::pipeline::refiner::refine_internal(
                *subquery,
                false,
                crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
                identities,
            )?;
            Ok(refined::TruthExpression::RelationalMembership(
                RelationalMembership {
                    probe: crate::pipeline::ast_transform::transform_probe(
                        &mut super::RefinerFold::inner(
                            crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
                            identities,
                        ),
                        probe,
                    )?,
                    relation: Box::new(refined_subquery),
                    negated,
                    addressing: (),
                },
            ))
        }
        resolved::TruthExpression::Conjunction(parts) => Ok(refined::TruthExpression::Conjunction(
            Box::new((*parts).try_map(|part| refine_predicate_boolean(part, identities))?),
        )),
        resolved::TruthExpression::Disjunction(parts) => Ok(refined::TruthExpression::Disjunction(
            Box::new((*parts).try_map(|part| refine_predicate_boolean(part, identities))?),
        )),
        resolved::TruthExpression::Not { expr: inner } => Ok(refined::TruthExpression::Not {
            expr: Box::new(refine_predicate_boolean(*inner, identities)?),
        }),
        // A leaf crosses through the refiner's own fold: its walk copies a
        // chainless leaf and routes a relation nested in one — a scalar
        // subquery in a comparison — through the hub.
        other => super::RefinerFold::inner(
            crate::pipeline::danger_gates::DangerGateMap::with_defaults(),
            identities,
        )
        .transform_boolean(other),
    }
}

/// The deferred items cross the phase as values: the position each was
/// minted for is the interior's, kept through the rebuild by the boundary
/// that carried it, and the value crosses through the refiner's fold like
/// any other.
fn carry_deferred(
    deferred: &[crate::pipeline::asts::core::expressions::DeferredItem<Resolved>],
    fold: &mut super::RefinerFold<'_>,
) -> Result<Vec<crate::pipeline::asts::core::expressions::DeferredItem<Refined>>> {
    deferred
        .iter()
        .map(|item| item.clone().crossing(|value| fold.transform_domain(value)))
        .collect()
}
