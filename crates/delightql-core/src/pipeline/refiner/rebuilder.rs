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
mod schema_computation;

use self::exists_handler::nest_interdependent_exists;
use self::join_builder::rebuild_join_segment;
use self::schema_computation::compute_filter_schema;
use super::analyzer::AnalyzedSegment;
use super::flattener::FlatTable;
use super::types::*;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::{Comparison, Existence, RelationalMembership};
use crate::pipeline::asts::refined::{LiteralValue, Refined};
use crate::pipeline::asts::resolved::{InnerRelationPattern, Resolved};
use crate::pipeline::asts::{refined, resolved};
use std::collections::HashMap;
use std::rc::Rc;

/// Main entry point - rebuild an analyzed segment into refined AST
pub(super) fn rebuild_internal(
    analyzed: AnalyzedSegment,
    is_top_level: bool,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &Rc<crate::names::Registry>,
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
            return Err(DelightQLError::parse_error(format!(
                "Forbidden predicate: {:?} (reason: {:?})",
                pred.expr, reason
            )));
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
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    apply_filter_predicates(result, op_predicates, OperatorRef::TopLevel, identities)
}

/// Apply filter predicates for a given operator reference
fn apply_filter_predicates(
    mut result: refined::Chain,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    op_ref: OperatorRef,
    identities: &Rc<crate::names::Registry>,
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
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    let cpr_schema = compute_filter_schema(&source, identities);
    Ok(source.then(refined::Continuation::Restrict {
        condition: refine_predicate_boolean(pred.expr.clone(), identities)?,
        origin: pred.origin,
        cpr_schema,
    }))
}

/// Convert a flat table to a refined relation
fn table_to_refined(
    table: &FlatTable,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    // Check if this is a consulted view (stored as opaque resolved Query)
    if let Some(ref query) = table.consulted_view_query {
        let refined_body =
            crate::pipeline::refiner::refine_query(query.as_ref().clone(), Rc::clone(identities))?;
        let result = refined::Chain::relation(refined::Relation::ConsultedView {
            body: Box::new(refined_body),
            scoped: table.schema,
            outer: table.outer,
        });

        return apply_table_filters(result, table, identities);
    }

    // Check if this is a pipe expression
    if let Some(ref pipe_expr) = table.pipe_expr {
        // Recursively refine the pipe expression
        // Pass is_top_level=false to skip outer join validation (this is an inner context)
        // The gates travel with the recursion: a danger the writer armed
        // on the query is armed inside the relation it stands on.
        return crate::pipeline::refiner::refine_internal(
            pipe_expr.as_ref().clone(),
            false,
            danger_gates.clone(),
            Rc::clone(identities),
        );
    }

    let result = build_base_relation(table, danger_gates, identities)?;
    // A filter kept local by the flattener belongs to the table it was
    // attached to regardless of relation kind. Applying these only to
    // consulted views silently discarded the same constraint on a structural
    // higher-order carrier.
    let mut result = apply_table_filters(result, table, identities)?;

    log::debug!("table_to_refined: Processing {:?}", table.identity);
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
                if *target == table.identity {
                    if let resolved::FilterOrigin::PositionalLiteral { source } = &pred.origin {
                        log::debug!(
                            "      Checking PositionalLiteral: source={:?}, table={:?}, op_ref={:?}",
                            source,
                            table.identity,
                            op_ref
                        );
                        log::debug!(
                            "      Applying PositionalLiteral filter to table {:?}",
                            table.identity
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
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    for (filter_expr, origin) in &table._table_filters {
        let refined_condition = refine_predicate_boolean(filter_expr.clone(), identities)?;
        result = result.then(refined::Continuation::Restrict {
            condition: refined_condition,
            origin: origin.clone(),
            cpr_schema: table.schema,
        });
    }
    Ok(result)
}

/// Build the base relation from a flat table
fn build_base_relation(
    table: &FlatTable,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    let schema_box = table.schema;

    if let Some(ref tvf_data) = table.tvf_data {
        return build_tvf_relation(tvf_data, schema_box);
    }

    if let Some(ref anon_data) = table.anonymous_data {
        return build_anonymous_relation(anon_data, table.outer, schema_box);
    }

    if let Some(ref inner_pattern) = table.inner_relation_pattern {
        // PHASE 5: Use flattened subquery if available (recursive flattening)
        if let Some(ref subquery_segment) = table.subquery_segment {
            return build_inner_relation_from_flattened(
                inner_pattern,
                subquery_segment,
                table.preminted_scope,
                table.outer,
                schema_box,
                danger_gates,
                identities,
            );
        } else {
            // Fallback: Old behavior (re-process AST)
            return build_inner_relation(
                inner_pattern,
                table.preminted_scope,
                table.outer,
                schema_box,
            );
        }
    }

    build_ground_relation(table, schema_box)
}

/// Build a TVF relation
fn build_tvf_relation(
    tvf_data: &super::flattener::TvfData,
    schema_box: crate::names::ScopeId,
) -> Result<refined::Chain> {
    let ho_arguments = tvf_data
        .arguments
        .iter()
        .map(|argument| {
            Ok(match argument {
                Some(argument) => crate::pipeline::asts::core::operators::HoArgument::Value(
                    crate::pipeline::asts::core::ArgumentValue::plain(super::carry::domain(
                        argument.clone(),
                    )?),
                ),
                // A valueless position rides back as the skip it is.
                None => crate::pipeline::asts::core::operators::HoArgument::Skip,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // A TVF is a READ like any other: the relation and what its parens asked
    // of it. Rebuilding the callable alone drops the access between the
    // resolved and refined trees, where nothing downstream can recover it.
    Ok(refined::Chain::read(
        refined::Relation::FunctorCall {
            alias: (),
            call: refined::SealedCall::from_inner(
                refined::FunctorCall {
                    callee: tvf_data.function,
                    arguments:
                        crate::pipeline::asts::core::operators::CallArguments::higher_order(
                            ho_arguments,
                        ),
                    marks: Default::default(),
                },
                false,
            ),
            cpr_schema: schema_box,
        },
        super::carry::access(tvf_data.access.clone())?,
        schema_box,
    ))
}

/// Build an anonymous table relation
fn build_anonymous_relation(
    anon_data: &super::flattener::AnonymousTableData,
    outer: bool,
    schema_box: crate::names::ScopeId,
) -> Result<refined::Chain> {
    let table = super::carry::anon_table(resolved::AnonTable {
        body: anon_data.body.clone(),
        cpr_schema: schema_box,
    })?;
    Ok(refined::Chain::ground(refined::Grelex::Literal(
        refined::AnonRelation {
            table,
            alias: None,
            outer,
        },
    )))
}

/// Build an INNER-RELATION
fn build_inner_relation(
    pattern: &InnerRelationPattern<Resolved>,
    preminted_scope: Option<crate::names::ScopeId>,
    outer: bool,
    schema_box: crate::names::ScopeId,
) -> Result<refined::Chain> {
    // For CDT-SJ and CDT-GJ: Remove correlation filters from subquery since they've been hoisted to JOIN ON
    let cleaned_pattern = match pattern {
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery,
        } => {
            // Remove the correlation filters from inside the subquery
            let cleaned_subquery =
                remove_correlation_filters_from_expr(subquery, correlation_filters);

            // Hygienic injections were already done by pattern_classifier
            // Just preserve them through the phase conversion
            InnerRelationPattern::CorrelatedScalarJoin {
                identifier: identifier.clone(),
                correlation_filters: correlation_filters.clone(),
                subquery: Box::new(cleaned_subquery),
            }
        }
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery,
        } => {
            // For CDT-GJ: Remove correlation filters from subquery, just like CDT-SJ!
            //
            // Discovery: User must explicitly include correlation column in modulo operator:
            //   orders(, orders.user_id = users.id |> %(user_id ~> count:(*)))
            //                                         ^^^^^^^^ explicit GROUP BY
            //
            // The correlation filter gets hoisted to JOIN ON (just like CDT-SJ)
            // The GROUP BY is already explicit in the modulo operator
            // No need to keep correlation filters inside the subquery!
            let cleaned_subquery =
                remove_correlation_filters_from_expr(subquery, correlation_filters);

            // Hygienic injections were already done by pattern_classifier
            InnerRelationPattern::CorrelatedGroupJoin {
                identifier: identifier.clone(),
                correlation_filters: correlation_filters.clone(),
                aggregations: aggregations.clone(),
                subquery: Box::new(cleaned_subquery),
            }
        }
        other => panic!(
            "catch-all hit in rebuilder.rs build_inner_relation (pattern clean): {:?}",
            other
        ),
    };

    let refined_pattern: InnerRelationPattern<Refined> =
        super::carry::inner_relation(cleaned_pattern)?;

    Ok(refined::Chain::relation(refined::Relation::InnerRelation {
        pattern: refined_pattern,
        preminted_scope,
        alias: None,
        outer,
        cpr_schema: schema_box,
    }))
}

/// Build INNER-RELATION from flattened subquery segment (PHASE 5: Recursive FAR)
/// This is the new code path that uses the pre-flattened subquery instead of re-processing AST
fn build_inner_relation_from_flattened(
    pattern: &InnerRelationPattern<Resolved>,
    subquery_segment: &super::flattener::FlatSegment,
    preminted_scope: Option<crate::names::ScopeId>,
    outer: bool,
    schema_box: crate::names::ScopeId,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    // The subquery segment has already been flattened
    // Correlation filters have already been hoisted
    // We need to: analyze it, then rebuild it

    // Analyze the flattened subquery segment
    let analyzed_subquery = super::analyzer::analyze(subquery_segment.clone(), identities)?;

    // Recursively rebuild the analyzed segment into a Refined AST
    // Pass is_top_level=false to skip outer join validation (this is an inner context)
    let rebuilt_subquery = rebuild_internal(analyzed_subquery, false, danger_gates, identities)?;

    // Convert pattern from Resolved to Refined, replacing the subquery with the rebuilt one
    let refined_pattern: InnerRelationPattern<Refined> = match pattern {
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            ..
        } => InnerRelationPattern::CorrelatedScalarJoin {
            identifier: identifier.clone(),
            correlation_filters: correlation_filters
                .iter()
                .map(|f| super::carry::boolean(f.clone()))
                .collect::<Result<Vec<_>>>()?,
            subquery: Box::new(rebuilt_subquery),
        },
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            ..
        } => InnerRelationPattern::CorrelatedGroupJoin {
            identifier: identifier.clone(),
            correlation_filters: correlation_filters
                .iter()
                .map(|f| super::carry::boolean(f.clone()))
                .collect::<Result<Vec<_>>>()?,
            aggregations: aggregations
                .iter()
                .map(|a| super::carry::domain(a.clone()))
                .collect::<Result<Vec<_>>>()?,
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

    Ok(refined::Chain::relation(refined::Relation::InnerRelation {
        pattern: refined_pattern,
        preminted_scope,
        alias: None,
        outer,
        cpr_schema: schema_box,
    }))
}
/// Remove correlation filters from a relational expression
/// Public wrapper for use by flattener when recursively flattening INNER-RELATIONs
pub fn remove_correlation_filters_from_expr(
    expr: &resolved::Chain,
    filters_to_remove: &[resolved::TruthExpression],
) -> resolved::Chain {
    // Only the shaping run and the members carry hoistable correlation
    // filters; a bag arm and the head are cleaned through their own roads.
    let mut cleaned = resolved::Chain {
        head: expr.head.clone(),
        continuations: Vec::with_capacity(expr.continuations.len()),
    };
    if let resolved::Grelex::Reference(rel) = &expr.head {
        cleaned.head = resolved::Grelex::Reference(remove_correlation_filters_from_relation(
            rel,
            filters_to_remove,
        ));
    }
    for continuation in &expr.continuations {
        match continuation {
            resolved::Continuation::Restrict {
                condition,
                origin,
                cpr_schema,
            } => {
                if filters_to_remove.contains(condition) {
                    // Hoisted to the join's ON clause; it does not stand twice.
                    continue;
                }
                cleaned
                    .continuations
                    .push(resolved::Continuation::Restrict {
                        condition: condition.clone(),
                        origin: origin.clone(),
                        cpr_schema: cpr_schema.clone(),
                    });
            }
            resolved::Continuation::Member {
                rhs,
                correlation,
                join_type,
                cpr_schema,
            } => {
                cleaned.continuations.push(resolved::Continuation::Member {
                    rhs: remove_correlation_filters_from_expr(rhs, filters_to_remove),
                    correlation: correlation.clone(),
                    join_type: join_type.clone(),
                    cpr_schema: cpr_schema.clone(),
                });
            }
            other => cleaned.continuations.push(other.clone()),
        }
    }
    cleaned
}

/// Clean the correlation filters a relation HEAD carries in its own
/// subqueries: a nested inner relation can hold a filter the outer segment
/// hoisted.
fn remove_correlation_filters_from_relation(
    rel: &resolved::Relation,
    filters_to_remove: &[resolved::TruthExpression],
) -> resolved::Relation {
    {
        {
            // Handle nested INNER-RELATIONs - correlation filters from outer relations
            // might be inside nested INNER-RELATION subqueries
            match rel {
                resolved::Relation::InnerRelation {
                    pattern,
                    preminted_scope,
                    alias: _,
                    outer,
                    cpr_schema,
                } => {
                    // Recursively clean the subquery in the pattern
                    let cleaned_pattern = match pattern {
                        resolved::InnerRelationPattern::CorrelatedScalarJoin {
                            identifier,
                            correlation_filters,
                            subquery,
                        } => resolved::InnerRelationPattern::CorrelatedScalarJoin {
                            identifier: identifier.clone(),
                            correlation_filters: correlation_filters.clone(),
                            subquery: Box::new(remove_correlation_filters_from_expr(
                                subquery,
                                filters_to_remove,
                            )),
                        },
                        resolved::InnerRelationPattern::CorrelatedGroupJoin {
                            identifier,
                            correlation_filters,
                            aggregations,
                            subquery,
                        } => resolved::InnerRelationPattern::CorrelatedGroupJoin {
                            identifier: identifier.clone(),
                            correlation_filters: correlation_filters.clone(),
                            aggregations: aggregations.clone(),
                            subquery: Box::new(remove_correlation_filters_from_expr(
                                subquery,
                                filters_to_remove,
                            )),
                        },
                        resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                            identifier,
                            subquery,
                            is_consulted_view,
                        } => resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                            identifier: identifier.clone(),
                            subquery: Box::new(remove_correlation_filters_from_expr(
                                subquery,
                                filters_to_remove,
                            )),
                            is_consulted_view: *is_consulted_view,
                        },
                        // Indeterminate: pass through (no subquery with filters to clean)
                        resolved::InnerRelationPattern::Indeterminate { .. } => pattern.clone(),
                    };

                    resolved::Relation::InnerRelation {
                        pattern: cleaned_pattern,
                        preminted_scope: *preminted_scope,
                        alias: None,
                        outer: *outer,
                        cpr_schema: cpr_schema.clone(),
                    }
                }
                // Other relation types: no subqueries with correlation filters to clean
                other => other.clone(),
            }
        }
    }
}

fn build_ground_relation(
    table: &FlatTable,
    schema_box: crate::names::ScopeId,
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

    Ok(refined::Relation::ground_read(
        super::carry::access(access)?,
        table.outer,
        schema_box,
    ))
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
    identities: &Rc<crate::names::Registry>,
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
                Rc::clone(identities),
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
                Rc::clone(identities),
            )?;
            Ok(refined::TruthExpression::RelationalMembership(
                RelationalMembership {
                    probe: super::carry::probe(probe)?,
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
        // Everything else: nothing to refine, so it is carried.
        other => super::carry::boolean(other),
    }
}
