// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Pattern Classifier for INNER-RELATION (SNEAKY-PARENTHESES)
//
// Classifies Indeterminate patterns into:
// - UDT (Uncorrelated Derived Table)
// - CDT-SJ (Correlated Derived Table - Scalar Join)
// - CDT-GJ (Correlated Derived Table - Group Join)
// - CDT-WJ (Correlated Derived Table - Window Join)
//
// Classification uses the AstTransform walk infrastructure to descend into
// all node types (including operators, ConsultedView bodies, ScalarSubquery,
// InnerExists). This fixes the classify_operator() no-op bug by construction.
use super::correlation_analyzer;
use crate::error::Result;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::resolved::{InnerRelationPattern, Resolved};

// =============================================================================
// ClassifierFold — AstTransform<Resolved, Resolved>
// =============================================================================
//
// A same-phase fold that classifies Indeterminate InnerRelation patterns.
// Uses the walk infrastructure to descend into operators, ConsultedView bodies,
// ScalarSubquery, InnerExists — everywhere the hand-rolled classify_patterns
// failed to recurse (the "classify_operator no-op" bug).
//
// Since this is Resolved→Resolved, it doesn't change the phase or run FAR.
// It only classifies InnerRelation patterns encountered during the walk.

struct ClassifierFold<'a> {
    identities: &'a crate::relation::Planning,
}

impl AstTransform<Resolved, Resolved> for ClassifierFold<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Resolved);

    fn transform_inner_relation(
        &mut self,
        pattern: InnerRelationPattern<Resolved>,
    ) -> Result<InnerRelationPattern<Resolved>> {
        match pattern {
            InnerRelationPattern::Indeterminate {
                identifier,
                subquery,
            } => {
                // Recursively classify the subquery first (the walk calls
                // transform_relational_action on the subquery, which eventually
                // calls transform_inner_relation for any nested patterns).
                let classified_subquery = self.transform_relational_action(*subquery)?.into_inner();

                // Classify this pattern based on the classified subquery.
                classify_inner_relation_pattern(identifier, classified_subquery, self.identities)
            }
            // Already classified — let the walk handle recursion into children
            other => crate::pipeline::ast_transform::walk_transform_inner_relation(self, other),
        }
    }
}

/// Classify all InnerRelation patterns in an AST using the walk infrastructure.
///
/// The walk descends into all node types by construction, including operators,
/// ConsultedView bodies, ScalarSubquery, InnerExists — fixing the
/// classify_operator() no-op bug.
pub fn classify_patterns_via_fold(
    ast: resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    let mut fold = ClassifierFold { identities };
    fold.transform_relational_action(ast)
        .map(|a| a.into_inner())
}

// =============================================================================
// Core Classification Logic
// =============================================================================

/// Core classification logic for a single InnerRelation pattern.
/// Inspects the subquery for correlation, aggregation, and limits.
pub fn classify_inner_relation_pattern(
    identifier: resolved::QualifiedName,
    subquery: resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<InnerRelationPattern<Resolved>> {
    // Step 1: Detect (but don't extract!) correlation filters from the subquery
    // The filters stay IN the subquery - we just use them for pattern detection
    let correlation_filters =
        correlation_analyzer::detect_correlation_filters_in_scope(&subquery, identities)?;

    // Step 2: Check if uncorrelated
    if correlation_filters.is_empty() {
        // No correlation → UDT
        return Ok(InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            subquery: Box::new(subquery),
            is_consulted_view: false,
        });
    }

    // Step 3: Has correlation + LIMIT — structurally rewrite into a
    // CDT-SJ-shaped subquery whose body explicitly contains a ROW_NUMBER()
    // window expression and a `WHERE rn <= N` filter (Fork-1, P0').
    // The rewriter also runs hygienic-column injection when the user's
    // projection strips correlation columns. It is called directly rather
    // than by recursing through classify, which would re-run injection on a
    // shape that no longer matches its trigger.
    if has_limit(&subquery) {
        let rewritten = super::cdt_wj_rewriter::rewrite_window_join_subquery(
            subquery,
            &correlation_filters,
            identities,
        )?;
        return Ok(InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery: Box::new(rewritten),
        });
    }

    // Inject hygienic columns if projection excludes correlation columns
    // This must happen BEFORE flattening so the flattener can rewrite predicates
    let final_subquery =
        inject_hygienic_columns_if_needed(subquery, &correlation_filters, identities)?;

    // Step 4: Check for aggregation (CDT-GJ pattern)
    if has_aggregation(&final_subquery) {
        let aggregations = extract_aggregations(&final_subquery)?;
        return Ok(InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery: Box::new(final_subquery),
        });
    }

    // Step 5: Default - Correlated Scalar Join
    Ok(InnerRelationPattern::CorrelatedScalarJoin {
        identifier,
        correlation_filters,
        subquery: Box::new(final_subquery),
    })
}

// ============================================================================
// Helper Functions - Aggregation Detection
// ============================================================================

/// Does the top-level shaping run hold an aggregation operator?
///
/// Rides [`Chain::source_spine`]: an aggregation inside a member's chain, a
/// bag arm, or a subquery is NOT top-level, so the walk stops at the
/// continuation that brings that relation in and answers `false`. Pinned by
/// `source_spine_reads_restrictions_and_pipes_outermost_first` and
/// `source_spine_stops_at_a_member_without_entering_either_relation`.
fn has_aggregation(expr: &resolved::Chain) -> bool {
    use crate::pipeline::asts::core::expressions::chain::SpineStep;
    expr.source_spine()
        .any(|step| matches!(step, SpineStep::Pipe(resolved::PipeOp::Group(_))))
}

fn extract_aggregations(_expr: &resolved::Chain) -> Result<Vec<resolved::DomainExpression>> {
    // TODO: Extract aggregation expressions from GroupBy/WholeTableAggregation operators
    Ok(Vec::new())
}

// ============================================================================
// Helper Functions - Limit/Order By Detection
// ============================================================================

/// Does the top-level shaping run hold a `#<N` bound — arbitrary, or the
/// one an ordering consumed?
///
/// Rides [`Chain::source_spine`] and asks each step for the bound it
/// carries; a bound inside a member's chain, a bag arm, or a subquery is
/// not top-level, so the walk stops at the continuation that brings that
/// relation in. Pinned by
/// `source_spine_reads_restrictions_and_pipes_outermost_first` and
/// `source_spine_stops_at_a_member_without_entering_either_relation`.
fn has_limit(expr: &resolved::Chain) -> bool {
    expr.source_spine().any(|step| {
        matches!(
            step.bound(),
            Some(resolved::TupleOrdinalClause {
                operator: resolved::TupleOrdinalOperator::LessThan,
                value: _,
                offset: _,
            })
        )
    })
}

// ============================================================================
// Hygienic Column Injection
// ============================================================================

/// Inject hygienic columns into the projection when it strips correlation
/// columns.
///
/// The injected carriers are found again by asking the registry — see
/// [`correlation_carriers`] — so nothing here is returned to be stored.
pub(crate) fn inject_hygienic_columns_if_needed(
    subquery: resolved::Chain,
    correlation_filters: &[resolved::TruthExpression],
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    let inner = subquery.semantic_relation();
    let correlation_columns =
        correlation_analyzer::extract_correlation_columns(correlation_filters, inner, identities);

    inject_hygienic_carriers(subquery, &correlation_columns, identities)
}

/// Carry exact support positions through the projection that would otherwise
/// strip them. Callers already own the resolved positions; this operation
/// only performs the generic projection rebuild.
pub(crate) fn inject_hygienic_carriers(
    subquery: resolved::Chain,
    correlation_columns: &[crate::relation::PortId],
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    inject_carriers(subquery, correlation_columns, identities, false)
}

pub(crate) fn inject_crossing_carriers(
    subquery: resolved::Chain,
    carriers: &[crate::relation::PortId],
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    inject_carriers(subquery, carriers, identities, true)
}

fn inject_carriers(
    subquery: resolved::Chain,
    correlation_columns: &[crate::relation::PortId],
    identities: &crate::relation::Planning,
    crossing: bool,
) -> Result<resolved::Chain> {
    if correlation_columns.is_empty() {
        return Ok(subquery);
    }

    // WHICH STEP DROPPED IT IS A CHAIN QUESTION, NOT A LAST-STEP ONE. Only
    // a projection can strip a correlation column, but the steps a projection
    // is followed by — an ordering, a bound, a restriction — publish the
    // NARROWED heading, so a projection three steps back has dropped the
    // column just as completely as a trailing one. The carrier is injected at
    // the projection that dropped it, because that is the last level standing
    // on the operand that still has it; every transparent step above carries
    // the dependency onward.
    let Some(at) = subquery.continuations().iter().rposition(|step| {
        matches!(
            step.form(),
            resolved::Continuation::Pipe {
                operator: resolved::PipeOp::Project(_) | resolved::PipeOp::Embed(_),
                ..
            }
        )
    }) else {
        // No projection anywhere: every column the operand published is
        // still published, so there is nothing to carry.
        return Ok(subquery);
    };

    // A STEP THAT REPLACES THE RELATION CANNOT CARRY WHAT IT DOES NOT KNOW
    // ABOUT. Injecting under a grouping or a set would leave the carrier
    // owed by a relation nothing above publishes it from; the transparent
    // run is exactly the steps whose result IS the projection's.
    let final_relation = subquery.semantic_relation();
    let tail: Vec<_> = subquery.continuations()[at + 1..].to_vec();
    if tail.iter().any(|step| *step.result() != final_relation) {
        return Ok(subquery);
    }

    // Extract the projection expressions
    let projection = subquery.continuations().get(at).cloned();
    if let Some(projection) = projection {
        let pipe_relation = *projection.result();
        if let resolved::Continuation::Pipe {
            operator: pipe_operator,
            named: (),
        } = projection.into_form()
        {
            if let resolved::PipeOp::Project(items) | resolved::PipeOp::Embed(items) =
                &pipe_operator
            {
                // Check which correlation columns are missing from projection.
                // The occurrence an item READS is what containment asks about,
                // not the one it publishes.
                fn projected_column(item: &resolved::OutItem) -> Option<crate::relation::PortId> {
                    match item.value() {
                        Some(resolved::DomainExpression::Reference(Reference::Named(
                            NamedReference(ColumnOccurrence { column, .. }),
                        ))) => Some(*column),
                        _ => None,
                    }
                }
                let projected_columns: std::collections::HashSet<crate::relation::PortId> =
                    items.iter().filter_map(projected_column).collect();

                let new_items = items.clone().into_vec();
                let mut rebuilt = subquery.clone();
                rebuilt = rebuilt.truncated(at);
                let operand = rebuilt.semantic_relation();
                let operand_ports = crate::relation::published_ports(identities, &operand)?;
                let mut carriers = Vec::new();

                for source in correlation_columns {
                    // Already-projected is a CHAIN question, not a ColId one:
                    // the projection references a downstream occurrence of the
                    // access column the correlation names, and injecting beside
                    // it mints a second carrier of the same value — which later
                    // makes a by-value re-anchor genuinely ambiguous.
                    let authority = identities.authority();
                    let source_token = authority.residual_row_token(*source);
                    let token_already_projected = source_token.is_some_and(|token| {
                        projected_columns.iter().any(|projected| {
                            authority.residual_row_token(*projected) == Some(token)
                        })
                    });
                    if projected_columns.contains(source) || token_already_projected {
                        continue;
                    }
                    // THE CARRIER IS NOT A POSITION OF THE RESULT. It is an
                    // input position a hoisted correlation still reads, which is
                    // what a dependency IS: the heading the caller addresses is
                    // unchanged, and lowering emits the carrier beside it as
                    // physical support that every boundary above carries.
                    //
                    // WHICH position of the operand that is comes from the
                    // construction record: a boundary between the read and this
                    // projection republished it, and the carrier is the position
                    // standing here, not the one the correlation was written
                    // against.
                    if let Some(value) = authority.residual_capture_value(*source) {
                        let matches: Vec<_> = operand_ports
                            .iter()
                            .copied()
                            .filter(|port| authority.residual_capture_value(*port) == Some(value))
                            .collect();
                        match matches.as_slice() {
                            [landed] => {
                                carriers.push(*landed);
                                continue;
                            }
                            [] if crossing => continue,
                            [] | [_, _, ..] => {
                                return Err(crate::error::DelightQLError::transformation_error(
                                    "a residual configured value does not land exactly once in the projection operand",
                                    "correlation injection",
                                ));
                            }
                        }
                    }
                    if let Some(token) = authority.residual_row_token(*source) {
                        let matches: Vec<_> = operand_ports
                            .iter()
                            .copied()
                            .filter(|port| authority.residual_row_token(*port) == Some(token))
                            .collect();
                        match matches.as_slice() {
                            [landed] => {
                                carriers.push(*landed);
                                continue;
                            }
                            [] if crossing => continue,
                            [] | [_, _, ..] => {
                                return Err(crate::error::DelightQLError::transformation_error(
                                    "a residual row token does not land exactly once in the projection operand",
                                    "correlation injection",
                                ));
                            }
                        }
                    }
                    let Some(landed) =
                        crate::relation::landed_in(identities, &operand_ports, *source)?
                    else {
                        return Err(crate::error::DelightQLError::transformation_error(
                            "a correlation carrier is not a position of the projection operand",
                            "correlation injection",
                        ));
                    };
                    carriers.push(landed);
                }

                if carriers.is_empty() {
                    // All correlation columns already present
                    return Ok(subquery);
                }

                let authority = identities.authority();
                let injected = {
                    // ONE ACT: the injected interface, the items that stand
                    // at it, and the map from the projection this REPLACES
                    // are all written by the same derivation. Nothing here
                    // asks afterwards whether two finished relations are
                    // related — the replacement says where its operand's
                    // positions went while it is putting them there.
                    let (staged, _) = authority.bind(if crossing {
                        crate::relation::pending::Pending::CrossingCarrierInjection {
                            replaces: pipe_relation,
                            carriers,
                            items: new_items,
                            stored: match &pipe_operator {
                                resolved::PipeOp::Embed(_) => {
                                    crate::relation::pending::Publishes::Edited
                                }
                                _ => crate::relation::pending::Publishes::Anew,
                            },
                        }
                    } else {
                        crate::relation::pending::Pending::CarrierInjection {
                            replaces: pipe_relation,
                            carriers,
                            items: new_items,
                            stored: match &pipe_operator {
                                resolved::PipeOp::Embed(_) => {
                                    crate::relation::pending::Publishes::Edited
                                }
                                _ => crate::relation::pending::Publishes::Anew,
                            },
                        }
                    })?;
                    let mut chain = authority.reland(rebuilt, staged)?;
                    // Every tail step continues onto what the previous one
                    // published: a transparent step is RESTATED there, and a
                    // stage republication (the ordering) is re-derived over
                    // the injected operand with its landing recorded — so
                    // references that resolved against the old stage bind
                    // through the record.
                    let mut stood = pipe_relation;
                    for step in tail {
                        let next = *step.result();
                        chain = authority.continue_over(chain, step, stood)?;
                        stood = next;
                    }
                    chain
                };
                return Ok(injected);
            }
        }
    }

    Ok(subquery)
}

/// The correlation carriers a subquery publishes, each paired with what it
/// stands for.
///
/// The subquery's own scope is resolved here; the answer comes from the one
/// authority, which reads the registry. Nothing records this a second time,
/// so there is nothing to drift.
pub(super) fn correlation_carriers(
    subquery: &resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<Vec<(crate::relation::PortId, crate::relation::PortId)>> {
    let Some(last) = subquery.continuations().last() else {
        return Ok(Vec::new());
    };
    let resolved::Continuation::Pipe {
        operator: resolved::PipeOp::Project(items) | resolved::PipeOp::Embed(items),
        ..
    } = last.form()
    else {
        return Ok(Vec::new());
    };
    let outputs = crate::relation::published_ports(identities, last.result())?;
    Ok(items
        .iter()
        .filter_map(|item| {
            let one = match item {
                resolved::OutItem::One(one) => one,
                _ => return None,
            };
            let source = match &one.expr {
                resolved::DomainExpression::Reference(Reference::Named(NamedReference(
                    ColumnOccurrence { column, .. },
                ))) => *column,
                _ => return None,
            };
            let output = *one.output();
            outputs.contains(&output).then_some((source, output))
        })
        .collect())
}
