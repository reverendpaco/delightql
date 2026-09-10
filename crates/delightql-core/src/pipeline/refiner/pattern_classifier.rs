// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Pattern Classifier for INNER-RELATION (SNEAKY-PARENTHESES)
//
// Classifies Indeterminate patterns into:
// - UDT (Uncorrelated Derived Table)
// - CDT-SJ (Correlated Derived Table - Scalar Join)
// - CDT-GJ (Correlated Derived Table - Group Join)
// - CDT-WJ (Correlated Derived Table - Window Join), rewritten to CDT-SJ
//
// A CORRELATION IS A TYPED STEP THE RESOLVER WROTE: the one sealed value
// the correlation act minted, holding the condition the enclosing join
// evaluates and the interior occurrences it reads. Classification takes
// those steps out of the subquery and SPENDS them on the pattern — that is
// the hoisting the enclosing join performs — and reads nothing else to
// decide. What the boundary must keep readable for the hoisted condition
// was carried by construction from the correlation act to the boundary;
// nothing here rebuilds a projection or looks a carrier up, and what comes
// off the pattern is a plain predicate no interface takes back as a
// correlation.
//
// Classification uses the AstTransform walk infrastructure to descend into
// all node types (including operators, ConsultedView bodies, ScalarSubquery,
// InnerExists), so a nested interior is classified before the one standing
// over it.
use crate::error::Result;
use crate::pipeline::ast_transform::AstTransform;
use crate::pipeline::asts::core::expressions::relational::Realized;
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::resolved::{InnerRelationPattern, Resolved};

// =============================================================================
// ClassifierFold — AstTransform<Resolved, Resolved>
// =============================================================================
//
// A same-phase fold that classifies Indeterminate InnerRelation patterns.
// Since this is Resolved→Resolved, it doesn't change the phase or run FAR.
// It only classifies InnerRelation patterns encountered during the walk.

struct ClassifierFold<'a> {
    identities: &'a crate::relation::Planning,
}

impl AstTransform<Resolved, Resolved> for ClassifierFold<'_> {
    crate::pipeline::ast_transform::same_phase_payload_folds!(Resolved);

    /// THE REALIZATION IS THE CLASSIFICATION. The walk has already
    /// descended the subquery — every interior nested in it is realized
    /// by the time this is asked — so an indeterminate pattern is
    /// classified over its descended body, and the chain carrier judges
    /// the answer against the body the head stood over.
    fn realize_interior(
        &mut self,
        pattern: InnerRelationPattern<Resolved>,
    ) -> Result<Realized<Resolved>> {
        match pattern {
            InnerRelationPattern::Indeterminate {
                identifier,
                subquery,
            } => classify_inner_relation_pattern(identifier, *subquery, self.identities),
            already_classified => Ok(Realized::kept(already_classified)),
        }
    }
}

/// Classify all InnerRelation patterns in an AST using the walk infrastructure.
///
/// The walk descends into all node types by construction, including operators,
/// ConsultedView bodies, ScalarSubquery, InnerExists.
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

/// ONE HOISTED CORRELATION: the value the correlation act minted, taken
/// off the interior whole — the condition the enclosing join evaluates and
/// the interior occurrences it reads, still one value until it is spent.
pub(super) type Hoisted = crate::relation::Correlated<Resolved>;

/// Core classification logic for a single InnerRelation pattern.
///
/// The correlations are the typed steps of the shaping run, taken out here
/// and held on the pattern; the subquery that remains is what the derived
/// table evaluates. Aggregation and a row bound are read off the run that
/// remains.
pub fn classify_inner_relation_pattern(
    identifier: resolved::QualifiedName,
    subquery: resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<Realized<Resolved>> {
    // Taking the correlated steps off keeps the body's identity: each
    // published its operand's relation, and every step above lands back on
    // the operand it was derived over.
    let (subquery, hoisted) = take_correlations(subquery, identities)?;
    // THE POSITIONS THE ENCLOSING JOIN COMPUTES are read off the interior's
    // publications by the record the publication act wrote: an item whose
    // position is evaluated at the boundary is copied to the pattern for
    // the join that brings the boundary in. The publication keeps the
    // position; only its emission moves.
    let deferred = deferred_items(&subquery, identities);

    if hoisted.is_empty() && deferred.is_empty() {
        return Ok(Realized::kept(
            InnerRelationPattern::UncorrelatedDerivedTable {
                identifier,
                subquery: Box::new(subquery),
                is_consulted_view: false,
            },
        ));
    }

    // Correlation + a row bound: the bound is per outer row, so the
    // subquery is rewritten to rank within each correlation class and keep
    // the interval the bound spells — a CDT-SJ shape whose body carries the
    // ranking witness. Every spelling of a bound: a cap, a cap with the
    // offset it consumed, a bare offset.
    //
    // THE REALIZATION REPLACES THE BODY, and only the authority may stand
    // a different relation under the boundary's kept identity: the rewrite
    // runs through `rebuilt`, which admits the product only as a rebuild
    // of the body carrying every position it published — recorded as its
    // replacement — and the carrier judges the answer against the body the
    // head stood over.
    if has_bound(&subquery) {
        let replacement = identities.authority().rebuilt(subquery, |subquery| {
            super::cdt_wj_rewriter::rewrite_window_join_subquery(subquery, &hoisted, identities)
        })?;
        let correlation_filters = conditions(hoisted);
        return Ok(Realized::replaced(replacement, |subquery| {
            InnerRelationPattern::CorrelatedScalarJoin {
                identifier,
                correlation_filters,
                deferred,
                subquery,
            }
        }));
    }

    if has_aggregation(&subquery) {
        let aggregations = extract_aggregations(&subquery)?;
        return Ok(Realized::kept(InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters: conditions(hoisted),
            aggregations,
            deferred,
            subquery: Box::new(subquery),
        }));
    }

    Ok(Realized::kept(InnerRelationPattern::CorrelatedScalarJoin {
        identifier,
        correlation_filters: conditions(hoisted),
        deferred,
        subquery: Box::new(subquery),
    }))
}

/// THE ITEMS THE ENCLOSING JOIN COMPUTES, off the interior's shaping run:
/// every one-value publication item whose position the record says its
/// publication stated as evaluated at the boundary — the originating
/// items; a later publication restating one carries the same position —
/// along the spine and each member's right arm, the same extent the
/// correlated steps come off. A nested interior's head owns its own.
fn deferred_items(
    chain: &resolved::Chain,
    identities: &crate::relation::Planning,
) -> Vec<crate::pipeline::asts::core::expressions::DeferredItem<Resolved>> {
    use crate::pipeline::asts::core::{OutItem, PipeOp};
    let mut items = Vec::new();
    for step in chain.continuations() {
        match step.form() {
            resolved::Continuation::Pipe {
                operator: PipeOp::Project(published) | PipeOp::Embed(published),
                ..
            } => {
                for item in published.iter() {
                    let OutItem::One(one) = item else {
                        continue;
                    };
                    items.extend(crate::pipeline::asts::core::expressions::DeferredItem::of(
                        one, identities,
                    ));
                }
            }
            resolved::Continuation::Member { rhs, .. } => {
                items.extend(deferred_items(rhs, identities));
            }
            _ => {}
        }
    }
    items
}

/// SPEND the hoisted correlations on the pattern: the conditions the
/// enclosing join carries, as plain predicates.
fn conditions(hoisted: Vec<Hoisted>) -> Vec<resolved::TruthExpression> {
    hoisted.into_iter().map(Hoisted::into_condition).collect()
}

/// TAKE THE CORRELATED STEPS OUT of the interior's shaping run.
///
/// The run is the top-level continuations and each member's right arm,
/// recursively — the steps whose relations the boundary stands over. A
/// nested interior's head owns its own correlations and is not entered; a
/// bag arm cannot carry one, because the set that would stand over it
/// refused at construction; a condition's own subquery evaluates in place.
/// The steps come off in authored order, whole: a transparent step comes off
/// a chain without moving what any other node publishes, and every step
/// above lands back on the operand it was derived over.
fn take_correlations(
    chain: resolved::Chain,
    identities: &crate::relation::Planning,
) -> Result<(resolved::Chain, Vec<Hoisted>)> {
    let mut hoisted = Vec::new();
    let chain = take_correlations_into(chain, &mut hoisted, identities)?;
    Ok((chain, hoisted))
}

#[stacksafe::stacksafe]
fn take_correlations_into(
    chain: resolved::Chain,
    out: &mut Vec<Hoisted>,
    identities: &crate::relation::Planning,
) -> Result<resolved::Chain> {
    let peeled = match chain.peel() {
        Ok(peeled) => peeled,
        Err(chain) => return Ok(chain),
    };
    let (operand, last) = peeled.split();
    let operand = take_correlations_into(operand, out, identities)?;
    match last.form() {
        // THE STEP COMES OFF WHOLE: the value the act minted, and nothing
        // else, is what the boundary spends. Its relation stays what it was
        // — the step published its operand's — so the operand stands.
        resolved::Continuation::Correlated(_) => {
            let resolved::Continuation::Correlated(correlated) = last.into_form() else {
                unreachable!("the step was just matched as a correlated restriction")
            };
            out.push(correlated);
            Ok(operand)
        }
        resolved::Continuation::Member { .. } => {
            let last = last.rebuilding_arm(|rhs| take_correlations_into(rhs, out, identities))?;
            identities.authority().reland(operand, last)
        }
        resolved::Continuation::Restrict { .. }
        | resolved::Continuation::Access { .. }
        | resolved::Continuation::Bound { .. }
        | resolved::Continuation::Correlate { .. }
        | resolved::Continuation::Destructure { .. }
        | resolved::Continuation::Pipe { .. }
        | resolved::Continuation::Structural(_)
        | resolved::Continuation::BagOp { .. }
        | resolved::Continuation::ErJoin(_) => identities.authority().reland(operand, last),
    }
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

/// Does the top-level shaping run hold a row bound — a cap, an offset, or
/// the one an ordering consumed?
///
/// Rides [`Chain::source_spine`] and asks each step for the bound it
/// carries; a bound inside a member's chain, a bag arm, or a subquery is
/// not top-level, so the walk stops at the continuation that brings that
/// relation in. Pinned by
/// `source_spine_reads_restrictions_and_pipes_outermost_first` and
/// `source_spine_stops_at_a_member_without_entering_either_relation`.
fn has_bound(expr: &resolved::Chain) -> bool {
    expr.source_spine().any(|step| step.bound().is_some())
}
