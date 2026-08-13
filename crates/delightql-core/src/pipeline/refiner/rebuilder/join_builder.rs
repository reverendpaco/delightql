// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use super::schema_computation::compute_join_schema;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::refined::{self, JoinType};
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::analyzer::AnalyzedSegment;
use crate::pipeline::refiner::flattener;
use crate::pipeline::refiner::rebuilder::{
    apply_top_level_filters, combine_predicates_with_and, table_to_refined,
};
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;
use std::rc::Rc;

/// Rebuild a segment containing only joins
pub(super) fn rebuild_join_segment(
    analyzed: AnalyzedSegment,
    mut op_predicates: HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    is_top_level: bool,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &Rc<crate::names::Registry>,
) -> Result<refined::Chain> {
    // Start with the first table
    if analyzed.tables.is_empty() {
        return Err(DelightQLError::parse_error("No tables in segment"));
    }

    // Validate outer join markers before processing (only at top level)
    // Inner-relation subqueries are not validated because they're not standalone queries
    log::debug!(
        "rebuild_join_segment: is_top_level={}, {} tables",
        is_top_level,
        analyzed.tables.len()
    );
    if is_top_level {
        log::debug!("Running validation...");
        validate_outer_join_markers(&analyzed, &op_predicates)?;
    } else {
        log::debug!("Skipping validation (inner context)");
    }

    let mut result = table_to_refined(
        &analyzed.tables[0],
        &mut op_predicates,
        danger_gates,
        identities,
    )?;
    let mut table_idx = 1;

    // Process operators left to right (CPR-ltr semantics)
    for (op_idx, op) in analyzed.operators.iter().enumerate() {
        let flattener::FlatOperatorKind::Join { correspondence } = &op.kind;
        // The one operator kind: `let` here is irrefutable by construction.
        let (new_result, new_table_idx) = process_single_join(
            result,
            &analyzed,
            table_idx,
            op_idx,
            correspondence,
            &mut op_predicates,
            danger_gates,
            identities,
        )?;
        result = new_result;
        table_idx = new_table_idx;
    }

    // Apply any top-level filters
    result = apply_top_level_filters(result, &mut op_predicates, identities)?;

    Ok(result)
}

/// Process a single join operator
pub(super) fn process_single_join(
    result: refined::Chain,
    analyzed: &AnalyzedSegment,
    table_idx: usize,
    op_idx: usize,
    correspondence: &Option<resolved::Correspondence>,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &Rc<crate::names::Registry>,
) -> Result<(refined::Chain, usize)> {
    // Get the right table for this join
    if table_idx >= analyzed.tables.len() {
        return Err(DelightQLError::parse_error("Not enough tables for join"));
    }

    let right_table = table_to_refined(
        &analyzed.tables[table_idx],
        op_predicates,
        danger_gates,
        identities,
    )?;
    let new_table_idx = table_idx + 1;

    // Get FJC predicates for this join
    let op_ref = OperatorRef::Join { position: op_idx };
    let join_predicates = op_predicates.remove(&op_ref).unwrap_or_default();

    // Build join condition
    let (correlation, leftover_conditions) =
        build_correlation(correspondence, join_predicates, identities)?;

    // Determine join type
    let join_type = determine_join_type(analyzed, table_idx);

    if !leftover_conditions.is_empty() && join_type != JoinType::Inner {
        return Err(crate::error::DelightQLError::validation_error_categorized(
            "join/using/extra_condition",
            "a USING-style join with an additional multi-relation condition \
is not expressible for an outer join: USING has no ON clause to carry it, \
and WHERE placement would change which rows match",
            "write the join fully explicitly: replace .(cols) with equality \
predicates alongside the extra condition",
        ));
    }

    // Build the join with proper schema
    let join_expr = create_join(
        result,
        right_table,
        correlation,
        Some(join_type),
        identities,
    );

    // Inner join: the leftovers filter the joined rows — WHERE placement
    // is exactly equivalent to ON for an inner join.
    let join_expr = if leftover_conditions.is_empty() {
        join_expr
    } else {
        let schema = match join_expr.continuations.last() {
            Some(refined::Continuation::Member { cpr_schema, .. }) => *cpr_schema,
            _ => unreachable!("create_join appends a member"),
        };
        join_expr.then(refined::Continuation::Restrict {
            condition: combine_predicates_with_and(leftover_conditions),
            origin: resolved::FilterOrigin::UserWritten,
            cpr_schema: schema,
        })
    };

    Ok((join_expr, new_table_idx))
}

/// Build join condition from USING columns and predicates
/// Returns (join condition, leftovers). When USING wins the join slot,
/// FJC predicates cannot ride the ON clause — they are RETURNED, never
/// dropped: the caller places them as WHERE (inner join) or refuses
/// (outer join, where placement changes match semantics).
pub(super) fn build_correlation(
    correspondence: &Option<resolved::Correspondence>,
    join_predicates: Vec<AnalyzedPredicate>,
    identities: &Rc<crate::names::Registry>,
) -> Result<(
    Option<refined::MemberCorrelation>,
    Vec<refined::TruthExpression>,
)> {
    let mut correlations = Vec::new();

    log::debug!("build_correlation: {} predicates", join_predicates.len());

    for p in join_predicates {
        log::debug!("Processing predicate: {:?}", p.expr);
        if matches!(p.class, PredicateClass::FJC { .. }) {
            let refined = super::refine_predicate_boolean(p.expr, identities)?;
            correlations.push(downgrade_null_safe_eq(refined));
        }
        // Other predicates (FIC, etc.): not join conditions, skip here.
        // They'll be placed as WHERE filters by the predicate placement logic.
    }

    // The correspondence WINS the join slot. The FJC predicates it displaces
    // are returned, never dropped: USING has no ON clause to carry them.
    Ok(match correspondence {
        Some(correspondence) if !correspondence.is_empty() => {
            log::debug!("Join corresponds on: {:?}", correspondence.columns);
            (
                Some(refined::MemberCorrelation::Correspond(
                    correspondence.clone(),
                )),
                correlations,
            )
        }
        _ if !correlations.is_empty() => (
            Some(refined::MemberCorrelation::Condition(
                combine_predicates_with_and(correlations),
            )),
            Vec::new(),
        ),
        _ => (None, Vec::new()),
    })
}

pub(super) fn create_join(
    left: refined::Chain,
    right: refined::Chain,
    correlation: Option<refined::MemberCorrelation>,
    join_type: Option<JoinType>,
    identities: &Rc<crate::names::Registry>,
) -> refined::Chain {
    let jt = join_type.unwrap_or(JoinType::Inner);
    let cpr_schema = compute_join_schema(&left, &right, jt.clone(), identities);
    left.then(refined::Continuation::Member {
        rhs: right,
        correlation,
        join_type: Some(jt),
        cpr_schema,
    })
}

pub(super) fn determine_join_type(analyzed: &AnalyzedSegment, table_idx: usize) -> JoinType {
    // Markedness, not comma position, determines join role: the unmarked
    // tables form the required core; each ?-marked table LEFT-joins onto
    // the tree; FULL OUTER happens only when EVERY join party is marked
    // (left-fold in written order).

    if analyzed.tables.iter().all(|t| t.outer) {
        return JoinType::FullOuter;
    }
    if analyzed.tables[table_idx].outer {
        return JoinType::LeftOuter;
    }
    // Right side unmarked: if everything joined so far is marked, this
    // table anchors the tree and the accumulated optional cluster
    // preserves as a unit — RIGHT here; the transformer swaps operands
    // back to LEFT.
    if (0..table_idx).all(|i| analyzed.tables[i].outer) {
        JoinType::RightOuter
    } else {
        JoinType::Inner
    }
}

/// Validate that outer join markers have explicit join conditions
///
/// Rule: Standalone table cannot have outer join marker (nothing to join to)
fn validate_outer_join_markers(
    analyzed: &AnalyzedSegment,
    op_predicates: &HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<()> {
    log::debug!(
        "validate_outer_join_markers: checking {} tables, {} operators",
        analyzed.tables.len(),
        analyzed.operators.len()
    );

    log::debug!(
        "  Operators: {:?}",
        analyzed
            .operators
            .iter()
            .map(|op| &op.kind)
            .collect::<Vec<_>>()
    );
    log::debug!(
        "  Predicates by operator: {:?}",
        op_predicates.keys().collect::<Vec<_>>()
    );

    // Rule 1: Check for standalone table with outer marker
    if analyzed.tables.len() == 1 && analyzed.tables[0].outer {
        log::debug!(
            "ERROR: Standalone relation with outer marker: {:?}",
            analyzed.tables[0].identity
        );
        return Err(DelightQLError::parse_error(
            "Outer join marker on standalone relation\n\n\
            The table has an outer join marker (?, <, or >) but there are no other tables\n\
            to join it to. Outer join markers require at least one join operation.\n\n\
            Remove the marker from the relation."
                .to_string(),
        ));
    }

    // Rule 2: FULL OUTER — the all-marked case — requires a join
    // condition. The ? sigil marks its relation as OPTIONAL; a one-sided
    // mark without a condition is a legal LEFT join over the cross
    // product. Only when EVERY relation is marked does the join become
    // FULL OUTER, whose two-part construction needs a condition to find
    // each side's unmatched rows.
    let all_marked = analyzed.tables.iter().all(|t| t.outer);
    if !all_marked {
        return Ok(());
    }

    for (join_idx, _op) in analyzed.operators.iter().enumerate() {
        let right_table_idx = join_idx + 1;
        if right_table_idx >= analyzed.tables.len() {
            continue; // No right table (shouldn't happen but be safe)
        }
        let left_table = &analyzed.tables[join_idx];
        let right_table = &analyzed.tables[right_table_idx];

        let op_ref = OperatorRef::Join { position: join_idx };
        let has_correlation = op_predicates
            .get(&op_ref)
            .map(|preds| !preds.is_empty())
            .unwrap_or(false)
            || matches!(
                &analyzed.operators[join_idx].kind,
                flattener::FlatOperatorKind::Join {
                    correspondence: Some(_)
                }
            );

        if !has_correlation {
            return Err(DelightQLError::parse_error_categorized(
                "general",
                format!(
                    "FULL OUTER JOIN requires an explicit join condition\n\n\
                Every relation in this chain is marked optional (?), which makes\n\
                the join FULL OUTER — but a pair of adjacent relations has no\n\
                condition saying how the two sides align, so there is no way to\n\
                tell which rows of each side are unmatched.\n\n\
                Add a join condition or give the patterns a shared variable.\n\n\
                (One-sided ? marks do not need a condition: the marked relation\n\
                LEFT-joins the rest.)\n\n\
                Affected relation identities: {:?} and {:?}",
                    left_table.identity, right_table.identity,
                ),
            ));
        }
    }

    Ok(())
}

/// Rewrite `null_safe_eq` → `traditional_eq` in join conditions.
///
/// In join position, equality is CORRESPONDENCE: null never matches, because
/// null-matching ON clauses multiply the null groups AND assert a
/// correspondence between absences. There is deliberately no gate back into
/// INDF here — a null that is meant to match is a value wearing null's
/// clothes; the spelling is a named key (coalesce into a marker), never a
/// mode switch.
fn downgrade_null_safe_eq(expr: refined::TruthExpression) -> refined::TruthExpression {
    match expr {
        refined::TruthExpression::Comparison(Comparison {
            operator,
            left,
            right,
        }) if operator == crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual => {
            refined::TruthExpression::Comparison(Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left,
                right,
            })
        }
        refined::TruthExpression::Conjunction(parts) => {
            refined::TruthExpression::Conjunction(Box::new((*parts).map(downgrade_null_safe_eq)))
        }
        refined::TruthExpression::Disjunction(parts) => {
            refined::TruthExpression::Disjunction(Box::new((*parts).map(downgrade_null_safe_eq)))
        }
        refined::TruthExpression::Not { expr: inner } => refined::TruthExpression::Not {
            expr: Box::new(downgrade_null_safe_eq(*inner)),
        },
        other => other,
    }
}
