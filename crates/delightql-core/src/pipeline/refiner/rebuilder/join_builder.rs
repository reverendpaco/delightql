// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::refined::{self, JoinType};
use crate::pipeline::asts::resolved;
use crate::pipeline::refiner::analyzer::AnalyzedSegment;
use crate::pipeline::refiner::flattener;
use crate::pipeline::refiner::rebuilder::{
    apply_top_level_filters, combine_predicates_with_and, table_to_refined,
};
use crate::pipeline::refiner::types::*;
use std::collections::HashMap;

/// Rebuild a segment containing only joins
pub(super) fn rebuild_join_segment(
    analyzed: AnalyzedSegment,
    mut op_predicates: HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    is_top_level: bool,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
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
        let flattener::FlatOperatorKind::Join { correlation } = &op.kind;
        // The one operator kind: `let` here is irrefutable by construction.
        let (new_result, new_table_idx) = process_single_join(
            result,
            &analyzed,
            table_idx,
            op_idx,
            correlation,
            &mut op_predicates,
            danger_gates,
            identities,
        )?;
        result = new_result;
        table_idx = new_table_idx;
    }

    // Apply any top-level filters
    result = apply_top_level_filters(result, &mut op_predicates, identities)?;

    // THE REBUILD SAYS WHAT IT REPLACED. It stood over the tables it
    // flattened out of the operand and emitted them again, so it is the one
    // thing that knows which sources relate the two — and it states them
    // here rather than leaving a later reader to notice a resemblance.
    let over: Vec<_> = analyzed
        .tables
        .iter()
        .map(|table| table.stood_over())
        .collect();
    identities
        .authority()
        .replacing(analyzed.operand, &over, result)
}

/// Process a single join operator
pub(super) fn process_single_join(
    result: refined::Chain,
    analyzed: &AnalyzedSegment,
    table_idx: usize,
    op_idx: usize,
    stated: &resolved::MemberCorrelation,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    danger_gates: &crate::pipeline::danger_gates::DangerGateMap,
    identities: &crate::relation::Planning,
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
        build_correlation(stated, join_predicates, identities)?;

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
    )?;

    // Inner join: the leftovers filter the joined rows — WHERE placement
    // is exactly equivalent to ON for an inner join.
    let join_expr = if leftover_conditions.is_empty() {
        join_expr
    } else {
        join_expr.transparently(refined::Transparent::Restrict {
            condition: combine_predicates_with_and(leftover_conditions),
            origin: resolved::FilterOrigin::UserWritten,
        })
    };

    Ok((join_expr, new_table_idx))
}

/// Build the join's correlation from what the MEMBER STATED and what the
/// analyzer bucketed here.
///
/// Returns (join condition, leftovers). When a correspondence wins the join
/// slot, the conditions it displaces cannot ride the ON clause — they are
/// RETURNED, never dropped: the caller places them as WHERE (inner join) or
/// refuses (outer join, where placement changes match semantics).
///
/// NO PREDICATE IS RECLASSIFIED HERE. Every comparison arrives with the
/// equality class its construction gave it, and this function only chooses
/// where the SQL says it.
pub(super) fn build_correlation(
    stated: &resolved::MemberCorrelation,
    join_predicates: Vec<AnalyzedPredicate>,
    identities: &crate::relation::Planning,
) -> Result<(refined::MemberCorrelation, Vec<refined::TruthExpression>)> {
    let mut correlations = Vec::new();

    log::debug!("build_correlation: {} predicates", join_predicates.len());

    // The member's own condition is the first conjunct: it is the join's
    // correlation by construction, and it stands ahead of whatever the
    // analyzer bucketed alongside it.
    if let Some(condition) = stated.condition() {
        correlations.push(super::refine_predicate_boolean(
            condition.clone(),
            identities,
        )?);
    }

    for p in join_predicates {
        log::debug!("Processing predicate: {:?}", p.expr);
        match p.class {
            // BOTH CLASSES RIDE THE SAME CONDITION, and neither is touched
            // on the way. A single-table predicate bucketed at a join was
            // minted against the table this join brings in — an anonymous
            // header's ground constraint: for an inner join ON and WHERE
            // agree, and for an outer join ON is the pre-filter of the
            // introduced side, which is where a constraint on that side's own
            // cells belongs. Dropping it here returned every row the grid was
            // written to refuse.
            PredicateClass::FJC { .. } | PredicateClass::F { .. } => {
                correlations.push(super::refine_predicate_boolean(
                    p.expr.into_truth(),
                    identities,
                )?);
            }
            PredicateClass::Fx | PredicateClass::Forbidden { .. } => {}
        }
    }

    // The correspondence WINS the join slot. The conditions it displaces
    // are returned, never dropped: USING has no ON clause to carry them.
    // A pair with neither is the deliberate cross the resolver decided —
    // stated, so lowering holds a judgment and not an absence.
    Ok(match stated {
        resolved::MemberCorrelation::Correspond(correspondence) if !correspondence.is_empty() => {
            log::debug!("Join corresponds on: {:?}", correspondence.pairs);
            (
                refined::MemberCorrelation::Correspond(correspondence.clone()),
                correlations,
            )
        }
        _ if !correlations.is_empty() => (
            refined::MemberCorrelation::Condition(combine_predicates_with_and(correlations)),
            Vec::new(),
        ),
        _ => (refined::MemberCorrelation::Cartesian(()), Vec::new()),
    })
}

pub(super) fn create_join(
    left: refined::Chain,
    right: refined::Chain,
    correlation: refined::MemberCorrelation,
    join_type: Option<JoinType>,
    identities: &crate::relation::Planning,
) -> Result<refined::Chain> {
    let jt = join_type.unwrap_or(JoinType::Inner);
    // A CORRESPONDENCE MERGES POSITIONS, and the join it stands on is the
    // one the resolver already derived. Rebuilding with an empty merge list
    // and an inner kind would publish the right operand's shared columns a
    // second time, so the rebuilt relation would not stand where the
    // resolved one stood and every reference through it would move.
    let merged = match &correlation {
        refined::MemberCorrelation::Correspond(correspondence) => correspondence.pairs.clone(),
        refined::MemberCorrelation::Condition(_) | refined::MemberCorrelation::Cartesian(_) => {
            Vec::new()
        }
    };
    let right_relation = right.semantic_relation();
    // ONE DESCRIPTION: the variant says both what the step is and the law
    // its result comes from, and the left operand is the chain's own.
    identities.authority().extend(
        left,
        crate::relation::builder::StepOp::Join {
            rhs: right,
            correlation,
            join_type: Some(jt.clone()),
            right: right_relation,
            kind: match jt {
                JoinType::LeftOuter => crate::relation::form::JoinKind::LeftOuter,
                JoinType::RightOuter => crate::relation::form::JoinKind::RightOuter,
                JoinType::FullOuter => crate::relation::form::JoinKind::FullOuter,
                JoinType::Inner => crate::relation::form::JoinKind::Inner,
            },
            merged: &merged,
        },
    )
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
            analyzed.tables[0].relation
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
        let flattener::FlatOperatorKind::Join { correlation } = &analyzed.operators[join_idx].kind;
        // A DECIDED CARTESIAN IS THE ABSENCE. The member's correlation is
        // total, so anything else — a correspondence or a stated condition —
        // is a correlation this join already holds.
        let has_correlation = !matches!(correlation, resolved::MemberCorrelation::Cartesian(_))
            || op_predicates
                .get(&op_ref)
                .map(|preds| !preds.is_empty())
                .unwrap_or(false);

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
                    left_table.relation, right_table.relation,
                ),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod equality_position_tests {
    use super::*;
    use crate::pipeline::asts::core::{Comparison, FunctionApplication, LiteralValue};
    use crate::pipeline::asts::vocabulary::CmpOp;

    fn comparison(operator: CmpOp) -> resolved::TruthExpression {
        let null = || {
            resolved::DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Null))
        };
        resolved::TruthExpression::Comparison(Comparison {
            operator,
            left: Box::new(null()),
            right: Box::new(null()),
        })
    }

    fn cartesian() -> resolved::MemberCorrelation {
        resolved::MemberCorrelation::Cartesian(())
    }

    fn operators(correlation: refined::MemberCorrelation) -> Vec<CmpOp> {
        let refined::MemberCorrelation::Condition(condition) = correlation else {
            panic!("the predicates should have become the join condition")
        };
        let mut found = Vec::new();
        collect(&condition, &mut found);
        found
    }

    fn collect(condition: &refined::TruthExpression, found: &mut Vec<CmpOp>) {
        match condition {
            refined::TruthExpression::Comparison(Comparison { operator, .. }) => {
                found.push(*operator)
            }
            refined::TruthExpression::Conjunction(parts)
            | refined::TruthExpression::Disjunction(parts) => {
                for part in parts.iter() {
                    collect(part, found);
                }
            }
            refined::TruthExpression::Not { expr } => collect(expr, found),
            _ => panic!("the fixtures build comparisons and connectives only"),
        }
    }

    fn bucketed(
        class: PredicateClass,
        expr: resolved::TruthExpression,
        identities: &crate::relation::Planning,
    ) -> AnalyzedPredicate {
        AnalyzedPredicate {
            class,
            expr: crate::pipeline::refiner::settled::fixtures::settled_over_nothing(
                expr, identities,
            ),
            operator_ref: OperatorRef::Join { position: 0 },
            origin: resolved::FilterOrigin::UserWritten,
        }
    }

    /// SQL placement does not change a predicate's semantic class.
    ///
    /// A slot constraint on the optional operand has to ride that operand's
    /// `ON` clause so it filters before null extension. It nevertheless asks
    /// one row whether its cell is null; it is not correspondence between two
    /// relations. Reclassifying it merely because it is emitted in `ON`
    /// turns `rhs?(null, value)` into the unsatisfiable `rhs.k = NULL`.
    #[test]
    fn a_single_relation_constraint_stays_null_safe_when_placed_in_on() {
        let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let table = crate::relation::any_relation(&identities).scope();
        let predicate = bucketed(
            PredicateClass::F { table },
            comparison(CmpOp::NullSafeEqual),
            &identities,
        );

        let (correlation, leftovers) =
            build_correlation(&cartesian(), vec![predicate], &identities).unwrap();
        assert!(leftovers.is_empty());
        assert_eq!(
            operators(correlation),
            vec![CmpOp::NullSafeEqual],
            "a one-relation filter remains null-safe even when lowering places it in ON"
        );
    }

    /// A COMPLETE TREE IN `ON` IS NOT A TREE OF JOINS.
    ///
    /// One predicate can hold both a correspondence leaf and leaves that ask
    /// a single row about its own cells. The tree lands in `ON` whole, and
    /// each leaf keeps the class its construction gave it — the compound is
    /// the shape a per-tree rewrite got wrong, because it reached every leaf
    /// the moment any leaf related two operands.
    #[test]
    fn a_compound_condition_keeps_each_leaf_class_in_on() {
        let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let left = crate::relation::any_relation(&identities).scope();
        let right = crate::relation::any_relation(&identities).scope();
        let tree = resolved::TruthExpression::all(vec![
            comparison(CmpOp::Equal),
            resolved::TruthExpression::any(vec![
                comparison(CmpOp::NullSafeEqual),
                comparison(CmpOp::NullSafeEqual),
            ])
            .expect("two disjuncts"),
        ])
        .expect("two conjuncts");
        let predicate = bucketed(PredicateClass::FJC { left, right }, tree, &identities);

        let (correlation, leftovers) =
            build_correlation(&cartesian(), vec![predicate], &identities).unwrap();
        assert!(leftovers.is_empty());
        assert_eq!(
            operators(correlation),
            vec![CmpOp::Equal, CmpOp::NullSafeEqual, CmpOp::NullSafeEqual],
            "only the leaf its construction made a correspondence is one"
        );
    }

    /// THE MEMBER'S OWN CONDITION IS THE JOIN'S CORRELATION.
    ///
    /// It reaches lowering as the construction stated it, without passing
    /// through the classifier: a correlation whose ports the join's tables
    /// no longer publish has no references for a later reader to recover an
    /// owner from, and would be placed as a top-level filter — which for an
    /// outer join is a different query.
    #[test]
    fn a_stated_condition_is_the_join_correlation() {
        let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let stated = resolved::MemberCorrelation::Condition(comparison(CmpOp::Equal));

        let (correlation, leftovers) = build_correlation(&stated, Vec::new(), &identities).unwrap();
        assert!(leftovers.is_empty());
        assert_eq!(operators(correlation), vec![CmpOp::Equal]);
    }
}
