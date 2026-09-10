// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// inner_relation.rs - INNER-RELATION pattern handling and correlation hoisting

use super::context::FlattenContext;
use super::expression::add_predicate;
use super::types::{FlatSegment, FlatTable};
use crate::diagnostic::Internal;
use crate::error::Result;
use crate::pipeline::asts::resolved::{self, InnerRelationPattern};

/// Flatten an INNER-RELATION (a derived table, correlated or not).
///
/// THE CORRELATIONS ARE THE PATTERN'S. Classification took the typed
/// correlated steps out of the subquery and holds their conditions on the
/// pattern; the flattener hoists them onto the enclosing segment exactly as
/// written, where the analyzer places them on the join that brings the
/// derived table in. The interior occurrences a hoisted condition names are
/// readable at the boundary BY CONSTRUCTION — every relation from the
/// correlation act to the boundary carried them — so the condition is
/// neither renamed nor searched for a carrier, and the boundary answers for
/// those occurrences when the analyzer asks which table owns them.
pub(super) fn flatten_inner_relation(
    pattern: InnerRelationPattern<resolved::Resolved>,
    head: resolved::Grelex,
    outer: bool,
    result: crate::relation::SemanticRelation,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    let (subquery, correlation_filters): (resolved::Chain, &[resolved::TruthExpression]) =
        match &pattern {
            InnerRelationPattern::CorrelatedScalarJoin {
                subquery,
                correlation_filters,
                ..
            }
            | InnerRelationPattern::CorrelatedGroupJoin {
                subquery,
                correlation_filters,
                ..
            } => ((**subquery).clone(), correlation_filters.as_slice()),
            InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. } => {
                ((**subquery).clone(), &[])
            }
            InnerRelationPattern::Indeterminate { .. } => {
                return Err(Internal::invariant(
                    "refiner::flattener",
                    "an unclassified interior reached the flattener: every inner relation is \
                     classified before the segment standing over it flattens",
                ));
            }
        };

    // The subquery is flattened for the nested FAR cycle; the hoisted
    // conditions join the PARENT segment's predicates so they become the
    // enclosing join's own.
    let operand = subquery.semantic_relation();
    let flattened_subquery = super::flatten(subquery, operand, ctx.identities)?;
    for filter in correlation_filters {
        add_predicate(
            filter.clone(),
            resolved::FilterOrigin::UserWritten,
            segment,
            ctx,
        );
    }

    // The head travels with the table: the rebuilder crosses it into the
    // refined phase, keeping what it publishes, and rebuilds the subquery
    // from the flattened segment beside it.
    segment.tables.push(FlatTable {
        relation: result,
        head: Some(head),
        position: ctx.position,
        _scope_id: ctx.scope_id,
        access: resolved::Access::All,
        outer,
        anonymous_data: None,
        narrowed: None,
        subquery_segment: Some(Box::new(flattened_subquery)),
        pipe_expr: None,
        _table_filters: vec![],
        tvf_data: None,
    });
    ctx.position += 1;

    Ok(())
}
