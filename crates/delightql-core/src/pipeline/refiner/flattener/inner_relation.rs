// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// inner_relation.rs - INNER-RELATION pattern handling and correlation filter hoisting

use super::context::FlattenContext;
use super::expression::add_predicate;
use super::rewrite::rewrite_with_hygienic_names;
use super::types::{FlatSegment, FlatTable};
use crate::error::Result;
use crate::pipeline::asts::resolved::{self, InnerRelationPattern};

/// Flatten an INNER-RELATION (correlated subquery)
pub(super) fn flatten_inner_relation(
    pattern: InnerRelationPattern<resolved::Resolved>,
    preminted_scope: Option<crate::names::ScopeId>,
    outer: bool,
    cpr_schema: crate::names::ScopeId,
    segment: &mut FlatSegment,
    ctx: &mut FlattenContext,
) -> Result<()> {
    // INNER-RELATION: Pattern-specific handling

    // For CDT-SJ and CDT-GJ: Extract correlation filters from inside subquery
    // and add them as segment predicates so FAR can handle them
    // Extract hygienic injections if present
    // PHASE 3 FIX: Re-classify Indeterminate patterns
    // During resolution, some patterns may be left as Indeterminate because
    // correlation detection couldn't run (e.g., unresolved qualifiers).
    // Now in the refiner, we can re-run pattern classification.
    //
    // NOTE: Only Indeterminate patterns are reclassified. UncorrelatedDerivedTable
    // patterns are trusted — the resolver creates them for view expansions where
    // the subquery is definitionally uncorrelated. Reclassifying UDT would cause
    // the correlation heuristic to misidentify internal join conditions (e.g.,
    // u.id = r.user_id) as correlation filters, hoisting them out of the subquery.
    let pattern = if matches!(
        pattern,
        resolved::InnerRelationPattern::Indeterminate { .. }
    ) {
        match pattern {
            resolved::InnerRelationPattern::Indeterminate {
                identifier,
                subquery,
                ..
            } => {
                // Re-run pattern classification
                // This fixes cases where pattern classification failed during resolution
                // because qualifiers weren't fully resolved yet
                super::super::pattern_classifier::classify_inner_relation_pattern(
                    identifier.clone(),
                    *subquery.clone(),
                    &ctx.identities,
                )?
            }
            other => panic!(
                "catch-all hit in flattener/inner_relation.rs (re-classify pattern): {:?}",
                other
            ),
        }
    } else {
        pattern.clone()
    };

    crate::probe::probe!(
        spec,
        "flatten_inner_relation: {:?} -> FlatTable with a hardcoded Glob",
        std::mem::discriminant(&pattern)
    );
    // What the subquery publishes for a stripped correlation column is a
    // question its own heading answers. Asking it here, rather than reading a
    // list the classifier attached, means the carriers a boundary republished
    // and the carriers a hoisted condition names cannot come apart.
    let carriers = match &pattern {
        resolved::InnerRelationPattern::CorrelatedScalarJoin { subquery, .. }
        | resolved::InnerRelationPattern::CorrelatedGroupJoin { subquery, .. } => {
            super::super::pattern_classifier::correlation_carriers(subquery, &ctx.identities)?
        }
        resolved::InnerRelationPattern::UncorrelatedDerivedTable { .. }
        | resolved::InnerRelationPattern::Indeterminate { .. } => vec![],
    };

    match &pattern {
        resolved::InnerRelationPattern::CorrelatedScalarJoin {
            identifier: _,
            correlation_filters,
            subquery,
            ..
        }
        | resolved::InnerRelationPattern::CorrelatedGroupJoin {
            identifier: _,
            correlation_filters,
            aggregations: _,
            subquery,
            ..
        } => {
            // PHASE 3: RECURSIVELY FLATTEN THE SUBQUERY
            // CRITICAL: Remove correlation filters from the subquery AST BEFORE flattening
            // The filters have been extracted by pattern_classifier but are still in the AST
            // We need to remove them so they don't get flattened into the child segment
            let cleaned_subquery = super::super::rebuilder::remove_correlation_filters_from_expr(
                subquery,
                correlation_filters,
            );

            let flattened_subquery =
                super::flatten(cleaned_subquery, std::rc::Rc::clone(&ctx.identities))?;

            // Extract correlation filters and add to PARENT segment predicates
            // This hoists them out of the subquery so they become JOIN ON clauses
            for filter in correlation_filters {
                let mut rewritten_filter = filter.clone();

                if !carriers.is_empty() {
                    rewritten_filter = rewrite_with_hygienic_names(rewritten_filter, &carriers)?;
                }

                add_predicate(
                    rewritten_filter,
                    resolved::FilterOrigin::UserWritten,
                    segment,
                    ctx,
                );
            }

            let identity = cpr_schema;
            // An injected carrier RIDES this table's boundary: the hoisted
            // filter above references the injection's occurrence, the
            // rebuilder's join republishes THIS scope's heading, and the
            // reference re-anchors along the republish chain — a carrier
            // present only inside the subquery leaves the hoisted condition
            // holding an occurrence no FROM entry publishes. The boundary
            // was registered before the injection existed, so it is
            // extended here, once (a reflatten finds the occurrence already
            // riding).
            for (_, carrier) in &carriers {
                let riding = ctx
                    .identities
                    .known_heading(identity)?
                    .iter()
                    .any(|column| ctx.identities.republishes(*column, *carrier));
                if !riding {
                    ctx.identities.republish_column(
                        *carrier,
                        identity,
                        crate::names::Republish::Correlation,
                        None,
                        crate::names::Addressing::Hygienic,
                        |_| {},
                    );
                }
            }
            // Add the table with BOTH the pattern AND the flattened subquery
            // The pattern is kept for metadata, the flattened subquery is used by rebuilder
            segment.tables.push(FlatTable {
                identity,
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: resolved::Access::All,
                schema: cpr_schema,
                outer,
                anonymous_data: None,
                inner_relation_pattern: Some(pattern.clone()),
                preminted_scope,
                subquery_segment: Some(Box::new(flattened_subquery)), // PHASE 3: Store flattened subquery
                pipe_expr: None,
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: None,
            });
            ctx.position += 1;
        }
        _ => {
            // UDT patterns are trusted as uncorrelated — the resolver creates them
            // for view expansions where the subquery is definitionally uncorrelated.
            // Do NOT re-run correlation detection here; the heuristic would misidentify
            // internal join conditions (e.g., u.id = r.user_id) as correlation filters
            // and hoist them out of the subquery, producing wrong results.

            // Default: UDT with no correlation, or Indeterminate
            let subquery_opt = match &pattern {
                resolved::InnerRelationPattern::Indeterminate { .. } => None,
                resolved::InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. } => {
                    Some(subquery)
                }
                // These shouldn't reach here (handled above), but for completeness
                resolved::InnerRelationPattern::CorrelatedScalarJoin { .. }
                | resolved::InnerRelationPattern::CorrelatedGroupJoin { .. } => None,
            };

            // Recursively flatten subquery if present, passing through inherited scope
            let flattened_subquery_opt = if let Some(subquery) = subquery_opt {
                Some(Box::new(super::flatten(
                    (**subquery).clone(),
                    std::rc::Rc::clone(&ctx.identities),
                )?))
            } else {
                None
            };

            segment.tables.push(FlatTable {
                identity: cpr_schema,
                position: ctx.position,
                _scope_id: ctx.scope_id,
                access: resolved::Access::All,
                schema: cpr_schema,
                outer,
                anonymous_data: None,
                inner_relation_pattern: Some(pattern.clone()),
                preminted_scope,
                subquery_segment: flattened_subquery_opt,
                pipe_expr: None,
                consulted_view_query: None,
                _table_filters: vec![],
                tvf_data: None,
            });
            ctx.position += 1;
        }
    }

    Ok(())
}
