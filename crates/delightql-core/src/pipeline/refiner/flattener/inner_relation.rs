// inner_relation.rs - INNER-RELATION pattern handling and correlation filter hoisting

use super::context::FlattenContext;
use super::expression::add_predicate;
use super::rewrite::{
    collect_filter_qualifiers, could_be_inner_alias, rewrite_correlation_filter_with_scope,
    rewrite_subquery_self_references, rewrite_with_hygienic_names,
};
use super::types::{FlatSegment, FlatTable, OperationContext};
use crate::error::Result;
use crate::pipeline::asts::resolved::{self, CprSchema, InnerRelationPattern, PhaseBox};

/// Flatten an INNER-RELATION (correlated subquery)
pub(super) fn flatten_inner_relation(
    pattern: InnerRelationPattern<resolved::Resolved>,
    alias: Option<String>,
    outer: bool,
    cpr_schema: PhaseBox<CprSchema, resolved::Resolved>,
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

    let hygienic_injections = match &pattern {
        resolved::InnerRelationPattern::CorrelatedScalarJoin {
            hygienic_injections,
            ..
        }
        | resolved::InnerRelationPattern::CorrelatedGroupJoin {
            hygienic_injections,
            ..
        } => hygienic_injections.clone(),
        // UDT, Indeterminate, CorrelatedWindowJoin: no hygienic injections
        resolved::InnerRelationPattern::UncorrelatedDerivedTable { .. }
        | resolved::InnerRelationPattern::Indeterminate { .. }
        | resolved::InnerRelationPattern::CorrelatedWindowJoin { .. } => vec![],
    };

    match &pattern {
        resolved::InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery,
            ..
        }
        | resolved::InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations: _,
            subquery,
            ..
        }
        | resolved::InnerRelationPattern::CorrelatedWindowJoin {
            identifier,
            correlation_filters,
            order_by: _,
            limit: _,
            subquery,
        } => {
            // Determine the derived table's actual alias
            // If no explicit alias, use table name (schema shadowing)
            let derived_table_alias = alias.clone().unwrap_or_else(|| identifier.name.to_string());

            // PHASE 3: RECURSIVELY FLATTEN THE SUBQUERY
            // CRITICAL: Remove correlation filters from the subquery AST BEFORE flattening
            // The filters have been extracted by pattern_classifier but are still in the AST
            // We need to remove them so they don't get flattened into the child segment
            let cleaned_subquery = super::super::rebuilder::remove_correlation_filters_from_expr(
                subquery,
                correlation_filters,
            );

            // CDT-WJ SPECIFIC: Also remove LIMIT and ORDER BY from the subquery
            // These are converted to ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) + WHERE rn <= N
            // If we don't remove them, they'll be double-applied (once in subquery, once in window function)
            let cleaned_subquery = if matches!(
                pattern,
                resolved::InnerRelationPattern::CorrelatedWindowJoin { .. }
            ) {
                let cleaned = super::super::rebuilder::remove_limit_from_expr(&cleaned_subquery);
                super::super::rebuilder::remove_order_by_from_expr(&cleaned)
            } else {
                cleaned_subquery
            };

            // Rewrite self-reference qualifiers in non-correlation filters
            // e.g., `o.status = "completed"` → `orders.status = "completed"`
            let cleaned_subquery =
                rewrite_subquery_self_references(cleaned_subquery, &identifier.name);

            // INDUCTIVE STEP: Build scope map for recursion.
            // Start with inherited scope_aliases from parent depths, then add
            // self-reference aliases from THIS depth's correlation filters.
            let mut new_scope = ctx.scope_aliases.clone();
            for filter in correlation_filters.iter() {
                for q in collect_filter_qualifiers(filter) {
                    if could_be_inner_alias(&q, &identifier.name) {
                        new_scope.insert(q, identifier.name.to_string());
                    }
                }
            }
            // Also add the exact table name so child depths can resolve it
            new_scope.insert(identifier.name.to_string(), identifier.name.to_string());

            log::debug!(
                "[SCOPE-INDUCTIVE] table={}, parent_scope={:?}, new_scope={:?}, corr_filters={:?}",
                identifier.name,
                ctx.scope_aliases,
                new_scope,
                correlation_filters
                    .iter()
                    .map(|f| format!("{:?}", f))
                    .collect::<Vec<_>>()
            );

            // Flatten the cleaned subquery recursively WITH scope context
            let flattened_subquery = super::flatten_with_scope(cleaned_subquery, new_scope)?;

            // Extract correlation filters and add to PARENT segment predicates
            // This hoists them out of the subquery so they become JOIN ON clauses
            // IMPORTANT: Use scope-aware rewriting so ancestor aliases (depth N-2+)
            //            are resolved to their canonical table names.
            for filter in correlation_filters {
                let mut rewritten_filter = rewrite_correlation_filter_with_scope(
                    filter.clone(),
                    &identifier.name,
                    &derived_table_alias,
                    &ctx.scope_aliases,
                );

                // Apply hygienic column name rewrites if injections exist
                if !hygienic_injections.is_empty() {
                    rewritten_filter = rewrite_with_hygienic_names(
                        rewritten_filter,
                        &derived_table_alias,
                        &hygienic_injections,
                    );
                }

                add_predicate(
                    rewritten_filter,
                    resolved::FilterOrigin::UserWritten,
                    segment,
                    ctx,
                );
            }

            // Add the table with BOTH the pattern AND the flattened subquery
            // The pattern is kept for metadata, the flattened subquery is used by rebuilder
            segment.tables.push(FlatTable {
                identifier: identifier.clone(),
                canonical_name: None,
                alias: alias.clone(),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                domain_spec: resolved::DomainSpec::Glob,
                operation_context: OperationContext::Direct,
                schema: cpr_schema.get().clone(),
                outer,
                anonymous_data: None,
                correlation_refs: Vec::new(),
                inner_relation_pattern: Some(pattern.clone()),
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
            let (identifier, subquery_opt) = match &pattern {
                resolved::InnerRelationPattern::Indeterminate { identifier, .. } => {
                    (identifier.clone(), None)
                }
                resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                    identifier,
                    subquery,
                    ..
                } => (identifier.clone(), Some(subquery)),
                // These shouldn't reach here (handled above), but for completeness
                resolved::InnerRelationPattern::CorrelatedScalarJoin { identifier, .. }
                | resolved::InnerRelationPattern::CorrelatedGroupJoin { identifier, .. }
                | resolved::InnerRelationPattern::CorrelatedWindowJoin { identifier, .. } => {
                    (identifier.clone(), None)
                }
            };

            // Recursively flatten subquery if present, passing through inherited scope
            let flattened_subquery_opt = if let Some(subquery) = subquery_opt {
                Some(Box::new(super::flatten_with_scope(
                    (**subquery).clone(),
                    ctx.scope_aliases.clone(),
                )?))
            } else {
                None
            };

            segment.tables.push(FlatTable {
                identifier: identifier.clone(),
                canonical_name: None,
                alias: alias.clone(),
                position: ctx.position,
                _scope_id: ctx.scope_id,
                domain_spec: resolved::DomainSpec::Glob,
                operation_context: OperationContext::Direct,
                schema: cpr_schema.get().clone(),
                outer,
                anonymous_data: None,
                correlation_refs: Vec::new(),
                inner_relation_pattern: Some(pattern.clone()),
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
