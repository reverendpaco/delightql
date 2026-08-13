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
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved;
use crate::pipeline::asts::resolved::{InnerRelationPattern, Resolved};
use std::rc::Rc;

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
    identities: &'a Rc<crate::names::Registry>,
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
    identities: &Rc<crate::names::Registry>,
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
    identities: &Rc<crate::names::Registry>,
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
    expr.source_spine().any(|step| {
        matches!(
            step,
            SpineStep::Pipe(resolved::PipeOp::Group(_))
        )
    })
}

fn extract_aggregations(_expr: &resolved::Chain) -> Result<Vec<resolved::DomainExpression>> {
    // TODO: Extract aggregation expressions from GroupBy/WholeTableAggregation operators
    Ok(Vec::new())
}

// ============================================================================
// Helper Functions - Limit/Order By Detection
// ============================================================================

/// Does the top-level shaping run hold a `#<N` bound?
///
/// Rides [`Chain::source_spine`]: each restriction's condition is inspected
/// for a `TupleOrdinal LessThan`; a bound inside a member's chain, a bag arm,
/// or a subquery is not top-level, so the walk stops at the continuation that
/// brings that relation in. Pinned by
/// `source_spine_reads_restrictions_and_pipes_outermost_first` and
/// `source_spine_stops_at_a_member_without_entering_either_relation`.
fn has_limit(expr: &resolved::Chain) -> bool {
    use crate::pipeline::asts::core::expressions::chain::SpineStep;
    expr.source_spine().any(|step| {
        matches!(
            step,
            SpineStep::Bound(resolved::TupleOrdinalClause {
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
pub(super) fn inject_hygienic_columns_if_needed(
    subquery: resolved::Chain,
    correlation_filters: &[resolved::TruthExpression],
    identities: &Rc<crate::names::Registry>,
) -> Result<resolved::Chain> {
    use crate::pipeline::asts::resolved;

    let inner_scope = relational_scope(&subquery)?;
    let correlation_columns = correlation_analyzer::extract_correlation_columns(
        correlation_filters,
        inner_scope,
        identities,
    );

    if correlation_columns.is_empty() {
        return Ok(subquery);
    }

    // Check if subquery ends with a projection
    // Only a trailing explicit projection can drop a correlation column;
    // anything else preserves every column, so nothing needs injecting.
    let needs_injection = matches!(
        subquery.continuations.last(),
        Some(resolved::Continuation::Pipe {
            operator: resolved::PipeOp::Project {
                ..
            },
            ..
        })
    );

    if !needs_injection {
        // Subquery doesn't end with projection - all columns preserved (map-cover or glob)
        return Ok(subquery);
    }

    // Extract the projection expressions
    if let Some(resolved::Continuation::Pipe {
        operator: pipe_operator,
        named: (),
        cpr_schema: pipe_cpr_schema,
    }) = subquery.continuations.last().cloned()
    {
        if let resolved::PipeOp::Project(items)
        | resolved::PipeOp::Embed(items) = &pipe_operator
        {
            // Check which correlation columns are missing from projection.
            // The occurrence an item READS is what containment asks about,
            // not the one it publishes.
            fn projected_column(item: &resolved::OutItem) -> Option<crate::names::ColId> {
                match item.domain_value() {
                    Some(resolved::DomainExpression::Reference(Reference::Named(
                        NamedReference(ColumnOccurrence { column, .. }),
                    ))) => Some(*column),
                    _ => None,
                }
            }
            let projected_columns: std::collections::HashSet<crate::names::ColId> =
                items.iter().filter_map(projected_column).collect();

            let mut injected = 0usize;
            let mut new_items = items.clone().into_vec();

            for source in correlation_columns {
                // Already-projected is a CHAIN question, not a ColId one:
                // the projection references a downstream occurrence of the
                // access column the correlation names, and injecting beside
                // it mints a second carrier of the same value — which later
                // makes a by-value re-anchor genuinely ambiguous.
                let projected = projected_columns
                    .iter()
                    .any(|column| identities.republishes(*column, source));
                if !projected {
                    // The carrier IS the record: it is hygienic, it lives in
                    // the inner scope, and its origin names what it stands
                    // for. A list kept beside the tree said the same three
                    // things a second time, and a boundary republishing one
                    // without the other made them disagree.
                    let output = identities.republish_column(
                        source,
                        inner_scope,
                        crate::names::Republish::Correlation,
                        None,
                        crate::names::Addressing::Hygienic,
                        |_| {},
                    );

                    // A carrier the refiner injects publishes the occurrence
                    // it just minted and answers to no authored name.
                    new_items.push(resolved::OutItem::One(resolved::OneOut {
                        expr: OutValue::Domain(resolved::DomainExpression::Reference(
                            Reference::Named(NamedReference(ColumnOccurrence {
                                column: output,
                                explicit_qualifier: false,
                            })),
                        )),
                        naming: None,
                        output: Some(output),
                    }));

                    injected += 1;
                }
            }

            if injected == 0 {
                // All correlation columns already present
                return Ok(subquery);
            }

            // Rebuild the trailing projection with the injected columns.
            let mut rebuilt = subquery.clone();
            rebuilt.continuations.pop();
            return Ok(rebuilt.then(resolved::Continuation::Pipe {
                operator: resolved::PipeOp::Project(
                    crate::pipeline::asts::vocabulary::Vec1::try_from_vec(new_items)
                        .expect("injection extends a nonempty projection"),
                ),
                named: (),
                cpr_schema: pipe_cpr_schema,
            }));
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
    identities: &Rc<crate::names::Registry>,
) -> Result<Vec<(crate::names::ColId, crate::names::ColId)>> {
    crate::pipeline::transformer::builder::correlation_carriers(
        relational_scope(subquery)?,
        identities,
    )
}

pub(super) fn relational_scope(expr: &resolved::Chain) -> Result<crate::names::ScopeId> {
    match expr.continuations.last() {
        Some(continuation) => Ok(*continuation
            .cpr_schema()
            .expect("ER chains should be resolved before pattern classification")),
        None => match &expr.head {
            resolved::Grelex::Literal(anon) => Ok(anon.table.cpr_schema),
            resolved::Grelex::Reference(relation) => match relation {
                resolved::Relation::Ground { cpr_schema, .. }
                | resolved::Relation::InnerRelation { cpr_schema, .. }
                | resolved::Relation::FunctorCall { cpr_schema, .. } => Ok(*cpr_schema),
                resolved::Relation::ConsultedView { scoped, .. } => Ok(*scoped),
            },
        },
    }
}
