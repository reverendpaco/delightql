// REFINER V2 - PRINCIPLED FAR (Flatten-Analyze-Rebuild) ARCHITECTURE
//
// Following PRINCIPLED-RELOOK-AT-REFINER.md with:
// - FJC/FIC/F/Fx/F! predicate classification
// - Laws 1-6 enforcement
// - PhaseBox for correlation (refined-phase only)
// - FAR (Flatten-Analyze-Rebuild) cycle
//
// EXISTS HANDLING:
// - The `+table` syntax creates an InnerExists predicate in the AST
// - InnerExists predicates flow through the FAR cycle as regular predicates
// - They're classified as F (filter) predicates since they filter the source
// - The transformer converts InnerExists to SQL EXISTS subqueries
// - NOT EXISTS uses `-table` syntax and becomes InnerExists with exists=false
//
// EPOCH 2: RefinerFold
//
// The refiner is now a single AstTransform<Resolved, Refined> implementation.
// Classification of InnerRelation patterns happens inline in transform_relation
// instead of a separate pre-pass. The walk handles operator descent by
// construction, fixing the classify_operator() no-op bug.

mod analyzer;
mod correlation_alias_fixer;
mod correlation_analyzer;
mod flattener;
mod laws;
mod pattern_classifier;
mod rebuilder;
mod types;

use crate::error::Result;
use crate::pipeline::ast_transform::{walk_transform_relation, AstTransform, FoldAction};
use crate::pipeline::asts::refined::Refined;
use crate::pipeline::asts::resolved::Resolved;
use crate::pipeline::asts::{refined, resolved};

// =============================================================================
// RefinerFold — AstTransform<Resolved, Refined>
// =============================================================================

struct RefinerFold {
    is_top_level: bool,
}

impl AstTransform<Resolved, Refined> for RefinerFold {
    // -------------------------------------------------------------------------
    // transform_relational_action — the routing hub
    // -------------------------------------------------------------------------
    //
    // Every arm returns FoldAction::Replaced because the refiner fully handles
    // each subtree: either via FAR (refine_segment), via explicit recursion
    // (pipes, filters), or via delegation to transform_relation (InnerRelation,
    // ConsultedView). The walk's default recursion is never used directly on
    // RelationalExpression nodes.
    fn transform_relational_action(
        &mut self,
        expr: resolved::RelationalExpression,
    ) -> Result<FoldAction<refined::RelationalExpression>> {
        log::debug!(
            "RefinerFold::transform_relational_action: {:?}, is_top_level={}",
            std::mem::discriminant(&expr),
            self.is_top_level
        );

        // Step 0: Fix correlation aliases before refining
        let expr = correlation_alias_fixer::fix_correlation_aliases(expr)?;

        match expr {
            // InnerRelation — delegate to transform_relation which handles
            // classification inline
            resolved::RelationalExpression::Relation(
                rel @ resolved::Relation::InnerRelation { .. },
            ) => {
                let refined = self.transform_relation(rel)?;
                Ok(FoldAction::Replaced(
                    refined::RelationalExpression::Relation(refined),
                ))
            }

            // ConsultedView — delegate to transform_relation
            resolved::RelationalExpression::Relation(
                rel @ resolved::Relation::ConsultedView { .. },
            ) => {
                let refined = self.transform_relation(rel)?;
                Ok(FoldAction::Replaced(
                    refined::RelationalExpression::Relation(refined),
                ))
            }

            // Non-predicate filter (TupleOrdinal for LIMIT) — refine source,
            // keep filter. Bypasses FAR because LIMIT operates on row counts.
            resolved::RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            } if !matches!(condition, resolved::SigmaCondition::Predicate(_)) => {
                let refined_source =
                    Box::new(self.transform_relational_action(*source)?.into_inner());
                Ok(FoldAction::Replaced(
                    refined::RelationalExpression::Filter {
                        source: refined_source,
                        condition: condition.into(),
                        origin,
                        cpr_schema: cpr_schema.into_refined(),
                    },
                ))
            }

            // Pipe — linearize, refine base, mechanical .into() on operators.
            // The operators' expressions (domain, boolean) are already resolved;
            // the walk's default transform_operator handles Resolved→Refined
            // rephase for all expression children inside operators.
            resolved::RelationalExpression::Pipe(_) => {
                let (base, segments) = crate::pipeline::pipe_chain::collect_pipe_chain(expr);
                let refined_base = self.transform_relational_action(base)?.into_inner();
                let refined_segments = segments
                    .into_iter()
                    .map(|seg| {
                        Ok(crate::pipeline::pipe_chain::PipeSegment {
                            operator: self.transform_operator(seg.operator)?,
                            cpr_schema: seg.cpr_schema.into_refined(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(FoldAction::Replaced(
                    crate::pipeline::pipe_chain::reconstruct_pipe_chain(
                        refined_base,
                        refined_segments,
                    ),
                ))
            }

            // Join, Filter(predicate), SetOperation, Ground, Anonymous, TVF,
            // PseudoPredicate → FAR cycle via refine_segment
            resolved::RelationalExpression::Join { .. }
            | resolved::RelationalExpression::Relation(
                resolved::Relation::Ground { .. }
                | resolved::Relation::Anonymous { .. }
                | resolved::Relation::TVF { .. }
                | resolved::Relation::PseudoPredicate { .. },
            )
            | resolved::RelationalExpression::Filter { .. }
            | resolved::RelationalExpression::SetOperation { .. } => {
                let refined = refine_segment(expr, self.is_top_level)?;
                Ok(FoldAction::Replaced(refined))
            }

            // ER chains consumed by resolver
            resolved::RelationalExpression::ErJoinChain { .. }
            | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
                unreachable!("ER chains should be resolved before refinement")
            }
        }
    }

    // -------------------------------------------------------------------------
    // transform_relation — inline classification of InnerRelation patterns
    // -------------------------------------------------------------------------
    //
    // This replaces the classify_patterns pre-pass. When encountering an
    // Indeterminate InnerRelation, we classify it on the resolved subquery
    // then convert to refined phase mechanically. The FAR cycle (via the
    // rebuilder) handles real subquery refinement later.
    //
    // For already-classified patterns, walk_transform_inner_relation handles
    // recursion into subqueries and correlation filters.
    fn transform_relation(&mut self, rel: resolved::Relation) -> Result<refined::Relation> {
        match rel {
            resolved::Relation::InnerRelation {
                pattern,
                alias,
                outer,
                cpr_schema,
            } => {
                let refined_pattern = match pattern {
                    resolved::InnerRelationPattern::Indeterminate {
                        identifier,
                        subquery,
                    } => {
                        // Recursively classify nested InnerRelation patterns
                        // in the subquery. This uses the fold's walk to descend
                        // into operators and ConsultedView bodies — fixing the
                        // classify_operator() no-op bug by construction.
                        let classified_subquery =
                            pattern_classifier::classify_patterns_via_fold(*subquery)?;

                        // Classify the outer pattern on the classified subquery.
                        let classified = pattern_classifier::classify_inner_relation_pattern(
                            identifier,
                            classified_subquery,
                        )?;

                        // Mechanical conversion to refined phase. The subquery
                        // will be refined later by the rebuilder (FAR cycle)
                        // when this InnerRelation is encountered in a segment.
                        classified.into()
                    }
                    already_classified => {
                        // Already classified — mechanical conversion. The
                        // subquery inside was already classified by the
                        // pre-pass or by a previous fold invocation.
                        already_classified.into()
                    }
                };

                Ok(refined::Relation::InnerRelation {
                    pattern: refined_pattern,
                    alias,
                    outer,
                    cpr_schema: cpr_schema.rephase(),
                })
            }

            resolved::Relation::ConsultedView {
                identifier,
                body,
                scoped,
                outer,
            } => {
                let refined_body = self.transform_query(*body)?;
                Ok(refined::Relation::ConsultedView {
                    identifier,
                    body: Box::new(refined_body),
                    scoped: scoped.rephase(),
                    outer,
                })
            }

            // Everything else: walk handles it
            other => walk_transform_relation(self, other),
        }
    }

    // -------------------------------------------------------------------------
    // transform_query — refine_query logic through the fold
    // -------------------------------------------------------------------------
    fn transform_query(&mut self, query: resolved::Query) -> Result<refined::Query> {
        match query {
            resolved::Query::Relational(expr) => {
                let mut fold = RefinerFold { is_top_level: true };
                Ok(refined::Query::Relational(
                    fold.transform_relational_action(expr)?.into_inner(),
                ))
            }
            resolved::Query::WithCtes { ctes, query } => {
                let refined_ctes = ctes
                    .into_iter()
                    .map(|cte| {
                        let mut fold = RefinerFold { is_top_level: true };
                        Ok(refined::CteBinding {
                            expression: fold
                                .transform_relational_action(cte.expression)?
                                .into_inner(),
                            name: cte.name,
                            is_recursive: refined::PhaseBox::phantom(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                let mut fold = RefinerFold { is_top_level: true };
                let refined_main = fold.transform_relational_action(query)?.into_inner();

                Ok(refined::Query::WithCtes {
                    ctes: refined_ctes,
                    query: refined_main,
                })
            }
            resolved::Query::ReplTempTable { query, table_name } => {
                Ok(refined::Query::ReplTempTable {
                    query: Box::new(self.transform_query(*query)?),
                    table_name,
                })
            }
            resolved::Query::ReplTempView { query, view_name } => {
                Ok(refined::Query::ReplTempView {
                    query: Box::new(self.transform_query(*query)?),
                    view_name,
                })
            }
            resolved::Query::WithCfes { .. } => Err(crate::error::DelightQLError::ParseError {
                message: "CFE queries must be precompiled before refining".to_string(),
                source: None,
                subcategory: None,
            }),
            resolved::Query::WithPrecompiledCfes { cfes, query } => {
                let refined_inner = Box::new(self.transform_query(*query)?);
                Ok(refined::Query::WithPrecompiledCfes {
                    cfes,
                    query: refined_inner,
                })
            }
            resolved::Query::WithErContext { .. } => {
                unreachable!("ER-context consumed by resolver")
            }
        }
    }
}

// =============================================================================
// Public entry points (unchanged API)
// =============================================================================

/// Main entry point for AST refinement (for RelationalExpression)
pub fn refine(ast: resolved::RelationalExpression) -> Result<refined::RelationalExpression> {
    refine_internal(ast, true)
}

/// Internal refine with context tracking
#[stacksafe::stacksafe]
pub(crate) fn refine_internal(
    ast: resolved::RelationalExpression,
    is_top_level: bool,
) -> Result<refined::RelationalExpression> {
    let mut fold = RefinerFold { is_top_level };
    fold.transform_relational_action(ast)
        .map(|a| a.into_inner())
}

/// Refine a single segment (no pipes) using the FAR cycle
fn refine_segment(
    ast: resolved::RelationalExpression,
    is_top_level: bool,
) -> Result<refined::RelationalExpression> {
    log::debug!(
        "refine_segment: Processing AST type: {:?}, is_top_level={}",
        std::mem::discriminant(&ast),
        is_top_level
    );

    // Phase 1: Flatten the AST into a flat segment
    let flat_segment = flattener::flatten(ast)?;
    log::debug!(
        "refine_segment: After flatten - {} tables, {} operators",
        flat_segment.tables.len(),
        flat_segment.operators.len()
    );

    // Phase 2: Analyze the segment and classify predicates
    let analyzed_segment = analyzer::analyze(flat_segment)?;
    log::debug!(
        "refine_segment: After analyze - segment_type={:?}, {} predicates",
        analyzed_segment.segment_type,
        analyzed_segment.predicates.len()
    );

    // Phase 3: Rebuild the AST with predicates in proper locations
    log::debug!("refine_segment: Calling rebuilder::rebuild");
    let refined_ast = rebuilder::rebuild_internal(analyzed_segment, is_top_level)?;

    Ok(refined_ast)
}

/// Refine a full Query (with CTEs)
pub fn refine_query(query: resolved::Query) -> Result<refined::Query> {
    let mut fold = RefinerFold { is_top_level: true };
    fold.transform_query(query)
}
