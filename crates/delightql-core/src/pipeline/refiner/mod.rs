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

mod analyzer;
mod correlation_alias_fixer;
mod correlation_analyzer;
mod flattener;
mod laws;
mod pattern_classifier;
mod rebuilder;
mod types;

use crate::error::Result;
use crate::pipeline::asts::{refined, resolved};

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
    log::debug!(
        "refine: Called with AST type: {:?}, is_top_level={}",
        std::mem::discriminant(&ast),
        is_top_level
    );

    // STEP 0: Fix correlation aliases before refining
    // This ensures that correlated subqueries (ScalarSubquery, InnerExists) have the correct
    // table aliases inferred from their correlation predicates
    let ast = correlation_alias_fixer::fix_correlation_aliases(ast)?;

    // STEP 0.5: Classify INNER-RELATION patterns
    // This analyzes Indeterminate patterns and classifies them into UDT/CDT-SJ/CDT-GJ/CDT-WJ
    let ast = pattern_classifier::classify_patterns(ast)?;

    // Handle special cases at the top level
    match ast {
        // INNER-RELATION bypasses FAR cycle
        // The pattern has already been classified by pattern_classifier
        // The transformer will handle pattern-specific SQL generation
        resolved::RelationalExpression::Relation(resolved::Relation::InnerRelation {
            pattern,
            alias,
            outer,
            cpr_schema,
        }) => {
            // Simply convert to refined phase - FAR doesn't apply
            Ok(refined::RelationalExpression::Relation(
                refined::Relation::InnerRelation {
                    pattern: pattern.into(),
                    alias,
                    outer,
                    cpr_schema: cpr_schema.into_refined(),
                },
            ))
        }
        // CONSULTED-VIEW: view body goes through full refinement
        // The body is a full Query (may include CTEs) — refine_query handles all variants
        resolved::RelationalExpression::Relation(resolved::Relation::ConsultedView {
            identifier,
            body,
            scoped,
            outer,
        }) => {
            let refined_body = refine_query(*body)?;
            Ok(refined::RelationalExpression::Relation(
                refined::Relation::ConsultedView {
                    identifier,
                    body: Box::new(refined_body),
                    scoped: scoped.into_refined(),
                    outer,
                },
            ))
        }
        // Special handling for Filters with non-predicate conditions (e.g., TupleOrdinal for LIMIT)
        // These bypass FAR because:
        // 1. TupleOrdinal (LIMIT/OFFSET) operates on row counts, not predicates
        // 2. They can't be pushed down or rearranged - must stay at their exact position
        // 3. FAR is designed for predicate analysis, not row-level operations
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } if !matches!(condition, resolved::SigmaCondition::Predicate(_)) => {
            // Refine source, then wrap with the non-predicate filter
            // Pass through is_top_level to maintain context
            let refined_source = Box::new(refine_internal(*source, is_top_level)?);
            Ok(refined::RelationalExpression::Filter {
                source: refined_source,
                condition: condition.into(),
                origin,
                cpr_schema: cpr_schema.into_refined(),
            })
        }
        resolved::RelationalExpression::Pipe(_) => {
            let (base, segments) = crate::pipeline::pipe_chain::collect_pipe_chain(ast);
            let refined_base = refine_internal(base, is_top_level)?;
            let refined_segments = segments
                .into_iter()
                .map(|seg| crate::pipeline::pipe_chain::PipeSegment {
                    operator: seg.operator.into(),
                    cpr_schema: seg.cpr_schema.into_refined(),
                })
                .collect();
            Ok(crate::pipeline::pipe_chain::reconstruct_pipe_chain(
                refined_base,
                refined_segments,
            ))
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => {
            // ALL joins go through FAR cycle, regardless of pipes
            // This ensures GlobWithUsing and other features work uniformly
            refine_segment(
                resolved::RelationalExpression::Join {
                    left,
                    right,
                    join_condition,
                    join_type,
                    cpr_schema,
                },
                is_top_level,
            )
        }
        // Everything else (Relation::Ground, Relation::Anonymous, Relation::TVF,
        // Filter with predicate, SetOperation) goes through the FAR cycle.
        resolved::RelationalExpression::Relation(
            resolved::Relation::Ground { .. }
            | resolved::Relation::Anonymous { .. }
            | resolved::Relation::TVF { .. }
            | resolved::Relation::PseudoPredicate { .. },
        )
        | resolved::RelationalExpression::Filter { .. }
        | resolved::RelationalExpression::SetOperation { .. } => refine_segment(ast, is_top_level),
        // ER chains are consumed by resolver — should not reach refiner.
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before refinement")
        }
    }
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
    match query {
        resolved::Query::Relational(expr) => Ok(refined::Query::Relational(refine(expr)?)),
        resolved::Query::WithCtes { ctes, query } => {
            // Recursively refine each CTE's expression
            let refined_ctes = ctes
                .into_iter()
                .map(|cte| {
                    Ok(refined::CteBinding {
                        expression: refine(cte.expression)?,
                        name: cte.name,
                        is_recursive: refined::PhaseBox::phantom(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            // Refine the main query
            let refined_main = refine(query)?;

            Ok(refined::Query::WithCtes {
                ctes: refined_ctes,
                query: refined_main,
            })
        }
        resolved::Query::ReplTempTable { query, table_name } => {
            // For REPL temp tables, recursively refine the inner query
            Ok(refined::Query::ReplTempTable {
                query: Box::new(refine_query(*query)?),
                table_name,
            })
        }
        resolved::Query::ReplTempView { query, view_name } => {
            // For REPL temp views, recursively refine the inner query
            Ok(refined::Query::ReplTempView {
                query: Box::new(refine_query(*query)?),
                view_name,
            })
        }
        resolved::Query::WithCfes { .. } => Err(crate::error::DelightQLError::ParseError {
            message: "CFE queries must be precompiled before refining".to_string(),
            source: None,
            subcategory: None,
        }),
        resolved::Query::WithPrecompiledCfes { cfes, query } => {
            // CFE bodies are already refined - just pass them through and refine the main query
            let refined_inner = Box::new(refine_query(*query)?);

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
