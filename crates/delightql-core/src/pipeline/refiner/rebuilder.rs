// rebuilder.rs - Phase 3 of FAR cycle: Rebuild AST with predicates in proper homes
//
// The rebuilder takes the analyzed segment and rebuilds it into a refined AST
// with predicates pushed down to their appropriate operators according to
// their classification.
//
// Submodules for organization
mod exists_handler;
mod join_builder;
mod schema_computation;
mod setop_builder;

use self::exists_handler::nest_interdependent_exists;
use self::join_builder::{create_join, process_single_join, rebuild_join_segment};
use self::schema_computation::{compute_filter_schema, compute_setop_schema};
use self::setop_builder::{extract_fic_correlation, rebuild_setop_segment};
use super::analyzer::AnalyzedSegment;
use super::flattener::FlatTable;
use super::types::*;
use crate::error::{DelightQLError, Result};
use crate::pipeline::asts::refined::{LiteralValue, PhaseBox, Refined, SetOperator};
use crate::pipeline::asts::resolved::{CprSchema, InnerRelationPattern, Resolved};
use crate::pipeline::asts::{refined, resolved};
use delightql_types::SqlIdentifier;
use std::collections::HashMap;

/// Strategy for handling set operation column alignment
enum SetOperatorStrategy {
    Correspondence,     // UC (;) - align by name
    SameColumnsReorder, // SUA (|;|) and SIC (|^|) - same columns, reorder
}

/// Main entry point - rebuild an analyzed segment into refined AST
pub(super) fn rebuild_internal(
    analyzed: AnalyzedSegment,
    is_top_level: bool,
) -> Result<refined::RelationalExpression> {
    log::debug!(
        "rebuild: segment_type={:?}, {} tables, {} operators, {} predicates, is_top_level={}",
        analyzed.segment_type,
        analyzed.tables.len(),
        analyzed.operators.len(),
        analyzed.predicates.len(),
        is_top_level
    );

    // Check for forbidden predicates first
    for pred in &analyzed.predicates {
        if let PredicateClass::Forbidden { reason } = &pred.class {
            return Err(DelightQLError::parse_error(format!(
                "Forbidden predicate: {:?} (reason: {:?})",
                pred.expr, reason
            )));
        }
    }

    // Group predicates by their operator association
    let mut op_predicates = group_predicates_by_operator(&analyzed.predicates);

    // Handle interdependent EXISTS predicates by nesting them
    nest_interdependent_exists(&mut op_predicates, &analyzed.exists_dependencies)?;

    // Build the expression tree based on segment type
    match analyzed.segment_type {
        SegmentType::Join => {
            log::debug!("Calling rebuild_join_segment");
            rebuild_join_segment(analyzed, op_predicates, is_top_level)
        }
        SegmentType::SetOperation => {
            log::debug!("Calling rebuild_setop_segment");
            rebuild_setop_segment(analyzed, op_predicates)
        }
        SegmentType::Mixed => {
            log::debug!("Calling rebuild_mixed_segment");
            rebuild_mixed_segment(analyzed, op_predicates, is_top_level)
        }
    }
}

/// Group predicates by which operator they modify
fn group_predicates_by_operator(
    predicates: &[AnalyzedPredicate],
) -> HashMap<OperatorRef, Vec<AnalyzedPredicate>> {
    let mut grouped = HashMap::new();

    for pred in predicates {
        grouped
            .entry(pred.operator_ref.clone())
            .or_insert_with(Vec::new)
            .push(pred.clone());
    }

    grouped
}

/// Apply top-level filters to the result
fn apply_top_level_filters(
    result: refined::RelationalExpression,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<refined::RelationalExpression> {
    apply_filter_predicates(result, op_predicates, OperatorRef::TopLevel)
}

/// Apply filter predicates for a given operator reference
fn apply_filter_predicates(
    mut result: refined::RelationalExpression,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    op_ref: OperatorRef,
) -> Result<refined::RelationalExpression> {
    if let Some(preds) = op_predicates.remove(&op_ref) {
        for pred in preds {
            match pred.class {
                PredicateClass::F { .. } | PredicateClass::Fx => {
                    result = wrap_with_filter(result, pred)?;
                }
                other => panic!(
                    "catch-all hit in rebuilder.rs apply_filter_predicates: {:?}",
                    other
                ),
            }
        }
    }
    Ok(result)
}

/// Wrap an expression with a filter
fn wrap_with_filter(
    source: refined::RelationalExpression,
    pred: AnalyzedPredicate,
) -> Result<refined::RelationalExpression> {
    Ok(refined::RelationalExpression::Filter {
        source: Box::new(source.clone()),
        condition: refined::SigmaCondition::Predicate(
            refine_predicate_boolean(pred.expr.clone())?,
        ),
        origin: pred.origin,
        cpr_schema: compute_filter_schema(&source),
    })
}

/// Rebuild a mixed segment (joins and set operations)
fn rebuild_mixed_segment(
    analyzed: AnalyzedSegment,
    mut op_predicates: HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
    is_top_level: bool,
) -> Result<refined::RelationalExpression> {
    // Mixed segments require careful left-to-right processing
    // respecting CPR-ltr semantics from PRINCIPLED-RELOOK-AT-REFINER.md

    if analyzed.tables.is_empty() {
        return Err(DelightQLError::parse_error("No tables in mixed segment"));
    }

    // TODO: Add validation for mixed segments if needed (is_top_level can be used here)
    let _ = is_top_level; // Suppress unused warning for now

    // Start with the first table
    let mut result = table_to_refined(&analyzed.tables[0], &mut op_predicates)?;
    let mut table_idx = 1;

    // Process operators left to right, building the expression incrementally
    for (op_idx, op) in analyzed.operators.iter().enumerate() {
        let (new_result, new_table_idx) =
            process_mixed_operator(result, &analyzed, table_idx, op_idx, op, &mut op_predicates)?;
        result = new_result;
        table_idx = new_table_idx;
    }

    // Apply any top-level filters
    result = apply_top_level_filters(result, &mut op_predicates)?;

    Ok(result)
}

/// Process a single operator in a mixed segment
fn process_mixed_operator(
    result: refined::RelationalExpression,
    analyzed: &AnalyzedSegment,
    table_idx: usize,
    op_idx: usize,
    op: &super::flattener::FlatOperator,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<(refined::RelationalExpression, usize)> {
    match &op.kind {
        super::flattener::FlatOperatorKind::Join { using_columns } => {
            // Reuse join processing logic
            process_single_join(
                result,
                analyzed,
                table_idx,
                op_idx,
                using_columns,
                op_predicates,
            )
        }

        super::flattener::FlatOperatorKind::SetOp { operator } => process_mixed_setop(
            result,
            analyzed,
            table_idx,
            op_idx,
            *operator,
            op_predicates,
        ),
    }
}

/// Process a set operation in a mixed segment
fn process_mixed_setop(
    result: refined::RelationalExpression,
    analyzed: &AnalyzedSegment,
    table_idx: usize,
    op_idx: usize,
    operator: SetOperator,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<(refined::RelationalExpression, usize)> {
    // Handle set operation - consume exactly one table as right operand
    if table_idx >= analyzed.tables.len() {
        return Err(DelightQLError::parse_error(
            "Not enough tables for set operation",
        ));
    }
    let right_operand = table_to_refined(&analyzed.tables[table_idx], op_predicates)?;
    let new_table_idx = table_idx + 1;

    // Get FIC predicates for correlation
    let op_ref = OperatorRef::SetOp {
        position: op_idx,
        operator,
    };
    let correlation = extract_fic_correlation(&op_ref, op_predicates);

    // Create set operation expression
    let operands = vec![result.clone(), right_operand.clone()];
    let setop_expr = refined::RelationalExpression::SetOperation {
        operands: operands.clone(),
        operator,
        correlation: <PhaseBox<Option<refined::BooleanExpression>, Refined>>::with_correlation(
            correlation,
        ),
        cpr_schema: compute_setop_schema(operator, &operands),
    };

    Ok((setop_expr, new_table_idx))
}

/// Convert a flat table to a refined relation
fn table_to_refined(
    table: &FlatTable,
    op_predicates: &mut HashMap<OperatorRef, Vec<AnalyzedPredicate>>,
) -> Result<refined::RelationalExpression> {
    // Check if this is a consulted view (stored as opaque resolved Query)
    if let Some(ref query) = table.consulted_view_query {
        let refined_body = crate::pipeline::refiner::refine_query(query.as_ref().clone())?;
        let alias: SqlIdentifier = table
            .alias
            .clone()
            .unwrap_or_else(|| table.identifier.name.to_string())
            .into();
        let scoped = refined::ScopedSchema::from_parts(alias, table.schema.clone());
        return Ok(refined::RelationalExpression::Relation(
            refined::Relation::ConsultedView {
                identifier: table.identifier.clone(),
                body: Box::new(refined_body),
                scoped: PhaseBox::new(scoped).into_refined(),
                outer: table.outer,
            },
        ));
    }

    // Check if this is a pipe expression
    if let Some(ref pipe_expr) = table.pipe_expr {
        // Recursively refine the pipe expression
        // Pass is_top_level=false to skip outer join validation (this is an inner context)
        return crate::pipeline::refiner::refine_internal(pipe_expr.as_ref().clone(), false);
    }

    let mut result = build_base_relation(table)?;

    let table_name = table.alias.as_deref().unwrap_or(&table.identifier.name);

    log::debug!("table_to_refined: Processing table '{}'", table_name);
    log::debug!(
        "Available operator refs: {:?}",
        op_predicates.keys().collect::<Vec<_>>()
    );

    for (op_ref, preds) in op_predicates.iter() {
        for pred in preds {
            log::debug!(
                "  Op {:?} has predicate: class={:?}, origin={:?}",
                op_ref,
                pred.class,
                pred.origin
            );
        }
    }

    let mut fic_filters_to_apply = Vec::new();

    for (_, preds) in op_predicates.iter() {
        for pred in preds {
            if let PredicateClass::FIC { left, right } = &pred.class {
                if left == table_name || right == table_name {
                    log::debug!(
                        "Table '{}' is mentioned in FIC predicate: {:?}",
                        table_name,
                        pred.expr
                    );
                    fic_filters_to_apply.push(pred.clone());
                }
            }
        }
    }

    for fic_pred in fic_filters_to_apply {
        log::debug!(
            "Applying FIC predicate to table '{}': {:?}",
            table_name,
            fic_pred.expr
        );
    }

    let mut filters_to_apply = Vec::new();

    for (op_ref, preds) in op_predicates.iter_mut() {
        let mut remaining = Vec::new();
        for pred in preds.drain(..) {
            if let PredicateClass::F { table: target } = &pred.class {
                if target == table_name {
                    if let resolved::FilterOrigin::PositionalLiteral { source_table } = &pred.origin
                    {
                        log::debug!("      Checking PositionalLiteral: source_table='{}', table_name='{}', op_ref={:?}",
                                   source_table, table_name, op_ref);
                        log::debug!(
                            "      Applying PositionalLiteral filter to table '{}'",
                            table_name
                        );
                        filters_to_apply.push(pred);
                        continue;
                    }
                }
            }
            remaining.push(pred);
        }
        *preds = remaining;
    }

    for filter_pred in filters_to_apply {
        result = wrap_with_filter(result, filter_pred)?;
    }

    Ok(result)
}

/// Build the base relation from a flat table
fn build_base_relation(table: &FlatTable) -> Result<refined::RelationalExpression> {
    let schema_box = PhaseBox::new(table.schema.clone()).into_refined();

    if let Some(ref tvf_data) = table.tvf_data {
        return Ok(build_tvf_relation(tvf_data, &table.alias, schema_box));
    }

    if let Some(ref anon_data) = table.anonymous_data {
        return Ok(build_anonymous_relation(
            anon_data,
            &table.alias,
            table.outer,
            schema_box,
        ));
    }

    if let Some(ref inner_pattern) = table.inner_relation_pattern {
        // PHASE 5: Use flattened subquery if available (recursive flattening)
        if let Some(ref subquery_segment) = table.subquery_segment {
            return build_inner_relation_from_flattened(
                inner_pattern,
                subquery_segment,
                &table.alias,
                table.outer,
                schema_box,
            );
        } else {
            // Fallback: Old behavior (re-process AST)
            return build_inner_relation(inner_pattern, &table.alias, table.outer, schema_box);
        }
    }

    Ok(build_ground_relation(table, schema_box))
}

/// Build a TVF relation
fn build_tvf_relation(
    tvf_data: &super::flattener::TvfData,
    alias: &Option<String>,
    schema_box: PhaseBox<CprSchema, refined::Refined>,
) -> refined::RelationalExpression {
    refined::RelationalExpression::Relation(refined::Relation::TVF {
        function: tvf_data.function.clone().into(),
        arguments: tvf_data.arguments.clone(),
        domain_spec: tvf_data.domain_spec.clone().into(),
        alias: alias.clone().map(|s| s.into()),
        namespace: tvf_data.namespace.clone(),
        grounding: tvf_data.grounding.clone(),
        cpr_schema: schema_box,
        argument_groups: None,
        first_parens_spec: None,
    })
}

/// Build an anonymous table relation
fn build_anonymous_relation(
    anon_data: &super::flattener::AnonymousTableData,
    alias: &Option<String>,
    outer: bool,
    schema_box: PhaseBox<CprSchema, refined::Refined>,
) -> refined::RelationalExpression {
    refined::RelationalExpression::Relation(refined::Relation::Anonymous {
        column_headers: anon_data
            .column_headers
            .as_ref()
            .map(|headers| headers.iter().map(|e| e.clone().into()).collect()),
        rows: anon_data.rows.iter().map(|r| r.clone().into()).collect(),
        alias: alias.clone().map(|s| s.into()),
        outer,
        exists_mode: anon_data.exists_mode, // EPOCH 3: Preserve EXISTS mode flag
        qua_target: None,
        cpr_schema: schema_box,
    })
}

/// Build an INNER-RELATION
fn build_inner_relation(
    pattern: &InnerRelationPattern<Resolved>,
    alias: &Option<String>,
    outer: bool,
    schema_box: PhaseBox<CprSchema, refined::Refined>,
) -> Result<refined::RelationalExpression> {
    // For CDT-SJ and CDT-GJ: Remove correlation filters from subquery since they've been hoisted to JOIN ON
    let cleaned_pattern = match pattern {
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery,
            hygienic_injections,
        } => {
            // Remove the correlation filters from inside the subquery
            let cleaned_subquery =
                remove_correlation_filters_from_expr(subquery, correlation_filters);

            // Hygienic injections were already done by pattern_classifier
            // Just preserve them through the phase conversion
            InnerRelationPattern::CorrelatedScalarJoin {
                identifier: identifier.clone(),
                correlation_filters: correlation_filters.clone(),
                subquery: Box::new(cleaned_subquery),
                hygienic_injections: hygienic_injections.clone(),
            }
        }
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery,
            hygienic_injections,
        } => {
            // For CDT-GJ: Remove correlation filters from subquery, just like CDT-SJ!
            //
            // Discovery: User must explicitly include correlation column in modulo operator:
            //   orders(, orders.user_id = users.id |> %(user_id ~> count:(*)))
            //                                         ^^^^^^^^ explicit GROUP BY
            //
            // The correlation filter gets hoisted to JOIN ON (just like CDT-SJ)
            // The GROUP BY is already explicit in the modulo operator
            // No need to keep correlation filters inside the subquery!
            let cleaned_subquery =
                remove_correlation_filters_from_expr(subquery, correlation_filters);

            // Hygienic injections were already done by pattern_classifier
            InnerRelationPattern::CorrelatedGroupJoin {
                identifier: identifier.clone(),
                correlation_filters: correlation_filters.clone(),
                aggregations: aggregations.clone(),
                subquery: Box::new(cleaned_subquery),
                hygienic_injections: hygienic_injections.clone(),
            }
        }
        InnerRelationPattern::CorrelatedWindowJoin {
            identifier,
            correlation_filters,
            order_by,
            limit,
            subquery,
        } => {
            // For CDT-WJ: Remove correlation filters, LIMIT, and ORDER BY from subquery
            //
            // - Correlation filters get hoisted to JOIN ON
            // - LIMIT is converted to WHERE rn <= N (via ROW_NUMBER window function)
            // - ORDER BY is converted to ORDER BY inside ROW_NUMBER() OVER (... ORDER BY ...)
            //
            // All three must be stripped from the subquery to avoid double-application!
            let cleaned_subquery =
                remove_correlation_filters_from_expr(subquery, correlation_filters);
            let cleaned_subquery = remove_limit_from_expr(&cleaned_subquery);
            let cleaned_subquery = remove_order_by_from_expr(&cleaned_subquery);

            InnerRelationPattern::CorrelatedWindowJoin {
                identifier: identifier.clone(),
                correlation_filters: correlation_filters.clone(),
                order_by: order_by.clone(),
                limit: *limit,
                subquery: Box::new(cleaned_subquery),
            }
        }
        other => panic!(
            "catch-all hit in rebuilder.rs build_inner_relation (pattern clean): {:?}",
            other
        ),
    };

    // Convert pattern from Resolved to Refined phase
    let refined_pattern: InnerRelationPattern<Refined> = cleaned_pattern.into();

    Ok(refined::RelationalExpression::Relation(
        refined::Relation::InnerRelation {
            pattern: refined_pattern,
            alias: alias.clone().map(|s| s.into()),
            outer,
            cpr_schema: schema_box,
        },
    ))
}

/// Build INNER-RELATION from flattened subquery segment (PHASE 5: Recursive FAR)
/// This is the new code path that uses the pre-flattened subquery instead of re-processing AST
fn build_inner_relation_from_flattened(
    pattern: &InnerRelationPattern<Resolved>,
    subquery_segment: &super::flattener::FlatSegment,
    alias: &Option<String>,
    outer: bool,
    schema_box: PhaseBox<CprSchema, refined::Refined>,
) -> Result<refined::RelationalExpression> {
    // The subquery segment has already been flattened
    // Correlation filters have already been hoisted
    // We need to: analyze it, then rebuild it

    // Analyze the flattened subquery segment
    let analyzed_subquery = super::analyzer::analyze(subquery_segment.clone())?;

    // Recursively rebuild the analyzed segment into a Refined AST
    // Pass is_top_level=false to skip outer join validation (this is an inner context)
    let rebuilt_subquery = rebuild_internal(analyzed_subquery, false)?;

    // Convert pattern from Resolved to Refined, replacing the subquery with the rebuilt one
    let refined_pattern: InnerRelationPattern<Refined> = match pattern {
        InnerRelationPattern::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            hygienic_injections,
            ..
        } => InnerRelationPattern::CorrelatedScalarJoin {
            identifier: identifier.clone(),
            correlation_filters: correlation_filters
                .iter()
                .map(|f| f.clone().into())
                .collect(),
            subquery: Box::new(rebuilt_subquery),
            hygienic_injections: hygienic_injections.clone(),
        },
        InnerRelationPattern::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            hygienic_injections,
            ..
        } => InnerRelationPattern::CorrelatedGroupJoin {
            identifier: identifier.clone(),
            correlation_filters: correlation_filters
                .iter()
                .map(|f| f.clone().into())
                .collect(),
            aggregations: aggregations.iter().map(|a| a.clone().into()).collect(),
            subquery: Box::new(rebuilt_subquery),
            hygienic_injections: hygienic_injections.clone(),
        },
        InnerRelationPattern::CorrelatedWindowJoin {
            identifier,
            correlation_filters,
            order_by,
            limit,
            ..
        } => InnerRelationPattern::CorrelatedWindowJoin {
            identifier: identifier.clone(),
            correlation_filters: correlation_filters
                .iter()
                .map(|f| f.clone().into())
                .collect(),
            order_by: order_by.iter().map(|o| o.clone().into()).collect(),
            limit: *limit,
            subquery: Box::new(rebuilt_subquery),
        },
        InnerRelationPattern::UncorrelatedDerivedTable {
            identifier,
            is_consulted_view,
            ..
        } => InnerRelationPattern::UncorrelatedDerivedTable {
            identifier: identifier.clone(),
            subquery: Box::new(rebuilt_subquery),
            is_consulted_view: *is_consulted_view,
        },
        InnerRelationPattern::Indeterminate { identifier, .. } => {
            InnerRelationPattern::Indeterminate {
                identifier: identifier.clone(),
                subquery: Box::new(rebuilt_subquery),
            }
        }
    };

    Ok(refined::RelationalExpression::Relation(
        refined::Relation::InnerRelation {
            pattern: refined_pattern,
            alias: alias.clone().map(|s| s.into()),
            outer,
            cpr_schema: schema_box,
        },
    ))
}
/// Remove correlation filters from a relational expression
/// Public wrapper for use by flattener when recursively flattening INNER-RELATIONs
pub fn remove_correlation_filters_from_expr(
    expr: &resolved::RelationalExpression,
    filters_to_remove: &[resolved::BooleanExpression],
) -> resolved::RelationalExpression {
    match expr {
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            // Check if this filter is one of the correlation filters to remove
            if let resolved::SigmaCondition::Predicate(pred) = condition {
                if filters_to_remove.contains(pred) {
                    // Skip this filter - it's been hoisted to JOIN ON
                    return remove_correlation_filters_from_expr(source, filters_to_remove);
                }
            }

            // Keep this filter, but recursively clean the source
            resolved::RelationalExpression::Filter {
                source: Box::new(remove_correlation_filters_from_expr(
                    source,
                    filters_to_remove,
                )),
                condition: condition.clone(),
                origin: origin.clone(),
                cpr_schema: cpr_schema.clone(),
            }
        }
        resolved::RelationalExpression::Pipe(pipe_expr) => {
            // Recursively clean the source
            resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                resolved::PipeExpression {
                    source: remove_correlation_filters_from_expr(
                        &pipe_expr.source,
                        filters_to_remove,
                    ),
                    operator: pipe_expr.operator.clone(),
                    cpr_schema: pipe_expr.cpr_schema.clone(),
                },
            )))
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => {
            // Recursively clean both sides of the join
            resolved::RelationalExpression::Join {
                left: Box::new(remove_correlation_filters_from_expr(
                    left,
                    filters_to_remove,
                )),
                right: Box::new(remove_correlation_filters_from_expr(
                    right,
                    filters_to_remove,
                )),
                join_condition: join_condition.clone(),
                join_type: join_type.clone(),
                cpr_schema: cpr_schema.clone(),
            }
        }
        resolved::RelationalExpression::Relation(rel) => {
            // Handle nested INNER-RELATIONs - correlation filters from outer relations
            // might be inside nested INNER-RELATION subqueries
            match rel {
                resolved::Relation::InnerRelation {
                    pattern,
                    alias,
                    outer,
                    cpr_schema,
                } => {
                    // Recursively clean the subquery in the pattern
                    let cleaned_pattern = match pattern {
                        resolved::InnerRelationPattern::CorrelatedScalarJoin {
                            identifier,
                            correlation_filters,
                            subquery,
                            hygienic_injections,
                        } => resolved::InnerRelationPattern::CorrelatedScalarJoin {
                            identifier: identifier.clone(),
                            correlation_filters: correlation_filters.clone(),
                            subquery: Box::new(remove_correlation_filters_from_expr(
                                subquery,
                                filters_to_remove,
                            )),
                            hygienic_injections: hygienic_injections.clone(),
                        },
                        resolved::InnerRelationPattern::CorrelatedGroupJoin {
                            identifier,
                            correlation_filters,
                            aggregations,
                            subquery,
                            hygienic_injections,
                        } => resolved::InnerRelationPattern::CorrelatedGroupJoin {
                            identifier: identifier.clone(),
                            correlation_filters: correlation_filters.clone(),
                            aggregations: aggregations.clone(),
                            subquery: Box::new(remove_correlation_filters_from_expr(
                                subquery,
                                filters_to_remove,
                            )),
                            hygienic_injections: hygienic_injections.clone(),
                        },
                        resolved::InnerRelationPattern::CorrelatedWindowJoin {
                            identifier,
                            correlation_filters,
                            order_by,
                            limit,
                            subquery,
                        } => resolved::InnerRelationPattern::CorrelatedWindowJoin {
                            identifier: identifier.clone(),
                            correlation_filters: correlation_filters.clone(),
                            order_by: order_by.clone(),
                            limit: *limit,
                            subquery: Box::new(remove_correlation_filters_from_expr(
                                subquery,
                                filters_to_remove,
                            )),
                        },
                        resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                            identifier,
                            subquery,
                            is_consulted_view,
                        } => resolved::InnerRelationPattern::UncorrelatedDerivedTable {
                            identifier: identifier.clone(),
                            subquery: Box::new(remove_correlation_filters_from_expr(
                                subquery,
                                filters_to_remove,
                            )),
                            is_consulted_view: *is_consulted_view,
                        },
                        // Indeterminate: pass through (no subquery with filters to clean)
                        resolved::InnerRelationPattern::Indeterminate { .. } => pattern.clone(),
                    };

                    resolved::RelationalExpression::Relation(resolved::Relation::InnerRelation {
                        pattern: cleaned_pattern,
                        alias: alias.clone(),
                        outer: *outer,
                        cpr_schema: cpr_schema.clone(),
                    })
                }
                // Other relation types: no subqueries with correlation filters to clean
                other => resolved::RelationalExpression::Relation(other.clone()),
            }
        }
        // SetOperation: recurse into operands
        resolved::RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => resolved::RelationalExpression::SetOperation {
            operator: operator.clone(),
            operands: operands
                .iter()
                .map(|op| remove_correlation_filters_from_expr(op, filters_to_remove))
                .collect(),
            correlation: correlation.clone(),
            cpr_schema: cpr_schema.clone(),
        },
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains consumed before correlation filter removal")
        }
    }
}

/// Remove LIMIT (TupleOrdinal) filters from a relational expression
/// Used by CDT-WJ to strip the LIMIT clause since it's converted to ROW_NUMBER() + WHERE rn <= N
pub fn remove_limit_from_expr(
    expr: &resolved::RelationalExpression,
) -> resolved::RelationalExpression {
    match expr {
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            // Check if this is a TupleOrdinal (LIMIT) filter
            if matches!(condition, resolved::SigmaCondition::TupleOrdinal(_)) {
                // Skip this filter - it's been converted to ROW_NUMBER() window function
                return remove_limit_from_expr(source);
            }

            // Keep this filter, but recursively clean the source
            resolved::RelationalExpression::Filter {
                source: Box::new(remove_limit_from_expr(source)),
                condition: condition.clone(),
                origin: origin.clone(),
                cpr_schema: cpr_schema.clone(),
            }
        }
        resolved::RelationalExpression::Pipe(pipe_expr) => {
            // Recursively clean the source
            resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                resolved::PipeExpression {
                    source: remove_limit_from_expr(&pipe_expr.source),
                    operator: pipe_expr.operator.clone(),
                    cpr_schema: pipe_expr.cpr_schema.clone(),
                },
            )))
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => resolved::RelationalExpression::Join {
            left: Box::new(remove_limit_from_expr(left)),
            right: Box::new(remove_limit_from_expr(right)),
            join_condition: join_condition.clone(),
            join_type: join_type.clone(),
            cpr_schema: cpr_schema.clone(),
        },
        // Leaf nodes: no LIMIT to strip. Return unchanged.
        // Relation variants (Ground, Anonymous, TVF, InnerRelation, ConsultedView, PseudoPredicate)
        // and SetOperation are terminal — they don't contain inner ORDER BY to strip.
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::SetOperation { .. } => expr.clone(),
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before CDT-WJ processing")
        }
    }
}

/// Remove ORDER BY (TupleOrdering) pipes from a relational expression
/// Used by CDT-WJ to strip the ORDER BY clause since it's converted to ROW_NUMBER() OVER (... ORDER BY ...)
pub fn remove_order_by_from_expr(
    expr: &resolved::RelationalExpression,
) -> resolved::RelationalExpression {
    match expr {
        resolved::RelationalExpression::Pipe(pipe_expr) => {
            // Check if this is a TupleOrdering (ORDER BY) pipe
            if matches!(
                pipe_expr.operator,
                resolved::UnaryRelationalOperator::TupleOrdering { .. }
            ) {
                // Skip this pipe - it's been converted to ORDER BY inside ROW_NUMBER() window function
                return remove_order_by_from_expr(&pipe_expr.source);
            }

            // Keep this pipe, but recursively clean the source
            resolved::RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                resolved::PipeExpression {
                    source: remove_order_by_from_expr(&pipe_expr.source),
                    operator: pipe_expr.operator.clone(),
                    cpr_schema: pipe_expr.cpr_schema.clone(),
                },
            )))
        }
        resolved::RelationalExpression::Filter {
            source,
            condition,
            origin,
            cpr_schema,
        } => {
            // Recursively clean the source
            resolved::RelationalExpression::Filter {
                source: Box::new(remove_order_by_from_expr(source)),
                condition: condition.clone(),
                origin: origin.clone(),
                cpr_schema: cpr_schema.clone(),
            }
        }
        resolved::RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => resolved::RelationalExpression::Join {
            left: Box::new(remove_order_by_from_expr(left)),
            right: Box::new(remove_order_by_from_expr(right)),
            join_condition: join_condition.clone(),
            join_type: join_type.clone(),
            cpr_schema: cpr_schema.clone(),
        },
        // Leaf nodes: no ORDER BY to strip. Return unchanged.
        resolved::RelationalExpression::Relation(_)
        | resolved::RelationalExpression::SetOperation { .. } => expr.clone(),
        resolved::RelationalExpression::ErJoinChain { .. }
        | resolved::RelationalExpression::ErTransitiveJoin { .. } => {
            unreachable!("ER chains should be resolved before CDT-WJ processing")
        }
    }
}

fn build_ground_relation(
    table: &FlatTable,
    schema_box: PhaseBox<CprSchema, refined::Refined>,
) -> refined::RelationalExpression {
    let domain_spec = match &table.domain_spec {
        // GlobWithUsing/GlobWithUsingAll: USING columns already extracted into join
        // predicates by analyzer. Revert to plain Glob for SQL generation.
        resolved::DomainSpec::GlobWithUsing(_) | resolved::DomainSpec::GlobWithUsingAll => {
            resolved::DomainSpec::Glob
        }
        // Glob/Positional/Bare: pass through unchanged.
        // Positional must survive — transformer uses it to generate column renames.
        resolved::DomainSpec::Glob => resolved::DomainSpec::Glob,
        resolved::DomainSpec::Positional(exprs) => resolved::DomainSpec::Positional(exprs.clone()),
        resolved::DomainSpec::Bare => resolved::DomainSpec::Bare,
    };

    refined::RelationalExpression::Relation(refined::Relation::Ground {
        identifier: table.identifier.clone(),
        canonical_name: PhaseBox::new(table.canonical_name.clone()).into_refined(),
        domain_spec: domain_spec.into(),
        alias: table.alias.clone().map(|s| s.into()),
        outer: table.outer,
        mutation_target: false,
        passthrough: false,
        cpr_schema: schema_box,
        hygienic_injections: Vec::new(),
    })
}

fn combine_predicates_with_and(
    predicates: Vec<refined::BooleanExpression>,
) -> refined::BooleanExpression {
    if predicates.is_empty() {
        create_true_literal()
    } else if predicates.len() == 1 {
        predicates.into_iter().next().unwrap()
    } else {
        predicates
            .into_iter()
            .reduce(|acc, pred| refined::BooleanExpression::And {
                left: Box::new(acc),
                right: Box::new(pred),
            })
            .unwrap()
    }
}

fn create_true_literal() -> refined::BooleanExpression {
    refined::BooleanExpression::Comparison {
        operator: "=".to_string(),
        left: Box::new(refined::DomainExpression::Literal {
            value: LiteralValue::Number("1".to_string()),
            alias: None,
        }),
        right: Box::new(refined::DomainExpression::Literal {
            value: LiteralValue::Number("1".to_string()),
            alias: None,
        }),
    }
}

/// Convert a resolved boolean expression to refined, refining InnerExists/InRelational
/// subqueries through the full refiner pipeline. Without this, InnerRelation patterns
/// inside InnerExists stay as Indeterminate and the transformer can't handle them.
pub(super) fn refine_predicate_boolean(
    expr: resolved::BooleanExpression,
) -> Result<refined::BooleanExpression> {
    match expr {
        resolved::BooleanExpression::InnerExists {
            exists,
            identifier,
            subquery,
            alias,
            using_columns,
        } => {
            // Refine the InnerExists subquery through the full refiner pipeline
            let refined_subquery =
                crate::pipeline::refiner::refine_internal(*subquery, false)?;
            Ok(refined::BooleanExpression::InnerExists {
                exists,
                identifier,
                subquery: Box::new(refined_subquery),
                alias,
                using_columns,
            })
        }
        resolved::BooleanExpression::InRelational {
            value,
            subquery,
            identifier,
            negated,
        } => {
            let refined_subquery =
                crate::pipeline::refiner::refine_internal(*subquery, false)?;
            Ok(refined::BooleanExpression::InRelational {
                value: Box::new((*value).into()),
                subquery: Box::new(refined_subquery),
                identifier,
                negated,
            })
        }
        resolved::BooleanExpression::And { left, right } => {
            Ok(refined::BooleanExpression::And {
                left: Box::new(refine_predicate_boolean(*left)?),
                right: Box::new(refine_predicate_boolean(*right)?),
            })
        }
        resolved::BooleanExpression::Or { left, right } => {
            Ok(refined::BooleanExpression::Or {
                left: Box::new(refine_predicate_boolean(*left)?),
                right: Box::new(refine_predicate_boolean(*right)?),
            })
        }
        resolved::BooleanExpression::Not { expr: inner } => {
            Ok(refined::BooleanExpression::Not {
                expr: Box::new(refine_predicate_boolean(*inner)?),
            })
        }
        // All other variants: mechanical phase conversion
        other => Ok(other.into()),
    }
}

fn collect_columns_from_schema(schema: &CprSchema, columns: &mut indexmap::IndexSet<String>) {
    if let CprSchema::Resolved(cols) = schema {
        for col in cols {
            columns.insert(col.name().to_string());
        }
    }
}
