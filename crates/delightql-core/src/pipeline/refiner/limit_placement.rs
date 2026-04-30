// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Limit Placement Pass (Phase 2 of LIMIT-PLACEMENT-PLAN)
//
// Resolved-phase rewrite that walks pipe/filter chains and inserts UDT
// subquery boundaries where structural sealing is required:
//
//   - Limit followed by aggregation (Modulo or AggregatePipe) — wrap the
//     limit-bearing prefix as InnerRelation::Indeterminate so the aggregation
//     operates on the limited subquery.
//   - Multiple limits in the same segment — fold to a single `Filter(#<min)`
//     since "smallest binds" matches DQL's pipeline semantics. Avoids the
//     SQL emission bug where only the last LIMIT survives.
//
// Wrapping uses InnerRelation::Indeterminate so the existing pattern
// classifier reclassifies as UDT during refinement (no correlation, no
// limit at the wrapped subquery's outer level since we strip it before
// wrapping is unnecessary — the limit stays inside the wrapped subquery).
//
// Set-op + limit handling lives in Phase 3, not here.

use crate::error::Result;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern as CoreInnerRelationPattern;
use crate::pipeline::asts::resolved::{
    self, CprSchema, FilterOrigin, ModuloSpec, NamespacePath, PhaseBox, PipeExpression,
    QualifiedName, Relation, RelationalExpression, Resolved, SigmaCondition, TupleOrdinalClause,
    TupleOrdinalOperator, UnaryRelationalOperator,
};
use delightql_types::SqlIdentifier;

type InnerRelationPattern = CoreInnerRelationPattern<Resolved>;

/// Apply the limit-placement transformation across an entire resolved AST.
///
/// Recursive: descends into joins, set-ops, and inner-relation subqueries
/// before transforming each level's pipe/filter chain.
#[stacksafe::stacksafe]
pub fn apply(expr: RelationalExpression) -> Result<RelationalExpression> {
    // Linearize this level's chain.
    let (base, steps) = linearize(expr);

    // Recurse into the base (joins, set-ops, inner-relation subqueries).
    let base = apply_to_base(base)?;

    // Recurse into operators that carry their own subqueries (scalar subqueries,
    // pivots, etc. — currently only handled within step expressions; pipe
    // operators don't typically carry relational subqueries directly).

    // Transform the chain.
    rewrite_chain(base, steps)
}

#[stacksafe::stacksafe]
fn apply_to_base(base: RelationalExpression) -> Result<RelationalExpression> {
    match base {
        RelationalExpression::Join {
            left,
            right,
            join_condition,
            join_type,
            cpr_schema,
        } => Ok(RelationalExpression::Join {
            left: Box::new(apply(*left)?),
            right: Box::new(apply(*right)?),
            join_condition,
            join_type,
            cpr_schema,
        }),
        RelationalExpression::SetOperation {
            operator,
            operands,
            correlation,
            cpr_schema,
        } => {
            // Phase 3 of LIMIT-PLACEMENT-PLAN: a set-op branch carrying a
            // top-level LIMIT must be sealed in a subquery — raw `LIMIT N
            // UNION ALL ...` is invalid SQL, and even where dialects accept
            // it, an unwrapped LIMIT on one side hoists to the outer set-op
            // level and fails to bind to its intended branch.
            let new_operands = operands
                .into_iter()
                .map(|op| {
                    let refined = apply(op)?;
                    Ok(if branch_has_top_level_limit(&refined) {
                        wrap_as_indeterminate(refined)
                    } else {
                        refined
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(RelationalExpression::SetOperation {
                operator,
                operands: new_operands,
                correlation,
                cpr_schema,
            })
        }
        RelationalExpression::Relation(rel) => Ok(RelationalExpression::Relation(apply_to_relation(rel)?)),
        // Pipe and Filter cannot appear at the base position (linearize consumes them).
        // ER chains and IntersectCorresponding never reach here in resolved phase.
        other => Ok(other),
    }
}

#[stacksafe::stacksafe]
fn apply_to_relation(rel: Relation) -> Result<Relation> {
    match rel {
        Relation::InnerRelation {
            pattern,
            alias,
            outer,
            cpr_schema,
        } => {
            let new_pattern = apply_to_pattern(pattern)?;
            Ok(Relation::InnerRelation {
                pattern: new_pattern,
                alias,
                outer,
                cpr_schema,
            })
        }
        // Other relation variants don't carry sub-expressions that need
        // limit-placement (Ground, Anonymous, TVF, ConsultedView body is
        // handled by the refiner's transform_query path which calls apply).
        other => Ok(other),
    }
}

#[stacksafe::stacksafe]
fn apply_to_pattern(
    pattern: InnerRelationPattern,
) -> Result<InnerRelationPattern> {
    use InnerRelationPattern as P;
    match pattern {
        P::Indeterminate {
            identifier,
            subquery,
        } => Ok(P::Indeterminate {
            identifier,
            subquery: Box::new(apply(*subquery)?),
        }),
        P::UncorrelatedDerivedTable {
            identifier,
            subquery,
            is_consulted_view,
        } => Ok(P::UncorrelatedDerivedTable {
            identifier,
            subquery: Box::new(apply(*subquery)?),
            is_consulted_view,
        }),
        P::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery,
            hygienic_injections,
        } => Ok(P::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery: Box::new(apply(*subquery)?),
            hygienic_injections,
        }),
        P::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery,
            hygienic_injections,
        } => Ok(P::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery: Box::new(apply(*subquery)?),
            hygienic_injections,
        }),
    }
}

// ============================================================================
// Chain step linearization
// ============================================================================

enum ChainStep {
    Pipe {
        operator: UnaryRelationalOperator,
        cpr_schema: PhaseBox<CprSchema, resolved::Resolved>,
    },
    Filter {
        condition: SigmaCondition,
        origin: FilterOrigin,
        cpr_schema: PhaseBox<CprSchema, resolved::Resolved>,
    },
}

#[stacksafe::stacksafe]
fn linearize(expr: RelationalExpression) -> (RelationalExpression, Vec<ChainStep>) {
    let mut steps_rev = Vec::new();
    let mut current = expr;
    loop {
        match current {
            RelationalExpression::Pipe(pipe) => {
                let pipe = (*pipe).into_inner();
                steps_rev.push(ChainStep::Pipe {
                    operator: pipe.operator,
                    cpr_schema: pipe.cpr_schema,
                });
                current = pipe.source;
            }
            RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            } => {
                steps_rev.push(ChainStep::Filter {
                    condition,
                    origin,
                    cpr_schema,
                });
                current = *source;
            }
            other => {
                steps_rev.reverse();
                return (other, steps_rev);
            }
        }
    }
}

fn reconstruct(base: RelationalExpression, steps: Vec<ChainStep>) -> RelationalExpression {
    steps.into_iter().fold(base, |source, step| match step {
        ChainStep::Pipe { operator, cpr_schema } => RelationalExpression::Pipe(Box::new(
            stacksafe::StackSafe::new(PipeExpression {
                source,
                operator,
                cpr_schema,
            }),
        )),
        ChainStep::Filter {
            condition,
            origin,
            cpr_schema,
        } => RelationalExpression::Filter {
            source: Box::new(source),
            condition,
            origin,
            cpr_schema,
        },
    })
}

// ============================================================================
// Step classification
// ============================================================================

/// Returns the limit value if this step is a `Filter(TupleOrdinal(LessThan, N))`.
fn limit_value(step: &ChainStep) -> Option<i64> {
    if let ChainStep::Filter {
        condition:
            SigmaCondition::TupleOrdinal(TupleOrdinalClause {
                operator: TupleOrdinalOperator::LessThan,
                value,
                offset: _,
            }),
        ..
    } = step
    {
        Some(*value)
    } else {
        None
    }
}

/// Steps that change the row-set in ways that demand the preceding limit
/// be sealed in a subquery: aggregations, grouping, distinct (Modulo).
fn is_row_collapsing_pipe(step: &ChainStep) -> bool {
    let ChainStep::Pipe { operator, .. } = step else {
        return false;
    };
    match operator {
        UnaryRelationalOperator::AggregatePipe { .. } => true,
        UnaryRelationalOperator::Modulo { spec, .. } => match spec {
            ModuloSpec::Columns(_) => true,
            ModuloSpec::GroupBy { reducing_on, .. } => !reducing_on.is_empty(),
        },
        _ => false,
    }
}

// ============================================================================
// Chain rewriting
// ============================================================================

#[stacksafe::stacksafe]
fn rewrite_chain(
    base: RelationalExpression,
    steps: Vec<ChainStep>,
) -> Result<RelationalExpression> {
    let mut current_base = base;
    let mut buffer: Vec<ChainStep> = Vec::new();

    for step in steps {
        if is_row_collapsing_pipe(&step) {
            // Row-collapsing operator. If the buffer has any limit, it must
            // emit inside a sealed subquery before we apply this operator.
            if buffer_has_limit(&buffer) {
                let buffer_now = std::mem::take(&mut buffer);
                let folded = fold_limits(buffer_now);
                let wrapped_inner = reconstruct(current_base, folded);
                current_base = wrap_as_indeterminate(wrapped_inner);
            }
            buffer.push(step);
        } else {
            buffer.push(step);
        }
    }

    // Final pass: fold any consecutive limits that survived in the trailing
    // buffer. Handles the multi-limit-composition case where no aggregation
    // ever appears.
    let folded_tail = fold_limits(buffer);

    Ok(reconstruct(current_base, folded_tail))
}

fn buffer_has_limit(buf: &[ChainStep]) -> bool {
    buf.iter().any(|s| limit_value(s).is_some())
}

/// Reduce all `Filter(TupleOrdinal)` entries in a step list to a single
/// trailing `Filter(TupleOrdinal(min(values)))`. Non-limit steps keep their
/// original positions (relative order preserved). The folded limit is
/// appended at the end of the chain — semantically equivalent to applying
/// the smallest limit to the chain's final output, which matches DQL's
/// "smallest binds" pipeline semantics.
fn fold_limits(steps: Vec<ChainStep>) -> Vec<ChainStep> {
    let mut min_limit: Option<(i64, FilterOrigin, PhaseBox<CprSchema, resolved::Resolved>)> = None;
    let mut non_limit_steps: Vec<ChainStep> = Vec::new();

    for step in steps {
        if let Some(value) = limit_value(&step) {
            // Capture origin/cpr_schema from the first limit we see; they're
            // interchangeable for purposes of the folded result.
            let ChainStep::Filter {
                origin, cpr_schema, ..
            } = step
            else {
                unreachable!("limit_value matched on a non-Filter step")
            };
            match min_limit.as_mut() {
                None => min_limit = Some((value, origin, cpr_schema)),
                Some((cur, _, _)) if value < *cur => *cur = value,
                _ => {}
            }
        } else {
            non_limit_steps.push(step);
        }
    }

    if let Some((value, origin, cpr_schema)) = min_limit {
        non_limit_steps.push(ChainStep::Filter {
            condition: SigmaCondition::TupleOrdinal(TupleOrdinalClause {
                operator: TupleOrdinalOperator::LessThan,
                value,
                offset: None,
            }),
            origin,
            cpr_schema,
        });
    }

    non_limit_steps
}

/// True if `expr` carries a `Filter(TupleOrdinal)` anywhere along its
/// top-level pipe/filter chain (without descending into joins, set-ops,
/// or inner-relation subqueries — those are sealed contexts).
fn branch_has_top_level_limit(expr: &RelationalExpression) -> bool {
    let mut current = expr;
    loop {
        match current {
            RelationalExpression::Filter {
                source, condition, ..
            } => {
                if matches!(condition, SigmaCondition::TupleOrdinal(_)) {
                    return true;
                }
                current = source;
            }
            RelationalExpression::Pipe(pipe) => {
                current = &pipe.source;
            }
            _ => return false,
        }
    }
}

/// Wrap an expression as a synthetic UDT-shaped InnerRelation. Uses the
/// `Indeterminate` pattern variant so the existing pattern classifier
/// reclassifies it (no correlation, no top-level limit ⇒ UDT).
fn wrap_as_indeterminate(inner: RelationalExpression) -> RelationalExpression {
    let cpr_schema = extract_cpr_box(&inner);
    let identifier = QualifiedName {
        namespace_path: NamespacePath::empty(),
        name: SqlIdentifier::from("__dql_limit_wrap"),
        grounding: None,
    };
    RelationalExpression::Relation(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier,
            subquery: Box::new(inner),
        },
        alias: None,
        outer: false,
        cpr_schema,
    })
}

fn extract_cpr_box(expr: &RelationalExpression) -> PhaseBox<CprSchema, resolved::Resolved> {
    match expr {
        RelationalExpression::Relation(rel) => match rel {
            Relation::Ground { cpr_schema, .. }
            | Relation::Anonymous { cpr_schema, .. }
            | Relation::TVF { cpr_schema, .. }
            | Relation::InnerRelation { cpr_schema, .. } => cpr_schema.clone(),
            Relation::ConsultedView { scoped, .. } => PhaseBox::new(scoped.get().schema().clone()),
            Relation::PseudoPredicate { .. } => PhaseBox::new(CprSchema::Unknown),
        },
        RelationalExpression::Filter { cpr_schema, .. }
        | RelationalExpression::Join { cpr_schema, .. }
        | RelationalExpression::SetOperation { cpr_schema, .. } => cpr_schema.clone(),
        RelationalExpression::Pipe(pipe) => pipe.cpr_schema.clone(),
        RelationalExpression::IntersectCorresponding { cpr_schema, .. } => cpr_schema.clone(),
        RelationalExpression::ErJoinChain { .. } | RelationalExpression::ErTransitiveJoin { .. } => {
            PhaseBox::new(CprSchema::Unknown)
        }
    }
}

