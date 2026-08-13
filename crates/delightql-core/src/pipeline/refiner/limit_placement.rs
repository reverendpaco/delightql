// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Limit Placement Pass
//
// Resolved-phase rewrite that walks pipe/filter chains and inserts UDT
// subquery boundaries where structural sealing is required:
//
//   - Limit followed by a step that must observe the bounded row set —
//     a predicate, an ordering, an aggregation (a Group step),
//     or a destructure — wrap the limit-bearing prefix as
//     InnerRelation::Indeterminate so the later operation sees only the
//     limited rows.
//   - Multiple limits in the same segment — fold to a single `Filter(#<min)`
//     since "smallest binds" matches DQL's pipeline semantics. Avoids the
//     SQL emission bug where only the last LIMIT survives.
//
// Wrapping uses InnerRelation::Indeterminate so the existing pattern
// classifier reclassifies as UDT during refinement (no correlation, no
// limit at the wrapped subquery's outer level since we strip it before
// wrapping is unnecessary — the limit stays inside the wrapped subquery).
//
// Set-op + limit handling lives in the set-op branch pass, not here.

use crate::error::Result;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern as CoreInnerRelationPattern;
use crate::pipeline::asts::resolved::{
    Chain, Continuation, FilterOrigin, Grelex, GroupSpec, NamespacePath, PipeOp, QualifiedName,
    Relation, Resolved, TruthExpression, TupleOrdinalClause, TupleOrdinalOperator,
};
use delightql_types::SqlIdentifier;

type InnerRelationPattern = CoreInnerRelationPattern<Resolved>;

/// Apply the limit-placement transformation across an entire resolved AST.
///
/// Recursive: descends into joins, set-ops, and inner-relation subqueries
/// before transforming each level's pipe/filter chain.
#[stacksafe::stacksafe]
pub fn apply(expr: Chain) -> Result<Chain> {
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
fn apply_to_base(base: Chain) -> Result<Chain> {
    let mut base = base;
    match base.pop_step() {
        Some(Continuation::Member {
            rhs,
            correlation,
            join_type,
            cpr_schema,
        }) => Ok(apply(base)?.then(Continuation::Member {
            rhs: apply(rhs)?,
            correlation,
            join_type,
            cpr_schema,
        })),
        Some(Continuation::BagOp {
            operator,
            arm,
            correlation,
            cpr_schema,
        }) => {
            // A set-op branch carrying a
            // top-level LIMIT must be sealed in a subquery — raw `LIMIT N
            // UNION ALL ...` is invalid SQL, and even where dialects accept
            // it, an unwrapped LIMIT on one side hoists to the outer set-op
            // level and fails to bind to its intended branch.
            let seal = |arm: Chain| -> Result<Chain> {
                let refined = apply(arm)?;
                Ok(if branch_has_top_level_limit(&refined) {
                    wrap_as_indeterminate(refined)
                } else {
                    refined
                })
            };
            let left = seal(base)?;
            let arm = seal(arm)?;
            Ok(left.bag_op(operator, arm, correlation, cpr_schema))
        }
        Some(other) => {
            // No pipe, restriction, bound, or destructure can appear at the
            // base position (linearize consumes them all); an ER edge never
            // reaches the resolved phase.
            base.continuations.push(other);
            Ok(base)
        }
        // The base is the head and the access its own read consumes; the
        // access rides back untouched on the relation it asked of.
        None => {
            let read = std::mem::take(&mut base.continuations);
            let mut rebuilt = match base.head {
                Grelex::Reference(rel) => Chain::relation(apply_to_relation(rel)?),
                head => Chain::ground(head),
            };
            rebuilt.continuations = read;
            Ok(rebuilt)
        }
    }
}

#[stacksafe::stacksafe]
fn apply_to_relation(rel: Relation) -> Result<Relation> {
    match rel {
        Relation::InnerRelation {
            pattern,
            preminted_scope,
            alias,
            outer,
            cpr_schema,
        } => {
            let new_pattern = apply_to_pattern(pattern)?;
            Ok(Relation::InnerRelation {
                pattern: new_pattern,
                preminted_scope,
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
fn apply_to_pattern(pattern: InnerRelationPattern) -> Result<InnerRelationPattern> {
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
        } => Ok(P::CorrelatedScalarJoin {
            identifier,
            correlation_filters,
            subquery: Box::new(apply(*subquery)?),
        }),
        P::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery,
        } => Ok(P::CorrelatedGroupJoin {
            identifier,
            correlation_filters,
            aggregations,
            subquery: Box::new(apply(*subquery)?),
        }),
    }
}

// ============================================================================
// Chain step linearization
// ============================================================================

enum ChainStep {
    Access {
        access: crate::pipeline::asts::resolved::Access,
        cpr_schema: crate::names::ScopeId,
    },
    Pipe {
        operator: PipeOp,
        cpr_schema: crate::names::ScopeId,
    },
    Filter {
        condition: TruthExpression,
        origin: FilterOrigin,
        cpr_schema: crate::names::ScopeId,
    },
    Bound {
        bound: TupleOrdinalClause,
        cpr_schema: crate::names::ScopeId,
    },
    /// Carried whole: this pass never reads a destructure's pattern, and the
    /// step it hands back must be the step it took. It is linearized because
    /// a step left at the base position stops the descent, and the chain
    /// below a destructure would then never have its limits placed.
    Destructure { step: Continuation },
    /// A structural continuation carried whole: ordering, reposition, meta,
    /// the witnesses, drill, narrowing. The step handed back is the step
    /// taken.
    Structural { step: Continuation },
}

/// A continuation this pass linearizes, or the base boundary handed back.
///
/// ONE CLASSIFICATION ACT. Membership is this match's own answer, so there is
/// no second list of admitted names beside it to disagree with it.
fn classify(continuation: Continuation) -> std::result::Result<ChainStep, Continuation> {
    match continuation {
        Continuation::Access { access, cpr_schema } => Ok(ChainStep::Access { access, cpr_schema }),
        // `named: ()` rather than `..`: this pass MOVES steps, and a mover
        // that quietly leaves a field behind hands back a different step.
        // The unit type is the proof there is no authored name here to
        // leave behind.
        Continuation::Pipe {
            operator,
            named: (),
            cpr_schema,
        } => Ok(ChainStep::Pipe {
            operator,
            cpr_schema,
        }),
        Continuation::Restrict {
            condition,
            origin,
            cpr_schema,
        } => Ok(ChainStep::Filter {
            condition,
            origin,
            cpr_schema,
        }),
        Continuation::Bound { bound, cpr_schema } => Ok(ChainStep::Bound { bound, cpr_schema }),
        // A correlation stands on a bag run, and a bag run is opaque to
        // this pass: handing it back stops the descent where it should.
        step @ Continuation::Correlate { .. } => Err(step),
        step @ Continuation::Destructure { .. } => Ok(ChainStep::Destructure { step }),
        step @ Continuation::Structural(_) => Ok(ChainStep::Structural { step }),
        // A member and a set-op arm carry their own chains; `apply_to_base`
        // descends into those rather than moving them.
        boundary @ (Continuation::Member { .. } | Continuation::BagOp { .. }) => Err(boundary),
    }
}

#[stacksafe::stacksafe]
fn linearize(expr: Chain) -> (Chain, Vec<ChainStep>) {
    let mut current = expr;
    let mut steps_rev = Vec::new();
    // THE HEAD'S OWN ACCESS IS NOT A STEP. `pop_step` stops at the read the
    // chain stands on, so no boundary this pass inserts can land between a
    // mention and what its parens asked for.
    while let Some(continuation) = current.pop_step() {
        match classify(continuation) {
            Ok(step) => steps_rev.push(step),
            Err(boundary) => {
                current.continuations.push(boundary);
                break;
            }
        }
    }
    steps_rev.reverse();
    (current, steps_rev)
}

fn reconstruct(base: Chain, steps: Vec<ChainStep>) -> Chain {
    steps.into_iter().fold(base, |source, step| match step {
        ChainStep::Access { access, cpr_schema } => {
            source.then(Continuation::Access { access, cpr_schema })
        }
        ChainStep::Pipe {
            operator,
            cpr_schema,
        } => source.then(Continuation::Pipe {
            operator: operator,
            named: (),
            cpr_schema: cpr_schema,
        }),
        ChainStep::Filter {
            condition,
            origin,
            cpr_schema,
        } => source.then(Continuation::Restrict {
            condition,
            origin,
            cpr_schema,
        }),
        ChainStep::Bound { bound, cpr_schema } => {
            source.then(Continuation::Bound { bound, cpr_schema })
        }
        ChainStep::Destructure { step } => source.then(step),
        ChainStep::Structural { step } => source.then(step),
    })
}

// ============================================================================
// Step classification
// ============================================================================

/// Returns the limit value if this step is a `#<n` bound.
///
/// `#>n` is an OFFSET, which selects no maximum, so only the less-than
/// spelling answers here.
fn limit_value(step: &ChainStep) -> Option<i64> {
    if let ChainStep::Bound {
        bound:
            TupleOrdinalClause {
                operator: TupleOrdinalOperator::LessThan,
                value,
                offset: _,
            },
        ..
    } = step
    {
        Some(*value)
    } else {
        None
    }
}

/// Steps that must observe an already-bounded row set rather than move
/// inside its bound.
///
/// Exhaustive over `ChainStep`, because the answer is a property of the step's
/// KIND: a predicate drops rows, an ordering re-decides which rows a bound
/// keeps, an aggregation collapses them, and an aggregate destructure
/// multiplies them. Only a further bound composes with a buffered one, and
/// `fold_limits` — which appends the survivor to the END of the segment it is
/// given — is what composes it. That append is why a step answering `true`
/// here also fixes the authored position of everything buffered below it.
fn seals_preceding_bound(step: &ChainStep) -> bool {
    match step {
        ChainStep::Bound { .. } | ChainStep::Access { .. } => false,
        ChainStep::Filter { .. } | ChainStep::Destructure { .. } => true,
        ChainStep::Pipe { operator, .. } => match operator {
            PipeOp::Group(spec) => match spec {
                GroupSpec::Distinct { .. } => true,
                // A delegate selects rather than collapses; only the other
                // members are the aggregation this pass seals under.
                GroupSpec::Reduce { reductions, .. } => reductions.iter().any(|item| {
                    !matches!(
                        item,
                        crate::pipeline::asts::core::ReductionItem::Delegate(_)
                    )
                }),
            },
            _ => false,
        },
        // An ordering re-decides which rows a bound keeps; the other
        // structural forms neither drop, reorder, nor collapse rows this
        // pass's bounds could observe differently.
        ChainStep::Structural { step } => matches!(
            step,
            Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::Ordering { .. },
                ..
            })
        ),
    }
}

// ============================================================================
// Chain rewriting
// ============================================================================

#[stacksafe::stacksafe]
fn rewrite_chain(base: Chain, steps: Vec<ChainStep>) -> Result<Chain> {
    let mut current_base = base;
    let mut buffer: Vec<ChainStep> = Vec::new();

    for step in steps {
        if seals_preceding_bound(&step) {
            if buffer_has_bound(&buffer) {
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

/// A BOUND SEALS WHAT PRECEDES IT, either spelling.
///
/// `#>a` denotes a relation exactly as `#<m` does, so a step that must
/// observe an already-bounded row set must observe a skipped one too — an
/// offset left pending across a predicate or an ordering would be applied
/// to rows those steps had already chosen.
fn buffer_has_bound(buf: &[ChainStep]) -> bool {
    buf.iter()
        .any(|s| limit_value(s).is_some() || offset_value(s).is_some())
}

/// Reduce all `#<n` bounds in a step list to a single `#<min(values)`. Non-limit steps keep their original
/// positions (relative order preserved). Callers partition the chain at
/// cardinality-sensitive boundaries, so the folded limit may be appended to
/// the end of this row-preserving segment.
///
/// A chain ending in a DML terminal is the exception, and it keeps the limit
/// where the author put it. There is no "final output" to append to: the
/// terminal consumes the rows, and the transformer finds it by reading the
/// chain's LAST step, so a limit appended past it reads as a mid-chain
/// operator after a mutation. Landing it just before the terminal is equally
/// wrong — an `update!` reads its assignments from the outermost select, so a
/// limit inserted between the `$$` cover and the terminal hides them behind a
/// passthrough layer. Every step between an authored limit and the terminal
/// is row-preserving (a row-collapsing one seals the prefix above), so the
/// authored slot needs no adjusting.
fn fold_limits(steps: Vec<ChainStep>) -> Vec<ChainStep> {
    // AN OFFSET IS CONSUMED BY THE CAP IT PRECEDES, and by nothing else.
    //
    // `#>a` selects no maximum, so it does not compete with `#<m` for the
    // minimum — it says where that minimum starts counting, which is the
    // `offset` the clause has always had a field for, and skipping `a` then
    // `b` before one cap skips `a + b`. Order decides: capping and then
    // skipping is two relations and stays two steps, and offsets with no cap
    // to join stay exactly as many steps as the author wrote.
    //
    // The sum is the OPTIMIZATION, not the meaning. Every authored offset is
    // a lawful `i64` and their sum need not be, so a sum that will not fit
    // stops folding rather than wrapping into a negative skip: the relations
    // the author wrote say the same thing exactly, one clause each.
    let mut min_limit: Option<(i64, crate::names::ScopeId)> = None;
    let mut pending: Vec<ChainStep> = Vec::new();
    let mut pending_sum: i64 = 0;
    let mut consumed_offset: Option<i64> = None;
    let mut non_limit_steps: Vec<ChainStep> = Vec::new();
    for step in steps {
        if let Some(value) = offset_value(&step) {
            if min_limit.is_some() {
                // The cap already stands; this skips its result.
                non_limit_steps.push(step);
            } else {
                match pending_sum.checked_add(value) {
                    Some(sum) => pending_sum = sum,
                    None => {
                        non_limit_steps.append(&mut pending);
                        pending_sum = value;
                    }
                }
                pending.push(step);
            }
            continue;
        }
        if let Some(value) = limit_value(&step) {
            // Capture origin/cpr_schema from the first limit we see; they're
            // interchangeable for purposes of the folded result.
            let ChainStep::Bound { cpr_schema, .. } = step else {
                unreachable!("limit_value matched on a non-Bound step")
            };
            match min_limit.as_mut() {
                None => {
                    min_limit = Some((value, cpr_schema));
                    if !pending.is_empty() {
                        consumed_offset = Some(pending_sum);
                        pending.clear();
                    }
                }
                Some((cur, _)) if value < *cur => *cur = value,
                _ => {}
            }
        } else {
            non_limit_steps.push(step);
        }
    }

    // Offsets that met no cap are still the steps the author wrote.
    non_limit_steps.extend(pending);

    if let Some((value, cpr_schema)) = min_limit {
        non_limit_steps.push(ChainStep::Bound {
            bound: TupleOrdinalClause {
                operator: TupleOrdinalOperator::LessThan,
                value,
                offset: consumed_offset,
            },
            cpr_schema,
        });
    }

    non_limit_steps
}

/// Returns the skipped-row count if this step is a `#>n` bound.
fn offset_value(step: &ChainStep) -> Option<i64> {
    if let ChainStep::Bound {
        bound:
            TupleOrdinalClause {
                operator: TupleOrdinalOperator::GreaterThan,
                value,
                offset: _,
            },
        ..
    } = step
    {
        Some(*value)
    } else {
        None
    }
}

/// True if `expr` carries a row bound anywhere along its
/// top-level pipe/filter chain (without descending into joins, set-ops,
/// or inner-relation subqueries — those are sealed contexts).
fn branch_has_top_level_limit(expr: &Chain) -> bool {
    use crate::pipeline::asts::core::expressions::chain::SpineStep;
    expr.source_spine()
        .any(|step| matches!(step, SpineStep::Bound(_)))
}

/// Wrap an expression as a synthetic UDT-shaped InnerRelation. Uses the
/// `Indeterminate` pattern variant so the existing pattern classifier
/// reclassifies it (no correlation, no top-level limit ⇒ UDT).
fn wrap_as_indeterminate(inner: Chain) -> Chain {
    let cpr_schema = crate::pipeline::resolver::helpers::extraction::extract_cpr_schema(&inner);
    let identifier = QualifiedName {
        namespace_path: NamespacePath::empty(),
        name: SqlIdentifier::from("__dql_limit_wrap"),
    };
    Chain::relation(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier,
            subquery: Box::new(inner),
        },
        preminted_scope: None,
        alias: None,
        outer: false,
        cpr_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::Registry;
    use crate::pipeline::asts::core::expressions::pipes::DestructureMode;
    use crate::pipeline::asts::resolved::{Access, DomainExpression, FunctionApplication};

    fn scope(registry: &Registry) -> crate::names::ScopeId {
        registry.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::User(registry.intern("t", false)),
            None,
        )
    }

    fn bound(value: i64, cpr_schema: crate::names::ScopeId) -> Continuation {
        Continuation::Bound {
            bound: TupleOrdinalClause {
                operator: TupleOrdinalOperator::LessThan,
                value,
                offset: None,
            },
            cpr_schema,
        }
    }

    /// An aggregate destructure: the mode that MULTIPLIES rows, so a bound
    /// below it and a bound above it keep different rows.
    fn destructure(cpr_schema: crate::names::ScopeId) -> Continuation {
        Continuation::Destructure {
            // The source and pattern are irrelevant to placement; what
            // matters is that an aggregate destructure stands here.
            source: Box::new(DomainExpression::Application(FunctionApplication::Ground(
                crate::pipeline::asts::core::LiteralValue::Null,
            ))),
            pattern: crate::pipeline::asts::core::TreePattern::Record(
                crate::pipeline::asts::core::RecordPattern {
                    members: crate::pipeline::asts::vocabulary::Vec1::new(
                        crate::pipeline::asts::core::RecordPatternMember::Disregarded,
                    ),
                },
            ),
            mode: DestructureMode::Aggregate,
            schema: Vec::new(),
            cpr_schema,
        }
    }

    /// A restriction whose CONTENT is irrelevant to placement — only that
    /// one stands here. It is a real comparison because there is no
    /// synthetic truth leaf to reach for.
    fn restrict(cpr_schema: crate::names::ScopeId) -> Continuation {
        Continuation::Restrict {
            condition: TruthExpression::Comparison(crate::pipeline::asts::core::Comparison {
                operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
                left: Box::new(DomainExpression::Application(FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Number("1".into()),
                ))),
                right: Box::new(DomainExpression::Application(FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Number("1".into()),
                ))),
            }),
            origin: FilterOrigin::UserWritten,
            cpr_schema,
        }
    }

    /// `t(*) #<2, x ~= ~> {…}, <predicate>` — the bound is BELOW the
    /// destructure, and the predicate above it is only correct if it reads the
    /// already-bounded rows.
    ///
    /// Three separate claims, and each fails a different way:
    ///
    ///   - the seal exists at all (a walk that stops at the destructure never
    ///     sees the bound, so nothing is sealed);
    ///   - the seal holds the bound and NOTHING ELSE (a walk that linearizes
    ///     the destructure but does not let it seal lets `fold_limits` append
    ///     the bound past the destructure, which is a different query
    ///     whenever the destructure explodes a row);
    ///   - the destructure and the predicate stay above the seal, in the
    ///     order they were written.
    #[test]
    fn a_bound_below_a_destructure_is_sealed_in_place() {
        let registry = Registry::new(&[]);
        let s = scope(&registry);
        let chain = Relation::ground_read(Access::All, false, s)
            .then(bound(2, s))
            .then(destructure(s))
            .then(restrict(s));

        let placed = apply(chain).expect("limit placement answers");

        let Grelex::Reference(Relation::InnerRelation {
            pattern: InnerRelationPattern::Indeterminate { subquery, .. },
            ..
        }) = &placed.head
        else {
            panic!("the bound below the destructure was never sealed: {placed:?}");
        };
        assert!(
            matches!(subquery.steps(), [Continuation::Bound { .. }]),
            "the seal must hold the bound alone, so the destructure still runs \
             on the bounded rows: {:?}",
            subquery.continuations
        );
        assert!(
            matches!(
                placed.steps(),
                [
                    Continuation::Destructure { .. },
                    Continuation::Restrict { .. }
                ]
            ),
            "the destructure and the predicate keep their authored order above \
             the seal: {:?}",
            placed.continuations
        );
    }

    /// The base boundary is handed back untouched. `linearize` pops before it
    /// classifies, so a continuation it does not take must be pushed back —
    /// a chain standing on a set-op arm still stands on it afterwards.
    #[test]
    fn a_bag_op_is_returned_to_the_base() {
        let registry = Registry::new(&[]);
        let s = scope(&registry);
        let arm = Relation::ground_read(Access::All, false, s);
        let chain = Relation::ground_read(Access::All, false, s)
            .bag_op(
                crate::pipeline::asts::resolved::SetOperator::UnionCorresponding,
                arm,
                None,
                s,
            )
            .then(bound(2, s));

        let (base, steps) = linearize(chain);

        assert_eq!(steps.len(), 1, "only the bound linearizes");
        assert!(
            matches!(base.steps(), [Continuation::BagOp { .. }]),
            "the set-op step is still where it was: {:?}",
            base.continuations
        );
    }
}
