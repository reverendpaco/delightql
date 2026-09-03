// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Limit Placement Pass
//
// Resolved-phase rewrite that walks pipe/filter chains and inserts derived
// table boundaries where structural sealing is required:
//
//   - A bound followed by a step that must observe the bounded row set —
//     a predicate, an ordering (bounded or not), an aggregation (a Group
//     step), or a destructure — wrap the bound-bearing prefix as an
//     uncorrelated derived table so the later operation sees only the
//     bounded rows. An ordered bound is such a bound: the ordering's node
//     carries it, and the node is sealed whole.
//   - Multiple ARBITRARY bounds in the same segment fold to a single
//     `#<min`, since "smallest binds" matches DQL's pipeline semantics.
//     Only the arbitrary bound moves: it is transparent. An ordered bound
//     is the ordering's own act and stays exactly where it stands.
//
// Set-op + limit handling lives in the set-op branch pass, not here.

use crate::error::Result;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern as CoreInnerRelationPattern;
use crate::pipeline::asts::resolved::{
    Chain, Continuation, GroundForm, GroupSpec, NamespacePath, PipeOp, QualifiedName, Relation,
    Resolved, Step, TupleOrdinalClause, TupleOrdinalOperator,
};
use crate::relation::Planning;
use delightql_types::SqlIdentifier;

type InnerRelationPattern = CoreInnerRelationPattern<Resolved>;

/// Apply the limit-placement transformation across an entire resolved AST.
///
/// Recursive: descends into joins, set-ops, and inner-relation subqueries
/// before transforming each level's pipe/filter chain.
#[stacksafe::stacksafe]
pub fn apply(expr: Chain, identities: &Planning) -> Result<Chain> {
    // Linearize this level's chain.
    let (base, steps) = linearize(expr);

    // Recurse into the base (joins, set-ops, inner-relation subqueries).
    let base = apply_to_base(base, identities)?;

    // Recurse into operators that carry their own subqueries (scalar subqueries,
    // pivots, etc. — currently only handled within step expressions; pipe
    // operators don't typically carry relational subqueries directly).

    // Transform the chain.
    rewrite_chain(base, steps, identities)
}

#[stacksafe::stacksafe]
fn apply_to_base(base: Chain, identities: &Planning) -> Result<Chain> {
    let peeled = match base.peel() {
        Err(base) => {
            // The base is the head and the access its own read consumes; the
            // access rides back untouched on the relation it asked of, and
            // every node keeps the relation it published.
            return base.rebuilding(
                |subquery| apply(subquery, identities),
                |_, _| Ok(crate::pipeline::asts::core::Standing::Keep),
            );
        }
        Ok(peeled) => peeled,
    };
    match peeled.last().form() {
        Continuation::Member { .. } => {
            peeled.rebuilding_arms(|base| apply(base, identities), |rhs| apply(rhs, identities))
        }
        Continuation::BagOp { .. } => {
            // A set-op branch carrying a
            // top-level LIMIT must be sealed in a subquery — raw `LIMIT N
            // UNION ALL ...` is invalid SQL, and even where dialects accept
            // it, an unwrapped LIMIT on one side hoists to the outer set-op
            // level and fails to bind to its intended branch.
            let seal = |arm: Chain| -> Result<Chain> {
                let refined = apply(arm, identities)?;
                Ok(if branch_has_top_level_limit(&refined) {
                    wrap_as_indeterminate(refined, identities)?
                } else {
                    refined
                })
            };
            peeled.rebuilding_arms(&seal, &seal)
        }
        // No pipe, restriction, bound, or destructure can appear at the
        // base position (linearize consumes them all); an ER edge never
        // reaches the resolved phase.
        _ => Ok(peeled.rejoin()),
    }
}

// ============================================================================
// Chain step linearization
// ============================================================================

/// One linearized step.
///
/// Every kind but the bound is CARRIED WHOLE — the step handed back is the
/// step taken, relation included, so moving a segment cannot re-pair one.
/// A bound carries only its clause: this pass MOVES bounds, and a bound is
/// transparent, so what it publishes is the relation it comes to stand on
/// and is derived there.
enum ChainStep {
    Access(Step),
    Pipe(Step),
    Filter(Step),
    Bound(TupleOrdinalClause),
    Destructure(Step),
    Structural(Step),
}

/// A continuation this pass linearizes, or the base boundary handed back.
///
/// ONE CLASSIFICATION ACT. Membership is this match's own answer, so there is
/// no second list of admitted names beside it to disagree with it.
fn classify(step: Step) -> std::result::Result<ChainStep, Step> {
    match step.form() {
        Continuation::Access { .. } => Ok(ChainStep::Access(step)),
        Continuation::Pipe { .. } => Ok(ChainStep::Pipe(step)),
        Continuation::Restrict { .. } => Ok(ChainStep::Filter(step)),
        Continuation::Bound { bound } => {
            let bound = bound.clone();
            Ok(ChainStep::Bound(bound))
        }
        // A correlation stands on a bag run, and a bag run is opaque to
        // this pass: handing it back stops the descent where it should.
        Continuation::Correlate { .. } => Err(step),
        Continuation::Destructure { .. } => Ok(ChainStep::Destructure(step)),
        Continuation::Structural(_) => Ok(ChainStep::Structural(step)),
        // A member and a set-op arm carry their own chains; `apply_to_base`
        // descends into those rather than moving them.
        Continuation::Member { .. } | Continuation::BagOp { .. } => Err(step),
        Continuation::ErJoin(_) => Err(step),
    }
}

#[stacksafe::stacksafe]
fn linearize(expr: Chain) -> (Chain, Vec<ChainStep>) {
    // THE HEAD'S OWN ACCESS IS NOT A STEP. The peel stops at the read the
    // chain stands on, so no boundary this pass inserts can land between a
    // mention and what its parens asked for. A step this pass does not
    // linearize stays where it is rather than coming off and going back.
    let (base, steps) = expr.peel_while(linearizes).into_parts();
    (
        base,
        steps
            .into_iter()
            .map(|step| classify(step).expect("the peel admitted only linearized forms"))
            .collect(),
    )
}

/// Whether this pass linearizes a continuation. A member and a set-op arm
/// carry their own chains — `apply_to_base` descends into those rather than
/// moving them — a correlation stands on a bag run, which is opaque here,
/// and an ER edge never reaches the resolved phase.
fn linearizes(form: &Continuation) -> bool {
    matches!(
        form,
        Continuation::Access { .. }
            | Continuation::Pipe { .. }
            | Continuation::Restrict { .. }
            | Continuation::Bound { .. }
            | Continuation::Destructure { .. }
            | Continuation::Structural(_)
    )
}

fn reconstruct(base: Chain, steps: Vec<ChainStep>, identities: &Planning) -> Result<Chain> {
    let mut source = base;
    for step in steps {
        source = match step {
            ChainStep::Access(step)
            | ChainStep::Pipe(step)
            | ChainStep::Filter(step)
            | ChainStep::Destructure(step)
            // WRAPPING REPUBLISHES: the boundary this pass inserts hands
            // back a chain publishing the relation it wrapped, so the step
            // lands on the relation it was derived over. `relanded` states
            // that rather than assuming it.
            | ChainStep::Structural(step) => identities.authority().reland(source, step)?,
            // A BOUND IS TRANSPARENT, so what it publishes is the relation
            // it stands on — and this pass MOVES it. Deriving it where the
            // bound LANDS is what keeps the chain from publishing an
            // earlier heading than the statement emits.
            ChainStep::Bound(bound) => source.transparently(crate::pipeline::asts::resolved::Transparent::Bound { bound }),
        };
    }
    Ok(source)
}

// ============================================================================
// Step classification
// ============================================================================

/// Returns the limit value if this step is a `#<n` bound.
///
/// `#>n` is an OFFSET, which selects no maximum, so only the less-than
/// spelling answers here.
fn limit_value(step: &ChainStep) -> Option<i64> {
    if let ChainStep::Bound(TupleOrdinalClause {
        operator: TupleOrdinalOperator::LessThan,
        value,
        offset: _,
    }) = step
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
        ChainStep::Bound(_) | ChainStep::Access(_) => false,
        ChainStep::Filter(_) | ChainStep::Destructure(_) => true,
        ChainStep::Pipe(step) => match pipe_operator(step) {
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
        ChainStep::Structural(step) => matches!(
            step.form(),
            Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::Ordering { .. },
                ..
            })
        ),
    }
}

/// The operator a pipe step carries.
fn pipe_operator(step: &Step) -> &PipeOp {
    match step.form() {
        Continuation::Pipe { operator, .. } => operator,
        _ => unreachable!("a pipe step carries a pipe operator"),
    }
}

// ============================================================================
// Chain rewriting
// ============================================================================

#[stacksafe::stacksafe]
fn rewrite_chain(base: Chain, steps: Vec<ChainStep>, identities: &Planning) -> Result<Chain> {
    let mut current_base = base;
    let mut buffer: Vec<ChainStep> = Vec::new();

    for step in steps {
        if seals_preceding_bound(&step) {
            if buffer_has_bound(&buffer) {
                let buffer_now = std::mem::take(&mut buffer);
                let folded = fold_limits(buffer_now);
                let wrapped_inner = reconstruct(current_base, folded, identities)?;
                current_base = wrap_as_indeterminate(wrapped_inner, identities)?;
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

    reconstruct(current_base, folded_tail, identities)
}

/// A BOUND SEALS WHAT PRECEDES IT, every spelling.
///
/// `#>a` denotes a relation exactly as `#<m` does, so a step that must
/// observe an already-bounded row set must observe a skipped one too — an
/// offset left pending across a predicate or an ordering would be applied
/// to rows those steps had already chosen. An ordering carrying its bound
/// bounds the same way: the step after it observes the members it chose.
fn buffer_has_bound(buf: &[ChainStep]) -> bool {
    buf.iter().any(|s| {
        limit_value(s).is_some() || offset_value(s).is_some() || ordered_bound(s).is_some()
    })
}

/// The bound an ordering consumed, if this step is that ordering.
fn ordered_bound(step: &ChainStep) -> Option<&TupleOrdinalClause> {
    match step {
        ChainStep::Structural(step) => match step.form() {
            Continuation::Structural(crate::pipeline::asts::core::StructuralStep {
                form:
                    crate::pipeline::asts::core::StructuralForm::Ordering {
                        bound: Some(bound), ..
                    },
                ..
            }) => Some(bound),
            _ => None,
        },
        ChainStep::Access(_)
        | ChainStep::Pipe(_)
        | ChainStep::Filter(_)
        | ChainStep::Bound(_)
        | ChainStep::Destructure(_) => None,
    }
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
    let mut min_limit: Option<i64> = None;
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
            // Capture origin/result from the first limit we see; they're
            // interchangeable for purposes of the folded result.
            match min_limit.as_mut() {
                None => {
                    min_limit = Some(value);
                    if !pending.is_empty() {
                        consumed_offset = Some(pending_sum);
                        pending.clear();
                    }
                }
                Some(cur) if value < *cur => *cur = value,
                _ => {}
            }
        } else {
            non_limit_steps.push(step);
        }
    }

    // Offsets that met no cap are still the steps the author wrote.
    non_limit_steps.extend(pending);

    if let Some(value) = min_limit {
        non_limit_steps.push(ChainStep::Bound(TupleOrdinalClause {
            operator: TupleOrdinalOperator::LessThan,
            value,
            offset: consumed_offset,
        }));
    }

    non_limit_steps
}

/// Returns the skipped-row count if this step is a `#>n` bound.
fn offset_value(step: &ChainStep) -> Option<i64> {
    if let ChainStep::Bound(TupleOrdinalClause {
        operator: TupleOrdinalOperator::GreaterThan,
        value,
        offset: _,
    }) = step
    {
        Some(*value)
    } else {
        None
    }
}

/// True if `expr` carries a row bound — arbitrary or ordered — anywhere
/// along its top-level pipe/filter chain (without descending into joins,
/// set-ops, or inner-relation subqueries — those are sealed contexts).
fn branch_has_top_level_limit(expr: &Chain) -> bool {
    expr.source_spine().any(|step| step.bound().is_some())
}

/// Wrap an expression as a synthetic derived table.
///
/// UNCORRELATED BY CONSTRUCTION. The wrap re-stages a chain that stood at
/// the top level so a later step observes its bounded rows; the relation on
/// both sides is one relation and nothing outside it correlates with it.
/// Leaving it Indeterminate handed it to the pattern classifier, which reads
/// the bound this wrap EXISTS FOR as a top-level limit and any correlation
/// the wrapped body carries — an ER edge's hops, say — as the wrap's own, and
/// then asked the interior top-N road to prove a partition over correlations
/// that were never the wrap's.
fn wrap_as_indeterminate(inner: Chain, identities: &Planning) -> Result<Chain> {
    let identifier = QualifiedName {
        namespace_path: NamespacePath::empty(),
        name: SqlIdentifier::from("__dql_limit_wrap"),
    };
    Ok(Chain::ground(identities.authority().wrapping_head(
        GroundForm::Reference(Relation::InnerRelation {
            pattern: InnerRelationPattern::UncorrelatedDerivedTable {
                identifier,
                subquery: Box::new(inner),
                is_consulted_view: false,
            },
            alias: None,
            outer: false,
        }),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::asts::core::expressions::pipes::DestructureMode;
    use crate::pipeline::asts::resolved::{
        Access, DomainExpression, FilterOrigin, FunctionApplication, TruthExpression,
    };

    /// One transparent step, landed exactly as production lands one.
    fn step(chain: Chain, form: Continuation) -> Chain {
        chain.transparently(
            crate::pipeline::asts::resolved::Transparent::of(form)
                .unwrap_or_else(|_| unreachable!("the fixtures build transparent forms")),
        )
    }

    fn scope(registry: &crate::relation::Planning) -> crate::relation::SemanticRelation {
        crate::relation::any_relation(registry)
    }

    fn bound(value: i64) -> Continuation {
        Continuation::Bound {
            bound: TupleOrdinalClause {
                operator: TupleOrdinalOperator::LessThan,
                value,
                offset: None,
            },
        }
    }

    /// An aggregate destructure: the mode that MULTIPLIES rows, so a bound
    /// below it and a bound above it keep different rows.
    /// A restriction whose CONTENT is irrelevant to placement — only that
    /// one stands here. It is a real comparison because there is no
    /// synthetic truth leaf to reach for.
    fn restrict() -> Continuation {
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
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let s = scope(&registry);
        let chain = registry
            .authority()
            .ground_read(Access::All, false, s)
            .expect("a ground read");
        let chain = step(chain, bound(2));
        // The source and pattern are irrelevant to placement; what matters
        // is that an aggregate destructure stands here, and it is stated to
        // the authority like any other.
        let (staged, _) = registry
            .authority()
            .bind(crate::relation::pending::Pending::Destructure {
                input: s,
                source: DomainExpression::Application(FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Null,
                )),
                mode: DestructureMode::Aggregate,
                pattern: crate::pipeline::asts::core::TreePattern::Record(
                    crate::pipeline::asts::core::RecordPattern {
                        members: crate::pipeline::asts::vocabulary::Vec1::new(
                            crate::pipeline::asts::core::RecordPatternMember::Disregarded,
                        ),
                    },
                ),
            })
            .expect("a destructure over the same relation");
        let chain = registry
            .authority()
            .reland(chain, staged)
            .expect("the destructure lands on the relation it was derived over");
        let chain = step(chain, restrict());

        let placed = apply(chain, &registry).expect("limit placement answers");

        let GroundForm::Reference(Relation::InnerRelation {
            pattern: InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. },
            ..
        }) = placed.head().form()
        else {
            panic!("the bound below the destructure was never sealed: {placed:?}");
        };
        assert!(
            matches!(
                subquery.step_forms().as_slice(),
                [Continuation::Bound { .. }]
            ),
            "the seal must hold the bound alone, so the destructure still runs \
             on the bounded rows: {:?}",
            subquery.continuations()
        );
        assert!(
            matches!(
                placed.step_forms().as_slice(),
                [
                    Continuation::Destructure { .. },
                    Continuation::Restrict { .. }
                ]
            ),
            "the destructure and the predicate keep their authored order above \
             the seal: {:?}",
            placed.continuations()
        );
    }

    /// An ordering carrying its bound, derived by the authority over `chain`.
    fn ordered_bound(registry: &crate::relation::Planning, chain: Chain, value: i64) -> Chain {
        let (staged, _) = registry
            .authority()
            .bind(crate::relation::pending::Pending::Ordering {
                input: chain.semantic_relation(),
                specs: Vec::new(),
                bound: Some(TupleOrdinalClause {
                    operator: TupleOrdinalOperator::LessThan,
                    value,
                    offset: None,
                }),
            })
            .expect("an ordered bound derives");
        registry
            .authority()
            .reland(chain, staged)
            .expect("the ordered bound lands on the relation it was derived over")
    }

    /// AN ORDERED BOUND IS A BOUND, BOTH WAYS. `t(*) #<3, #(x) #<2, #(y)`:
    /// the arbitrary bound below it is sealed (the ordered bound re-decides
    /// membership, so it must observe the three), and the ordered bound is
    /// itself sealed under the later presentation ordering — whole, its
    /// clause never taken off the ordering's node.
    #[test]
    fn an_ordered_bound_seals_and_is_sealed_whole() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let s = scope(&registry);
        let chain = registry
            .authority()
            .ground_read(Access::All, false, s)
            .expect("a ground read");
        let chain = step(chain, bound(3));
        let chain = ordered_bound(&registry, chain, 2);
        let (presentation, _) = registry
            .authority()
            .bind(crate::relation::pending::Pending::Ordering {
                input: chain.semantic_relation(),
                specs: Vec::new(),
                bound: None,
            })
            .expect("a presentation ordering derives");
        let chain = registry
            .authority()
            .reland(chain, presentation)
            .expect("the presentation lands");

        let placed = apply(chain, &registry).expect("limit placement answers");

        // Outermost: the presentation stands over a seal.
        let GroundForm::Reference(Relation::InnerRelation {
            pattern: InnerRelationPattern::UncorrelatedDerivedTable { subquery, .. },
            ..
        }) = placed.head().form()
        else {
            panic!("the ordered bound was never sealed under the presentation: {placed:?}");
        };
        assert!(
            matches!(
                placed.step_forms().as_slice(),
                [Continuation::Structural(
                    crate::pipeline::asts::core::StructuralStep {
                        form: crate::pipeline::asts::core::StructuralForm::Ordering {
                            bound: None,
                            ..
                        },
                        ..
                    }
                )]
            ),
            "only the presentation stands above the seal: {:?}",
            placed.continuations()
        );
        // Inside: the ordered bound, whole, over a seal holding the arbitrary bound.
        assert!(
            matches!(
                subquery.step_forms().as_slice(),
                [Continuation::Structural(
                    crate::pipeline::asts::core::StructuralStep {
                        form: crate::pipeline::asts::core::StructuralForm::Ordering {
                            bound: Some(TupleOrdinalClause { value: 2, .. }),
                            ..
                        },
                        ..
                    }
                )]
            ),
            "the ordered bound keeps its clause on the ordering's node: {:?}",
            subquery.continuations()
        );
        let GroundForm::Reference(Relation::InnerRelation {
            pattern:
                InnerRelationPattern::UncorrelatedDerivedTable {
                    subquery: inner, ..
                },
            ..
        }) = subquery.head().form()
        else {
            panic!("the arbitrary bound was never sealed under the ordered bound: {subquery:?}");
        };
        assert!(
            matches!(
                inner.step_forms().as_slice(),
                [Continuation::Bound {
                    bound: TupleOrdinalClause { value: 3, .. }
                }]
            ),
            "the arbitrary bound is what the ordered bound observes: {:?}",
            inner.continuations()
        );
    }

    /// The base boundary is handed back untouched. `linearize` pops before it
    /// classifies, so a continuation it does not take must be pushed back —
    /// a chain standing on a set-op arm still stands on it afterwards.
    #[test]
    fn a_bag_op_is_returned_to_the_base() {
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let s = scope(&registry);
        let arm = registry
            .authority()
            .ground_read(Access::All, false, s)
            .expect("a ground read");
        let set = registry
            .authority()
            .set_step(
                crate::pipeline::asts::resolved::SetOperator::UnionCorresponding,
                &[s, s],
            )
            .expect("a corresponding set of two arms is built");
        let chain = registry
            .authority()
            .ground_read(Access::All, false, s)
            .expect("a ground read");
        let chain = registry.authority().bag(chain, set, arm, None);
        let chain = step(chain, bound(2));

        let (base, steps) = linearize(chain);

        assert_eq!(steps.len(), 1, "only the bound linearizes");
        assert!(
            matches!(base.step_forms().as_slice(), [Continuation::BagOp { .. }]),
            "the set-op step is still where it was: {:?}",
            base.continuations()
        );
    }
}
