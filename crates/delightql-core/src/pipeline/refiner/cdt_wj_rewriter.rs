// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// CDT-WJ → CDT-SJ Structural Rewriter
//
// Rewrites a correlated-with-LIMIT subquery into a CDT-SJ-shaped subquery
// whose body explicitly contains a ROW_NUMBER() window expression and a
// `WHERE rn <= N` filter. The correlation itself is held on the pattern, so
// the resulting pattern classifies as CorrelatedScalarJoin, which the
// rebuilder/transformer already lower correctly (correlation hoists to
// JOIN ON; the windowed subquery materializes naturally).
//
// The refiner does the rewriting, not the transformer: refined AST is
// descriptive of the target SQL shape, not prescriptive input the
// transformer has to reinterpret.
//
// Input shape (resolved phase, correlations already taken out):
//
//   Ordering { specs, bound: Some(#<N) }          — the ordered bound, ONE node
//     over ...inner filter+relation...
//   or Bound(#<N) over the same                    — the arbitrary bound
//
// Output shape:
//
//   Filter(condition=Predicate(__dql_rn <= N),
//     source=Pipe(operator=General[Glob, Window(row_number, partition, order_by, alias=__dql_rn)],
//       source=...inner with the bounding step removed...))
//
// The ordered bound's specs become the window's ORDER BY and its N the rn
// comparison — both read off the one node, so the window cannot rank by an
// ordering the bound did not consume. An arbitrary bound orders nothing.
//
// THE PARTITION KEY IS THE CORRELATION'S OWN INTERIOR OCCURRENCE, exactly
// as the correlation act named it. The window stands over a relation that
// owes that occurrence by construction, so the reference lands on whatever
// position keeps it readable there; nothing here looks a carrier up.

use crate::diagnostic::{DelightQLError, Interior, Internal};
use crate::error::Result;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved::{
    self, Chain, DomainExpression, FilterOrigin, FunctionApplication, LiteralValue,
    TruthExpression, TupleOrdinalClause, TupleOrdinalOperator,
};

use super::pattern_classifier::Hoisted;

type OrderingSpec = crate::pipeline::asts::core::OrderingSpec<resolved::Resolved>;

/// Flatten one correlation filter through `and` into its conjunct
/// comparisons, PROVING each conjunct is an equality. Anything not
/// provable refuses: non-equality comparisons (each outer row would see
/// a different candidate set), `or`/`not` (no single child group per
/// outer row), and every unrecognized predicate form. The flattened
/// list is what partition-key extraction consumes, so compound
/// equalities partition identically to comma-separated ones.
fn prove_equality_conjunction<'a>(
    f: &'a TruthExpression,
    out: &mut Vec<&'a TruthExpression>,
) -> Result<()> {
    match f {
        TruthExpression::Conjunction(parts) => {
            for part in parts.iter() {
                prove_equality_conjunction(part, out)?;
            }
            Ok(())
        }
        TruthExpression::Comparison(Comparison { operator, .. })
            if matches!(
                operator,
                crate::pipeline::asts::vocabulary::CmpOp::NullSafeEqual
                    | crate::pipeline::asts::vocabulary::CmpOp::Equal
            ) =>
        {
            out.push(f);
            Ok(())
        }
        TruthExpression::Comparison(Comparison { operator, .. }) => {
            let spelled = match operator {
                crate::pipeline::asts::vocabulary::CmpOp::LessThan => "<",
                crate::pipeline::asts::vocabulary::CmpOp::LessThanOrEqual => "<=",
                crate::pipeline::asts::vocabulary::CmpOp::GreaterThan => ">",
                crate::pipeline::asts::vocabulary::CmpOp::GreaterThanOrEqual => ">=",
                crate::pipeline::asts::vocabulary::CmpOp::NotEqual
                | crate::pipeline::asts::vocabulary::CmpOp::NullSafeNotEqual => "!=",
                other => other.sql_name(),
            };
            Err(DelightQLError::from(Interior::TopnNoneqCorrelation {
    message: format!(
                    "interior top-N requires equality correlation: '{}' makes each outer row see a different candidate set, and the pre-ranked lowering would rank the wrong population",
                    spelled
                ),
}))
        }
        other => Err(DelightQLError::from(Interior::TopnNoneqCorrelation {
    message: format!(
                "interior top-N requires equality correlation, provable as a conjunction of equalities — this correlation contains {}",
                match other {
                    TruthExpression::Disjunction(_) => "an `or`",
                    TruthExpression::Not { .. } => "a `not`",
                    _ => "a predicate form the pre-ranked lowering cannot prove sound",
                }
            ),
})),
    }
}

/// Rewrite a (correlated, has-limit) subquery into a CDT-SJ-shaped subquery.
///
/// Walks the subquery, captures the limit value and order_by specs, removes
/// those nodes, and adds a window-projection pipe + rn-filter on top. The
/// window partitions by the correlations' interior occurrences, which the
/// relation it stands over owes by construction.
///
/// The caller builds a `CorrelatedScalarJoin` pattern directly with the
/// result, holding the hoisted conditions itself.
pub(super) fn rewrite_window_join_subquery(
    subquery: Chain,
    hoisted: &[Hoisted],
    identities: &crate::relation::Planning,
) -> Result<Chain> {
    // This lowering pre-ranks per correlation-key group and joins AFTER —
    // sound only when the correlation is a CONJUNCTION OF EQUALITIES,
    // because then each outer row sees exactly one child group and
    // per-group top-N equals per-outer-row top-N. Acceptance is by
    // PROOF, default-deny: the filters flatten through `and` into
    // conjunct comparisons, every conjunct must be an equality, and
    // `or`/`not`/any unrecognized predicate form refuses. Detection of
    // known-bad shapes is not enough — an `and`-compound once slipped a
    // top-level-only check and emitted an UNPARTITIONED ranking,
    // wronger than the phantom-row bug this guards against.
    //
    // The second half of the proof, BEFORE any rewriting: every proved
    // equality conjunct must contribute exactly one directly
    // representable partition key, or the whole rewrite refuses —
    // extraction that silently skips a conjunct emits an unpartitioned
    // (or under-partitioned) ranking, the same silent-wrong-answer
    // family the flattening guard above closes.
    let mut partition_columns: Vec<crate::relation::PortId> = Vec::new();
    for correlation in hoisted {
        let mut conjuncts = Vec::new();
        prove_equality_conjunction(correlation.condition(), &mut conjuncts)?;
        for conjunct in conjuncts {
            partition_columns.push(prove_partition_key(conjunct, correlation)?);
        }
    }

    // The bounding step comes off whole: the complete clause — the rows it
    // skips and the rows it keeps — and the ordering it consumed. ONE
    // interval: a second bound still standing on the run would need a rank
    // over a rank, which this realization does not spell.
    let (subquery_no_order, bound, order_specs) = take_bound(subquery)?;
    if subquery_no_order
        .source_spine()
        .any(|step| step.bound().is_some())
    {
        return Err(DelightQLError::from(Interior::BoundInterval {
            message: "a correlated interior's row bound is realized per outer row as one rank \
                      interval, and this interior bounds its rows twice — compose the two \
                      bounds into one, or rank explicitly with a row_number window"
                .to_string(),
        }));
    }

    // THE WINDOW NAMES THE KEY AS THE CORRELATION READ IT. The relation
    // under the window keeps that occurrence readable by construction —
    // published, or emitted beside its heading in a position that
    // continues it — and SQL binding answers a reference to the occurrence
    // with whichever position that is.
    let partition_by: Vec<DomainExpression> = partition_columns
        .into_iter()
        .map(|column| {
            DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence::engine(
                column,
            ))))
        })
        .collect();

    // Wrap with the embed: the operand's whole heading, then the row-number
    // witness standing at the port that same derivation minted for it.
    let authority = identities.authority();
    let (staged, published) = authority.bind(crate::relation::pending::Pending::WindowWitness {
        input: subquery_no_order.semantic_relation(),
        partition: partition_by,
        ordering: order_specs,
    })?;
    let row_number_port = *published
        .last()
        .expect("the window projection appends one row-number port");
    let projected = authority.reland(subquery_no_order, staged)?;

    // Wrap with the rank interval the bound spells.
    wrap_with_rn_filter(projected, row_number_port, bound)
}

/// The top-N partition-proof contract: convert ONE proved correlation
/// equality into the partition key it contributes, or refuse. Sound
/// pre-ranking requires each conjunct to pin a whole partition group
/// per outer row, which holds exactly when one side is a plain interior
/// column (the key) and the other side provably references only the
/// outer scope (constant per outer row). A wrapped interior key has no
/// directly representable partition column; an interior reference on
/// the non-key side narrows the candidate set within a group after
/// ranking. Both refuse, default-deny — the alternative is a plausible
/// ranking over the wrong population (an unpartitioned or
/// mispartitioned row_number()).
///
/// Which occurrences are interior is the correlation act's own record,
/// derived from the condition and the relation it stood on. Nothing here
/// walks scopes to decide it again.
fn prove_partition_key(
    filter: &TruthExpression,
    support: &Hoisted,
) -> Result<crate::relation::PortId> {
    let TruthExpression::Comparison(Comparison { left, right, .. }) = filter else {
        unreachable!("prove_equality_conjunction flattens correlation filters to comparisons")
    };

    let topn_hint = "join normally and rank explicitly: ... |> (..., row_number:(<~ %(outer identity), #(ordering)) as rnk), rnk <= N";

    let (key, flank) = match (
        interior_key_column(left, support),
        interior_key_column(right, support),
    ) {
        (Some(key), None) => (key, right),
        (None, Some(key)) => (key, left),
        (Some(_), Some(_)) => {
            return Err(DelightQLError::from(Interior::TopnUnprovablePartition {
    message: format!("interior top-N requires the non-key side of each correlation equality to reference only the outer scope: both sides of this equality read the interior relation; {topn_hint}"),
}))
        }
        (None, None) => {
            return Err(DelightQLError::from(Interior::TopnUnprovablePartition {
    message: format!("interior top-N requires each correlation equality to name a plain interior column on one side: here the interior key is wrapped in an expression, so no partition key is directly representable and the pre-ranked lowering would rank the wrong population; {topn_hint}"),
}))
        }
    };

    if !provably_outer_only(flank, support) {
        return Err(DelightQLError::from(Interior::TopnUnprovablePartition {
    message: format!("interior top-N requires the non-key side of each correlation equality to reference only the outer scope: both sides of this equality read the interior relation; {topn_hint}"),
}));
    }

    Ok(key)
}

/// A directly representable interior partition key: a bare reference to
/// an occurrence the correlation reads from the interior. Anything wrapped
/// (functions, parentheses) is not directly representable: None.
fn interior_key_column(
    expr: &DomainExpression,
    support: &Hoisted,
) -> Option<crate::relation::PortId> {
    match expr {
        DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column,
            ..
        }))) if support.reads(*column) => Some(*column),
        _ => None,
    }
}

/// Default-deny purity check for the non-key side of a correlation
/// equality: true only for shapes PROVABLY constant per outer row —
/// references the correlation does not read from the interior, literals,
/// and plain function/parenthesis composition over those. Any shape this
/// match does not affirmatively admit (case expressions, windows,
/// subqueries, ...) is unproven and answers false.
fn provably_outer_only(expr: &DomainExpression, support: &Hoisted) -> bool {
    // A relation beneath the value is its own scope, whose references this
    // walk cannot enumerate: unproven, so false.
    if expr.nests_relation() {
        return false;
    }
    match expr {
        DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
            column,
            ..
        }))) => !support.reads(*column),
        DomainExpression::Application(FunctionApplication::Ground(_)) => true,
        DomainExpression::Application(func) => match func {
            FunctionApplication::Standard(application) => {
                let arguments = &application.call().arguments;
                arguments
                    .value_domains()
                    .all(|expr| provably_outer_only(expr, support))
                    && arguments
                        .scalar_members()
                        .iter()
                        .all(|member| member.scalar_domain().is_some())
                    && arguments
                        .ho_members()
                        .all(|argument| argument.scalar_domain().is_some())
            }
            FunctionApplication::Enclyph(crate::pipeline::asts::core::Enclyph::Tuple(tuple)) => {
                tuple
                    .elements
                    .iter()
                    .all(|element| provably_outer_only(element.value(), support))
            }
            FunctionApplication::Infix(infix) => {
                provably_outer_only(&infix.left, support)
                    && provably_outer_only(&infix.right, support)
            }
            // A crossed truth is outer-only when every value it reads is.
            FunctionApplication::Crossed(crossing) => crossing
                .truth()
                .scalar_operands()
                .into_iter()
                .all(|operand| provably_outer_only(operand, support)),
            _ => false,
        },
        _ => false,
    }
}

/// TAKE THE BOUND OFF THE SHAPING RUN, WITH THE ORDERING IT CONSUMED.
///
/// Returns the chain, the COMPLETE bound clause, and the ordering that
/// clause consumed — read off ONE node. An ordering carrying its bound is
/// the membership act: the window now performs it, ranking by exactly the
/// specs that bound consumed. The ordering's node STAYS and surrenders the
/// bound: it republishes its operand through the stage export, and
/// everything above it stands on the ports that export minted; the ORDER
/// BY it still emits is inert, the rank filter owns the selection. An
/// arbitrary bound comes out alone and the window orders nothing:
/// `row_number()` with no ORDER BY is legal SQL and ranks arbitrarily
/// within each partition, exactly the members the law lets an unordered
/// bound choose. Both spellings of the bound come off whole — a cap with
/// the offset it consumed, or a bare offset — so the rank filter spells
/// the interval the author wrote and never a cap alone.
///
/// The scan covers the shaping run above the relation and DELIBERATELY
/// does not descend a member's chain, a bag arm, or a condition's subquery
/// — the boundary is where the shaping stops.
fn take_bound(expr: Chain) -> Result<(Chain, TupleOrdinalClause, Vec<OrderingSpec>)> {
    let mut chain = expr;
    for index in (0..chain.continuations().len()).rev() {
        match chain.continuations()[index].form() {
            resolved::Continuation::Bound { bound } => {
                let bound = bound.clone();
                return Ok((chain.without(index)?, bound, Vec::new()));
            }
            resolved::Continuation::Structural(resolved::StructuralStep {
                form:
                    resolved::StructuralForm::Ordering {
                        specs,
                        bound: Some(_),
                    },
                ..
            }) => {
                let specs = specs.clone();
                let bound = chain
                    .surrender_bound(index)
                    .expect("the ordering just matched carries its bound");
                return Ok((chain, bound, specs));
            }
            resolved::Continuation::Restrict { .. }
            | resolved::Continuation::Pipe { .. }
            | resolved::Continuation::Structural(_) => {}
            _ => break,
        }
    }

    Err(Internal::invariant(
        "refiner::cdt_wj_rewriter",
        "rewrite_window_join_subquery: expected a row bound but found none".to_string(),
    ))
}

/// Wrap an expression with the RANK INTERVAL the bound spells, over the
/// generated row-number occurrence: `#>a, #<m` keeps ranks `a < rn <= a+m`,
/// `#<m` alone keeps `rn <= m`, and a bare `#>a` keeps `rn > a`. The whole
/// clause is consumed; an interval the rank cannot spell refuses.
fn wrap_with_rn_filter(
    source: Chain,
    row_number_column: crate::relation::PortId,
    bound: TupleOrdinalClause,
) -> Result<Chain> {
    let rank = || {
        DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence::engine(
            row_number_column,
        ))))
    };
    let number = |value: i64| {
        DomainExpression::Application(FunctionApplication::Ground(LiteralValue::Number(
            value.to_string(),
        )))
    };
    let compare = |operator, value: i64| {
        TruthExpression::Comparison(Comparison {
            operator,
            left: Box::new(rank()),
            right: Box::new(number(value)),
        })
    };
    use crate::pipeline::asts::vocabulary::CmpOp;
    let condition = match bound.operator {
        TupleOrdinalOperator::LessThan => {
            let skipped = bound.offset.unwrap_or(0);
            let last = skipped.checked_add(bound.value).ok_or_else(|| {
                DelightQLError::from(Interior::BoundInterval {
                    message: "a correlated interior's row bound skips and keeps more rows \
                              than one rank interval can count"
                        .to_string(),
                })
            })?;
            let keeps = compare(CmpOp::LessThanOrEqual, last);
            if skipped > 0 {
                TruthExpression::all(vec![compare(CmpOp::GreaterThan, skipped), keeps])
                    .expect("two conjuncts make a conjunction")
            } else {
                keeps
            }
        }
        TupleOrdinalOperator::GreaterThan => {
            if bound.offset.is_some() {
                return Err(Internal::invariant(
                    "refiner::cdt_wj_rewriter",
                    "an offset bound carries no offset of its own",
                ));
            }
            compare(CmpOp::GreaterThan, bound.value)
        }
        // `#=` has no authored spelling: `row_bound` derives `#<` and `#>`
        // and nothing builds this arm.
        TupleOrdinalOperator::Exactly => {
            return Err(Internal::invariant(
                "refiner::cdt_wj_rewriter",
                "an exact row bound has no authored spelling",
            ))
        }
    };

    Ok(source.transparently(resolved::Transparent::Restrict {
        condition,
        origin: FilterOrigin::Generated,
    }))
}
