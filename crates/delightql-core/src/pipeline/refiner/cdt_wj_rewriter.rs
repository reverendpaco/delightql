// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// CDT-WJ → CDT-SJ Structural Rewriter
//
// Rewrites a correlated-with-LIMIT subquery into a CDT-SJ-shaped subquery
// whose body explicitly contains a ROW_NUMBER() window expression and a
// `WHERE rn <= N` filter. The outer correlation predicate is preserved so
// the resulting pattern classifies as CorrelatedScalarJoin, which the
// rebuilder/transformer already lower correctly (correlation hoists to
// JOIN ON; the windowed subquery materializes naturally).
//
// The refiner does the rewriting, not the transformer: refined AST is
// descriptive of the target SQL shape, not prescriptive input the
// transformer has to reinterpret.
//
// Input shape (resolved phase):
//
//   Ordering { specs, bound: Some(#<N) }          — the ordered bound, ONE node
//     over ...inner correlation+filter+relation...
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

use crate::error::Result;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved::{
    self, Chain, DomainExpression, FilterOrigin, FunctionApplication, LiteralValue,
    TruthExpression, TupleOrdinalClause, TupleOrdinalOperator,
};

type OrderingSpec = crate::pipeline::asts::core::OrderingSpec<resolved::Resolved>;

/// Synthetic column name for the row_number() window output.

/// Flatten one correlation filter through `and` into its conjunct
/// comparisons, PROVING each conjunct is an equality. Anything not
/// provable refuses: non-equality comparisons (each outer row would see
/// a different candidate set), `or`/`not` (no single child group per
/// outer row), and every unrecognized predicate form. The flattened
/// list is what partition-key extraction consumes, so compound
/// equalities partition identically to comma-separated ones.
fn prove_equality_conjunction(f: &TruthExpression, out: &mut Vec<TruthExpression>) -> Result<()> {
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
            out.push(f.clone());
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
            Err(crate::error::DelightQLError::validation_error_categorized(
                "interior/topn/noneq_correlation",
                format!(
                    "interior top-N requires equality correlation: '{}' makes each outer row see a different candidate set, and the pre-ranked lowering would rank the wrong population",
                    spelled
                ),
                "join normally and rank explicitly: ... |> (..., row_number:(<~ %(outer identity), #(ordering)) as rnk), rnk <= N",
            ))
        }
        other => Err(crate::error::DelightQLError::validation_error_categorized(
            "interior/topn/noneq_correlation",
            format!(
                "interior top-N requires equality correlation, provable as a conjunction of equalities — this correlation contains {}",
                match other {
                    TruthExpression::Disjunction(_) => "an `or`",
                    TruthExpression::Not { .. } => "a `not`",
                    _ => "a predicate form the pre-ranked lowering cannot prove sound",
                }
            ),
            "join normally and rank explicitly: ... |> (..., row_number:(<~ %(outer identity), #(ordering)) as rnk), rnk <= N",
        )),
    }
}

/// Rewrite a (correlated, has-limit) subquery into a CDT-SJ-shaped subquery.
///
/// Walks the subquery, captures the limit value and order_by specs, removes
/// those nodes, and adds a window-projection pipe + rn-filter on top. If the
/// user's projection (when present) strips the correlation columns,
/// hygienic injection adds them back under synthetic names — those names are
/// what the window function's PARTITION BY references.
///
/// The caller builds a `CorrelatedScalarJoin` pattern directly with the
/// result, bypassing the recursive classifier path so the injection is not
/// re-run on a shape that no longer matches its trigger.
pub fn rewrite_window_join_subquery(
    subquery: Chain,
    correlation_filters: &[TruthExpression],
    identities: &crate::relation::Planning,
) -> Result<Chain> {
    let inner = subquery.semantic_relation();
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
    let mut flat_conjuncts: Vec<TruthExpression> = Vec::new();
    for f in correlation_filters {
        prove_equality_conjunction(f, &mut flat_conjuncts)?;
    }
    let correlation_filters: &[TruthExpression] = &flat_conjuncts;

    // The second half of the proof, BEFORE any rewriting: every proved
    // equality conjunct must contribute exactly one directly
    // representable partition key, or the whole rewrite refuses —
    // extraction that silently skips a conjunct emits an unpartitioned
    // (or under-partitioned) ranking, the same silent-wrong-answer
    // family the flattening guard above closes.
    let original_partition_columns = correlation_filters
        .iter()
        .map(|filter| super::correlation_analyzer::prove_partition_key(filter, inner, identities))
        .collect::<Result<Vec<crate::relation::PortId>>>()?;

    // The bounding step comes off whole: its cap and the ordering it consumed.
    let (subquery_no_order, limit_value, order_specs) = take_bound(subquery)?;

    // If the user's projection strips correlation columns, hygienic-inject
    // them so the window function (placed above the projection) can still
    // reference them via synthetic names.
    let injected_subquery = super::pattern_classifier::inject_hygienic_columns_if_needed(
        subquery_no_order,
        correlation_filters,
        identities,
    )?;

    // Map partition keys through hygienic injection: if a correlation
    // column was injected, the synthetic alias is used; otherwise the
    // original name (because no projection stripped it). The carriers are
    // read back off the subquery that now publishes them — the same question
    // the flattener asks later, answered the same way.
    let injection_lookup: std::collections::HashMap<
        crate::relation::PortId,
        crate::relation::PortId,
    > = super::pattern_classifier::correlation_carriers(&injected_subquery, identities)?
        .into_iter()
        .collect();

    let partition_by: Vec<DomainExpression> = original_partition_columns
        .iter()
        .map(|column| {
            DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence::engine(
                injection_lookup.get(column).copied().unwrap_or(*column),
            ))))
        })
        .collect();

    // Wrap with the embed: the operand's whole heading, then the row-number
    // witness standing at the port that same derivation minted for it.
    let authority = identities.authority();
    let (staged, published) = authority.bind(crate::relation::pending::Pending::WindowWitness {
        input: injected_subquery.semantic_relation(),
        partition: partition_by,
        ordering: order_specs,
    })?;
    let row_number_port = *published
        .last()
        .expect("the window projection appends one row-number port");
    let projected = authority.reland(injected_subquery, staged)?;

    // Wrap with WHERE __dql_rn <= N.
    let limited = wrap_with_rn_filter(projected, row_number_port, limit_value, identities)?;

    Ok(limited)
}

/// TAKE THE BOUND OFF THE SHAPING RUN, WITH THE ORDERING IT CONSUMED.
///
/// Returns the chain, the cap, and the ordering that cap consumed — read
/// off ONE node. An ordering carrying its bound is the membership act: the
/// window now performs it, ranking by exactly the specs that bound
/// consumed. The ordering's node STAYS and surrenders the bound: it
/// republishes its operand through the stage export, and everything above
/// it stands on the ports that export minted; the ORDER BY it still emits
/// is inert, the rank filter owns the selection. An arbitrary bound comes
/// out alone and the window orders nothing: `row_number()` with no ORDER
/// BY is legal SQL and ranks arbitrarily within each partition, exactly
/// the members the law lets an unordered bound choose. A loose ordering
/// standing elsewhere in the run is not this bound's and is left alone.
///
/// The scan covers the shaping run above the relation and DELIBERATELY
/// does not descend a member's chain, a bag arm, or a condition's subquery
/// — the boundary is where the shaping stops.
fn take_bound(expr: Chain) -> Result<(Chain, i64, Vec<OrderingSpec>)> {
    use crate::error::DelightQLError;

    let mut chain = expr;
    for index in (0..chain.continuations().len()).rev() {
        match chain.continuations()[index].form() {
            resolved::Continuation::Bound {
                bound:
                    TupleOrdinalClause {
                        operator: TupleOrdinalOperator::LessThan,
                        value,
                        offset: _,
                    },
                ..
            } => {
                let value = *value;
                return Ok((chain.without(index)?, value, Vec::new()));
            }
            resolved::Continuation::Structural(resolved::StructuralStep {
                form:
                    resolved::StructuralForm::Ordering {
                        specs,
                        bound:
                            Some(TupleOrdinalClause {
                                operator: TupleOrdinalOperator::LessThan,
                                ..
                            }),
                    },
                ..
            }) => {
                let specs = specs.clone();
                let bound = chain
                    .surrender_bound(index)
                    .expect("the ordering just matched carries its bound");
                return Ok((chain, bound.value, specs));
            }
            resolved::Continuation::Restrict { .. }
            | resolved::Continuation::Bound { .. }
            | resolved::Continuation::Pipe { .. }
            | resolved::Continuation::Structural(_) => {}
            _ => break,
        }
    }

    Err(DelightQLError::ParseError {
        message: "rewrite_window_join_subquery: expected a row bound but found none".to_string(),
        source: None,
        subcategory: None,
    })
}

/// Wrap an expression with a filter over the generated row-number occurrence.
fn wrap_with_rn_filter(
    source: Chain,
    row_number_column: crate::relation::PortId,
    limit: i64,
    _identities: &crate::relation::Planning,
) -> Result<Chain> {
    let rn_lvar = DomainExpression::Reference(Reference::Named(NamedReference(
        ColumnOccurrence::engine(row_number_column),
    )));
    let limit_literal = DomainExpression::Application(FunctionApplication::Ground(
        LiteralValue::Number(limit.to_string()),
    ));

    let comparison = TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::LessThanOrEqual,
        left: Box::new(rn_lvar),
        right: Box::new(limit_literal),
    });

    Ok(source.transparently(resolved::Transparent::Restrict {
        condition: comparison,
        origin: FilterOrigin::Generated,
    }))
}
