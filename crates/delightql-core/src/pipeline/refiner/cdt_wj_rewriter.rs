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
//   Filter(condition=TupleOrdinal(LessThan, N),
//     source=Pipe(operator=TupleOrdering[specs],
//       source=...inner correlation+filter+relation...))
//
// Output shape:
//
//   Filter(condition=Predicate(__dql_rn <= N),
//     source=Pipe(operator=General[Glob, Window(row_number, partition, order_by, alias=__dql_rn)],
//       source=...inner with TupleOrdering removed...))
//
// The TupleOrdering pipe is consumed into the window's ORDER BY.
// The TupleOrdinal filter is consumed into the rn comparison.

use crate::error::Result;
use crate::pipeline::asts::core::ColumnOccurrence;
use crate::pipeline::asts::core::Comparison;
use crate::pipeline::asts::core::OutValue;
use crate::pipeline::asts::core::{NamedReference, Reference};
use crate::pipeline::asts::resolved::{
    self, Chain, DomainExpression, FilterOrigin, FunctionApplication, LiteralValue, PipeOp,
    TruthExpression, TupleOrdinalClause, TupleOrdinalOperator,
};
use std::rc::Rc;

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
    identities: &Rc<crate::names::Registry>,
) -> Result<Chain> {
    let inner_scope = super::pattern_classifier::relational_scope(&subquery)?;
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
        .map(|filter| {
            super::correlation_analyzer::prove_partition_key(filter, inner_scope, identities)
        })
        .collect::<Result<Vec<crate::names::ColId>>>()?;

    // Capture limit value and the inner expression (without the TupleOrdinal filter).
    let (subquery_no_limit, limit_value) = strip_limit(subquery)?;

    // Capture order_by specs and the deeper expression (without the TupleOrdering pipe).
    let (subquery_no_order, order_specs) = strip_order_by(subquery_no_limit);

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
    let injection_lookup: std::collections::HashMap<crate::names::ColId, crate::names::ColId> =
        super::pattern_classifier::correlation_carriers(&injected_subquery, identities)?
            .into_iter()
            .collect();

    let partition_by: Vec<DomainExpression> = original_partition_columns
        .iter()
        .map(|column| {
            DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
                column: injection_lookup.get(column).copied().unwrap_or(*column),
                explicit_qualifier: false,
            })))
        })
        .collect();

    let row_number_scope = match injected_subquery.continuations.last() {
        Some(resolved::Continuation::Pipe { cpr_schema, .. }) => Some(*cpr_schema),
        _ => None,
    }
    .unwrap_or_else(|| {
        identities.mint_scope(
            crate::names::ScopeOrigin::AnonRelation,
            crate::names::Hint::None,
            None,
        )
    });
    let row_number_column = identities.mint_column(
        row_number_scope,
        crate::names::ColumnOrigin::Minted {
            by: crate::names::MintReason::RowNumber,
        },
        None,
        crate::names::Addressing::Hygienic,
        crate::names::ValueFacts::default(),
    );
    let window_item = resolved::OutItem::One(resolved::OneOut {
        expr: OutValue::Domain(DomainExpression::Application(
            FunctionApplication::Standard(crate::pipeline::asts::core::StandardApplication {
                call: crate::pipeline::asts::core::PureCall::from_inner(
                    crate::pipeline::asts::core::FunctorCall::<resolved::Resolved> {
                        callee: identities
                            .mint_function(identities.intern("row_number", false), Vec::new()),
                        arguments: crate::pipeline::asts::core::operators::CallArguments::Scalar(
                            Vec::new(),
                        ),
                        marks: Default::default(),
                    },
                ),
                guard: None,
                window: Some(crate::pipeline::asts::core::WindowSpec {
                    partition: partition_by,
                    ordering: order_specs,
                    frame: None,
                }),
            }),
        )),
        // A compiler-minted witness answers to no authored name.
        naming: None,
        output: Some(row_number_column),
    });

    // Wrap with a General projection: (*, row_number(...) as __dql_rn).
    let projected = wrap_with_projection(injected_subquery, window_item);

    // Wrap with WHERE __dql_rn <= N.
    let limited = wrap_with_rn_filter(projected, row_number_column, limit_value);

    Ok(limited)
}

/// Walk the expression to find the (single) TupleOrdinal limit filter,
/// return (expression with that filter removed, captured limit value).
fn strip_limit(expr: Chain) -> Result<(Chain, i64)> {
    use crate::error::DelightQLError;

    // The bound lives in the shaping run above the relation: this scans that
    // run and DELIBERATELY does not descend a member's chain, a bag arm, or a
    // condition's subquery — the boundary is where the shaping stops.
    let mut stripped = expr;
    let mut found = None;
    for index in (0..stripped.continuations.len()).rev() {
        match &stripped.continuations[index] {
            resolved::Continuation::Bound {
                bound:
                    TupleOrdinalClause {
                        operator: TupleOrdinalOperator::LessThan,
                        value,
                        offset: _,
                    },
                ..
            } => {
                found = Some(*value);
                stripped.continuations.remove(index);
                break;
            }
            resolved::Continuation::Restrict { .. }
            | resolved::Continuation::Bound { .. }
            | resolved::Continuation::Pipe { .. } => {}
            _ => break,
        }
    }

    let value = found.ok_or_else(|| DelightQLError::ParseError {
        message: "rewrite_window_join_subquery: expected a row bound but found none".to_string(),
        source: None,
        subcategory: None,
    })?;
    Ok((stripped, value))
}

/// Walk the expression to find the (first) TupleOrdering pipe, return
/// (expression with that pipe removed, captured specs). If no order_by
/// is present, returns the expression unchanged with an empty spec list —
/// row_number() with no ORDER BY is legal SQL though deterministic only
/// per-partition, mirroring DQL's "limit without order" semantics.
fn strip_order_by(expr: Chain) -> (Chain, Vec<OrderingSpec>) {
    let mut stripped = expr;
    let mut found: Option<Vec<OrderingSpec>> = None;
    for index in (0..stripped.continuations.len()).rev() {
        match &stripped.continuations[index] {
            resolved::Continuation::Structural(resolved::StructuralStep {
                form: resolved::StructuralForm::Ordering { specs },
                ..
            }) => {
                found = Some(specs.clone());
                stripped.continuations.remove(index);
                break;
            }
            resolved::Continuation::Restrict { .. }
            | resolved::Continuation::Pipe { .. }
            | resolved::Continuation::Structural(_) => {}
            _ => break,
        }
    }
    (stripped, found.unwrap_or_default())
}

/// Wrap an expression with a General projection carrying the whole operand
/// and then the window item.
///
/// A RESOLVED PROJECTION HOLDS NO AUTHORED SPREAD. What this needs is not
/// the expansion of a glob but the operand ITSELF: the subquery's hygienic
/// columns have to ride through, and a column named in a select list
/// cannot be hygienic. `Whole` is that meaning, and it is also why the
/// schema below can stay the placeholder the FAR cycle recomputes — there
/// is no heading to read here.
fn wrap_with_projection(source: Chain, window_item: resolved::OutItem) -> Chain {
    // Carry the source's cpr_schema as the operator's schema. The FAR cycle
    // recomputes schemas during rebuild, so this is a placeholder.
    let source_schema = crate::pipeline::resolver::helpers::extraction::extract_cpr_schema(&source);

    source.then(resolved::Continuation::Pipe {
        operator: PipeOp::Project(
            crate::pipeline::asts::vocabulary::Vec1::try_from_vec(vec![
                resolved::OutItem::Whole,
                window_item,
            ])
            .expect("the window projection carries the whole and the window item"),
        ),
        named: (),
        cpr_schema: source_schema,
    })
}

/// Wrap an expression with a filter over the generated row-number occurrence.
fn wrap_with_rn_filter(source: Chain, row_number_column: crate::names::ColId, limit: i64) -> Chain {
    let rn_lvar = DomainExpression::Reference(Reference::Named(NamedReference(ColumnOccurrence {
        column: row_number_column,
        explicit_qualifier: false,
    })));
    let limit_literal = DomainExpression::Application(FunctionApplication::Ground(
        LiteralValue::Number(limit.to_string()),
    ));

    let comparison = TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::LessThanOrEqual,
        left: Box::new(rn_lvar),
        right: Box::new(limit_literal),
    });

    let source_schema = crate::pipeline::resolver::helpers::extraction::extract_cpr_schema(&source);

    source.then(resolved::Continuation::Restrict {
        condition: comparison,
        origin: FilterOrigin::Generated,
        cpr_schema: source_schema,
    })
}
