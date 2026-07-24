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
// This implements the Fork-1 path of LIMIT-PLACEMENT-PLAN.md: refined AST
// is descriptive, not prescriptive (P0').
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
use crate::pipeline::asts::core::ProjectionExpr;
use crate::pipeline::asts::resolved::{
    self, BooleanExpression, ContainmentSemantic, CprSchema, DomainExpression, FilterOrigin,
    FunctionExpression, LiteralValue, NamespacePath, PhaseBox, RelationalExpression,
    SigmaCondition, TupleOrdinalClause, TupleOrdinalOperator, UnaryRelationalOperator,
};
use delightql_types::SqlIdentifier;

type OrderingSpec = crate::pipeline::asts::core::OrderingSpec<resolved::Resolved>;

/// Synthetic column name for the row_number() window output.
const RN_COLUMN: &str = "__dql_rn";

/// Flatten one correlation filter through `and` into its conjunct
/// comparisons, PROVING each conjunct is an equality. Anything not
/// provable refuses: non-equality comparisons (each outer row would see
/// a different candidate set), `or`/`not` (no single child group per
/// outer row), and every unrecognized predicate form. The flattened
/// list is what partition-key extraction consumes, so compound
/// equalities partition identically to comma-separated ones.
fn prove_equality_conjunction(
    f: &BooleanExpression,
    out: &mut Vec<BooleanExpression>,
) -> Result<()> {
    match f {
        BooleanExpression::And { left, right } => {
            prove_equality_conjunction(left, out)?;
            prove_equality_conjunction(right, out)
        }
        BooleanExpression::Comparison { operator, .. }
            if matches!(
                operator.as_str(),
                "null_safe_eq" | "traditional_eq" | "eq" | "="
            ) =>
        {
            out.push(f.clone());
            Ok(())
        }
        BooleanExpression::Comparison { operator, .. } => {
            let spelled = match operator.as_str() {
                "less_than" => "<",
                "less_than_eq" => "<=",
                "greater_than" => ">",
                "greater_than_eq" => ">=",
                "traditional_ne" | "null_safe_ne" => "!=",
                other => other,
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
                    BooleanExpression::Or { .. } => "an `or`",
                    BooleanExpression::Not { .. } => "a `not`",
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
/// Returns (rewritten_subquery, hygienic_injections). The caller builds a
/// `CorrelatedScalarJoin` pattern directly with these — bypassing the
/// recursive classifier path so injections survive.
pub fn rewrite_window_join_subquery(
    subquery: RelationalExpression,
    correlation_filters: &[BooleanExpression],
    table_identifier: &resolved::QualifiedName,
) -> Result<(RelationalExpression, Vec<(String, String)>)> {
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
    let mut flat_conjuncts: Vec<BooleanExpression> = Vec::new();
    for f in correlation_filters {
        prove_equality_conjunction(f, &mut flat_conjuncts)?;
    }
    let correlation_filters: &[BooleanExpression] = &flat_conjuncts;

    // The second half of the proof, BEFORE any rewriting: every proved
    // equality conjunct must contribute exactly one directly
    // representable partition key, or the whole rewrite refuses —
    // extraction that silently skips a conjunct emits an unpartitioned
    // (or under-partitioned) ranking, the same silent-wrong-answer
    // family the flattening guard above closes.
    let original_partition_columns = correlation_filters
        .iter()
        .map(|f| super::correlation_analyzer::prove_partition_key(f, table_identifier))
        .collect::<Result<Vec<String>>>()?;

    // Capture limit value and the inner expression (without the TupleOrdinal filter).
    let (subquery_no_limit, limit_value) = strip_limit(subquery)?;

    // Capture order_by specs and the deeper expression (without the TupleOrdering pipe).
    let (subquery_no_order, order_specs) = strip_order_by(subquery_no_limit);

    // If the user's projection strips correlation columns, hygienic-inject
    // them so the window function (placed above the projection) can still
    // reference them via synthetic names.
    let (injected_subquery, injections) = super::pattern_classifier::inject_hygienic_columns_if_needed(
        subquery_no_order,
        correlation_filters,
        table_identifier,
    )?;

    // Map partition keys through hygienic injection: if a correlation
    // column was injected, the synthetic alias is used; otherwise the
    // original name (because no projection stripped it).
    let injection_lookup: std::collections::HashMap<&str, &str> = injections
        .iter()
        .map(|(orig, hyg)| (orig.as_str(), hyg.as_str()))
        .collect();

    let partition_by: Vec<DomainExpression> = original_partition_columns
        .iter()
        .map(|name| {
            let resolved_name = injection_lookup
                .get(name.as_str())
                .copied()
                .unwrap_or(name.as_str());
            make_lvar(resolved_name)
        })
        .collect();

    // Build the window function expression: row_number() OVER (PARTITION BY ... ORDER BY ...) AS __dql_rn.
    let window_expr = DomainExpression::Function(FunctionExpression::Window {
        name: SqlIdentifier::from("row_number"),
        arguments: vec![],
        partition_by,
        order_by: order_specs,
        frame: None,
        alias: Some(SqlIdentifier::from(RN_COLUMN)),
    });

    // Wrap with a General projection: (*, row_number(...) as __dql_rn).
    let projected = wrap_with_projection(injected_subquery, window_expr);

    // Wrap with WHERE __dql_rn <= N.
    let limited = wrap_with_rn_filter(projected, limit_value);

    Ok((limited, injections))
}

/// Walk the expression to find the (single) TupleOrdinal limit filter,
/// return (expression with that filter removed, captured limit value).
fn strip_limit(expr: RelationalExpression) -> Result<(RelationalExpression, i64)> {
    use crate::error::DelightQLError;

    fn walk(expr: RelationalExpression) -> (RelationalExpression, Option<i64>) {
        match expr {
            RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            } => {
                if let SigmaCondition::TupleOrdinal(TupleOrdinalClause {
                    operator: TupleOrdinalOperator::LessThan,
                    value,
                    offset: _,
                }) = &condition
                {
                    let captured = *value;
                    // Drop this filter; recurse to keep walking inner.
                    return (*source, Some(captured));
                }
                let (inner, found) = walk(*source);
                (
                    RelationalExpression::Filter {
                        source: Box::new(inner),
                        condition,
                        origin,
                        cpr_schema,
                    },
                    found,
                )
            }
            RelationalExpression::Pipe(pipe) => {
                let pipe = pipe.into_inner();
                let (inner_source, found) = walk(pipe.source);
                (
                    RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                        resolved::PipeExpression {
                            source: inner_source,
                            operator: pipe.operator,
                            cpr_schema: pipe.cpr_schema,
                        },
                    ))),
                    found,
                )
            }
            // STOP at the WHOLE node: limits live in the linear Filter/Pipe chain
            // above the Join/Relation/SetOperation/IntersectCorresponding/ER, so
            // those are returned WHOLESALE — we deliberately do NOT descend a Join
            // arm or a SetOperation operand. Returning the whole node drops no
            // recursive field (the node IS the boundary), so this catch-all is
            // R-I3-safe; the boundary is stated by this comment. (The Filter arm
            // above preserves `condition` wholesale — it peels the limit off the
            // spine without recursing into the condition, the base-spine contract.)
            other => (other, None),
        }
    }

    let (stripped, found) = walk(expr);
    let value = found.ok_or_else(|| DelightQLError::ParseError {
        message: "rewrite_window_join_subquery: expected TupleOrdinal limit but found none"
            .to_string(),
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
fn strip_order_by(expr: RelationalExpression) -> (RelationalExpression, Vec<OrderingSpec>) {
    fn walk(
        expr: RelationalExpression,
    ) -> (RelationalExpression, Option<Vec<OrderingSpec>>) {
        match expr {
            RelationalExpression::Pipe(pipe) => {
                let pipe = pipe.into_inner();
                if let UnaryRelationalOperator::TupleOrdering { specs, .. } = &pipe.operator {
                    let captured = specs.clone();
                    // Drop this pipe; continue walking inner for completeness.
                    let (inner, _) = walk(pipe.source);
                    return (inner, Some(captured));
                }
                let (inner_source, found) = walk(pipe.source);
                (
                    RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(
                        resolved::PipeExpression {
                            source: inner_source,
                            operator: pipe.operator,
                            cpr_schema: pipe.cpr_schema,
                        },
                    ))),
                    found,
                )
            }
            RelationalExpression::Filter {
                source,
                condition,
                origin,
                cpr_schema,
            } => {
                let (inner, found) = walk(*source);
                (
                    RelationalExpression::Filter {
                        source: Box::new(inner),
                        condition,
                        origin,
                        cpr_schema,
                    },
                    found,
                )
            }
            other => (other, None),
        }
    }

    let (stripped, found) = walk(expr);
    (stripped, found.unwrap_or_default())
}

/// Wrap an expression with a General projection that adds `Glob, window_expr`.
fn wrap_with_projection(
    source: RelationalExpression,
    window_expr: DomainExpression,
) -> RelationalExpression {
    let glob = DomainExpression::Projection(ProjectionExpr::Glob {
        qualifier: None,
        namespace_path: NamespacePath::empty(),
    });

    // Carry the source's cpr_schema as the operator's schema. The FAR cycle
    // recomputes schemas during rebuild, so this is a placeholder.
    let source_schema = extract_cpr_box(&source);

    let pipe = resolved::PipeExpression {
        source,
        operator: UnaryRelationalOperator::General {
            containment_semantic: ContainmentSemantic::Parenthesis,
            expressions: vec![glob, window_expr],
        },
        cpr_schema: source_schema,
    };
    RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(pipe)))
}

/// Wrap an expression with a Filter `__dql_rn <= N`.
fn wrap_with_rn_filter(source: RelationalExpression, limit: i64) -> RelationalExpression {
    let rn_lvar = make_lvar(RN_COLUMN);
    let limit_literal = DomainExpression::Literal {
        value: LiteralValue::Number(limit.to_string()),
        alias: None,
    };

    let comparison = BooleanExpression::Comparison {
        operator: "<=".to_string(),
        left: Box::new(rn_lvar),
        right: Box::new(limit_literal),
    };

    let source_schema = extract_cpr_box(&source);

    RelationalExpression::Filter {
        source: Box::new(source),
        condition: SigmaCondition::Predicate(comparison),
        origin: FilterOrigin::Generated,
        cpr_schema: source_schema,
    }
}

/// Build an unqualified Lvar referring to a synthetic column.
fn make_lvar(name: &str) -> DomainExpression {
    DomainExpression::Lvar {
        name: SqlIdentifier::from(name),
        qualifier: None,
        namespace_path: NamespacePath::empty(),
        alias: None,
        provenance: PhaseBox::phantom(),
    }
}

/// Extract the cpr_schema PhaseBox from a relational expression, cloning it
/// so we can reuse it as a placeholder for newly constructed wrapping nodes.
/// FAR rebuild recomputes schemas, so any reasonable carrier is fine.
fn extract_cpr_box(expr: &RelationalExpression) -> PhaseBox<CprSchema, resolved::Resolved> {
    match expr {
        RelationalExpression::Relation(rel) => match rel {
            resolved::Relation::Ground { cpr_schema, .. }
            | resolved::Relation::Anonymous { cpr_schema, .. }
            | resolved::Relation::TVF { cpr_schema, .. }
            | resolved::Relation::InnerRelation { cpr_schema, .. } => cpr_schema.clone(),
            resolved::Relation::ConsultedView { scoped, .. } => {
                PhaseBox::new(scoped.get().schema().clone())
            }
            resolved::Relation::PseudoPredicate { .. } => PhaseBox::new(CprSchema::Unknown),
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
