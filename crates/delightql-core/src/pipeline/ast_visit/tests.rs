// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Unit tests for the `AstVisit` closure and its two control axes.
//!
//! These are the red-first pins for the new traversal infrastructure
//! (INDUCTIVE-TRAVERSAL-PLAN §5 Phases B and E; house rule "a comment claiming a
//! guarantee names its test"):
//! - `r_i4_recursion_closure_matrix` — **THE R-I4 recursion-closure matrix**
//!   (PLAN §5 Phase E; R-I4). It grew from the Phase-B seed
//!   (`visit_reaches_every_query_bearing_edge`) into the comprehensive matrix
//!   the plan calls for: a distinct sentinel beneath EVERY query-bearing edge
//!   INVENTORY §5 enumerates — including EACH Pipe-operator argument form
//!   (General expressions, AggregatePipe aggregations, Transform transformations
//!   AND its `conditioned_on`, MapCover `conditioned_on`, Modulo GroupBy, and a
//!   `DirectivePipeInvocation` relational argument), reached by the default
//!   full-descend walk. It is the standing structural proof that "no recursive
//!   field is silently dropped": dropping the recursive edge that uniquely
//!   reaches a given sentinel fails that sentinel's assertion (drop-an-edge
//!   spot-checked against `Filter.condition` and `General.expressions`). This
//!   COMPLEMENTS — does not replace — the Phase-C `p1_closure_matrix` (which
//!   proves the two P1 schemes coincide on one fixture); this one proves the
//!   `AstVisit` default walk itself is complete.
//! - `visit_reaches_setoperation_correlation` — the matrix's correlation arm:
//!   the one edge the cross-phase transform must phantom (§5 finding 9), reached
//!   via `PhaseBox::correlation`. It lives in its own test because correlation
//!   is only populatable at Refined, the phase this arm therefore pins.
//! - `skip_subtree_prunes_exactly_its_subtree` — boundary axis.
//! - `break_stops_the_walk_promptly` — early termination (§5 finding 12).
//! - `err_hook_short_circuits_the_walk` — error propagation (§5 finding 13).

use super::*;
use crate::error::DelightQLError;
use crate::pipeline::asts::core::expressions::helpers::QualifiedName;
use crate::pipeline::asts::core::expressions::metadata_types::{FilterOrigin, SetOperator};
use crate::pipeline::asts::core::metadata::NamespacePath;
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::{
    BooleanExpression, CteBinding, DomainExpression, FunctionExpression, PhaseBox, PipeExpression,
    Query, Refined, Relation, RelationalExpression, SigmaCondition, UnaryRelationalOperator,
    Unresolved,
};
use crate::pipeline::asts::core::DomainSpec;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::specs::{ContainmentSemantic, ModuloSpec, OutputDomainExpression};
use delightql_types::SqlIdentifier;

// Additional carriers exercised by the RED-4 extended matrix.
use crate::pipeline::asts::core::operators::{ColumnSelector, DmlKind};
use crate::pipeline::asts::core::specs::{DelegateSpec, OrderingSpec, RenameSpec, RenameTarget, RepositionSpec};
use crate::pipeline::asts::core::expressions::functions::{ArrayMember, CaseArm, CurlyMember, StringTemplatePart};
use crate::pipeline::asts::core::expressions::metadata_types::{CteRequirements, TreeGroupLocation};

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

fn qn(name: &str) -> QualifiedName {
    QualifiedName {
        namespace_path: NamespacePath::empty(),
        name: name.into(),
        grounding: None,
    }
}

/// A recognizable "directive" sentinel: a `PseudoPredicate` whose `name` is the
/// tag. Phase-generic (`PseudoPredicate` and `PhaseBox::phantom` are both
/// phase-generic), so it seeds both the Unresolved closure tree and the Refined
/// SetOperation-correlation tree.
fn sentinel<P>(tag: &str) -> RelationalExpression<P> {
    RelationalExpression::Relation(Relation::PseudoPredicate {
        name: tag.to_string(),
        namespace: Vec::new(),
        access: DomainSpec::Glob,
        arguments: vec![],
        alias: None,
        cpr_schema: PhaseBox::phantom(),
    })
}

/// Collects the tags of every `PseudoPredicate` sentinel the walk enters.
#[derive(Default)]
struct SentinelCollector {
    seen: Vec<String>,
}

impl<P> AstVisit<P> for SentinelCollector {
    fn enter_relation(&mut self, r: &Relation<P>) -> Result<Descent> {
        if let Relation::PseudoPredicate { name, .. } = r {
            self.seen.push(name.clone());
        }
        Ok(Descent::Continue)
    }
}

fn chain(exprs: Vec<RelationalExpression<Unresolved>>) -> RelationalExpression<Unresolved> {
    exprs
        .into_iter()
        .reduce(|left, right| RelationalExpression::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_condition: None,
            join_type: None,
            cpr_schema: PhaseBox::phantom(),
        })
        .expect("at least one carrier")
}

/// A scalar subquery whose subquery IS a `tag` sentinel: the query-bearing
/// `DomainExpression::ScalarSubquery` edge, used to smuggle a nested relation
/// beneath any domain-valued operator argument.
fn scalar_sub(tag: &str) -> DomainExpression<Unresolved> {
    DomainExpression::ScalarSubquery {
        identifier: qn(tag),
        subquery: Box::new(sentinel(tag)),
        alias: None,
    }
}

/// An `EXISTS` whose subquery IS a `tag` sentinel: the query-bearing
/// `BooleanExpression::InnerExists` edge, used to smuggle a nested relation
/// beneath any boolean-valued operator argument (`conditioned_on`).
fn inner_exists(tag: &str) -> BooleanExpression<Unresolved> {
    BooleanExpression::InnerExists {
        exists: true,
        identifier: qn(tag),
        subquery: Box::new(sentinel(tag)),
        alias: None,
        using_columns: vec![],
    }
}

/// A one-operator pipe: `sentinel(source_tag) |> operator`. The source sentinel
/// pins the `Pipe.source` edge; `operator` pins whichever operator-argument edge
/// the caller wired a sentinel beneath.
fn pipe_with(
    source_tag: &str,
    operator: UnaryRelationalOperator<Unresolved>,
) -> RelationalExpression<Unresolved> {
    RelationalExpression::Pipe(Box::new(stacksafe::StackSafe::new(PipeExpression {
        source: sentinel(source_tag),
        operator,
        cpr_schema: PhaseBox::phantom(),
    })))
}

// ---------------------------------------------------------------------------
// The R-I4 closure seed
// ---------------------------------------------------------------------------

/// THE R-I4 recursion-closure matrix (PLAN §5 Phase E; R-I4).
///
/// The default full-descend walk reaches a distinct sentinel beneath EVERY
/// query-bearing edge INVENTORY §5 enumerates — Filter.condition, join_condition,
/// EACH Pipe-operator argument form, WithCtes body, ConsultedView body, CFE body,
/// HO table argument, and InnerRelation subquery. (SetOperation.correlation is
/// the matrix's Refined-phase arm, `visit_reaches_setoperation_correlation`.)
/// Each sentinel sits beneath a structurally-unique carrier, so dropping the
/// recursive edge that uniquely reaches a sentinel fails that sentinel's
/// assertion — the standing structural proof that "no recursive field is
/// silently dropped." Spot-checked load-bearing by deleting the `Filter →
/// condition` and the `General.expressions` descent (each fails exactly its own
/// assertion, nothing else). This generalizes the anti-P1/P2 (dropped
/// `Filter.condition`) guarantee to the whole grammar; it complements the
/// Phase-C `p1_closure_matrix` (which proves the two P1 recursion schemes
/// coincide) by proving the shared `AstVisit` walk is itself complete.
#[test]
fn r_i4_recursion_closure_matrix() {
    // Finding 2 — Filter.condition (headline edge; the P1/P2 hole).
    let filter_condition = RelationalExpression::Filter {
        source: Box::new(sentinel("filter_source")),
        condition: SigmaCondition::Predicate(BooleanExpression::InRelational {
            value: Box::new(DomainExpression::NonUnifiyingUnderscore),
            subquery: Box::new(sentinel("filter_condition")),
            identifier: qn("f"),
            negated: false,
        }),
        origin: FilterOrigin::UserWritten,
        cpr_schema: PhaseBox::phantom(),
    };

    // Finding 3 — Join.join_condition (via InnerExists subquery).
    let join_condition = RelationalExpression::Join {
        left: Box::new(sentinel("join_left")),
        right: Box::new(sentinel("join_right")),
        join_condition: Some(inner_exists("join_condition")),
        join_type: None,
        cpr_schema: PhaseBox::phantom(),
    };

    // Finding 4 — EACH Pipe-operator argument form carries a query-bearing edge.
    // A single operator arm was the seed's only probe; the matrix probes every
    // arm through which a nested relation (subquery / directive) can hide.
    //
    // (a) Transform transformations: a scalar subquery in a `$$` expression (the
    //     edge missed by ALL relational-entry whole-tree walkers today).
    let op_transform_transformation = pipe_with(
        "op_transform_source",
        UnaryRelationalOperator::Transform {
            transformations: vec![(
                scalar_sub("op_transform_transformation"),
                "a".to_string(),
                None,
            )],
            conditioned_on: None,
        },
    );
    // (b) Transform conditioned_on: an EXISTS guarding a `$$` cover.
    let op_transform_conditioned = pipe_with(
        "op_transform_cond_source",
        UnaryRelationalOperator::Transform {
            transformations: vec![],
            conditioned_on: Some(Box::new(inner_exists("op_transform_conditioned_on"))),
        },
    );
    // (c) General projection expressions: a scalar subquery in `(...)` / `[...]`.
    let op_general = pipe_with(
        "op_general_source",
        UnaryRelationalOperator::General {
            containment_semantic: ContainmentSemantic::Parenthesis,
            expressions: vec![scalar_sub("op_general_expr")],
        },
    );
    // (d) AggregatePipe aggregations: a scalar subquery in a `|~>` aggregation.
    let op_aggregate = pipe_with(
        "op_aggregate_source",
        UnaryRelationalOperator::AggregatePipe {
            aggregations: vec![scalar_sub("op_aggregate_expr")],
        },
    );
    // (e) MapCover conditioned_on: an EXISTS guarding a `$` map cover — a DISTINCT
    //     operator arm from Transform's conditioned_on (a dropped edge on either
    //     arm must be caught independently).
    let op_mapcover = pipe_with(
        "op_mapcover_source",
        UnaryRelationalOperator::MapCover {
            function: FunctionExpression::Bracket {
                arguments: vec![],
                alias: None,
            },
            columns: vec![],
            containment_semantic: ContainmentSemantic::Parenthesis,
            conditioned_on: Some(Box::new(inner_exists("op_mapcover_conditioned_on"))),
        },
    );
    // (f) Modulo GroupBy reducing_on: a scalar subquery in a `%(...)` group — the
    //     descent path the now-deleted `_tg_N` tree-group walk (W8) took.
    let op_modulo = pipe_with(
        "op_modulo_source",
        UnaryRelationalOperator::Modulo {
            containment_semantic: ContainmentSemantic::Parenthesis,
            spec: ModuloSpec::GroupBy {
                reducing_by: vec![],
                reducing_on: vec![OutputDomainExpression {
                    expr: scalar_sub("op_modulo_reducing_on"),
                    output: PhaseBox::phantom(),
                }],
                delegates: vec![],
            },
        },
    );
    // (g) DirectivePipeInvocation argument: a directive's relational HO argument
    //     (a query-bearing edge that returns straight to relational syntax).
    let op_directive = pipe_with(
        "op_directive_source",
        UnaryRelationalOperator::DirectivePipeInvocation {
            name: "d!".to_string(),
            argument: Box::new(sentinel("op_directive_pipe_arg")),
            domain_spec: DomainSpec::Glob,
        },
    );

    // Finding 8 — a higher-order table argument on a TVF.
    let ho_tvf = RelationalExpression::Relation(Relation::TVF {
        function: "tvf".into(),
        argument_groups: None,
        ho_arguments: vec![HoArgument::Table(sentinel("ho_argument"))],
        domain_spec: DomainSpec::Glob,
        alias: None,
        namespace: None,
        backend_schema: PhaseBox::phantom(),
        grounding: None,
        cpr_schema: PhaseBox::phantom(),
    });

    // Finding 10 — an InnerRelation subquery.
    let inner_relation = RelationalExpression::Relation(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier: qn("i"),
            subquery: Box::new(sentinel("inner_relation_subquery")),
        },
        alias: None,
        outer: false,
        cpr_schema: PhaseBox::phantom(),
    });

    // Finding 6 — a consulted-view body (a full Query).
    let consulted_view = RelationalExpression::Relation(Relation::ConsultedView {
        identifier: qn("v"),
        body: Box::new(Query::Relational(sentinel("consulted_view_body"))),
        scoped: PhaseBox::phantom(),
        outer: false,
    });

    let body = chain(vec![
        filter_condition,
        join_condition,
        op_transform_transformation,
        op_transform_conditioned,
        op_general,
        op_aggregate,
        op_mapcover,
        op_modulo,
        op_directive,
        ho_tvf,
        inner_relation,
        consulted_view,
    ]);

    // Finding 5 — a WithCtes body (a CteBinding expression).
    let query = Query::WithCtes {
        ctes: vec![CteBinding {
            expression: sentinel("cte_body"),
            name: "c".to_string(),
            origin: Default::default(),
            resolution_owner: Default::default(),
            effect_label: false,
            is_recursive: PhaseBox::phantom(),
        }],
        query: body,
    };

    let mut c = SentinelCollector::default();
    let flow = walk_visit_query(&mut c, &query).expect("walk ok");
    assert_eq!(flow, Descent::Continue);

    for expected in [
        // Filter.condition / join_condition
        "filter_condition",
        "join_condition",
        // each Pipe-operator argument form (finding 4)
        "op_transform_transformation",
        "op_transform_conditioned_on",
        "op_general_expr",
        "op_aggregate_expr",
        "op_mapcover_conditioned_on",
        "op_modulo_reducing_on",
        "op_directive_pipe_arg",
        // structural boundaries
        "ho_argument",
        "inner_relation_subquery",
        "consulted_view_body",
        "cte_body",
    ] {
        assert!(
            c.seen.iter().any(|s| s == expected),
            "closure walk did not reach sentinel `{expected}`; reached: {:?}",
            c.seen
        );
    }

    // CFE bodies are reached by ROOTING a fresh visit at the body (the W12
    // pattern), because `WithCfes.cfes` is a non-phase-parameterized side
    // structure the generic walk cannot descend. Prove the rooted-at-domain
    // entry reaches a sentinel beneath it.
    let cfe_body: DomainExpression<Unresolved> = DomainExpression::ScalarSubquery {
        identifier: qn("c"),
        subquery: Box::new(sentinel("cfe_body")),
        alias: None,
    };
    let mut cfe = SentinelCollector::default();
    walk_visit_domain(&mut cfe, &cfe_body).expect("walk ok");
    assert!(
        cfe.seen.iter().any(|s| s == "cfe_body"),
        "rooted CFE-body visit did not reach its sentinel; reached: {:?}",
        cfe.seen
    );
}

/// The `SetOperation.correlation` edge — phantomed by the cross-phase transform,
/// reached by the same-phase visit (§5 finding 9). Correlation is only
/// populatable at Refined (`PhaseBox::with_correlation`), so this pins at Refined.
#[test]
fn visit_reaches_setoperation_correlation() {
    let correlation = BooleanExpression::<Refined>::InnerExists {
        exists: true,
        identifier: qn("q"),
        subquery: Box::new(sentinel::<Refined>("setop_correlation")),
        alias: None,
        using_columns: vec![],
    };
    let setop = RelationalExpression::<Refined>::SetOperation {
        operator: SetOperator::SmartUnionAll,
        operands: vec![
            sentinel::<Refined>("setop_operand_a"),
            sentinel::<Refined>("setop_operand_b"),
        ],
        correlation: <PhaseBox<Option<BooleanExpression<Refined>>, Refined>>::with_correlation(
            Some(correlation),
        ),
        cpr_schema: PhaseBox::phantom(),
    };

    let mut c = SentinelCollector::default();
    walk_visit_relational(&mut c, &setop).expect("walk ok");

    assert!(
        c.seen.iter().any(|s| s == "setop_correlation"),
        "visit did not reach SetOperation.correlation; reached: {:?}",
        c.seen
    );
    // Operands are reached too (sanity).
    assert!(c.seen.iter().any(|s| s == "setop_operand_a"));
    assert!(c.seen.iter().any(|s| s == "setop_operand_b"));
}

// ---------------------------------------------------------------------------
// RED-4 (F4): the R-I4 matrix under-covers its stated guarantee
// ---------------------------------------------------------------------------

/// An `OutputDomainExpression` whose expr is a `tag` scalar-subquery sentinel.
fn out_dom(tag: &str) -> OutputDomainExpression<Unresolved> {
    OutputDomainExpression {
        expr: scalar_sub(tag),
        output: PhaseBox::phantom(),
    }
}

/// Wrap a `FunctionExpression` as a `DomainExpression` so it can ride a
/// `General` projection's `expressions` vec.
fn func_dom(f: FunctionExpression<Unresolved>) -> DomainExpression<Unresolved> {
    DomainExpression::Function(f)
}

/// A `General` projection pipe carrying one function-valued expression: the
/// smuggling vector for every FunctionExpression container carrier.
fn general_with_func(source_tag: &str, f: FunctionExpression<Unresolved>) -> RelationalExpression<Unresolved> {
    pipe_with(
        source_tag,
        UnaryRelationalOperator::General {
            containment_semantic: ContainmentSemantic::Parenthesis,
            expressions: vec![func_dom(f)],
        },
    )
}

/// THE R-I4 matrix EXTENSION (RED-4; review pnxwskxr::nzpzwxqx [P2]).
///
/// `r_i4_recursion_closure_matrix` claims to cover "EACH Pipe-operator argument
/// form" and generalizes to "the whole grammar," but the review enumerates ~a
/// dozen independent query-bearing carriers it never probes. This test plants a
/// distinct sentinel beneath EACH — every operator-argument carrier the review
/// lists, plus the function/container fields it flags as un-pinned. Any sentinel
/// the default `AstVisit` walk does NOT reach is a real closure hole and reds
/// its assertion; the rest close the coverage gap so the "no recursive field is
/// silently dropped" guarantee is actually earned.
#[test]
fn r_i4_recursion_closure_matrix_extended() {
    let mut carriers: Vec<RelationalExpression<Unresolved>> = Vec::new();

    // --- Operator-argument carriers the review lists as MISSING ---------------

    // ProjectOut.expressions — `-(...)`
    carriers.push(pipe_with(
        "projectout_src",
        UnaryRelationalOperator::ProjectOut {
            containment_semantic: ContainmentSemantic::Parenthesis,
            expressions: vec![scalar_sub("projectout_expressions")],
        },
    ));

    // TupleOrdering.specs — `#(...)`
    carriers.push(pipe_with(
        "tupleordering_src",
        UnaryRelationalOperator::TupleOrdering {
            containment_semantic: ContainmentSemantic::Parenthesis,
            specs: vec![OrderingSpec {
                column: scalar_sub("tupleordering_specs"),
                direction: None,
            }],
        },
    ));

    // MapCover.function
    carriers.push(pipe_with(
        "mapcover_fn_src",
        UnaryRelationalOperator::MapCover {
            function: FunctionExpression::Bracket {
                arguments: vec![scalar_sub("mapcover_function")],
                alias: None,
            },
            columns: vec![],
            containment_semantic: ContainmentSemantic::Parenthesis,
            conditioned_on: None,
        },
    ));
    // MapCover.columns
    carriers.push(pipe_with(
        "mapcover_cols_src",
        UnaryRelationalOperator::MapCover {
            function: FunctionExpression::Bracket {
                arguments: vec![],
                alias: None,
            },
            columns: vec![scalar_sub("mapcover_columns")],
            containment_semantic: ContainmentSemantic::Parenthesis,
            conditioned_on: None,
        },
    ));

    // Modulo GroupBy.reducing_by
    carriers.push(pipe_with(
        "modulo_rb_src",
        UnaryRelationalOperator::Modulo {
            containment_semantic: ContainmentSemantic::Parenthesis,
            spec: ModuloSpec::GroupBy {
                reducing_by: vec![out_dom("modulo_reducing_by")],
                reducing_on: vec![],
                delegates: vec![],
            },
        },
    ));
    // Modulo GroupBy.delegates
    carriers.push(pipe_with(
        "modulo_del_src",
        UnaryRelationalOperator::Modulo {
            containment_semantic: ContainmentSemantic::Parenthesis,
            spec: ModuloSpec::GroupBy {
                reducing_by: vec![],
                reducing_on: vec![],
                delegates: vec![DelegateSpec {
                    payload: vec![out_dom("modulo_delegates")],
                    order: vec![],
                }],
            },
        },
    ));
    // Modulo Columns (the simple reducing_by list, a distinct ModuloSpec arm)
    carriers.push(pipe_with(
        "modulo_cols_src",
        UnaryRelationalOperator::Modulo {
            containment_semantic: ContainmentSemantic::Parenthesis,
            spec: ModuloSpec::Columns(vec![scalar_sub("modulo_columns")]),
        },
    ));

    // RenameCover.specs — `*(...)`
    carriers.push(pipe_with(
        "renamecover_src",
        UnaryRelationalOperator::RenameCover {
            specs: vec![RenameSpec {
                from: scalar_sub("renamecover_specs"),
                to: RenameTarget::Literal("x".to_string()),
            }],
        },
    ));

    // Reposition.moves — `|col as n|`
    carriers.push(pipe_with(
        "reposition_src",
        UnaryRelationalOperator::Reposition {
            moves: vec![RepositionSpec {
                column: scalar_sub("reposition_moves"),
                position: 0,
            }],
        },
    ));

    // EmbedMapCover.function
    carriers.push(pipe_with(
        "embed_fn_src",
        UnaryRelationalOperator::EmbedMapCover {
            function: FunctionExpression::Bracket {
                arguments: vec![scalar_sub("embedmapcover_function")],
                alias: None,
            },
            selector: ColumnSelector::All,
            alias_template: None,
            containment_semantic: ContainmentSemantic::Parenthesis,
        },
    ));
    // EmbedMapCover.selector
    carriers.push(pipe_with(
        "embed_sel_src",
        UnaryRelationalOperator::EmbedMapCover {
            function: FunctionExpression::Bracket {
                arguments: vec![],
                alias: None,
            },
            selector: ColumnSelector::Explicit(vec![scalar_sub("embedmapcover_selector")]),
            alias_template: None,
            containment_semantic: ContainmentSemantic::Parenthesis,
        },
    ));

    // HoViewApplication.domain_spec and .first_parens_spec
    carriers.push(pipe_with(
        "hoview_ds_src",
        UnaryRelationalOperator::HoViewApplication {
            function: "v".to_string(),
            arguments: vec![],
            first_parens_spec: None,
            domain_spec: DomainSpec::Positional(vec![scalar_sub("hoview_domain_spec")]),
            namespace: None,
            grounding: None,
        },
    ));
    carriers.push(pipe_with(
        "hoview_fp_src",
        UnaryRelationalOperator::HoViewApplication {
            function: "v".to_string(),
            arguments: vec![],
            first_parens_spec: Some(DomainSpec::Positional(vec![scalar_sub(
                "hoview_first_parens_spec",
            )])),
            domain_spec: DomainSpec::Glob,
            namespace: None,
            grounding: None,
        },
    ));

    // DmlTerminal.domain_spec
    carriers.push(pipe_with(
        "dml_src",
        UnaryRelationalOperator::DmlTerminal {
            kind: DmlKind::Insert,
            target: "t".to_string(),
            target_namespace: None,
            domain_spec: DomainSpec::Positional(vec![scalar_sub("dml_domain_spec")]),
        },
    ));

    // DirectiveTerminal.arguments — `|> d!(args)`
    carriers.push(pipe_with(
        "directiveterminal_src",
        UnaryRelationalOperator::DirectiveTerminal {
            name: "d!".to_string(),
            arguments: vec![scalar_sub("directiveterminal_arguments")],
        },
    ));

    // DirectivePipeInvocation.domain_spec (the matrix already pins its argument)
    carriers.push(pipe_with(
        "dpi_ds_src",
        UnaryRelationalOperator::DirectivePipeInvocation {
            name: "d!".to_string(),
            argument: Box::new(sentinel("dpi_ds_argument_ignored")),
            domain_spec: DomainSpec::Positional(vec![scalar_sub("dpi_domain_spec")]),
        },
    ));

    // --- Function / container carriers the review flags as un-pinned ----------

    // Case arms — condition (Searched) and result.
    carriers.push(general_with_func(
        "case_src",
        FunctionExpression::CaseExpression {
            arms: vec![CaseArm::Searched {
                condition: Box::new(inner_exists("case_arm_condition")),
                result: Box::new(scalar_sub("case_arm_result")),
            }],
            alias: None,
        },
    ));

    // Curly members — a key/value member.
    carriers.push(general_with_func(
        "curly_src",
        FunctionExpression::Curly {
            members: vec![CurlyMember::KeyValue {
                key: "k".to_string(),
                nested_reduction: false,
                value: Box::new(scalar_sub("curly_member")),
            }],
            inner_grouping_keys: vec![],
            cte_requirements: None,
            alias: None,
        },
    ));

    // Curly cte_requirements — the resolver-populated side structure.
    carriers.push(general_with_func(
        "cte_req_src",
        FunctionExpression::Curly {
            members: vec![],
            inner_grouping_keys: vec![],
            cte_requirements: Some(CteRequirements {
                needs_cte: false,
                accumulated_grouping_keys: vec![(None, scalar_sub("cte_requirements"))],
                join_keys: vec![],
                location: TreeGroupLocation::InReducingBy,
                nested_members_info: vec![],
            }),
            alias: None,
        },
    ));

    // Arrays — an array member's path.
    carriers.push(general_with_func(
        "array_src",
        FunctionExpression::Array {
            members: vec![ArrayMember::Index {
                path: Box::new(scalar_sub("array_member")),
                alias: None,
            }],
            alias: None,
        },
    ));

    // Windows — arguments, partition_by, and order_by.
    carriers.push(general_with_func(
        "window_arg_src",
        FunctionExpression::Window {
            name: "w".into(),
            arguments: vec![scalar_sub("window_arguments")],
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            alias: None,
        },
    ));
    carriers.push(general_with_func(
        "window_part_src",
        FunctionExpression::Window {
            name: "w".into(),
            arguments: vec![],
            partition_by: vec![scalar_sub("window_partition_by")],
            order_by: vec![],
            frame: None,
            alias: None,
        },
    ));
    carriers.push(general_with_func(
        "window_order_src",
        FunctionExpression::Window {
            name: "w".into(),
            arguments: vec![],
            partition_by: vec![],
            order_by: vec![OrderingSpec {
                column: scalar_sub("window_order_by"),
                direction: None,
            }],
            frame: None,
            alias: None,
        },
    ));

    // Templates — a string-template interpolation.
    carriers.push(general_with_func(
        "template_src",
        FunctionExpression::StringTemplate {
            parts: vec![StringTemplatePart::Interpolation(Box::new(scalar_sub(
                "template_interpolation",
            )))],
            alias: None,
        },
    ));

    // JSON paths — the source expression.
    carriers.push(general_with_func(
        "jsonpath_src",
        FunctionExpression::JsonPath {
            source: Box::new(scalar_sub("jsonpath_source")),
            path: Box::new(DomainExpression::NonUnifiyingUnderscore),
            alias: None,
        },
    ));

    let body = chain(carriers);

    let mut c = SentinelCollector::default();
    walk_visit_relational(&mut c, &body).expect("walk ok");

    // Every carrier's sentinel MUST be reached. A missing one is a real closure
    // hole in the shared `AstVisit` walk (RED — the fix closes that walk edge).
    let expected = [
        "projectout_expressions",
        "tupleordering_specs",
        "mapcover_function",
        "mapcover_columns",
        "modulo_reducing_by",
        "modulo_delegates",
        "modulo_columns",
        "renamecover_specs",
        "reposition_moves",
        "embedmapcover_function",
        "embedmapcover_selector",
        "hoview_domain_spec",
        "hoview_first_parens_spec",
        "dml_domain_spec",
        "directiveterminal_arguments",
        "dpi_domain_spec",
        "case_arm_condition",
        "case_arm_result",
        "curly_member",
        "cte_requirements",
        "array_member",
        "window_arguments",
        "window_partition_by",
        "window_order_by",
        "template_interpolation",
        "jsonpath_source",
    ];
    let missing: Vec<&str> = expected
        .iter()
        .copied()
        .filter(|e| !c.seen.iter().any(|s| s == e))
        .collect();
    assert!(
        missing.is_empty(),
        "the AstVisit walk dropped these query-bearing edges (real closure holes): {:?}\nreached: {:?}",
        missing,
        c.seen
    );
}

// ---------------------------------------------------------------------------
// Control axis: SkipSubtree
// ---------------------------------------------------------------------------

/// A collector that returns `SkipSubtree` when it enters an InnerRelation,
/// pruning that node's subquery — and nothing else.
#[derive(Default)]
struct PruneInnerRelations {
    seen: Vec<String>,
}
impl AstVisit<Unresolved> for PruneInnerRelations {
    fn enter_relation(&mut self, r: &Relation<Unresolved>) -> Result<Descent> {
        if let Relation::PseudoPredicate { name, .. } = r {
            self.seen.push(name.clone());
        }
        Ok(Descent::Continue)
    }
    fn enter_inner_relation(&mut self, _i: &InnerRelationPattern<Unresolved>) -> Result<Descent> {
        Ok(Descent::SkipSubtree)
    }
}

#[test]
fn skip_subtree_prunes_exactly_its_subtree() {
    let pruned = RelationalExpression::Relation(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier: qn("i"),
            subquery: Box::new(sentinel("under_pruned")),
        },
        alias: None,
        outer: false,
        cpr_schema: PhaseBox::phantom(),
    });
    let tree = RelationalExpression::Join {
        left: Box::new(pruned),
        right: Box::new(sentinel("sibling")),
        join_condition: None,
        join_type: None,
        cpr_schema: PhaseBox::phantom(),
    };

    let mut c = PruneInnerRelations::default();
    walk_visit_relational(&mut c, &tree).expect("walk ok");

    assert!(
        c.seen.iter().any(|s| s == "sibling"),
        "SkipSubtree wrongly pruned a sibling; reached: {:?}",
        c.seen
    );
    assert!(
        !c.seen.iter().any(|s| s == "under_pruned"),
        "SkipSubtree failed to prune the subtree; reached: {:?}",
        c.seen
    );
}

// ---------------------------------------------------------------------------
// Control axis: Break
// ---------------------------------------------------------------------------

/// Records sentinels, and returns `Break` immediately after seeing `stop_at`.
struct BreakAt {
    stop_at: &'static str,
    seen: Vec<String>,
}
impl AstVisit<Unresolved> for BreakAt {
    fn enter_relation(&mut self, r: &Relation<Unresolved>) -> Result<Descent> {
        if let Relation::PseudoPredicate { name, .. } = r {
            self.seen.push(name.clone());
            if name == self.stop_at {
                return Ok(Descent::Break);
            }
        }
        Ok(Descent::Continue)
    }
}

#[test]
fn break_stops_the_walk_promptly() {
    // Left-to-right: first, then (second, third).
    let tree = RelationalExpression::Join {
        left: Box::new(sentinel("first")),
        right: Box::new(RelationalExpression::Join {
            left: Box::new(sentinel("second")),
            right: Box::new(sentinel("third")),
            join_condition: None,
            join_type: None,
            cpr_schema: PhaseBox::phantom(),
        }),
        join_condition: None,
        join_type: None,
        cpr_schema: PhaseBox::phantom(),
    };

    let mut c = BreakAt {
        stop_at: "second",
        seen: vec![],
    };
    let flow = walk_visit_relational(&mut c, &tree).expect("walk ok");

    assert_eq!(flow, Descent::Break, "Break must propagate to the caller");
    assert_eq!(
        c.seen,
        vec!["first".to_string(), "second".to_string()],
        "Break did not stop promptly; reached: {:?}",
        c.seen
    );
}

// ---------------------------------------------------------------------------
// Error propagation
// ---------------------------------------------------------------------------

/// Returns `Err` when it enters `boom`.
struct FailAt {
    boom: &'static str,
    seen: Vec<String>,
}
impl AstVisit<Unresolved> for FailAt {
    fn enter_relation(&mut self, r: &Relation<Unresolved>) -> Result<Descent> {
        if let Relation::PseudoPredicate { name, .. } = r {
            if name == self.boom {
                return Err(DelightQLError::parse_error("boom"));
            }
            self.seen.push(name.clone());
        }
        Ok(Descent::Continue)
    }
}

#[test]
fn err_hook_short_circuits_the_walk() {
    let tree = RelationalExpression::Join {
        left: Box::new(sentinel("before")),
        right: Box::new(RelationalExpression::Join {
            left: Box::new(sentinel("boom")),
            right: Box::new(sentinel("after")),
            join_condition: None,
            join_type: None,
            cpr_schema: PhaseBox::phantom(),
        }),
        join_condition: None,
        join_type: None,
        cpr_schema: PhaseBox::phantom(),
    };

    let mut c = FailAt {
        boom: "boom",
        seen: vec![],
    };
    let result = walk_visit_relational(&mut c, &tree);

    assert!(result.is_err(), "Err hook must short-circuit the walk");
    assert!(
        c.seen.iter().any(|s| s == "before"),
        "nodes before the failure should have been visited: {:?}",
        c.seen
    );
    assert!(
        !c.seen.iter().any(|s| s == "after"),
        "nodes after the failure must not be visited: {:?}",
        c.seen
    );
}

/// REVIEW REMEDIATION (Phase 3a review, P1): `PseudoPredicate.access` is a
/// recursive field — a demand or subquery embedded in the receipt-access
/// spec must be visible to every `AstVisit` tenant (effect discipline, R9
/// discovery, compile purity). This is the structural regression for the
/// missing walker edge: a sentinel smuggled beneath `access` via a scalar
/// subquery must be seen by the generic walk.
#[test]
fn visit_reaches_pseudo_predicate_access() {
    let expr: RelationalExpression<Unresolved> =
        RelationalExpression::Relation(Relation::PseudoPredicate {
            name: "outer!".to_string(),
            namespace: Vec::new(),
            arguments: vec![scalar_sub("inside-arguments")],
            access: DomainSpec::Positional(vec![scalar_sub("inside-access")]),
            alias: None,
            cpr_schema: PhaseBox::phantom(),
        });

    let mut collector = SentinelCollector::default();
    walk_visit_relational(&mut collector, &expr).expect("walk succeeds");

    assert!(
        collector.seen.contains(&"inside-arguments".to_string()),
        "arguments edge lost: {:?}",
        collector.seen
    );
    assert!(
        collector.seen.contains(&"inside-access".to_string()),
        "ACCESS edge lost — the recursive field has no walker edge: {:?}",
        collector.seen
    );
}
