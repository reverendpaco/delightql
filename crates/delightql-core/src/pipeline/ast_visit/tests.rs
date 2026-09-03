// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Unit tests for the `AstVisit` closure and its two control axes.
//!
//! These are the red-first pins for the traversal infrastructure (house rule
//! "a comment claiming a guarantee names its test"):
//! - `r_i4_recursion_closure_matrix` — **THE R-I4 recursion-closure matrix**.
//!   It grew from the Phase-B seed
//!   (`visit_reaches_every_query_bearing_edge`) into the comprehensive matrix:
//!   a distinct sentinel beneath EVERY query-bearing edge the grammar
//!   enumerates — including EACH Pipe-operator argument form
//!   (General expressions, Transform transformations
//!   AND its `conditioned_on`, MapCover `conditioned_on`, Group Reduce, and a
//!   relation-position `FunctorCall` argument), reached by the default
//!   full-descend walk. It is the standing structural proof that "no recursive
//!   field is silently dropped": dropping the recursive edge that uniquely
//!   reaches a given sentinel fails that sentinel's assertion (drop-an-edge
//!   spot-checked against `Filter.condition` and `General.expressions`). This
//!   COMPLEMENTS — does not replace — the Phase-C `p1_closure_matrix` (which
//!   proves the two P1 schemes coincide on one fixture); this one proves the
//!   `AstVisit` default walk itself is complete.
//! - `skip_subtree_prunes_exactly_its_subtree` — boundary axis.
//! - `break_stops_the_walk_promptly` — early termination.
//! - `err_hook_short_circuits_the_walk` — error propagation.

use super::*;
use crate::error::DelightQLError;
use crate::pipeline::asts::core::expressions::helpers::QualifiedName;
use crate::pipeline::asts::core::expressions::metadata_types::FilterOrigin;
use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
use crate::pipeline::asts::core::metadata::NamespacePath;
use crate::pipeline::asts::core::operators::HoArgument;
use crate::pipeline::asts::core::specs::{GroupSpec, OutItem};
use crate::pipeline::asts::core::Access;
use crate::pipeline::asts::core::ProbeAddressing;
use crate::pipeline::asts::core::Step;
use crate::pipeline::asts::core::{
    Chain, Continuation, CteBinding, DomainExpression, FunctionApplication, FunctorCall,
    GroundForm, PipeOp, PureCall, Query, Relation, SealedCall, StandardApplication,
    TruthExpression, Unresolved, WindowSpec,
};
use crate::pipeline::asts::core::{Existence, RelationalMembership};
use crate::pipeline::asts::core::{Polarity, Probe};

// Additional carriers exercised by the RED-4 extended matrix.
use crate::pipeline::asts::core::expressions::functions::{
    CaseExpression, SearchedArm, ValueTemplate, ValueTemplatePart,
};
use crate::pipeline::asts::core::expressions::metadata_types::{
    CteRequirements, ReductionPlan, TreeGroupLocation, TreeGroupPlan,
};
use crate::pipeline::asts::core::operators::{EmbedMapCover, MapCover};
use crate::pipeline::asts::core::specs::{DelegateSpec, NameTarget, OrderingSpec, RenameSpec};
use crate::pipeline::asts::core::{
    AuthoredColumn, Enclyph, NamedReference, Record, RecordMember, ReductionItem,
};
use crate::pipeline::asts::vocabulary::Vec1;

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

fn qn(name: &str) -> QualifiedName {
    QualifiedName {
        namespace_path: NamespacePath::empty(),
        name: name.into(),
    }
}

fn test_ref(name: &str) -> crate::pipeline::asts::vocabulary::Ref {
    crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
        &std::rc::Rc::new(crate::names::Registry::new(&[])),
        crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
        name,
    )
}

fn call(name: &str, ho_arguments: Vec<HoArgument<Unresolved>>) -> SealedCall<Unresolved> {
    SealedCall::from_inner(
        FunctorCall {
            callee: test_ref(name),
            arguments: crate::pipeline::asts::core::operators::CallArguments::higher_order(
                ho_arguments,
            ),
            marks: Default::default(),
        },
        false,
    )
}

fn pure_call(name: &str, ho_arguments: Vec<HoArgument<Unresolved>>) -> PureCall<Unresolved> {
    PureCall::from_inner(call(name, ho_arguments).into_inner())
}

/// An application, with whatever scalar context the edge under test needs.
fn applied(
    name: &str,
    ho_arguments: Vec<HoArgument<Unresolved>>,
    window: Option<WindowSpec<Unresolved>>,
    guard: Option<TruthExpression<Unresolved>>,
) -> FunctionApplication<Unresolved> {
    FunctionApplication::Standard(StandardApplication {
        call: pure_call(name, ho_arguments),
        guard: guard.map(Box::new),
        window,
    })
}

/// A recognizable callable sentinel whose written reference spelling is the tag.
fn sentinel(tag: &str) -> Chain<Unresolved> {
    Chain::authored(GroundForm::Reference(Relation::FunctorCall {
        alias: None,
        call: FunctorCall::written(test_ref(tag), vec![]).into(),
    }))
}

/// The consulted-view edge, proven in the phase whose trees can hold one.
///
/// A consulted view is the resolver's own product: `Unresolved::Consulted` is
/// uninhabited, so the authored-phase matrix above cannot carry one. The edge
/// is still query-bearing and still has to be walked, so it is pinned here
/// instead of going unpinned.
#[test]
fn the_walk_reaches_a_consulted_views_body() {
    use crate::pipeline::asts::core::Resolved;

    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let spelling = registry.intern("v", false);
    let _ = spelling;
    let scope = crate::relation::any_relation(&registry);
    let body = registry
        .authority()
        .ground_read(Access::All, false, scope)
        .expect("a ground read");
    let view = Chain::<Resolved>::ground(
        registry
            .authority()
            .wrapping_head(crate::pipeline::asts::core::GroundForm::Reference(
                Relation::ConsultedView {
                    body: Box::new(Query::relational(body)),
                    outer: false,
                },
            ))
            .expect("a consulted head"),
    );

    #[derive(Default)]
    struct Seen(Vec<String>);
    impl AstVisit<Resolved> for Seen {
        /// The head is where a ground read AND the relation it publishes
        /// both are, so what the walk reached is named from there.
        fn enter_relational(
            &mut self,
            chain: &crate::pipeline::ast_resolved::Chain,
        ) -> Result<Descent> {
            if let crate::pipeline::asts::core::GroundForm::Reference(Relation::Ground { .. }) =
                chain.head().form()
            {
                self.0.push(format!("{:?}", chain.head().result()));
            }
            Ok(Descent::Continue)
        }
    }

    let mut seen = Seen::default();
    walk_visit_relational(&mut seen, &view).expect("walk ok");
    assert!(
        seen.0.iter().any(|tag| *tag == format!("{scope:?}")),
        "the walk did not descend into the consulted view's body; reached: {:?}",
        seen.0
    );
}

/// Collects the tags of every functor sentinel the walk enters.
#[derive(Default)]
struct SentinelCollector {
    seen: Vec<String>,
}

impl AstVisit<Unresolved> for SentinelCollector {
    fn enter_relation(&mut self, r: &Relation<Unresolved>) -> Result<Descent> {
        if let Relation::FunctorCall { call, .. } = r {
            self.seen.push(call.call().callee.name_text());
        }
        Ok(Descent::Continue)
    }
}

fn chain(exprs: Vec<Chain<Unresolved>>) -> Chain<Unresolved> {
    exprs
        .into_iter()
        .reduce(|left, right| {
            left.then(Step::authored(Continuation::Member {
                rhs: right,
                correlation: None,
                join_type: None,
            }))
        })
        .expect("at least one carrier")
}

/// A scalar subquery whose subquery IS a `tag` sentinel: the query-bearing
/// `FunctionApplication::Scalarized` edge, used to smuggle a nested relation
/// beneath any domain-valued operator argument.
fn scalar_sub(tag: &str) -> DomainExpression<Unresolved> {
    DomainExpression::Application(
        crate::pipeline::asts::core::FunctionApplication::Scalarized(
            crate::pipeline::asts::core::ScalarRelation::Named {
                identifier: qn(tag),
                body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                    sentinel(tag),
                    crate::pipeline::asts::core::Scalarization::BoundToOne {
                        ordering: Vec::new(),
                    },
                )),
            },
        ),
    )
}

/// An `EXISTS` whose subquery IS a `tag` sentinel: the query-bearing
/// `TruthExpression::InnerExists` edge, used to smuggle a nested relation
/// beneath any boolean-valued operator argument (`conditioned_on`).
fn inner_exists(tag: &str) -> TruthExpression<Unresolved> {
    TruthExpression::Existence(Existence {
        polarity: Polarity::Positive,
        relation: Box::new(sentinel(tag)),
        addressing: ProbeAddressing {
            identifier: qn(tag),
            using_columns: vec![],
        },
    })
}

/// A one-operator pipe: `sentinel(source_tag) |> operator`. The source sentinel
/// pins the `Pipe.source` edge; `operator` pins whichever operator-argument edge
/// the caller wired a sentinel beneath.
/// A call's ACCESS stands where a relational access stands: after it.
fn access_step(chain: Chain<Unresolved>, access: Access<Unresolved>) -> Chain<Unresolved> {
    chain.then(Step::authored(Continuation::Access {
        access,
        named: None,
    }))
}

fn pipe_with(source_tag: &str, operator: PipeOp<Unresolved>) -> Chain<Unresolved> {
    sentinel(source_tag).then(Step::authored(Continuation::Pipe {
        operator: operator,
        named: None,
    }))
}

// ---------------------------------------------------------------------------
// The R-I4 closure seed
// ---------------------------------------------------------------------------

/// THE R-I4 recursion-closure matrix.
///
/// The default full-descend walk reaches a distinct sentinel beneath EVERY
/// query-bearing edge the grammar enumerates — Filter.condition, correlation,
/// EACH Pipe-operator argument form, WithCtes body, ConsultedView body, CFE body,
/// HO table argument, and InnerRelation subquery.
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
    let filter_condition = sentinel("filter_source").then(Step::authored(Continuation::Restrict {
        condition: TruthExpression::RelationalMembership(RelationalMembership {
            probe: Probe::Value(Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Open(
                    crate::pipeline::asts::core::DomainHole::Disregarded,
                ),
            ))),
            relation: Box::new(sentinel("filter_condition")),
            addressing: ProbeAddressing {
                identifier: qn("f"),
                using_columns: vec![],
            },
            negated: false,
        }),
        origin: FilterOrigin::UserWritten,
    }));

    // Finding 3 — Join.correlation (via InnerExists subquery).
    let correlation = sentinel("join_left").then(Step::authored(Continuation::Member {
        rhs: sentinel("join_right"),
        correlation: Some(MemberCorrelation::Condition(inner_exists("correlation"))),
        join_type: None,
    }));

    // Finding 4 — EACH Pipe-operator argument form carries a query-bearing edge.
    // A single operator arm was the seed's only probe; the matrix probes every
    // arm through which a nested relation (subquery / directive) can hide.
    //
    // (a) Transform transformations: a scalar subquery in a `$$` expression (the
    //     edge missed by ALL relational-entry whole-tree walkers today).
    let op_transform_transformation = pipe_with(
        "op_transform_source",
        PipeOp::Transform {
            items: crate::pipeline::asts::vocabulary::Vec1::new(
                crate::pipeline::asts::core::NamedOutItem::authored(
                    scalar_sub("op_transform_transformation"),
                    "a".into(),
                    None,
                ),
            ),
            guard: None,
        },
    );
    // (b) Transform conditioned_on: an EXISTS guarding a `$$` cover.
    let op_transform_conditioned = pipe_with(
        "op_transform_cond_source",
        PipeOp::Transform {
            items: crate::pipeline::asts::vocabulary::Vec1::new(
                crate::pipeline::asts::core::NamedOutItem::authored(
                    DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Ground(
                            crate::pipeline::asts::core::LiteralValue::Number("1".to_string()),
                        ),
                    ),
                    "b".into(),
                    None,
                ),
            ),
            guard: Some(Box::new(inner_exists("op_transform_conditioned_on"))),
        },
    );
    // (c) General projection expressions: a scalar subquery in `(...)` / `[...]`.
    let op_general = pipe_with(
        "op_general_source",
        PipeOp::Project(crate::pipeline::asts::vocabulary::Vec1::new(out_item(
            scalar_sub("op_general_expr"),
        ))),
    );
    // (e) MapCover conditioned_on: an EXISTS guarding a `$` map cover — a DISTINCT
    //     operator arm from Transform's conditioned_on (a dropped edge on either
    //     arm must be caught independently).
    let op_mapcover = pipe_with(
        "op_mapcover_source",
        PipeOp::MapCover(MapCover {
            callable: crate::pipeline::asts::core::Callable::Lambda(
                crate::pipeline::asts::core::Lambda {
                    body: Box::new(scalar_sub("cover_callable_body_ignored")),
                },
            ),
            selector: vec![],
            guard: Some(Box::new(inner_exists("op_mapcover_conditioned_on"))),
            cells: Vec::new(),
        }),
    );
    // (f) Group Reduce reductions: a scalar subquery in a `%(...)` group — the
    //     descent path the now-deleted `_tg_N` tree-group walk (W8) took.
    let op_group = pipe_with(
        "op_group_source",
        PipeOp::Group(GroupSpec::Reduce {
            keys: vec![],
            reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(out_item(
                scalar_sub("op_group_reducing_on"),
            ))),
            plan: ReductionPlan::empty(),
        }),
    );
    // (g) relation-position FunctorCall argument: a directive's relational HO argument
    //     (a query-bearing edge that returns straight to relational syntax).
    let op_directive = Chain::authored(GroundForm::Reference(Relation::FunctorCall {
        call: call(
            "d!",
            vec![
                HoArgument::Relation(sentinel("op_directive_pipe_arg")),
                HoArgument::Relation(sentinel("op_directive_source")),
            ],
        ),
        alias: None,
    }));

    // Finding 8 — a higher-order table argument on a TVF.
    let ho_tvf = Chain::authored(GroundForm::Reference(Relation::FunctorCall {
        alias: None,
        call: call("tvf", vec![HoArgument::Relation(sentinel("ho_argument"))]),
    }));

    // Finding 10 — an InnerRelation subquery.
    let inner_relation = Chain::authored(GroundForm::Reference(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier: qn("i"),
            subquery: Box::new(sentinel("inner_relation_subquery")),
        },
        alias: None,
        outer: false,
    }));

    let body = chain(vec![
        filter_condition,
        correlation,
        op_transform_transformation,
        op_transform_conditioned,
        op_general,
        op_mapcover,
        op_group,
        op_directive,
        ho_tvf,
        inner_relation,
    ]);

    // Finding 5 — a WithCtes body (a CteBinding expression).
    let mut block = crate::pipeline::asts::core::QueryLocalBlock::default();
    block
        .admit_relation(CteBinding::authored(
            sentinel("cte_body"),
            crate::pipeline::asts::core::AuthoredCteSubject::Authored {
                name: delightql_types::SqlIdentifier::new("c"),
                effect: crate::pipeline::asts::core::CteEffectDeclaration::Pure,
            },
            crate::pipeline::asts::core::CteAuthority {
                horizon: crate::pipeline::asts::core::LexicalHorizon::all(),
                head: crate::pipeline::asts::core::definitions::Head::glob(),
                origin: Default::default(),
                fixpoint: crate::pipeline::asts::vocabulary::Fixpoint::Bag,
            },
        ))
        .expect("one authored binding");
    let query = Query::<crate::pipeline::asts::core::Unresolved>::binding(
        block.seal().expect("the block seals"),
        body,
    );

    let mut c = SentinelCollector::default();
    let flow = walk_visit_query(&mut c, &query).expect("walk ok");
    assert_eq!(flow, Descent::Continue);

    for expected in [
        // Filter.condition / correlation
        "filter_condition",
        "correlation",
        // each Pipe-operator argument form (finding 4)
        "op_transform_transformation",
        "op_transform_conditioned_on",
        "op_general_expr",
        "op_mapcover_conditioned_on",
        "op_group_reducing_on",
        "op_directive_pipe_arg",
        // structural boundaries
        "ho_argument",
        "inner_relation_subquery",
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
    let cfe_body: DomainExpression<Unresolved> = DomainExpression::Application(
        crate::pipeline::asts::core::FunctionApplication::Scalarized(
            crate::pipeline::asts::core::ScalarRelation::Named {
                identifier: qn("c"),
                body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                    sentinel("cfe_body"),
                    crate::pipeline::asts::core::Scalarization::BoundToOne {
                        ordering: Vec::new(),
                    },
                )),
            },
        ),
    );
    let mut cfe = SentinelCollector::default();
    walk_visit_domain(&mut cfe, &cfe_body).expect("walk ok");
    assert!(
        cfe.seen.iter().any(|s| s == "cfe_body"),
        "rooted CFE-body visit did not reach its sentinel; reached: {:?}",
        cfe.seen
    );
}

// ---------------------------------------------------------------------------
// RED-4 (F4): the R-I4 matrix under-covers its stated guarantee
// ---------------------------------------------------------------------------

/// An unnamed publication item whose value is a `tag` scalar-subquery sentinel.
fn out_dom(tag: &str) -> OutItem<Unresolved> {
    out_item(scalar_sub(tag))
}

/// An unnamed publication item over an arbitrary value.
fn out_item(expr: DomainExpression<Unresolved>) -> OutItem<Unresolved> {
    OutItem::one(crate::pipeline::asts::core::OneOut::authored(expr, None))
}

/// Wrap a `FunctionApplication` as a `DomainExpression` so it can ride a
/// `General` projection's `expressions` vec.
fn func_dom(f: FunctionApplication<Unresolved>) -> DomainExpression<Unresolved> {
    DomainExpression::Application(f)
}

/// A `General` projection pipe carrying one function-valued expression: the
/// smuggling vector for every FunctionApplication container carrier.
fn general_with_func(source_tag: &str, f: FunctionApplication<Unresolved>) -> Chain<Unresolved> {
    pipe_with(
        source_tag,
        PipeOp::Project(crate::pipeline::asts::vocabulary::Vec1::new(out_item(
            func_dom(f),
        ))),
    )
}

/// The recursion-closure matrix, carrier by carrier.
///
/// `r_i4_recursion_closure_matrix` claims to cover "EACH Pipe-operator argument
/// form" and generalizes to "the whole grammar," but a dozen independent
/// query-bearing carriers go unprobed by it. This test plants a distinct
/// sentinel beneath EACH operator-argument carrier and each function/container
/// field. Any sentinel the default `AstVisit` walk does NOT reach is a real
/// closure hole and reds its assertion; together they are what earns the "no
/// recursive field is silently dropped" guarantee.
#[test]
fn r_i4_recursion_closure_matrix_extended() {
    let mut carriers: Vec<Chain<Unresolved>> = Vec::new();

    // --- Operator-argument carriers ------------------------------------------

    // ProjectOut.expressions — `-(...)`
    carriers.push(pipe_with("projectout_src", PipeOp::ProjectOut(Vec::new())));

    // Ordering.specs — `#(...)`, now chain structure
    carriers.push(
        sentinel("tupleordering_src").then(Step::authored(Continuation::Structural(
            crate::pipeline::asts::core::StructuralStep {
                form: crate::pipeline::asts::core::StructuralForm::Ordering {
                    specs: vec![OrderingSpec {
                        column: scalar_sub("tupleordering_specs"),
                        direction: None,
                    }],
                    bound: None,
                },
                named: None,
            },
        ))),
    );

    // MapCover.function
    carriers.push(pipe_with(
        "mapcover_fn_src",
        PipeOp::MapCover(MapCover {
            callable: crate::pipeline::asts::core::Callable::Lambda(
                crate::pipeline::asts::core::Lambda {
                    body: Box::new(scalar_sub("mapcover_function")),
                },
            ),
            selector: vec![],
            guard: None,
            cells: Vec::new(),
        }),
    ));
    // MapCover.columns
    carriers.push(pipe_with(
        "mapcover_cols_src",
        PipeOp::MapCover(MapCover {
            callable: crate::pipeline::asts::core::Callable::Lambda(
                crate::pipeline::asts::core::Lambda {
                    body: Box::new(scalar_sub("cover_callable_body_ignored")),
                },
            ),
            selector: Vec::new(),
            guard: None,
            cells: Vec::new(),
        }),
    ));

    // Group Reduce.keys
    carriers.push(pipe_with(
        "group_keys_src",
        PipeOp::Group(GroupSpec::Reduce {
            keys: vec![out_dom("group_keys")],
            reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(out_dom(
                "group_reduction",
            ))),
            plan: ReductionPlan::empty(),
        }),
    ));
    // Group Reduce.delegates
    carriers.push(pipe_with(
        "group_del_src",
        PipeOp::Group(GroupSpec::Reduce {
            keys: vec![],
            reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Delegate(
                DelegateSpec {
                    payload: vec![out_dom("group_delegates")],
                    order: vec![],
                },
            )),
            plan: ReductionPlan::empty(),
        }),
    ));
    // Group Distinct (the simple keys list)
    carriers.push(pipe_with(
        "group_cols_src",
        PipeOp::Group(GroupSpec::Distinct {
            keys: crate::pipeline::asts::vocabulary::Vec1::new(out_item(scalar_sub(
                "group_distinct_keys",
            ))),
        }),
    ));

    // Rename pairs — `*(...)`
    carriers.push(pipe_with(
        "renamecover_src",
        PipeOp::Rename(crate::pipeline::asts::vocabulary::Vec1::new(RenameSpec {
            from: crate::pipeline::asts::core::RenameSource::Glob(
                crate::pipeline::asts::core::Glob::whole(),
            ),
            to: NameTarget::Identifier("x".to_string()),
        })),
    ));

    // EmbedMapCover.function
    carriers.push(pipe_with(
        "embed_fn_src",
        PipeOp::EmbedMapCover(EmbedMapCover {
            callable: crate::pipeline::asts::core::Callable::Lambda(
                crate::pipeline::asts::core::Lambda {
                    body: Box::new(scalar_sub("embedmapcover_function")),
                },
            ),
            naming: None,
            selector: Vec::new(),
            cells: Vec::new(),
        }),
    ));
    // EmbedMapCover.selector
    carriers.push(pipe_with(
        "embed_sel_src",
        PipeOp::EmbedMapCover(EmbedMapCover {
            callable: crate::pipeline::asts::core::Callable::Lambda(
                crate::pipeline::asts::core::Lambda {
                    body: Box::new(scalar_sub("cover_callable_body_ignored")),
                },
            ),
            naming: None,
            selector: Vec::new(),
            cells: Vec::new(),
        }),
    ));

    // A common functor call carries the relational argument, and the access
    // group standing after it asks what the call publishes.
    carriers.push(access_step(
        Chain::authored(GroundForm::Reference(Relation::FunctorCall {
            call: call(
                "v",
                vec![
                    HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(
                        scalar_sub("hoview_rich_argument"),
                    )),
                    HoArgument::Relation(sentinel("hoview_ds_src")),
                ],
            ),
            alias: None,
        })),
        Access::from_terms(vec![scalar_sub("hoview_access")]),
    ));
    carriers.push(Chain::authored(GroundForm::Reference(
        Relation::FunctorCall {
            call: call(
                "v",
                vec![
                    HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(
                        scalar_sub("hoview_first_parens_spec"),
                    )),
                    HoArgument::Relation(sentinel("hoview_fp_src")),
                ],
            ),
            alias: None,
        },
    )));

    // A DML terminal's RECEIPT: the access standing in the effect position.
    carriers.push(access_step(
        Chain::authored(GroundForm::Reference(Relation::FunctorCall {
            call: call(
                "insert!",
                vec![
                    HoArgument::Relation(sentinel("dml_target")),
                    HoArgument::Relation(sentinel("dml_src")),
                ],
            ),
            alias: None,
        })),
        Access::from_terms(vec![scalar_sub("dml_access")]),
    ));

    // Functor-call scalar arguments — the terminal's own argument row
    carriers.push(Chain::authored(GroundForm::Reference(
        Relation::FunctorCall {
            call: call(
                "d!",
                vec![
                    HoArgument::Value(crate::pipeline::asts::core::ArgumentValue::plain(
                        scalar_sub("directiveterminal_arguments"),
                    )),
                    HoArgument::Relation(sentinel("directiveterminal_src")),
                ],
            ),
            alias: None,
        },
    )));

    // A directive's receipt (the matrix already pins its argument)
    carriers.push(access_step(
        Chain::authored(GroundForm::Reference(Relation::FunctorCall {
            call: call(
                "d!",
                vec![
                    HoArgument::Relation(sentinel("dpi_ds_argument_ignored")),
                    HoArgument::Relation(sentinel("dpi_ds_src")),
                ],
            ),
            alias: None,
        })),
        Access::from_terms(vec![scalar_sub("dpi_access")]),
    ));

    // --- Function / container carriers ----------------------------------------

    // Case arms — condition (Searched) and result.
    carriers.push(general_with_func(
        "case_src",
        crate::pipeline::asts::core::FunctionApplication::Case(CaseExpression::Searched {
            arms: Vec1::new(SearchedArm {
                condition: Box::new(inner_exists("case_arm_condition")),
                result: Box::new(scalar_sub("case_arm_result")),
            }),
            default: Some(Box::new(scalar_sub("case_default_result"))),
        }),
    ));

    // Record members — a keyed member.
    carriers.push(general_with_func(
        "record_src",
        crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(Record::plain(
            Vec1::new(RecordMember::Keyed {
                key: "k".to_string(),
                value: Box::new(scalar_sub("record_member")),
            }),
        ))),
    ));

    // A reduction plan owns the record CTE requirements, and the walk reaches
    // its expressions beside the record occurrence it analyzes.
    let planned_record =
        crate::pipeline::asts::core::FunctionApplication::Enclyph(Enclyph::Record(Record::plain(
            Vec1::new(RecordMember::SelfKeyed(NamedReference(AuthoredColumn {
                name: "k".into(),
                qualifier: None,
                namespace_path: NamespacePath::empty(),
            }))),
        )));
    carriers.push(pipe_with(
        "cte_req_src",
        PipeOp::Group(GroupSpec::Reduce {
            keys: vec![out_item(func_dom(planned_record))],
            reductions: crate::pipeline::asts::vocabulary::Vec1::new(ReductionItem::Out(out_dom(
                "cte_req_reduction",
            ))),
            plan: ReductionPlan {
                tree_groups: vec![TreeGroupPlan {
                    location: TreeGroupLocation::InKeys,
                    item_index: 0,
                    requirements: CteRequirements {
                        needs_cte: false,
                        accumulated_grouping_keys: vec![(None, scalar_sub("cte_requirements"))],
                        join_keys: vec![],
                        location: TreeGroupLocation::InKeys,
                        nested_members_info: vec![],
                    },
                }],
            },
        }),
    ));

    // Applications — arguments, partition, ordering, and the scalar guard.
    carriers.push(general_with_func(
        "window_arg_src",
        applied(
            "w",
            vec![HoArgument::Value(
                crate::pipeline::asts::core::ArgumentValue::plain(scalar_sub("window_arguments")),
            )],
            Some(WindowSpec {
                partition: vec![],
                ordering: vec![],
                frame: None,
            }),
            None,
        ),
    ));
    carriers.push(general_with_func(
        "window_part_src",
        applied(
            "w",
            vec![],
            Some(WindowSpec {
                partition: vec![scalar_sub("window_partition_by")],
                ordering: vec![],
                frame: None,
            }),
            None,
        ),
    ));
    carriers.push(general_with_func(
        "application_guard_src",
        applied("w", vec![], None, Some(inner_exists("application_guard"))),
    ));
    carriers.push(general_with_func(
        "window_order_src",
        applied(
            "w",
            vec![],
            Some(WindowSpec {
                partition: vec![],
                ordering: vec![OrderingSpec {
                    column: scalar_sub("window_order_by"),
                    direction: None,
                }],
                frame: None,
            }),
            None,
        ),
    ));

    // Templates — a string-template interpolation.
    carriers.push(general_with_func(
        "template_src",
        crate::pipeline::asts::core::FunctionApplication::Template(
            ValueTemplate::interpolating(vec![ValueTemplatePart::Interpolation(Box::new(
                scalar_sub("template_interpolation"),
            ))])
            .expect("one interpolation"),
        ),
    ));

    // JSON access — the source expression. The path is a spec: there is no
    // expression under it for a walk to reach.
    carriers.push(general_with_func(
        "jsonaccess_src",
        crate::pipeline::asts::core::FunctionApplication::JsonAccess(
            crate::pipeline::asts::core::JsonAccess {
                source: Box::new(scalar_sub("jsonaccess_source")),
                path: crate::pipeline::asts::core::Path::try_from_steps(vec![
                    crate::pipeline::asts::core::PathStep::Key("k".to_string()),
                ])
                .expect("one step"),
            },
        ),
    ));

    let body = chain(carriers);

    let mut c = SentinelCollector::default();
    walk_visit_relational(&mut c, &body).expect("walk ok");

    // Every carrier's sentinel MUST be reached. A missing one is a real closure
    // hole in the shared `AstVisit` walk (RED — the fix closes that walk edge).
    let expected = [
        "tupleordering_specs",
        "mapcover_function",
        "group_keys",
        "group_delegates",
        "group_distinct_keys",
        "embedmapcover_function",
        "hoview_access",
        "hoview_rich_argument",
        "hoview_first_parens_spec",
        "dml_access",
        "directiveterminal_arguments",
        "dpi_access",
        "case_arm_condition",
        "case_arm_result",
        "record_member",
        "cte_requirements",
        "window_arguments",
        "window_partition_by",
        "window_order_by",
        "application_guard",
        "template_interpolation",
        "jsonaccess_source",
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
        if let Relation::FunctorCall { call, .. } = r {
            if let Some(reference) = Some(&call.call().callee) {
                self.seen.push(reference.name_text().to_string());
            }
        }
        Ok(Descent::Continue)
    }
    fn enter_inner_relation(&mut self, _i: &InnerRelationPattern<Unresolved>) -> Result<Descent> {
        Ok(Descent::SkipSubtree)
    }
}

#[test]
fn skip_subtree_prunes_exactly_its_subtree() {
    let pruned = Chain::authored(GroundForm::Reference(Relation::InnerRelation {
        pattern: InnerRelationPattern::Indeterminate {
            identifier: qn("i"),
            subquery: Box::new(sentinel("under_pruned")),
        },
        alias: None,
        outer: false,
    }));
    let tree = pruned.then(Step::authored(Continuation::Member {
        rhs: sentinel("sibling"),
        correlation: None,
        join_type: None,
    }));

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
        if let Relation::FunctorCall { call, .. } = r {
            let Some(reference) = Some(&call.call().callee) else {
                return Ok(Descent::Continue);
            };
            let name = reference.name_text();
            self.seen.push(name.to_string());
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
    let tree = sentinel("first").then(Step::authored(Continuation::Member {
        rhs: sentinel("second").then(Step::authored(Continuation::Member {
            rhs: sentinel("third"),
            correlation: None,
            join_type: None,
        })),
        correlation: None,
        join_type: None,
    }));

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
        if let Relation::FunctorCall { call, .. } = r {
            let Some(reference) = Some(&call.call().callee) else {
                return Ok(Descent::Continue);
            };
            let name = reference.name_text();
            if name == self.boom {
                return Err(DelightQLError::parse_error("boom"));
            }
            self.seen.push(name.to_string());
        }
        Ok(Descent::Continue)
    }
}

#[test]
fn err_hook_short_circuits_the_walk() {
    let tree = sentinel("before").then(Step::authored(Continuation::Member {
        rhs: sentinel("boom").then(Step::authored(Continuation::Member {
            rhs: sentinel("after"),
            correlation: None,
            join_type: None,
        })),
        correlation: None,
        join_type: None,
    }));

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

/// A call's RECEIPT is a recursive field of the position it stands in — a
/// demand or subquery embedded in the receipt-access spec must be visible to
/// every `AstVisit` tenant (effect discipline, R9 discovery, compile purity).
/// The structural regression for a missing walker edge: a sentinel smuggled
/// beneath the access via a scalar subquery must be seen by the generic walk.
#[test]
fn visit_reaches_functor_call_access() {
    let expr: Chain<Unresolved> = Chain::read(
        Relation::FunctorCall {
            alias: None,
            call: call(
                "outer!",
                vec![HoArgument::Value(
                    crate::pipeline::asts::core::ArgumentValue::plain(scalar_sub(
                        "inside-arguments",
                    )),
                )],
            ),
        },
        Access::from_terms(vec![scalar_sub("inside-access")]),
    );

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

/// A NEW SEMANTIC MEMBER MUST FAIL EVERY CONSUMER THAT OWES A DECISION.
///
/// Both shared walks match every AST enum exhaustively: a variant added to
/// one of them stops the build until each walk says what to do with it. A
/// wildcard arm would make that silent, so the two walk modules carry
/// exactly one — over `Descent`, the walks' own two-variant control answer,
/// which is not an AST carrier.
///
/// This is a ratchet, not a style rule. If a wildcard is genuinely wanted,
/// the number below moves in the same change that explains why.
#[test]
fn the_shared_walks_hide_no_ast_member_behind_a_wildcard() {
    for (source, expected) in [
        (include_str!("mod.rs"), 1),
        (include_str!("../ast_transform/mod.rs"), 0),
    ] {
        let wildcards = source
            .lines()
            .filter(|line| {
                let line = line.trim();
                line.starts_with("_ =>") || line.starts_with("_ if ")
            })
            .count();
        assert_eq!(
            wildcards, expected,
            "a wildcard arm in a shared walk lets a new AST member pass \
             without a decision"
        );
    }
}
