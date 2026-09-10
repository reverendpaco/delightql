// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The authority's own unit tests: the ones that drive the walk directly,
//! below the plan entrances.

use super::*;
use crate::pipeline::ast_unresolved::{GroundMention, Relation};
use crate::pipeline::asts::core::{Access, DomainExpression, GroundForm, QualifiedName};
use crate::pipeline::effect_transformer::tests::world_system;

#[test]
fn star_shaped_plan_scope_keeps_its_resolved_heading() {
    let system = world_system();
    let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let x = registry.intern("x", false);
    let slots = [crate::relation::form::ScratchSlot {
        position: 0,
        named: x,
    }];
    let scratch = registry
        .authority()
        .scratch_row(crate::relation::form::ScratchSpec::stating(
            crate::relation::form::ScratchWhy::Result,
            None,
            &slots,
        ))
        .expect("the scratch and its heading are one construction");
    let column = crate::relation::published_ports(&registry, &scratch.relation())
        .expect("scratch interface")[0]
        .column();
    let query = Query::relational(Chain::read(
        Relation::Ground {
            mention: GroundMention::Scratch { row: scratch },
            outer: false,
        },
        Access::All,
    ));
    let mut plan = PlanBuilder::discovering_for_test(&system, None, registry);
    let world = EffectWorld::program(plan.system(), "home").expect("the plan's own program world");
    let mut walk = EffectWalk::over(&mut plan);
    let compiled = walk
        .compile_statement(&top_walk_ctx(&world), query)
        .expect("direct plan-scope read");
    assert_eq!(compiled.columns, vec![column]);
}

/// A `QualifiedName` for the RED-6 fixtures.
fn qn_red6(name: &str) -> QualifiedName {
    QualifiedName {
        namespace_path: crate::pipeline::asts::core::metadata::NamespacePath::empty(),
        name: name.into(),
    }
}

/// A positional access spec that hides a directive (`insert!`) in a scalar
/// subquery — the exact shape `access_demands_directive` detects.
fn directive_bearing_access() -> Access {
    let inner = Chain::authored(GroundForm::Reference(Relation::FunctorCall {
        alias: None,
        call: crate::pipeline::asts::core::FunctorCall::written(
            crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                &crate::relation::Planning::open(crate::names::Registry::new(&[])).names(),
                crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                "insert!",
            ),
            vec![],
        )
        .into(),
    }));
    Access::from_terms(vec![DomainExpression::Application(
        crate::pipeline::asts::core::FunctionApplication::Scalarized(
            crate::pipeline::asts::core::ScalarRelation::Named {
                identifier: qn_red6("s"),
                body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                    inner,
                    crate::pipeline::asts::core::Scalarization::BoundToOne {
                        ordering: Vec::new(),
                    },
                )),
            },
        ),
    )])
}

fn top_walk_ctx(world: &EffectWorld) -> WalkCtx<'_> {
    WalkCtx {
        world,
        guards: Vec::new(),
        sink: None,
        bindings: HashMap::new(),
        receipt_name: "main".to_string(),
    }
}

/// RED-6 (Ground): a read whose access spec demands a directive must refuse
/// with the honest not-yet-lowerable diagnostic — never return the directive
/// unprocessed.
#[test]
fn ground_access_spec_directive_refuses_at_lowering() {
    let system = world_system();
    let planning = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let mut plan = PlanBuilder::discovering_for_test(&system, Some("fx"), planning);
    let ground = Relation::Ground {
        mention: GroundMention::Named {
            identifier: qn_red6("orders"),
            alias: None,
            mutation_target: false,
            passthrough: false,
        },
        outer: false,
    };

    let world = EffectWorld::program(plan.system(), "fx").expect("the plan's own program world");
    let mut walk = EffectWalk::over(&mut plan);
    let err = walk
        .walk_read(
            ground,
            Some(directive_bearing_access()),
            &top_walk_ctx(&world),
        )
        .expect_err("a directive in a Ground access spec must refuse, not lower unprocessed");
    let msg = format!("{err}");
    assert!(
        msg.contains("predicate-position lowering is not yet supported"),
        "Ground lowering guard must emit the effect/predicate/unsupported refusal \
         (RED-verifiable — deleting the mod.rs:792 guard drops it): {msg}"
    );
}

/// RED-6 (DML): `handle_dml` on a DML terminal whose access spec demands a
/// directive must refuse before any statement is compiled — never lower the
/// directive unprocessed. Pins the guard at mod.rs:1053.
#[test]
fn dml_access_spec_directive_refuses_at_lowering() {
    let system = world_system();
    let planning = crate::relation::Planning::open(crate::names::Registry::new(&[]));
    let mut plan = PlanBuilder::discovering_for_test(&system, Some("fx"), planning);
    // The walked source is immaterial: the guard fires before it is touched.
    let walked_source = Chain::read(
        Relation::Ground {
            mention: GroundMention::Named {
                identifier: qn_red6("source_rows"),
                alias: None,
                mutation_target: false,
                passthrough: false,
            },
            outer: false,
        },
        Access::All,
    );

    let world = EffectWorld::program(plan.system(), "fx").expect("the plan's own program world");
    let mut walk = EffectWalk::over(&mut plan);
    let err = walk
        .handle_dml(
            walked_source,
            crate::names::DmlVerb::Insert,
            "orders_eu".to_string(),
            Some("warehouse".to_string()),
            Chain::read(
                Relation::Ground {
                    mention: GroundMention::Named {
                        identifier: qn_red6("orders_eu"),
                        alias: None,
                        mutation_target: false,
                        passthrough: false,
                    },
                    outer: false,
                },
                Access::All,
            ),
            crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                &crate::relation::Planning::open(crate::names::Registry::new(&[])).names(),
                crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                "insert!",
            ),
            directive_bearing_access(),
            &top_walk_ctx(&world),
        )
        .expect_err("a directive in a DML access spec must refuse, not lower unprocessed");
    let msg = format!("{err}");
    assert!(
        msg.contains("predicate-position lowering is not yet supported"),
        "DML lowering guard must emit the effect/predicate/unsupported refusal \
         (RED-verifiable — deleting the mod.rs:1053 guard drops it): {msg}"
    );
}

// ============================================================================
// P1 closure matrix (INDUCTIVE-TRAVERSAL-PLAN R-I4 / R-I6)
// ============================================================================
//
// The two private walkers this phase removed (collect_ground_names_into detect
// + rename_ground_reads rewrite) shared a bug precisely because NOTHING forced
// their closures to coincide: detection could see a hole the rewrite missed
// (or vice versa). R-I6 replaces that with two CENTRALIZED recursion schemes
// (AstVisit for detect, AstTransform<P,P> for rewrite) whose equivalence is
// ENFORCED here — the SAME representative fixture, a bare Ground read beneath
// EVERY query-bearing edge, run through BOTH. If either scheme drops an edge,
// this test fails on that edge's name.
//
// This fixture covers the recursive carriers that can occur in unresolved
// effect plans. A bag operation's correlation predicate is descended by both
// generic walkers.

mod p1_closure_matrix {
    use super::super::walk::{
        collect_ground_names, make_pipe, named_ground_read, rename_ground_reads,
    };
    use super::super::ReceiptNaming;
    use crate::error::Result;
    use crate::pipeline::ast_unresolved::{Chain, Continuation, GroundMention, PipeOp, Relation};
    use crate::pipeline::ast_visit::{walk_visit_relational, AstVisit, Descent};
    use crate::pipeline::asts::core::expressions::metadata_types::{FilterOrigin, SetOperator};
    use crate::pipeline::asts::core::expressions::relational::InnerRelationPattern;
    use crate::pipeline::asts::core::{
        Access, Existence, GroundForm, Polarity, Probe, ProbeAddressing, QualifiedName,
        RelationalMembership, Step, Unresolved,
    };
    use crate::pipeline::asts::core::{DomainExpression, TruthExpression};

    fn qn(name: &str) -> QualifiedName {
        QualifiedName {
            namespace_path: crate::pipeline::ast_unresolved::NamespacePath::empty(),
            name: name.into(),
        }
    }

    fn join(left: Chain, right: Chain, cond: Option<TruthExpression>) -> Chain {
        left.then(Step::authored(Continuation::Member {
            rhs: right,
            correlation: cond.map(crate::pipeline::ast_unresolved::MemberCorrelation::Condition),
            join_type: None,
        }))
    }

    /// One bare Ground read beneath every query-bearing edge, each uniquely
    /// named after the edge it sits under. The union of these names is the
    /// closure both schemes must reach.
    fn every_edge_fixture() -> (Chain, Vec<&'static str>) {
        // Filter.source + Filter.condition (via InRelational subquery) — the
        // P1 headline hole.
        let filter =
            named_ground_read("g_filter_source").then(Step::authored(Continuation::Restrict {
                condition: TruthExpression::RelationalMembership(RelationalMembership {
                    probe: Probe::Value(Box::new(DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Open(
                            crate::pipeline::asts::core::DomainHole::Disregarded,
                        ),
                    ))),
                    relation: Box::new(named_ground_read("g_filter_condition")),
                    addressing: ProbeAddressing {
                        identifier: qn("f"),
                    },
                    negated: false,
                }),
                origin: FilterOrigin::UserWritten,
            }));

        // Join.left / Join.right / Join.correlation (via InnerExists).
        let join_with_cond = join(
            named_ground_read("g_join_left"),
            named_ground_read("g_join_right"),
            Some(TruthExpression::Existence(Existence {
                polarity: Polarity::Positive,
                relation: Box::new(named_ground_read("g_correlation")),
                addressing: ProbeAddressing {
                    identifier: qn("j"),
                },
            })),
        );

        // Pipe.source + a pipe-OPERATOR argument subquery (Transform → scalar
        // subquery): the edge missed by ALL relational-entry walkers today.
        let pipe = make_pipe(
            named_ground_read("g_pipe_source"),
            PipeOp::Transform { items: crate::pipeline::asts::vocabulary::Vec1::new(crate::pipeline::asts::core::NamedOutItem::authored(DomainExpression::Application(
                        crate::pipeline::asts::core::FunctionApplication::Scalarized(
                            crate::pipeline::asts::core::ScalarRelation::Named {
                                identifier: qn("s"),
                                body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                                    named_ground_read("g_operator_arg"),

                                        crate::pipeline::asts::core::Scalarization::BoundToOne {
                                            ordering: Vec::new(),
                                        },
                                )),
                            },
                        ),
                    ), "a".into(), None)), guard: None },
        );

        // SetOperation operand + an InnerRelation subquery.
        let setop = named_ground_read("g_setop_operand").bag_op(
            SetOperator::SmartUnionAll,
            Chain::authored(GroundForm::Reference(Relation::InnerRelation {
                pattern: InnerRelationPattern::Indeterminate {
                    identifier: qn("i"),
                    subquery: Box::new(named_ground_read("g_inner_relation")),
                },
                alias: None,
                outer: false,
            })),
            (),
        );

        let fixture = join(
            filter,
            join(join_with_cond, join(pipe, setop, None), None),
            None,
        );
        let names = vec![
            "g_filter_source",
            "g_filter_condition",
            "g_join_left",
            "g_join_right",
            "g_correlation",
            "g_pipe_source",
            "g_operator_arg",
            "g_setop_operand",
            "g_inner_relation",
        ];
        (fixture, names)
    }

    #[test]
    fn p1_closure_matrix_detection_and_rewrite_agree() {
        let (fixture, names) = every_edge_fixture();
        struct PlanScopeCollector {
            scopes: std::collections::HashSet<crate::names::ScopeId>,
        }
        impl AstVisit<Unresolved> for PlanScopeCollector {
            fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
                match relation {
                    Relation::Ground {
                        mention: GroundMention::Scratch { row },
                        ..
                    } => {
                        self.scopes.insert(row.relation().scope());
                    }
                    Relation::Ground {
                        mention: GroundMention::Receipt { receipt, .. },
                        ..
                    } => {
                        self.scopes.insert(receipt.row().relation().scope());
                    }
                    _ => {}
                }
                Ok(Descent::Continue)
            }
        }

        // --- Detection (AstVisit) reaches every edge. ---
        let detected = collect_ground_names(&fixture);
        for n in &names {
            assert!(
                detected.contains(*n),
                "P1 DETECTION (collect_ground_names) dropped edge `{n}`; saw: {:?}",
                detected
            );
        }

        // --- Rewrite (AstTransform<P,P>) reaches every edge, and detection
        // agrees. For each edge's read, renaming it must (a) make the old name
        // vanish and (b) introduce the snapshot name — proving the rewrite
        // reached that exact edge and the detector sees the substitution. That
        // the SAME name set drives both halves is the R-I6 coincidence. ---
        for n in &names {
            let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
            let snap = crate::relation::any_scratch(&identities);
            let rewritten = rename_ground_reads(
                fixture.clone(),
                crate::relation::NamedScratch::under(snap, (*n).into(), ReceiptNaming(())),
            );
            let after = collect_ground_names(&rewritten);
            assert!(
                !after.contains(*n),
                "P1 REWRITE (rename_ground_reads) failed to reach edge `{n}` \
                 (old name survived); after: {:?}",
                after
            );
            let mut scopes = PlanScopeCollector {
                scopes: std::collections::HashSet::new(),
            };
            walk_visit_relational(&mut scopes, &rewritten)
                .expect("plan-scope detection is infallible");
            assert!(scopes.scopes.contains(&snap.relation().scope()));
        }
    }

    #[test]
    fn plan_scope_rewrite_preserves_authored_access_shape() {
        let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let snap = crate::relation::any_scratch(&identities);
        let source = Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier: qn("valid"),
                    alias: Some("v".into()),
                    mutation_target: false,
                    passthrough: false,
                },
                outer: true,
            },
            Access::Unasked,
        );

        let rewritten = rename_ground_reads(
            source,
            crate::relation::NamedScratch::under(snap, "valid".into(), ReceiptNaming(())),
        );
        let access = rewritten
            .head_access()
            .cloned()
            .expect("the read carries its access");
        let Some(Relation::Ground {
            mention: GroundMention::Receipt { receipt, alias },
            outer,
            ..
        }) = rewritten.as_read_relation()
        else {
            panic!("the authored access should become an access-bearing receipt read")
        };
        let (scope, authored_name, alias, outer) = (
            receipt.row(),
            Some(receipt.name().clone()),
            alias.clone(),
            *outer,
        );
        assert_eq!(scope, snap);
        assert_eq!(authored_name.as_deref(), Some("valid"));
        assert!(matches!(access, Access::Unasked));
        assert_eq!(alias.as_deref(), Some("v"));
        assert!(outer);
    }

    #[test]
    fn matched_plan_scope_still_rewrites_reads_inside_its_access() {
        let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let snap = crate::relation::any_scratch(&identities);
        let source = Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier: qn("valid"),
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            Access::from_terms(vec![DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Scalarized(
                    crate::pipeline::asts::core::ScalarRelation::Named {
                        identifier: qn("probe"),
                        body: Box::new(crate::pipeline::asts::core::ScalarizedRelation::authored(
                            named_ground_read("valid"),
                            crate::pipeline::asts::core::Scalarization::BoundToOne {
                                ordering: Vec::new(),
                            },
                        )),
                    },
                ),
            )]),
        );
        let rewritten = rename_ground_reads(
            source,
            crate::relation::NamedScratch::under(snap, "valid".into(), ReceiptNaming(())),
        );
        struct CountPlanScopes(usize);
        impl AstVisit<Unresolved> for CountPlanScopes {
            fn enter_relation(&mut self, relation: &Relation) -> Result<Descent> {
                if matches!(
                    relation,
                    Relation::Ground {
                        mention: GroundMention::Receipt { .. },
                        ..
                    }
                ) {
                    self.0 += 1;
                }
                Ok(Descent::Continue)
            }
        }
        let mut count = CountPlanScopes(0);
        walk_visit_relational(&mut count, &rewritten).expect("plan-scope visit is infallible");
        assert_eq!(
            count.0, 2,
            "both the access root and its scalar-subquery read are rewritten"
        );
    }

    #[test]
    fn qualified_same_name_reads_are_outside_the_snapshot_rewrite() {
        let mut identifier = qn("valid");
        identifier.namespace_path =
            crate::pipeline::ast_unresolved::NamespacePath::single("source");
        let source = Chain::read(
            Relation::Ground {
                mention: GroundMention::Named {
                    identifier,
                    alias: None,
                    mutation_target: false,
                    passthrough: false,
                },
                outer: false,
            },
            Access::All,
        );
        assert!(
            collect_ground_names(&source).is_empty(),
            "hazard detection and rewrite share the unqualified-access boundary"
        );

        let identities = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let snap = crate::relation::any_scratch(&identities);
        assert!(matches!(
            rename_ground_reads(
                source,
                crate::relation::NamedScratch::under(snap, "valid".into(), ReceiptNaming(()))
            )
            .as_read_relation(),
            Some(Relation::Ground { .. })
        ));
    }
}
