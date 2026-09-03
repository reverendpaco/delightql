// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Per-walk pins for the chain's two shared traversals.
//!
//! `source_spine` and `fold_tail` are boundaries the resolver, refiner,
//! pipeline and effect code all stand on: each answers a question about the
//! OUTER chain and must not drift into the relations a continuation brings
//! in. Reaching further would answer with the wrong base relation, the wrong
//! operator, or the wrong ending. The corpus does not pin these directly —
//! a walk that over-reaches still compiles and usually still produces SQL.

use super::Step;
use super::*;
use crate::pipeline::asts::core::expressions::metadata_types::{FilterOrigin, SetOperator};
use crate::pipeline::asts::core::metadata::NamespacePath;
use crate::pipeline::asts::core::{
    Access, FunctorCall, GroundForm, GroundMention, QualifiedName, Relation, TruthExpression,
    Unresolved,
};

// --- fixtures -------------------------------------------------------------

/// A recognizable head: a functor call whose name is the tag.
fn sentinel(tag: &str) -> Chain<Unresolved> {
    Chain::authored(GroundForm::Reference(Relation::FunctorCall {
        alias: None,
        call: FunctorCall::written(
            crate::pipeline::asts::vocabulary::Ref::synthetic_with_display(
                &std::rc::Rc::new(crate::names::Registry::new(&[])),
                crate::pipeline::asts::vocabulary::SyntheticReason::EffectReceipt,
                tag,
            ),
            vec![],
        )
        .into(),
    }))
}

fn restrict(chain: Chain<Unresolved>) -> Chain<Unresolved> {
    chain.then(Step::authored(Continuation::Restrict {
        // A restriction whose CONTENT is irrelevant to the spine walk —
        // only that one stands here. There is no synthetic truth leaf to
        // reach for, so it is a real comparison.
        condition: TruthExpression::Comparison(crate::pipeline::asts::core::Comparison {
            operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
            left: Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Number("1".into()),
                ),
            )),
            right: Box::new(DomainExpression::Application(
                crate::pipeline::asts::core::FunctionApplication::Ground(
                    crate::pipeline::asts::core::LiteralValue::Number("1".into()),
                ),
            )),
        }),
        origin: FilterOrigin::UserWritten,
    }))
}

fn access(chain: Chain<Unresolved>, access: Access<Unresolved>) -> Chain<Unresolved> {
    chain.then(Step::authored(Continuation::Access {
        access,
        named: None,
    }))
}

fn member(left: Chain<Unresolved>, right: Chain<Unresolved>) -> Chain<Unresolved> {
    left.then(Step::authored(Continuation::Member {
        rhs: right,
        correlation: None,
        join_type: None,
    }))
}

fn bag(operands: Vec<Chain<Unresolved>>) -> Chain<Unresolved> {
    let mut operands = operands.into_iter();
    let mut accumulated = operands.next().expect("a bag operation has a left operand");
    for arm in operands {
        accumulated = accumulated.bag_op(SetOperator::UnionCorresponding, arm, ());
    }
    accumulated
}

/// The tag a head carries, when it is one of this module's sentinels.
fn head_tag(chain: &Chain<Unresolved>) -> Option<String> {
    match chain.as_bare_relation()? {
        Relation::FunctorCall { call, .. } => Some(call.call().callee.name_text()),
        _ => None,
    }
}

// --- source_spine ---------------------------------------------------------

#[test]
fn source_spine_reads_restrictions_and_pipes_outermost_first() {
    // `sentinel * ^ , ` — two pipes then a restriction. The run is
    // deliberately ASYMMETRIC: a palindromic one reads the same in either
    // direction and would pin nothing about order.
    let chain = restrict(access(sentinel("term"), Access::All).then(Step::authored(
        Continuation::Structural(StructuralStep {
            form: StructuralForm::Meta,
            named: None,
        }),
    )));

    // Read from the OUTSIDE in: the last continuation written is the first
    // step seen, which is what makes "the top-level operator" answerable.
    let shape: Vec<char> = chain
        .source_spine()
        .map(|step| match step {
            SpineStep::Restrict(_) => 'r',
            SpineStep::Correlate(_) => 'c',
            SpineStep::Bound(_) => 'b',
            SpineStep::Destructure => 'd',
            SpineStep::Pipe(_) => 'p',
            SpineStep::Access(_) => 'a',
            SpineStep::Structural(form) => match form {
                StructuralForm::Ordering { .. } => 'o',
                StructuralForm::Reposition { .. } => 'z',
                StructuralForm::Meta => 'm',
                StructuralForm::Witness { .. } => 'w',
                StructuralForm::SignedWitness => 's',
                StructuralForm::Drill { .. } => 'x',
                StructuralForm::Narrow { .. } => 'n',
            },
        })
        .collect();
    assert_eq!(shape, vec!['r', 'm', 'a']);

    // Every continuation was shaping, so the walk reached the head.
    assert_eq!(chain.source_spine().count(), chain.continuations.len());
    assert_eq!(
        head_tag(&Chain::ground(chain.head)).as_deref(),
        Some("term")
    );
}

#[test]
fn source_spine_stops_at_a_member_without_entering_either_relation() {
    // `left , right , ` — the member brings another relation in, so the walk
    // stops AT it: neither the member's own chain nor the prefix below it is
    // part of this chain's shaping run.
    let chain = restrict(member(
        restrict(sentinel("left")),
        restrict(sentinel("right")),
    ));

    // Exactly the one outer restriction — not the member's, not the prefix's.
    assert_eq!(chain.source_spine().count(), 1);
    assert!(chain.source_spine().count() < chain.continuations.len());
}

#[test]
fn source_spine_stops_at_a_bag_operation_and_at_an_edge() {
    let over_bag = restrict(bag(vec![sentinel("a"), sentinel("b")]));
    assert_eq!(over_bag.source_spine().count(), 1);

    let over_edge = restrict(sentinel("a").then(Step::authored(Continuation::ErJoin(
        ErJoinStep {
            transitive: false,
            context: Some("ctx".to_string()),
            left_spelling: "a".to_string(),
            right_spelling: "b".to_string(),
            rhs: Chain::read(
                Relation::Ground {
                    mention: GroundMention::named(QualifiedName {
                        namespace_path: NamespacePath::empty(),
                        name: "b".into(),
                    }),
                    outer: false,
                },
                Access::All,
            ),
        },
    ))));
    assert_eq!(over_edge.source_spine().count(), 1);
}

// --- fold_tail ------------------------------------------------------------

/// The tags of every tail LEAF `fold_tail` hands to `leaf`, flattening bag
/// arms — the exact set the ledger-tail readers fold over.
fn tail_tags(chain: &Chain<Unresolved>) -> Vec<String> {
    chain.fold_tail(
        &|leaf: &Chain<Unresolved>| head_tag(leaf).into_iter().collect::<Vec<_>>(),
        &|arms: Vec<Vec<String>>| arms.into_iter().flatten().collect(),
    )
}

#[test]
fn fold_tail_ends_in_a_members_right_hand_chain() {
    // A chain ends where its LAST member ends; the left operand is never a
    // tail, so a directive standing there does not make the chain end in one.
    assert_eq!(
        tail_tags(&member(sentinel("left_ignored"), sentinel("right_kept"))),
        vec!["right_kept".to_string()]
    );

    // The member's own chain is followed to ITS ending, not just its head.
    assert_eq!(
        tail_tags(&member(
            sentinel("left_ignored"),
            member(sentinel("inner_ignored"), sentinel("innermost")),
        )),
        vec!["innermost".to_string()]
    );
}

#[test]
fn fold_tail_folds_every_bag_arm_left_operand_first() {
    // Every arm is an ending, in written order — and the chain-so-far is
    // arm 0, so a union's first operand is folded before the arms after it.
    assert_eq!(
        tail_tags(&bag(vec![sentinel("a"), sentinel("b"), sentinel("c")])),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    // Nested: the left operand's own ending is what arm 0 contributes.
    assert_eq!(
        tail_tags(&bag(vec![
            member(sentinel("x_ignored"), sentinel("keep1")),
            sentinel("keep2"),
        ])),
        vec!["keep1".to_string(), "keep2".to_string()]
    );
}

#[test]
fn fold_tail_treats_a_trailing_restriction_as_an_opaque_leaf() {
    // A trailing restriction is handed to `leaf` WHOLE — the fold does not
    // peel it, so the relation under it is not the chain's ending. Peeling
    // would make `a(*) |> stdout!(*), x = 1` look like it ends in a
    // directive when the restriction is what stands last.
    assert!(tail_tags(&restrict(sentinel("hidden"))).is_empty());

    // A trailing access is a leaf for the same reason.
    assert!(tail_tags(&access(sentinel("hidden"), Access::All)).is_empty());

    // The bare head IS its own ending.
    assert_eq!(tail_tags(&sentinel("bare")), vec!["bare".to_string()]);
}

#[test]
fn fold_tail_hands_the_leaf_the_whole_chain() {
    // "Wholesale" is load-bearing: the leaf receives the chain, not just its
    // head, so a caller that must inspect the trailing operator can.
    let chain = access(sentinel("base"), Access::All);
    let saw_operator = chain.fold_tail(
        &|leaf: &Chain<Unresolved>| {
            matches!(
                leaf.continuations.last().map(|step| step.form()),
                Some(Continuation::Access {
                    access: Access::All,
                    ..
                })
            )
        },
        &|arms: Vec<bool>| arms.into_iter().all(|arm| arm),
    );
    assert!(saw_operator);
}

/// A pipe's authored name lives in the authored phase and nowhere else.
///
/// The type is the real pin and it is in `phases.rs`: past resolution the
/// slot IS `()`, so `Continuation::<Refined>::Pipe { named: Some(…) }` is
/// not a value anyone can write and no lowering has a spelling to read. What
/// cannot be said in the type is what happens to a fold that arrives at a
/// spent phase still holding one — dropping it would leave a named stage
/// unreachable by its own name, and looking the other way is how a second
/// carrier drifts from the scope that owns the answer. It refuses.
#[test]
fn a_spent_phase_refuses_an_authored_stage_name_rather_than_dropping_it() {
    use crate::pipeline::asts::core::{Phase, Refined, Resolved};

    let written = delightql_types::SqlIdentifier::new("f");

    assert_eq!(
        Unresolved::admit_stage_name(Some(written.clone())).unwrap(),
        Some(written.clone())
    );

    for refusal in [
        Resolved::admit_stage_name(Some(written.clone())).err(),
        Refined::admit_stage_name(Some(written.clone())).err(),
        Refined::admit_stage_name(Some(written.clone())).err(),
    ] {
        let refusal = refusal.expect("a spent phase has nowhere to put a name");
        assert!(
            refusal.to_string().contains("already spent it"),
            "should name what went wrong: {refusal}"
        );
    }

    // An UNNAMED pipe crosses every phase, because that is the ordinary
    // case and not a name at all.
    assert!(Resolved::admit_stage_name(None).is_ok());
    assert!(Refined::admit_stage_name(None).is_ok());
}

/// THE PARTITION IS THE ONE MEMBERSHIP ANSWER, and it is one OPERATION:
/// `pop_run_step` takes a run step off as the exact family, and restores a
/// nonmember unchanged — so no consumer holds a boolean membership list
/// beside it, and there is no second answer to disagree with.
#[test]
fn the_run_partition_is_one_operation() {
    let mut running =
        sentinel("base").then(Step::authored(Continuation::Structural(StructuralStep {
            form: StructuralForm::Ordering {
                specs: vec![],
                bound: None,
            },
            named: None,
        })));
    assert!(matches!(
        running.pop_run_step().map(|step| step.into_form()),
        Some(RunForm::Structural(StructuralStep {
            form: StructuralForm::Ordering { .. },
            ..
        }))
    ));
    assert!(running.continuations.is_empty());
    // The base has no further run step, and nothing was taken.
    assert!(running.pop_run_step().is_none());

    let mut bounded = sentinel("base").then(Step::authored(Continuation::Bound {
        bound: crate::pipeline::asts::core::specs::TupleOrdinalClause {
            operator: crate::pipeline::asts::core::specs::TupleOrdinalOperator::LessThan,
            value: 2,
            offset: None,
        },
    }));
    // A nonmember is RESTORED, not consumed: the chain is unchanged.
    assert!(bounded.pop_run_step().is_none());
    assert!(matches!(
        bounded.continuations.last().map(|step| step.form()),
        Some(Continuation::Bound { .. })
    ));
}
