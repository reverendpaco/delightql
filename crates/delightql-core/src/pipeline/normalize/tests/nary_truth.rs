// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE N-ARY CARRIER IS CANONICAL.
//!
//! `and` and `or` are associative, so a same-operator nest is the same truth
//! written with extra brackets. The AST holds ONE node per operator run: a
//! consumer reads its members and never has to decide whether the member it
//! is holding is itself the same composition continued.
//!
//! Two roads reach the carrier and both are pinned, because a splice on one
//! of them is not the law: authored parentheses arrive through
//! normalization, and a programmatic caller arrives through the smart
//! constructor.
//!
//! EVERY SUPPORTED CONSTRUCTOR PRODUCES THE CANONICAL SHAPE, and `all` and
//! `any` are all of them. A binary `and`/`or` pair beside them took two
//! members and made a two-member node, declining the splice; it had no
//! caller and is deleted, with a construction fence saying so. The variants
//! themselves stay constructible inside the crate — the carrier is not
//! opaque — so the claim is about the doors, not about what a `match` arm
//! could assemble by hand.

use super::support::query;
use crate::pipeline::asts::core::{
    Comparison, Continuation, DomainExpression, LiteralValue, TruthExpression, Unresolved,
};

type Truth = TruthExpression<Unresolved>;

/// The one restriction a single-comma query states.
fn restriction(source: &str) -> Truth {
    let (_, continuations) = query(source).body.into_parts();
    let mut found: Vec<Truth> = continuations
        .into_iter()
        .filter_map(|continuation| match continuation.into_form() {
            Continuation::Restrict { condition, .. } => Some(condition),
            _ => None,
        })
        .collect();
    assert_eq!(found.len(), 1, "{source:?} states one restriction");
    found.remove(0)
}

fn ground(n: &str) -> DomainExpression<Unresolved> {
    DomainExpression::Application(crate::pipeline::asts::core::FunctionApplication::Ground(
        LiteralValue::Number(n.to_string()),
    ))
}

/// A comparison whose content does not matter, distinguishable by its
/// operands so the members of a composition can be told apart.
fn leaf(n: &str) -> Truth {
    TruthExpression::Comparison(Comparison {
        operator: crate::pipeline::asts::vocabulary::CmpOp::Equal,
        left: Box::new(ground(n)),
        right: Box::new(ground(n)),
    })
}

/// The members of a conjunction, or a panic naming what stood there.
fn conjuncts(truth: &Truth) -> Vec<&Truth> {
    match truth {
        TruthExpression::Conjunction(parts) => parts.iter().collect(),
        other => panic!("expected one conjunction, got {other:?}"),
    }
}

fn disjuncts(truth: &Truth) -> Vec<&Truth> {
    match truth {
        TruthExpression::Disjunction(parts) => parts.iter().collect(),
        other => panic!("expected one disjunction, got {other:?}"),
    }
}

/// No member of a composition is that composition continued.
fn no_member_repeats(truth: &Truth) {
    let members: Vec<&Truth> = match truth {
        TruthExpression::Conjunction(parts) => parts.iter().collect(),
        TruthExpression::Disjunction(parts) => parts.iter().collect(),
        other => panic!("expected a composition, got {other:?}"),
    };
    let same_kind =
        |member: &&Truth| std::mem::discriminant(*member) == std::mem::discriminant(truth);
    assert!(
        !members.iter().any(same_kind),
        "a same-kind member survived the splice: {truth:?}"
    );
}

// ---------------------------------------------------------------------
// Authored parentheses
// ---------------------------------------------------------------------

/// `(a or b) or c` is ONE disjunction of three. The brackets are the CST's
/// to remember.
#[test]
fn parenthesized_disjunction_is_one_three_member_node() {
    let truth = restriction("users(*), (age > 0 or age < 2) or age = 1");
    assert_eq!(disjuncts(&truth).len(), 3);
    no_member_repeats(&truth);
}

/// The same on the other side of the parentheses, so the pin is not reading
/// a left-fold accident.
#[test]
fn a_trailing_parenthesized_disjunction_splices_too() {
    let truth = restriction("users(*), age = 1 or (age > 0 or age < 2)");
    assert_eq!(disjuncts(&truth).len(), 3);
    no_member_repeats(&truth);
}

/// `and` is the mirror. Its comma spelling already splits into separate
/// continuations, so the pin writes the keyword.
#[test]
fn parenthesized_conjunction_is_one_three_member_node() {
    let truth = restriction("users(*), (age > 0 and age < 2) and age = 1");
    assert_eq!(conjuncts(&truth).len(), 3);
    no_member_repeats(&truth);
}

/// THE OPPOSITE OPERATOR IS AN ORDINARY MEMBER. `or` inside `and` is one
/// member of the conjunction, not three; splicing it would change what the
/// query means.
#[test]
fn the_opposite_operator_stays_one_member() {
    let truth = restriction("users(*), (age > 0 or age < 2) and age = 1");
    let members = conjuncts(&truth);
    assert_eq!(members.len(), 2);
    assert_eq!(disjuncts(&members[0]).len(), 2);
}

/// And a negation is a member like any other: `!( … )` is Kleene NOT, not a
/// bracket around a continued run.
#[test]
fn a_negation_stays_one_member() {
    let truth = restriction("users(*), !(age > 0 or age < 2) or age = 1");
    let members = disjuncts(&truth);
    assert_eq!(members.len(), 2);
    assert!(matches!(members[0], TruthExpression::Not { .. }));
}

// ---------------------------------------------------------------------
// The smart constructors
// ---------------------------------------------------------------------

/// A programmatic caller reaches the same canonical node. Without this the
/// carrier would be canonical only for what an author happened to write.
#[test]
fn all_splices_a_conjunction_handed_to_it() {
    let nested = TruthExpression::all(vec![leaf("1"), leaf("2")]).expect("two parts conjoin");
    let combined = TruthExpression::all(vec![nested, leaf("3")]).expect("three parts conjoin");
    assert_eq!(conjuncts(&combined).len(), 3);
    no_member_repeats(&combined);
}

#[test]
fn any_splices_a_disjunction_handed_to_it() {
    let nested = TruthExpression::any(vec![leaf("1"), leaf("2")]).expect("two parts disjoin");
    let combined = TruthExpression::any(vec![nested, leaf("3")]).expect("three parts disjoin");
    assert_eq!(disjuncts(&combined).len(), 3);
    no_member_repeats(&combined);
}

/// The splice does not cross operators in either direction.
#[test]
fn neither_constructor_splices_the_other_operator() {
    let disjunction = TruthExpression::any(vec![leaf("1"), leaf("2")]).expect("two parts disjoin");
    let conjoined = TruthExpression::all(vec![disjunction, leaf("3")]).expect("two parts conjoin");
    assert_eq!(conjuncts(&conjoined).len(), 2);

    let conjunction = TruthExpression::all(vec![leaf("1"), leaf("2")]).expect("two parts conjoin");
    let disjoined = TruthExpression::any(vec![conjunction, leaf("3")]).expect("two parts disjoin");
    assert_eq!(disjuncts(&disjoined).len(), 2);
}

/// A run assembled BY GROWTH is canonical at every step, which is what a
/// caller that folds over a list of parts is doing. Without this the pins
/// above would only cover a run built in one call.
#[test]
fn folding_parts_one_at_a_time_stays_canonical() {
    let mut conjoined = leaf("1");
    for part in ["2", "3", "4"] {
        conjoined = TruthExpression::all(vec![conjoined, leaf(part)]).expect("two parts conjoin");
    }
    assert_eq!(conjuncts(&conjoined).len(), 4);
    no_member_repeats(&conjoined);

    let mut disjoined = leaf("1");
    for part in ["2", "3", "4"] {
        disjoined = TruthExpression::any(vec![disjoined, leaf(part)]).expect("two parts disjoin");
    }
    assert_eq!(disjuncts(&disjoined).len(), 4);
    no_member_repeats(&disjoined);
}

/// A splice can collapse a run to ONE member, and one member IS that member
/// — there is no one-member composition to hand back.
#[test]
fn a_spliced_singleton_is_the_member_itself() {
    let sole = TruthExpression::all(vec![leaf("1")]).expect("one part is that part");
    let again = TruthExpression::all(vec![sole]).expect("splicing one member yields it");
    assert!(matches!(again, TruthExpression::Comparison(_)));
    assert!(TruthExpression::<Unresolved>::all(vec![]).is_none());
    assert!(TruthExpression::<Unresolved>::any(vec![]).is_none());
}
