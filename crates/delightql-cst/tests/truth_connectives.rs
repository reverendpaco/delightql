// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! Truth composition obeys ONE grammar in every truth position.
//!
//! NO PEMDAS at the connective tier: `and` and `or` have no precedence
//! between them, so an ungrouped mixture has no derivation in either order,
//! while either parenthesized reading derives with its own shape. A
//! TRUTH-ONLY position — a sigma body, a case-arm condition — reads the comma
//! as the conjunction `and` spells; a comma joining relational members is a
//! different position and stays one.

mod support;

use delightql_cst::cst;
use delightql_cst::Parser;
use support::{admits, admits_file, count, first, refuses_file, refuses_query};

/// The operands and separators of a conjunction node, as kind names.
fn conjunction_children(tree: &delightql_cst::SyntaxTree) -> Vec<&'static str> {
    first::<cst::ConjunctionExpression>(tree)
        .children()
        .map(|child| match child {
            cst::ConjunctionExpressionChild::AndKeyword(_) => "and",
            cst::ConjunctionExpressionChild::CommaSigil(_) => ",",
            cst::ConjunctionExpressionChild::Comparison(_) => "comparison",
            cst::ConjunctionExpressionChild::SigmaApplication(_) => "sigma",
            cst::ConjunctionExpressionChild::ParenthesizedTruth(_) => "parens",
            cst::ConjunctionExpressionChild::Negation(_) => "negation",
            cst::ConjunctionExpressionChild::Existence(_) => "existence",
            cst::ConjunctionExpressionChild::Membership(_) => "membership",
            cst::ConjunctionExpressionChild::RelationalMembership(_) => "relational_membership",
            cst::ConjunctionExpressionChild::HeadingCorrelation(_) => "correlation",
        })
        .collect()
}

/// THE MIXTURE IS RECOGNIZED TO BE REFUSED: a run mixing the two connectives
/// derives only as the witness node, in either order, never as a conjunction
/// or a disjunction that silently chose a reading.
#[test]
fn mixed_connectives_derive_only_as_the_witness_in_either_order() {
    for source in [
        "_(a @ 1), a = 1 or b = 1 and c = 1",
        "_(a @ 1), a = 1 and b = 1 or c = 1",
        "_(a @ 1), a = 1 or b = 1 and c = 1 or d = 1",
    ] {
        let tree = admits(source);
        assert_eq!(count::<cst::MixedConnectiveRun>(&tree), 1, "{source}");
        assert_eq!(count::<cst::ConjunctionExpression>(&tree), 0, "{source}");
        assert_eq!(count::<cst::DisjunctionExpression>(&tree), 0, "{source}");
    }
}

#[test]
fn either_grouped_reading_derives_with_its_own_shape() {
    let sql_reading = admits("_(a @ 1), a = 1 or (b = 1 and c = 1)");
    let member = first::<cst::CommaContinuation>(&sql_reading)
        .member()
        .expect("the comma member");
    assert!(
        matches!(
            member,
            cst::CommaContinuationMember::TruthExpression(
                cst::TruthExpression::DisjunctionExpression(_)
            )
        ),
        "`a or (b and c)` is a disjunction"
    );

    let left_reading = admits("_(a @ 1), (a = 1 or b = 1) and c = 1");
    assert_eq!(
        conjunction_children(&left_reading),
        ["parens", "and", "comparison"],
        "`(a or b) and c` is a conjunction whose first operand is the group"
    );
}

#[test]
fn one_connective_repeats_as_one_node() {
    let tree = admits("_(a @ 1), a = 1 and b = 1 and c = 1");
    assert_eq!(count::<cst::ConjunctionExpression>(&tree), 1);
    assert_eq!(
        conjunction_children(&tree),
        ["comparison", "and", "comparison", "and", "comparison"]
    );
}

#[test]
fn the_law_is_the_same_inside_a_sigma_body() {
    for source in [
        "ambiguous(x) :- x = 1 or x = 2 and x = 3",
        "ambiguous(x) :- x = 1 and x = 2 or x = 3",
    ] {
        let tree = admits_file(source);
        assert_eq!(count::<cst::MixedConnectiveRun>(&tree), 1, "{source}");
    }
    admits_file("selected(x) :- x = 1 or (x = 2 and x = 3)");
    admits_file("selected(x) :- (x = 1 or x = 2) and x = 3");
}

#[test]
fn a_sigma_body_reads_the_comma_as_conjunction() {
    let tree = admits_file("between_open(x) :- x > 0, x < 10");
    assert_eq!(conjunction_children(&tree), ["comparison", ",", "comparison"]);
}

#[test]
fn the_comma_composes_heterogeneous_sigma_leaves() {
    let tree = admits_file("positive(x) :- x > 0\nsmall_positive(x) :- +positive(x), x < 10");
    assert_eq!(conjunction_children(&tree), ["sigma", ",", "comparison"]);
}

#[test]
fn the_two_conjunction_spellings_mix_in_one_run() {
    let tree = admits_file("m(x) :- x > 0, x < 10 and x != 5");
    assert_eq!(count::<cst::ConjunctionExpression>(&tree), 1);
    assert_eq!(
        conjunction_children(&tree),
        ["comparison", ",", "comparison", "and", "comparison"]
    );
    let tree = admits_file("m(x) :- x > 0 and x < 10, x != 5");
    assert_eq!(count::<cst::ConjunctionExpression>(&tree), 1);
    assert_eq!(
        conjunction_children(&tree),
        ["comparison", "and", "comparison", ",", "comparison"]
    );
}

/// The comma IS a conjunction, so it never meets `or` ungrouped either.
#[test]
fn the_comma_never_meets_or_ungrouped() {
    refuses_file("m(x) :- x = 1 or x = 2, x = 3");
    refuses_file("m(x) :- x = 1, x = 2 or x = 3");
    admits_file("m(x) :- (x = 1 or x = 2), x = 3");
}

#[test]
fn an_arm_condition_reads_the_comma_as_conjunction() {
    let tree = admits("_(x @ 1) |> (_:(x > 0, x < 2 -> 1; _ -> 0) as y)");
    let condition = first::<cst::ArmCondition>(&tree)
        .child()
        .expect("the arm's condition");
    assert!(matches!(
        condition,
        cst::TruthExpression::ConjunctionExpression(_)
    ));
    assert_eq!(conjunction_children(&tree), ["comparison", ",", "comparison"]);
    refuses_query("_(x @ 1) |> (_:(x > 0 or x < 2, x = 1 -> 1; _ -> 0) as y)");
}

/// The comma joining relational members is a different position: two members,
/// no conjunction node.
#[test]
fn a_relational_comma_still_joins_members() {
    let tree = admits("users(*), a = 1, b = 2");
    assert_eq!(count::<cst::CommaContinuation>(&tree), 2);
    assert_eq!(count::<cst::ConjunctionExpression>(&tree), 0);
}

/// The `;` disjunction carries its own parens, so its arms are whole truths.
#[test]
fn the_semicolon_disjunction_keeps_whole_truths() {
    let tree = admits_file("m(x) :- (x = 1 and x = 2; x = 3)");
    assert_eq!(count::<cst::DisjunctionExpression>(&tree), 1);
    assert_eq!(conjunction_children(&tree), ["comparison", "and", "comparison"]);
}

/// WORDS ARE NOT RESERVED: a form named by a connective word may follow a
/// truth body, and the parser tells the two readings apart by what follows.
#[test]
fn a_keyword_named_form_may_follow_a_truth_body() {
    let tree = admits_file("p(x) :- x = 1\nand(*) :- users(*)\nor(x) :- x = 2\nhas(*) :- users(*)");
    assert_eq!(count::<cst::SigmaRule>(&tree), 2);
    assert_eq!(count::<cst::FoRule>(&tree), 2);
}

/// THE RUNTIME'S RECOVERY CAP IS PART OF THE GRAMMAR'S CONTRACT. The pinned
/// runtime, recovering at end of input with more than six live stack
/// versions, pauses a version instead of halting it and re-enters the same
/// recovery forever. A connective tier whose operand forks three ways after
/// every comparison drives a value-tier PONY input past that cap; the tier
/// is shaped so a comparison forks only where the crossing already forks it.
/// The witness parses under a cooperative step budget that a terminating
/// recovery never approaches.
#[test]
fn value_tier_pony_recovery_terminates_through_the_prompt_entrance() {
    for input in [
        "_(x @ 1), x * 2 + 1 = 3",
        "_(x @ 1), 1 + 2 * 3 = x",
        "users(*), a + 1 > 2 and b < 2",
        "_(a @ 1), a = 1 or b = 1 and c = ???",
    ] {
        let mut polls = 0usize;
        let mut budget = |_: usize| {
            polls += 1;
            polls > 100_000
        };
        match Parser::new().parse_prompt_cancellable(input, &mut budget) {
            delightql_cst::CancellableParse::Completed(tree) => {
                assert!(tree.has_defects(), "{input:?} must refuse");
            }
            delightql_cst::CancellableParse::Cancelled { .. } => {
                panic!("recovery of {input:?} did not terminate within the step budget")
            }
        }
    }
}
