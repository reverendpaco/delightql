// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE CROSSING IS BUILT WHERE IT IS WRITTEN.
//!
//! Truth reaches value position at exactly three places, and each carries it
//! in its own type. These pins read the PRODUCTION unresolved tree, because a
//! carrier that exists but is never constructed is a claim the tree does not
//! make: the position's type would say "a truth may stand here" while the
//! actual node was something else.
//!
//! Each licensed position gets a positive pin naming its carrier. The FENCE
//! below is the other half: no value anywhere in the tree — at any phase — is
//! a truth read as a value, so the crossing's three homes are all of them.

use super::support::{query, refusal, shows};

/// The wrapper every crossing travels in, whatever position admits it.
const CROSSING: &str = "truth_as_value";

// ---------------------------------------------------------------------
// Publication
// ---------------------------------------------------------------------

/// An out item's value admits the crossing, and says so in its own carrier.
#[test]
fn a_published_crossing_is_an_out_value_truth() {
    let published = query("users(*) |> (age, (age > 18) as adult)");
    assert!(shows(&published, "out_value:truth"));
    assert!(shows(&published, CROSSING));
    assert!(shows(&published, "truth_expression:comparison"));
    // The domain arm is still the ordinary road for the value beside it.
    assert!(shows(&published, "out_value:domain"));
}

/// A group key is a publication position, so it admits the same crossing.
#[test]
fn a_group_key_crossing_is_an_out_value_truth() {
    let grouped = query("users(*) |> %((age > 18) as older ~> count:(*))");
    assert!(shows(&grouped, "out_value:truth"));
    assert!(shows(&grouped, CROSSING));
}

/// A transform writes into a column it names, and its value admits the
/// crossing too.
#[test]
fn a_transform_crossing_is_an_out_value_truth() {
    let transformed = query("users(*) |> $$((age > 18) as age)");
    assert!(shows(&transformed, "out_value:truth"));
    assert!(shows(&transformed, CROSSING));
}

// ---------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------

/// An argument's value admits the crossing, and DISTINCT is the argument's
/// own datum beside it.
#[test]
fn an_argued_crossing_is_an_argument_value_truth() {
    let argued = query("users(*) |> (bump:(age > 5) as score)");
    assert!(shows(&argued, "argument_value:truth"));
    assert!(shows(&argued, CROSSING));
}

// ---------------------------------------------------------------------
// Slot constraints
// ---------------------------------------------------------------------

/// A caller-pattern slot constrains its column with the crossing — a VALUE
/// the column unifies with, never a predicate over the row.
#[test]
fn a_slot_crossing_is_a_slot_constraint_truth() {
    let constrained = query(r#"users(("x" = "x"), b, _, _, _, _, _, _, _, _)"#);
    assert!(shows(&constrained, "slot_constraint:truth"));
    assert!(shows(&constrained, CROSSING));
}

// ---------------------------------------------------------------------
// The pre-carved existence spelling
// ---------------------------------------------------------------------

/// `+f( … )` in value position is the crossing wearing its own surface —
/// one occurrence, one carrier — so the position that admits it builds the
/// position's crossing and not a second thing that means the same.
#[test]
fn a_published_existence_is_the_positions_crossing() {
    let published = query("users(*) |> (id, +orders(, user_id = id) as has)");
    assert!(shows(&published, "out_value:truth"));
    assert!(shows(&published, CROSSING));
    assert!(shows(&published, "truth_expression:existence"));
}

#[test]
fn an_argued_existence_is_the_arguments_crossing() {
    let argued = query("users(*) |> (bump:(+orders(, user_id = id)) as score)");
    assert!(shows(&argued, "argument_value:truth"));
    assert!(shows(&argued, CROSSING));
    assert!(shows(&argued, "truth_expression:existence"));
}

#[test]
fn a_slotted_existence_is_the_slots_crossing() {
    let constrained = query("users(+orders(, total > 0), b, _, _, _, _, _, _, _, _)");
    assert!(shows(&constrained, "slot_constraint:truth"));
    assert!(shows(&constrained, CROSSING));
    assert!(shows(&constrained, "truth_expression:existence"));
}

/// And NOWHERE ELSE. A general value position derives no adapter, so the
/// same spelling standing inside an arithmetic operand has no reading.
#[test]
fn a_general_value_position_refuses_the_existence_spelling() {
    let refused = refusal("users(*) |> (id, 1 + (+orders(, user_id = id)) as n)");
    assert!(
        refused.contains("existence entering value position is the crossing"),
        "a nested existence should name the crossing law: {refused}"
    );
}

/// AN AUTHORED CASE RESULT IS A DOMAIN EXPRESSION. The grammar's
/// `match_arm`, `searched_arm` and `default_arm` all end in one, and the
/// crossing's positions are enumerated without it — so the same spelling
/// that publishes fine has no reading here.
#[test]
fn an_authored_case_result_refuses_the_existence_spelling() {
    let refused =
        refusal("users(*) |> (id, _:(id > 5 -> +orders(, user_id = id) ; _ -> false) as x)");
    assert!(
        refused.contains("existence entering value position is the crossing"),
        "a case result should name the crossing law: {refused}"
    );
}

/// And a fact function's OUTPUT is the same production, so it answers the
/// same way. A multi-clause value rule assembles into its own selection
/// carrier precisely so that neither of these has to widen to make room.
#[test]
fn a_fact_function_output_refuses_the_existence_spelling() {
    let refused = super::support::file_refusal(concat!(
        "style_of(\n",
        "  variant -> style\n",
        "  ------------------\n",
        "  \"t\" -> (+orders(, user_id = variant));\n",
        "  _   -> \"grey\"\n",
        ")"
    ));
    assert!(
        refused.contains("existence entering value position is the crossing"),
        "a fact-function output should name the crossing law: {refused}"
    );
}

// ---------------------------------------------------------------------
// THE FENCE — no value, in any phase, is a truth read as a value
// ---------------------------------------------------------------------

/// The unresolved boundary: every crossing the tree holds sits in one of the
/// three carriers, and no `DomainExpression` reifies a truth.
///
/// The needle is the crossing's own rendering. Because a `DomainExpression`
/// has no variant that can hold one, every occurrence of it in a rendered
/// tree is preceded by the position that admits it — and this walks the
/// rendering to say so, rather than trusting the enum.
#[test]
fn every_crossing_in_an_unresolved_tree_sits_at_a_licensed_position() {
    for source in [
        "users(*) |> (age, (age > 18) as adult)",
        "users(*) |> %((age > 18) as older ~> count:(*))",
        "users(*) |> $$((age > 18) as age)",
        "users(*) |> (bump:(age > 5) as score)",
        r#"users(("x" = "x"), b, _, _, _, _, _, _, _, _)"#,
        "users(*) |> (id, +orders(, user_id = id) as has)",
        "users(+orders(, total > 0), b, _, _, _, _, _, _, _, _)",
    ] {
        let built = query(source);
        let rendered: String = super::support::lispy(&built)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            rendered.contains(CROSSING),
            "{source:?} should carry the crossing"
        );
        for (at, _) in rendered.match_indices(CROSSING) {
            // The nearest position tag standing to the crossing's left is
            // the position that admitted it, and every one of the three
            // renders its crossing arm as `<position>:truth`.
            let before = &rendered[..at];
            let nearest = ["out_value:", "argument_value:", "slot_constraint:"]
                .iter()
                .filter_map(|tag| before.rfind(tag).map(|from| &before[from..]))
                .min_by_key(|tail| tail.len());
            assert!(
                nearest.is_some_and(|tail| tail.starts_with("out_value:truth")
                    || tail.starts_with("argument_value:truth")
                    || tail.starts_with("slot_constraint:truth")),
                "{source:?} carries a crossing at an unlicensed position: …{}",
                &before[before.len().saturating_sub(80)..]
            );
        }
    }
}
