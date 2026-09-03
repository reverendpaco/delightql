// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! TRUTH ENTERS VALUE ONCE, THEN COMPOSES AS A VALUE.
//!
//! The crossing is directed: a truth may be reified as a scalar value, while
//! an ordinary scalar does not acquire implicit truthiness. Reification is not
//! terminal, however. Once crossed, the truth must be accepted by every
//! recursively scalar container subject to that container's own laws.
//!
//! These tests deliberately use the production parser and normalizer without
//! naming the future AST carrier. They describe the semantic boundary the
//! carrier must eventually represent.

use super::support::{file, query, refusal, shows};

// ---------------------------------------------------------------------
// Existing direct crossings remain lawful
// ---------------------------------------------------------------------

#[test]
fn a_published_truth_is_a_value() {
    let published = query("users(*) |> (age, (age > 18) as adult)");
    assert!(shows(&published, "truth_expression:comparison"));
}

#[test]
fn a_group_key_truth_is_a_value() {
    let grouped = query("users(*) |> %((age > 18) as older ~> count:(*))");
    assert!(shows(&grouped, "truth_expression:comparison"));
}

#[test]
fn a_transform_truth_is_a_value() {
    let transformed = query("users(*) |> $$((age > 18) as age)");
    assert!(shows(&transformed, "truth_expression:comparison"));
}

#[test]
fn an_argument_truth_is_a_value() {
    let argued = query("users(*) |> (bump:(age > 5) as score)");
    assert!(shows(&argued, "truth_expression:comparison"));
}

#[test]
fn a_slot_constraint_truth_is_a_value() {
    let constrained = query(r#"users(("x" = "x"), b, _, _, _, _, _, _, _, _)"#);
    assert!(shows(&constrained, "truth_expression:comparison"));
}

#[test]
fn direct_existence_crossings_remain_values() {
    let published = query("users(*) |> (id, +orders(, user_id = id) as has)");
    assert!(shows(&published, "truth_expression:existence"));

    let argued = query("users(*) |> (bump:(+orders(, user_id = id)) as score)");
    assert!(shows(&argued, "truth_expression:existence"));

    let constrained = query("users(+orders(, total > 0), b, _, _, _, _, _, _, _, _)");
    assert!(shows(&constrained, "truth_expression:existence"));
}

// ---------------------------------------------------------------------
// Recursive scalar closure — intentionally red on the current tree
// ---------------------------------------------------------------------

/// Sigma and existence are both truth families. Neither spelling receives a
/// privileged comparison rule: reification makes either one a lawful scalar
/// operand.
#[test]
fn a_sigma_truth_composes_as_a_comparison_operand() {
    let compared = query("foo(*) |> ((+bar(x) = true) as barval)");
    assert!(shows(&compared, "truth_expression:sigma"));
    assert!(shows(&compared, "truth_expression:comparison"));
}

#[test]
fn an_existence_truth_composes_as_a_comparison_operand() {
    let compared = query("foo(*) |> ((+users(, x = y) = true) as users)");
    assert!(shows(&compared, "truth_expression:existence"));
    assert!(shows(&compared, "truth_expression:comparison"));
}

/// Arithmetic is an ordinary recursive scalar constructor. The numeric
/// compatibility of a truth value is a later type/target judgment; grammar and
/// normalization must first preserve the composed expression.
#[test]
fn an_existence_truth_composes_inside_arithmetic() {
    let nested = query("users(*) |> (id, 1 + (+orders(, user_id = id)) as n)");
    assert!(shows(&nested, "truth_expression:existence"));
}

/// A case result is not one of the historical terminal crossing containers.
/// It is the non-output scalar-container witness for closure.
#[test]
fn an_existence_truth_composes_as_a_case_result() {
    let nested = query("users(*) |> (id, _:(id > 5 -> +orders(, user_id = id) ; _ -> false) as x)");
    assert!(shows(&nested, "truth_expression:existence"));
}

/// Function bodies consume scalar values by the same induction law as query
/// expressions. A fact-function output must not need its own truth exception.
#[test]
fn an_existence_truth_composes_as_a_fact_function_output() {
    let normalized = file(concat!(
        "style_of(\n",
        "  variant -> style\n",
        "  ------------------\n",
        "  \"t\" -> (+orders(, user_id = variant));\n",
        "  _   -> \"grey\"\n",
        ")"
    ));
    assert_eq!(normalized.into_definitions().len(), 1);
}

// ---------------------------------------------------------------------
// The crossing remains one-way
// ---------------------------------------------------------------------

#[test]
fn an_ordinary_value_does_not_acquire_implicit_truthiness() {
    let refused = refusal("users(*), active");
    assert!(
        refused.to_ascii_lowercase().contains("parse"),
        "a bare value in truth position must remain structurally refused: {refused}"
    );
}
