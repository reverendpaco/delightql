// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! A HOOK BELONGS TO THE QUERY IT STANDS IN.
//!
//! The road these pin is the EARLY refusal — one raised before a goal exists
//! to carry its own declaration. Every case here is a submission of two
//! forms, because a submission of one cannot tell an owned declaration from a
//! borrowed one.

use crate::pipeline::normalize::{declared_error_within, Entrance, Normalizer};
use crate::pipeline::parse;

/// What the production roads decide, without a session: parse the sequence,
/// and on a refusal ask whether the form that refused DECLARED it — the same
/// weighing the relay performs against `declared_error_within`, and nothing
/// wider than the owning form's extent. A normalization refusal is owned by
/// the form the walk was BUILDING when it refused, which the normalizer
/// records for exactly this weighing.
fn judged(source: &str) -> Option<String> {
    let registry = std::rc::Rc::new(crate::names::Registry::new(&[]));
    let (tree, owner, error) =
        match parse::query_sequence_attributed(source, registry.limits().nesting()) {
            Err(refusal) => (refusal.tree, refusal.query, refusal.error),
            Ok(tree) => {
                let mut normalizer = Normalizer::new(&tree, registry);
                match normalizer.run_into(Entrance::QuerySequence) {
                    Ok(()) => panic!("this submission is supposed to refuse: {source:?}"),
                    Err(error) => {
                        let owner = normalizer.building.clone();
                        drop(normalizer);
                        (tree, owner, error)
                    }
                }
            }
        };
    // FAIL CLOSED: no owning form means no declaration is read at all. The
    // alternative — reading the submission — is what lets a later query claim
    // an earlier query's outcome.
    let owner = owner?;
    let expected = declared_error_within(&tree, &owner)?;
    expected
        .matches(&error.error_uri())
        .then(|| expected.display_uri())
}

/// The refusal the first form makes, spelled so the tests read as one
/// thing: a sparse column filled twice in one row.
const REFUSES: &str = "_(a, b? @ 1, _(b @ 2), _(b @ 3))";
const DECLARES: &str = "(~~error://semantic/anon/sparse_duplicate ~~)";
const HOOK_URI: &str = "error://semantic/anon/sparse_duplicate";

#[test]
fn a_query_that_declares_its_own_refusal_is_judged_by_it() {
    assert_eq!(
        judged(&format!("{REFUSES} {DECLARES}\n_(x @ 9)")).as_deref(),
        Some(HOOK_URI),
    );
}

#[test]
fn a_later_hook_does_not_catch_an_earlier_refusal() {
    assert_eq!(judged(&format!("{REFUSES}\n_(x @ 9) {DECLARES}")), None);
}

#[test]
fn an_earlier_hook_does_not_catch_a_later_refusal() {
    assert_eq!(judged(&format!("_(x @ 9) {DECLARES}\n{REFUSES}")), None);
}

/// Two declarations are two forms' business. Reading the submission would
/// find two and give up; reading ONE form finds that form's.
#[test]
fn two_queries_may_each_declare_without_erasing_the_other() {
    assert_eq!(
        judged(&format!(
            "{REFUSES} {DECLARES}\n_(a @ 2, 3) (~~error://parse/anon ~~)"
        ))
        .as_deref(),
        Some(HOOK_URI),
    );
    assert_eq!(
        judged(&format!(
            "_(a @ 2, 3) (~~error://parse/anon ~~)\n{REFUSES} {DECLARES}"
        ))
        .as_deref(),
        Some("error://parse/anon"),
    );
}

/// However the hook is read, it is read inside the owning form and
/// nowhere else.
#[test]
fn a_siblings_hook_is_never_the_failing_forms() {
    let refuses = "employees!!(*), employees!!(*) as e2, employees.id = e2.id \
                   |> delete!(employees(*))(*)";
    let hook = "(~~error://dml/marker/multiple ~~)";
    assert_eq!(
        judged(&format!("{refuses} {hook}")).as_deref(),
        Some("error://dml/marker/multiple"),
        "the hook the failing form wrote is found through its tokens",
    );
    assert_eq!(
        judged(&format!("{refuses}\n_(x @ 1) {hook}")),
        None,
        "a sibling's hook is not the failing form's, however it is read",
    );
}

/// DIAGNOSIS AND OWNERSHIP ARE ONE ACT. The teaching patterns key on the
/// tokens the author typed, so a scan wider than the failing form reports
/// what a SIBLING form spelled — and the first defect's form then catches
/// an identity it never produced. Here query one supplies the defect (a
/// second mutation mark) and query two supplies the bracket accessor.
#[test]
fn a_sibling_form_does_not_supply_the_failing_form_s_teaching() {
    let source = "employees!!(*), employees!!(*) as e2, employees.id = e2.id\n  \
                  |> delete!(employees(*))(*)\n  (~~error://parse/path_variable ~~)\n\
                  users(*) |> (arr:[1])";
    assert_eq!(judged(source), None);
}

/// The mirror, over the SAME two forms: the failing form spells its own
/// teaching and declares it. A wider scan would still find the sibling's
/// bracket accessor — the diagnoses are tried in order and that one is
/// tried first — so this passing is what says the narrower scan decided.
#[test]
fn a_form_that_spelled_the_teaching_is_judged_by_it() {
    let source = "employees!!(*), employees!!(*) as e2, employees.id = e2.id\n  \
                  |> delete!(employees(*))(*)\n  (~~error://dml/marker/multiple ~~)\n\
                  users(*) |> (arr:[1])";
    assert_eq!(
        judged(source).as_deref(),
        Some("error://dml/marker/multiple")
    );
}

/// A defect standing between two proven forms belongs to the GAP they
/// leave, and a proven sibling's declaration is outside it.
#[test]
fn a_proven_sibling_is_outside_the_gap_a_defect_falls_in() {
    assert_eq!(
        judged("users(*) foo bar\n_(x @ 1) (~~error://parse/general ~~)"),
        None,
    );
    assert_eq!(
        judged("_(x @ 1) (~~error://parse/general ~~)\nusers(*) foo bar"),
        None,
    );
}

/// A submission recovery could not divide AT ALL is one extent: nothing
/// was proven, so there is no sibling anywhere to have lent the hook, and
/// the form that failed is the only form there is.
#[test]
fn a_submission_recovery_proved_nothing_in_still_owns_its_own_hook() {
    let source = "employees?(*), users!!(*), employees.id = users.id \
                  (~~error://dml/marker/mismatch ~~) |> delete!(employees(*))(*)";
    assert_eq!(
        judged(source).as_deref(),
        Some("error://dml/marker/mismatch"),
    );
}

/// The hook is read from the author's BYTES when recovery destroyed the
/// annotation's node — and recovery destroys more than the node: the `//`
/// in the URI opens a line comment that swallows the closing delimiter, so
/// a reader keyed on the pieces recovery hands back finds nothing.
#[test]
fn a_hook_recovery_relexed_is_still_the_hook_the_author_wrote() {
    let source = "users(*) |> (arr:[1])\n  (~~error://parse/path_variable ~~)";
    assert_eq!(
        judged(source).as_deref(),
        Some("error://parse/path_variable"),
    );
}

/// A hook spelled inside a STRING declares nothing: the opener is only
/// recognized where a token begins, and a string literal is one token.
#[test]
fn a_hook_inside_a_string_literal_declares_nothing() {
    let source = "_(a, b? @ 1, _(b @ 2), _(b @ 3), \
                  _(c @ \"(~~error://semantic/anon/sparse_duplicate ~~)\"))";
    assert_eq!(judged(source), None);
}

/// The BARE hook accepts any error, and that is exactly why a body the
/// grammar refuses may not be read as one: a mistyped annotation would
/// then catch the parse failure the mistyping caused.
#[test]
fn a_malformed_hook_body_declares_nothing() {
    for malformed in [
        "(~~error nonsense ~~)",
        "(~~error semantic/anon ~~)",
        "(~~error:/semantic ~~)",
        "(~~error://semantic anon ~~)",
        "(~~error://-leading ~~)",
    ] {
        assert_eq!(
            judged(&format!("{REFUSES} {malformed}")),
            None,
            "{malformed} is not a body the grammar admits",
        );
    }
}

/// Both lawful bodies still read: nothing at all is the bare hook, and a
/// `://` path is the named one.
#[test]
fn the_bare_and_named_hooks_both_read_after_recovery() {
    let refuses = "users(*) |> (arr:[1])";
    assert_eq!(
        judged(&format!("{refuses} (~~error ~~)")).as_deref(),
        Some("(any error)"),
    );
    assert_eq!(
        judged(&format!("{refuses} (~~error://parse/path_variable ~~)")).as_deref(),
        Some("error://parse/path_variable"),
    );
}
