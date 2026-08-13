// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE SUBSTITUTION LAW, spent at the one boundary that spends it.
//!
//! A function-pipe step is the ordinary application it denotes. The judgment
//! is over the callable's WHOLE payload — a call's argument row, the window
//! it is modified by, and the guard it is filtered by — because the hole
//! stands wherever a value stands. Reading only the argument row takes the
//! implicit landing over the author's head and leaves the written hole for a
//! later pass to trip over.

use super::support::{query, refusal};
use crate::error::Result;
use crate::pipeline::ast_visit::{walk_visit_query, AstVisit, Descent};
use crate::pipeline::asts::core::{DomainExpression, DomainHole, FunctionApplication, Unresolved};

/// Every hole left standing after normalization.
#[derive(Default)]
struct SurvivingHoles {
    found: Vec<String>,
}

impl AstVisit<Unresolved> for SurvivingHoles {
    fn enter_domain(&mut self, expression: &DomainExpression<Unresolved>) -> Result<Descent> {
        if let DomainExpression::Application(FunctionApplication::Open(hole)) = expression {
            let spelling = match hole {
                DomainHole::CompositionInput => "@ (composition input)",
                // `_` binds nothing and is not what a pipe lands in.
                DomainHole::Disregarded => return Ok(Descent::Continue),
            };
            self.found.push(spelling.to_string());
        }
        Ok(Descent::Continue)
    }
}

/// A pipe spends its landing; nothing it wrote survives the boundary.
fn no_hole_survives(source: &str) {
    let normalized = query(source);
    let mut holes = SurvivingHoles::default();
    walk_visit_query(&mut holes, &normalized).expect("the walk reads, it does not fail");
    assert!(
        holes.found.is_empty(),
        "{source:?} left {:?} standing after normalization; a function pipe is spent \
         where it is read",
        holes.found
    );
}

/// The lispy of a normalized query, for asserting the shape a step became.
fn shape(source: &str) -> String {
    super::support::lispy(&query(source))
}

#[test]
fn the_argument_row_takes_the_implicit_landing() {
    // ZERO HOLES: `/->` lands first, `/->>` lands last. This is why
    // `x /-> upper:(y)` means `upper(x, y)`.
    no_hole_survives("_(x@1;2) |> (x /-> max:(0) as m)");
    no_hole_survives("_(x@1;2) |> (x /->> max:(0) as m)");

    let first = shape("_(x@1;2) |> (x /-> max:(0) as m)");
    let last = shape("_(x@1;2) |> (x /->> max:(0) as m)");
    assert_ne!(
        first, last,
        "the two directions land in different places and cannot normalize alike"
    );
}

#[test]
fn a_written_hole_takes_the_landing_from_the_argument_row() {
    // One written `@` overrides the implicit landing wherever it stands,
    // including under another application.
    no_hole_survives("_(x@1;2) |> (x /-> max:(@, 0) as m)");
    no_hole_survives("_(x@1;2) |> (x /-> max:(abs:(@), 0) as m)");
}

#[test]
fn a_window_position_is_a_landing_site() {
    // `row_number:()` writes no argument and no argument is what it takes:
    // the landing the author wrote is in the PARTITION, and reading only
    // the argument row would insert the value there and leave this one
    // standing.
    no_hole_survives("_(x@1;2) |> (x /-> row_number:() <~ %(@) as rn)");
    no_hole_survives("_(x@1;2) |> (x /-> row_number:() <~ #(@) as rn)");
}

#[test]
fn a_guard_position_is_a_landing_site() {
    // A guard is a value position too — the truth it holds compares values,
    // and one of them may be what flows in. THE SLOT IS ONE, so the form
    // that wants the value in BOTH places names it.
    no_hole_survives("_(x@1;2) |> (x /-> sum:(| @ > 0) as s)");
    no_hole_survives("_(x@1;2) |> (x /-> :(|v| sum:(v | v > 0)) as s)");
}

#[test]
fn the_slot_is_one() {
    // One value flows in and `@` names nothing, so a second bare hole has
    // no reading. The refusal names the spelling that does name it.
    let refusal = refusal("_(x@1;2) |> (x /-> sum:(@ | @ > 0) as s)");
    assert!(
        refusal.contains("writes '@' 2 times"),
        "a second bare hole refuses, counting them: {refusal}"
    );
    assert!(
        refusal.contains("the bare hole lands once"),
        "the refusal states the law it applied: {refusal}"
    );
}

#[test]
fn the_binder_names_the_flow() {
    // With a binder the NAME is the flowing value, so it may stand at as
    // many places as the author writes it — and at none is a discard.
    no_hole_survives("_(x@1;2) |> (x /-> :(|v| v - v) as d)");
    assert!(
        refusal("_(x@1;2) |> (x /-> :(|v| 1) as d)").contains("never uses it"),
        "a binder that stands nowhere receives nothing"
    );
    assert!(
        refusal("_(x@1;2) |> (x /-> :(|v| v + @) as d)").contains("the binder IS the flow"),
        "a binder beside a hole spells the flow twice"
    );
}

#[test]
fn a_nested_callable_keeps_its_own_hole() {
    // A lambda handed to a callee writes its OWN slot, and nothing at this
    // boundary lands into it: the hole is spent at instantiation, where the
    // callee applies it. That is why the landing walk stops at a nested
    // callable, and why this shape must still carry its hole afterwards.
    //
    // The pipe cannot reach this from the outside — the grammar admits a
    // lambda in an argument row and not in a pipe step's callable — so the
    // stop is a guard on the boundary, and what is pinned here is the
    // invariant it guards.
    let shape = shape("_(x@1;2) |> (apply:(:(@ * 2), x) as t)");
    assert!(
        shape.contains("scalar_argument:callable"),
        "the lambda is handed to the callee whole: {shape}"
    );
    assert!(
        shape.contains("domain_hole:composition_input"),
        "the lambda's own hole survives to instantiation: {shape}"
    );
}

#[test]
fn a_form_with_no_argument_row_must_write_its_hole() {
    // A lambda and an open string have nowhere to put the value, so a
    // hole-less one would discard it. The refusal names both repairs.
    for source in [
        "_(x@1;2) |> (x /-> :(3) as c)",
        "_(x@1;2) |> (x /-> :\"hi\" as c)",
    ] {
        let tree = crate::pipeline::syntax::Parser::new().parse_query_sequence(source);
        let refused = crate::pipeline::normalize::query_sequence(
            &tree,
            std::rc::Rc::new(crate::names::Registry::new(&[])),
        );
        let error = refused
            .err()
            .unwrap_or_else(|| panic!("{source:?} discards the piped value and must refuse"));
        assert!(
            format!("{error}").contains("discarded"),
            "{source:?} refused for the wrong reason: {error}"
        );
    }
}
