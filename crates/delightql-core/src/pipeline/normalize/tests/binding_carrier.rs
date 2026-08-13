// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The binding/body carrier: one query is one binding preamble and ONE
//! body. These pins are the structural half of the carrier's contract —
//! what a query CAN hold and in what order — read off the one struct,
//! because there are no wrappers left to select among: a future consumer
//! handles `Query { cfes, ctes, body }` or does not compile.

use super::support::query;

/// CTEs and CFEs ride ONE query together — fields on the same value, not
/// nested wrappers, so neither kind can accidentally own or reorder the
/// other by wrapping it.
#[test]
fn a_query_carries_both_binding_kinds_without_nesting() {
    let q = query("double:(x) : (x * 2)  a(*) : c  c(*) |> (double:(age) as d)");
    assert_eq!(q.cfes.len(), 1, "one CFE definition");
    assert_eq!(q.ctes.len(), 1, "one CTE binding");
    assert_eq!(q.cfes[0].name.as_str(), "double");
    assert_eq!(
        q.ctes[0].subject.authored_name().map(|n| n.as_str()),
        Some("c")
    );
}

/// Binding order is the AUTHORED order, per collection.
#[test]
fn binding_order_is_authored_order() {
    let q = query("a(*) : first  b(*) : second  first(*), second(*)");
    let names: Vec<_> = q
        .ctes
        .iter()
        .map(|cte| {
            cte.subject
                .authored_name()
                .expect("an authored binding names itself")
                .as_str()
                .to_string()
        })
        .collect();
    assert_eq!(names, ["first", "second"]);

    let q = query("f:(x) : (x + 1)  g:(x) : (x + 2)  a(*) |> (f:(age), g:(age))");
    let names: Vec<_> = q.cfes.iter().map(|cfe| cfe.name.as_str()).collect();
    assert_eq!(names, ["f", "g"]);
}

/// A bare query is its body alone — and says so in one place.
#[test]
fn a_bare_query_is_its_body() {
    let q = query("users(*)");
    assert!(q.is_bare());
    assert!(q.into_bare_body().is_ok());

    let bound = query("a(*) : c  c(*)");
    assert!(!bound.is_bare());
    assert!(bound.into_bare_body().is_err());
}

/// The effect classification reads the canonical bindings: an
/// effect-marked binding is visible on `ctes` exactly as a pure one is,
/// with its declaration intact.
#[test]
fn effect_declarations_ride_the_canonical_bindings() {
    let q = query("x(*) : pure_one  s!(*) : marked!  pure_one(*), marked!(*)");
    assert_eq!(q.ctes.len(), 2);
    assert!(!q.ctes[0].subject.declares_effect());
    assert!(q.ctes[1].subject.declares_effect());
}
