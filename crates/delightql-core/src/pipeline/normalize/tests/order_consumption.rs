// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! ORDERING IS ADMITTED; PRESERVATION IS NOT IMPLIED. An ordering normalizes
//! wherever it is written: before any later step, ending a relation something
//! else reads, in a definition's body, in a goal read as a relation. No
//! normalization step judges where an ordering stands; its lowering is the
//! realizer's.

use super::support::{file, queries, query};

const R: &str = "r(*) : _(id, s @ 1, 20; 2, 30; 3, 10)\n";

#[test]
fn a_consumed_ordering_normalizes() {
    for source in [
        // presentation: the statement's last step, in either spelling
        "r(*) |> #(s desc)",
        "r(*), s > 1, #(s desc)",
        // the adjacent bound: every later step reads the chosen members
        "r(*) |> #(s desc), #<2 |> (id)",
        "r(*) |> #(s desc), #>1, #<1 |> (id)",
        "r(*) |> #(s desc), #<2 as top |> (top.id)",
        "t(*) : r(*) |> #(s), #<1\nt(*)",
        "r(, #(s desc), #<1)",
        // order-consuming specs own their ordering outright
        "r(*) |> (id, row_number:(<~ #(s desc)) as rn)",
        "r(*) |> %(id ~> (s) <~ #(s desc))",
        "_(k @ 1) |> (k, _:(, r(*) |> (s) |> #(s desc), #<1) as top)",
        // a later ordering presents the members an earlier bound chose
        "r(*) |> #(id), #<2 |> #(s desc)",
    ] {
        query(&format!("{R}{source}"));
    }
}

/// An ordering another step follows normalizes, whatever the step.
#[test]
fn an_ordering_a_later_step_follows_normalizes() {
    for source in [
        "r(*) |> #(s) |> (id)",
        "r(*), #(s), id > 1",
        "r(*), #(s) |> (id)",
        "r(*) |> #(s) |> +(row_number:(<~ #(id)) as rn)",
        "r(*) |> #(s) |> (id, lag:(id <~ #(id)) as p)",
        "r(*) |> #(s) |> %(id ~> (s) <~ #(id))",
        "r(*) |> #(s) ~> [id] as ids",
        "r(*) |> #(s) ; r(*)",
        "r(*) |> #(s), _(k @ 1)",
        "r(*) |> #(s) |> #(id)",
        "r(*) |> #(s) as n |> (n.id)",
        "r(*) |> #(s) |> (id), #<1",
        "r(*) |> #(s), id > 0, #<1",
    ] {
        query(&format!("{R}{source}"));
    }
}

/// An ordering ending a relation something else reads normalizes.
#[test]
fn an_ordering_ending_a_relation_something_reads_normalizes() {
    for source in [
        "t(*) : r(*) |> #(s)\nt(*)",
        "r(, #(s))",
        "r(*) as a, r(, #(s)) as b, a.id = b.id",
        "_(k @ 1) |> (k, _:(, r(*) |> #(s) ~> sum:(s)) as total)",
        "r(*) |> #(s) !> assert!(exists(*))(*)",
        "(~~ddl v(*) :- _(id @ 2; 1) |> #(id) ~~)\nv(*)",
    ] {
        query(&format!("{R}{source}"));
    }
}

/// A definition's body may order, ending in the ordering or not.
#[test]
fn a_definition_body_may_order() {
    for source in [
        "v(*) :- _(id @ 1; 2) |> #(id)",
        "v(*) :- _(id @ 1; 2) |> #(id) |> (id)",
        "f:(x) :- x + _:(, _(v @ 1; 2) |> #(v) ~> sum:(v))",
        "v(*) :- _(id @ 1; 2) |> #(id), #<1",
    ] {
        file(source);
    }
}

/// A goal read as a relation is the same query a statement runs.
#[test]
fn a_goal_read_as_a_relation_keeps_its_ordering() {
    for source in ["_(id @ 2; 1) |> #(id)", "_(id @ 2; 1) |> #(id), #<1"] {
        queries(source).into_queries().remove(0).into_query();
    }
}
