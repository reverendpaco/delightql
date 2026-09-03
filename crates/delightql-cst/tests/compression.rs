// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! THE COMPRESSION CLOSES THE INTERIOR.
//!
//! CARDINALITY IS AUTHORED: a relation enters value position only through an
//! inner form whose FINAL continuation is a compression. The placement is part
//! of the surface — nothing may follow the compression and reopen the
//! relational interior — and an uncompressed inner form has no derivation at
//! all, so the refusal cannot be a builder check someone forgets to run.
//!
//! The one-column guarantee is a different matter and stays a resolution
//! judgment against the registry. Only the one-ROW guarantee is spelled here.

mod support;

use delightql_cst::cst::*;
use support::{admits, count, first, refuses};

/// The two ratified spellings, both compressed, both exposing the compression
/// as a typed accessor rather than as a position G.2 would have to recompute.
#[test]
fn a_compressed_interior_names_its_compression() {
    // `users:( ~> count:(*))` — the reduction closes it.
    let reduced = admits("users(*) |> (orders:(, id = 1 ~> count:(*)) as n)");
    let named = first::<ScalarSubquery>(&reduced);
    let interior = named.interior().expect("a compressed interior");
    assert!(matches!(
        interior.compression().expect("a compression"),
        Compression::SingletonReduction(_)
    ));
    assert_eq!(
        interior.continuation().count(),
        1,
        "the comma member is an ordinary continuation; the reduction is not"
    );

    // `one_col_table:( |> #(c desc), #<1)` — the bound closes it.
    let bounded = admits("users(*) |> (one_col:( |> #(c desc), #<1) as n)");
    let interior = first::<ScalarSubquery>(&bounded)
        .interior()
        .expect("a compressed interior");
    assert!(matches!(
        interior.compression().expect("a compression"),
        Compression::BoundToOne(_)
    ));
}

/// The anonymous form takes the same carrier. One compression law, two
/// surfaces — not two spellings that could drift.
#[test]
fn the_sourceless_inner_form_shares_the_carrier() {
    let tree = admits("users(*) |> (_:(, _(1;2) ~> count:(*)) as n)");
    assert_eq!(count::<AnonScalarSubquery>(&tree), 1);
    let interior = first::<AnonScalarSubquery>(&tree)
        .interior()
        .expect("a compressed interior");
    assert!(matches!(
        interior.compression().expect("a compression"),
        Compression::SingletonReduction(_)
    ));
}

/// An uncompressed inner form refuses. This is the case that silently answered
/// an arbitrary first row: an actor with nineteen films "answering" one.
#[test]
fn an_uncompressed_inner_form_refuses() {
    refuses("users(*) |> (orders:(, id = 1) as n)");
    refuses("users(*) |> (_:(, _(1;2)) as n)");
    refuses("users(*) |> (orders:( |> (id)) as n)");
    refuses("users(*), a = orders:(, id = 1)");
}

/// Nothing may follow the compression: the interior is CLOSED by it, so a
/// continuation after one would reopen a relation already compressed to a
/// value.
#[test]
fn nothing_follows_the_compression() {
    refuses("users(*) |> (orders:(, id = 1 ~> count:(*) |> (c)) as n)");
    refuses("users(*) |> (orders:( |> #(c), #<1 |> (c)) as n)");
    refuses("users(*) |> (orders:(, id = 1 ~> count:(*), x = 1) as n)");
}

/// A bound-to-one is exactly that. `#>` is not a compression, and neither is a
/// count other than one — the refusals are structural, not range checks.
#[test]
fn only_at_most_one_compresses() {
    refuses("users(*) |> (one_col:(, #>1) as n)");
    refuses("users(*) |> (one_col:(, #<2) as n)");
    refuses("users(*) |> (one_col:(, #<0) as n)");
    // …and the ordinary row bound is untouched by any of it.
    let ordinary = admits("users(*), #<3");
    assert_eq!(count::<RowBound>(&ordinary), 1);
    assert_eq!(count::<BoundToOne>(&ordinary), 0);
}

/// The bound-to-one keeps the tooling name its operator has everywhere else,
/// so one highlight query finds every `#<`.
#[test]
fn the_compression_bound_keeps_the_operator_name() {
    let tree = admits("users(*) |> (one_col:(, #<1) as n)");
    let bound = first::<BoundToOne>(&tree);
    assert_eq!(tree.text(bound), ", #<1");
    assert_eq!(count::<BoundOp>(&tree), 1);
}

/// An existence guard is the truth-compression and needs no authored one: it
/// takes the ordinary interior, and that distinction stays visible.
///
/// ONE TRUTH CARRIER IN EVERY POSITION. In value position the existence is
/// the same `existence` node, read through the crossing; in comma position
/// it stands bare. There is no second existence spelling for value position.
#[test]
fn existence_needs_no_authored_compression() {
    let value = admits("users(*) |> (+orders(, id = 1) as e)");
    assert_eq!(count::<Existence>(&value), 1);
    assert_eq!(count::<CrossedTruth>(&value), 1);
    assert_eq!(count::<CompressedInterior>(&value), 0);
    assert_eq!(count::<InteriorContinuation>(&value), 1);

    // In comma position existence is the same TRUTH carrier. Its truth
    // restricts the current relation; a semi/antijoin is only a lowering.
    let member = admits("users(*), +orders(, id = 1)");
    assert_eq!(count::<Existence>(&member), 1);
    assert_eq!(count::<CrossedTruth>(&member), 0);
}

/// A mode-compressed call needs no authored compression either — one row by
/// declared functional dependency — and stays a different carrier.
#[test]
fn a_mode_compressed_call_needs_no_authored_compression() {
    let tree = admits("users(*) |> (foo:(x).out1)");
    assert_eq!(count::<FieldSelect>(&tree), 1);
    assert_eq!(count::<CompressedInterior>(&tree), 0);
}
