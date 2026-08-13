// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The weld: every concrete kind the consolidated grammar can put in a tree
//! stands in exactly one of the registry's two lists.
//!
//! The enumeration comes from `delightql_cst::cst::ALL`, which the grammar
//! writes — so a grammar arc that adds a form goes red here until the
//! formatter takes a position on it. That is what stops a new semantic member
//! from being echoed unnoticed, which no wildcard arm could.

use delightql_cst::cst::{Kind, ALL, SUBTYPES};
use delightql_formatter::registry::{LAID_OUT, VERBATIM};
use std::collections::BTreeSet;

/// Kinds a node can actually HAVE. A supertype names a family, never a node,
/// and the typed enums over those families are exhaustive in Rust already.
fn concrete_kinds() -> BTreeSet<&'static str> {
    let supertypes: BTreeSet<&str> = SUBTYPES.iter().map(|(name, _)| *name).collect();
    ALL.iter()
        .map(|kind| kind.as_str())
        .filter(|name| !supertypes.contains(name))
        .collect()
}

#[test]
fn every_concrete_kind_has_a_position() {
    let grammar = concrete_kinds();
    let laid_out: BTreeSet<&str> = LAID_OUT.iter().map(|k| k.as_str()).collect();
    let verbatim: BTreeSet<&str> = VERBATIM.iter().map(|k| k.as_str()).collect();

    let overlap: Vec<&&str> = laid_out.intersection(&verbatim).collect();
    assert!(
        overlap.is_empty(),
        "kinds listed as both laid out and echoed: {overlap:?}"
    );

    let unplaced: Vec<&&str> = grammar
        .iter()
        .filter(|k| !laid_out.contains(*k) && !verbatim.contains(*k))
        .collect();
    assert!(
        unplaced.is_empty(),
        "grammar kinds the formatter takes no position on — lay them out or \
         add them to registry::VERBATIM: {unplaced:?}"
    );

    let stale: Vec<&&str> = laid_out
        .union(&verbatim)
        .filter(|k| !grammar.contains(*k))
        .collect();
    assert!(
        stale.is_empty(),
        "registry entries the grammar no longer produces — remove them: {stale:?}"
    );
}

/// A supertype is a family name. Listing one would claim a position on
/// something no node can be, and would hide the members that need one.
#[test]
fn no_supertype_is_listed() {
    let supertypes: BTreeSet<&str> = SUBTYPES.iter().map(|(name, _)| *name).collect();
    let listed: Vec<&Kind> = LAID_OUT
        .iter()
        .chain(VERBATIM.iter())
        .filter(|k| supertypes.contains(k.as_str()))
        .collect();
    assert!(
        listed.is_empty(),
        "supertypes are families, not node kinds: {listed:?}"
    );
}
