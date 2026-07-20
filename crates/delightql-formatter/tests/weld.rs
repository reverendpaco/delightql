// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The weld: every named node kind in the grammar appears in exactly
//! one of the registry's two lists. A grammar arc that adds a node
//! kind goes red here until the formatter takes a position on it.

use delightql_formatter::registry::{DELIBERATELY_UNHANDLED, HANDLED};
use std::collections::BTreeSet;

/// Named node kinds from the generated grammar description — the
/// authoritative enumeration of what can appear in a syntax tree.
fn grammar_named_kinds() -> BTreeSet<String> {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../grammar_dql/src/node-types.json"
    );
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("cannot read {path}: {e} — regenerate the grammar"));
    let node_types: serde_json::Value = serde_json::from_str(&text).expect("valid node-types.json");
    node_types
        .as_array()
        .expect("node-types.json is an array")
        .iter()
        .filter(|n| n["named"].as_bool() == Some(true))
        .map(|n| n["type"].as_str().expect("type is a string").to_string())
        .collect()
}

#[test]
fn every_named_kind_has_a_position() {
    let grammar = grammar_named_kinds();
    let handled: BTreeSet<&str> = HANDLED.iter().copied().collect();
    let unhandled: BTreeSet<&str> = DELIBERATELY_UNHANDLED.iter().copied().collect();

    let overlap: Vec<&&str> = handled.intersection(&unhandled).collect();
    assert!(
        overlap.is_empty(),
        "kinds listed as both handled and deliberately unhandled: {overlap:?}"
    );

    let unplaced: Vec<&String> = grammar
        .iter()
        .filter(|k| !handled.contains(k.as_str()) && !unhandled.contains(k.as_str()))
        .collect();
    assert!(
        unplaced.is_empty(),
        "grammar node kinds the formatter takes no position on — \
         handle them or add them to registry::DELIBERATELY_UNHANDLED: {unplaced:?}"
    );

    let stale: Vec<&&str> = handled
        .union(&unhandled)
        .filter(|k| !grammar.contains(**k))
        .collect();
    assert!(
        stale.is_empty(),
        "registry entries no longer in the grammar — remove them: {stale:?}"
    );
}
