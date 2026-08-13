// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

//! No unfielded slot may admit two kinds that OVERLAP.
//!
//! Supertypes nest: every `ground` is a `literal`-or-`mention`, and every one
//! of those is also a `non_infix_application`, a `function_application`, and a
//! `domain_expression`. So a production written `seq($.ground, $.arrow,
//! $.domain_expression)` has two positions the child list cannot tell apart —
//! `null -> null` is one node kind in both — and a consumer reading the
//! unfielded children silently fills the wrong one.
//!
//! The distinction in such a production is ORDER, and only a FIELD carries
//! order into the typed API. This test is the enumeration that proves no such
//! slot is left: it walks every node type's unfielded child set and fails on
//! any pair where one member is reachable from the other through supertype
//! membership.

use delightql_cst::cst::{subtypes_of, SUBTYPES};
use std::collections::{BTreeMap, BTreeSet};

/// Everything a supertype ultimately admits, transitively. A member that is
/// itself a supertype contributes its own members.
fn closure(name: &str) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut stack: Vec<String> = subtypes_of(name).iter().map(|s| s.to_string()).collect();
    while let Some(member) = stack.pop() {
        if !seen.insert(member.clone()) {
            continue;
        }
        stack.extend(subtypes_of(&member).iter().map(|s| s.to_string()));
    }
    seen
}

/// The unfielded child sets, read from the SAME `node-types.json` the typed
/// API is generated from. Reading the generated table rather than the JSON
/// would only measure the generator against itself.
fn unfielded_slots() -> BTreeMap<String, Vec<String>> {
    let json = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(std::path::Path::parent)
            .expect("crates/<pkg> sits two levels under the workspace root")
            .join("grammar/src/node-types.json"),
    )
    .expect("node-types.json is generated beside the parser");
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("node-types.json parses");

    let mut slots = BTreeMap::new();
    for entry in parsed.as_array().expect("node-types.json is an array") {
        if !entry["named"].as_bool().unwrap_or(false) {
            continue;
        }
        let Some(kind) = entry["type"].as_str() else {
            continue;
        };
        let Some(types) = entry["children"]["types"].as_array() else {
            continue;
        };
        let members: Vec<String> = types
            .iter()
            .filter(|t| t["named"].as_bool().unwrap_or(false))
            .filter_map(|t| t["type"].as_str())
            .map(str::to_string)
            .collect();
        if members.len() > 1 {
            slots.insert(kind.to_string(), members);
        }
    }
    slots
}

#[test]
fn no_unfielded_slot_admits_overlapping_kinds() {
    let closures: BTreeMap<String, BTreeSet<String>> = SUBTYPES
        .iter()
        .map(|(name, _)| (name.to_string(), closure(name)))
        .collect();

    let mut offenders = Vec::new();
    for (owner, members) in unfielded_slots() {
        for outer in &members {
            let Some(reachable) = closures.get(outer) else {
                continue;
            };
            for inner in &members {
                if inner != outer && reachable.contains(inner) {
                    offenders.push(format!(
                        "{owner}: an unfielded child may be `{inner}` OR `{outer}`, and every \
                         `{inner}` is an `{outer}` — field the positions"
                    ));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "these slots cannot be read positionally:\n  {}",
        offenders.join("\n  ")
    );
}
