// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Danger Gate System
//
// Named safety boundaries that are OFF by default. Each gate is identified
// by a badge-form URI (e.g. "delightql-danger://cardinality/nulljoin",
// URI-DESIGN.md §2) and controls whether the compiler uses a safe or
// dangerous code path. Annotations and flags carry the bare hierarchy
// (their sigil declares the kind); canonical_danger_uri() normalizes.

use std::collections::HashMap;

use super::asts::core::{DangerSpec, DangerState};

/// Known danger URIs, their default states, and whether CLI override is allowed.
///
/// Semantic dangers (those that change what operators mean) are inline-only.
/// Guardrail dangers (execution policy) may be overridden from the CLI.
const KNOWN_DANGERS: &[(&str, DangerState, bool)] = &[
    //                                          default           cli_overridable
    ("delightql-danger://cardinality/nulljoin", DangerState::Off, false),
    ("delightql-danger://cardinality/cartesian", DangerState::Off, true),
    ("delightql-danger://termination/unbounded", DangerState::Off, true),
    ("delightql-danger://semantics/min_multiplicity", DangerState::Off, false), // semantic — inline-only
];

/// A map of danger URIs to their current states, supporting prefix matching.
#[derive(Debug, Clone)]
pub struct DangerGateMap {
    gates: HashMap<String, DangerState>,
}

impl DangerGateMap {
    pub fn with_defaults() -> Self {
        let gates = KNOWN_DANGERS
            .iter()
            .map(|(uri, state, _cli)| (uri.to_string(), *state))
            .collect();
        Self { gates }
    }

    pub fn apply_overrides(&mut self, specs: &[DangerSpec]) {
        for spec in specs {
            self.gates.insert(spec.uri.clone(), spec.state);
        }
    }

    pub fn is_enabled(&self, uri: &str) -> bool {
        match self.get(uri) {
            Some(DangerState::On) => true,
            Some(DangerState::Severity(n)) if *n > 0 => true,
            _ => false,
        }
    }

    pub fn get(&self, uri: &str) -> Option<&DangerState> {
        self.gates.get(uri)
    }
}

/// The danger badge scheme (URI-DESIGN.md §2).
pub const DANGER_URI_SCHEME: &str = "delightql-danger://";

/// Canonicalize a danger URI: bare hierarchy (annotation/flag sugar)
/// gains the badge scheme; badge forms pass through.
pub fn canonical_danger_uri(input: &str) -> String {
    if input.starts_with(DANGER_URI_SCHEME) {
        input.to_string()
    } else {
        format!("{DANGER_URI_SCHEME}{input}")
    }
}

/// Bare hierarchies of all known dangers (for teaching errors).
pub fn known_danger_hierarchies() -> Vec<&'static str> {
    KNOWN_DANGERS
        .iter()
        .map(|(uri, _, _)| uri.trim_start_matches(DANGER_URI_SCHEME))
        .collect()
}

/// Check whether a danger URI may be overridden from CLI flags.
/// Returns false for semantic dangers that must be specified inline.
pub fn is_cli_overridable(uri: &str) -> bool {
    KNOWN_DANGERS
        .iter()
        .find(|(known_uri, _, _)| *known_uri == uri)
        .map(|(_, _, cli)| *cli)
        .unwrap_or(false)
}

impl Default for DangerGateMap {
    fn default() -> Self {
        Self::with_defaults()
    }
}
