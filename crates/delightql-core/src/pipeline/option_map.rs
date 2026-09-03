// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Config Map System (user-facing kind: delightql-config://; the internal
// type names retain "option" until a cosmetic rename batch).
//
// Strategy/preference selection. Each config is identified by a badge-form
// URI (e.g. "delightql-config://generation/rule/inlining/view")
// and controls which code path the compiler uses when
// multiple paths lead to the same result. Annotations and flags carry the
// bare hierarchy; canonical_config_uri() normalizes.

use std::collections::HashMap;

use super::asts::core::{OptionSpec, OptionState};

/// Known option URIs and their default states.
const KNOWN_OPTIONS: &[(&str, OptionState)] = &[
    (
        "delightql-config://generation/rule/inlining/view",
        OptionState::Off,
    ),
    (
        "delightql-config://generation/rule/inlining/fact",
        OptionState::Off,
    ),
];

/// The config badge scheme.
pub const CONFIG_URI_SCHEME: &str = "delightql-config://";

/// Canonicalize a config URI: bare hierarchy gains the badge scheme.
pub fn canonical_config_uri(input: &str) -> String {
    if input.starts_with(CONFIG_URI_SCHEME) {
        input.to_string()
    } else {
        format!("{CONFIG_URI_SCHEME}{input}")
    }
}

/// Bare hierarchies of all known configs (for teaching errors).
pub fn known_config_hierarchies() -> Vec<&'static str> {
    KNOWN_OPTIONS
        .iter()
        .map(|(uri, _)| uri.trim_start_matches(CONFIG_URI_SCHEME))
        .collect()
}

/// A map of option URIs to their current states.
#[derive(Debug, Clone)]
pub struct OptionMap {
    options: HashMap<String, OptionState>,
}

impl OptionMap {
    pub fn with_defaults() -> Self {
        let options = KNOWN_OPTIONS
            .iter()
            .map(|(uri, state)| (uri.to_string(), *state))
            .collect();
        Self { options }
    }

    pub fn apply_overrides(&mut self, specs: &[OptionSpec]) {
        for spec in specs {
            self.options.insert(spec.uri.clone(), spec.state);
        }
    }
}

impl Default for OptionMap {
    fn default() -> Self {
        Self::with_defaults()
    }
}
