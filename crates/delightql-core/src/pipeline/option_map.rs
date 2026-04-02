// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Option Map System
//
// Strategy/preference selection. Each option is identified by a hierarchical
// URI (e.g. "generation/rule/inlining/view") and controls which code path
// the compiler uses when multiple paths lead to the same result.

use std::collections::HashMap;

use super::asts::core::{OptionSpec, OptionState};

/// Known option URIs and their default states.
const KNOWN_OPTIONS: &[(&str, OptionState)] = &[
    ("generation/rule/inlining/view", OptionState::Off),
    ("generation/rule/inlining/fact", OptionState::Off),
];

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
