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

    pub fn is_enabled(&self, uri: &str) -> bool {
        match self.get(uri) {
            Some(OptionState::On) => true,
            Some(OptionState::Severity(n)) if *n > 0 => true,
            _ => false,
        }
    }

    pub fn get(&self, uri: &str) -> Option<&OptionState> {
        self.options.get(uri)
    }
}

impl Default for OptionMap {
    fn default() -> Self {
        Self::with_defaults()
    }
}
