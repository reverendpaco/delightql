// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Danger Gate System
//
// Named safety boundaries that are OFF by default. Each gate is identified
// by a badge-form URI (e.g. "delightql-danger://cardinality/cartesian")
// and controls whether the compiler uses a safe or dangerous code path.
// Annotations and flags carry the bare hierarchy (their sigil declares
// the kind); canonical_danger_uri() normalizes.

use std::collections::HashMap;

use super::asts::core::{DangerSpec, DangerState};

/// Known danger URIs, their default states, and whether CLI override is allowed.
///
/// Semantic dangers (those that change what operators mean) are inline-only.
/// Guardrail dangers (execution policy) may be overridden from the CLI.
const KNOWN_DANGERS: &[(&str, DangerState, bool)] = &[
    //                                          default           cli_overridable
    (
        "delightql-danger://cardinality/cartesian",
        DangerState::Off,
        true,
    ),
    (
        "delightql-danger://termination/unbounded",
        DangerState::Off,
        true,
    ),
    (
        "delightql-danger://semantics/min_multiplicity",
        DangerState::Off,
        false,
    ), // semantic — inline-only
    // TWO LIVE SCOPES NEVER SHARE A NAME: acknowledging admits the
    // ambiguous co-activation. Guardrail class — it changes what the
    // session permits, not what an operator means — so CLI override is
    // allowed.
    ("delightql-danger://scope/duplicate", DangerState::Off, true),
];

/// A map of danger URIs to their current states. A gate is named exactly:
/// lookup is by canonical URI, so opening `cardinality/` opens nothing.
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

/// The danger badge scheme's URI prefix.
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

/// Parse a CLI `--danger hierarchy=STATE` argument into a validated
/// `DangerSpec`. Every failure is a loud teaching error: a `--danger`
/// that cannot take effect refuses, never no-ops — a silently ignored
/// safety flag is worse than no flag.
pub fn parse_cli_danger_spec(input: &str) -> crate::error::Result<DangerSpec> {
    let (hierarchy, state_text) = input.split_once('=').ok_or_else(|| {
        crate::error::DelightQLError::validation_error(
            format!(
                "--danger takes hierarchy=STATE (e.g. cardinality/cartesian=ON), got '{input}'"
            ),
            "parse_cli_danger_spec",
        )
    })?;
    let uri = canonical_danger_uri(hierarchy.trim());

    if !KNOWN_DANGERS.iter().any(|(known, _, _)| *known == uri) {
        return Err(crate::error::DelightQLError::validation_error(
            format!(
                "unknown danger '{}'. Known dangers: {}",
                hierarchy.trim(),
                known_danger_hierarchies().join(", ")
            ),
            "parse_cli_danger_spec",
        ));
    }

    if !is_cli_overridable(&uri) {
        return Err(crate::error::DelightQLError::validation_error(
            format!(
                "danger '{}' cannot be opened from the CLI: it changes what the \
                 query MEANS, so it must be visible in the query text — spell it \
                 inline: (~~danger://{}~~)",
                hierarchy.trim(),
                hierarchy.trim(),
            ),
            "parse_cli_danger_spec",
        ));
    }

    let state = match state_text.trim().to_ascii_uppercase().as_str() {
        "ON" => DangerState::On,
        "OFF" => DangerState::Off,
        "ALLOW" => DangerState::Allow,
        other => match other.parse::<u8>() {
            Ok(n @ 1..=9) => DangerState::Severity(n),
            _ => {
                return Err(crate::error::DelightQLError::validation_error(
                    format!(
                        "--danger state must be ON, OFF, ALLOW, or a severity 1-9, \
                         got '{state_text}'"
                    ),
                    "parse_cli_danger_spec",
                ))
            }
        },
    };

    Ok(DangerSpec { uri, state })
}

#[cfg(test)]
mod cli_danger_spec_tests {
    use super::*;

    #[test]
    fn overridable_gate_parses() {
        let spec = parse_cli_danger_spec("cardinality/cartesian=ON").unwrap();
        assert_eq!(spec.uri, "delightql-danger://cardinality/cartesian");
        assert_eq!(spec.state, DangerState::On);
    }

    // The refusal must be LOUD — a silently ignored override spec is
    // the tempting regression.
    #[test]
    fn non_overridable_gate_refuses_with_inline_teaching() {
        let err = parse_cli_danger_spec("semantics/min_multiplicity=ON").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("inline"),
            "must teach the inline spelling: {msg}"
        );
        assert!(
            msg.contains("(~~danger://semantics/min_multiplicity~~)"),
            "{msg}"
        );
    }

    #[test]
    fn unknown_gate_lists_known_hierarchies() {
        let err = parse_cli_danger_spec("cardinality/typo=ON").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cardinality/cartesian"), "{msg}");
    }

    #[test]
    fn bad_state_refuses() {
        let err = parse_cli_danger_spec("cardinality/cartesian=MAYBE").unwrap_err();
        assert!(err.to_string().contains("ON, OFF, ALLOW"), "{}", err);
    }

    #[test]
    fn missing_equals_refuses() {
        assert!(parse_cli_danger_spec("cardinality/cartesian").is_err());
    }
}

impl Default for DangerGateMap {
    fn default() -> Self {
        Self::with_defaults()
    }
}
