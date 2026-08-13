// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The naming policy — how the compiler spells a name it invented.
//!
//! A name nobody authored is OUTPUT ONLY: a heading has to say something and
//! a SQL engine will name the column regardless, but no reference in the
//! language reaches it. So the default spelling is DRAWN FRESH for every
//! compilation. A client that keys on one finds out on its second run rather
//! than after shipping, and a compiler road that parses a spelling it did not
//! author stops working immediately rather than quietly.
//!
//! The canonical policy spells the same occurrence `<mint:N>` — the Nth name
//! invented in that heading, and the Nth relation the bundle had to name for
//! itself. It exists so a contract lane can pin emitted SQL and published
//! headings without pinning a spelling that is meant to move.
//!
//! The canonical form is produced HERE, where the compiler still knows which
//! occurrences it invented. Deriving it by rewriting finished SQL cannot
//! work: the characters no longer say who chose them, so an authored `fn`
//! and an invented one are the same token.
//!
//! This is the one site that reads the setting. A policy consulted twice is
//! not a policy — the second reader keeps working when the first changes,
//! which is exactly the drift the mint exists to flush out.

/// The environment variable that selects a policy.
pub const POLICY_VAR: &str = "DQL_NAME_POLICY";

/// How an invented name is spelled.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum NamePolicy {
    /// Drawn fresh per compilation. The shipped default.
    #[default]
    Poison,
    /// `<mint:N>`, deterministic, for a contract lane.
    Canonical,
}

impl NamePolicy {
    /// Read the active policy, or refuse an unrecognized one.
    ///
    /// A misspelled policy must not fall back silently: a lane that asked for
    /// canonical names and received drawn ones would report a heading
    /// contract nobody can reproduce.
    pub fn from_env() -> Result<Self, ()> {
        match std::env::var(POLICY_VAR) {
            Err(_) => Ok(NamePolicy::Poison),
            Ok(value) => Self::parse(&value),
        }
    }

    fn parse(value: &str) -> Result<Self, ()> {
        match value.trim() {
            "" | "poison" => Ok(NamePolicy::Poison),
            "canonical" => Ok(NamePolicy::Canonical),
            _ => Err(()),
        }
    }
}

/// The one authority that spells a name the compiler invented.
///
/// Two counters, because they answer different questions. `drawn` is
/// bundle-wide and exists only to keep drawn spellings distinct from each
/// other; the canonical ordinal is local to the heading (or to the bundle's
/// relation reports) so that adding a CTE ahead of an output does not
/// renumber the output's names.
pub(super) struct Mint {
    policy: NamePolicy,
    salt: u64,
    drawn: u64,
}

impl Mint {
    pub(super) fn new(policy: NamePolicy) -> Self {
        Self {
            policy,
            salt: fresh_salt(),
            drawn: 0,
        }
    }

    /// Spell the `ordinal`-th invented name of one namespace. `ordinal` is
    /// 1-based and supplied by the caller, which is the only place that knows
    /// what the namespace is.
    pub(super) fn spell(&mut self, ordinal: u32) -> String {
        self.drawn += 1;
        match self.policy {
            NamePolicy::Canonical => format!("<mint:{ordinal}>"),
            NamePolicy::Poison => format!("mint_{:016x}", mix(self.salt, self.drawn)),
        }
    }
}

/// A value that differs between processes, drawn without a dependency.
///
/// `RandomState` seeds itself from the OS once per thread and moves on every
/// construction, so two compilations never share a salt and neither do two
/// runs.
fn fresh_salt() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(0x6d69_6e74_5f73_616c);
    hasher.finish()
}

/// Spread a salt over an ordinal so consecutive draws do not read as
/// consecutive numbers. A reader who can see the step can predict the next
/// name, which is the property the draw exists to deny.
fn mix(salt: u64, ordinal: u64) -> u64 {
    let mut value = salt ^ ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_spells_the_supplied_ordinal() {
        let mut mint = Mint::new(NamePolicy::Canonical);
        assert_eq!(mint.spell(1), "<mint:1>");
        assert_eq!(mint.spell(2), "<mint:2>");
        assert_eq!(mint.spell(1), "<mint:1>");
    }

    #[test]
    fn drawn_names_are_distinct_within_one_mint() {
        let mut mint = Mint::new(NamePolicy::Poison);
        let first = mint.spell(1);
        let second = mint.spell(2);
        let repeat = mint.spell(1);
        assert_ne!(first, second);
        assert_ne!(first, repeat);
    }

    #[test]
    fn drawn_names_differ_between_mints() {
        let first = Mint::new(NamePolicy::Poison).spell(1);
        let second = Mint::new(NamePolicy::Poison).spell(1);
        assert_ne!(first, second);
    }

    #[test]
    fn a_drawn_name_needs_no_quoting() {
        let drawn = Mint::new(NamePolicy::Poison).spell(1);
        assert!(drawn
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
    }

    #[test]
    fn an_unknown_policy_refuses() {
        assert_eq!(NamePolicy::parse("poison"), Ok(NamePolicy::Poison));
        assert_eq!(NamePolicy::parse("canonical"), Ok(NamePolicy::Canonical));
        assert_eq!(NamePolicy::parse("Canonical"), Err(()));
        assert_eq!(NamePolicy::parse("off"), Err(()));
    }
}
