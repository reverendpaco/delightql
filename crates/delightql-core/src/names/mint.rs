// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The mint — how the compiler spells a name it invented.
//!
//! A name nobody authored is OUTPUT ONLY: a heading has to say something and
//! a SQL engine will name the column regardless, but no reference in the
//! language reaches it. So its spelling is DRAWN FRESH for every
//! compilation. A client that keys on one finds out on its second run rather
//! than after shipping, and a compiler road that parses a spelling it did not
//! author stops working immediately rather than quietly.
//!
//! There is no deterministic spelling to select. A host that needs one — a
//! digest, a contract lane — reads which columns are minted from the result
//! Header and renders them itself; nothing in the compiler depends on it.
//!
//! A MINT SAYS WHAT IT LOST, AND ONLY THAT. An occurrence whose authored name
//! lost a collision keeps that name left of the mark — `id⊥80c43d5c2a8b6cd9`
//! — and nothing else ever stands there: a fixed intent word would read as a
//! lost authored name. The name is for a human reading the heading; the
//! spelling as a whole still moves.

/// The one authority that spells a name the compiler invented.
///
/// `drawn` is bundle-wide and exists only to keep drawn spellings distinct
/// from each other.
pub(super) struct Mint {
    salt: u64,
    drawn: u64,
}

impl Mint {
    pub(super) fn new() -> Self {
        Self::with_salt(fresh_salt())
    }

    /// A mint whose draws a test can predict, for laws about a collision
    /// with a spelling the mint would draw.
    pub(super) fn with_salt(salt: u64) -> Self {
        Self { salt, drawn: 0 }
    }

    /// Draw the next invented name under its `mark`.
    pub(super) fn spell(&mut self, mark: Mark<'_>) -> String {
        self.drawn += 1;
        let (lost, word) = match mark {
            Mark::Bare => ("", String::new()),
            Mark::Lost(name) => (name, String::new()),
            Mark::Origin(word) => ("", format!("{word}_")),
        };
        format!("{lost}⊥{word}{:016x}", mix(self.salt, self.drawn))
    }
}

/// What an invented name says beside its mark. An authored name only ever
/// stands left of it; a word for where the value came from only ever
/// stands after it, so neither can be read as the other.
#[derive(Clone, Copy, Debug)]
pub(super) enum Mark<'a> {
    /// Nothing is known: the mark and the digits.
    Bare,
    /// The authored name the occurrence lost or keeps: `id⊥…`.
    Lost(&'a str),
    /// Where a value nobody named came from: `⊥expr_…`.
    Origin(&'static str),
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
    fn drawn_names_are_distinct_within_one_mint() {
        let mut mint = Mint::new();
        let first = mint.spell(Mark::Bare);
        let second = mint.spell(Mark::Bare);
        assert_ne!(first, second);
    }

    #[test]
    fn drawn_names_differ_between_mints() {
        let first = Mint::new().spell(Mark::Bare);
        let second = Mint::new().spell(Mark::Bare);
        assert_ne!(first, second);
    }

    /// The mark, then sixteen drawn hexadecimal digits.
    fn drawn_tail(tail: &str) -> bool {
        tail.strip_prefix('⊥').is_some_and(|digits| {
            digits.len() == 16
                && digits
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
    }

    #[test]
    fn a_drawn_name_is_the_mark_and_sixteen_digits() {
        let drawn = Mint::new().spell(Mark::Bare);
        assert!(drawn_tail(&drawn), "{drawn}");
    }

    #[test]
    fn a_lost_name_stands_left_of_the_mark() {
        let drawn = Mint::new().spell(Mark::Lost("id"));
        assert!(drawn.strip_prefix("id").is_some_and(drawn_tail), "{drawn}");
    }

    #[test]
    fn an_origin_word_stands_after_the_mark() {
        let drawn = Mint::new().spell(Mark::Origin("expr"));
        assert!(
            drawn
                .strip_prefix("⊥expr_")
                .is_some_and(|digits| drawn_tail(&format!("⊥{digits}"))),
            "{drawn}"
        );
    }

    #[test]
    fn one_salt_draws_one_sequence() {
        let mut first = Mint::with_salt(7);
        let mut second = Mint::with_salt(7);
        assert_eq!(first.spell(Mark::Bare), second.spell(Mark::Bare));
        assert_eq!(first.spell(Mark::Bare), second.spell(Mark::Bare));
    }
}
