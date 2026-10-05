// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Unruled questions the core would otherwise answer. Each is a statement
//! input read only by its named decider; the default is today's behavior,
//! and the alternative is the other reading the question leaves open.

/// A comparison of two occurrences computed as a value and consumed by an
/// outer match (open question O-EQ-9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CrossedComparison {
    /// Today: a comparison read as a value is a within-row verdict.
    WithinRow,
}

/// A configured value's lineage reaching a reduction that merges its
/// construction rows (open question O-OCC-8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LineageDrop {
    /// Today: the spend refuses.
    Refuse,
}

/// Two completions of one configured value joined on a column (open
/// question C4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompletionPairing {
    /// Today: the join refuses as uncovered.
    Refuse,
}

/// A glob covering a latent dimension of an inchoate operand. The law
/// says a glob does not skip a cell it covers and that no name reaches a
/// latent dimension; whether the glob activates the dimension or refuses
/// is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LatentUnderGlob {
    /// Today: the glob refuses as a latent name.
    Refuse,
}

/// A membership written in a declared table's CHECK, over candidates that
/// may hold NULL (open question: a database CHECK's membership over a NULL
/// candidate).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckMembership {
    /// Today: the CHECK is a verdict about one row. The probe equals a
    /// candidate row when every component equals null-safe, so a NULL row is
    /// admitted by a NULL candidate.
    NullSafe,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Switches {
    pub(crate) crossed_comparison: CrossedComparison,
    pub(crate) lineage_drop: LineageDrop,
    pub(crate) completion_pairing: CompletionPairing,
    pub(crate) latent_under_glob: LatentUnderGlob,
    pub(crate) check_membership: CheckMembership,
}

impl Default for Switches {
    fn default() -> Self {
        Switches {
            crossed_comparison: CrossedComparison::WithinRow,
            lineage_drop: LineageDrop::Refuse,
            completion_pairing: CompletionPairing::Refuse,
            latent_under_glob: LatentUnderGlob::Refuse,
            check_membership: CheckMembership::NullSafe,
        }
    }
}
