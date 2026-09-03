// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! THE CALLER ROW AT A CALLABLE STANDING IN JOIN POSITION.
//!
//! A join's left operand is resolved before the callable on its right, and
//! the callable may take that row into itself — a higher-order expansion
//! binds it as a structural carrier, a closed residual seals it into a
//! configured value. What happened to the row was carried as an
//! `Option<ResolvedRelation>` beside a `bool`: four states for three
//! answers, with a refusal standing where the two disagreed.
//!
//! One value answers instead. The row is here, or something at the call
//! absorbed it, or none ever stood — and the road that takes the row is
//! the road that records the taking, so no caller states the outcome
//! twice and nothing can spend the row after it is gone.

use crate::pipeline::resolver::ResolvedRelation;

/// WHAT STANDS AT A CALLABLE'S LEFT, AND WHAT BECAME OF IT.
///
/// The standing row is not held here: it is the innermost FRAME of the
/// fold's lexical position, entered by the join road so the callable's
/// arguments resolve over it, and it comes back from that frame exactly
/// once — to the call that absorbs it, or to the join road that assembles
/// the join. This value records which.
pub(crate) enum CallerRow {
    /// Nothing stands here. A callable that is not a join's right member
    /// resolves in this state, and so does one whose row was never built.
    Absent,
    /// The resolved left operand stands as the position's innermost frame,
    /// available to exactly one consumer.
    Framed,
    /// Taken by the call. Ordinary join assembly must not add it again —
    /// the carrier the call built already stands for it.
    Absorbed,
}

impl CallerRow {
    /// Whether a row is still standing here.
    pub(crate) fn stands(&self) -> bool {
        matches!(self, CallerRow::Framed)
    }

    /// Borrow the exact row that stands at this call before a consumer
    /// takes it: the position's innermost frame.
    pub(crate) fn standing_relation<'p>(
        &self,
        lexical: &'p crate::pipeline::resolver::Position<'_>,
    ) -> Option<&'p ResolvedRelation> {
        match self {
            CallerRow::Framed => lexical.current(),
            CallerRow::Absent | CallerRow::Absorbed => None,
        }
    }

    /// TAKE THE ROW INTO THIS CALL.
    ///
    /// The one road that spends a standing row, and the one that records
    /// the site as having absorbed it: the frame the join road entered is
    /// left here, by value. Absorbing what is already gone answers with
    /// nothing and leaves the site as it stood: a call that finds no row
    /// did not absorb one.
    pub(crate) fn absorb(
        &mut self,
        lexical: &mut crate::pipeline::resolver::Position<'_>,
    ) -> Option<ResolvedRelation> {
        match std::mem::replace(self, CallerRow::Absorbed) {
            CallerRow::Framed => Some(lexical.leave()),
            CallerRow::Absent => {
                *self = CallerRow::Absent;
                None
            }
            CallerRow::Absorbed => None,
        }
    }
}
