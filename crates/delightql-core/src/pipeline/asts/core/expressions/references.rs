// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! References — the one carrier for addressing a column.
//!
//! A name and a position ask the SAME addressing question of the same
//! authority; what differs is how the author spelled it, which is data a
//! diagnostic reads and nothing else decides from. Resolution answers both
//! against the heading and what comes back is one occurrence, so the
//! positional spelling is uninhabited afterwards.

use super::super::{Phase, Unresolved};
use crate::{lispy::ToLispy, ToLispy};

/// `age`, `u.age`, `|2|`, `u|-1|` — one carrier, two authored spellings.
#[derive(Debug, Clone, PartialEq, ToLispy)]
pub enum Reference<P: Phase = Unresolved> {
    #[lispy("reference:named")]
    Named(NamedReference<P>),
    /// A POSITION. Spent at resolution into the occurrence a named
    /// reference carries, so the payload is uninhabited after it.
    #[lispy("reference:ordinal")]
    Ordinal(P::ColumnOrdinal),
    /// A physical SQL slot introduced only while lowering a refined tree.
    /// It is not semantic lookup evidence and no resolver constructs it.
    #[lispy("reference:physical")]
    Physical(P::PhysicalColumn),
}

/// The one node that names a column. What it HOLDS is the phase's answer:
/// authored characters before resolution, the occurrence they were bound to
/// after. See [`super::super::columns`].
#[derive(Debug, Clone, PartialEq, ToLispy)]
#[lispy("named_reference")]
pub struct NamedReference<P: Phase = Unresolved>(pub P::Col);

impl<P: Phase> NamedReference<P> {
    pub fn column(&self) -> &P::Col {
        &self.0
    }
}

impl<P: Phase> Reference<P> {
    pub fn named(column: P::Col) -> Self {
        Self::Named(NamedReference(column))
    }
}

impl Reference<crate::pipeline::asts::core::Refined> {
    pub(crate) fn physical(column: crate::names::ColId) -> Self {
        Self::Physical(column)
    }
}
