// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Output positions.
//!
//! A PORT is one output position of one relation occurrence. It is not a
//! value: `q.*, q.*, q.*` publishes three ports carrying one value, and
//! `|2|` selects the second port by position without searching for it.
//!
//! The one-way road is deliberate. A port answers which value it carries;
//! no API answers which port carries a value, because every road that ever
//! did picked one of several equal positions and called it the answer.

use std::fmt;

/// One output position of one relation occurrence.
///
/// Opaque, with a private payload and no public constructor: only the
/// semantic authority mints one, so a port in a heading is a port that
/// authority put there.
///
/// The payload is the registry occurrence the port is stored under. It is
/// private to the authority and there is no road back — nothing outside
/// can wrap a column into a port, which is what stops a phase from
/// deciding for itself that some column is an output position.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PortId(pub(super) crate::names::ColId);

/// One row-producing occurrence.
///
/// Separate from [`PortId`] because a relation is not its first column, and
/// separate from a definition because two uses of one definition are two
/// relations.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationId(pub(super) u32);

impl fmt::Debug for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl fmt::Debug for RelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "relation#{}", self.0)
    }
}

impl crate::lispy::ToLispy for PortId {
    fn to_lispy(&self) -> String {
        format!("{self:?}")
    }
}
