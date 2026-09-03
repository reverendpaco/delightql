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

/// One scalar value class.
///
/// Several ports may carry this identity, but the store exposes only the
/// one-way `PortId -> ValueId` question.  There is deliberately no inverse:
/// choosing one of several positions that happen to carry a value is not a
/// semantic operation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ValueId(pub(super) u32);

/// One typed-null contribution standing where an arm publishes nothing.
///
/// Its own identity rather than an absent port: a set result must be able
/// to say WHICH cell is padded and what null it pads with, and an absence
/// cannot carry either.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PaddingId(pub(super) u32);

/// One row-producing occurrence.
///
/// Separate from [`PortId`] because a relation is not its first column, and
/// separate from a definition because two uses of one definition are two
/// relations.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelationId(pub(super) u32);

impl PortId {
    /// The registry occurrence this port is stored under.
    ///
    /// READ-ONLY, and there is no inverse. The phases that have not yet
    /// moved onto the authority address columns by that identity; a column
    /// cannot become a port, so holding one publishes nothing.
    pub(crate) fn column(self) -> crate::names::ColId {
        self.0
    }
}

/// One ordered interface's contents: its ports, in publication order.
///
/// Fields are private and there is no constructor outside the authority.
/// An interface cannot be assembled beside a relation and attached to it —
/// the authority derives both together or neither exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interface {
    ports: Vec<PortId>,
    /// A relation whose dimensions are not enumerable. Identity exists;
    /// the heading does not. Distinct from a heading of width zero.
    opaque: bool,
}

impl Interface {
    pub(super) fn of(ports: Vec<PortId>) -> Self {
        Interface {
            ports,
            opaque: false,
        }
    }

    pub(super) fn opaque() -> Self {
        Interface {
            ports: Vec::new(),
            opaque: true,
        }
    }

    /// The ports, in publication order.
    pub fn ports(&self) -> &[PortId] {
        &self.ports
    }

    /// Whether the dimensions are unenumerable rather than absent.
    pub fn is_opaque(&self) -> bool {
        self.opaque
    }

    /// The published width. Zero for an opaque interface, which is why
    /// `is_opaque` and not this is the question a narrowing asks.
    pub fn width(&self) -> usize {
        self.ports.len()
    }
}

impl fmt::Debug for PortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl fmt::Debug for ValueId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "value#{}", self.0)
    }
}

impl fmt::Debug for PaddingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pad#{}", self.0)
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
