// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Minus: a left export plus an exact anti-match.
//!
//! Minus is not a merge. Its right arm contributes no row and no output
//! position — it is only ever probed — so treating it as a two-arm union
//! that happens to subtract puts right-side ports into result lineage,
//! where a later reader can address them.
//!
//! The structure says both halves separately. The left heading is exported
//! one-to-one into fresh result ports, and the right ports appear ONLY in
//! [`ExactPair`]s, which the null-safe anti-match predicate is built from
//! and nothing else reads.

use super::port::{PortId, RelationId};

/// One left port and the right port it must agree with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExactPair {
    left: PortId,
    right: PortId,
}

impl ExactPair {
    pub(super) fn of(left: PortId, right: PortId) -> Self {
        ExactPair { left, right }
    }

    pub fn left(self) -> PortId {
        self.left
    }

    /// The probed position. Evidence for the anti-match predicate, never
    /// lineage: nothing in the result carries it.
    pub fn right(self) -> PortId {
        self.right
    }
}

/// Why an exact-heading map refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExactHeadingError {
    /// The two headings publish different widths.
    DegreeMismatch { left: usize, right: usize },
    /// A port on one side appears in more than one pair, or in none.
    NotBijective,
    /// One side's dimensions are not enumerable, so exactness is
    /// unprovable rather than false.
    OpaqueHeading,
}

/// A total, bidirectional correspondence between two exact headings.
///
/// Every left port and every right port appears exactly once. A degree
/// mismatch, a repeated port, an unpaired port, or an opaque heading
/// refuses here — before any occurrence is minted — because a minus whose
/// exactness is only probable has no lawful result to publish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExactHeadingMap {
    left: RelationId,
    right: RelationId,
    pairs: Vec<ExactPair>,
}

impl ExactHeadingMap {
    pub(super) fn build(
        left: RelationId,
        right: RelationId,
        left_ports: &[PortId],
        right_ports: &[PortId],
        left_opaque: bool,
        right_opaque: bool,
        pairs: Vec<ExactPair>,
    ) -> Result<Self, ExactHeadingError> {
        if left_opaque || right_opaque {
            return Err(ExactHeadingError::OpaqueHeading);
        }
        if left_ports.len() != right_ports.len() {
            return Err(ExactHeadingError::DegreeMismatch {
                left: left_ports.len(),
                right: right_ports.len(),
            });
        }
        if pairs.len() != left_ports.len() {
            return Err(ExactHeadingError::NotBijective);
        }
        for port in left_ports {
            if pairs.iter().filter(|pair| pair.left == *port).count() != 1 {
                return Err(ExactHeadingError::NotBijective);
            }
        }
        for port in right_ports {
            if pairs.iter().filter(|pair| pair.right == *port).count() != 1 {
                return Err(ExactHeadingError::NotBijective);
            }
        }
        Ok(ExactHeadingMap { left, right, pairs })
    }

    /// The pairs, in the left heading's order.
    pub fn pairs(&self) -> &[ExactPair] {
        &self.pairs
    }

    /// The exported operand, as construction saw it: the relation and the
    /// ordered interface it published.
    ///
    /// The pairs ARE that heading — they were built by walking it, and the
    /// bijection check refuses unless every one of its ports appears exactly
    /// once — so the physical binding reads the export from here rather than
    /// pairing the two lists again.
    pub fn left_arm(&self) -> super::set::SetArmRecord {
        super::set::SetArmRecord::of(
            self.left,
            self.pairs.iter().map(|pair| pair.left()).collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::names::Addressing;

    struct Fixture {
        registry: crate::relation::Planning,
        scope: crate::names::ScopeId,
        next_relation: std::cell::Cell<u32>,
    }

    impl Fixture {
        fn new() -> Self {
            let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
            let scope = registry.anonymous_scope(None);
            Fixture {
                registry,
                scope,
                next_relation: std::cell::Cell::new(0),
            }
        }

        fn port(&self, _position: u32) -> PortId {
            PortId(self.registry.sql_column(self.scope, None, Addressing::Bare))
        }

        fn relation(&self) -> RelationId {
            let id = self.next_relation.get();
            self.next_relation.set(id + 1);
            RelationId(id)
        }
    }

    #[test]
    fn a_total_bijection_is_admitted() {
        let f = Fixture::new();
        let (l0, l1) = (f.port(0), f.port(1));
        let (r0, r1) = (f.port(2), f.port(3));
        let map = ExactHeadingMap::build(
            f.relation(),
            f.relation(),
            &[l0, l1],
            &[r0, r1],
            false,
            false,
            vec![ExactPair::of(l0, r0), ExactPair::of(l1, r1)],
        );
        assert_eq!(map.map(|m| m.pairs().len()), Ok(2));
    }

    #[test]
    fn a_degree_mismatch_refuses() {
        let f = Fixture::new();
        let l0 = f.port(0);
        let (r0, r1) = (f.port(1), f.port(2));
        assert_eq!(
            ExactHeadingMap::build(
                f.relation(),
                f.relation(),
                &[l0],
                &[r0, r1],
                false,
                false,
                vec![ExactPair::of(l0, r0)],
            ),
            Err(ExactHeadingError::DegreeMismatch { left: 1, right: 2 })
        );
    }

    #[test]
    fn a_repeated_side_refuses() {
        let f = Fixture::new();
        let (l0, l1) = (f.port(0), f.port(1));
        let (r0, r1) = (f.port(2), f.port(3));
        assert_eq!(
            ExactHeadingMap::build(
                f.relation(),
                f.relation(),
                &[l0, l1],
                &[r0, r1],
                false,
                false,
                vec![ExactPair::of(l0, r0), ExactPair::of(l1, r0)],
            ),
            Err(ExactHeadingError::NotBijective)
        );
    }

    #[test]
    fn an_opaque_heading_refuses_rather_than_guessing() {
        let f = Fixture::new();
        assert_eq!(
            ExactHeadingMap::build(f.relation(), f.relation(), &[], &[], false, true, vec![]),
            Err(ExactHeadingError::OpaqueHeading)
        );
    }

    #[test]
    fn a_right_port_never_becomes_lineage() {
        // The only road out of the map is `pairs`, and the only thing the
        // right half of a pair is used for is the anti-match predicate.
        // There is no accessor that returns the right heading as ports to
        // publish.
        let f = Fixture::new();
        let l0 = f.port(0);
        let r0 = f.port(1);
        let map = ExactHeadingMap::build(
            f.relation(),
            f.relation(),
            &[l0],
            &[r0],
            false,
            false,
            vec![ExactPair::of(l0, r0)],
        )
        .expect("a one-to-one pairing");
        assert_eq!(map.pairs().len(), 1);
        assert_eq!(map.pairs()[0].left(), l0);
        assert_eq!(map.pairs()[0].right(), r0);
    }
}
