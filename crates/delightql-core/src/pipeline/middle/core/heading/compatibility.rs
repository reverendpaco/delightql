// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Structural compatibility (W5 #8): may two headings be combined position
//! by position. A position pair agrees when both carry the same answering
//! name, or when both are the same binder column: the same position of an
//! interior one origin formed. A lost, minted or absent name agrees
//! with nothing else. This reads name states itself and does not ask name
//! correspondence.

use super::{Heading, Known, Made, NameState, Origin, Visibility};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Incompatible {
    pub(crate) position: usize,
}

/// Whether `left` and `right`, each an interior heading with what formed
/// it, are compatible position by position.
pub(crate) fn compatible(
    left: (&Heading, Option<Origin>),
    right: (&Heading, Option<Origin>),
) -> Result<(), Incompatible> {
    let same_source = matches!((left.1, right.1), (Some(a), Some(b)) if a == b);
    let (left, right) = (left.0, right.0);
    if left.len() != right.len() {
        return Err(Incompatible {
            position: left.len().min(right.len()),
        });
    }
    if same_source {
        return Ok(());
    }
    for (i, (l, r)) in left.positions().iter().zip(right.positions()).enumerate() {
        let named = |p: &super::Position| match (&p.visibility, &p.name) {
            (Visibility::Published, NameState::Authored(n) | NameState::Catalog(n)) => Some(n.clone()),
            (
                Visibility::Published | Visibility::Latent | Visibility::Hidden(_),
                NameState::Authored(_) | NameState::Catalog(_) | NameState::Lost(_) | NameState::Minted(_),
            ) => None,
        };
        let agree = match (named(l), named(r)) {
            (Some(a), Some(b)) => a == b,
            (Some(_), None) | (None, Some(_)) | (None, None) => false,
        };
        let interiors = match (&l.interior.known, &r.interior.known) {
            (Known::Shape(a, oa), Known::Shape(b, ob)) => compatible((a, Some(*oa)), (b, Some(*ob))).is_ok(),
            (Known::Shape(..) | Known::PerArm(_), _) | (_, Known::Shape(..) | Known::PerArm(_)) => false,
            _ => l.interior == r.interior || (l.interior.made == Made::Never && r.interior.made == Made::Never),
        };
        if !agree || !interiors {
            return Err(Incompatible { position: i });
        }
    }
    Ok(())
}
