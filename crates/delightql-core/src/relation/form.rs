// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

use super::port::PortId;

/// A merged position: one output standing for a port of each operand.
///
/// Recorded at the join rather than inferred from a shared name, because a
/// name cannot tell one operand's `id` from the other's and stops working
/// entirely once the name is one the compiler drew.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MergedKey {
    pub left: PortId,
    pub right: PortId,
}

impl crate::lispy::ToLispy for MergedKey {
    fn to_lispy(&self) -> String {
        format!("({:?} {:?})", self.left, self.right)
    }
}

