// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Name correspondence (W5 #7): which positions answer to a name. A pure
//! reading of stored name states; structural compatibility is a separate
//! judgment and never asks this one.

use super::{Heading, Name, NameState, Visibility};

/// The positions of `heading` that answer to `name`, in heading order.
pub(crate) fn answers_to(heading: &Heading, name: &Name) -> Vec<usize> {
    heading
        .positions()
        .iter()
        .enumerate()
        .filter(|(_, p)| p.answering_name() == Some(name))
        .map(|(i, _)| i)
        .collect()
}

/// The published positions that lost `name` by collision: a bare name
/// that reaches none of them is ambiguous, not absent.
pub(crate) fn lost(heading: &Heading, name: &Name) -> Vec<usize> {
    heading
        .positions()
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.visibility == Visibility::Published && p.name == NameState::Lost(name.clone())
        })
        .map(|(i, _)| i)
        .collect()
}
