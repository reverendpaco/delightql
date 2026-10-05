// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Free variables (W5 #3): a node's free binders are its children's, less
//! the binders the node itself introduces.

use crate::pipeline::middle::core::ids::BinderId;
use crate::pipeline::middle::core::node::BinderSet;

/// `fv(n) = ∪ fv(children) − introduced(n)`.
pub(crate) fn of<'a>(
    children: impl IntoIterator<Item = &'a BinderSet>,
    introduced: &[BinderId],
) -> BinderSet {
    let mut out = BinderSet::new();
    for child in children {
        out.extend(child.iter().copied());
    }
    for binder in introduced {
        out.remove(binder);
    }
    out
}
