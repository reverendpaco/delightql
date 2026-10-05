// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The core representation: binders, nodes and the graph. Node facts are
//! decided by the node constructors; the graph is frozen by
//! [`graph::Builder::finish`].

pub(crate) mod decide;
pub(crate) mod dump;
pub(crate) mod graph;
pub(crate) mod heading;
pub(crate) mod ids;
pub(crate) mod instance;
pub(crate) mod node;
#[cfg(debug_assertions)]
mod recompute;
pub(crate) mod refuse;
pub(crate) mod switches;
