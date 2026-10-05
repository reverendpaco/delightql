// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund

pub mod carrier;
pub mod form;
pub mod port;
pub(crate) mod support;

pub use carrier::StructuralRelation;
pub use support::Correlated;

pub use carrier::{NamedScratch, ScratchRow, SemanticRelation};
pub use port::PortId;
