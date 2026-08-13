// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Relay Protocol
//
// Layer 0: 14 data terms (ClientTerm, ServerTerm) + typestate client
// Layer 1: Control vocabulary (Reset) — structurally unreachable from DQL

pub mod layer0;
pub mod layer1;
pub mod manifest;
#[cfg(unix)]
pub mod socket;
pub mod stdio;

// Layer 0's vocabulary IS the crate's public vocabulary; the module split
// is an authoring boundary, not a namespace consumers must spell.
pub use layer0::*;

// Re-export layer1 control types
pub use layer1::{ClientMessage, ControlOp, ControlResult, ServerMessage};
