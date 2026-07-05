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

// Re-export layer0 for backward compat (existing code uses `delightql_protocol::ClientTerm`)
pub use layer0::*;

// Re-export layer1 control types
pub use layer1::{ClientMessage, ControlOp, ControlResult, ServerMessage};
