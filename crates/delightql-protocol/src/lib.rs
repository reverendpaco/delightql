// Relay Protocol
//
// Layer 0: 14 data terms (ClientTerm, ServerTerm) + typestate client
// Layer 1: Control vocabulary (Reset) — structurally unreachable from DQL

pub mod layer0;
pub mod layer1;
pub mod manifest;
#[cfg(unix)]
pub mod socket;

// Re-export layer0 for backward compat (existing code uses `delightql_protocol::ClientTerm`)
pub use layer0::*;

// Re-export layer1 control types
pub use layer1::{ClientMessage, ControlOp, ControlResult, ServerMessage};

// Re-export layer1 cell encoding utilities
pub use layer1::{
    CELL_TAG_BLOB, CELL_TAG_INTEGER, CELL_TAG_REAL, CELL_TAG_TEXT, decode_cell_for_hash,
    decode_cell_to_text,
};
