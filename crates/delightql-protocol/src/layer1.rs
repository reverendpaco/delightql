// Relay Protocol — Layer 1 Control Vocabulary + Cell Encoding
//
// Control operations that live outside the data term vocabulary.
// The DQL compiler produces ClientTerm (layer0) — it *cannot* produce
// a ControlOp. Only programmatic clients (pack-man) can send Control
// messages. Safety by construction: no REPL footgun.
//
// Cell type tags: per-cell type identification for content-blind cells.
// Layer 0 cells are opaque Option<Vec<u8>>. Layer 1 adds a convention:
// the first byte identifies the value type.

use serde::{Deserialize, Serialize};

use crate::layer0::{ClientTerm, ServerTerm};

// --- Cell type tags ---

pub const CELL_TAG_TEXT: u8 = 0x00;
pub const CELL_TAG_INTEGER: u8 = 0x01;
pub const CELL_TAG_REAL: u8 = 0x02;
pub const CELL_TAG_BLOB: u8 = 0x03;

/// Decode a tagged cell to its text representation.
/// Returns the text for display. Legacy (untagged) cells are treated as raw UTF-8.
pub fn decode_cell_to_text(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    match bytes[0] {
        CELL_TAG_INTEGER if bytes.len() == 9 => {
            let n = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
            n.to_string()
        }
        CELL_TAG_REAL if bytes.len() == 9 => {
            let f = f64::from_le_bytes(bytes[1..9].try_into().unwrap());
            f.to_string()
        }
        CELL_TAG_BLOB => {
            format!("<blob {} bytes>", bytes.len() - 1)
        }
        CELL_TAG_TEXT => String::from_utf8_lossy(&bytes[1..]).to_string(),
        _ => {
            // Legacy cells without tags — treat as raw UTF-8
            String::from_utf8_lossy(bytes).to_string()
        }
    }
}

/// Decode a tagged cell to raw bytes for hashing.
/// Strips the type tag, returns content bytes. For non-blob types,
/// returns the TEXT representation as bytes (preserves hash compat).
pub fn decode_cell_for_hash(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return vec![];
    }
    match bytes[0] {
        CELL_TAG_INTEGER if bytes.len() == 9 => {
            let n = i64::from_le_bytes(bytes[1..9].try_into().unwrap());
            n.to_string().into_bytes()
        }
        CELL_TAG_REAL if bytes.len() == 9 => {
            let f = f64::from_le_bytes(bytes[1..9].try_into().unwrap());
            f.to_string().into_bytes()
        }
        CELL_TAG_BLOB => {
            // Hash actual blob content
            bytes[1..].to_vec()
        }
        CELL_TAG_TEXT => bytes[1..].to_vec(),
        _ => {
            // Legacy: hash as-is
            bytes.to_vec()
        }
    }
}

/// Control operations (layer1). Structurally unreachable from DQL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlOp {
    Reset,
    Shutdown,
    /// Set per-session base path for relative file resolution (mount!, consult!, etc.).
    /// Cleared by Reset. Only meaningful over socket transport (pack-man → dql server).
    Cwd(String),
}

/// Result of a control operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlResult {
    Ok,
    Error { message: String },
}

/// Wire-level envelope: data term (layer0) or control op (layer1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMessage {
    Data(ClientTerm),
    Control(ControlOp),
}

/// Wire-level envelope: data term (layer0) or control result (layer1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerMessage {
    Data(ServerTerm),
    Control(ControlResult),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer0::*;

    fn b(s: &str) -> ByteSeq {
        s.as_bytes().to_vec()
    }

    // --- Cell decode tests ---

    #[test]
    fn decode_text_cell() {
        let mut cell = vec![CELL_TAG_TEXT];
        cell.extend_from_slice(b"hello");
        assert_eq!(decode_cell_to_text(&cell), "hello");
        assert_eq!(decode_cell_for_hash(&cell), b"hello".to_vec());
    }

    #[test]
    fn decode_integer_cell() {
        let mut cell = vec![CELL_TAG_INTEGER];
        cell.extend_from_slice(&42i64.to_le_bytes());
        assert_eq!(decode_cell_to_text(&cell), "42");
        assert_eq!(decode_cell_for_hash(&cell), b"42".to_vec());
    }

    #[test]
    fn decode_negative_integer_cell() {
        let mut cell = vec![CELL_TAG_INTEGER];
        cell.extend_from_slice(&(-7i64).to_le_bytes());
        assert_eq!(decode_cell_to_text(&cell), "-7");
        assert_eq!(decode_cell_for_hash(&cell), b"-7".to_vec());
    }

    #[test]
    fn decode_real_cell() {
        let mut cell = vec![CELL_TAG_REAL];
        cell.extend_from_slice(&3.14f64.to_le_bytes());
        assert_eq!(decode_cell_to_text(&cell), "3.14");
        assert_eq!(decode_cell_for_hash(&cell), b"3.14".to_vec());
    }

    #[test]
    fn decode_blob_cell() {
        let mut cell = vec![CELL_TAG_BLOB];
        cell.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(decode_cell_to_text(&cell), "<blob 4 bytes>");
        assert_eq!(decode_cell_for_hash(&cell), vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn decode_empty_cell() {
        assert_eq!(decode_cell_to_text(&[]), "");
        assert_eq!(decode_cell_for_hash(&[]), vec![] as Vec<u8>);
    }

    #[test]
    fn decode_legacy_cell() {
        // A cell without a known tag — treated as raw UTF-8
        let cell = b"legacy data".to_vec();
        assert_eq!(decode_cell_to_text(&cell), "legacy data");
        assert_eq!(decode_cell_for_hash(&cell), b"legacy data".to_vec());
    }

    #[test]
    fn integer_hash_matches_string_hash() {
        // Key invariant: i64 → tagged cell → decode_cell_for_hash == "42".as_bytes()
        let n: i64 = 42;
        let mut cell = vec![CELL_TAG_INTEGER];
        cell.extend_from_slice(&n.to_le_bytes());
        assert_eq!(decode_cell_for_hash(&cell), n.to_string().as_bytes());
    }

    #[test]
    fn real_hash_matches_string_hash() {
        let f: f64 = 3.14;
        let mut cell = vec![CELL_TAG_REAL];
        cell.extend_from_slice(&f.to_le_bytes());
        assert_eq!(decode_cell_for_hash(&cell), f.to_string().as_bytes());
    }

    #[test]
    fn control_reset_round_trip() {
        let msg = ClientMessage::Control(ControlOp::Reset);
        let framed = crate::manifest::frame_client_message(&msg).unwrap();
        let (payload, _) = crate::manifest::read_frame(&framed).unwrap().unwrap();
        let decoded = crate::manifest::decode_client_message(payload).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn control_cwd_round_trip() {
        let msg = ClientMessage::Control(ControlOp::Cwd("/tmp/dql-isolate-abc123".into()));
        let framed = crate::manifest::frame_client_message(&msg).unwrap();
        let (payload, _) = crate::manifest::read_frame(&framed).unwrap().unwrap();
        let decoded = crate::manifest::decode_client_message(payload).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn control_result_ok_round_trip() {
        let msg = ServerMessage::Control(ControlResult::Ok);
        let framed = crate::manifest::frame_server_message(&msg).unwrap();
        let (payload, _) = crate::manifest::read_frame(&framed).unwrap().unwrap();
        let decoded = crate::manifest::decode_server_message(payload).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn control_result_error_round_trip() {
        let msg = ServerMessage::Control(ControlResult::Error {
            message: "reset failed: db locked".to_string(),
        });
        let framed = crate::manifest::frame_server_message(&msg).unwrap();
        let (payload, _) = crate::manifest::read_frame(&framed).unwrap().unwrap();
        let decoded = crate::manifest::decode_server_message(payload).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn data_term_in_message_envelope() {
        let msg = ClientMessage::Data(ClientTerm::Query {
            text: b("users(*)"),
        });
        let framed = crate::manifest::frame_client_message(&msg).unwrap();
        let (payload, _) = crate::manifest::read_frame(&framed).unwrap().unwrap();
        let decoded = crate::manifest::decode_client_message(payload).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn server_data_in_message_envelope() {
        let msg = ServerMessage::Data(ServerTerm::End);
        let framed = crate::manifest::frame_server_message(&msg).unwrap();
        let (payload, _) = crate::manifest::read_frame(&framed).unwrap().unwrap();
        let decoded = crate::manifest::decode_server_message(payload).unwrap();
        assert_eq!(decoded, msg);
    }
}
