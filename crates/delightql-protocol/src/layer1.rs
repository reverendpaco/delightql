// Relay Protocol — Layer 1 Control Vocabulary
//
// Control operations that live outside the data term vocabulary.
// The DQL compiler produces ClientTerm (layer0) — it *cannot* produce
// a ControlOp. Only programmatic clients (pack-man) can send Control
// messages. Safety by construction: no REPL footgun.

use serde::{Deserialize, Serialize};

use crate::layer0::{ClientTerm, ServerTerm};

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
