// Relay Protocol — MessagePack Manifestation
//
// Serialization: Term <-> msgpack bytes (via rmp-serde)
// Framing: [4 bytes big-endian length][msgpack payload]
//
// Together these are the manifestation — the complete answer to
// "how do logical terms become bytes on the wire and back."

use crate::layer0::{ClientTerm, ServerTerm, TransportError};
use crate::layer1::{ClientMessage, ServerMessage};

// --- Layer 0: data term framing ---

/// Serialize a client term to a length-prefixed msgpack frame.
pub fn frame_client(term: &ClientTerm) -> Result<Vec<u8>, TransportError> {
    let payload = rmp_serde::to_vec(term).map_err(|e| TransportError {
        message: format!("msgpack encode error: {}", e),
    })?;
    Ok(frame(payload))
}

/// Serialize a server term to a length-prefixed msgpack frame.
pub fn frame_server(term: &ServerTerm) -> Result<Vec<u8>, TransportError> {
    let payload = rmp_serde::to_vec(term).map_err(|e| TransportError {
        message: format!("msgpack encode error: {}", e),
    })?;
    Ok(frame(payload))
}

/// Deserialize a client term from a msgpack payload (after framing removed).
pub fn decode_client(payload: &[u8]) -> Result<ClientTerm, TransportError> {
    rmp_serde::from_slice(payload).map_err(|e| TransportError {
        message: format!("msgpack decode error: {}", e),
    })
}

/// Deserialize a server term from a msgpack payload (after framing removed).
pub fn decode_server(payload: &[u8]) -> Result<ServerTerm, TransportError> {
    rmp_serde::from_slice(payload).map_err(|e| TransportError {
        message: format!("msgpack decode error: {}", e),
    })
}

// --- Layer 1: message envelope framing ---

/// Serialize a ClientMessage (data or control) to a length-prefixed msgpack frame.
pub fn frame_client_message(msg: &ClientMessage) -> Result<Vec<u8>, TransportError> {
    let payload = rmp_serde::to_vec(msg).map_err(|e| TransportError {
        message: format!("msgpack encode error: {}", e),
    })?;
    Ok(frame(payload))
}

/// Deserialize a ClientMessage from a msgpack payload.
pub fn decode_client_message(payload: &[u8]) -> Result<ClientMessage, TransportError> {
    rmp_serde::from_slice(payload).map_err(|e| TransportError {
        message: format!("msgpack decode error: {}", e),
    })
}

/// Serialize a ServerMessage (data or control) to a length-prefixed msgpack frame.
pub fn frame_server_message(msg: &ServerMessage) -> Result<Vec<u8>, TransportError> {
    let payload = rmp_serde::to_vec(msg).map_err(|e| TransportError {
        message: format!("msgpack encode error: {}", e),
    })?;
    Ok(frame(payload))
}

/// Deserialize a ServerMessage from a msgpack payload.
pub fn decode_server_message(payload: &[u8]) -> Result<ServerMessage, TransportError> {
    rmp_serde::from_slice(payload).map_err(|e| TransportError {
        message: format!("msgpack decode error: {}", e),
    })
}

// --- Framing ---

/// Read one frame from a byte slice: returns (payload, remaining bytes).
/// Returns None if the slice doesn't contain a complete frame.
pub fn read_frame(buf: &[u8]) -> Result<Option<(&[u8], &[u8])>, TransportError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    Ok(Some((&buf[4..4 + len], &buf[4 + len..])))
}

// --- Internal ---

fn frame(payload: Vec<u8>) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer0::*;

    fn b(s: &str) -> ByteSeq {
        s.as_bytes().to_vec()
    }

    fn dim(pos: Nat, name: &str, desc: &str) -> Dimension {
        Dimension {
            position: pos,
            name: b(name),
            descriptor: b(desc),
        }
    }

    fn cell(s: &str) -> Cell {
        Some(b(s))
    }

    // --- Round-trip: every client term variant ---

    #[test]
    fn round_trip_client_version() {
        let term = ClientTerm::Version { max_message_size: 1_000_000, protocol_version: b("relay0"), lease_ms: 300_000, orientations: vec![Orientation::Rows, Orientation::Columns] };
        let framed = frame_client(&term).unwrap();
        let (payload, rest) = read_frame(&framed).unwrap().unwrap();
        assert!(rest.is_empty());
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_client_query() {
        let term = ClientTerm::Query { text: b("SELECT * FROM users") };
        let framed = frame_client(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_client_fetch_rows() {
        let term = ClientTerm::Fetch {
            handle: b("h1"),
            projection: Projection::All,
            count: 100,
            orientation: Orientation::Rows,
        };
        let framed = frame_client(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_client_fetch_columns() {
        let term = ClientTerm::Fetch {
            handle: b("h1"),
            projection: Projection::Select(vec![ColumnRef::ByName(b("name")), ColumnRef::ByIndex(2)]),
            count: 50,
            orientation: Orientation::Columns,
        };
        let framed = frame_client(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_client_stat() {
        let term = ClientTerm::Stat { handle: b("h7") };
        let framed = frame_client(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_client_close() {
        let term = ClientTerm::Close { handle: b("h1") };
        let framed = frame_client(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_client_prepare() {
        let term = ClientTerm::Prepare {
            text: b("INSERT INTO users (name, email)"),
            dimensions: vec![dim(1, "name", "varchar"), dim(2, "email", "varchar")],
        };
        let framed = frame_client(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_client_offer() {
        let term = ClientTerm::Offer {
            handle: b("h1"),
            cells: vec![
                vec![cell("alice"), cell("alice@example.com")],
                vec![cell("bob"), cell("bob@example.com")],
            ],
            orientation: Orientation::Rows,
        };
        let framed = frame_client(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_client(payload).unwrap(), term);
    }

    // --- Round-trip: every server term variant ---

    #[test]
    fn round_trip_server_version() {
        let term = ServerTerm::Version { max_message_size: 1_000_000, protocol_version: b("relay0"), lease_ms: 300_000, orientations: vec![Orientation::Rows] };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_server_header() {
        let term = ServerTerm::Header {
            handle: b("h1"),
            dimensions: vec![dim(1, "name", "TEXT"), dim(2, "age", "INTEGER")],
        };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_server_data() {
        let term = ServerTerm::Data {
            cells: vec![
                vec![cell("Alice"), cell("30")],
                vec![cell("Bob"), None],
            ],
        };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_server_metadata() {
        let term = ServerTerm::Metadata {
            items: vec![
                MetaItem::Backend(b("sqlite"), b("3.45.0")),
                MetaItem::ExecutionTime(42),
            ],
        };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_server_end() {
        let term = ServerTerm::End;
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_server_ok() {
        let term = ServerTerm::Ok { count_hint: 0 };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_server_ok_with_count() {
        let term = ServerTerm::Ok { count_hint: 1000 };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    #[test]
    fn round_trip_server_error() {
        let term = ServerTerm::Error {
            kind: ErrorKind::Syntax,
            identity: vec![],
            message: b("unexpected token near FROM"),
        };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        assert_eq!(decode_server(payload).unwrap(), term);
    }

    // --- Framing ---

    #[test]
    fn frame_length_prefix_is_correct() {
        let term = ClientTerm::Close { handle: b("h1") };
        let framed = frame_client(&term).unwrap();
        let len = u32::from_be_bytes([framed[0], framed[1], framed[2], framed[3]]) as usize;
        assert_eq!(len, framed.len() - 4);
    }

    #[test]
    fn incomplete_frame_returns_none() {
        let term = ServerTerm::End;
        let framed = frame_server(&term).unwrap();
        // Truncate — missing last byte
        assert!(read_frame(&framed[..framed.len() - 1]).unwrap().is_none());
        // Too short for even the length prefix
        assert!(read_frame(&[0, 0]).unwrap().is_none());
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let t1 = ServerTerm::End;
        let t2 = ServerTerm::Ok { count_hint: 0 };
        let mut buf = frame_server(&t1).unwrap();
        buf.extend(frame_server(&t2).unwrap());

        let (payload1, rest) = read_frame(&buf).unwrap().unwrap();
        assert_eq!(decode_server(payload1).unwrap(), t1);

        let (payload2, rest) = read_frame(rest).unwrap().unwrap();
        assert_eq!(decode_server(payload2).unwrap(), t2);
        assert!(rest.is_empty());
    }

    // --- Opaque binary cells survive round-trip ---

    #[test]
    fn binary_cells_round_trip() {
        // Random non-UTF8 bytes — the protocol doesn't care
        let raw_bytes: Vec<u8> = (0..=255).collect();
        let term = ServerTerm::Data {
            cells: vec![vec![Some(raw_bytes.clone()), None, Some(vec![0, 0, 0])]],
        };
        let framed = frame_server(&term).unwrap();
        let (payload, _) = read_frame(&framed).unwrap().unwrap();
        let decoded = decode_server(payload).unwrap();
        assert_eq!(decoded, term);

        // Verify the raw bytes are exactly preserved
        if let ServerTerm::Data { cells } = decoded {
            assert_eq!(cells[0][0], Some(raw_bytes));
            assert_eq!(cells[0][1], None);
        }
    }
}
