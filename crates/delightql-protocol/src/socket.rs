// Relay Protocol — Unix Socket Transport
//
// Implements the Transport trait over a UnixStream, plus server-side
// helpers for reading ClientTerms and writing ServerTerms.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use crate::layer0::{ClientTerm, ServerTerm, Transport, TransportError};
use crate::layer1::{ClientMessage, ControlOp, ControlResult, ServerMessage};
use crate::manifest;

/// Transport implementation over a Unix domain socket.
pub struct SocketTransport {
    stream: UnixStream,
    buf: Vec<u8>,
}

impl SocketTransport {
    pub fn new(stream: UnixStream) -> Self {
        SocketTransport {
            stream,
            buf: Vec::new(),
        }
    }

    /// Send a control operation and receive the result.
    /// This is layer1-only — data terms go through Transport::exchange().
    pub fn control(&mut self, op: ControlOp) -> Result<ControlResult, TransportError> {
        let msg = ClientMessage::Control(op);
        let frame = manifest::frame_client_message(&msg)?;
        self.stream.write_all(&frame).map_err(|e| TransportError {
            message: format!("socket write error: {}", e),
        })?;

        loop {
            match manifest::read_frame(&self.buf)? {
                Some((payload, rest)) => {
                    let response = manifest::decode_server_message(payload)?;
                    self.buf = rest.to_vec();
                    match response {
                        ServerMessage::Control(result) => return Ok(result),
                        ServerMessage::Data(term) => {
                            return Err(TransportError {
                                message: format!(
                                    "protocol violation: expected Control response, got Data({:?})",
                                    term
                                ),
                            });
                        }
                    }
                }
                None => {
                    let mut tmp = [0u8; 8192];
                    let n = self.stream.read(&mut tmp).map_err(|e| TransportError {
                        message: format!("socket read error: {}", e),
                    })?;
                    if n == 0 {
                        return Err(TransportError {
                            message: "connection closed".into(),
                        });
                    }
                    self.buf.extend_from_slice(&tmp[..n]);
                }
            }
        }
    }
}

impl Transport for SocketTransport {
    fn exchange(&mut self, term: ClientTerm) -> Result<ServerTerm, TransportError> {
        // Wrap in ClientMessage::Data envelope for layer1-aware servers
        let msg = ClientMessage::Data(term);
        let frame = manifest::frame_client_message(&msg)?;
        self.stream.write_all(&frame).map_err(|e| TransportError {
            message: format!("socket write error: {}", e),
        })?;

        // Read response — may need multiple reads for a complete frame
        loop {
            match manifest::read_frame(&self.buf)? {
                Some((payload, rest)) => {
                    let response = manifest::decode_server_message(payload)?;
                    self.buf = rest.to_vec();
                    match response {
                        ServerMessage::Data(term) => return Ok(term),
                        ServerMessage::Control(result) => {
                            return Err(TransportError {
                                message: format!(
                                    "protocol violation: expected Data response, got Control({:?})",
                                    result
                                ),
                            });
                        }
                    }
                }
                None => {
                    // Need more data
                    let mut tmp = [0u8; 8192];
                    let n = self.stream.read(&mut tmp).map_err(|e| TransportError {
                        message: format!("socket read error: {}", e),
                    })?;
                    if n == 0 {
                        return Err(TransportError {
                            message: "connection closed".into(),
                        });
                    }
                    self.buf.extend_from_slice(&tmp[..n]);
                }
            }
        }
    }
}

// --- Server-side helpers ---

/// Read one ClientMessage from a UnixStream. Blocks until a complete frame arrives.
pub fn read_client_message(
    stream: &mut UnixStream,
    buf: &mut Vec<u8>,
) -> Result<ClientMessage, TransportError> {
    loop {
        match manifest::read_frame(buf)? {
            Some((payload, rest)) => {
                let msg = manifest::decode_client_message(payload)?;
                *buf = rest.to_vec();
                return Ok(msg);
            }
            None => {
                let mut tmp = [0u8; 8192];
                let n = stream.read(&mut tmp).map_err(|e| TransportError {
                    message: format!("socket read error: {}", e),
                })?;
                if n == 0 {
                    return Err(TransportError {
                        message: "connection closed".into(),
                    });
                }
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }
}

/// Write one ServerMessage to a UnixStream as a framed message.
pub fn write_server_message(
    stream: &mut UnixStream,
    msg: &ServerMessage,
) -> Result<(), TransportError> {
    let frame = manifest::frame_server_message(msg)?;
    stream.write_all(&frame).map_err(|e| TransportError {
        message: format!("socket write error: {}", e),
    })
}

// --- Legacy helpers (layer0-only, kept for backward compat) ---

/// Read one ClientTerm from a UnixStream. Blocks until a complete frame arrives.
/// DEPRECATED: Use read_client_message() for layer1-aware servers.
pub fn read_client_term(
    stream: &mut UnixStream,
    buf: &mut Vec<u8>,
) -> Result<ClientTerm, TransportError> {
    loop {
        match manifest::read_frame(buf)? {
            Some((payload, rest)) => {
                // Try decoding as ClientMessage first (layer1 envelope)
                if let Ok(msg) = manifest::decode_client_message(payload) {
                    *buf = rest.to_vec();
                    match msg {
                        ClientMessage::Data(term) => return Ok(term),
                        ClientMessage::Control(_) => {
                            return Err(TransportError {
                                message: "unexpected control message on layer0 path".into(),
                            });
                        }
                    }
                }
                // Fall back to raw ClientTerm decode (legacy layer0 clients)
                let term = manifest::decode_client(payload)?;
                *buf = rest.to_vec();
                return Ok(term);
            }
            None => {
                let mut tmp = [0u8; 8192];
                let n = stream.read(&mut tmp).map_err(|e| TransportError {
                    message: format!("socket read error: {}", e),
                })?;
                if n == 0 {
                    return Err(TransportError {
                        message: "connection closed".into(),
                    });
                }
                buf.extend_from_slice(&tmp[..n]);
            }
        }
    }
}

/// Write one ServerTerm to a UnixStream as a framed message.
/// DEPRECATED: Use write_server_message() for layer1-aware servers.
pub fn write_server_term(
    stream: &mut UnixStream,
    term: &ServerTerm,
) -> Result<(), TransportError> {
    let frame = manifest::frame_server(term)?;
    stream.write_all(&frame).map_err(|e| TransportError {
        message: format!("socket write error: {}", e),
    })
}

// --- Session::reset() ---

use crate::layer0::Session;

impl Session<SocketTransport> {
    /// Send a Reset control op to the server. Only available over socket transport.
    pub fn reset(&mut self) -> Result<ControlResult, TransportError> {
        self.transport.control(ControlOp::Reset)
    }

    /// Send a Shutdown control op to the server. Only available over socket transport.
    pub fn shutdown(&mut self) -> Result<ControlResult, TransportError> {
        self.transport.control(ControlOp::Shutdown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer0::*;
    use std::os::unix::net::UnixStream;

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

    /// Spawn a mock server thread that reads ClientMessages and replies with
    /// scripted ServerMessages, then run a SocketTransport client against it.
    #[test]
    fn socket_transport_round_trip() {
        let (client_stream, mut server_stream) = UnixStream::pair().unwrap();

        let server = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let responses = vec![
                ServerMessage::Data(ServerTerm::Version {
                    max_message_size: 1_000_000,
                    protocol_version: b("relay0"),
                    lease_ms: 300_000,
                    orientations: vec![Orientation::Rows],
                }),
                ServerMessage::Data(ServerTerm::Header {
                    handle: b("h1"),
                    dimensions: vec![dim(1, "name", "TEXT")],
                }),
                ServerMessage::Data(ServerTerm::Data {
                    cells: vec![vec![cell("Alice")], vec![cell("Bob")]],
                }),
                ServerMessage::Data(ServerTerm::End),
                ServerMessage::Data(ServerTerm::Ok { count_hint: 0 }),
            ];

            for resp in &responses {
                // Read client message (we don't inspect it here)
                let _client_msg = read_client_message(&mut server_stream, &mut buf).unwrap();
                write_server_message(&mut server_stream, resp).unwrap();
            }
        });

        let transport = SocketTransport::new(client_stream);
        let client = Client::new(transport);

        // Version handshake
        let session_result = client
            .version(
                1_000_000,
                b("relay0"),
                300_000,
                vec![Orientation::Rows],
            )
            .unwrap();
        let mut session = match session_result {
            VersionResult::Accepted(s) => s,
            VersionResult::Rejected { .. } => panic!("expected Accepted"),
        };

        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        // Query
        let handle = match session.query(b("SELECT name FROM t")).unwrap() {
            QueryResponse::Header { handle, dimensions } => {
                assert_eq!(dimensions.len(), 1);
                assert_eq!(dimensions[0].name, b("name"));
                handle
            }
            QueryResponse::Error { .. } => panic!("expected Header"),
        };

        // Fetch
        match session.fetch(&handle, Projection::All, 100, rows).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(cells.len(), 2);
                assert_eq!(cells[0][0], cell("Alice"));
                assert_eq!(cells[1][0], cell("Bob"));
            }
            _ => panic!("expected Data"),
        }

        // End
        assert_eq!(
            session.fetch(&handle, Projection::All, 100, rows).unwrap(),
            FetchResponse::End
        );

        // Close
        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);

        server.join().unwrap();
    }

    #[test]
    fn socket_transport_control_reset() {
        let (client_stream, mut server_stream) = UnixStream::pair().unwrap();

        let server = std::thread::spawn(move || {
            let mut buf = Vec::new();

            // Read version handshake
            let msg = read_client_message(&mut server_stream, &mut buf).unwrap();
            assert!(matches!(msg, ClientMessage::Data(ClientTerm::Version { .. })));
            write_server_message(
                &mut server_stream,
                &ServerMessage::Data(ServerTerm::Version {
                    max_message_size: 1_000_000,
                    protocol_version: b("relay0"),
                    lease_ms: 300_000,
                    orientations: vec![Orientation::Rows],
                }),
            )
            .unwrap();

            // Read Reset control
            let msg = read_client_message(&mut server_stream, &mut buf).unwrap();
            assert_eq!(msg, ClientMessage::Control(ControlOp::Reset));
            write_server_message(
                &mut server_stream,
                &ServerMessage::Control(ControlResult::Ok),
            )
            .unwrap();
        });

        let transport = SocketTransport::new(client_stream);
        let client = Client::new(transport);

        let mut session = match client
            .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
            .unwrap()
        {
            VersionResult::Accepted(s) => s,
            VersionResult::Rejected { .. } => panic!("expected Accepted"),
        };

        // Send Reset
        let result = session.reset().unwrap();
        assert_eq!(result, ControlResult::Ok);

        server.join().unwrap();
    }
}
