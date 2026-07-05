// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// Relay Protocol — stdio Transport
//
// Implements the Transport trait over a child process's stdin/stdout
// (the LSP model). The framing is identical to the socket transport;
// only the byte channel differs — write to the child's stdin, read from
// its stdout. Unlike a named socket, this needs no rendezvous path and
// no orphan-reaping machinery: the lifecycle is the pipe's. When the
// parent closes its end the child sees EOF; when this transport drops it
// kills and reaps the child, so an abandoned fatboy cannot outlive its
// spawner. Portable across Linux, macOS, and Windows with no per-OS code
// (no PR_SET_PDEATHSIG, no Unix-socket namespace).

use std::io::{Read, Write};
use std::process::Child;

use crate::layer0::{ClientTerm, ServerTerm, Transport, TransportError};
use crate::layer1::{ClientMessage, ServerMessage};
use crate::manifest;

/// Transport implementation over a pair of pipes (writer = peer stdin,
/// reader = peer stdout). Optionally owns the child process, killing and
/// reaping it on drop.
pub struct StdioTransport {
    writer: Box<dyn Write + Send>,
    reader: Box<dyn Read + Send>,
    /// Present when this transport spawned the peer; reaped on drop.
    child: Option<Child>,
    buf: Vec<u8>,
}

impl StdioTransport {
    /// Build a transport over arbitrary pipes (no owned process — for
    /// in-memory tests or externally managed peers).
    pub fn new(
        writer: impl Write + Send + 'static,
        reader: impl Read + Send + 'static,
    ) -> Self {
        StdioTransport {
            writer: Box::new(writer),
            reader: Box::new(reader),
            child: None,
            buf: Vec::new(),
        }
    }

    /// Take ownership of a spawned child's stdin/stdout. The child must
    /// have been spawned with both `Stdio::piped()`. Killed and reaped
    /// when this transport drops.
    pub fn from_child(mut child: Child) -> Result<Self, TransportError> {
        let writer = child.stdin.take().ok_or_else(|| TransportError {
            message: "fatboy child has no piped stdin".into(),
        })?;
        let reader = child.stdout.take().ok_or_else(|| TransportError {
            message: "fatboy child has no piped stdout".into(),
        })?;
        Ok(StdioTransport {
            writer: Box::new(writer),
            reader: Box::new(reader),
            child: Some(child),
            buf: Vec::new(),
        })
    }
}

impl Drop for StdioTransport {
    fn drop(&mut self) {
        // Deterministic teardown: no orphan can linger. (Closing stdin
        // would suffice via EOF, but kill+wait is race-free and we have
        // already drained any results we wanted.)
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Transport for StdioTransport {
    fn exchange(&mut self, term: ClientTerm) -> Result<ServerTerm, TransportError> {
        let msg = ClientMessage::Data(term);
        let frame = manifest::frame_client_message(&msg)?;
        self.writer.write_all(&frame).map_err(|e| TransportError {
            message: format!("stdio write error: {}", e),
        })?;
        // Pipes are buffered; without a flush the peer never sees the
        // request and both sides block forever.
        self.writer.flush().map_err(|e| TransportError {
            message: format!("stdio flush error: {}", e),
        })?;

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
                    let mut tmp = [0u8; 8192];
                    let n = self.reader.read(&mut tmp).map_err(|e| TransportError {
                        message: format!("stdio read error: {}", e),
                    })?;
                    if n == 0 {
                        return Err(TransportError {
                            message: "fatboy closed its stdout".into(),
                        });
                    }
                    self.buf.extend_from_slice(&tmp[..n]);
                }
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::layer0::*;

    fn b(s: &str) -> ByteSeq {
        s.as_bytes().to_vec()
    }

    /// Drive a StdioTransport client against an in-thread server, using a
    /// socketpair as the two pipes — proving the framing is byte-channel
    /// agnostic without spawning a process. (StdioTransport takes separate
    /// writer/reader handles; here both are clones of one duplex fd.)
    #[test]
    fn stdio_transport_round_trip() {
        use std::os::unix::net::UnixStream;

        let (client_end, mut server) = UnixStream::pair().unwrap();
        let client_writer = client_end.try_clone().unwrap();

        let server = std::thread::spawn(move || {
            let mut buf = Vec::new();
            let responses = vec![
                ServerMessage::Data(ServerTerm::Version {
                    max_message_size: 1_000_000,
                    protocol_version: b("relay0"),
                    lease_ms: 0,
                    orientations: vec![Orientation::Rows],
                }),
                ServerMessage::Data(ServerTerm::Header {
                    handle: b("h1"),
                    dimensions: vec![Dimension {
                        position: 1,
                        name: b("name"),
                        descriptor: b("TEXT"),
                    }],
                }),
                ServerMessage::Data(ServerTerm::Data {
                    cells: vec![vec![Some(b("Alice"))]],
                }),
                ServerMessage::Data(ServerTerm::End),
            ];
            for resp in &responses {
                let _ = crate::socket::read_client_message(&mut server, &mut buf).unwrap();
                crate::socket::write_server_message(&mut server, resp).unwrap();
            }
        });

        let transport = StdioTransport::new(client_writer, client_end);
        let client = Client::new(transport);
        let mut session = match client
            .version(1_000_000, b("relay0"), 0, vec![Orientation::Rows])
            .unwrap()
        {
            VersionResult::Accepted(s) => s,
            VersionResult::Rejected { .. } => panic!("expected Accepted"),
        };
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();
        let handle = match session.query(b("SELECT name FROM t")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            QueryResponse::Error { .. } => panic!("expected Header"),
        };
        match session.fetch(&handle, Projection::All, 100, rows).unwrap() {
            FetchResponse::Data { cells } => assert_eq!(cells[0][0], Some(b("Alice"))),
            _ => panic!("expected Data"),
        }
        assert_eq!(
            session.fetch(&handle, Projection::All, 100, rows).unwrap(),
            FetchResponse::End
        );
        drop(session);
        server.join().unwrap();
    }
}
