// Relay Protocol — Layer 0 Vocabulary
//
// Transcription of the logical model from protocol/SQL9-PROTOCOL-2.md.
// 14 top-level terms built from a small set of atomic and compound types.
//
// Typestate enforcement: Client<T> must call version() to obtain a
// Session<T> before any protocol operation. Handles are opaque types
// consumed by close(). Orientations require proof of agreement.

use serde::{Deserialize, Serialize};

// --- Atomic types ---

/// Opaque byte sequence. The protocol does not interpret contents.
pub type ByteSeq = Vec<u8>;

/// Column name.
pub type Name = ByteSeq;

/// Backend-specific type label, passed through opaquely.
pub type Descriptor = ByteSeq;

/// Server-assigned, client-opaque handle to an open result.
pub type Handle = ByteSeq;

/// Natural number (counts, positions, versions).
pub type Nat = u64;

// --- Compound types ---

/// A single column in a result: ordinal position, name, and backend type descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dimension {
    pub position: Nat,
    pub name: Name,
    pub descriptor: Descriptor,
}

// --- Sum types ---

/// Coarse error classification. Vendor detail lives in the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    Syntax,
    Constraint,
    Connection,
    Permission,
    Timeout,
}

/// Traversal orientation for fetch results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Orientation {
    Rows,
    Columns,
}

/// Reference to a column — by name or by ordinal index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnRef {
    ByName(Name),
    ByIndex(Nat),
}

/// Which columns to fetch: all, or a specific selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Projection {
    All,
    Select(Vec<ColumnRef>),
}

/// A single cell: absent (SQL NULL) or present (opaque bytes).
pub type Cell = Option<ByteSeq>;

/// Supplementary metadata about a result. Extensible at layer 1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaItem {
    Backend(Name, ByteSeq),
    ExecutionTime(Nat),
}

// --- Top-level terms (14 protocol messages, split by direction) ---

/// Terms the client sends. 7 variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientTerm {
    Version { max_message_size: Nat, protocol_version: ByteSeq, lease_ms: Nat, orientations: Vec<Orientation> },
    Query { text: ByteSeq },
    Fetch { handle: Handle, projection: Projection, count: Nat, orientation: Orientation },
    Stat { handle: Handle },
    Close { handle: Handle },
    Prepare { text: ByteSeq, dimensions: Vec<Dimension> },
    Offer { handle: Handle, cells: Vec<Vec<Cell>>, orientation: Orientation },
}

/// Terms the server sends. 7 variants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServerTerm {
    Version { max_message_size: Nat, protocol_version: ByteSeq, lease_ms: Nat, orientations: Vec<Orientation> },
    Header { handle: Handle, dimensions: Vec<Dimension> },
    Data { cells: Vec<Vec<Cell>> },
    Metadata { items: Vec<MetaItem> },
    End,
    Ok { count_hint: Nat },
    Error { kind: ErrorKind, identity: ByteSeq, message: ByteSeq },
}

// --- Opaque handle types (typestate enforcement) ---

/// Opaque handle to a query result. Created by `Session::query()`.
/// Consumed by `Session::close()`. Cannot be constructed outside this module.
#[derive(Debug, PartialEq, Eq)]
pub struct QueryHandle(Handle);

impl QueryHandle {
    /// Access the raw handle bytes (for forwarding in relay adapters).
    pub fn raw(&self) -> &Handle { &self.0 }
}

/// Opaque handle to a load target. Created by `Session::prepare()`.
/// Consumed by `Session::close()`. Cannot be constructed outside this module.
#[derive(Debug, PartialEq, Eq)]
pub struct LoadHandle(Handle);

impl LoadHandle {
    pub fn raw(&self) -> &Handle { &self.0 }
}

/// An orientation agreed during version negotiation.
/// Can only be obtained from `Session::agreed_orientation()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgreedOrientation(Orientation);

impl AgreedOrientation {
    pub fn orientation(&self) -> Orientation { self.0 }
}

/// Wrapper for close operations — accepts either QueryHandle or LoadHandle.
pub struct AnyHandle(Handle);

impl From<QueryHandle> for AnyHandle {
    fn from(h: QueryHandle) -> Self { AnyHandle(h.0) }
}

impl From<LoadHandle> for AnyHandle {
    fn from(h: LoadHandle) -> Self { AnyHandle(h.0) }
}

// --- Narrowed response types (one per script branch) ---

/// Response to Query: server opens a result or rejects.
#[derive(Debug, PartialEq, Eq)]
pub enum QueryResponse {
    Header { handle: QueryHandle, dimensions: Vec<Dimension> },
    Error { kind: ErrorKind, identity: ByteSeq, message: ByteSeq },
}

/// Response to Fetch: data, end, or error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchResponse {
    Data { cells: Vec<Vec<Cell>> },
    End,
    Error { kind: ErrorKind, identity: ByteSeq, message: ByteSeq },
}

/// Response to Stat: metadata or error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatResponse {
    Metadata { items: Vec<MetaItem> },
    Error { kind: ErrorKind, identity: ByteSeq, message: ByteSeq },
}

/// Response to Close: confirmed or error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseResponse {
    Ok,
    Error { kind: ErrorKind, identity: ByteSeq, message: ByteSeq },
}

/// Response to Prepare: server opens a load handle or rejects.
#[derive(Debug, PartialEq, Eq)]
pub enum PrepareResponse {
    Header { handle: LoadHandle, dimensions: Vec<Dimension> },
    Error { kind: ErrorKind, identity: ByteSeq, message: ByteSeq },
}

/// Response to Offer: accepted, done, or error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OfferResponse {
    Ok { count_hint: Nat },
    End,
    Error { kind: ErrorKind, identity: ByteSeq, message: ByteSeq },
}

// --- Transport trait ---

/// Abstraction over how terms move between client and server.
/// In-process, socket, pipe — same trait, different implementations.
pub trait Transport {
    fn exchange(&mut self, term: ClientTerm) -> Result<ServerTerm, TransportError>;
}

/// Transport-level failure (connection lost, IO error). Distinct from protocol Error terms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError {
    pub message: String,
}

// --- Typestate: Client (pre-version) → Session (post-version) ---

/// The result of a version handshake.
pub enum VersionResult<T: Transport> {
    /// Version agreed. Contains a ready-to-use Session.
    Accepted(Session<T>),
    /// Version rejected by the server.
    Rejected { kind: ErrorKind, message: ByteSeq },
}

impl<T: Transport> std::fmt::Debug for VersionResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionResult::Accepted(_) => write!(f, "VersionResult::Accepted(Session)"),
            VersionResult::Rejected { kind, message } => f
                .debug_struct("VersionResult::Rejected")
                .field("kind", kind)
                .field("message", &String::from_utf8_lossy(message))
                .finish(),
        }
    }
}

/// A relay protocol client before version negotiation.
/// The only available operation is `version()`, which consumes the client
/// and returns a `Session` on success.
pub struct Client<T: Transport> {
    transport: T,
}

impl<T: Transport> Client<T> {
    pub fn new(transport: T) -> Self {
        Client { transport }
    }

    /// Perform the version handshake. Consumes this Client.
    /// On success, returns `VersionResult::Accepted(Session)`.
    /// On rejection, returns `VersionResult::Rejected`.
    pub fn version(
        mut self,
        max_message_size: Nat,
        protocol_version: ByteSeq,
        lease_ms: Nat,
        orientations: Vec<Orientation>,
    ) -> Result<VersionResult<T>, TransportError> {
        let response = self.transport.exchange(ClientTerm::Version {
            max_message_size,
            protocol_version,
            lease_ms,
            orientations,
        })?;
        Ok(match response {
            ServerTerm::Version {
                max_message_size: _,
                protocol_version: _,
                lease_ms: _,
                orientations: agreed,
            } => VersionResult::Accepted(Session {
                transport: self.transport,
                agreed_orientations: agreed,
            }),
            ServerTerm::Error { kind, message, .. } => VersionResult::Rejected { kind, message },
            other => VersionResult::Rejected {
                kind: ErrorKind::Connection,
                message: format!("protocol violation: expected Version or Error, got {:?}", other)
                    .into_bytes(),
            },
        })
    }
}

/// A versioned protocol session. All protocol operations live here.
/// Created by `Client::version()` after successful version negotiation.
pub struct Session<T: Transport> {
    pub(crate) transport: T,
    agreed_orientations: Vec<Orientation>,
}

impl<T: Transport> Session<T> {
    /// Check whether a given orientation was agreed during version negotiation.
    /// Returns an `AgreedOrientation` token that can be passed to fetch/offer.
    pub fn agreed_orientation(&self, o: Orientation) -> Option<AgreedOrientation> {
        if self.agreed_orientations.contains(&o) {
            Some(AgreedOrientation(o))
        } else {
            None
        }
    }

    pub fn query(&mut self, text: ByteSeq) -> Result<QueryResponse, TransportError> {
        let response = self.transport.exchange(ClientTerm::Query { text })?;
        Ok(match response {
            ServerTerm::Header { handle, dimensions } => QueryResponse::Header {
                handle: QueryHandle(handle),
                dimensions,
            },
            ServerTerm::Error { kind, identity, message } => QueryResponse::Error { kind, identity, message },
            other => QueryResponse::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: format!("protocol violation: expected Header or Error, got {:?}", other)
                    .into_bytes(),
            },
        })
    }

    pub fn fetch(
        &mut self,
        handle: &QueryHandle,
        projection: Projection,
        count: Nat,
        orientation: AgreedOrientation,
    ) -> Result<FetchResponse, TransportError> {
        let response = self.transport.exchange(ClientTerm::Fetch {
            handle: handle.0.clone(),
            projection,
            count,
            orientation: orientation.0,
        })?;
        Ok(match response {
            ServerTerm::Data { cells } => FetchResponse::Data { cells },
            ServerTerm::End => FetchResponse::End,
            ServerTerm::Error { kind, identity, message } => FetchResponse::Error { kind, identity, message },
            other => FetchResponse::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: format!(
                    "protocol violation: expected Data, End, or Error, got {:?}",
                    other
                )
                .into_bytes(),
            },
        })
    }

    pub fn stat(&mut self, handle: &QueryHandle) -> Result<StatResponse, TransportError> {
        let response = self.transport.exchange(ClientTerm::Stat {
            handle: handle.0.clone(),
        })?;
        Ok(match response {
            ServerTerm::Metadata { items } => StatResponse::Metadata { items },
            ServerTerm::Error { kind, identity, message } => StatResponse::Error { kind, identity, message },
            other => StatResponse::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: format!(
                    "protocol violation: expected Metadata or Error, got {:?}",
                    other
                )
                .into_bytes(),
            },
        })
    }

    pub fn close(
        &mut self,
        handle: impl Into<AnyHandle>,
    ) -> Result<CloseResponse, TransportError> {
        let raw = handle.into().0;
        let response = self.transport.exchange(ClientTerm::Close { handle: raw })?;
        Ok(match response {
            ServerTerm::Ok { .. } => CloseResponse::Ok,
            ServerTerm::Error { kind, identity, message } => CloseResponse::Error { kind, identity, message },
            other => CloseResponse::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: format!(
                    "protocol violation: expected Ok or Error, got {:?}",
                    other
                )
                .into_bytes(),
            },
        })
    }

    pub fn prepare(
        &mut self,
        text: ByteSeq,
        dimensions: Vec<Dimension>,
    ) -> Result<PrepareResponse, TransportError> {
        let response = self
            .transport
            .exchange(ClientTerm::Prepare { text, dimensions })?;
        Ok(match response {
            ServerTerm::Header { handle, dimensions } => PrepareResponse::Header {
                handle: LoadHandle(handle),
                dimensions,
            },
            ServerTerm::Error { kind, identity, message } => PrepareResponse::Error { kind, identity, message },
            other => PrepareResponse::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: format!(
                    "protocol violation: expected Header or Error, got {:?}",
                    other
                )
                .into_bytes(),
            },
        })
    }

    pub fn offer(
        &mut self,
        handle: &LoadHandle,
        cells: Vec<Vec<Cell>>,
        orientation: AgreedOrientation,
    ) -> Result<OfferResponse, TransportError> {
        let response = self.transport.exchange(ClientTerm::Offer {
            handle: handle.0.clone(),
            cells,
            orientation: orientation.0,
        })?;
        Ok(match response {
            ServerTerm::Ok { count_hint } => OfferResponse::Ok { count_hint },
            ServerTerm::End => OfferResponse::End,
            ServerTerm::Error { kind, identity, message } => OfferResponse::Error { kind, identity, message },
            other => OfferResponse::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: format!(
                    "protocol violation: expected Ok, End, or Error, got {:?}",
                    other
                )
                .into_bytes(),
            },
        })
    }
}

// --- Handler trait ---

/// A protocol handler: receives a ClientTerm, returns a ServerTerm.
/// Any type that can serve protocol conversations implements this.
pub trait Handler {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm;
}

impl Handler for Box<dyn Handler> {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        (**self).handle(term)
    }
}

impl Handler for Box<dyn Handler + Send> {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        (**self).handle(term)
    }
}

// --- DirectTransport ---

/// Zero-cost in-process transport: delegates directly to a Handler.
pub struct DirectTransport<H: Handler> {
    handler: H,
}

impl<H: Handler> DirectTransport<H> {
    pub fn new(handler: H) -> Self {
        DirectTransport { handler }
    }
}

impl<H: Handler> Transport for DirectTransport<H> {
    fn exchange(&mut self, term: ClientTerm) -> Result<ServerTerm, TransportError> {
        Ok(self.handler.handle(term))
    }
}

// --- Projection resolution ---

/// Resolve a Projection to column indices (0-based) within the given column list.
pub fn resolve_projection(projection: &Projection, columns: &[String]) -> Vec<usize> {
    match projection {
        Projection::All => (0..columns.len()).collect(),
        Projection::Select(refs) => refs
            .iter()
            .filter_map(|r| match r {
                ColumnRef::ByName(name) => {
                    let name_str = String::from_utf8_lossy(name);
                    columns.iter().position(|c| c == name_str.as_ref())
                }
                ColumnRef::ByIndex(idx) => {
                    let i = (*idx as usize).checked_sub(1)?; // 1-based → 0-based
                    if i < columns.len() {
                        Some(i)
                    } else {
                        None
                    }
                }
            })
            .collect(),
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// A mock transport that replays a scripted sequence of server responses.
    struct MockTransport {
        responses: VecDeque<ServerTerm>,
        sent: Vec<ClientTerm>,
    }

    impl MockTransport {
        fn new(responses: Vec<ServerTerm>) -> Self {
            MockTransport {
                responses: responses.into(),
                sent: Vec::new(),
            }
        }
    }

    impl Transport for MockTransport {
        fn exchange(&mut self, term: ClientTerm) -> Result<ServerTerm, TransportError> {
            self.sent.push(term);
            self.responses.pop_front().ok_or(TransportError {
                message: "mock exhausted".into(),
            })
        }
    }

    // --- Helper constructors ---

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

    fn null_cell() -> Cell {
        None
    }

    fn version_ok() -> ServerTerm {
        ServerTerm::Version {
            max_message_size: 1_000_000,
            protocol_version: b("relay0"),
            lease_ms: 300_000,
            orientations: vec![Orientation::Rows, Orientation::Columns],
        }
    }

    /// Version handshake helper: consumes Client, returns Session.
    fn accept_version<T: Transport>(client: Client<T>) -> Session<T> {
        match client
            .version(
                1_000_000,
                b("relay0"),
                300_000,
                vec![Orientation::Rows, Orientation::Columns],
            )
            .unwrap()
        {
            VersionResult::Accepted(session) => session,
            VersionResult::Rejected { kind, message } => {
                panic!(
                    "version rejected: {:?}: {}",
                    kind,
                    String::from_utf8_lossy(&message)
                );
            }
        }
    }

    // --- Script: happy path (version, query, fetch, fetch, close) ---

    #[test]
    fn happy_path_select() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h1"),
                dimensions: vec![
                    dim(1, "name", "TEXT"),
                    dim(2, "age", "INTEGER"),
                ],
            },
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice"), cell("30")],
                    vec![cell("Bob"), cell("25")],
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        // Query
        let resp = session.query(b("SELECT name, age FROM users")).unwrap();
        let handle = match resp {
            QueryResponse::Header { handle, dimensions } => {
                assert_eq!(dimensions.len(), 2);
                assert_eq!(dimensions[0].name, b("name"));
                assert_eq!(dimensions[1].name, b("age"));
                handle
            }
            QueryResponse::Error { .. } => panic!("expected Header"),
        };

        // Fetch — get data
        let resp = session.fetch(&handle, Projection::All, 100, rows).unwrap();
        match resp {
            FetchResponse::Data { ref cells } => {
                assert_eq!(cells.len(), 2);
                assert_eq!(cells[0][0], cell("Alice"));
                assert_eq!(cells[1][1], cell("25"));
            }
            _ => panic!("expected Data"),
        }

        // Fetch — end
        let resp = session.fetch(&handle, Projection::All, 100, rows).unwrap();
        assert_eq!(resp, FetchResponse::End);

        // Close
        let resp = session.close(handle).unwrap();
        assert_eq!(resp, CloseResponse::Ok);
    }

    // --- Script: query error ---

    #[test]
    fn query_returns_error() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Error {
                kind: ErrorKind::Syntax,
                identity: vec![],
                message: b("parse error near FROM"),
            },
        ]);

        let mut session = accept_version(Client::new(mock));

        let resp = session.query(b("SELECTX * FROM users")).unwrap();
        assert_eq!(resp, QueryResponse::Error {
            kind: ErrorKind::Syntax,
            identity: vec![],
            message: b("parse error near FROM"),
        });
    }

    // --- Script: version rejected ---

    #[test]
    fn version_rejected() {
        let mock = MockTransport::new(vec![
            ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b("unsupported version"),
            },
        ]);

        let client = Client::new(mock);
        let result = client.version(1_000_000, b("future"), 300_000, vec![Orientation::Rows]).unwrap();
        match result {
            VersionResult::Rejected { kind, message } => {
                assert_eq!(kind, ErrorKind::Connection);
                assert_eq!(message, b("unsupported version"));
            }
            VersionResult::Accepted(_) => panic!("expected Rejected"),
        }
    }

    // --- Script: version — no common orientation ---

    #[test]
    fn version_no_common_orientation() {
        let mock = MockTransport::new(vec![
            ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b("no common orientation"),
            },
        ]);

        let client = Client::new(mock);
        let result = client.version(1_000_000, b("relay0"), 300_000, vec![Orientation::Columns]).unwrap();
        match result {
            VersionResult::Rejected { kind, message } => {
                assert_eq!(kind, ErrorKind::Connection);
                assert_eq!(message, b("no common orientation"));
            }
            VersionResult::Accepted(_) => panic!("expected Rejected"),
        }
    }

    // --- Script: DML returns relation (affected_rows) ---

    #[test]
    fn dml_returns_relation() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h2"),
                dimensions: vec![dim(1, "affected_rows", "integer")],
            },
            ServerTerm::Data {
                cells: vec![vec![cell("47")]],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let resp = session.query(b("UPDATE employees SET salary = salary * 1.1")).unwrap();
        let handle = match resp {
            QueryResponse::Header { handle, dimensions } => {
                assert_eq!(dimensions[0].name, b("affected_rows"));
                handle
            }
            _ => panic!("expected Header"),
        };

        let resp = session.fetch(&handle, Projection::All, 1, rows).unwrap();
        match resp {
            FetchResponse::Data { cells } => {
                assert_eq!(cells[0][0], cell("47"));
            }
            _ => panic!("expected Data"),
        }

        let resp = session.fetch(&handle, Projection::All, 1, rows).unwrap();
        assert_eq!(resp, FetchResponse::End);

        let resp = session.close(handle).unwrap();
        assert_eq!(resp, CloseResponse::Ok);
    }

    // --- Null cells ---

    #[test]
    fn null_cells_in_data() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h3"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "email", "TEXT")],
            },
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice"), null_cell()],
                    vec![cell("Bob"), cell("bob@example.com")],
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.query(b("SELECT name, email FROM users")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        let resp = session.fetch(&handle, Projection::All, 100, rows).unwrap();
        match resp {
            FetchResponse::Data { cells } => {
                assert_eq!(cells[0][1], null_cell());
                assert_eq!(cells[1][1], cell("bob@example.com"));
            }
            _ => panic!("expected Data"),
        }

        session.fetch(&handle, Projection::All, 100, rows).unwrap();
        session.close(handle).unwrap();
    }

    // --- Stat on open handle ---

    #[test]
    fn stat_returns_metadata() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h4"),
                dimensions: vec![dim(1, "id", "INTEGER")],
            },
            ServerTerm::Metadata {
                items: vec![
                    MetaItem::Backend(b("sqlite"), b("3.45.0")),
                    MetaItem::ExecutionTime(42),
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.query(b("SELECT id FROM t")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        let resp = session.stat(&handle).unwrap();
        match resp {
            StatResponse::Metadata { items } => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[1], MetaItem::ExecutionTime(42));
            }
            _ => panic!("expected Metadata"),
        }

        session.fetch(&handle, Projection::All, 100, rows).unwrap();
        session.close(handle).unwrap();
    }

    // --- Transport failure ---

    #[test]
    fn transport_failure() {
        let mock = MockTransport::new(vec![]); // empty — immediate exhaustion
        let client = Client::new(mock);

        let err = client.version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows]).unwrap_err();
        assert_eq!(err.message, "mock exhausted");
    }

    // --- Protocol violation: illegal response ---

    #[test]
    fn protocol_violation_on_close() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h5"),
                dimensions: vec![dim(1, "x", "INT")],
            },
            // Server sends Header in response to Close — illegal
            ServerTerm::Header {
                handle: b("what"),
                dimensions: vec![],
            },
        ]);

        let mut session = accept_version(Client::new(mock));

        let handle = match session.query(b("SELECT x FROM t")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        // Close gets a Header back — protocol violation, surfaced as Error
        let resp = session.close(handle).unwrap();
        match resp {
            CloseResponse::Error { kind, .. } => assert_eq!(kind, ErrorKind::Connection),
            _ => panic!("expected Error from protocol violation"),
        }
    }

    // --- Multiple concurrent handles ---

    #[test]
    fn multiple_handles() {
        let mock = MockTransport::new(vec![
            version_ok(),
            // First query
            ServerTerm::Header {
                handle: b("h1"),
                dimensions: vec![dim(1, "a", "INT")],
            },
            // Second query
            ServerTerm::Header {
                handle: b("h2"),
                dimensions: vec![dim(1, "b", "TEXT")],
            },
            // Fetch from h1
            ServerTerm::Data { cells: vec![vec![cell("1")]] },
            // Fetch from h2
            ServerTerm::Data { cells: vec![vec![cell("hello")]] },
            // End h1, End h2
            ServerTerm::End,
            ServerTerm::End,
            // Close both
            ServerTerm::Ok { count_hint: 0 },
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let h1 = match session.query(b("SELECT a FROM t1")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };
        let h2 = match session.query(b("SELECT b FROM t2")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        // Interleaved fetches on different handles
        match session.fetch(&h1, Projection::All, 10, rows).unwrap() {
            FetchResponse::Data { cells } => assert_eq!(cells[0][0], cell("1")),
            _ => panic!("expected Data"),
        }
        match session.fetch(&h2, Projection::All, 10, rows).unwrap() {
            FetchResponse::Data { cells } => assert_eq!(cells[0][0], cell("hello")),
            _ => panic!("expected Data"),
        }

        assert_eq!(session.fetch(&h1, Projection::All, 10, rows).unwrap(), FetchResponse::End);
        assert_eq!(session.fetch(&h2, Projection::All, 10, rows).unwrap(), FetchResponse::End);

        assert_eq!(session.close(h1).unwrap(), CloseResponse::Ok);
        assert_eq!(session.close(h2).unwrap(), CloseResponse::Ok);
    }

    // --- Column-oriented fetch ---

    #[test]
    fn fetch_column_oriented() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h6"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "age", "INT")],
            },
            // Column-oriented: outer = columns, inner = values in that column
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice"), cell("Bob")],   // name column
                    vec![cell("30"), cell("25")],        // age column
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let cols = session.agreed_orientation(Orientation::Columns).unwrap();

        let handle = match session.query(b("SELECT name, age FROM users")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        let resp = session.fetch(&handle, Projection::All, 100, cols).unwrap();
        match resp {
            FetchResponse::Data { cells } => {
                // Outer dimension is columns
                assert_eq!(cells[0], vec![cell("Alice"), cell("Bob")]);
                assert_eq!(cells[1], vec![cell("30"), cell("25")]);
            }
            _ => panic!("expected Data"),
        }

        session.fetch(&handle, Projection::All, 100, cols).unwrap();
        session.close(handle).unwrap();
    }

    // --- Projected fetch (specific columns) ---

    #[test]
    fn fetch_with_projection() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h7"),
                dimensions: vec![
                    dim(1, "id", "INT"),
                    dim(2, "name", "TEXT"),
                    dim(3, "email", "TEXT"),
                ],
            },
            // Only requested column "name"
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice")],
                    vec![cell("Bob")],
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.query(b("SELECT id, name, email FROM users")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        // Fetch only the "name" column
        let proj = Projection::Select(vec![ColumnRef::ByName(b("name"))]);
        let resp = session.fetch(&handle, proj, 100, rows).unwrap();
        match resp {
            FetchResponse::Data { cells } => {
                assert_eq!(cells.len(), 2);
                assert_eq!(cells[0][0], cell("Alice"));
            }
            _ => panic!("expected Data"),
        }

        session.fetch(&handle, Projection::All, 100, rows).unwrap();
        session.close(handle).unwrap();
    }

    // --- ManifestTransport (in-process round-trip through bytes) ---

    /// A transport that routes every exchange through the full manifestation path
    /// (serialize → frame → unframe → deserialize) in both directions.
    struct ManifestTransport<F: FnMut(ClientTerm) -> ServerTerm> {
        handler: F,
    }

    impl<F: FnMut(ClientTerm) -> ServerTerm> Transport for ManifestTransport<F> {
        fn exchange(&mut self, term: ClientTerm) -> Result<ServerTerm, TransportError> {
            // Client side: term → msgpack → framed bytes
            let client_bytes = crate::manifest::frame_client(&term)?;

            // Server side: framed bytes → msgpack → term
            let (payload, _) = crate::manifest::read_frame(&client_bytes)?
                .ok_or(TransportError { message: "incomplete frame".into() })?;
            let received = crate::manifest::decode_client(payload)?;

            // Mock server logic
            let response = (self.handler)(received);

            // Server side: term → msgpack → framed bytes
            let server_bytes = crate::manifest::frame_server(&response)?;

            // Client side: framed bytes → msgpack → term
            let (payload, _) = crate::manifest::read_frame(&server_bytes)?
                .ok_or(TransportError { message: "incomplete frame".into() })?;
            crate::manifest::decode_server(payload)
        }
    }

    fn manifest_transport(responses: Vec<ServerTerm>) -> ManifestTransport<impl FnMut(ClientTerm) -> ServerTerm> {
        let mut queue = VecDeque::from(responses);
        ManifestTransport {
            handler: move |_| queue.pop_front().expect("manifest mock exhausted"),
        }
    }

    #[test]
    fn happy_path_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h1"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "age", "INTEGER")],
            },
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice"), cell("30")],
                    vec![cell("Bob"), cell("25")],
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(transport));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.query(b("SELECT name, age FROM users")).unwrap() {
            QueryResponse::Header { handle, dimensions } => {
                assert_eq!(dimensions.len(), 2);
                assert_eq!(dimensions[0].name, b("name"));
                assert_eq!(dimensions[1].name, b("age"));
                handle
            }
            QueryResponse::Error { .. } => panic!("expected Header"),
        };

        match session.fetch(&handle, Projection::All, 100, rows).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(cells.len(), 2);
                assert_eq!(cells[0][0], cell("Alice"));
                assert_eq!(cells[1][1], cell("25"));
            }
            _ => panic!("expected Data"),
        }

        assert_eq!(session.fetch(&handle, Projection::All, 100, rows).unwrap(), FetchResponse::End);
        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn query_error_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Error { kind: ErrorKind::Syntax, identity: vec![], message: b("parse error near FROM") },
        ]);

        let mut session = accept_version(Client::new(transport));

        let resp = session.query(b("SELECTX * FROM users")).unwrap();
        assert_eq!(resp, QueryResponse::Error {
            kind: ErrorKind::Syntax,
            identity: vec![],
            message: b("parse error near FROM"),
        });
    }

    #[test]
    fn version_rejected_via_manifest() {
        let transport = manifest_transport(vec![
            ServerTerm::Error { kind: ErrorKind::Connection, identity: vec![], message: b("unsupported version") },
        ]);

        let client = Client::new(transport);
        let result = client.version(1_000_000, b("future"), 300_000, vec![Orientation::Rows]).unwrap();
        match result {
            VersionResult::Rejected { kind, message } => {
                assert_eq!(kind, ErrorKind::Connection);
                assert_eq!(message, b("unsupported version"));
            }
            VersionResult::Accepted(_) => panic!("expected Rejected"),
        }
    }

    #[test]
    fn null_cells_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h3"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "email", "TEXT")],
            },
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice"), null_cell()],
                    vec![cell("Bob"), cell("bob@example.com")],
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(transport));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.query(b("SELECT name, email FROM users")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        match session.fetch(&handle, Projection::All, 100, rows).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(cells[0][1], null_cell());
                assert_eq!(cells[1][1], cell("bob@example.com"));
            }
            _ => panic!("expected Data"),
        }

        assert_eq!(session.fetch(&handle, Projection::All, 100, rows).unwrap(), FetchResponse::End);
        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn projection_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h7"),
                dimensions: vec![dim(1, "id", "INT"), dim(2, "name", "TEXT"), dim(3, "email", "TEXT")],
            },
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice")],
                    vec![cell("Bob")],
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(transport));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.query(b("SELECT id, name, email FROM users")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        let proj = Projection::Select(vec![ColumnRef::ByName(b("name"))]);
        match session.fetch(&handle, proj, 100, rows).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(cells.len(), 2);
                assert_eq!(cells[0][0], cell("Alice"));
            }
            _ => panic!("expected Data"),
        }

        assert_eq!(session.fetch(&handle, Projection::All, 100, rows).unwrap(), FetchResponse::End);
        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn column_oriented_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("h6"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "age", "INT")],
            },
            ServerTerm::Data {
                cells: vec![
                    vec![cell("Alice"), cell("Bob")],   // name column
                    vec![cell("30"), cell("25")],        // age column
                ],
            },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(transport));
        let cols = session.agreed_orientation(Orientation::Columns).unwrap();

        let handle = match session.query(b("SELECT name, age FROM users")).unwrap() {
            QueryResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        match session.fetch(&handle, Projection::All, 100, cols).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(cells[0], vec![cell("Alice"), cell("Bob")]);
                assert_eq!(cells[1], vec![cell("30"), cell("25")]);
            }
            _ => panic!("expected Data"),
        }

        assert_eq!(session.fetch(&handle, Projection::All, 100, cols).unwrap(), FetchResponse::End);
        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    // --- Load path (Prepare / Offer) — MockTransport ---

    #[test]
    fn load_happy_path() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L1"),
                dimensions: vec![dim(1, "name", "varchar"), dim(2, "email", "varchar")],
            },
            ServerTerm::Ok { count_hint: 1000 },
            ServerTerm::Ok { count_hint: 1000 },
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.prepare(b("INSERT INTO users (name, email)"), vec![dim(1, "name", "varchar"), dim(2, "email", "varchar")]).unwrap() {
            PrepareResponse::Header { handle, dimensions } => {
                assert_eq!(dimensions.len(), 2);
                assert_eq!(dimensions[0].name, b("name"));
                handle
            }
            PrepareResponse::Error { .. } => panic!("expected Header"),
        };

        let resp = session.offer(&handle, vec![
            vec![cell("alice"), cell("alice@example.com")],
            vec![cell("bob"), cell("bob@example.com")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 1000 });

        let resp = session.offer(&handle, vec![
            vec![cell("carol"), cell("carol@example.com")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 1000 });

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn load_server_stop() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L2"),
                dimensions: vec![dim(1, "id", "integer"), dim(2, "value", "float")],
            },
            ServerTerm::Ok { count_hint: 500 },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.prepare(b("COPY INTO staging"), vec![dim(1, "id", "integer"), dim(2, "value", "float")]).unwrap() {
            PrepareResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        let resp = session.offer(&handle, vec![
            vec![cell("1"), cell("3.14")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 500 });

        let resp = session.offer(&handle, vec![
            vec![cell("2"), cell("2.72")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::End);

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn prepare_error() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b("INSERT not allowed on read-only replica"),
            },
        ]);

        let mut session = accept_version(Client::new(mock));

        let resp = session.prepare(b("INSERT INTO users (name)"), vec![dim(1, "name", "varchar")]).unwrap();
        assert_eq!(resp, PrepareResponse::Error {
            kind: ErrorKind::Permission,
            identity: vec![],
            message: b("INSERT not allowed on read-only replica"),
        });
    }

    #[test]
    fn offer_error() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L3"),
                dimensions: vec![dim(1, "id", "integer")],
            },
            ServerTerm::Error {
                kind: ErrorKind::Constraint,
                identity: vec![],
                message: b("UNIQUE constraint failed: users.id"),
            },
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.prepare(b("INSERT INTO users (id)"), vec![dim(1, "id", "integer")]).unwrap() {
            PrepareResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        let resp = session.offer(&handle, vec![
            vec![cell("1")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Error {
            kind: ErrorKind::Constraint,
            identity: vec![],
            message: b("UNIQUE constraint failed: users.id"),
        });

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn load_null_cells() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L4"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "email", "TEXT")],
            },
            ServerTerm::Ok { count_hint: 100 },
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.prepare(b("INSERT INTO users (name, email)"), vec![dim(1, "name", "TEXT"), dim(2, "email", "TEXT")]).unwrap() {
            PrepareResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        // Offer rows with null cells — nulls must survive the protocol
        let resp = session.offer(&handle, vec![
            vec![cell("alice"), null_cell()],
            vec![cell("bob"), cell("bob@example.com")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 100 });

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn load_column_oriented() {
        let mock = MockTransport::new(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L5"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "age", "INT")],
            },
            ServerTerm::Ok { count_hint: 500 },
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(mock));
        let cols = session.agreed_orientation(Orientation::Columns).unwrap();

        let handle = match session.prepare(b("INSERT INTO users (name, age)"), vec![dim(1, "name", "TEXT"), dim(2, "age", "INT")]).unwrap() {
            PrepareResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        // Column-oriented: outer = columns, inner = values
        let resp = session.offer(&handle, vec![
            vec![cell("alice"), cell("bob")],   // name column
            vec![cell("30"), cell("25")],        // age column
        ], cols).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 500 });

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    // --- Load path — ManifestTransport ---

    #[test]
    fn load_happy_path_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L1"),
                dimensions: vec![dim(1, "name", "varchar"), dim(2, "email", "varchar")],
            },
            ServerTerm::Ok { count_hint: 1000 },
            ServerTerm::Ok { count_hint: 1000 },
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(transport));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.prepare(b("INSERT INTO users (name, email)"), vec![dim(1, "name", "varchar"), dim(2, "email", "varchar")]).unwrap() {
            PrepareResponse::Header { handle, dimensions } => {
                assert_eq!(dimensions.len(), 2);
                handle
            }
            PrepareResponse::Error { .. } => panic!("expected Header"),
        };

        let resp = session.offer(&handle, vec![
            vec![cell("alice"), cell("alice@example.com")],
            vec![cell("bob"), cell("bob@example.com")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 1000 });

        let resp = session.offer(&handle, vec![
            vec![cell("carol"), cell("carol@example.com")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 1000 });

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn load_server_stop_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L2"),
                dimensions: vec![dim(1, "id", "integer"), dim(2, "value", "float")],
            },
            ServerTerm::Ok { count_hint: 500 },
            ServerTerm::End,
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(transport));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.prepare(b("COPY INTO staging"), vec![dim(1, "id", "integer"), dim(2, "value", "float")]).unwrap() {
            PrepareResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        assert_eq!(
            session.offer(&handle, vec![vec![cell("1"), cell("3.14")]], rows).unwrap(),
            OfferResponse::Ok { count_hint: 500 }
        );
        assert_eq!(
            session.offer(&handle, vec![vec![cell("2"), cell("2.72")]], rows).unwrap(),
            OfferResponse::End
        );

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }

    #[test]
    fn load_null_cells_via_manifest() {
        let transport = manifest_transport(vec![
            version_ok(),
            ServerTerm::Header {
                handle: b("L4"),
                dimensions: vec![dim(1, "name", "TEXT"), dim(2, "email", "TEXT")],
            },
            ServerTerm::Ok { count_hint: 100 },
            ServerTerm::Ok { count_hint: 0 },
        ]);

        let mut session = accept_version(Client::new(transport));
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();

        let handle = match session.prepare(b("INSERT INTO users (name, email)"), vec![dim(1, "name", "TEXT"), dim(2, "email", "TEXT")]).unwrap() {
            PrepareResponse::Header { handle, .. } => handle,
            _ => panic!("expected Header"),
        };

        let resp = session.offer(&handle, vec![
            vec![cell("alice"), null_cell()],
            vec![cell("bob"), cell("bob@example.com")],
        ], rows).unwrap();
        assert_eq!(resp, OfferResponse::Ok { count_hint: 100 });

        assert_eq!(session.close(handle).unwrap(), CloseResponse::Ok);
    }
}
