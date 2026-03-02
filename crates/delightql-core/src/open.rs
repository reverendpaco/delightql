//! Entry points for creating DQL sessions.
//!
//! `open()` is the sole public entry point for external crates.
//! It returns `Box<dyn DqlHandle>` — the compiler-enforced API boundary.
//! External crates interact with DQL exclusively through the
//! `DqlHandle`, `DqlSession`, and `ServerRelay` traits defined in `api.rs`.

use delightql_protocol::{
    Client, DirectTransport, FetchResponse, Handler, Orientation, Projection, QueryResponse,
    Session, VersionResult,
};

use crate::api::{self, ColumnInfo, ConnectionFactory, FetchResult, QueryResult};
use crate::relay::RelayParty;
use crate::system::DelightQLSystem;

// Type alias for the backend session type (erased handler)
type BackendSession = Session<DirectTransport<Box<dyn Handler + Send>>>;

// Type alias for the full relay session type
type RelaySession<'a> =
    Session<DirectTransport<RelayParty<'a, DirectTransport<Box<dyn Handler + Send>>>>>;

/// Concrete handle implementation. Not visible outside this crate.
pub(crate) struct DqlHandleImpl {
    system: Box<DelightQLSystem>,
    /// Creates new handlers wrapping the SAME user connection.
    /// After mount! does ATTACH, all handlers see the attached databases.
    handler_factory: Box<dyn Fn() -> Box<dyn Handler + Send> + Send + Sync>,
    /// Taken on first session/relay creation, then recreated via handler_factory.
    initial_backend: Option<Box<dyn Handler + Send>>,
}

/// Concrete session implementation. Not visible outside this crate.
pub(crate) struct DqlSessionImpl<'a> {
    session: RelaySession<'a>,
}

impl<'a> api::DqlSession for DqlSessionImpl<'a> {
    fn query(&mut self, text: &str) -> Result<QueryResult, String> {
        let resp = self
            .session
            .query(text.as_bytes().to_vec())
            .map_err(|e| e.message)?;

        match resp {
            QueryResponse::Header { handle, dimensions } => {
                let columns: Vec<ColumnInfo> = dimensions
                    .iter()
                    .enumerate()
                    .map(|(i, d)| ColumnInfo {
                        name: String::from_utf8_lossy(&d.name).to_string(),
                        descriptor: String::from_utf8_lossy(&d.descriptor).to_string(),
                        position: i,
                    })
                    .collect();
                Ok(QueryResult { handle, columns })
            }
            QueryResponse::Error {
                kind,
                identity,
                message,
            } => {
                let id = String::from_utf8_lossy(&identity);
                let msg = String::from_utf8_lossy(&message);
                if identity.is_empty() {
                    Err(format!("{:?}: {}", kind, msg))
                } else {
                    Err(format!("[{}] {:?}: {}", id, kind, msg))
                }
            }
        }
    }

    fn fetch(
        &mut self,
        handle: &delightql_protocol::QueryHandle,
        count: u64,
    ) -> Result<FetchResult, String> {
        let agreed = self
            .session
            .agreed_orientation(Orientation::Rows)
            .ok_or_else(|| "Rows orientation not agreed".to_string())?;
        let resp = self
            .session
            .fetch(handle, Projection::All, count, agreed)
            .map_err(|e| e.message)?;

        match resp {
            FetchResponse::Data { cells } => Ok(FetchResult {
                rows: cells,
                finished: false,
            }),
            FetchResponse::End => Ok(FetchResult {
                rows: vec![],
                finished: true,
            }),
            FetchResponse::Error {
                kind,
                identity,
                message,
            } => {
                let id = String::from_utf8_lossy(&identity);
                let msg = String::from_utf8_lossy(&message);
                if identity.is_empty() {
                    Err(format!("{:?}: {}", kind, msg))
                } else {
                    Err(format!("[{}] {:?}: {}", id, kind, msg))
                }
            }
        }
    }

    fn close(&mut self, handle: delightql_protocol::QueryHandle) -> Result<(), String> {
        self.session.close(handle).map_err(|e| e.message)?;
        Ok(())
    }
}

// ── Helper: create a backend session from a Handler ────────────

fn make_backend_session(backend: Box<dyn Handler + Send>) -> Result<BackendSession, String> {
    let transport = DirectTransport::new(backend);
    let client = Client::new(transport);
    match client
        .version(
            1_000_000,
            b"relay0".to_vec(),
            300_000,
            vec![Orientation::Rows],
        )
        .map_err(|e| format!("Backend version handshake failed: {}", e.message))?
    {
        VersionResult::Accepted(s) => Ok(s),
        VersionResult::Rejected { kind, message } => Err(format!(
            "Backend rejected ({:?}): {}",
            kind,
            String::from_utf8_lossy(&message)
        )),
    }
}

// ── DqlHandle trait implementation ──────────────────────────────

impl api::DqlHandle for DqlHandleImpl {
    fn session(&mut self) -> Result<Box<dyn api::DqlSession + '_>, String> {
        // Take the stored backend, or recreate from the SAME connection.
        // Using handler_factory (not factory.create) ensures the handler wraps
        // the same connection where mount! did ATTACH.
        let backend = match self.initial_backend.take() {
            Some(b) => b,
            None => (self.handler_factory)(),
        };

        let backend_session = make_backend_session(backend)?;

        let relay = RelayParty::new(&mut self.system, backend_session);
        let transport = DirectTransport::new(relay);
        let client = Client::new(transport);

        match client
            .version(
                1_000_000,
                b"relay0".to_vec(),
                300_000,
                vec![Orientation::Rows],
            )
            .map_err(|e| format!("Relay version handshake failed: {}", e.message))?
        {
            VersionResult::Accepted(session) => Ok(Box::new(DqlSessionImpl { session })),
            VersionResult::Rejected { kind, message } => Err(format!(
                "Relay version rejected ({:?}): {}",
                kind,
                String::from_utf8_lossy(&message)
            )),
        }
    }

    fn create_relay(&mut self) -> Result<Box<dyn api::ServerRelay + '_>, String> {
        // Take the stored backend, or recreate from the SAME connection.
        let backend = match self.initial_backend.take() {
            Some(b) => b,
            None => (self.handler_factory)(),
        };

        let backend_session = make_backend_session(backend)?;
        let relay = RelayParty::new(&mut self.system, backend_session);
        Ok(Box::new(relay))
    }
}

impl DqlHandleImpl {
    /// Get mutable access to the underlying system (crate-internal only).
    #[allow(dead_code)]
    pub(crate) fn system_mut(&mut self) -> &mut DelightQLSystem {
        &mut self.system
    }

    /// Get shared access to the underlying system (crate-internal only).
    #[allow(dead_code)]
    pub(crate) fn system(&self) -> &DelightQLSystem {
        &self.system
    }
}

// ── Public entry point ────────────────────────────────────────

/// Create a DqlHandle from a connection factory.
///
/// Flow:
/// 1. `factory.create(":memory:")` → initial connection + handler
/// 2. Create bootstrap (:memory: SQLite, independent of user DB)
/// 3. Register bootstrap (id=1) and user (id=2) connections
/// 4. Create empty "main" namespace — no user introspection
/// 5. The CLI sends `mount!("path", "main")` as its first query to populate "main"
pub fn open(factory: Box<dyn ConnectionFactory>) -> Result<Box<dyn api::DqlHandle>, String> {
    let created = factory
        .create(":memory:")
        .map_err(|e| format!("Failed to create initial connection: {}", e))?;

    let system = DelightQLSystem::new(created.connection, created.introspector, &created.db_type)
        .map_err(|e| format!("{}", e))?;

    Ok(Box::new(DqlHandleImpl {
        system: Box::new(system),
        handler_factory: created.handler_factory,
        initial_backend: Some(created.handler),
    }))
}
