// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

use crate::api::{self, ApiError, ColumnInfo, ConnectionFactory, FetchResult, QueryResult};
use crate::relay::RelayParty;
use crate::system::{DelightQLSystem, ReadySystem};

// Type alias for the backend session type (erased handler)
type BackendSession = Session<DirectTransport<Box<dyn Handler + Send>>>;

// Type alias for the full relay session type
type RelaySession<'a> =
    Session<DirectTransport<RelayParty<'a, DirectTransport<Box<dyn Handler + Send>>>>>;

/// Concrete handle implementation. Not visible outside this crate.
pub(crate) struct DqlHandleImpl {
    system: Box<ReadySystem>,
    /// Creates new handlers wrapping the SAME user connection.
    /// After mount! does ATTACH, all handlers see the attached databases.
    handler_factory: Box<dyn Fn() -> Box<dyn Handler + Send> + Send + Sync>,
    /// Taken on first session/relay creation, then recreated via handler_factory.
    initial_backend: Option<Box<dyn Handler + Send>>,
    /// Session-baseline danger overrides (CLI --danger), inherited by every
    /// session/relay created from this handle. Pre-validated by
    /// danger_gates::parse_cli_danger_spec.
    danger_overrides: Vec<crate::pipeline::ast_unresolved::DangerSpec>,
}

/// Concrete session implementation. Not visible outside this crate.
pub(crate) struct DqlSessionImpl<'a> {
    session: RelaySession<'a>,
}

impl<'a> api::DqlSession for DqlSessionImpl<'a> {
    fn query(&mut self, text: &str) -> Result<QueryResult, ApiError> {
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
                        naming: d.naming,
                    })
                    .collect();
                Ok(QueryResult { handle, columns })
            }
            QueryResponse::Error(error) => Err(ApiError::received(&error)),
        }
    }

    fn fetch(
        &mut self,
        handle: &delightql_protocol::QueryHandle,
        count: u64,
    ) -> Result<FetchResult, ApiError> {
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
            FetchResponse::Error(error) => Err(ApiError::received(&error)),
        }
    }

    fn close(&mut self, handle: delightql_protocol::QueryHandle) -> Result<(), ApiError> {
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
            delightql_protocol::PROTOCOL_VERSION.to_vec(),
            300_000,
            vec![Orientation::Rows],
        )
        .map_err(|e| format!("Backend version handshake failed: {}", e.message))?
    {
        VersionResult::Accepted(s) => Ok(s),
        // The peer's statement, presented as received — identity kept.
        VersionResult::Rejected(error) => Err(ApiError::received(&error).to_string()),
    }
}

// ── DqlHandle trait implementation ──────────────────────────────

impl api::DqlHandle for DqlHandleImpl {
    fn session(&mut self) -> Result<Box<dyn api::DqlSession + '_>, String> {
        self.session_with_hooks(api::SessionHooks::default())
    }

    fn session_with_hooks(
        &mut self,
        hooks: api::SessionHooks,
    ) -> Result<Box<dyn api::DqlSession + '_>, String> {
        self.open_session(crate::compiler_limits::Admission::Execute, hooks)
    }

    fn observation_session(&mut self) -> Result<Box<dyn api::DqlSession + '_>, String> {
        self.open_session(
            crate::compiler_limits::Admission::Observe,
            api::SessionHooks::default(),
        )
    }

    fn create_relay(&mut self) -> Result<Box<dyn api::ServerRelay + '_>, String> {
        // Take the stored backend, or recreate from the SAME connection.
        let backend = match self.initial_backend.take() {
            Some(b) => b,
            None => (self.handler_factory)(),
        };

        let backend_session = make_backend_session(backend)?;
        // A session starts from the host's boot values.
        self.system
            .clear_session_settings()
            .map_err(|e| e.to_string())?;
        let mut relay = RelayParty::new(&mut self.system, backend_session);
        relay.set_danger_overrides(self.danger_overrides.clone());
        Ok(Box::new(relay))
    }

    fn selftest(&self) -> Vec<crate::diagnostics::DiagnosticFinding> {
        crate::diagnostics::run_selftest(self.system())
    }

    fn bind_static_bytes(&mut self, name: &str, bytes: &'static [u8]) -> Result<(), String> {
        self.system
            .bind_static_bytes(name, bytes)
            .map_err(|e| e.to_string())
    }

    fn set_danger_overrides(&mut self, specs: &[String]) -> Result<(), String> {
        // Parsing lives HERE, not in the host: the textual spec is the
        // API surface; the compiler's DangerSpec never crosses it.
        self.danger_overrides = specs
            .iter()
            .map(|s| crate::pipeline::danger_gates::parse_cli_danger_spec(s))
            .collect::<crate::error::Result<Vec<_>>>()
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    fn bind_owned_bytes(&mut self, name: &str, bytes: Vec<u8>) -> Result<(), String> {
        self.system
            .bind_owned_bytes(name, bytes)
            .map_err(|e| e.to_string())
    }

    fn session_health(&self) -> api::SessionHealthReport {
        match self.system.health_incident() {
            None => api::SessionHealthReport::Healthy,
            Some((operation, message)) => api::SessionHealthReport::Quarantined {
                operation: operation.to_string(),
                message: message.to_string(),
            },
        }
    }

    fn recover_session(&mut self) -> Result<api::SessionRecovery, String> {
        // The one reset authority: retries pending compensation first and
        // clears the quarantine latch only after the fresh world is installed
        // — the same road the protocol Reset control takes.
        self.system.reinit_bootstrap().map_err(|e| e.to_string())?;
        Ok(api::SessionRecovery {
            rebuilt: "the session catalog: a fresh instance of the pristine world \
                      (bootstrap metadata, system namespaces, seed facts), and \
                      the health latch (cleared)"
                .to_string(),
            lost: "session-local state: mounts, consulted definitions, enlisted \
                   namespaces, temporary objects, and the session's \
                   assertions/danger/errors ledgers"
                .to_string(),
            retained: "the connected database itself — its tables and data are \
                       untouched"
                .to_string(),
        })
    }
}

impl DqlHandleImpl {
    /// The one session constructor: a relay over the handle's system and a
    /// fresh backend handler, under the admission the caller's road is
    /// entitled to. Every session of either kind is made here, so the
    /// observing kind cannot be assembled with an executing relay.
    fn open_session(
        &mut self,
        admission: crate::compiler_limits::Admission,
        hooks: api::SessionHooks,
    ) -> Result<Box<dyn api::DqlSession + '_>, String> {
        // Take the stored backend, or recreate from the SAME connection.
        // Using handler_factory (not factory.create) ensures the handler wraps
        // the same connection where mount! did ATTACH.
        let backend = match self.initial_backend.take() {
            Some(b) => b,
            None => (self.handler_factory)(),
        };

        let backend_session = make_backend_session(backend)?;

        // A session starts from the host's boot values.
        self.system
            .clear_session_settings()
            .map_err(|e| e.to_string())?;
        let mut relay = match admission {
            crate::compiler_limits::Admission::Execute => {
                RelayParty::new(&mut self.system, backend_session)
            }
            crate::compiler_limits::Admission::Observe => {
                RelayParty::observing(&mut self.system, backend_session)
            }
        };
        relay.set_danger_overrides(self.danger_overrides.clone());
        if hooks.on_ship.is_some() {
            relay.set_hooks(crate::relay::RelayHooks {
                on_ship: hooks.on_ship,
                ..Default::default()
            });
        }
        let transport = DirectTransport::new(relay);
        let client = Client::new(transport);

        match client
            .version(
                1_000_000,
                delightql_protocol::PROTOCOL_VERSION.to_vec(),
                300_000,
                vec![Orientation::Rows],
            )
            .map_err(|e| format!("Relay version handshake failed: {}", e.message))?
        {
            VersionResult::Accepted(session) => Ok(Box::new(DqlSessionImpl { session })),
            VersionResult::Rejected(error) => Err(ApiError::received(&error).to_string()),
        }
    }

    /// Get shared access to the underlying system (crate-internal only).
    pub(crate) fn system(&self) -> &DelightQLSystem {
        &self.system
    }
}

// ── Public entry point ────────────────────────────────────────

/// Create a DqlHandle from a connection factory.
///
/// Flow:
/// 1. `factory.create(":memory:")` → initial connection + handler
/// 2. Construct the pristine world (:memory: SQLite, independent of the
///    user DB): bootstrap (id=1) and user (id=2) connection rows, builtins,
///    overlays, seeds; freeze its image; instantiate it as the session
/// 3. The "main" namespace is empty — no user introspection
/// 4. The CLI sends `mount!("path", "main")` as its first query to populate "main"
///
/// `mount_factory` is the types-level factory used by `mount!`/`import!`
/// when the path is a URI scheme (`delightql-siso://`, etc.). Embeddings that can
/// mount URI-scheme databases pass `Some` (CLI, C-ABI); those that can't
/// pass `None` (WASM) and URI mounts error with an actionable message.
pub fn open(
    factory: Box<dyn ConnectionFactory>,
    mount_factory: Option<Box<dyn delightql_types::ConnectionFactory>>,
    boot: crate::settings::BootSettings,
) -> crate::Result<Box<dyn api::DqlHandle>> {
    // The contract comes first: a host that has not stated what only it can
    // know is refused before any connection or catalog exists.
    let boot = boot.admit()?;
    let created = factory.create(":memory:")?;

    // On native, `ReadySystem::booted` constructs, finalizes (stdlib
    // overlays, seed programs), freezes and instantiates the pristine
    // world: the handle receives the one reset-capable type, which cannot
    // exist without the image every reset installs. On wasm the name is
    // the wasm system itself, which has no bootstrap catalog and no reset
    // (`reinit_bootstrap` refuses there); it is not given a pretend image.
    let mut system = ReadySystem::booted(
        created.connection,
        created.introspector,
        &created.db_type,
        &boot,
    )?;
    if let Some(mf) = mount_factory {
        system.set_connection_factory(mf);
    }

    Ok(Box::new(DqlHandleImpl {
        system: Box::new(system),
        handler_factory: created.handler_factory,
        initial_backend: Some(created.handler),
        danger_overrides: Vec::new(),
    }))
}
