// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Public API facade for delightql-core.
//!
//! The CLI calls session.query() and session.fetch(). Nothing else
//! crosses the boundary.
//!
//! ## Compiler-enforced boundary
//!
//! `DqlHandle`, `DqlSession`, and `ServerRelay` are **traits**. The CLI
//! receives `Box<dyn DqlHandle>` from `open()`. Because you cannot add
//! methods to a trait object, new smuggling attempts are compile errors.
//!
//! DO NOT add re-exports here. If the CLI needs something from core,
//! the answer is to move the logic into core — not to widen this surface.

// Re-export the protocol Handler trait (needed by ConnectionFactory and ServerRelay)
pub use delightql_protocol::Handler;

// Re-export QueryHandle — opaque to the CLI
pub use delightql_protocol::QueryHandle;

// --- Traits (the compiler-enforced boundary) ---

/// Opaque handle that owns all DQL state. Created by `open()`.
///
/// The CLI interacts with DQL exclusively through this trait and
/// the `DqlSession` / `ServerRelay` it produces.
pub trait DqlHandle: Send {
    /// Create a session for query execution.
    fn session(&mut self) -> Result<Box<dyn DqlSession + '_>, String>;

    /// Create a session with side-channel hooks installed (Epic 3.3):
    /// mid-run `stdout!` result sets deliver through `hooks.on_ship` as
    /// they execute (the run's return value never passes through it).
    /// The CLI wires a console sink here; hosts that don't care call
    /// `session()`.
    fn session_with_hooks(
        &mut self,
        hooks: SessionHooks,
    ) -> Result<Box<dyn DqlSession + '_>, String>;

    /// Create a relay for raw protocol handling (server use).
    fn create_relay(&mut self) -> Result<Box<dyn ServerRelay + '_>, String>;

    /// Run self-diagnostics against this handle's system (see
    /// DIAGNOSTICS-DESIGN.md). Default: no findings, for hosts that do not
    /// implement diagnostics.
    fn selftest(&self) -> Vec<crate::diagnostics::DiagnosticFinding> {
        Vec::new()
    }

    /// Bind a static SQLite database image under a host-chosen name so DQL
    /// text can mount it: `mount!("delightql-bytes://<name>", "<ns>")`
    /// (BYTES-SCHEME-DESIGN.md). Not a mount verb — a name binding, the same
    /// ontological rank as the filesystem giving meaning to paths. Names are
    /// lowercase capability labels (`[a-z][a-z0-9._-]*`) and immutable for
    /// the life of the handle (rebinding refuses). Default: unsupported, for
    /// hosts (WASM) that cannot attach deserialized native SQLite schemas.
    fn bind_static_bytes(&mut self, _name: &str, _bytes: &'static [u8]) -> Result<(), String> {
        Err("bind_static_bytes is not supported by this host".to_string())
    }

    /// Owned-buffer sibling of `bind_static_bytes`: for database images
    /// built at runtime (e.g. the CLI's live surface database). Same name
    /// grammar, immutability, and bind-time validation; the buffer is
    /// copied into SQLite-owned memory at attach.
    fn bind_owned_bytes(&mut self, _name: &str, _bytes: Vec<u8>) -> Result<(), String> {
        Err("bind_owned_bytes is not supported by this host".to_string())
    }

    /// Set the handle's session-baseline danger overrides (CLI `--danger`).
    /// Every session and relay created afterward inherits them. Specs are
    /// the textual `hierarchy=STATE` forms; CORE parses and validates —
    /// unknown gates, bad states, and non-overridable gates refuse with a
    /// teaching error. Hosts never see the compiler's representation.
    /// Default: unsupported.
    fn set_danger_overrides(&mut self, _specs: &[String]) -> Result<(), String> {
        Err("danger overrides are not supported by this host".to_string())
    }
}

/// A DQL session for query/fetch/close operations.
///
/// Created via `DqlHandle::session()`. The session borrows the handle
/// for its lifetime.
pub trait DqlSession {
    /// Send a DQL query. Returns column metadata + an opaque handle.
    fn query(&mut self, text: &str) -> Result<QueryResult, String>;

    /// Fetch rows from an open query handle.
    fn fetch(&mut self, handle: &QueryHandle, count: u64) -> Result<FetchResult, String>;

    /// Close a query handle.
    fn close(&mut self, handle: QueryHandle) -> Result<(), String>;
}

/// A relay for raw protocol handling (server use).
///
/// Extends `Handler` (from delightql-protocol) with reset capability.
pub trait ServerRelay: Handler {
    /// Close all open handles and reinitialize the system.
    fn handle_reset(&mut self) -> Result<(), String>;
}

// --- Session hooks (the Epic-3.3 side channel, plain data across the boundary) ---

/// Hooks a host installs on a session (`DqlHandle::session_with_hooks`).
/// Deliberately a struct of plain callbacks — not a re-export of the
/// relay's internal hook type — per this module's boundary doctrine.
#[derive(Default)]
pub struct SessionHooks {
    /// Called for each NON-FINAL shipped result set (`stdout!`), in
    /// execution order, as it executes. Args: (columns, rows). The FINAL
    /// shipped set is the run's one wire response and never passes
    /// through here (Epic-3 protocol ruling). Unset = executed and
    /// discarded.
    pub on_ship: Option<Box<dyn FnMut(&[String], &[Vec<String>])>>,
}

// --- Return structs (not protocol types) ---

/// Column metadata returned by `DqlSession::query()`.
pub struct ColumnInfo {
    pub name: String,
    pub descriptor: String,
    pub position: usize,
}

/// Result of a successful `DqlSession::query()`.
pub struct QueryResult {
    pub handle: QueryHandle,
    pub columns: Vec<ColumnInfo>,
}

/// Result of a successful `DqlSession::fetch()`.
pub struct FetchResult {
    /// Each row is a vector of cells. `None` = SQL NULL.
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// True when the server has no more rows.
    pub finished: bool,
}

// --- Connection factory ---

/// Components produced by a connection factory.
///
/// Bundles the protocol handler, raw connection, introspector, and DB type
/// so that `open()` can initialize the system from a single factory call.
pub struct CreatedConnection {
    /// Protocol handler for SQL execution (streaming or eager).
    pub handler: Box<dyn Handler + Send>,
    /// Factory closure that creates new handlers wrapping the SAME connection.
    /// This is essential: after mount! does ATTACH on the connection,
    /// subsequent sessions need handlers that see the attached databases.
    /// Calling factory.create(":memory:") would create a DIFFERENT connection.
    pub handler_factory: Box<dyn Fn() -> Box<dyn Handler + Send> + Send + Sync>,
    /// Raw database connection (for ATTACH operations, connection routing).
    pub connection: std::sync::Arc<std::sync::Mutex<dyn delightql_types::DatabaseConnection>>,
    /// Entity introspector for discovering tables/views.
    pub introspector: Box<dyn delightql_types::introspect::DatabaseIntrospector>,
    /// Database type string ("sqlite", "duckdb", "postgres").
    pub db_type: String,
}

/// Factory that creates database connections from URIs.
///
/// Returns `CreatedConnection` — handler, connection, introspector, and DB type.
/// The CLI implements this; core defines and consumes the trait.
pub trait ConnectionFactory: Send + Sync {
    fn create(
        &self,
        uri: &str,
    ) -> std::result::Result<CreatedConnection, Box<dyn std::error::Error + Send + Sync>>;
}

// --- Entry point ---
pub use crate::open::open;

// --- Query splitting ---

/// Split DQL source into individual query texts.
///
/// Uses tree-sitter to find top-level `query` nodes and returns one
/// `String` per query. Clients that need sequential execution should
/// call this, then send each result as a separate `DqlSession::query()`.
///
/// Errors if the source has parse errors or contains zero queries.
pub fn split_queries(source: &str) -> Result<Vec<String>, String> {
    crate::pipeline::parser::split_queries(source).map_err(|e| format!("{}", e))
}

// --- Escape hatch for src/bin/ targets (fuzzgen) ---
// These bins live inside delightql-core but compile as external consumers.
// TODO: move fuzzgen to its own crate so this can be deleted.
#[doc(hidden)]
pub mod internals {
    pub use crate::pipeline::builder_v2;
    pub use crate::pipeline::parser;
}
