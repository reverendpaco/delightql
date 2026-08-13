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

    /// Create a session with side-channel hooks installed: mid-run
    /// `stdout!` result sets deliver through `hooks.on_ship` as
    /// they execute (the run's return value never passes through it).
    /// The CLI wires a console sink here; hosts that don't care call
    /// `session()`.
    fn session_with_hooks(
        &mut self,
        hooks: SessionHooks,
    ) -> Result<Box<dyn DqlSession + '_>, String>;

    /// Create a relay for raw protocol handling (server use).
    fn create_relay(&mut self) -> Result<Box<dyn ServerRelay + '_>, String>;

    /// Run self-diagnostics against this handle's system. Default: no
    /// findings, for hosts that do not implement diagnostics.
    fn selftest(&self) -> Vec<crate::diagnostics::DiagnosticFinding> {
        Vec::new()
    }

    /// Bind a static SQLite database image under a host-chosen name so DQL
    /// text can mount it: `mount!("delightql-bytes://<name>", "<ns>")`.
    /// Not a mount verb — a name binding, the same
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

    /// The session's typed health, for the host recovery boundary.
    ///
    /// A quarantined session refuses new queries until it is reset; an
    /// interactive host must not present another ordinary prompt backed by
    /// one. This report is the ONE way a host learns that — matching error
    /// text is forbidden, because messages carry the incident as data and
    /// change shape freely.
    fn session_health(&self) -> SessionHealthReport;

    /// Reset the session: retry any pending external-effect compensation,
    /// rebuild the catalog, and clear the quarantine latch — the same
    /// authority behind the protocol `Reset` control. Succeeds only when
    /// every step does; a failure retains the quarantine, and an interactive
    /// host must then terminate this connection rather than present a prompt
    /// backed by it.
    ///
    /// Legal on a healthy session too (it is an ordinary full reset), so a
    /// host needs no second road for explicit reset.
    fn recover_session(&mut self) -> Result<SessionRecovery, String>;
}

/// Typed session health, as plain data across the API boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionHealthReport {
    Healthy,
    /// The ruled incident (`runtime/session_health/external_effect`): an
    /// external effect's recovery became uncertain. `operation` names what
    /// was being compensated; `message` carries the primary failure.
    Quarantined { operation: String, message: String },
}

/// What a successful `recover_session` did, as plain data the host can
/// report verbatim: what was rebuilt, what session-local state was lost,
/// and what survives untouched.
#[derive(Debug, Clone)]
pub struct SessionRecovery {
    pub rebuilt: String,
    pub lost: String,
    pub retained: String,
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

// --- Session hooks (plain data across the boundary) ---

/// Hooks a host installs on a session (`DqlHandle::session_with_hooks`).
/// Deliberately a struct of plain callbacks — not a re-export of the
/// relay's internal hook type — per this module's boundary doctrine.
#[derive(Default)]
pub struct SessionHooks {
    /// Called for each NON-FINAL shipped result set (`stdout!`), in
    /// execution order, as it executes. Args: (columns, rows), each row a
    /// vector of cells where `None` is SQL NULL — the same shape
    /// `FetchResult` carries, so a shipped set and a fetched one read
    /// alike. The FINAL shipped set is the run's one wire response and
    /// never passes through here. Unset =
    /// executed and discarded.
    #[allow(clippy::type_complexity)]
    pub on_ship: Option<Box<dyn FnMut(&[String], &[Vec<Option<Vec<u8>>>])>>,
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
    crate::pipeline::split_queries(source).map_err(|e| format!("{}", e))
}

/// What a limit-setting request did, and what is in force afterwards.
///
/// A host has three outcomes to tell apart and cannot act on fewer: a request
/// above the ceiling CHANGES the process to the ceiling, while a request of
/// zero changes nothing. Reporting both as failure would leave a caller
/// unable to say whether its own number, the ceiling, or the value it never
/// chose is now armed.
pub use crate::compiler_limits::LimitOutcome;

/// Set this process's nesting budget — the depth beyond which a query is
/// refused rather than walked (`operational/resource/nesting`).
///
/// A host that knows its own stack states it; one that does not gets a
/// default measured against this tree's own debug binary. Zero is invalid and
/// leaves the previous budget standing; a value above [`nesting_ceiling`] is
/// applied AS the ceiling and says so.
///
/// A compilation already under way is not affected: it armed when it started.
pub fn set_max_nesting(levels: usize) -> LimitOutcome {
    crate::pipeline::parse::nesting::set_max_nesting(levels)
}

/// The nesting budget in force for compilations started from now on.
pub fn max_nesting() -> usize {
    crate::pipeline::parse::nesting::max_nesting()
}

/// The nesting depth ordinary runtime configuration cannot raise past.
///
/// It bounds CONFIGURATION rather than physics: a host thread smaller than
/// this process's main one can overflow below it. What it guarantees is that
/// no environment variable or setter reaches further out than this.
pub fn nesting_ceiling() -> usize {
    crate::compiler_limits::NESTING.ceiling()
}

/// Set this process's refinement budget — the active refiner frames one
/// compilation may hold before it is refused
/// (`operational/resource/refinement-depth`).
///
/// A DIFFERENT budget from [`set_max_nesting`]: that one measures the
/// authored tree before any walk, this one measures refinement while it
/// runs. Raising one does not raise the other.
///
/// Zero is invalid and leaves the previous budget standing; a value above
/// [`refinement_depth_ceiling`] is applied AS the ceiling and says so.
pub fn set_max_refinement_depth(frames: usize) -> LimitOutcome {
    crate::refinement_budget::set_max_refinement_depth(frames)
}

/// The refinement budget in force for compilations started from now on.
pub fn max_refinement_depth() -> usize {
    crate::refinement_budget::max_refinement_depth()
}

/// The refinement depth ordinary runtime configuration cannot raise past.
pub fn refinement_depth_ceiling() -> usize {
    crate::compiler_limits::REFINEMENT_DEPTH.ceiling()
}

/// The setter contract, pinned where a host meets it.
///
/// Every value stored here is AT OR ABOVE its default on purpose: the budgets
/// are process-wide and every compilation reads them, so a test that armed a
/// small one would be a spurious refusal in whatever the harness compiles in
/// that instant. Raising a ceiling harms nothing; lowering a floor does.
#[cfg(test)]
mod limit_setting_tests {
    use super::*;
    use crate::compiler_limits::ProcessLimitLease;

    /// Zero is not a spelling for "unlimited". It is invalid, and the budget
    /// the process already carried is what it still carries.
    #[test]
    fn zero_is_invalid_and_changes_nothing() {
        let _lease = ProcessLimitLease::take();
        set_max_nesting(700);
        set_max_refinement_depth(1024);

        assert_eq!(
            set_max_nesting(0),
            LimitOutcome::Invalid {
                requested: 0,
                effective: 700
            }
        );
        assert_eq!(max_nesting(), 700, "the previous budget still stands");

        assert_eq!(
            set_max_refinement_depth(0),
            LimitOutcome::Invalid {
                requested: 0,
                effective: 1024
            }
        );
        assert_eq!(max_refinement_depth(), 1024);
    }

    /// A value inside the range is applied as asked, and reported as exact.
    #[test]
    fn a_value_inside_the_range_is_applied_exactly() {
        let _lease = ProcessLimitLease::take();

        assert_eq!(set_max_nesting(700), LimitOutcome::Exact { effective: 700 });
        assert_eq!(max_nesting(), 700);

        assert_eq!(
            set_max_refinement_depth(1024),
            LimitOutcome::Exact { effective: 1024 }
        );
        assert_eq!(max_refinement_depth(), 1024);
    }

    /// Above the ceiling MUTATES — to the ceiling — and says that it did.
    /// This is the outcome the old boolean could not tell from a refusal.
    #[test]
    fn a_value_above_the_ceiling_is_applied_as_the_ceiling() {
        let _lease = ProcessLimitLease::take();

        assert_eq!(
            set_max_nesting(usize::MAX),
            LimitOutcome::Clamped {
                requested: usize::MAX,
                effective: nesting_ceiling()
            }
        );
        assert_eq!(max_nesting(), nesting_ceiling());

        assert_eq!(
            set_max_refinement_depth(usize::MAX),
            LimitOutcome::Clamped {
                requested: usize::MAX,
                effective: refinement_depth_ceiling()
            }
        );
        assert_eq!(max_refinement_depth(), refinement_depth_ceiling());
    }

    /// Whatever the outcome, the caller can read what is armed from it
    /// without a second call — including the invalid one, where the answer
    /// is the value it did not change.
    #[test]
    fn every_outcome_states_the_effective_value() {
        let _lease = ProcessLimitLease::take();
        for requested in [700, usize::MAX, 0] {
            assert_eq!(
                set_max_nesting(requested).effective(),
                max_nesting(),
                "nesting, asked for {requested}"
            );
        }
        for requested in [1024, usize::MAX, 0] {
            assert_eq!(
                set_max_refinement_depth(requested).effective(),
                max_refinement_depth(),
                "refinement depth, asked for {requested}"
            );
        }
    }
}

// --- Escape hatch for src/bin/ targets (fuzzgen) ---
// These bins live inside delightql-core but compile as external consumers.
// TODO: move fuzzgen to its own crate so this can be deleted.
#[doc(hidden)]
pub mod internals {
    pub use crate::pipeline::parse;
}
