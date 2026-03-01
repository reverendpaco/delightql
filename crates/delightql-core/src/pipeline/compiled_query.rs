//! Compiled query output types.
//!
//! A `CompiledQuery` bundles everything the core pipeline produces after
//! compilation: the primary SQL, assertion SQL, and emit streams. The host
//! (CLI, TUI, library) receives this and decides how to execute each piece.

/// A named SQL stream compiled from an `(~~emit:name ... ~~)` hook.
#[derive(Debug, Clone)]
pub struct EmitStream {
    /// Instance name from `(~~emit:name ~~)`.
    pub name: String,
    /// The filtered SQL query to execute.
    pub sql: String,
    /// Source location in the original DQL (byte start, byte end).
    pub _source_location: Option<(usize, usize)>,
}

/// Whether the compiled SQL is a query (returns rows) or a DML statement (returns affected count).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlKind {
    /// SELECT or similar — returns a result set.
    Query,
    /// DELETE, UPDATE, INSERT — mutates data, returns affected row count.
    Dml,
}

/// Everything the core produces after compilation, before execution.
///
/// The host receives this and decides how to execute each piece:
/// - Primary SQL goes to the main result display (stdout, table pane, etc.)
/// - Assertion SQL is evaluated for boolean verdicts
/// - Emit streams are routed to sinks (`--sink` flag, stderr, TUI panes, etc.)
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    /// The primary SQL query.
    pub primary_sql: String,
    /// Whether this is a query or DML statement.
    pub _kind: SqlKind,
    /// Assertion SQLs (boolean queries). Each is `(sql, source_location)`.
    pub assertion_sqls: Vec<(String, Option<(usize, usize)>)>,
    /// Named emit streams (filtered SQL variants).
    pub emit_streams: Vec<EmitStream>,
    /// Connection ID for routing (which backend to execute on).
    pub connection_id: Option<i64>,
}
