//! Types for the C-ABI boundary.
//!
//! - `DqlCabiHandle`: opaque, not repr(C) — only accessed through pointer.
//! - `DqlQueryResult`, `DqlFetchResult`, `DqlColumnInfo`, `DqlCell`: all repr(C).
//! - `FetchBacking`: internal, holds the contiguous byte buffer that cells point into.

use std::collections::HashMap;

use delightql_core::api::{DqlHandle, DqlSession, QueryHandle};

/// Opaque handle owning all DQL state.
///
/// Not `#[repr(C)]` — C callers only see `*mut DqlCabiHandle`.
/// Field order matters: `session` borrows from `handle`, so it must be
/// declared (and therefore dropped) first.
#[doc(hidden)]
pub struct DqlCabiHandle {
    // SAFETY: session borrows handle via lifetime erasure (transmute).
    // Declared first so it drops before handle.
    pub(crate) session: Box<dyn DqlSession + 'static>,
    #[allow(dead_code)]
    pub(crate) handle: Box<dyn DqlHandle>,
    pub(crate) queries: HashMap<u64, QueryHandle>,
    pub(crate) next_query_id: u64,
}

/// Column info returned to C callers.
#[repr(C)]
pub struct DqlColumnInfo {
    /// Null-terminated column name. Owned by the DqlQueryResult.
    pub name: *mut std::os::raw::c_char,
    pub position: usize,
}

/// Result of dql_query(). Caller must free with dql_free_query_result().
#[repr(C)]
pub struct DqlQueryResult {
    pub query_id: u64,
    pub columns: *mut DqlColumnInfo,
    pub num_columns: usize,
}

/// A single cell value.
#[repr(C)]
pub struct DqlCell {
    /// Pointer into the FetchBacking buffer. NULL means SQL NULL.
    pub data: *const u8,
    pub len: usize,
}

/// Result of dql_fetch(). Caller must free with dql_free_fetch_result().
#[repr(C)]
pub struct DqlFetchResult {
    /// Flat array of cells: rows × columns, row-major.
    pub cells: *mut DqlCell,
    pub num_rows: usize,
    pub num_cols: usize,
    /// True (1) when the server has no more rows.
    pub finished: i32,
    /// Opaque pointer to the backing store. Do not touch.
    pub _backing: *mut FetchBacking,
}

/// Result of dql_split_queries(). Caller must free with dql_free_split_result().
#[repr(C)]
pub struct DqlSplitResult {
    /// Array of null-terminated C strings (individual queries).
    pub queries: *mut *mut std::os::raw::c_char,
    pub num_queries: usize,
}

/// Internal backing store for fetch results.
///
/// Holds a contiguous byte buffer that all DqlCell pointers reference into,
/// plus the cells array itself. One free call reclaims everything.
pub struct FetchBacking {
    pub(crate) _buffer: Vec<u8>,
    pub(crate) _cells: Vec<DqlCell>,
}
