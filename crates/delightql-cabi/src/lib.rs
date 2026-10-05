// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! C-ABI shared library for DelightQL.
//!
//! Wraps the protocol-level API (DqlHandle / DqlSession) in extern "C"
//! functions suitable for FFI from Python, Swift, Go, etc.

mod factory;
mod types;

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic;
use std::sync::Once;

use delightql_core::api;
use types::{
    DqlCabiHandle, DqlCell, DqlColumnInfo, DqlFetchResult, DqlQueryResult, DqlSplitResult,
    FetchBacking,
};

// ---------------------------------------------------------------------------
// Stack-safe context
// ---------------------------------------------------------------------------

static STACKSAFE_INIT: Once = Once::new();

fn ensure_stacksafe() {
    STACKSAFE_INIT.call_once(|| {
        stacksafe::set_minimum_stack_size(512 * 1024);
    });
}

/// Run a closure inside a stack-safe context (sets the thread-local
/// `is_protected` flag and grows the stack when needed).
#[stacksafe::stacksafe]
fn with_stacksafe<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    f()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write an error string into `*error_out` if non-null. Returns a CString
/// that the caller must free with `dql_free_string`.
unsafe fn set_error(error_out: *mut *mut c_char, msg: &str) {
    if !error_out.is_null() {
        match CString::new(msg) {
            Ok(cs) => *error_out = cs.into_raw(),
            // If the message itself contains a null byte, truncate.
            Err(_) => {
                let sanitized = msg.replace('\0', "");
                if let Ok(cs) = CString::new(sanitized) {
                    *error_out = cs.into_raw();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// dql_open
// ---------------------------------------------------------------------------

/// Open a DQL handle backed by a SQLite database at `db_path`.
///
/// On success returns a non-null handle. On failure returns null and
/// writes a message into `*error_out` (free with `dql_free_string`).
#[no_mangle]
pub unsafe extern "C" fn dql_open(
    db_path: *const c_char,
    error_out: *mut *mut c_char,
) -> *mut DqlCabiHandle {
    if !error_out.is_null() {
        *error_out = std::ptr::null_mut();
    }

    if db_path.is_null() {
        set_error(error_out, "db_path is null");
        return std::ptr::null_mut();
    }

    let path = match CStr::from_ptr(db_path).to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(error_out, &format!("invalid UTF-8 in db_path: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Reject paths that look like DQL expressions rather than file paths.
    if path.contains('!') {
        set_error(
            error_out,
            "db_path contains '!' — expected a file path, not a DQL expression",
        );
        return std::ptr::null_mut();
    }

    // Verify the file exists before attempting mount — invalid paths can
    // cause heap corruption deep in the engine's error-handling path.
    if !std::path::Path::new(path).exists() {
        set_error(
            error_out,
            &format!("database file does not exist: {}", path),
        );
        return std::ptr::null_mut();
    }

    // Create factory and open handle. The second (types-level) factory
    // powers mount!/import! of URI-scheme databases.
    let factory = Box::new(factory::CabiConnectionFactory);
    let mount_factory = Box::new(factory::CabiConnectionFactory);
    // Only the embedding process knows its working directory, so the C
    // library states it at boot, captured once: a later chdir in the host
    // does not move where relative paths resolve.
    let base = std::env::current_dir()
        .ok()
        .and_then(|dir| dir.to_str().map(str::to_string));
    let boot = api::BootSettings::new().state(api::BASE_DIRECTORY, base.as_deref());
    let mut handle: Box<dyn api::DqlHandle> = match api::open(factory, Some(mount_factory), boot) {
        Ok(h) => h,
        Err(e) => {
            set_error(error_out, &api::ApiError::from(e).to_string());
            return std::ptr::null_mut();
        }
    };

    // Create session (borrows handle).
    let session = match handle.session() {
        Ok(s) => s,
        Err(e) => {
            set_error(error_out, &e.to_string());
            return std::ptr::null_mut();
        }
    };

    // SAFETY: Erase the session lifetime. The session borrows handle, and we
    // guarantee drop order (session field declared before handle field in
    // DqlCabiHandle, so it drops first).
    let session: Box<dyn api::DqlSession + 'static> = std::mem::transmute(session);

    let mut cabi = Box::new(DqlCabiHandle {
        session,
        handle,
        queries: HashMap::new(),
        next_query_id: 1,
    });

    // Send mount! to attach the user database.
    ensure_stacksafe();
    let mount_query = format!("mount!(\"{}\", \"main\")(*)", path.replace('"', "\\\""));
    let mount_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        with_stacksafe(|| {
            cabi.session
                .query(&delightql_cst::prompt_wrap(&mount_query))
        })
    }));
    match mount_result {
        Ok(Ok(result)) => {
            // Close the mount query handle immediately.
            let _ = cabi.session.close(result.handle);
        }
        Ok(Err(e)) => {
            set_error(error_out, &format!("mount failed: {}", e));
            return std::ptr::null_mut();
        }
        Err(_) => {
            set_error(error_out, "mount panicked (internal error)");
            return std::ptr::null_mut();
        }
    }

    Box::into_raw(cabi)
}

// ---------------------------------------------------------------------------
// dql_query
// ---------------------------------------------------------------------------

/// Execute a DQL query. Returns column metadata and a query_id for fetching.
/// `dql` is what a user typed at a prompt: this host writes the prompt wrap.
///
/// On failure, returns a zeroed result and writes `*error_out`.
#[no_mangle]
pub unsafe extern "C" fn dql_query(
    h: *mut DqlCabiHandle,
    dql: *const c_char,
    error_out: *mut *mut c_char,
) -> DqlQueryResult {
    if !error_out.is_null() {
        *error_out = std::ptr::null_mut();
    }

    let zero = DqlQueryResult {
        query_id: 0,
        columns: std::ptr::null_mut(),
        num_columns: 0,
    };

    if h.is_null() {
        set_error(error_out, "null handle");
        return zero;
    }

    let text = match CStr::from_ptr(dql).to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(error_out, &format!("invalid UTF-8 in query: {}", e));
            return zero;
        }
    };

    let cabi = &mut *h;

    ensure_stacksafe();
    let result = match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        with_stacksafe(|| cabi.session.query(&delightql_cst::prompt_wrap(text)))
    })) {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            set_error(error_out, &e.to_string());
            return zero;
        }
        Err(_) => {
            set_error(error_out, "query panicked (internal error)");
            return zero;
        }
    };

    // Intern the protocol QueryHandle behind a u64 ID.
    let query_id = cabi.next_query_id;
    cabi.next_query_id += 1;
    cabi.queries.insert(query_id, result.handle);

    // Build column info array.
    let num_columns = result.columns.len();
    let mut col_infos: Vec<DqlColumnInfo> = Vec::with_capacity(num_columns);
    for col in &result.columns {
        let name = CString::new(col.name.as_str()).unwrap_or_default();
        let type_name = CString::new(col.descriptor.as_str()).unwrap_or_default();
        col_infos.push(DqlColumnInfo {
            name: name.into_raw(),
            position: col.position,
            type_name: type_name.into_raw(),
            minted: u8::from(col.naming == delightql_core::api::Naming::Minted),
        });
    }

    let columns_ptr = if num_columns > 0 {
        let ptr = col_infos.as_mut_ptr();
        std::mem::forget(col_infos);
        ptr
    } else {
        std::ptr::null_mut()
    };

    DqlQueryResult {
        query_id,
        columns: columns_ptr,
        num_columns,
    }
}

// ---------------------------------------------------------------------------
// dql_fetch
// ---------------------------------------------------------------------------

/// Fetch up to `count` rows from an open query.
///
/// On failure, returns a zeroed result and writes `*error_out`.
#[no_mangle]
pub unsafe extern "C" fn dql_fetch(
    h: *mut DqlCabiHandle,
    query_id: u64,
    count: u64,
    error_out: *mut *mut c_char,
) -> DqlFetchResult {
    if !error_out.is_null() {
        *error_out = std::ptr::null_mut();
    }

    let zero = DqlFetchResult {
        cells: std::ptr::null_mut(),
        num_rows: 0,
        num_cols: 0,
        finished: 0,
        _backing: std::ptr::null_mut(),
    };

    if h.is_null() {
        set_error(error_out, "null handle");
        return zero;
    }

    let cabi = &mut *h;

    let qh = match cabi.queries.get(&query_id) {
        Some(qh) => qh,
        None => {
            set_error(error_out, "unknown query_id");
            return zero;
        }
    };

    let result = match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        with_stacksafe(|| cabi.session.fetch(qh, count))
    })) {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            set_error(error_out, &e.to_string());
            return zero;
        }
        Err(_) => {
            set_error(error_out, "fetch panicked (internal error)");
            return zero;
        }
    };

    let num_rows = result.rows.len();
    let num_cols = if num_rows > 0 {
        result.rows[0].len()
    } else {
        0
    };

    // Pack all cell data into one contiguous buffer.
    let mut buffer: Vec<u8> = Vec::new();
    // Offsets: (start, len) for each cell. usize::MAX means NULL.
    let mut offsets: Vec<(usize, usize)> = Vec::with_capacity(num_rows * num_cols);

    for row in &result.rows {
        for cell in row {
            match cell {
                Some(data) => {
                    let start = buffer.len();
                    buffer.extend_from_slice(data);
                    offsets.push((start, data.len()));
                }
                None => {
                    offsets.push((usize::MAX, 0));
                }
            }
        }
    }

    // Build DqlCell array with placeholder pointers (corrected after move into Box).
    let mut cells: Vec<DqlCell> = Vec::with_capacity(offsets.len());
    for &(start, len) in &offsets {
        if start == usize::MAX {
            cells.push(DqlCell {
                data: std::ptr::null(),
                len: 0,
            });
        } else {
            // Placeholder — will be corrected below.
            cells.push(DqlCell {
                data: std::ptr::null(),
                len,
            });
        }
    }

    // Move buffer + cells into a heap-allocated backing so pointers are stable.
    let backing = Box::new(FetchBacking {
        _buffer: buffer,
        _cells: cells,
    });
    let backing_ptr = Box::into_raw(backing);

    // Patch cell data pointers to reference the backing's buffer.
    let buf_ptr = (*backing_ptr)._buffer.as_ptr();
    for (cell, &(start, _)) in (*backing_ptr)._cells.iter_mut().zip(offsets.iter()) {
        if start != usize::MAX {
            cell.data = buf_ptr.add(start);
        }
    }

    let cells_ptr = if (*backing_ptr)._cells.is_empty() {
        std::ptr::null_mut()
    } else {
        (*backing_ptr)._cells.as_mut_ptr()
    };

    DqlFetchResult {
        cells: cells_ptr,
        num_rows,
        num_cols,
        finished: if result.finished { 1 } else { 0 },
        _backing: backing_ptr,
    }
}

// ---------------------------------------------------------------------------
// dql_close_query
// ---------------------------------------------------------------------------

/// Close an open query handle, releasing server-side resources.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn dql_close_query(
    h: *mut DqlCabiHandle,
    query_id: u64,
    error_out: *mut *mut c_char,
) -> i32 {
    if !error_out.is_null() {
        *error_out = std::ptr::null_mut();
    }

    if h.is_null() {
        set_error(error_out, "null handle");
        return -1;
    }

    let cabi = &mut *h;

    let qh = match cabi.queries.remove(&query_id) {
        Some(qh) => qh,
        None => {
            set_error(error_out, "unknown query_id");
            return -1;
        }
    };

    match panic::catch_unwind(panic::AssertUnwindSafe(|| cabi.session.close(qh))) {
        Ok(Ok(())) => 0,
        Ok(Err(e)) => {
            set_error(error_out, &e.to_string());
            -1
        }
        Err(_) => {
            set_error(error_out, "close_query panicked (internal error)");
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// dql_destroy
// ---------------------------------------------------------------------------

/// Destroy the DQL handle, closing the session and database connection.
///
/// After this call, `h` is dangling — do not use it.
#[no_mangle]
pub unsafe extern "C" fn dql_destroy(h: *mut DqlCabiHandle) {
    if !h.is_null() {
        // Box::from_raw reclaims ownership; drop order in DqlCabiHandle
        // ensures session drops before handle.
        let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let _ = Box::from_raw(h);
        }));
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Free a string previously returned in an `error_out` parameter.
#[no_mangle]
pub unsafe extern "C" fn dql_free_string(s: *mut c_char) {
    if !s.is_null() {
        let _ = CString::from_raw(s);
    }
}

/// Free a DqlQueryResult returned by `dql_query`.
#[no_mangle]
pub unsafe extern "C" fn dql_free_query_result(result: *mut DqlQueryResult) {
    if result.is_null() {
        return;
    }
    let r = &*result;
    if !r.columns.is_null() && r.num_columns > 0 {
        // Free each column name CString.
        let columns = Vec::from_raw_parts(r.columns, r.num_columns, r.num_columns);
        for col in columns {
            if !col.name.is_null() {
                let _ = CString::from_raw(col.name);
            }
            if !col.type_name.is_null() {
                let _ = CString::from_raw(col.type_name);
            }
        }
    }
    // Zero out the struct so double-free is harmless.
    (*result).columns = std::ptr::null_mut();
    (*result).num_columns = 0;
    (*result).query_id = 0;
}

/// Free a DqlFetchResult returned by `dql_fetch`.
#[no_mangle]
pub unsafe extern "C" fn dql_free_fetch_result(result: *mut DqlFetchResult) {
    if result.is_null() {
        return;
    }
    let r = &*result;
    if !r._backing.is_null() {
        let _ = Box::from_raw(r._backing);
    }
    // Zero out the struct so double-free is harmless.
    (*result).cells = std::ptr::null_mut();
    (*result).num_rows = 0;
    (*result).num_cols = 0;
    (*result)._backing = std::ptr::null_mut();
}

// ---------------------------------------------------------------------------
// dql_split_queries
// ---------------------------------------------------------------------------

/// Split DQL source into individual query strings using tree-sitter.
///
/// On success, returns a `DqlSplitResult` with an array of C strings.
/// On failure (parse error, DDL annotation, etc.), returns a zeroed result
/// and writes a message into `*error_out`.
///
/// Free with `dql_free_split_result`.
#[no_mangle]
pub unsafe extern "C" fn dql_split_queries(
    source: *const c_char,
    error_out: *mut *mut c_char,
) -> DqlSplitResult {
    if !error_out.is_null() {
        *error_out = std::ptr::null_mut();
    }

    let zero = DqlSplitResult {
        queries: std::ptr::null_mut(),
        num_queries: 0,
    };

    if source.is_null() {
        set_error(error_out, "source is null");
        return zero;
    }

    let text = match CStr::from_ptr(source).to_str() {
        Ok(s) => s,
        Err(e) => {
            set_error(error_out, &format!("invalid UTF-8 in source: {}", e));
            return zero;
        }
    };

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| split_queries_impl(text)));

    match result {
        Ok(Ok(queries)) => {
            let num = queries.len();
            let array = queries
                .into_iter()
                .map(|q| CString::new(q).unwrap_or_default().into_raw())
                .collect::<Vec<*mut c_char>>();
            let ptr = Box::into_raw(array.into_boxed_slice()) as *mut *mut c_char;
            DqlSplitResult {
                queries: ptr,
                num_queries: num,
            }
        }
        Ok(Err(e)) => {
            set_error(error_out, &e.to_string());
            zero
        }
        Err(_) => {
            set_error(error_out, "split_queries panicked (internal error)");
            zero
        }
    }
}

/// Free a `DqlSplitResult` returned by `dql_split_queries`.
#[no_mangle]
pub unsafe extern "C" fn dql_free_split_result(result: *mut DqlSplitResult) {
    if result.is_null() {
        return;
    }
    let r = &*result;
    if !r.queries.is_null() && r.num_queries > 0 {
        let slice = std::slice::from_raw_parts(r.queries, r.num_queries);
        for &ptr in slice {
            if !ptr.is_null() {
                let _ = CString::from_raw(ptr);
            }
        }
        // Reconstruct the boxed slice and drop it.
        let _ = Box::from_raw(std::slice::from_raw_parts_mut(r.queries, r.num_queries));
    }
    (*result).queries = std::ptr::null_mut();
    (*result).num_queries = 0;
}

// ---------------------------------------------------------------------------
// Result digest
// ---------------------------------------------------------------------------

/// The name of the digest framing `dql_digest_rows` computes: a static
/// NUL-terminated string. Do not free it.
#[no_mangle]
pub extern "C" fn dql_digest_version() -> *const c_char {
    static VERSION: std::sync::OnceLock<CString> = std::sync::OnceLock::new();
    VERSION
        .get_or_init(|| CString::new(delightql_protocol::digest::VERSION).unwrap_or_default())
        .as_ptr()
}

/// The data digest of `num_rows` rows of `num_cols` cells, as lowercase hex.
///
/// The cells are laid out row-major. `cell_lens[i]` is the length of cell `i`,
/// or -1 when the cell is SQL NULL; the present cells' bytes are concatenated,
/// in order, in `data` (`data_len` bytes, which may be null when zero). This
/// is the same digest the CLI's `-f hash` prints and the ball runner pins, so
/// a host passes the bytes it fetched, never a decoding of them.
///
/// On success returns a string to free with `dql_free_string`. On failure
/// returns null and writes `*error_out`.
#[no_mangle]
pub unsafe extern "C" fn dql_digest_rows(
    data: *const u8,
    data_len: usize,
    cell_lens: *const i64,
    num_rows: usize,
    num_cols: usize,
    error_out: *mut *mut c_char,
) -> *mut c_char {
    if !error_out.is_null() {
        *error_out = std::ptr::null_mut();
    }
    let Some(num_cells) = num_rows.checked_mul(num_cols) else {
        set_error(error_out, "num_rows * num_cols overflows");
        return std::ptr::null_mut();
    };
    if (data.is_null() && data_len > 0) || (cell_lens.is_null() && num_cells > 0) {
        set_error(error_out, "null buffer with a nonzero length");
        return std::ptr::null_mut();
    }
    let data: &[u8] = if data_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(data, data_len)
    };
    let lens: &[i64] = if num_cells == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(cell_lens, num_cells)
    };

    let mut observation = delightql_protocol::digest::Observation::new();
    let mut offset = 0usize;
    for row in lens.chunks(num_cols.max(1)).take(num_rows) {
        let mut cells: Vec<Option<&[u8]>> = Vec::with_capacity(num_cols);
        for &len in row {
            if len < 0 {
                cells.push(None);
                continue;
            }
            let end = match offset.checked_add(len as usize) {
                Some(end) if end <= data.len() => end,
                _ => {
                    set_error(error_out, "cell lengths exceed data_len");
                    return std::ptr::null_mut();
                }
            };
            cells.push(Some(&data[offset..end]));
            offset = end;
        }
        observation.row(cells);
    }
    // A zero-width result still has rows; `chunks` yields none for them.
    if num_cols == 0 {
        for _ in 0..num_rows {
            observation.row([]);
        }
    }
    if offset != data.len() {
        set_error(error_out, "data_len exceeds the cells' lengths");
        return std::ptr::null_mut();
    }
    match CString::new(observation.data().hex()) {
        Ok(hex) => hex.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

// ---------------------------------------------------------------------------
// Tree-sitter query splitting (internal)
// ---------------------------------------------------------------------------

/// The ONE splitter, reached through the core's public API.
///
/// This crate had its own copy: a second tree-sitter parse and a second
/// error-node walk. One split, stated once, over the shared entrance.
fn split_queries_impl(source: &str) -> Result<Vec<String>, String> {
    delightql_core::api::split_queries(source)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;


    #[test]
    fn round_trip_open_query_fetch_close_destroy() {
        // Self-contained fixture (the retired test_suite/ tree once served
        // this; sqlite-relay's tests were burned by the same dependency and
        // made the same move).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("roundtrip.db");
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE users (user_id INTEGER, name TEXT, age INTEGER);
             INSERT INTO users VALUES (1, 'ada', 36), (2, 'alan', 41);",
        )
        .unwrap();
        drop(conn);
        let db_path = CString::new(path.to_str().unwrap()).unwrap();

        unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();

            // Open
            let h = dql_open(db_path.as_ptr(), &mut err);
            if h.is_null() {
                let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                dql_free_string(err);
                panic!("dql_open failed: {}", msg);
            }

            // Query
            let query = CString::new("users(*)").unwrap();
            let qr = dql_query(h, query.as_ptr(), &mut err);
            if qr.query_id == 0 {
                let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                dql_free_string(err);
                dql_destroy(h);
                panic!("dql_query failed: {}", msg);
            }
            assert!(qr.num_columns > 0, "expected at least one column");

            // Fetch
            let fr = dql_fetch(h, qr.query_id, 100, &mut err);
            if !err.is_null() {
                let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                dql_free_string(err);
                dql_free_query_result(&qr as *const _ as *mut _);
                dql_destroy(h);
                panic!("dql_fetch failed: {}", msg);
            }
            assert!(fr.num_rows > 0, "expected at least one row");

            // Close query
            let rc = dql_close_query(h, qr.query_id, &mut err);
            assert_eq!(rc, 0, "dql_close_query failed");

            // Free results
            dql_free_fetch_result(&fr as *const _ as *mut _);
            dql_free_query_result(&qr as *const _ as *mut _);

            // Destroy
            dql_destroy(h);
        }
    }

    #[test]
    fn open_schemaless_valid_db_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        // A VALID SQLite database with no tables. A bare rusqlite open
        // leaves a 0-byte file, which mount! REFUSES (mount! is
        // attach-only and rejects missing/empty/invalid files) — write
        // the header so the file is a real database, the same way
        // mount_new! provisions one.
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA user_version = 1;").unwrap();
        drop(conn);
        let db_path = CString::new(path.to_str().unwrap()).unwrap();

        unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();
            let h = dql_open(db_path.as_ptr(), &mut err);
            if h.is_null() {
                let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                dql_free_string(err);
                panic!("dql_open failed on valid schemaless db: {}", msg);
            }
            dql_destroy(h);
        }
    }

    #[test]
    fn open_zero_byte_file_refuses() {
        // The nullmount ruling, pinned at the C-ABI boundary: a 0-byte
        // file is not a database and mount! (via dql_open)(*) refuses it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.db");
        rusqlite::Connection::open(&path).unwrap();
        let db_path = CString::new(path.to_str().unwrap()).unwrap();

        unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();
            let h = dql_open(db_path.as_ptr(), &mut err);
            assert!(h.is_null(), "0-byte file must refuse to mount");
            assert!(!err.is_null());
            dql_free_string(err);
        }
    }

    /// Every committed vector, marshalled the way a host marshals fetched
    /// cells, answers the shared digest's value.
    #[test]
    fn digest_rows_answers_the_committed_vectors() {
        let doc: serde_json::Value = serde_json::from_str(include_str!(
            "../../delightql-protocol/src/digest/vectors.json"
        ))
        .unwrap();
        unsafe {
            let version = CStr::from_ptr(dql_digest_version()).to_str().unwrap();
            assert_eq!(version, doc["version"]);
        }
        for vector in doc["vectors"].as_array().unwrap() {
            let rows = vector["rows"].as_array().unwrap();
            let num_cols = vector["heading"].as_array().unwrap().len();
            let mut data = Vec::new();
            let mut lens = Vec::new();
            for row in rows {
                for cell in row.as_array().unwrap() {
                    match cell.as_str() {
                        None => lens.push(-1i64),
                        Some(hex) => {
                            let bytes: Vec<u8> = (0..hex.len())
                                .step_by(2)
                                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                                .collect();
                            lens.push(bytes.len() as i64);
                            data.extend(bytes);
                        }
                    }
                }
            }
            unsafe {
                let mut err: *mut c_char = std::ptr::null_mut();
                let hex = dql_digest_rows(
                    data.as_ptr(),
                    data.len(),
                    lens.as_ptr(),
                    rows.len(),
                    num_cols,
                    &mut err,
                );
                assert!(!hex.is_null(), "{}", vector["name"]);
                assert_eq!(
                    CStr::from_ptr(hex).to_str().unwrap(),
                    vector["data"],
                    "{}",
                    vector["name"]
                );
                dql_free_string(hex);
            }
        }
    }

    #[test]
    fn digest_rows_refuses_lengths_that_disagree_with_the_data() {
        unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();
            let hex = dql_digest_rows(b"ab".as_ptr(), 2, [3i64].as_ptr(), 1, 1, &mut err);
            assert!(hex.is_null());
            assert!(!err.is_null());
            dql_free_string(err);
            let hex = dql_digest_rows(b"ab".as_ptr(), 2, [1i64].as_ptr(), 1, 1, &mut err);
            assert!(hex.is_null());
            assert!(!err.is_null());
            dql_free_string(err);
        }
    }

    #[test]
    fn null_handle_returns_error() {
        unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();
            let query = CString::new("users(*)").unwrap();
            let qr = dql_query(std::ptr::null_mut(), query.as_ptr(), &mut err);
            assert_eq!(qr.query_id, 0);
            assert!(!err.is_null());
            dql_free_string(err);
        }
    }
}
