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

    // Create factory and open handle.
    let factory = Box::new(factory::CabiConnectionFactory);
    let mut handle: Box<dyn api::DqlHandle> = match api::open(factory) {
        Ok(h) => h,
        Err(e) => {
            set_error(error_out, &e);
            return std::ptr::null_mut();
        }
    };

    // Create session (borrows handle).
    let session = match handle.session() {
        Ok(s) => s,
        Err(e) => {
            set_error(error_out, &e);
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
    let mount_query = format!("mount!(\"{}\", \"main\")", path.replace('"', "\\\""));
    let mount_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        with_stacksafe(|| cabi.session.query(&mount_query))
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
        with_stacksafe(|| cabi.session.query(text))
    })) {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            set_error(error_out, &e);
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
        col_infos.push(DqlColumnInfo {
            name: name.into_raw(),
            position: col.position,
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
            set_error(error_out, &e);
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
            set_error(error_out, &e);
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
            set_error(error_out, &e);
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
// Tree-sitter query splitting (internal)
// ---------------------------------------------------------------------------

fn split_queries_impl(source: &str) -> Result<Vec<String>, String> {
    use tree_sitter::Language;

    extern "C" {
        fn tree_sitter_delightql_v2() -> Language;
    }

    let mut parser = tree_sitter::Parser::new();
    let language = unsafe { tree_sitter_delightql_v2() };
    parser
        .set_language(&language)
        .map_err(|e| format!("language: {e}"))?;

    let tree = parser
        .parse(source, None)
        .ok_or("tree-sitter parse failed")?;
    let root = tree.root_node();

    if root.has_error() {
        return Err(find_first_error(&root, source));
    }

    let mut cursor = root.walk();
    let has_top_level_ddl = root
        .children(&mut cursor)
        .any(|c| c.kind() == "ddl_annotation");
    if has_top_level_ddl {
        return Err("contains top-level ddl_annotation (use CLI sequential mode)".into());
    }

    let mut cursor = root.walk();
    let queries: Vec<String> = root
        .children(&mut cursor)
        .filter(|c| c.kind() == "query")
        .map(|c| source[c.start_byte()..c.end_byte()].to_string())
        .collect();

    if queries.is_empty() {
        return Err("no queries found in source".into());
    }
    Ok(queries)
}

fn find_first_error(node: &tree_sitter::Node, source: &str) -> String {
    if node.kind() == "ERROR" || node.is_error() {
        let start = node.start_position();
        let snippet: String = source[node.start_byte()..node.end_byte()]
            .chars()
            .take(40)
            .collect();
        return format!(
            "syntax error at line {}:{}: {}",
            start.row + 1,
            start.column + 1,
            snippet
        );
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if child.has_error() {
            let msg = find_first_error(&child, source);
            if !msg.is_empty() {
                return msg;
            }
        }
    }
    "syntax error (unknown location)".into()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::path::PathBuf;

    /// Resolve a workspace-relative path to absolute.
    fn workspace_path(rel: &str) -> PathBuf {
        // CARGO_MANIFEST_DIR = .../crates/delightql-cabi
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest.join("../..").join(rel).canonicalize().unwrap()
    }

    #[test]
    fn round_trip_open_query_fetch_close_destroy() {
        let abs = workspace_path("test_suite/side-effect-free/fixtures/core/core.db");
        let db_path = CString::new(abs.to_str().unwrap()).unwrap();

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
    fn open_empty_db_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        // Create an empty SQLite database file so mount! finds it.
        rusqlite::Connection::open(&path).unwrap();
        let db_path = CString::new(path.to_str().unwrap()).unwrap();

        unsafe {
            let mut err: *mut c_char = std::ptr::null_mut();
            let h = dql_open(db_path.as_ptr(), &mut err);
            if h.is_null() {
                let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                dql_free_string(err);
                panic!("dql_open failed on empty db: {}", msg);
            }
            dql_destroy(h);
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
