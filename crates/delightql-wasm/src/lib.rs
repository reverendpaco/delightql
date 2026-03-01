//! DelightQL WASM Bridge
//!
//! Provides WebAssembly bindings for DelightQL using the protocol/session API.
//! Bridges between DelightQL's Rust engine and JavaScript's sqlite3-wasm.
//!
//! # Architecture
//!
//! Uses the same API boundary as native builds:
//!   open(factory) → DqlHandle → session() → DqlSession → query/fetch/close
//!
//! The WasmConnectionFactory creates connections backed by two JS bridge
//! functions: `bridge_sql` (query) and `bridge_execute` (DML/DDL).
//! All schema introspection goes through regular SQL (PRAGMA, sqlite_master).

// dlmalloc with "global" feature sets itself as global allocator
// (more robust than wee_alloc for tree-sitter's memory management)

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use delightql_core::api;
use delightql_protocol::{
    ByteSeq, Cell, ClientTerm, Dimension, ErrorKind, Handle, Handler, MetaItem, Orientation,
    Projection, ServerTerm, resolve_projection, CELL_TAG_TEXT,
};
use delightql_types::{
    DatabaseConnection, DbValue,
    DelightQLError, Result,
};
use serde::Deserialize;
use wasm_bindgen::prelude::*;

// ============================================================================
// JavaScript Bridge (2 functions)
// ============================================================================

#[wasm_bindgen]
extern "C" {
    /// Execute a SQL query and return results as JSON: {"columns": [...], "rows": [[...]]}
    ///
    /// Used for all SELECT-like queries including PRAGMA and sqlite_master lookups.
    /// Returns null/undefined if no results (DML executed as query).
    #[wasm_bindgen(js_name = bridge_sql)]
    fn js_bridge_sql(sql: &str) -> JsValue;

    /// Execute a SQL statement (INSERT, UPDATE, DELETE, DDL) and return rows affected.
    #[wasm_bindgen(js_name = bridge_execute)]
    fn js_bridge_execute(sql: &str) -> i32;

    /// Console.log for debugging
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

// ============================================================================
// JSON deserialization for bridge_sql results
// ============================================================================

#[derive(Deserialize)]
struct BridgeSqlResult {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

fn parse_bridge_result(js_val: &JsValue) -> Option<BridgeSqlResult> {
    if js_val.is_null() || js_val.is_undefined() {
        return None;
    }
    let json_str = js_sys::JSON::stringify(js_val).ok()?.as_string()?;
    serde_json::from_str(&json_str).ok()
}

fn json_value_to_string(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::Null => None,
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Bool(b) => Some(if *b { "1" } else { "0" }.to_string()),
        other => Some(other.to_string()),
    }
}

fn json_value_to_db_value(val: &serde_json::Value) -> DbValue {
    match val {
        serde_json::Value::Null => DbValue::Null,
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                DbValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                DbValue::Real(f)
            } else {
                DbValue::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => DbValue::Text(s.clone()),
        serde_json::Value::Bool(b) => DbValue::Integer(if *b { 1 } else { 0 }),
        other => DbValue::Text(other.to_string()),
    }
}

// ============================================================================
// WasmDatabaseConnection — implements DatabaseConnection
// ============================================================================

#[derive(Clone, Default)]
pub struct WasmDatabaseConnection;

impl WasmDatabaseConnection {
    pub fn new() -> Self {
        Self
    }
}

impl DatabaseConnection for WasmDatabaseConnection {
    fn execute(&self, sql: &str, _params: &[DbValue]) -> Result<usize> {
        let rows_affected = js_bridge_execute(sql);
        Ok(rows_affected as usize)
    }

    fn last_insert_rowid(&self) -> Result<i64> {
        let result = js_bridge_sql("SELECT last_insert_rowid()");
        if let Some(parsed) = parse_bridge_result(&result) {
            if let Some(row) = parsed.rows.first() {
                if let Some(val) = row.first() {
                    if let Some(i) = val.as_i64() {
                        return Ok(i);
                    }
                }
            }
        }
        Ok(0)
    }

    fn query_row_values(&self, sql: &str, _params: &[DbValue]) -> Result<Option<Vec<DbValue>>> {
        let result = js_bridge_sql(sql);
        match parse_bridge_result(&result) {
            Some(parsed) if !parsed.rows.is_empty() => {
                let row = &parsed.rows[0];
                Ok(Some(row.iter().map(json_value_to_db_value).collect()))
            }
            _ => Ok(None),
        }
    }

    fn query_all_string_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let result = js_bridge_sql(sql);
        match parse_bridge_result(&result) {
            Some(parsed) => {
                let rows = parsed
                    .rows
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|v| json_value_to_string(v).unwrap_or_else(|| "NULL".to_string()))
                            .collect()
                    })
                    .collect();
                Ok((parsed.columns, rows))
            }
            None => Ok((vec![], vec![])),
        }
    }

    fn query_all_nullable_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<Option<String>>>)> {
        let result = js_bridge_sql(sql);
        match parse_bridge_result(&result) {
            Some(parsed) => {
                let rows = parsed
                    .rows
                    .iter()
                    .map(|row| row.iter().map(json_value_to_string).collect())
                    .collect();
                Ok((parsed.columns, rows))
            }
            None => Ok((vec![], vec![])),
        }
    }
}

// ============================================================================
// WasmIntrospector — implements DatabaseIntrospector (stub, MVP)
// ============================================================================

pub struct WasmIntrospector;

impl delightql_types::introspect::DatabaseIntrospector for WasmIntrospector {
    fn introspect_entities(
        &self,
    ) -> std::result::Result<Vec<delightql_types::introspect::DiscoveredEntity>, DelightQLError>
    {
        Ok(vec![])
    }

    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> std::result::Result<Vec<delightql_types::introspect::DiscoveredEntity>, DelightQLError>
    {
        Ok(vec![])
    }
}

// ============================================================================
// WasmParty — implements Handler (eager execution, no threads)
// ============================================================================

struct BufferedCursor {
    columns: Vec<String>,
    rows: VecDeque<Vec<Option<String>>>,
}

pub struct WasmParty {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
    handles: HashMap<Handle, BufferedCursor>,
    next_handle_id: u64,
}

impl WasmParty {
    pub fn new(connection: Arc<Mutex<dyn DatabaseConnection>>) -> Self {
        WasmParty {
            connection,
            handles: HashMap::new(),
            next_handle_id: 1,
        }
    }

    fn handle_query(&mut self, text: ByteSeq) -> ServerTerm {
        let sql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(e) => {
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: vec![],
                    message: format!("invalid UTF-8: {}", e).into_bytes(),
                }
            }
        };

        let conn = self.connection.lock().unwrap();

        let (columns, rows) = match conn.query_all_nullable_rows(&sql, &[]) {
            Ok((cols, rows)) if !cols.is_empty() => (cols, VecDeque::from(rows)),
            Ok(_) | Err(_) => match conn.execute(&sql, &[]) {
                Ok(affected) => {
                    let mut rows = VecDeque::new();
                    rows.push_back(vec![Some(affected.to_string())]);
                    (vec!["affected_rows".to_string()], rows)
                }
                Err(e) => {
                    return ServerTerm::Error {
                        kind: ErrorKind::Syntax,
                        identity: vec![],
                        message: format!("{}", e).into_bytes(),
                    }
                }
            },
        };

        let handle_id = self.next_handle_id;
        self.next_handle_id += 1;
        let handle: Handle = format!("wasm{}", handle_id).into_bytes();

        let dimensions: Vec<Dimension> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| Dimension {
                position: (i + 1) as u64,
                name: name.as_bytes().to_vec(),
                descriptor: Vec::new(),
            })
            .collect();

        self.handles.insert(
            handle.clone(),
            BufferedCursor { columns, rows },
        );

        ServerTerm::Header { handle, dimensions }
    }

    fn handle_fetch(
        &mut self,
        handle: Handle,
        projection: Projection,
        count: u64,
        orientation: Orientation,
    ) -> ServerTerm {
        let state = match self.handles.get_mut(&handle) {
            Some(s) => s,
            None => {
                return ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"unknown handle".to_vec(),
                }
            }
        };

        let count = count as usize;
        let n = std::cmp::min(count, state.rows.len());
        if n == 0 {
            return ServerTerm::End;
        }

        let rows: Vec<Vec<Option<String>>> = state.rows.drain(..n).collect();
        let col_indices = resolve_projection(&projection, &state.columns);

        let cells: Vec<Vec<Cell>> = match orientation {
            Orientation::Rows => rows
                .iter()
                .map(|row| {
                    col_indices
                        .iter()
                        .map(|&ci| {
                            row[ci].as_ref().map(|s| {
                                let mut v = vec![CELL_TAG_TEXT];
                                v.extend_from_slice(s.as_bytes());
                                v
                            })
                        })
                        .collect()
                })
                .collect(),
            Orientation::Columns => {
                return ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"orientation Columns not supported".to_vec(),
                }
            }
        };

        ServerTerm::Data { cells }
    }

    fn handle_stat(&self, handle: Handle) -> ServerTerm {
        if !self.handles.contains_key(&handle) {
            return ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b"unknown handle".to_vec(),
            };
        }
        ServerTerm::Metadata {
            items: vec![MetaItem::Backend(
                b"wasm".to_vec(),
                b"wasm-party".to_vec(),
            )],
        }
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        if self.handles.remove(&handle).is_some() {
            ServerTerm::Ok { count_hint: 0 }
        } else {
            ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b"unknown handle".to_vec(),
            }
        }
    }
}

impl Handler for WasmParty {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        match term {
            ClientTerm::Version {
                max_message_size,
                protocol_version,
                lease_ms,
                orientations,
            } => {
                let supported = vec![Orientation::Rows];
                let agreed: Vec<Orientation> = orientations
                    .iter()
                    .copied()
                    .filter(|o| supported.contains(o))
                    .collect();
                if agreed.is_empty() {
                    ServerTerm::Error {
                        kind: ErrorKind::Connection,
                        identity: vec![],
                        message: b"no common orientation".to_vec(),
                    }
                } else {
                    ServerTerm::Version {
                        max_message_size,
                        protocol_version,
                        lease_ms,
                        orientations: agreed,
                    }
                }
            }

            ClientTerm::Query { text } => self.handle_query(text),

            ClientTerm::Fetch {
                handle,
                projection,
                count,
                orientation,
            } => self.handle_fetch(handle, projection, count, orientation),

            ClientTerm::Stat { handle } => self.handle_stat(handle),

            ClientTerm::Close { handle } => self.handle_close(handle),

            ClientTerm::Prepare { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Prepare not implemented in WasmParty".to_vec(),
            },

            ClientTerm::Offer { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Offer not implemented in WasmParty".to_vec(),
            },
        }
    }
}

// ============================================================================
// WasmConnectionFactory — implements api::ConnectionFactory
// ============================================================================

pub struct WasmConnectionFactory;

impl api::ConnectionFactory for WasmConnectionFactory {
    fn create(
        &self,
        _uri: &str,
    ) -> std::result::Result<api::CreatedConnection, Box<dyn std::error::Error + Send + Sync>>
    {
        let conn = WasmDatabaseConnection::new();
        let arc: Arc<Mutex<dyn DatabaseConnection>> = Arc::new(Mutex::new(conn));

        let handler: Box<dyn Handler + Send> = Box::new(WasmParty::new(arc.clone()));

        let handler_factory: Box<dyn Fn() -> Box<dyn Handler + Send> + Send + Sync> = {
            let arc = arc.clone();
            Box::new(move || Box::new(WasmParty::new(arc.clone())) as Box<dyn Handler + Send>)
        };

        let introspector = Box::new(WasmIntrospector);

        Ok(api::CreatedConnection {
            handler,
            handler_factory,
            connection: arc,
            introspector,
            db_type: "sqlite".to_string(),
        })
    }
}

// ============================================================================
// Global handle (thread_local for WASM — single-threaded)
// ============================================================================

thread_local! {
    static DQL_HANDLE: RefCell<Option<Box<dyn api::DqlHandle>>> = RefCell::new(None);
}

// ============================================================================
// WASM Entry Points
// ============================================================================

/// Initialize the DelightQL WASM module.
///
/// Must be called once before any queries. Creates the protocol stack
/// and bootstraps the DQL system.
#[wasm_bindgen]
pub fn init_delightql() -> std::result::Result<(), JsValue> {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();

    let factory = Box::new(WasmConnectionFactory);
    let handle = api::open(factory).map_err(|e| JsValue::from_str(&e))?;

    DQL_HANDLE.with(|h| {
        *h.borrow_mut() = Some(handle);
    });

    Ok(())
}

/// Execute a DelightQL query and return results as JSON.
///
/// Returns a JSON string: `{"columns": [...], "rows": [[...], ...]}`
#[wasm_bindgen]
pub fn execute_dql(query: &str) -> std::result::Result<String, JsValue> {
    DQL_HANDLE.with(|h| {
        let mut handle_ref = h.borrow_mut();
        let handle = handle_ref
            .as_mut()
            .ok_or_else(|| JsValue::from_str("Not initialized — call init_delightql() first"))?;

        let mut session = handle
            .session()
            .map_err(|e| JsValue::from_str(&e))?;

        let result = session.query(query).map_err(|e| JsValue::from_str(&e))?;

        let columns: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();

        // Fetch all rows
        let mut all_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        loop {
            let fetch = session
                .fetch(&result.handle, 1000)
                .map_err(|e| JsValue::from_str(&e))?;
            all_rows.extend(fetch.rows);
            if fetch.finished {
                break;
            }
        }

        let _ = session.close(result.handle);

        // Build JSON response
        let json_rows: Vec<Vec<serde_json::Value>> = all_rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        None => serde_json::Value::Null,
                        Some(bytes) => {
                            // Protocol cells are tagged: first byte is type tag
                            if bytes.is_empty() {
                                serde_json::Value::Null
                            } else {
                                let text = String::from_utf8_lossy(&bytes[1..]);
                                serde_json::Value::String(text.to_string())
                            }
                        }
                    })
                    .collect()
            })
            .collect();

        let response = serde_json::json!({
            "columns": columns,
            "rows": json_rows,
        });

        serde_json::to_string(&response)
            .map_err(|e| JsValue::from_str(&format!("JSON serialization error: {}", e)))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_value_to_db_value() {
        assert!(matches!(
            json_value_to_db_value(&serde_json::Value::Null),
            DbValue::Null
        ));
        assert!(matches!(
            json_value_to_db_value(&serde_json::json!(42)),
            DbValue::Integer(42)
        ));
        assert!(matches!(
            json_value_to_db_value(&serde_json::json!(3.14)),
            DbValue::Real(_)
        ));
        assert!(matches!(
            json_value_to_db_value(&serde_json::json!("hello")),
            DbValue::Text(_)
        ));
    }

    #[test]
    fn test_json_value_to_string() {
        assert_eq!(json_value_to_string(&serde_json::Value::Null), None);
        assert_eq!(
            json_value_to_string(&serde_json::json!(42)),
            Some("42".to_string())
        );
        assert_eq!(
            json_value_to_string(&serde_json::json!("hello")),
            Some("hello".to_string())
        );
    }
}
