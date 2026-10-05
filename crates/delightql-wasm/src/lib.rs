// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
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

// dlmalloc with "global" feature sets itself as the module's one global allocator
// (more robust than wee_alloc for tree-sitter's memory management)

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use delightql_core::api;
use delightql_protocol::{
    resolve_projection, ByteSeq, Cell, ClientTerm, Dimension, Handle, Handler, MetaItem,
    Orientation, Projection, ServerTerm, WireError,
};
use delightql_types::diagnostic::Sqlite;
use delightql_types::{DatabaseConnection, DbValue, DelightQLError, Result};
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

// ============================================================================
// JavaScript Bridge (2 functions)
// ============================================================================

#[wasm_bindgen]
extern "C" {
    /// Execute SQL and return the structured browser-bridge response.
    /// Used for SELECT-like queries, PRAGMA, and sqlite_master lookups.
    #[wasm_bindgen(js_name = bridge_sql)]
    fn js_bridge_sql(sql: &str) -> JsValue;

    /// Execute a SQL statement (INSERT, UPDATE, DELETE, DDL).
    #[wasm_bindgen(js_name = bridge_execute)]
    fn js_bridge_execute(sql: &str) -> JsValue;

}

// ============================================================================
// JSON deserialization for bridge_sql results
// ============================================================================

#[derive(Deserialize)]
struct BridgeSqlResult {
    ok: bool,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<BridgeCell>>,
    #[serde(default)]
    changes: usize,
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum BridgeCell {
    Null,
    Integer(String),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

fn parse_bridge_result(js_val: &JsValue, operation: &str) -> Result<BridgeSqlResult> {
    if js_val.is_null() || js_val.is_undefined() {
        return Err(Sqlite::Engine {
            message: format!("browser SQLite bridge returned no response for {operation}"),
        }
        .into());
    }
    let json_str = js_sys::JSON::stringify(js_val)
        .map_err(|_| Sqlite::Engine {
            message: format!("browser SQLite bridge returned a non-serializable {operation} response"),
        })?
        .as_string()
        .ok_or_else(|| {
            DelightQLError::from(Sqlite::Engine {
                message: format!("browser SQLite bridge returned a malformed {operation} response"),
            })
        })?;
    let parsed: BridgeSqlResult = serde_json::from_str(&json_str).map_err(|error| {
        DelightQLError::from(Sqlite::Engine {
            message: format!("browser SQLite bridge returned malformed {operation} JSON: {error}"),
        })
    })?;
    if !parsed.ok {
        return Err(Sqlite::Engine {
            message: parsed
                .error
                .clone()
                .unwrap_or_else(|| format!("browser SQLite {operation} failed")),
        }
        .into());
    }
    Ok(parsed)
}

fn bridge_cell_to_db_value(val: &BridgeCell) -> Result<DbValue> {
    Ok(match val {
        BridgeCell::Null => DbValue::Null,
        BridgeCell::Integer(value) => DbValue::Integer(value.parse::<i64>().map_err(|error| {
            DelightQLError::from(Sqlite::Engine {
                message: format!(
                    "browser SQLite bridge returned invalid 64-bit integer '{value}': {error}"
                ),
            })
        })?),
        BridgeCell::Real(value) => DbValue::Real(*value),
        BridgeCell::Text(value) => DbValue::Text(value.clone()),
        BridgeCell::Blob(value) => DbValue::Blob(value.clone()),
    })
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
        Ok(parse_bridge_result(&js_bridge_execute(sql), "execute")?.changes)
    }

    fn last_insert_rowid(&self) -> Result<i64> {
        let result = js_bridge_sql("SELECT last_insert_rowid()");
        let parsed = parse_bridge_result(&result, "last_insert_rowid query")?;
        if let Some(cell) = parsed.rows.first().and_then(|row| row.first()) {
            if let DbValue::Integer(value) = bridge_cell_to_db_value(cell)? {
                return Ok(value);
            }
        }
        Err(Sqlite::Engine {
            message: "browser SQLite bridge returned no integer last_insert_rowid".to_string(),
        }
        .into())
    }

    fn query_row_values(&self, sql: &str, _params: &[DbValue]) -> Result<Option<Vec<DbValue>>> {
        let result = js_bridge_sql(sql);
        match parse_bridge_result(&result, "row query")? {
            parsed if !parsed.rows.is_empty() => {
                let row = &parsed.rows[0];
                Ok(Some(
                    row.iter()
                        .map(bridge_cell_to_db_value)
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            _ => Ok(None),
        }
    }

    fn query_all_rows(
        &self,
        sql: &str,
        _params: &[DbValue],
    ) -> Result<(Vec<String>, Vec<Vec<DbValue>>)> {
        let result = js_bridge_sql(sql);
        let parsed = parse_bridge_result(&result, "query")?;
        if parsed.columns.is_empty() {
            return Ok((
                vec!["affected_rows".to_string()],
                vec![vec![DbValue::Integer(parsed.changes as i64)]],
            ));
        }
        if let Some(row) = parsed
            .rows
            .iter()
            .find(|row| row.len() != parsed.columns.len())
        {
            return Err(Sqlite::Engine {
                message: format!(
                    "browser SQLite bridge returned a row with {} cells for {} columns",
                    row.len(),
                    parsed.columns.len()
                ),
            }
            .into());
        }
        let rows = parsed
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(bridge_cell_to_db_value)
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((parsed.columns, rows))
    }
}

// ============================================================================
// WasmIntrospector — browser-backed DatabaseIntrospector
// ============================================================================

pub struct WasmIntrospector;

fn quoted_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

impl WasmIntrospector {
    fn entities(
        &self,
        schema: Option<&str>,
    ) -> Result<Vec<delightql_types::introspect::DiscoveredEntity>> {
        use delightql_types::introspect::{DiscoveredAttribute, DiscoveredEntity};
        let connection = WasmDatabaseConnection::new();
        let master = schema
            .map(|schema| format!("{}.sqlite_master", quoted_identifier(schema)))
            .unwrap_or_else(|| "sqlite_master".to_string());
        let (_, rows) = connection.query_all_rows(
            &format!(
                "SELECT name, type FROM {master} WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ),
            &[],
        )?;
        let mut entities = Vec::with_capacity(rows.len());
        for row in rows {
            let name = row
                .first()
                .and_then(DbValue::as_wire_text)
                .ok_or_else(|| Sqlite::Engine {
                    message: "browser SQLite sqlite_master row omitted a text name".to_string(),
                })?;
            let kind = row
                .get(1)
                .and_then(DbValue::as_wire_text)
                .ok_or_else(|| Sqlite::Engine {
                    message: format!(
                        "browser SQLite sqlite_master row for {name} omitted a text kind"
                    ),
                })?;
            let pragma = match schema {
                Some(schema) => format!(
                    "PRAGMA {}.table_xinfo({})",
                    quoted_identifier(schema),
                    quoted_identifier(&name)
                ),
                None => format!("PRAGMA table_xinfo({})", quoted_identifier(&name)),
            };
            let (columns, cells) = connection.query_all_rows(&pragma, &[])?;
            let index = |column: &str| columns.iter().position(|name| name == column);
            let (Some(cid_index), Some(name_index), Some(type_index), Some(notnull_index)) = (
                index("cid"),
                index("name"),
                index("type"),
                index("notnull"),
            )
            else {
                return Err(Sqlite::Engine {
                    message: format!(
                        "browser SQLite table_xinfo for {name} omitted required columns"
                    ),
                }
                .into());
            };
            let mut attributes = Vec::with_capacity(cells.len());
            for row in cells {
                let required = |position: usize, field: &str| {
                    row.get(position)
                        .and_then(DbValue::as_wire_text)
                        .ok_or_else(|| Sqlite::Engine {
                            message: format!(
                                "browser SQLite table_xinfo for {name} has invalid {field}"
                            ),
                        })
                };
                let position = required(cid_index, "cid")?
                    .parse::<i32>()
                    .map_err(|_| Sqlite::Engine {
                        message: format!(
                            "browser SQLite table_xinfo for {name} has a non-integer cid"
                        ),
                    })?;
                if position < 0 {
                    return Err(Sqlite::Engine {
                        message: format!(
                            "browser SQLite table_xinfo for {name} has a negative cid"
                        ),
                    }
                    .into());
                }
                let column_name = required(name_index, "name")?;
                let data_type = required(type_index, "type")?;
                let notnull = required(notnull_index, "notnull")?;
                attributes.push(DiscoveredAttribute {
                    name: column_name.into(),
                    data_type,
                    position,
                    is_nullable: notnull != "1",
                });
            }
            entities.push(DiscoveredEntity {
                name: name.into(),
                entity_type_id: if kind == "view" { 11 } else { 10 },
                attributes,
            });
        }
        Ok(entities)
    }
}

/// `PRAGMA [schema.]name('argument')` over the bridge, with each answer
/// column found by name.
struct PragmaRows {
    columns: Vec<String>,
    rows: Vec<Vec<DbValue>>,
}

impl PragmaRows {
    fn read(schema: Option<&str>, name: &str, argument: &str) -> Result<Self> {
        let sql = match schema {
            Some(schema) => format!(
                "PRAGMA {}.{name}({})",
                quoted_identifier(schema),
                sqlite_literal(argument)
            ),
            None => format!("PRAGMA {name}({})", sqlite_literal(argument)),
        };
        let (columns, rows) = WasmDatabaseConnection::new().query_all_rows(&sql, &[])?;
        Ok(PragmaRows { columns, rows })
    }

    /// Each row's text in the named columns. An answer with no rows is
    /// absence, whatever heading came with it; columns are resolved only
    /// against rows that need them.
    fn texts(&self, wanted: &[&str]) -> Result<Vec<Vec<String>>> {
        if self.rows.is_empty() {
            return Ok(Vec::new());
        }
        let positions = wanted
            .iter()
            .map(|column| {
                self.columns
                    .iter()
                    .position(|name| name == column)
                    .ok_or_else(|| malformed_schema(column))
            })
            .collect::<Result<Vec<_>>>()?;
        self.rows
            .iter()
            .map(|row| {
                positions
                    .iter()
                    .zip(wanted)
                    .map(|(at, column)| {
                        row.get(*at)
                            .and_then(DbValue::as_wire_text)
                            .ok_or_else(|| malformed_schema(column))
                    })
                    .collect()
            })
            .collect()
    }
}

/// The stored table an unqualified name reads (the temp schema's, then
/// main's, then an attached one's): its schema, and whether it is declared
/// `WITHOUT ROWID`. A view, a virtual table and an absent name are none.
fn stored_table(schema: Option<&str>, relation_name: &str) -> Result<Option<(String, bool)>> {
    let listed = PragmaRows::read(schema, "table_list", relation_name)?;
    let rank = |s: &str| match s {
        "temp" => 0,
        "main" => 1,
        _ => 2,
    };
    Ok(listed
        .texts(&["schema", "type", "wr"])?
        .into_iter()
        .min_by_key(|row| rank(&row[0]))
        .filter(|row| matches!(row[1].as_str(), "table" | "shadow"))
        .map(|row| (row[0].clone(), row[2] != "0")))
}

impl delightql_types::introspect::DatabaseIntrospector for WasmIntrospector {
    fn introspect_entities(
        &self,
    ) -> std::result::Result<Vec<delightql_types::introspect::DiscoveredEntity>, DelightQLError>
    {
        self.entities(None)
    }

    fn introspect_entities_in_schema(
        &self,
        schema: &str,
    ) -> std::result::Result<Vec<delightql_types::introspect::DiscoveredEntity>, DelightQLError>
    {
        self.entities(Some(schema))
    }

    /// An ordinary or STRICT table guarantees its declared types; a virtual
    /// table and a view do not.
    fn storage_guarantees_declared_types(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<bool>> {
        let kinds = PragmaRows::read(schema, "table_list", relation_name)?.texts(&["type"])?;
        Ok(match kinds.as_slice() {
            [row] => match row[0].as_str() {
                "table" | "shadow" => Some(true),
                "virtual" | "view" => Some(false),
                _ => None,
            },
            _ => None,
        })
    }

    /// A stored table keeps its rowid unless declared `WITHOUT ROWID`; a
    /// column is the rowid itself when it is the one primary key column,
    /// declared exactly `INTEGER`, with no `pk` index of its own.
    fn stored_row_identity(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<delightql_types::introspect::StoredRowIdentity>> {
        use delightql_types::introspect::StoredRowIdentity;
        let Some((owner, without_rowid)) = stored_table(schema, relation_name)? else {
            return Ok(None);
        };
        let columns = PragmaRows::read(Some(&owner), "table_xinfo", relation_name)?
            .texts(&["name", "type", "pk"])?;
        let mut keys: Vec<(i64, String, String)> = Vec::new();
        let mut occupied: Vec<delightql_types::SqlIdentifier> = Vec::new();
        for row in &columns {
            let place: i64 = row[2].parse().map_err(|_| malformed_schema("pk"))?;
            if place > 0 {
                keys.push((place, row[0].clone(), row[1].clone()));
            }
            occupied.push(row[0].as_str().into());
        }
        keys.sort_by_key(|(place, _, _)| *place);
        if without_rowid {
            return Ok(Some(StoredRowIdentity::Unlocated {
                key: keys.iter().map(|(_, name, _)| name.as_str().into()).collect(),
            }));
        }
        let key_indexed = PragmaRows::read(Some(&owner), "index_list", relation_name)?
            .texts(&["origin"])?
            .iter()
            .any(|row| row[0] == "pk");
        let alias = match keys.as_slice() {
            [(_, name, declared)] if declared.eq_ignore_ascii_case("INTEGER") && !key_indexed => {
                Some(name.as_str().into())
            }
            _ => None,
        };
        Ok(Some(StoredRowIdentity::Located { alias, occupied }))
    }

    /// A stored table's generated columns: `table_xinfo`'s `hidden` is 2 for
    /// a virtual generated column and 3 for a stored one.
    fn computed_columns(
        &self,
        schema: Option<&str>,
        relation_name: &str,
    ) -> Result<Option<Vec<delightql_types::SqlIdentifier>>> {
        let Some((owner, _)) = stored_table(schema, relation_name)? else {
            return Ok(None);
        };
        Ok(Some(
            PragmaRows::read(Some(&owner), "table_xinfo", relation_name)?
                .texts(&["name", "hidden"])?
                .into_iter()
                .filter(|row| matches!(row[1].as_str(), "2" | "3"))
                .map(|row| row[0].as_str().into())
                .collect(),
        ))
    }
}

// ============================================================================
// WasmParty — implements Handler (eager execution, no threads)
// ============================================================================

struct BufferedCursor {
    columns: Vec<String>,
    rows: VecDeque<Vec<Cell>>,
}

pub struct WasmParty {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
    handles: HashMap<Handle, BufferedCursor>,
    next_handle_id: u64,
}

/// The one projection of a party diagnostic onto the wire.
fn wire(diagnostic: Sqlite) -> ServerTerm {
    ServerTerm::Error(WireError::of(&diagnostic.into()))
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
                return wire(Sqlite::ProtocolText {
                    message: format!("invalid UTF-8: {}", e),
                })
            }
        };

        let conn = match self.connection.lock() {
            Ok(connection) => connection,
            Err(error) => {
                return wire(Sqlite::Engine {
                    message: format!("browser SQLite connection lock was poisoned: {error}"),
                })
            }
        };

        let (columns, rows) = match conn.query_all_rows(&sql, &[]) {
            Ok((cols, rows)) => (
                cols,
                rows.into_iter()
                    .map(|row| row.into_iter().map(|v| v.into_wire_bytes()).collect())
                    .collect(),
            ),
            Err(e) => {
                return wire(Sqlite::Engine {
                    message: format!("{}", e),
                })
            }
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
                descriptor: b"TEXT".to_vec(),
                naming: delightql_protocol::Naming::Authored,
            })
            .collect();

        self.handles
            .insert(handle.clone(), BufferedCursor { columns, rows });

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
            None => return wire(Sqlite::UnknownHandle),
        };

        let count = count as usize;
        let n = std::cmp::min(count, state.rows.len());
        if n == 0 {
            return ServerTerm::End;
        }

        let rows: Vec<Vec<Cell>> = state.rows.drain(..n).collect();
        let col_indices = resolve_projection(&projection, &state.columns);

        let cells: Vec<Vec<Cell>> = match orientation {
            Orientation::Rows => {
                let mut projected = Vec::with_capacity(rows.len());
                for row in &rows {
                    let mut cells = Vec::with_capacity(col_indices.len());
                    for &column in &col_indices {
                        let Some(cell) = row.get(column) else {
                            return wire(Sqlite::ProtocolText {
                                message: format!(
                                    "cursor row has {} cells but projection requested column {}",
                                    row.len(),
                                    column
                                ),
                            });
                        };
                        cells.push(cell.clone());
                    }
                    projected.push(cells);
                }
                projected
            }
            Orientation::Columns => {
                return wire(Sqlite::Orientation {
                    message: "orientation Columns not supported".to_string(),
                })
            }
        };

        ServerTerm::Data { cells }
    }

    fn handle_stat(&self, handle: Handle) -> ServerTerm {
        if !self.handles.contains_key(&handle) {
            return wire(Sqlite::UnknownHandle);
        }
        ServerTerm::Metadata {
            items: vec![MetaItem::Backend(b"wasm".to_vec(), b"wasm-party".to_vec())],
        }
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        if self.handles.remove(&handle).is_some() {
            ServerTerm::Ok { count_hint: 0 }
        } else {
            wire(Sqlite::UnknownHandle)
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
                if let Some(refusal) = delightql_protocol::version_refusal(&protocol_version) {
                    return ServerTerm::Error(delightql_protocol::WireError::of(&refusal));
                }
                let supported = vec![Orientation::Rows];
                let agreed: Vec<Orientation> = orientations
                    .iter()
                    .copied()
                    .filter(|o| supported.contains(o))
                    .collect();
                if agreed.is_empty() {
                    wire(Sqlite::Orientation {
                        message: "no common orientation".to_string(),
                    })
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

            ClientTerm::Prepare { .. } => wire(Sqlite::Unimplemented {
                message: "Prepare not implemented in WasmParty".to_string(),
            }),

            ClientTerm::Offer { .. } => wire(Sqlite::Unimplemented {
                message: "Offer not implemented in WasmParty".to_string(),
            }),
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
    ) -> std::result::Result<api::CreatedConnection, delightql_types::DelightQLError> {
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
// The page's database, mounted as `main`
// ============================================================================

/// The locator the page's SQLite database is mounted under. The session
/// catalog starts with an empty `main`; this host fills it with one
/// `mount!`, as the CLI does for `--db`.
const PAGE_DATABASE_LOCATOR: &str = "delightql-browser://page";

fn sqlite_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn sqlite_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn malformed_schema(field: &str) -> DelightQLError {
    delightql_types::diagnostic::Runtime::General {
        message: "browser SQLite schema metadata is malformed".to_string(),
        details: format!("invalid or missing required column '{field}'"),
    }
    .into()
}

/// Column lookups over the page's SQLite, through the bridge.
struct BridgeSchema {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
}

impl delightql_types::DatabaseSchema for BridgeSchema {
    fn get_table_columns(
        &self,
        schema: Option<&str>,
        table_name: &str,
    ) -> Result<Option<Vec<delightql_types::ColumnInfo>>> {
        let sql = match schema {
            Some(schema) => format!(
                "PRAGMA {}.table_xinfo({})",
                sqlite_identifier(schema),
                sqlite_literal(table_name)
            ),
            None => format!("PRAGMA table_xinfo({})", sqlite_literal(table_name)),
        };
        let conn = self.connection.lock().map_err(|error| {
            delightql_types::diagnostic::Runtime::poisoned(
                "Failed to acquire browser schema connection",
                error.to_string(),
            )
        })?;
        let (columns, rows) = conn.query_all_rows(&sql, &[])?;
        if rows.is_empty() {
            return Ok(None);
        }
        let index = |name: &str| {
            columns
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| malformed_schema(name))
        };
        let cid_idx = index("cid")?;
        let name_idx = index("name")?;
        let type_idx = index("type")?;
        let notnull_idx = index("notnull")?;
        let cols = rows
            .iter()
            .map(|row| {
                let cid = row
                    .get(cid_idx)
                    .and_then(|value| value.as_wire_text())
                    .and_then(|value| value.parse::<i64>().ok())
                    .filter(|value| *value >= 0)
                    .ok_or_else(|| malformed_schema("cid"))?;
                let name = row
                    .get(name_idx)
                    .and_then(|value| value.as_wire_text())
                    .ok_or_else(|| malformed_schema("name"))?;
                let declared_type = row
                    .get(type_idx)
                    .and_then(|value| value.as_wire_text())
                    .ok_or_else(|| malformed_schema("type"))?;
                let notnull = row
                    .get(notnull_idx)
                    .and_then(|value| value.as_wire_text())
                    .and_then(|value| value.parse::<i64>().ok())
                    .ok_or_else(|| malformed_schema("notnull"))?;
                Ok(delightql_types::ColumnInfo {
                    name: name.into(),
                    nullable: notnull == 0,
                    position: (cid + 1) as usize,
                    declared_type: (!declared_type.is_empty()).then_some(declared_type),
                    interior: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(cols))
    }

    fn table_exists(&self, schema: Option<&str>, table_name: &str) -> Result<bool> {
        Ok(self.get_table_columns(schema, table_name)?.is_some())
    }
}

/// Answers the page-database locator with components over the bridge; the
/// browser has no other database to mount.
struct WasmMountFactory;

impl delightql_types::ConnectionFactory for WasmMountFactory {
    fn create(
        &self,
        uri: &str,
    ) -> std::result::Result<delightql_types::ConnectionComponents, DelightQLError> {
        if uri != PAGE_DATABASE_LOCATOR {
            return Err(delightql_types::diagnostic::Runtime::Unsupported {
                message: format!(
                    "cannot mount '{uri}': the browser mounts only its page database \
                     ({PAGE_DATABASE_LOCATOR})"
                ),
            }
            .into());
        }
        let connection: Arc<Mutex<dyn DatabaseConnection>> =
            Arc::new(Mutex::new(WasmDatabaseConnection::new()));
        Ok(delightql_types::ConnectionComponents {
            schema: Box::new(BridgeSchema {
                connection: connection.clone(),
            }),
            connection,
            introspector: Box::new(WasmIntrospector),
            db_type: "sqlite".to_string(),
            mechanism: "in-process".to_string(),
            identity: Some(PAGE_DATABASE_LOCATOR.to_string()),
            mounted_schema: None,
        })
    }
}

/// Run one prompt statement to completion and discard its rows.
fn run_to_completion(
    handle: &mut Box<dyn api::DqlHandle>,
    statement: &str,
) -> std::result::Result<(), JsValue> {
    let mut session = handle
        .session()
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    let result = session
        .query(&delightql_cst::prompt_wrap(statement))
        .map_err(|e| JsValue::from_str(&e.to_string()))?;
    loop {
        let fetch = session
            .fetch(&result.handle, 1000)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        if fetch.finished {
            break;
        }
    }
    let _ = session.close(result.handle);
    Ok(())
}

// ============================================================================
// Global handle (thread_local for WASM — single-threaded)
// ============================================================================

struct WasmState {
    handle: Option<Box<dyn api::DqlHandle>>,
    instance_id: String,
    initialization_count: u64,
    execution_count: u64,
}

impl Default for WasmState {
    fn default() -> Self {
        Self {
            handle: None,
            instance_id: format!(
                "wasm-{:x}-{:x}",
                js_sys::Date::now().to_bits(),
                (js_sys::Math::random() * u64::MAX as f64) as u64
            ),
            initialization_count: 0,
            execution_count: 0,
        }
    }
}

thread_local! {
    static DQL_STATE: RefCell<WasmState> = RefCell::new(WasmState::default());
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
    // A browser has no filesystem: it states that it has no base directory,
    // so a relative path refuses.
    let boot = api::BootSettings::new().state(api::BASE_DIRECTORY, None);
    let mut handle = api::open(factory, Some(Box::new(WasmMountFactory)), boot)
        .map_err(|e| JsValue::from_str(&api::ApiError::from(e).to_string()))?;
    run_to_completion(
        &mut handle,
        &format!("mount!(\"{PAGE_DATABASE_LOCATOR}\", \"main\")(*)"),
    )?;

    DQL_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.handle = Some(handle);
        state.initialization_count += 1;
    });

    Ok(())
}

/// Execute a DelightQL query and return results as JSON. `query` is what a
/// user typed at a prompt: this host writes the prompt wrap.
///
/// Returns a JSON string: `{"columns": [...], "minted": [...], "rows":
/// [[...], ...]}`. `minted[i]` is true when the compiler minted
/// `columns[i]`: its spelling moves between compilations, so a caller must
/// not key on it.
#[wasm_bindgen]
pub fn execute_dql(query: &str) -> std::result::Result<String, JsValue> {
    DQL_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.execution_count += 1;
        let handle = state
            .handle
            .as_mut()
            .ok_or_else(|| JsValue::from_str("Not initialized — call init_delightql() first"))?;

        let mut session = handle
            .session()
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let result = session
            .query(&delightql_cst::prompt_wrap(query))
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        let columns: Vec<String> = result.columns.iter().map(|c| c.name.clone()).collect();
        let minted: Vec<bool> = result
            .columns
            .iter()
            .map(|c| c.naming == delightql_core::api::Naming::Minted)
            .collect();

        let mut all_rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
        loop {
            let fetch = session
                .fetch(&result.handle, 1000)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;
            all_rows.extend(fetch.rows);
            if fetch.finished {
                break;
            }
        }

        let _ = session.close(result.handle);

        let json_rows: Vec<Vec<serde_json::Value>> = all_rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| match cell {
                        None => serde_json::Value::Null,
                        Some(bytes) => {
                            let text = String::from_utf8_lossy(bytes);
                            serde_json::Value::String(text.to_string())
                        }
                    })
                    .collect()
            })
            .collect();

        let response = serde_json::json!({
            "columns": columns,
            "minted": minted,
            "rows": json_rows,
        });

        serde_json::to_string(&response)
            .map_err(|e| JsValue::from_str(&format!("JSON serialization error: {}", e)))
    })
}

/// Reset the DelightQL session while retaining the current WebAssembly module
/// instance and its monotonic execution count.
#[wasm_bindgen]
pub fn reset_delightql() -> std::result::Result<(), JsValue> {
    init_delightql()
}

#[derive(Serialize)]
struct RuntimeIdentity<'a> {
    delightql_version: &'a str,
    delightql_build: &'a str,
    profile: &'a str,
    tree_sitter_runtime: &'a str,
    grammar_fingerprint: &'a str,
    instance_id: String,
    initialization_count: u64,
    execution_count: u64,
}

/// Product and module-lifetime identity consumed by the browser harness and
/// included in downloadable transcripts.
#[wasm_bindgen]
pub fn runtime_identity() -> std::result::Result<String, JsValue> {
    DQL_STATE.with(|state| {
        let state = state.borrow();
        serde_json::to_string(&RuntimeIdentity {
            delightql_version: env!("CARGO_PKG_VERSION"),
            delightql_build: option_env!("DQL_BUILD_IDENT").unwrap_or("unidentified"),
            profile: if cfg!(debug_assertions) { "dev" } else { "release" },
            tree_sitter_runtime: delightql_cst::PARSER_RUNTIME,
            grammar_fingerprint: delightql_cst::GRAMMAR_FINGERPRINT,
            instance_id: state.instance_id.clone(),
            initialization_count: state.initialization_count,
            execution_count: state.execution_count,
        })
        .map_err(|error| JsValue::from_str(&format!("identity serialization error: {error}")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_value_to_db_value() {
        assert!(matches!(
            bridge_cell_to_db_value(&BridgeCell::Null).unwrap(),
            DbValue::Null
        ));
        assert!(matches!(
            bridge_cell_to_db_value(&BridgeCell::Integer("42".to_string())).unwrap(),
            DbValue::Integer(42)
        ));
        assert!(matches!(
            bridge_cell_to_db_value(&BridgeCell::Real(3.14)).unwrap(),
            DbValue::Real(_)
        ));
        assert!(matches!(
            bridge_cell_to_db_value(&BridgeCell::Text("hello".to_string())).unwrap(),
            DbValue::Text(_)
        ));
        assert!(bridge_cell_to_db_value(&BridgeCell::Integer(
            "9223372036854775808".to_string()
        ))
        .is_err());
    }
}
