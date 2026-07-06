// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// SqlParty — Back-End Seam (Epoch 6)
//
// Receives raw SQL, executes it against rusqlite with a live cursor,
// and streams results in batches through protocol terms. No full
// materialization — a worker thread owns the cursor and sends batches
// through a bounded channel.
//
// The self-referential struct problem (Statement<'conn> borrows Connection,
// Rows<'stmt> borrows Statement) is solved by spawning one worker thread
// per query. The thread owns the MutexGuard, Statement, and Rows iterator.
// The main thread holds the Receiver.

use std::collections::{HashMap, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use delightql_protocol::{
    ByteSeq, Cell, ClientTerm, Dimension, ErrorKind, Handle, Handler, MetaItem, Orientation,
    Projection, ServerTerm, resolve_projection,
};

pub mod siso;

#[cfg(test)]
mod tests;

const BATCH_SIZE: usize = 1024;

// --- StreamBatch ---

enum StreamBatch {
    Rows(Vec<Vec<Cell>>),
    Done,
    Error(String),
}

// --- CursorState ---

struct CursorState {
    columns: Vec<String>,
    buffer: VecDeque<Vec<Cell>>,
    receiver: mpsc::Receiver<StreamBatch>,
    exhausted: bool,
}

// --- SqlParty ---

pub struct SqlParty {
    connection: Arc<Mutex<rusqlite::Connection>>,
    handles: HashMap<Handle, CursorState>,
    next_handle_id: u64,
}

/// Convert a sqlite value to a wire cell plus the engine's storage class
/// ("" for NULL — a null declares nothing about its column).
fn value_to_cell(val: rusqlite::types::Value) -> (Cell, &'static str) {
    match val {
        rusqlite::types::Value::Null => (None, ""),
        rusqlite::types::Value::Integer(n) => (Some(n.to_string().into_bytes()), "INTEGER"),
        rusqlite::types::Value::Real(f) => (Some(f.to_string().into_bytes()), "REAL"),
        rusqlite::types::Value::Text(s) => (Some(s.into_bytes()), "TEXT"),
        rusqlite::types::Value::Blob(b) => (Some(b), "BLOB"),
    }
}

impl SqlParty {
    pub fn new(connection: Arc<Mutex<rusqlite::Connection>>) -> Self {
        SqlParty {
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

        // Bounded channel: 2 batches of backpressure
        let (tx, rx) = mpsc::sync_channel::<StreamBatch>(2);
        // Oneshot for column metadata (sync handshake)
        let (col_tx, col_rx) =
            mpsc::sync_channel::<Result<(Vec<String>, Vec<String>), String>>(1);

        let conn = Arc::clone(&self.connection);
        thread::spawn(move || {
            let guard = conn.lock().unwrap();
            let mut stmt = match guard.prepare(&sql) {
                Ok(s) => s,
                Err(e) => {
                    let _ = col_tx.send(Err(format!("{}", e)));
                    return;
                }
            };

            // DML (INSERT/UPDATE/DELETE without RETURNING) has column_count == 0.
            // Query (SELECT, or DML with RETURNING) has column_count > 0.
            if stmt.column_count() == 0 {
                // DML: execute and synthesize affected_rows relation
                match stmt.execute([]) {
                    Ok(_) => {
                        let affected = guard.changes();
                        if col_tx
                            .send(Ok((
                                vec!["affected_rows".to_string()],
                                vec!["INTEGER".to_string()],
                            )))
                            .is_err()
                        {
                            return;
                        }
                        let v = (affected as i64).to_string().into_bytes();
                        let _ = tx.send(StreamBatch::Rows(vec![vec![Some(v)]]));
                        let _ = tx.send(StreamBatch::Done);
                    }
                    Err(e) => {
                        let _ = col_tx.send(Err(format!("{}", e)));
                    }
                }
                return;
            }

            // Query: extract column names and declared types, then stream rows.
            let columns: Vec<String> =
                stmt.column_names().iter().map(|s| s.to_string()).collect();
            let mut declared_types: Vec<String> = (0..stmt.column_count())
                .map(|i| {
                    stmt.columns()
                        .get(i)
                        .and_then(|c| c.decl_type())
                        .unwrap_or("")
                        .to_string()
                })
                .collect();

            let mut rows = match stmt.query([]) {
                Ok(r) => r,
                Err(e) => {
                    let _ = col_tx.send(Err(format!("{}", e)));
                    return;
                }
            };

            // decl_type only exists for real table columns; expressions,
            // aggregates, and anonymous tables (SELECT 1 AS x) report
            // none, which downstream renders as stringly JSON. For those
            // columns, peek the first row BEFORE the column handshake and
            // let the engine's own storage class be the declaration —
            // sqlite typed the value; this is not a parsing heuristic. A
            // mixed column can make the first row unrepresentative, which
            // is safe: the CLI's round-trip guard demotes any cell that
            // does not match its descriptor back to a string.
            let mut pending: Option<Vec<Cell>> = match rows.next() {
                Ok(Some(row)) => {
                    let ncols = row.as_ref().column_count();
                    let mut cells: Vec<Cell> = Vec::with_capacity(ncols);
                    for i in 0..ncols {
                        let val: rusqlite::types::Value = row.get_unwrap(i);
                        let (cell, storage_class) = value_to_cell(val);
                        if declared_types[i].is_empty() && !storage_class.is_empty() {
                            declared_types[i] = storage_class.to_string();
                        }
                        cells.push(cell);
                    }
                    Some(cells)
                }
                Ok(None) => None,
                Err(e) => {
                    // First-step failure: headers go out, then the error —
                    // same shape a mid-stream step failure has always had.
                    if col_tx.send(Ok((columns, declared_types))).is_err() {
                        return;
                    }
                    let _ = tx.send(StreamBatch::Error(format!("{}", e)));
                    return;
                }
            };
            if col_tx.send(Ok((columns, declared_types))).is_err() {
                return; // receiver dropped
            }
            if pending.is_none() {
                let _ = tx.send(StreamBatch::Done);
                return;
            }

            loop {
                let mut batch = Vec::with_capacity(BATCH_SIZE);
                if let Some(first) = pending.take() {
                    batch.push(first);
                }

                loop {
                    match rows.next() {
                        Ok(Some(row)) => {
                            let ncols = row.as_ref().column_count();
                            let mut cells: Vec<Cell> = Vec::with_capacity(ncols);
                            for i in 0..ncols {
                                let val: rusqlite::types::Value = row.get_unwrap(i);
                                cells.push(value_to_cell(val).0);
                            }
                            batch.push(cells);
                            if batch.len() >= BATCH_SIZE {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let _ = tx.send(StreamBatch::Error(format!("{}", e)));
                            return;
                        }
                    }
                }

                if batch.is_empty() {
                    let _ = tx.send(StreamBatch::Done);
                    return;
                }

                let is_last = batch.len() < BATCH_SIZE;
                if tx.send(StreamBatch::Rows(batch)).is_err() {
                    return; // receiver dropped (Close)
                }
                if is_last {
                    let _ = tx.send(StreamBatch::Done);
                    return;
                }
            }
        });

        // Wait for column metadata from worker
        let (columns, declared_types) = match col_rx.recv() {
            Ok(Ok(meta)) => meta,
            Ok(Err(e)) => {
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: vec![],
                    message: e.into_bytes(),
                }
            }
            Err(_) => {
                return ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"worker thread died before sending columns".to_vec(),
                }
            }
        };

        // Create handle
        let handle_id = self.next_handle_id;
        self.next_handle_id += 1;
        let handle: Handle = format!("sql{}", handle_id).into_bytes();

        let dimensions: Vec<Dimension> = columns
            .iter()
            .zip(declared_types.iter())
            .enumerate()
            .map(|(i, (name, dtype))| Dimension {
                position: (i + 1) as u64,
                name: name.as_bytes().to_vec(),
                descriptor: dtype.as_bytes().to_vec(),
            })
            .collect();

        self.handles.insert(
            handle.clone(),
            CursorState {
                columns,
                buffer: VecDeque::new(),
                receiver: rx,
                exhausted: false,
            },
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

        // Fill buffer from channel until we have enough or stream is exhausted
        while state.buffer.len() < count && !state.exhausted {
            match state.receiver.recv() {
                Ok(StreamBatch::Rows(batch)) => {
                    for row in batch {
                        state.buffer.push_back(row);
                    }
                }
                Ok(StreamBatch::Done) => {
                    state.exhausted = true;
                }
                Ok(StreamBatch::Error(msg)) => {
                    return ServerTerm::Error {
                        kind: ErrorKind::Connection,
                        identity: vec![],
                        message: msg.into_bytes(),
                    };
                }
                Err(_) => {
                    // Channel closed unexpectedly
                    state.exhausted = true;
                }
            }
        }

        // Drain up to count rows from buffer
        let n = std::cmp::min(count, state.buffer.len());
        if n == 0 {
            return ServerTerm::End;
        }

        let rows: Vec<Vec<Cell>> = state.buffer.drain(..n).collect();
        let col_indices = resolve_projection(&projection, &state.columns);

        let cells: Vec<Vec<Cell>> = match orientation {
            Orientation::Rows => rows
                .iter()
                .map(|row| {
                    col_indices
                        .iter()
                        .map(|&ci| row[ci].clone())
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
                b"sqlite".to_vec(),
                b"sql-adapter".to_vec(),
            )],
        }
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        if self.handles.remove(&handle).is_some() {
            // Receiver dropped → worker thread's send() returns Err → thread exits
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

impl Handler for SqlParty {
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
                message: b"Prepare not implemented in SqlParty".to_vec(),
            },

            ClientTerm::Offer { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Offer not implemented in SqlParty".to_vec(),
            },
        }
    }
}
