// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// SisoParty — Back-End Seam (Generic DatabaseConnection)
//
// Backed by Arc<Mutex<dyn DatabaseConnection>>. Eager execution,
// buffered fetch. No worker thread — query_all_rows loads the entire
// result set as typed values, then fetch drains from a VecDeque.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use delightql_protocol::{
    resolve_projection, ByteSeq, Cell, ClientTerm, Dimension, Handle, Handler, MetaItem,
    Orientation, Projection, ServerTerm, WireError,
};

use delightql_types::diagnostic::{DelightQLError, Siso};
use delightql_types::DatabaseConnection;

fn error_term(diagnostic: DelightQLError) -> ServerTerm {
    ServerTerm::Error(WireError::of(&diagnostic))
}

// --- BufferedCursor ---

struct BufferedCursor {
    columns: Vec<String>,
    rows: VecDeque<Vec<Cell>>,
}

// --- SisoParty ---

pub struct SisoParty {
    connection: Arc<Mutex<dyn DatabaseConnection>>,
    handles: HashMap<Handle, BufferedCursor>,
    next_handle_id: u64,
}

impl SisoParty {
    pub fn new(connection: Arc<Mutex<dyn DatabaseConnection>>) -> Self {
        SisoParty {
            connection,
            handles: HashMap::new(),
            next_handle_id: 1,
        }
    }

    fn handle_query(&mut self, text: ByteSeq) -> ServerTerm {
        let sql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(e) => {
                return error_term(
                    Siso::ProtocolText {
                        message: format!("invalid UTF-8: {}", e),
                    }
                    .into(),
                )
            }
        };

        let conn = self.connection.lock().unwrap();

        // The connection answers in its own value vocabulary; each value
        // becomes a cell, so a null stays absent and a blob stays bytes.
        // If it returns empty columns, fall back to execute for DML.
        let (columns, rows) = match conn.query_all_rows(&sql, &[]) {
            Ok((cols, rows)) if !cols.is_empty() => (
                cols,
                rows.into_iter()
                    .map(|row| row.into_iter().map(|v| v.into_wire_bytes()).collect())
                    .collect(),
            ),
            Ok(_) | Err(_) => {
                // DML or not implemented — try execute
                match conn.execute(&sql, &[]) {
                    Ok(affected) => {
                        let mut rows = VecDeque::new();
                        rows.push_back(vec![Some(affected.to_string().into_bytes())]);
                        (vec!["affected_rows".to_string()], rows)
                    }
                    // The connection's own typed refusal crosses whole.
                    Err(e) => return error_term(e),
                }
            }
        };

        // Create handle
        let handle_id = self.next_handle_id;
        self.next_handle_id += 1;
        let handle: Handle = format!("siso{}", handle_id).into_bytes();

        let dimensions: Vec<Dimension> = columns
            .iter()
            .enumerate()
            .map(|(i, name)| Dimension {
                position: (i + 1) as u64,
                name: name.as_bytes().to_vec(),
                descriptor: b"TEXT".to_vec(),
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
            None => return error_term(Siso::UnknownHandle.into()),
        };

        let count = count as usize;

        // Drain up to count rows from buffer
        let n = std::cmp::min(count, state.rows.len());
        if n == 0 {
            return ServerTerm::End;
        }

        let rows: Vec<Vec<Cell>> = state.rows.drain(..n).collect();
        let col_indices = resolve_projection(&projection, &state.columns);

        let cells: Vec<Vec<Cell>> = match orientation {
            Orientation::Rows => rows
                .iter()
                .map(|row| col_indices.iter().map(|&ci| row[ci].clone()).collect())
                .collect(),
            Orientation::Columns => {
                return error_term(
                    Siso::Orientation {
                        message: "orientation Columns not supported".to_string(),
                    }
                    .into(),
                )
            }
        };

        ServerTerm::Data { cells }
    }

    fn handle_stat(&self, handle: Handle) -> ServerTerm {
        if !self.handles.contains_key(&handle) {
            return error_term(Siso::UnknownHandle.into());
        }
        ServerTerm::Metadata {
            items: vec![MetaItem::Backend(b"siso".to_vec(), b"siso-party".to_vec())],
        }
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        if self.handles.remove(&handle).is_some() {
            ServerTerm::Ok { count_hint: 0 }
        } else {
            error_term(Siso::UnknownHandle.into())
        }
    }
}

impl Handler for SisoParty {
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
                    error_term(
                        Siso::Orientation {
                            message: "no common orientation".to_string(),
                        }
                        .into(),
                    )
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

            ClientTerm::Prepare { .. } => error_term(
                Siso::Unimplemented {
                    message: "Prepare not implemented in SisoParty".to_string(),
                }
                .into(),
            ),

            ClientTerm::Offer { .. } => error_term(
                Siso::Unimplemented {
                    message: "Offer not implemented in SisoParty".to_string(),
                }
                .into(),
            ),
        }
    }
}
