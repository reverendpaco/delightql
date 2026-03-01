// SisoParty — Back-End Seam (Generic DatabaseConnection)
//
// Backed by Arc<Mutex<dyn DatabaseConnection>>. Eager execution,
// buffered fetch. No worker thread — query_all_string_rows loads the
// entire result set, then fetch drains from a VecDeque.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use delightql_protocol::{
    ByteSeq, Cell, ClientTerm, Dimension, ErrorKind, Handle, Handler, MetaItem, Orientation,
    Projection, ServerTerm, resolve_projection, CELL_TAG_TEXT,
};

use delightql_types::DatabaseConnection;

// --- BufferedCursor ---

struct BufferedCursor {
    columns: Vec<String>,
    rows: VecDeque<Vec<Option<String>>>,
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
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: vec![],
                    message: format!("invalid UTF-8: {}", e).into_bytes(),
                }
            }
        };

        let conn = self.connection.lock().unwrap();

        // Use query_all_nullable_rows to preserve NULL fidelity.
        // If it returns empty columns, fall back to execute for DML.
        let (columns, rows) = match conn.query_all_nullable_rows(&sql, &[]) {
            Ok((cols, rows)) if !cols.is_empty() => {
                (cols, VecDeque::from(rows))
            }
            Ok(_) | Err(_) => {
                // DML or not implemented — try execute
                match conn.execute(&sql, &[]) {
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

        // Drain up to count rows from buffer
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
                b"siso".to_vec(),
                b"siso-party".to_vec(),
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
                message: b"Prepare not implemented in SisoParty".to_vec(),
            },

            ClientTerm::Offer { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Offer not implemented in SisoParty".to_vec(),
            },
        }
    }
}
