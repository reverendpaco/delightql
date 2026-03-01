// SqlParty Integration Tests
//
// Each test creates a raw rusqlite connection to the test database,
// wraps it in SqlParty + DirectTransport + Client, does a version
// handshake to obtain a Session, and runs a protocol conversation
// with raw SQL (not DQL).

use std::sync::{Arc, Mutex};

use delightql_protocol::{
    Client, CloseResponse, DirectTransport, FetchResponse, Orientation, Projection,
    QueryResponse, Session, VersionResult,
    CELL_TAG_INTEGER, CELL_TAG_TEXT, decode_cell_to_text,
};

use crate::SqlParty;

fn test_db_path() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let workspace_root = std::path::Path::new(manifest_dir)
        .parent() // crates/
        .unwrap()
        .parent() // workspace root
        .unwrap();
    workspace_root
        .join("test_suite/side-effect-free/fixtures/core/core.db")
        .to_string_lossy()
        .to_string()
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn text_cell(s: &str) -> Option<Vec<u8>> {
    let mut v = vec![CELL_TAG_TEXT];
    v.extend_from_slice(s.as_bytes());
    Some(v)
}

fn int_cell(n: i64) -> Option<Vec<u8>> {
    let mut v = vec![CELL_TAG_INTEGER];
    v.extend_from_slice(&n.to_le_bytes());
    Some(v)
}

fn null_cell() -> Option<Vec<u8>> {
    None
}

/// Decode a cell to its text string for assertion comparisons.
fn cell_text(cell: &Option<Vec<u8>>) -> String {
    match cell {
        Some(bytes) => decode_cell_to_text(bytes),
        None => "NULL".to_string(),
    }
}

fn make_sql_session() -> Session<DirectTransport<SqlParty>> {
    let conn =
        rusqlite::Connection::open(test_db_path()).expect("failed to open test database");
    let adapter = SqlParty::new(Arc::new(Mutex::new(conn)));
    let transport = DirectTransport::new(adapter);
    let client = Client::new(transport);

    match client
        .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
        .expect("version handshake failed")
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected { message, .. } => {
            panic!(
                "version rejected: {}",
                String::from_utf8_lossy(&message)
            )
        }
    }
}

// --- Test 1: SELECT * FROM users ---

#[test]
fn raw_sql_select_star() {
    let mut session = make_sql_session();
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    let resp = session.query(b("SELECT * FROM users")).unwrap();
    let handle = match resp {
        QueryResponse::Header {
            handle, dimensions, ..
        } => {
            assert_eq!(dimensions.len(), 10);
            assert_eq!(dimensions[0].name, b("id"));
            assert_eq!(dimensions[1].name, b("first_name"));
            handle
        }
        QueryResponse::Error { message, .. } => {
            panic!(
                "expected Header, got Error: {}",
                String::from_utf8_lossy(&message)
            );
        }
    };

    // Fetch all rows (15 users in test db)
    let resp = session
        .fetch(&handle, Projection::All, 10000, rows)
        .unwrap();
    match resp {
        FetchResponse::Data { cells } => {
            assert_eq!(cells.len(), 15);
            assert_eq!(cell_text(&cells[0][0]), "1");
            assert_eq!(cell_text(&cells[0][1]), "John");
        }
        other => panic!("expected Data, got {:?}", other),
    }

    // Next fetch should be End
    let resp = session
        .fetch(&handle, Projection::All, 10000, rows)
        .unwrap();
    assert_eq!(resp, FetchResponse::End);

    let resp = session.close(handle).unwrap();
    assert_eq!(resp, CloseResponse::Ok);
}

// --- Test 2: Streaming batches ---

#[test]
fn raw_sql_streaming_batches() {
    let mut session = make_sql_session();
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    let resp = session.query(b("SELECT * FROM users")).unwrap();
    let handle = match resp {
        QueryResponse::Header { handle, .. } => handle,
        QueryResponse::Error { message, .. } => {
            panic!(
                "expected Header, got Error: {}",
                String::from_utf8_lossy(&message)
            );
        }
    };

    // Fetch 10 at a time: 15 users → 10, 5, End
    let mut total = 0;
    let mut batch_sizes = Vec::new();

    loop {
        let resp = session
            .fetch(&handle, Projection::All, 10, rows)
            .unwrap();
        match resp {
            FetchResponse::Data { cells } => {
                batch_sizes.push(cells.len());
                total += cells.len();
            }
            FetchResponse::End => break,
            FetchResponse::Error { message, .. } => {
                panic!(
                    "unexpected error: {}",
                    String::from_utf8_lossy(&message)
                );
            }
        }
    }

    assert_eq!(total, 15);
    assert_eq!(batch_sizes, vec![10, 5]);

    session.close(handle).unwrap();
}

// --- Test 3: NULL fidelity ---

#[test]
fn raw_sql_null_fidelity() {
    let mut session = make_sql_session();
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    let resp = session.query(b("SELECT NULL, 'hello'")).unwrap();
    let handle = match resp {
        QueryResponse::Header {
            handle, dimensions, ..
        } => {
            assert_eq!(dimensions.len(), 2);
            handle
        }
        QueryResponse::Error { message, .. } => {
            panic!(
                "expected Header, got Error: {}",
                String::from_utf8_lossy(&message)
            );
        }
    };

    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    match resp {
        FetchResponse::Data { cells } => {
            assert_eq!(cells.len(), 1);
            assert_eq!(cells[0][0], null_cell());
            assert_eq!(cells[0][1], text_cell("hello"));
        }
        other => panic!("expected Data, got {:?}", other),
    }

    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    assert_eq!(resp, FetchResponse::End);

    session.close(handle).unwrap();
}

// --- Test 4: SQL error ---

#[test]
fn raw_sql_error() {
    let mut session = make_sql_session();

    let resp = session.query(b("SELECT * FROM nonexistent_table")).unwrap();
    match resp {
        QueryResponse::Error { kind, .. } => {
            assert_eq!(kind, delightql_protocol::ErrorKind::Syntax);
        }
        QueryResponse::Header { .. } => {
            panic!("expected Error, got Header");
        }
    }
}

// --- Test 5: Close mid-stream (no leak) ---

#[test]
fn raw_sql_close_mid_stream() {
    let mut session = make_sql_session();
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    let resp = session.query(b("SELECT * FROM users")).unwrap();
    let handle = match resp {
        QueryResponse::Header { handle, .. } => handle,
        QueryResponse::Error { message, .. } => {
            panic!(
                "expected Header, got Error: {}",
                String::from_utf8_lossy(&message)
            );
        }
    };

    // Fetch just one batch (10 of 35 rows)
    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    match resp {
        FetchResponse::Data { cells } => {
            assert_eq!(cells.len(), 10);
        }
        other => panic!("expected Data, got {:?}", other),
    }

    // Close before exhausted — should succeed
    let resp = session.close(handle).unwrap();
    assert_eq!(resp, CloseResponse::Ok);
}

// --- Test 6: Empty result ---

#[test]
fn raw_sql_empty_result() {
    let mut session = make_sql_session();
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    let resp = session
        .query(b("SELECT * FROM users WHERE 1=0"))
        .unwrap();
    let handle = match resp {
        QueryResponse::Header {
            handle, dimensions, ..
        } => {
            assert_eq!(dimensions.len(), 10);
            handle
        }
        QueryResponse::Error { message, .. } => {
            panic!(
                "expected Header, got Error: {}",
                String::from_utf8_lossy(&message)
            );
        }
    };

    // Immediate End — no data
    let resp = session
        .fetch(&handle, Projection::All, 10000, rows)
        .unwrap();
    assert_eq!(resp, FetchResponse::End);

    session.close(handle).unwrap();
}

// --- Test 7: DML returns affected_rows relation ---

#[test]
fn raw_sql_dml_affected_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("dml_test.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER, name TEXT);
         INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');",
    )
    .unwrap();

    let adapter = SqlParty::new(Arc::new(Mutex::new(conn)));
    let transport = DirectTransport::new(adapter);
    let client = Client::new(transport);

    let mut session = match client
        .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
        .unwrap()
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected { message, .. } => {
            panic!("version rejected: {}", String::from_utf8_lossy(&message))
        }
    };
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    // DELETE 2 of 3 rows
    let resp = session
        .query(b("DELETE FROM t WHERE id > 1"))
        .unwrap();
    let handle = match resp {
        QueryResponse::Header {
            handle, dimensions, ..
        } => {
            assert_eq!(dimensions.len(), 1);
            assert_eq!(dimensions[0].name, b("affected_rows"));
            handle
        }
        QueryResponse::Error { message, .. } => {
            panic!(
                "expected Header, got Error: {}",
                String::from_utf8_lossy(&message)
            );
        }
    };

    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    match resp {
        FetchResponse::Data { cells } => {
            assert_eq!(cells.len(), 1);
            assert_eq!(cells[0][0], int_cell(2)); // deleted 2 rows
        }
        other => panic!("expected Data, got {:?}", other),
    }

    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    assert_eq!(resp, FetchResponse::End);

    session.close(handle).unwrap();
}

// --- Test 8: DML insert then verify ---

#[test]
fn raw_sql_dml_insert_then_select() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("insert_test.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch("CREATE TABLE t (id INTEGER, name TEXT);")
        .unwrap();

    let adapter = SqlParty::new(Arc::new(Mutex::new(conn)));
    let transport = DirectTransport::new(adapter);
    let client = Client::new(transport);

    let mut session = match client
        .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
        .unwrap()
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected { message, .. } => {
            panic!("version rejected: {}", String::from_utf8_lossy(&message))
        }
    };
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    // INSERT 3 rows
    let resp = session
        .query(b("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"))
        .unwrap();
    let handle = match resp {
        QueryResponse::Header {
            handle, dimensions, ..
        } => {
            assert_eq!(dimensions[0].name, b("affected_rows"));
            handle
        }
        QueryResponse::Error { message, .. } => {
            panic!("insert error: {}", String::from_utf8_lossy(&message));
        }
    };

    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    match resp {
        FetchResponse::Data { cells } => {
            assert_eq!(cells[0][0], int_cell(3)); // inserted 3 rows
        }
        other => panic!("expected Data, got {:?}", other),
    }
    session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    session.close(handle).unwrap();

    // Now SELECT to verify the rows are there
    let resp = session.query(b("SELECT * FROM t ORDER BY id")).unwrap();
    let handle = match resp {
        QueryResponse::Header { handle, .. } => handle,
        QueryResponse::Error { message, .. } => {
            panic!("select error: {}", String::from_utf8_lossy(&message));
        }
    };

    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    match resp {
        FetchResponse::Data { cells } => {
            assert_eq!(cells.len(), 3);
            assert_eq!(cells[0][0], int_cell(1));
            assert_eq!(cells[0][1], text_cell("a"));
            assert_eq!(cells[2][0], int_cell(3));
            assert_eq!(cells[2][1], text_cell("c"));
        }
        other => panic!("expected Data, got {:?}", other),
    }

    session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    session.close(handle).unwrap();
}

// --- Test 9: Columns orientation not agreed ---
//
// With typestate enforcement, the client can't even send a Columns fetch
// if Columns wasn't agreed in the version handshake. This test verifies
// that agreed_orientation() correctly rejects unagreed orientations.

#[test]
fn columns_orientation_not_agreed() {
    let session = make_sql_session();
    // Only Rows was agreed in version handshake
    assert!(session.agreed_orientation(Orientation::Rows).is_some());
    assert!(session.agreed_orientation(Orientation::Columns).is_none());
}
