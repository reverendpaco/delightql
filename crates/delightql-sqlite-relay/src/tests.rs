// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// SqlParty Integration Tests
//
// Each test builds a SELF-CONTAINED in-memory database (these tests
// once pointed at the retired test_suite/ fixture tree and silently
// rotted when it was deleted — never again), wraps it in SqlParty +
// DirectTransport + Client, does a version handshake to obtain a
// Session, and runs a protocol conversation with raw SQL (not DQL).

use std::sync::{Arc, Mutex};

use delightql_protocol::{
    Client, CloseResponse, DirectTransport, FetchResponse, Orientation, Projection,
    QueryResponse, Session, VersionResult,
};

use crate::SqlParty;

/// The fixture the retired core.db provided: users with 10 columns and
/// 15 rows, first row (1, 'John', ...).
fn fixture_conn() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE users (
             id INTEGER, first_name TEXT, last_name TEXT, email TEXT,
             age INTEGER, city TEXT, country TEXT, score REAL,
             active INTEGER, notes TEXT
         );",
    )
    .unwrap();
    let names = [
        "John", "Jane", "Ada", "Grace", "Alan", "Edsger", "Barbara", "Donald",
        "Tony", "Leslie", "Ken", "Dennis", "Bjarne", "Guido", "Anders",
    ];
    for (i, name) in names.iter().enumerate() {
        conn.execute(
            "INSERT INTO users VALUES (?1, ?2, 'X', 'x@example.com', 30,
                                       'Town', 'Land', 1.5, 1, NULL)",
            rusqlite::params![i as i64 + 1, name],
        )
        .unwrap();
    }
    conn
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn text_cell(s: &str) -> Option<Vec<u8>> {
    Some(s.as_bytes().to_vec())
}

fn int_cell(n: i64) -> Option<Vec<u8>> {
    Some(n.to_string().into_bytes())
}

fn null_cell() -> Option<Vec<u8>> {
    None
}

/// Decode a cell to its text string for assertion comparisons.
fn cell_text(cell: &Option<Vec<u8>>) -> String {
    match cell {
        Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
        None => "NULL".to_string(),
    }
}

fn make_sql_session() -> Session<DirectTransport<SqlParty>> {
    let conn = fixture_conn();
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

    // Fetch just one batch (10 of 15 rows)
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

// --- Test 10: descriptor fallback for undeclared columns ---
//
// sqlite reports decl_type only for real table columns; expressions,
// aggregates, and anonymous tables (SELECT 1 AS x) report none, which
// downstream renders as stringly JSON (ALPHA-CLI-UX-WORRIES #3 — and
// count(*) over a real table had the same disease). For undeclared
// columns the relay peeks the first row and uses the engine's own
// storage class as the declaration. NULL declares nothing.

#[test]
fn expression_descriptors_fall_back_to_storage_class() {
    let mut session = make_sql_session();
    let rows = session.agreed_orientation(Orientation::Rows).unwrap();

    let resp = session
        .query(b(
            "SELECT id, count(*) AS c, 1.5 AS f, 'x' AS t, NULL AS n FROM users",
        ))
        .unwrap();
    let handle = match resp {
        QueryResponse::Header {
            handle, dimensions, ..
        } => {
            assert_eq!(dimensions[0].descriptor, b("INTEGER")); // declared on the table
            assert_eq!(dimensions[1].descriptor, b("INTEGER")); // aggregate: storage class
            assert_eq!(dimensions[2].descriptor, b("REAL")); // literal: storage class
            assert_eq!(dimensions[3].descriptor, b("TEXT"));
            assert_eq!(dimensions[4].descriptor, b("")); // NULL declares nothing
            handle
        }
        QueryResponse::Error { message, .. } => {
            panic!(
                "expected Header, got Error: {}",
                String::from_utf8_lossy(&message)
            );
        }
    };

    // The peeked first row must still arrive as data — peeking must
    // not eat it.
    let resp = session
        .fetch(&handle, Projection::All, 10, rows)
        .unwrap();
    match resp {
        FetchResponse::Data { cells } => {
            assert_eq!(cells.len(), 1);
            assert_eq!(cell_text(&cells[0][1]), "15"); // count(*) over 15 users
        }
        other => panic!("expected Data, got {:?}", other),
    }
    session.close(handle).unwrap();
}
