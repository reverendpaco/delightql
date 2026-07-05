// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Smoke test for the dql-fatboy-postgres binary: spawn the real fatboy,
//! talk the relay protocol over its stdin/stdout, run the full lifecycle
//! — the relay protocol crossing a process boundary into a foreign
//! engine. SKIPs loudly when the sweep container is down.

use std::process::{Command, Stdio};
use std::time::Duration;

use delightql_protocol::stdio::StdioTransport;
use delightql_protocol::{
    Client, FetchResponse, Orientation, Projection, QueryResponse, VersionResult,
};

fn pg_reachable() -> bool {
    std::net::TcpStream::connect_timeout(
        &"127.0.0.1:5433".parse().unwrap(),
        Duration::from_millis(500),
    )
    .is_ok()
}

#[test]
fn fatboy_full_lifecycle_over_stdio() {
    if !pg_reachable() {
        eprintln!(
            "SKIP: postgres not reachable at 127.0.0.1:5433; \
             start it with: ./new_test_suite/sweep.py postgres"
        );
        return;
    }

    // Spawn the fatboy and talk over its pipes. The transport owns the
    // child and reaps it on drop — no guard, no socket, no cleanup.
    let child = Command::new(env!("CARGO_BIN_EXE_dql-fatboy-postgres"))
        .arg("--database")
        .arg("dql_core")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn fatboy");
    let transport = StdioTransport::from_child(child).expect("stdio transport");

    // Version handshake.
    let client = Client::new(transport);
    let VersionResult::Accepted(mut session) = client
        .version(1_000_000, b"relay0".to_vec(), 0, vec![Orientation::Rows])
        .expect("handshake transport")
    else {
        panic!("handshake rejected")
    };
    let rows_o = session.agreed_orientation(Orientation::Rows).unwrap();

    // Query → Fetch → End → Close, across the process boundary.
    let QueryResponse::Header { handle, dimensions } =
        session.query(b"SELECT 1 AS x".to_vec()).expect("query transport")
    else {
        panic!("expected Header")
    };
    assert_eq!(dimensions[0].name, b"x".to_vec());
    assert_eq!(dimensions[0].descriptor, b"int4".to_vec());

    match session
        .fetch(&handle, Projection::All, 100, rows_o)
        .expect("fetch transport")
    {
        FetchResponse::Data { cells } => {
            assert_eq!(cells, vec![vec![Some(b"1".to_vec())]]);
        }
        other => panic!("expected Data, got {:?}", other),
    }
    assert!(matches!(
        session
            .fetch(&handle, Projection::All, 100, rows_o)
            .expect("fetch transport"),
        FetchResponse::End
    ));
    session.close(handle).expect("close transport");

    // Dropping the session drops the transport, which kills and reaps the
    // fatboy child — the stdio lifecycle, no Shutdown control op needed.
    drop(session);
}
