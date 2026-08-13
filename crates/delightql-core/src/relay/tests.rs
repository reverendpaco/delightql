// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Relay module unit tests.
//!
//! Full integration tests (real database, protocol stack) live in
//! crates/delightql-cli/tests/ because they depend on CLI infrastructure
//! (ConnectionManager, CliConnectionFactory, SqlParty).
//!
//! What a database answered must reach the wire unchanged, and the case
//! that proves it is a result holding SQL NULL beside the text whose
//! characters are `NULL`. There is one road per connection class —
//! streaming (the session's own), eager on the bootstrap store, eager on
//! an imported connection — and each is exercised here on the SAME values,
//! because the defect these pin was one road disagreeing with the others.

use std::sync::{Arc, Mutex};

use delightql_protocol::{Cell, ClientTerm, Handler, Orientation, Projection, ServerTerm};
use delightql_types::db_traits::{DatabaseConnection, DbValue};
use delightql_types::factory::ConnectionComponents;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockSchemaProvider;

use super::pump_tests::{fresh_system, plan, relay_over, shared_sqlite, TestRelay};
use crate::pipeline::compiled_query::{PlanEntry, PlanStatement};

/// A shipped statement routed to a named connection: 1 = the bootstrap
/// store (eager), 2 = the session's own backend (streaming), >= 3 = an
/// imported connection (eager).
fn ship_on(sql: &str, connection_id: i64) -> PlanEntry {
    PlanEntry::ShippedStatement(PlanStatement {
        sql: sql.to_string(),
        connection_id: Some(connection_id),
        comment: None,
    })
}

/// Drain a Header response as CELLS — no string conversion anywhere, so
/// what is asserted is what the wire carries.
fn fetch_cells(relay: &mut TestRelay<'_>, term: ServerTerm) -> Vec<Vec<Cell>> {
    let handle = match term {
        ServerTerm::Header { handle, .. } => handle,
        other => panic!("expected Header, got {:?}", other),
    };
    let mut rows = Vec::new();
    loop {
        match relay.handle(ClientTerm::Fetch {
            handle: handle.clone(),
            projection: Projection::All,
            count: u64::MAX,
            orientation: Orientation::Rows,
        }) {
            ServerTerm::Data { cells } => rows.extend(cells),
            ServerTerm::End => break,
            other => panic!("unexpected fetch response: {:?}", other),
        }
    }
    rows
}

/// One row holding every value kind a database cell can be, in an order
/// the assertions below name: absent, the text that spells absence, empty
/// text, an integer, a real, a blob whose bytes read as text, and a blob
/// that is not text at all.
const EVERY_KIND_SQL: &str = "SELECT NULL AS absent, 'NULL' AS text_null, '' AS empty, \
     7 AS whole, 0.5 AS fractional, CAST('NULL' AS BLOB) AS blob_text, \
     x'0001ff' AS blob_bytes";

fn every_kind_cells() -> Vec<Cell> {
    vec![
        None,
        Some(b"NULL".to_vec()),
        Some(Vec::new()),
        Some(b"7".to_vec()),
        Some(b"0.5".to_vec()),
        Some(b"NULL".to_vec()),
        Some(vec![0x00, 0x01, 0xff]),
    ]
}

#[test]
fn ordinary_route_carries_every_value_kind() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let response = relay.handle_plan(&plan(vec![ship_on(EVERY_KIND_SQL, 2)]));
    assert_eq!(fetch_cells(&mut relay, response), vec![every_kind_cells()]);
}

#[test]
fn bootstrap_route_carries_every_value_kind() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);

    let response = relay.handle_plan(&plan(vec![ship_on(EVERY_KIND_SQL, 1)]));
    assert_eq!(fetch_cells(&mut relay, response), vec![every_kind_cells()]);
}

#[test]
fn imported_route_carries_every_value_kind() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let connection_id = register_probe_connection(&mut system);
    let mut relay = relay_over(&mut system, conn);

    let response = relay.handle_plan(&plan(vec![ship_on(EVERY_KIND_SQL, connection_id)]));
    assert_eq!(fetch_cells(&mut relay, response), vec![every_kind_cells()]);
}

/// An absent cell and a cell spelling `NULL` are not equal — the property
/// the three route pins above each depend on, said once on its own so a
/// failure reads as what it is.
#[test]
fn absence_is_not_the_text_that_spells_it() {
    let absent: Cell = DbValue::Null.into_wire_bytes();
    let spelled: Cell = DbValue::Text("NULL".to_string()).into_wire_bytes();
    assert_eq!(absent, None);
    assert_eq!(spelled, Some(b"NULL".to_vec()));
    assert_ne!(absent, spelled);
}

// ---------------------------------------------------------------------
// An imported connection: answers one canned row of typed values, the
// way a fatboy or coprocess connection answers its own engine.
// ---------------------------------------------------------------------

struct ProbeConnection;

impl DatabaseConnection for ProbeConnection {
    fn execute(&self, _sql: &str, _params: &[DbValue]) -> delightql_types::Result<usize> {
        Ok(0)
    }

    fn last_insert_rowid(&self) -> delightql_types::Result<i64> {
        Ok(0)
    }

    fn query_row_values(
        &self,
        _sql: &str,
        _params: &[DbValue],
    ) -> delightql_types::Result<Option<Vec<DbValue>>> {
        Ok(None)
    }

    fn query_all_rows(
        &self,
        _sql: &str,
        _params: &[DbValue],
    ) -> delightql_types::Result<(Vec<String>, Vec<Vec<DbValue>>)> {
        let columns = [
            "absent",
            "text_null",
            "empty",
            "whole",
            "fractional",
            "blob_text",
            "blob_bytes",
        ]
        .iter()
        .map(|c| c.to_string())
        .collect();
        let row = vec![
            DbValue::Null,
            DbValue::Text("NULL".to_string()),
            DbValue::Text(String::new()),
            DbValue::Integer(7),
            DbValue::Real(0.5),
            DbValue::Blob(b"NULL".to_vec()),
            DbValue::Blob(vec![0x00, 0x01, 0xff]),
        ];
        Ok((columns, vec![row]))
    }
}

struct NoEntities;

impl DatabaseIntrospector for NoEntities {
    fn introspect_entities(&self) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
    fn introspect_entities_in_schema(
        &self,
        _schema: &str,
    ) -> delightql_types::Result<Vec<DiscoveredEntity>> {
        Ok(vec![])
    }
}

fn register_probe_connection(system: &mut crate::system::DelightQLSystem) -> i64 {
    let components = ConnectionComponents {
        connection: Arc::new(Mutex::new(ProbeConnection)),
        schema: Box::new(MockSchemaProvider::new()),
        introspector: Box::new(NoEntities),
        db_type: "sqlite".to_string(),
        mechanism: "in-process".to_string(),
        identity: None,
        mounted_schema: None,
    };
    let (connection_id, _entities) = system
        .register_external_connection(components, "data::probe", "mock://probe")
        .expect("an imported connection registers");
    assert!(
        connection_id >= 3,
        "an imported connection is neither bootstrap (1) nor the session's own (2)"
    );
    connection_id
}
