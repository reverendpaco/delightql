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

use super::pump_tests::{
    fresh_system, observing_relay_over, plan, relay_over, shared_sqlite, statement, TestRelay,
};
use crate::pipeline::compiled_query::{PlanEntry, PlanStatement};

/// A shipped statement routed to a named connection: 1 = the bootstrap
/// store (eager), 2 = the session's own backend (streaming), >= 3 = an
/// imported connection (eager).
fn ship_on(sql: &str, connection_id: i64) -> PlanEntry {
    PlanEntry::ShippedStatement(PlanStatement {
        sql: sql.to_string(),
        connection_id: Some(connection_id),
        comment: None,
        naming: None,
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

/// Wire ingress is one act: a party's identity is admitted against the
/// declared tree before it is matched, carried, or forwarded.
mod ingress {
    use super::super::{admitted_bytes, judge_bytes};
    use crate::diagnostic::{selector, DiagnosticClass};

    #[test]
    fn a_declared_identity_is_admitted_and_matched_typed() {
        let occurrence = admitted_bytes(
            b"delightql-error://semantic/resolution/table",
            b"Table not found: t",
        );
        assert_eq!(
            occurrence.error_uri(),
            "delightql-error://semantic/resolution/table"
        );
        assert_eq!(occurrence.class(), DiagnosticClass::Syntax);
        assert_eq!(occurrence.to_string(), "Table not found: t");
        let family = selector(&["semantic"]).unwrap();
        let (matched, detail) = judge_bytes(
            &family,
            b"delightql-error://semantic/resolution/table",
            b"x",
        );
        assert!(matched, "{detail}");
        let native = admitted_bytes(b"delightql-error://target/sqlite/constraint/2067", b"dup");
        assert_eq!(native.class(), DiagnosticClass::Constraint);
        assert!(selector(&["target", "sqlite"])
            .unwrap()
            .matches(&native.id()));
    }

    #[test]
    fn an_undeclared_identity_is_a_protocol_violation_and_matches_no_family() {
        let occurrence = admitted_bytes(b"delightql-error://semantic/invented", b"peer text");
        assert_eq!(
            occurrence.error_uri(),
            "delightql-error://runtime/relay/protocol"
        );
        let family = selector(&["semantic"]).unwrap();
        let (matched, detail) = judge_bytes(&family, b"delightql-error://semantic/invented", b"x");
        assert!(!matched, "{detail}");
        // A tail the provider's code law contradicts is undeclared too.
        let (matched, _) = judge_bytes(
            &selector(&["target", "postgres"]).unwrap(),
            b"delightql-error://target/postgres/constraint/42P01",
            b"x",
        );
        assert!(!matched);
        // A party that named nothing is judged under runtime/bug, as always.
        let nameless = admitted_bytes(b"", b"engine text");
        assert_eq!(nameless.error_uri(), "delightql-error://runtime/bug");
        assert!(selector(&["runtime", "bug"])
            .unwrap()
            .matches(&nameless.id()));
    }
}

/// THE ELECTION LAW ON THE EAGER ROADS: a declared type wins; an undeclared
/// column takes the storage class of its first non-NULL value, whatever row
/// that is; a column with none declares nothing. NULL never elects, so a
/// NULL-leading column does not type as "". The cells are the values' own
/// wire bytes, carried with the dimensions they were elected from.
#[test]
fn eager_result_elects_descriptors_from_first_non_null_values() {
    use delightql_types::DbValue;
    let result = super::BufferedResult::elected(
        vec!["declared".into(), "x".into(), "y".into(), "empty".into()],
        vec![Some("VARCHAR".to_string()), None, None, None],
        vec![
            vec![
                DbValue::Integer(1),
                DbValue::Null,
                DbValue::Text("t".into()),
                DbValue::Null,
            ],
            vec![
                DbValue::Integer(2),
                DbValue::Real(2.5),
                DbValue::Null,
                DbValue::Null,
            ],
        ],
    );
    let descriptors: Vec<String> = result
        .dimensions
        .iter()
        .map(|d| String::from_utf8_lossy(&d.descriptor).into_owned())
        .collect();
    assert_eq!(descriptors, ["VARCHAR", "REAL", "TEXT", ""]);
    assert_eq!(result.names(), ["declared", "x", "y", "empty"]);
    assert_eq!(result.rows()[1][1].as_deref(), Some(b"2.5".as_slice()));
    assert_eq!(result.rows()[0][1], None);
}

/// A composed result states every descriptor beside its name; nothing is
/// elected from the cells.
#[test]
fn composed_result_states_its_descriptors() {
    let result = super::BufferedResult::composed(
        vec![
            ("success".to_string(), "TEXT"),
            ("n".to_string(), "INTEGER"),
        ],
        vec![vec![Some(b"1".to_vec()), Some(b"7".to_vec())]],
    );
    let descriptors: Vec<&[u8]> = result
        .dimensions
        .iter()
        .map(|d| d.descriptor.as_slice())
        .collect();
    assert_eq!(descriptors, [b"TEXT".as_slice(), b"INTEGER".as_slice()]);
}

/// What the relay answers a DQL query's Header with: each column's naming,
/// in position order. The handle is closed before returning.
fn namings(relay: &mut TestRelay<'_>, text: &str) -> Vec<delightql_protocol::Naming> {
    let term = relay.handle(statement(text));
    let ServerTerm::Header { handle, dimensions } = term else {
        panic!("{text}: expected Header, got {term:?}");
    };
    let _ = relay.handle(ClientTerm::Close { handle });
    dimensions.iter().map(|d| d.naming).collect()
}

/// A column's name is minted exactly where baptism drew it: an unnamed
/// value, an anonymous cell, and every authored name that lost a
/// collision. A name someone wrote and kept is authored. The session's own
/// backend (streaming) and the bootstrap store (eager) answer alike.
#[test]
fn a_header_marks_each_minted_name_on_every_road() {
    use delightql_protocol::Naming::{Authored, Minted};
    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);
    for (text, expected) in [
        ("_(a @ 1) |> (a, a + 1)", vec![Authored, Minted]),
        ("_(a @ 1) |> (a as b, a * 2 as c)", vec![Authored, Authored]),
        ("_(1, \"a\")", vec![Minted, Minted]),
        (
            "_(id, k @ 1, 1) as l, _(id, k @ 2, 1) as r, l.k = r.k",
            vec![Minted, Minted, Minted, Minted],
        ),
        (
            "_(a @ 1) |> (a, a + 1) ; _(a @ 2) |> (a, a + 1)",
            vec![Authored, Minted],
        ),
        (
            "sys::ns.namespace(*) |> (fq_name, 1 + 1)",
            vec![Authored, Minted],
        ),
    ] {
        assert_eq!(namings(&mut relay, text), expected, "{text}");
    }
}

/// A query's rows as text, `None` for SQL NULL. The handle is consumed.
fn text_rows(relay: &mut TestRelay<'_>, text: &str) -> Vec<Vec<Option<String>>> {
    let term = relay.handle(statement(text));
    fetch_cells(relay, term)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
                .collect()
        })
        .collect()
}

/// The error a query answers with, as text.
fn refusal(relay: &mut TestRelay<'_>, text: &str) -> String {
    match relay.handle(statement(text)) {
        ServerTerm::Error(error) => String::from_utf8_lossy(error.message()).into_owned(),
        other => panic!("{text}: expected a refusal, got {other:?}"),
    }
}

/// THE READER TAKES CANONICAL TEXT. A naked query is not a goal: the same
/// bytes unwrapped are read as canonical text — here a fact about `_` — and
/// never answered as a query. The wrap is the host's. Two queries typed
/// together stand behind the one marker the host wrote, and the refusal
/// says so.
#[test]
fn a_submission_is_canonical_text_and_the_wrap_is_the_host_s() {
    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);

    assert_eq!(text_rows(&mut relay, "_(x @ 7)"), [[Some("7".to_string())]]);
    match relay.handle(ClientTerm::Query {
        text: b"_(x @ 7)".to_vec(),
    }) {
        ServerTerm::Error(_) => {}
        other => panic!("a naked query is not answered as one, got {other:?}"),
    }

    let two = refusal(&mut relay, "_(x @ 1) |> (x)\n_(x @ 2) |> (x)");
    assert!(two.contains("found 2 queries"), "{two}");
}

/// What a submission of canonical text answers, as rows of text.
fn canonical_rows(relay: &mut TestRelay<'_>, text: &str) -> Vec<Vec<String>> {
    let term = relay.handle(ClientTerm::Query {
        text: text.as_bytes().to_vec(),
    });
    fetch_cells(relay, term)
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8_lossy(&cell.expect("no NULL here")).into_owned())
                .collect()
        })
        .collect()
}

/// A SUBMISSION OF DEFINITIONS is an unnamed block at the prompt: admitted
/// into `home`, answered with what it defined, and replacing an earlier
/// definition of the same subject. A block's named child lands under
/// `home` too. Definitions beside a query refuse rather than vanish, and a
/// block written before a query leads it.
#[test]
fn a_submission_of_definitions_lands_in_home() {
    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);

    assert_eq!(
        canonical_rows(
            &mut relay,
            "two(*) :- _(v @ 2)\ndouble:(x) :- x * 2\ntwo(*) :- _(v @ 20)\n\
             (~~ddl:\"aside\" one(*) :- _(v @ 1) ~~)\n",
        ),
        [["home", "two"], ["home", "double"], ["home::aside", "one"]]
    );
    assert_eq!(
        text_rows(&mut relay, "two(*) |> (double:(v) as w) |> #(w)"),
        [[Some("4".to_string())], [Some("40".to_string())]]
    );
    assert_eq!(
        text_rows(&mut relay, "home::aside.one(*)"),
        [[Some("1".to_string())]]
    );

    canonical_rows(&mut relay, "two(*) :- _(v @ 22)");
    assert_eq!(text_rows(&mut relay, "two(*)"), [[Some("22".to_string())]]);

    let beside = refusal(&mut relay, "two(*)\nthree(*) :- _(v @ 3)");
    assert!(beside.contains("beside its query"), "{beside}");
    assert!(
        matches!(relay.handle(statement("three(*)")), ServerTerm::Error(_)),
        "a refused submission defines nothing"
    );

    assert_eq!(
        canonical_rows(&mut relay, "(~~ddl four(*) :- _(v @ 4) ~~)\n?- four(*)"),
        [["4"]],
        "a block before the query leads it"
    );
}

/// An observing session admits no definitions: registering them is an
/// effect on the session.
#[test]
fn an_observing_session_refuses_a_submission_of_definitions() {
    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = observing_relay_over(&mut system, conn);
    match relay.handle(ClientTerm::Query {
        text: b"five(*) :- _(v @ 5)".to_vec(),
    }) {
        ServerTerm::Error(error) => assert!(
            String::from_utf8_lossy(error.message()).contains("this session observes"),
            "{}",
            String::from_utf8_lossy(error.message())
        ),
        other => panic!("an observation defines nothing, got {other:?}"),
    }
}

/// The host's boot row is what the session starts from; a session row
/// stands over it; a reset returns to the boot value.
#[test]
fn a_session_setting_stands_over_the_boot_value_until_reset() {
    use crate::api::ServerRelay;
    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);
    let settings = "sys::config.setting(*) |> (key, layer, value) |> #(layer)";

    // The test system states its base directory as none.
    assert_eq!(
        text_rows(&mut relay, settings),
        [[Some("base_directory".into()), Some("boot".into()), None]]
    );

    relay
        .set_session_setting(crate::settings::BASE_DIRECTORY, Some("/tmp/session"))
        .expect("a session may set its base directory");
    assert_eq!(
        text_rows(&mut relay, settings),
        [
            [Some("base_directory".into()), Some("boot".into()), None],
            [
                Some("base_directory".into()),
                Some("session".into()),
                Some("/tmp/session".into())
            ]
        ]
    );

    relay.handle_reset().expect("reset");
    assert_eq!(
        text_rows(&mut relay, settings),
        [[Some("base_directory".into()), Some("boot".into()), None]]
    );
}

/// With no base directory, a relative path is refused rather than resolved
/// against the process's own directory; an absolute path, or a relative one
/// under a session base, reads the file.
#[test]
fn a_relative_path_needs_a_base_directory() {
    use crate::api::ServerRelay;
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("defs.dql"), "f(*) :- _(a @ 1)\n").expect("write");
    let absolute = dir.path().join("defs.dql");

    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);

    let why = refusal(&mut relay, "consult!(\"defs.dql\", \"lib\")(*)");
    assert!(why.contains("stated no base directory"), "{why}");

    let consulted = format!("consult!(\"{}\", \"abs\")(*)", absolute.display());
    assert_eq!(text_rows(&mut relay, &consulted).len(), 1, "{consulted}");

    relay
        .set_session_setting(
            crate::settings::BASE_DIRECTORY,
            Some(dir.path().to_str().expect("utf-8 tempdir")),
        )
        .expect("session base");
    assert_eq!(
        text_rows(&mut relay, "consult!(\"defs.dql\", \"rel\")(*)").len(),
        1
    );
}

/// A session cannot state a relative base directory.
#[test]
fn a_session_base_directory_must_be_absolute() {
    use crate::api::ServerRelay;
    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);
    let refused = relay
        .set_session_setting(crate::settings::BASE_DIRECTORY, Some("relative/dir"))
        .expect_err("a relative base refuses");
    assert!(
        refused.to_string().contains("is not an absolute path"),
        "{refused}"
    );
}

/// The keys core declares are published as `sys::config.setting_key`, so a
/// host author can query what the contract asks of them rather than read
/// the source.
#[test]
fn the_declared_keys_are_published_as_setting_key() {
    let mut system = fresh_system();
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);
    let rows = text_rows(
        &mut relay,
        "sys::config.setting_key(*) |> (key, required, nullable, session, summary)",
    );
    let flags = |row: &Vec<Option<String>>| {
        row[..4]
            .iter()
            .map(|cell| cell.clone().unwrap_or_default())
            .collect::<Vec<_>>()
    };
    let summary = |key: &str| {
        rows.iter()
            .find(|row| row[0].as_deref() == Some(key))
            .and_then(|row| row[4].clone())
            .unwrap_or_default()
    };
    let mut declared: Vec<_> = rows.iter().map(flags).collect();
    declared.sort();
    assert_eq!(
        declared,
        [
            ["base_directory", "1", "1", "1"],
            ["dialect", "0", "0", "0"],
        ],
        "{rows:?}"
    );
    assert!(
        summary("base_directory").contains("relative file path"),
        "{rows:?}"
    );
    assert!(summary("dialect").contains("connection"), "{rows:?}");
}
