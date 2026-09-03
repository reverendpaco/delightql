// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Pump integration tests (plan §3.2 acceptance): hand-constructed
//! `CompiledPlan`s — the 2.3 pattern, no effect transformer needed — played
//! by `RelayParty::handle_plan` against a REAL in-memory SQLite backend, so
//! transaction rollback is verified on actual data, not on a mock's call
//! log. The backend is a minimal eager protocol `Handler` over rusqlite
//! (the same seat SqlParty occupies in production); the test keeps an `Arc`
//! clone of the connection to inspect database state after each run.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use delightql_protocol::{
    Cell, Client, ClientTerm, Dimension, DirectTransport, ErrorKind, Handler, Orientation,
    Projection, ServerTerm, Session, VersionResult,
};

use super::RelayParty;
use crate::external_effects::CreatedObjectCatalog;
use crate::external_effects::CreatedObjectRegistration;
use crate::pipeline::compiled_query::{CompiledPlan, PlanEntry, PlanStatement};
use crate::relay::RelayHooks;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;

// ---------------------------------------------------------------------
// Test backend: an eager protocol Handler over a real rusqlite connection.
// ---------------------------------------------------------------------

pub(super) struct EagerSqliteHandler {
    conn: Arc<Mutex<rusqlite::Connection>>,
    buffers: HashMap<Vec<u8>, (Vec<Dimension>, Vec<Vec<Cell>>, usize)>,
    next_handle: u64,
    /// Every SQL text this backend was asked to run, in order — the
    /// E-T5 exit-peek-window pins read it (peeks are otherwise
    /// invisible: their errors are swallowed by design).
    sql_log: Arc<Mutex<Vec<String>>>,
}

impl EagerSqliteHandler {
    fn new(conn: Arc<Mutex<rusqlite::Connection>>) -> Self {
        EagerSqliteHandler {
            conn,
            buffers: HashMap::new(),
            next_handle: 1,
            sql_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn run_query(&mut self, sql: &str) -> ServerTerm {
        self.sql_log.lock().unwrap().push(sql.to_string());
        // All connection work happens in this scope so the guard (and the
        // statement borrowing it) are gone before `store` borrows self.
        let executed: Result<(Vec<Dimension>, Vec<Vec<Cell>>), String> = (|| {
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare(sql).map_err(|e| format!("{}", e))?;
            let col_count = stmt.column_count();
            if col_count == 0 {
                // DML/DDL/transaction control: execute, empty header.
                stmt.execute([]).map_err(|e| format!("{}", e))?;
                return Ok((vec![], vec![]));
            }
            let dimensions: Vec<Dimension> = stmt
                .column_names()
                .iter()
                .enumerate()
                .map(|(i, name)| Dimension {
                    position: i as u64,
                    name: name.as_bytes().to_vec(),
                    descriptor: b"TEXT".to_vec(),
                })
                .collect();
            let mapped = stmt
                .query_map([], |row| {
                    let mut cells: Vec<Cell> = Vec::with_capacity(col_count);
                    for idx in 0..col_count {
                        let val: rusqlite::types::Value = row.get(idx)?;
                        cells.push(match val {
                            rusqlite::types::Value::Null => None,
                            rusqlite::types::Value::Integer(i) => Some(i.to_string().into_bytes()),
                            rusqlite::types::Value::Real(f) => Some(f.to_string().into_bytes()),
                            rusqlite::types::Value::Text(s) => Some(s.into_bytes()),
                            rusqlite::types::Value::Blob(b) => Some(b),
                        });
                    }
                    Ok(cells)
                })
                .map_err(|e| format!("{}", e))?;
            let mut all_rows: Vec<Vec<Cell>> = Vec::new();
            for r in mapped {
                all_rows.push(r.map_err(|e| format!("{}", e))?);
            }
            Ok((dimensions, all_rows))
        })();

        match executed {
            Ok((dimensions, rows)) => self.store(dimensions, rows),
            Err(msg) => ServerTerm::Error {
                kind: ErrorKind::Constraint,
                identity: vec![],
                message: msg.into_bytes(),
            },
        }
    }

    fn store(&mut self, dimensions: Vec<Dimension>, rows: Vec<Vec<Cell>>) -> ServerTerm {
        let handle = format!("b{}", self.next_handle).into_bytes();
        self.next_handle += 1;
        self.buffers
            .insert(handle.clone(), (dimensions.clone(), rows, 0));
        ServerTerm::Header { handle, dimensions }
    }
}

impl Handler for EagerSqliteHandler {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        match term {
            ClientTerm::Version {
                max_message_size,
                protocol_version,
                lease_ms,
                orientations,
            } => ServerTerm::Version {
                max_message_size,
                protocol_version,
                lease_ms,
                orientations: orientations
                    .into_iter()
                    .filter(|o| *o == Orientation::Rows)
                    .collect(),
            },
            ClientTerm::Query { text } => {
                let sql = String::from_utf8_lossy(&text).to_string();
                self.run_query(&sql)
            }
            ClientTerm::Fetch { handle, .. } => match self.buffers.get_mut(&handle) {
                Some((_dims, rows, cursor)) => {
                    if *cursor >= rows.len() {
                        ServerTerm::End
                    } else {
                        let batch = rows[*cursor..].to_vec();
                        *cursor = rows.len();
                        ServerTerm::Data { cells: batch }
                    }
                }
                None => ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"unknown handle".to_vec(),
                },
            },
            ClientTerm::Close { handle } => {
                self.buffers.remove(&handle);
                ServerTerm::Ok { count_hint: 0 }
            }
            ClientTerm::Stat { .. } => ServerTerm::Metadata { items: vec![] },
            ClientTerm::Prepare { .. } | ClientTerm::Offer { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"not implemented".to_vec(),
            },
        }
    }
}

// ---------------------------------------------------------------------
// Fixture plumbing.
// ---------------------------------------------------------------------

/// Minimal introspector — the pump never compiles, so an empty user
/// catalog is correct (same shape as system.rs's seed tests).
struct EmptyIntrospector;
impl DatabaseIntrospector for EmptyIntrospector {
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

pub(super) fn fresh_system() -> DelightQLSystem {
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
        .expect("fresh in-memory system should build")
}

pub(super) type TestRelay<'a> = RelayParty<'a, DirectTransport<EagerSqliteHandler>>;

/// Build a relay whose default (connection 2) backend is `conn` — a REAL
/// rusqlite connection the caller keeps a clone of for state inspection.
pub(super) fn relay_over(
    system: &mut DelightQLSystem,
    conn: Arc<Mutex<rusqlite::Connection>>,
) -> TestRelay<'_> {
    relay_over_with_log(system, conn).0
}

/// `relay_over` plus a clone of the backend's SQL log (for the peek pins).
fn relay_over_with_log(
    system: &mut DelightQLSystem,
    conn: Arc<Mutex<rusqlite::Connection>>,
) -> (TestRelay<'_>, Arc<Mutex<Vec<String>>>) {
    let handler = EagerSqliteHandler::new(conn);
    let sql_log = Arc::clone(&handler.sql_log);
    let transport = DirectTransport::new(handler);
    let client = Client::new(transport);
    let session: Session<DirectTransport<EagerSqliteHandler>> = match client
        .version(
            1_000_000,
            b"relay0".to_vec(),
            300_000,
            vec![Orientation::Rows],
        )
        .expect("in-process handshake cannot fail at transport level")
    {
        VersionResult::Accepted(s) => s,
        VersionResult::Rejected { message, .. } => panic!(
            "test backend rejected version: {}",
            String::from_utf8_lossy(&message)
        ),
    };
    (RelayParty::new(system, session), sql_log)
}

pub(super) fn shared_sqlite() -> Arc<Mutex<rusqlite::Connection>> {
    Arc::new(Mutex::new(
        rusqlite::Connection::open_in_memory().expect("in-memory sqlite"),
    ))
}

#[test]
fn quarantined_session_refuses_a_new_query_before_compilation() {
    let mut system = fresh_system();
    system.quarantine_session("test operation", "uncertain cleanup");
    let conn = shared_sqlite();
    let mut relay = relay_over(&mut system, conn);

    let response = relay.handle(ClientTerm::Query {
        text: b"select 1".to_vec(),
    });
    match response {
        ServerTerm::Error {
            kind,
            identity,
            message,
        } => {
            assert_eq!(kind, ErrorKind::Connection);
            assert_eq!(
                identity,
                b"delightql-error://runtime/session_health/external_effect".to_vec()
            );
            assert!(String::from_utf8_lossy(&message).contains("reset or reconnect"));
        }
        other => panic!("quarantined query must be refused, got {other:?}"),
    }
}

#[test]
fn quarantined_session_allows_fetch_and_close_on_an_existing_handle() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let response = relay.handle_plan(&plan(vec![ship("SELECT 1 AS value")]));
    let handle = match response {
        ServerTerm::Header { handle, .. } => handle,
        other => panic!("expected a handle before quarantining, got {other:?}"),
    };
    relay
        .system
        .quarantine_session("test operation", "uncertain cleanup");

    match relay.handle(ClientTerm::Fetch {
        handle: handle.clone(),
        projection: Projection::All,
        count: u64::MAX,
        orientation: Orientation::Rows,
    }) {
        ServerTerm::Data { cells } => assert_eq!(cells.len(), 1),
        other => panic!("quarantine must not retract existing data: {other:?}"),
    }
    assert!(matches!(
        relay.handle(ClientTerm::Fetch {
            handle: handle.clone(),
            projection: Projection::All,
            count: u64::MAX,
            orientation: Orientation::Rows,
        }),
        ServerTerm::End
    ));
    assert!(matches!(
        relay.handle(ClientTerm::Close { handle }),
        ServerTerm::Ok { .. }
    ));
}

#[test]
fn post_run_unsupported_registration_is_a_quarantine_invariant_breach() {
    let conn = shared_sqlite();
    // Bypass the planner's pre-flight only to exercise the invariant-breach
    // branch: a target approved earlier must never abstain during read-back.
    let mut system = DelightQLSystem::new(
        Arc::new(Mutex::new(MockDatabaseConnection::new())),
        Box::new(EmptyIntrospector),
        "postgres",
    )
    .expect("fresh postgres system should build");
    let mut relay = relay_over(&mut system, Arc::clone(&conn));
    let p = CompiledPlan {
        entries: vec![ship("SELECT 1 AS value")],
        exit_probe_sql: None,
        created_objects: vec![crate::pipeline::compiled_query::PlanCreatedObject {
            name: "created".to_string(),
            is_view: false,
            connection_id: None,
        }],
        typed: None,
    };

    let response = relay.play_plan_with_catalog(&p, &CatalogShouldNotRun);
    let (identity, message) = error_message(response);
    assert_eq!(
        identity,
        b"delightql-error://runtime/session_health/external_effect".to_vec()
    );
    assert!(
        message.contains("session_health/registration_unsupported"),
        "the invariant breach URI remains in message data: {message}"
    );
    assert!(relay.system.require_healthy().is_err());
}

fn bare(sql: &str) -> PlanEntry {
    PlanEntry::Statement(PlanStatement::bare(sql))
}

fn ship(sql: &str) -> PlanEntry {
    PlanEntry::ShippedStatement(PlanStatement::bare(sql))
}

pub(super) fn plan(entries: Vec<PlanEntry>) -> CompiledPlan {
    CompiledPlan {
        entries,
        exit_probe_sql: None,
        created_objects: Vec::new(),
        typed: None,
    }
}

struct CatalogShouldNotRun;

impl CreatedObjectCatalog for CatalogShouldNotRun {
    fn reconcile(
        &self,
        _catalog: &rusqlite::Connection,
        _registrations: &[CreatedObjectRegistration],
    ) -> delightql_types::Result<()> {
        panic!("catalog reconciliation must not run after a failed target read-back")
    }
}

/// Drain a Header response through the relay's own Fetch handling
/// (exercises both the eager-buffer and the streaming path, whichever the
/// pump chose) and return (columns, rows) as strings.
fn fetch_all(relay: &mut TestRelay<'_>, term: ServerTerm) -> (Vec<String>, Vec<Vec<String>>) {
    let (handle, dimensions) = match term {
        ServerTerm::Header { handle, dimensions } => (handle, dimensions),
        other => panic!("expected Header, got {:?}", other),
    };
    let columns: Vec<String> = dimensions
        .iter()
        .map(|d| String::from_utf8_lossy(&d.name).to_string())
        .collect();
    let mut rows: Vec<Vec<String>> = Vec::new();
    loop {
        match relay.handle(ClientTerm::Fetch {
            handle: handle.clone(),
            projection: Projection::All,
            count: u64::MAX,
            orientation: Orientation::Rows,
        }) {
            ServerTerm::Data { cells } => {
                for row in cells {
                    rows.push(
                        row.into_iter()
                            .map(|c| match c {
                                Some(bytes) => String::from_utf8_lossy(&bytes).to_string(),
                                None => "NULL".to_string(),
                            })
                            .collect(),
                    );
                }
            }
            ServerTerm::End => break,
            other => panic!("expected Data/End, got {:?}", other),
        }
    }
    let _ = relay.handle(ClientTerm::Close { handle });
    (columns, rows)
}

fn error_message(term: ServerTerm) -> (Vec<u8>, String) {
    match term {
        ServerTerm::Error {
            identity, message, ..
        } => (identity, String::from_utf8_lossy(&message).to_string()),
        other => panic!("expected Error, got {:?}", other),
    }
}

fn count_rows(conn: &Arc<Mutex<rusqlite::Connection>>, table: &str) -> i64 {
    conn.lock()
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {}", table), [], |r| r.get(0))
        .unwrap()
}

// ---------------------------------------------------------------------
// The acceptance tests (plan §3.2).
// ---------------------------------------------------------------------

/// Entries execute first to last; the final shipped statement's result set
/// is the run's return value on the ordinary Query → Header cycle.
#[test]
fn plays_entries_in_order_and_returns_final_ship() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        bare("CREATE TABLE log (step TEXT)"),
        bare("INSERT INTO log VALUES ('a')"),
        bare("INSERT INTO log VALUES ('b')"),
        ship("SELECT group_concat(step, ',') AS steps FROM (SELECT step FROM log ORDER BY rowid)"),
    ]);
    let resp = relay.handle_plan(&p);
    let (columns, rows) = fetch_all(&mut relay, resp);
    assert_eq!(columns, vec!["steps"]);
    assert_eq!(rows, vec![vec!["a,b".to_string()]]);
}

#[test]
fn created_object_registration_failure_retires_the_unsent_final_handle() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));
    let p = CompiledPlan {
        entries: vec![ship("SELECT 1 AS value")],
        exit_probe_sql: None,
        created_objects: vec![crate::pipeline::compiled_query::PlanCreatedObject {
            name: "created".to_string(),
            is_view: false,
            connection_id: None,
        }],
        typed: None,
    };

    let response = relay.play_plan_with_catalog(&p, &CatalogShouldNotRun);
    let (identity, message) = error_message(response);
    assert_eq!(
        identity,
        b"delightql-error://runtime/session_health/external_effect".to_vec()
    );
    assert!(
        message.contains("created-object registration failed"),
        "{message}"
    );
    assert!(
        message.contains("delightql-error://"),
        "the primary failure URI is retained as message data: {message}"
    );
    assert!(
        relay.handles.is_empty(),
        "an unsent streaming handle is retired"
    );
    assert!(
        relay.eager_buffers.is_empty(),
        "an unsent eager handle is retired"
    );
}

/// Per-entry connection routing: entries carrying `Some(1)` execute on the
/// bootstrap connection, not the default backend. The table must exist on
/// the bootstrap side and must NOT exist on the backend connection.
#[test]
fn routes_entries_per_connection() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let on_bootstrap = |sql: &str| {
        PlanEntry::Statement(PlanStatement {
            sql: sql.to_string(),
            connection_id: Some(1),
            comment: None,
        })
    };
    let p = plan(vec![
        on_bootstrap("CREATE TABLE pump_route_probe (v TEXT)"),
        on_bootstrap("INSERT INTO pump_route_probe VALUES ('routed')"),
        PlanEntry::ShippedStatement(PlanStatement {
            sql: "SELECT v FROM pump_route_probe".to_string(),
            connection_id: Some(1),
            comment: None,
        }),
    ]);
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["routed".to_string()]]);
}

// ---------------------------------------------------------------------
// D3a — the typed walk: requirement edges sampled at the dependent;
// exit is an ordinary Absent edge; the peek window is retired (one
// pre-COMMIT latch read decides the post-COMMIT tail).
// ---------------------------------------------------------------------

use crate::pipeline::compiled_query::{
    AbortProvenance, EffectAction, EffectStep, GuardDefinition, GuardPolarity, Requirement,
    TerminalAction, TypedEffectPlan,
};

fn step(action: EffectAction, occurrence: &str, requirements: Vec<Requirement>) -> EffectStep {
    EffectStep {
        occurrence: occurrence.to_string(),
        operation: occurrence.to_string(),
        route: None,
        requirements,
        action,
    }
}

fn stmts(sqls: &[&str]) -> Vec<PlanStatement> {
    sqls.iter().map(|s| PlanStatement::bare(*s)).collect()
}

fn req(guard_id: usize, polarity: GuardPolarity) -> Requirement {
    Requirement {
        guard_id,
        polarity,
        reason: "comma",
    }
}

fn bracketed_typed_plan(
    body_steps: Vec<EffectStep>,
    guards: Vec<GuardDefinition>,
    exit_probe_sql: Option<&str>,
    cleanup: Vec<PlanStatement>,
) -> CompiledPlan {
    let mut steps = vec![step(
        EffectAction::Begin {
            connection_id: None,
        },
        "begin",
        vec![],
    )];
    steps.extend(body_steps);
    steps.push(step(
        EffectAction::Commit {
            connection_id: None,
        },
        "commit",
        vec![],
    ));
    if !cleanup.is_empty() {
        steps.push(step(EffectAction::Cleanup(cleanup), "cleanup", vec![]));
    }
    let typed = TypedEffectPlan { steps, guards };
    CompiledPlan {
        entries: typed.flatten(),
        exit_probe_sql: exit_probe_sql.map(str::to_string),
        created_objects: Vec::new(),
        typed: Some(typed),
    }
}

/// exit as an Absent edge (Q-D7): once the latch is written, every later
/// step's edge samples closed — data steps, non-final ships, and the
/// final ship alike. The run answers the empty header, and the
/// post-COMMIT tail is skipped by the pre-COMMIT latch read.
#[test]
fn exit_absent_edges_skip_later_steps_and_the_tail() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch(
            "CREATE TABLE __exit (hit INTEGER);
             CREATE TABLE t (v TEXT);
             CREATE TABLE cleanup_probe (v TEXT);",
        )
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let shipped = Arc::new(Mutex::new(0usize));
    let shipped_in_hook = Arc::clone(&shipped);
    relay.set_hooks(RelayHooks {
        on_ship: Some(Box::new(move |_cols, _rows| {
            *shipped_in_hook.lock().unwrap() += 1;
        })),
        ..RelayHooks::default()
    });

    let guards = vec![GuardDefinition {
        guard_id: 0,
        sql: "SELECT 1 FROM __exit".to_string(),
    }];
    let absent = || {
        vec![Requirement {
            guard_id: 0,
            polarity: GuardPolarity::Absent,
            reason: "exit",
        }]
    };
    let p = bracketed_typed_plan(
        vec![
            step(
                EffectAction::Terminal(TerminalAction::Exit {
                    statements: stmts(&["INSERT INTO __exit VALUES (1)"]),
                }),
                "exit!#0",
                vec![],
            ),
            step(
                EffectAction::Dml(stmts(&["INSERT INTO t VALUES ('must not happen')"])),
                "insert!#1",
                absent(),
            ),
            step(
                EffectAction::Host {
                    statements: vec![],
                    ship: PlanStatement::bare("SELECT v FROM t"),
                },
                "stdout!#2",
                absent(),
            ),
            step(
                EffectAction::Return {
                    statements: vec![],
                    ship: Some(PlanStatement::bare("SELECT count(*) AS n FROM t")),
                },
                "return!#3",
                absent(),
            ),
        ],
        guards,
        Some("SELECT count(*) FROM __exit"),
        stmts(&["INSERT INTO cleanup_probe VALUES ('tail')"]),
    );
    let resp = relay.handle_plan(&p);
    let (columns, rows) = fetch_all(&mut relay, resp);
    assert!(columns.is_empty(), "post-exit run answers the empty header");
    assert!(rows.is_empty());
    assert_eq!(
        count_rows(&conn, "t"),
        0,
        "data step after exit is declined"
    );
    assert_eq!(
        *shipped.lock().unwrap(),
        0,
        "ship step after exit is declined"
    );
    assert_eq!(
        count_rows(&conn, "cleanup_probe"),
        0,
        "the post-COMMIT tail is skipped by the pre-COMMIT latch read"
    );
}

/// The typed walk samples a step's Present edges at the DEPENDENT and
/// declines the whole statement stream when closed; an open edge lets
/// the stream run. The sample reads through the count(*) wrapper.
#[test]
fn typed_walk_declines_steps_with_closed_present_edges() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch(
            "CREATE TABLE gate (v TEXT);
             CREATE TABLE hit (v TEXT);
             CREATE TABLE miss (v TEXT);",
        )
        .unwrap();
    let mut system = fresh_system();
    let (mut relay, sql_log) = relay_over_with_log(&mut system, Arc::clone(&conn));

    let guards = vec![GuardDefinition {
        guard_id: 0,
        sql: "SELECT count(*) FROM (SELECT 1 WHERE EXISTS (SELECT 1 FROM gate)) AS guard_scope"
            .to_string(),
    }];
    let p = bracketed_typed_plan(
        vec![
            step(
                EffectAction::Dml(stmts(&["INSERT INTO miss VALUES ('closed')"])),
                "insert!#0",
                vec![req(0, GuardPolarity::Present)],
            ),
            step(
                EffectAction::Dml(stmts(&["INSERT INTO gate VALUES ('open')"])),
                "insert!#1",
                vec![],
            ),
            step(
                EffectAction::Dml(stmts(&["INSERT INTO hit VALUES ('reached')"])),
                "insert!#2",
                vec![req(0, GuardPolarity::Present)],
            ),
            step(
                EffectAction::Return {
                    statements: vec![],
                    ship: Some(PlanStatement::bare("SELECT count(*) AS n FROM hit")),
                },
                "return!#3",
                vec![],
            ),
        ],
        guards,
        None,
        vec![],
    );
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
    assert_eq!(
        count_rows(&conn, "miss"),
        0,
        "closed edge declines the step"
    );
    assert_eq!(count_rows(&conn, "hit"), 1, "open edge lets the step run");
    let log = sql_log.lock().unwrap();
    assert!(
        log.iter()
            .any(|sql| sql.contains("SELECT count(*) FROM (SELECT 1 WHERE EXISTS")),
        "edges sample through the count(*) wrapper; log:\n{:#?}",
        *log
    );
    assert!(
        !log.iter().any(|sql| sql.contains("miss")),
        "a declined step's statements never reach the backend; log:\n{:#?}",
        *log
    );
}

/// Fault ATTRIBUTION across the whole typed program.
/// A mid-step statement failure marks THAT step `error` (with the
/// message), every completed step `done` (control steps included), and
/// every unreached step `pending` — read back from the materialized
/// effect_run, because these executor state transitions are hard to
/// induce reliably through .dql.
#[test]
fn statement_failure_attributes_error_done_and_pending() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let outcomes = {
        let mut relay = relay_over(&mut system, Arc::clone(&conn));
        let p = bracketed_typed_plan(
            vec![
                step(
                    EffectAction::Dml(stmts(&["INSERT INTO t VALUES ('ok')"])),
                    "insert!#0",
                    vec![],
                ),
                step(
                    EffectAction::Dml(stmts(&["INSERT INTO nope_no_table VALUES (1)"])),
                    "insert!#1",
                    vec![],
                ),
                step(
                    EffectAction::Return {
                        statements: vec![],
                        ship: Some(PlanStatement::bare("SELECT count(*) AS n FROM t")),
                    },
                    "return!#2",
                    vec![],
                ),
            ],
            vec![],
            None,
            stmts(&["DROP TABLE IF EXISTS never_reached"]),
        );
        let resp = relay.handle_plan(&p);
        assert!(matches!(resp, ServerTerm::Error { .. }), "the run aborts");
        read_effect_run(&system)
    };
    // Steps: 0=begin, 1=insert ok, 2=insert failing, 3=return, 4=commit,
    // 5=cleanup.
    assert_eq!(outcomes[0], (0, "done".to_string()));
    assert_eq!(outcomes[1], (1, "done".to_string()));
    assert_eq!(outcomes[2].1, "error");
    assert_eq!(outcomes[3].1, "pending");
    assert_eq!(outcomes[4].1, "pending");
    assert_eq!(outcomes[5].1, "pending");
}

/// A GUARD-SAMPLING failure is the dependent step's
/// failure — `error` with the sampling message, never `pending`.
#[test]
fn guard_sampling_failure_attributes_to_the_dependent_step() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let outcomes = {
        let mut relay = relay_over(&mut system, Arc::clone(&conn));
        let guards = vec![GuardDefinition {
            guard_id: 0,
            sql: "SELECT 1 FROM this_table_does_not_exist".to_string(),
        }];
        let p = bracketed_typed_plan(
            vec![step(
                EffectAction::Dml(stmts(&["SELECT 1"])),
                "insert!#0",
                vec![req(0, GuardPolarity::Present)],
            )],
            guards,
            None,
            vec![],
        );
        let resp = relay.handle_plan(&p);
        assert!(matches!(resp, ServerTerm::Error { .. }));
        read_effect_run(&system)
    };
    // Steps: 0=begin (done), 1=the guarded dml (error), 2=commit (pending).
    assert_eq!(outcomes[0].1, "done");
    assert_eq!(outcomes[1].1, "error");
    assert_eq!(outcomes[2].1, "pending");
}

fn read_effect_run(system: &DelightQLSystem) -> Vec<(i64, String)> {
    let conn = system.get_bootstrap_connection();
    let guard = conn.lock().unwrap();
    let mut stmt = guard
        .prepare("SELECT step_id, status FROM effect_run ORDER BY step_id")
        .unwrap();
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows
}

/// D3a contract: an UNTYPED plan (hand-built, degenerate) has no exit
/// machinery and no edge sampling — every entry simply runs. The
/// transformer always attaches the typed layer; exit semantics live
/// there.
#[test]
fn untyped_plans_have_no_exit_machinery() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE __exit (hit INTEGER); CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = CompiledPlan {
        entries: vec![
            bare("INSERT INTO __exit VALUES (1)"),
            bare("INSERT INTO t VALUES ('runs anyway')"),
            ship("SELECT count(*) AS n FROM t"),
        ],
        exit_probe_sql: Some("SELECT count(*) FROM temp.__exit".to_string()),
        created_objects: Vec::new(),
        typed: None,
    };
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

/// Bracket happy path: BEGIN and COMMIT execute on the routed connection;
/// the mutation is durable and the connection leaves the transaction.
#[test]
fn bracket_commits_on_success() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        PlanEntry::BeginTransaction {
            connection_id: None,
            comment: None,
        },
        bare("INSERT INTO t VALUES ('committed')"),
        PlanEntry::CommitTransaction {
            connection_id: None,
            comment: None,
        },
        ship("SELECT v FROM t"),
    ]);
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["committed".to_string()]]);
    assert!(
        conn.lock().unwrap().is_autocommit(),
        "COMMIT must close the bracket"
    );
    assert_eq!(count_rows(&conn, "t"), 1);
}

/// Mid-bracket entry error: the pump ROLLBACKs, then aborts the run with
/// the error. Verified on the data itself: the pre-error INSERT is gone.
#[test]
fn bracket_rolls_back_on_mid_bracket_statement_error() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch(
            "CREATE TABLE t (v TEXT);
             INSERT INTO t VALUES ('keep');",
        )
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        PlanEntry::BeginTransaction {
            connection_id: None,
            comment: None,
        },
        bare("INSERT INTO t VALUES ('gone')"),
        bare("INSERT INTO no_such_table VALUES (1)"), // the mid-bracket error
        PlanEntry::CommitTransaction {
            connection_id: None,
            comment: None,
        },
        ship("SELECT v FROM t"),
    ]);
    let resp = relay.handle_plan(&p);
    let (_identity, message) = error_message(resp);
    assert!(
        message.contains("no_such_table"),
        "the abort must surface the failing statement's error, got: {}",
        message
    );
    assert!(
        conn.lock().unwrap().is_autocommit(),
        "the bracket must be rolled back, not left open"
    );
    let vals: Vec<String> = {
        let guard = conn.lock().unwrap();
        let mut stmt = guard.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let vals = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        vals
    };
    assert_eq!(
        vals,
        vec!["keep".to_string()],
        "the pre-error INSERT must have been rolled back"
    );
}

/// Assertion failure mid-plan: verdict hook fires (Pass then Fail), the run
/// aborts with today's identity and message shape, an open bracket rolls
/// back, and later entries never execute.
#[test]
fn compiler_check_failure_mid_plan_refuses_and_rolls_back() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let verdicts = Arc::new(Mutex::new(Vec::<(bool, Option<String>)>::new()));
    let verdicts_in_hook = Arc::clone(&verdicts);
    relay.set_hooks(RelayHooks {
        on_verdict: Some(Box::new(move |v| {
            verdicts_in_hook.lock().unwrap().push((
                matches!(v.outcome, crate::pipeline::verdict::VerdictOutcome::Pass),
                v.detail.clone(),
            ));
        })),
        ..RelayHooks::default()
    });

    let p = plan(vec![
        PlanEntry::BeginTransaction {
            connection_id: None,
            comment: None,
        },
        bare("INSERT INTO t VALUES ('rolled back')"),
        PlanEntry::Check {
            refusal: None,
            statement: PlanStatement::bare("SELECT 1"),
        },
        PlanEntry::Check {
            refusal: Some(crate::pipeline::compiled_query::Refusal {
                identity: "runtime/precondition".to_string(),
                message: "precondition failed".to_string(),
            }),
            statement: PlanStatement::bare("SELECT 0"),
        },
        bare("INSERT INTO t VALUES ('never reached')"),
        PlanEntry::CommitTransaction {
            connection_id: None,
            comment: None,
        },
        ship("SELECT v FROM t"),
    ]);
    let resp = relay.handle_plan(&p);
    let (identity, message) = error_message(resp);
    assert_eq!(identity, b"delightql-error://runtime/precondition".to_vec());
    assert_eq!(message, "precondition failed");

    let seen = verdicts.lock().unwrap();
    assert!(seen.is_empty(), "compiler checks are not assertion events");

    assert!(conn.lock().unwrap().is_autocommit());
    assert_eq!(
        count_rows(&conn, "t"),
        0,
        "the bracketed INSERT must have been rolled back on assertion abort"
    );
}

/// Assertion abort outside any bracket: earlier effects stand (nothing to
/// roll back), later entries are never executed.
#[test]
fn compiler_check_refusal_skips_later_entries_without_bracket() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        bare("INSERT INTO t VALUES ('a')"),
        PlanEntry::Check {
            refusal: Some(crate::pipeline::compiled_query::Refusal {
                identity: "runtime/precondition".to_string(),
                message: "precondition failed".to_string(),
            }),
            statement: PlanStatement::bare("SELECT 0"),
        },
        bare("INSERT INTO t VALUES ('b')"),
        ship("SELECT v FROM t"),
    ]);
    let resp = relay.handle_plan(&p);
    let (identity, message) = error_message(resp);
    assert_eq!(identity, b"delightql-error://runtime/precondition".to_vec());
    assert_eq!(message, "precondition failed");

    let vals: Vec<String> = {
        let guard = conn.lock().unwrap();
        let mut stmt = guard.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let vals = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        vals
    };
    assert_eq!(
        vals,
        vec!["a".to_string()],
        "unbracketed pre-abort effects stand; post-abort entries never run"
    );
}

#[test]
fn assertion_abort_rolls_back_skips_the_tail_and_persists_its_verdict() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_hook = Arc::clone(&seen);
    relay.set_hooks(RelayHooks {
        on_verdict: Some(Box::new(move |v| {
            seen_by_hook
                .lock()
                .unwrap()
                .push((v.outcome.clone(), v.identity.name.clone()));
        })),
        ..RelayHooks::default()
    });

    let abort = EffectAction::Terminal(TerminalAction::Abort {
        provenance: AbortProvenance::Assertion {
            label: "authored label".to_string(),
        },
        statements: Vec::new(),
        probe: PlanStatement::bare("SELECT 1"),
    });
    let p = bracketed_typed_plan(
        vec![
            step(
                EffectAction::Stage(stmts(&["INSERT INTO t VALUES ('rolled back')"])),
                "before",
                vec![],
            ),
            step(abort, "assert", vec![]),
            step(
                EffectAction::Stage(stmts(&["INSERT INTO t VALUES ('never')"])),
                "after",
                vec![],
            ),
        ],
        vec![],
        None,
        vec![],
    );
    let (identity, message) = error_message(relay.handle_plan(&p));
    assert_eq!(identity, b"delightql-error://runtime/assertion".to_vec());
    assert!(message.contains("authored label"));
    assert!(conn.lock().unwrap().is_autocommit());
    assert_eq!(count_rows(&conn, "t"), 0);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[(
            crate::pipeline::verdict::VerdictOutcome::Fail,
            Some("authored label".to_string())
        )]
    );
    let recorded = relay
        .system
        .bootstrap_connection()
        .lock()
        .unwrap()
        .query_row(
            "SELECT name, outcome FROM assertions ORDER BY id DESC LIMIT 1",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap();
    assert_eq!(recorded, ("authored label".to_string(), "fail".to_string()));
}

#[test]
fn assertion_observation_failure_is_typed_health_and_cannot_report_pass() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    system
        .bootstrap_connection()
        .lock()
        .unwrap()
        .execute_batch("PRAGMA query_only = ON")
        .unwrap();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));
    let plan = bracketed_typed_plan(
        vec![
            step(
                EffectAction::Stage(stmts(&["INSERT INTO t VALUES ('rolled back')"])),
                "before",
                vec![],
            ),
            step(
                EffectAction::Terminal(TerminalAction::Abort {
                    statements: vec![],
                    probe: PlanStatement::bare("SELECT 1 WHERE 0"),
                    provenance: AbortProvenance::Assertion {
                        label: "must observe pass".to_string(),
                    },
                }),
                "assert",
                vec![],
            ),
        ],
        vec![],
        None,
        vec![],
    );

    let (identity, message) = error_message(relay.handle_plan(&plan));
    assert_eq!(
        identity,
        b"delightql-error://runtime/session_health/external_effect".to_vec()
    );
    assert!(
        message.contains("assertion verdict observation"),
        "{message}"
    );
    assert_eq!(count_rows(&conn, "t"), 0);
    assert!(relay.system.health_incident().is_some());
}

#[test]
fn failed_assertion_keeps_primary_identity_when_observation_quarantines() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    system
        .bootstrap_connection()
        .lock()
        .unwrap()
        .execute_batch("PRAGMA query_only = ON")
        .unwrap();
    let mut relay = relay_over(&mut system, conn);
    let plan = bracketed_typed_plan(
        vec![step(
            EffectAction::Terminal(TerminalAction::Abort {
                statements: vec![],
                probe: PlanStatement::bare("SELECT 1"),
                provenance: AbortProvenance::Assertion {
                    label: "primary assertion".to_string(),
                },
            }),
            "assert",
            vec![],
        )],
        vec![],
        None,
        vec![],
    );

    let (identity, message) = error_message(relay.handle_plan(&plan));
    assert_eq!(identity, b"delightql-error://runtime/assertion".to_vec());
    assert!(message.contains("primary assertion"), "{message}");
    assert!(message.contains("session quarantined"), "{message}");
    assert!(relay.system.health_incident().is_some());
}

#[test]
fn committed_run_survives_abort_and_the_session_remains_usable() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let committed = bracketed_typed_plan(
        vec![step(
            EffectAction::Stage(stmts(&["INSERT INTO t VALUES ('committed')"])),
            "write",
            vec![],
        )],
        vec![],
        None,
        vec![],
    );
    assert!(!matches!(
        relay.handle_plan(&committed),
        ServerTerm::Error { .. }
    ));

    let aborted = bracketed_typed_plan(
        vec![
            step(
                EffectAction::Stage(stmts(&["INSERT INTO t VALUES ('gone')"])),
                "write",
                vec![],
            ),
            step(
                EffectAction::Terminal(TerminalAction::Abort {
                    provenance: AbortProvenance::Authored {
                        identity: "runtime/abort-test".to_string(),
                        label: "stop".to_string(),
                    },
                    statements: Vec::new(),
                    probe: PlanStatement::bare("SELECT 1"),
                }),
                "abort",
                vec![],
            ),
        ],
        vec![],
        None,
        vec![],
    );
    let (identity, _) = error_message(relay.handle_plan(&aborted));
    assert_eq!(identity, b"delightql-error://runtime/abort-test".to_vec());

    let response = relay.handle_plan(&plan(vec![ship("SELECT v FROM t ORDER BY v")]));
    let (_, rows) = fetch_all(&mut relay, response);
    assert_eq!(rows, vec![vec!["committed".to_string()]]);
}

#[test]
fn abort_probe_execution_error_keeps_the_runtime_execution_identity() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);
    let p = bracketed_typed_plan(
        vec![step(
            EffectAction::Terminal(TerminalAction::Abort {
                provenance: AbortProvenance::Authored {
                    identity: "runtime/should-not-replace".to_string(),
                    label: "unreached".to_string(),
                },
                statements: Vec::new(),
                probe: PlanStatement::bare("SELECT * FROM no_such_table"),
            }),
            "abort",
            vec![],
        )],
        vec![],
        None,
        vec![],
    );
    let (identity, message) = error_message(relay.handle_plan(&p));
    assert_eq!(identity, b"delightql-error://runtime/execution".to_vec());
    assert!(message.contains("no_such_table"));
}

#[test]
fn authored_abort_reaches_only_on_nonempty_input_and_keeps_the_session_usable() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);

    let empty = relay.handle(ClientTerm::Query {
        text: b"_(x @ 1), x = 99 |> abort!(\"runtime/abort-test\", \"empty\")(*)".to_vec(),
    });
    let _ = fetch_all(&mut relay, empty);

    let reached = relay.handle(ClientTerm::Query {
        text: b"_(x @ 1) |> abort!(\"runtime/abort-test\", \"reached\")(*)".to_vec(),
    });
    let (identity, message) = error_message(reached);
    assert_eq!(identity, b"delightql-error://runtime/abort-test".to_vec());
    assert!(message.contains("reached"));

    let usable = relay.handle(ClientTerm::Query {
        text: b"_(x @ 2)".to_vec(),
    });
    let (_, rows) = fetch_all(&mut relay, usable);
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

#[test]
fn qualified_builtin_identity_is_preserved_at_the_runtime_entry() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);

    let standard = relay.handle(ClientTerm::Query {
        text: b"#!dql query-sequence\n\
                nonempty(T(*))(*) : T(*)\n\
                _(x @ 1) !> std::prelude.assert!(nonempty(*), \"standard\")(*) |> #(x)"
            .to_vec(),
    });
    let (_, rows) = fetch_all(&mut relay, standard);
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    let standard_abort = relay.handle(ClientTerm::Query {
        text: b"_(x @ 1), x = 2 |> std::prelude.abort!(\"runtime/unreached\")(*)".to_vec(),
    });
    let _ = fetch_all(&mut relay, standard_abort);

    for source in [
        "#!dql query-sequence\nnonempty(T(*))(*) : T(*)\n_(x @ 1) !> bogus.assert!(nonempty(*))(*)",
        "_(x @ 1) |> bogus.abort!(\"runtime/must-not-run\")(*)",
        "_(x @ 1) |> bogus.insert!(sink(*))(*)",
    ] {
        let (identity, message) = error_message(relay.handle(ClientTerm::Query {
            text: source.as_bytes().to_vec(),
        }));
        assert_ne!(identity, b"delightql-error://runtime/assertion".to_vec());
        assert_ne!(identity, b"delightql-error://runtime/must-not-run".to_vec());
        assert!(
            message.contains("no effect rule")
                || message.contains("not a built-in")
                || message.contains("no DQL callable")
                || message.contains("Unknown directive"),
            "{message}"
        );
    }
}

#[test]
fn configured_assert_releases_the_exact_rows_and_reports_one_pass() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_hook = Arc::clone(&seen);
    relay.set_hooks(RelayHooks {
        on_verdict: Some(Box::new(move |v| {
            seen_by_hook
                .lock()
                .unwrap()
                .push((v.outcome.clone(), v.identity.name.clone()));
        })),
        ..RelayHooks::default()
    });
    let dql = "#!dql query-sequence\n\
               at_least(n, T(*))(*) : T(*) ~> count:(*) as c, c >= n\n\
               _(x @ 1; 2; 3) !> assert!(at_least(2), \"three rows\")(*) |> #(x)";
    let response = relay.handle(ClientTerm::Query {
        text: dql.as_bytes().to_vec(),
    });
    let (columns, rows) = fetch_all(&mut relay, response);
    assert_eq!(columns, vec!["x"]);
    assert_eq!(
        rows,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()]
        ]
    );
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        &[(
            crate::pipeline::verdict::VerdictOutcome::Pass,
            Some("three rows".to_string())
        )]
    );
}

#[test]
fn direct_assert_receipt_exposes_the_witness_and_returned_occurrences() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);
    let dql = "#!dql query-sequence\n\
               one(T(*))(*) : T(*), x = 1\n\
               assert!(one(*), \"direct\", _(x @ 1; 2))(*)";
    let response = relay.handle(ClientTerm::Query {
        text: dql.as_bytes().to_vec(),
    });
    let (columns, rows) = fetch_all(&mut relay, response);
    assert_eq!(
        columns,
        vec!["success", "operation", "label", "witnesses", "returned"]
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(&rows[0][..3], &["1", "assert!", "direct"]);
    assert!(rows[0][3].contains("\"x\":1"), "{}", rows[0][3]);
    assert!(rows[0][4].contains("\"x\":1"), "{}", rows[0][4]);
    assert!(rows[0][4].contains("\"x\":2"), "{}", rows[0][4]);
}

#[test]
fn volatile_assert_input_is_one_occurrence_in_witness_and_returned_payloads() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);
    let dql = "#!dql query-sequence\n\
               echo_property(T(*))(*) : T(*)\n\
               volatile(*) : _(seed @ 1) |> (random:() as token)\n\
               assert!(echo_property(*), \"volatile\", volatile(*))(*)";
    let response = relay.handle(ClientTerm::Query {
        text: dql.as_bytes().to_vec(),
    });
    let (_, rows) = fetch_all(&mut relay, response);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][3], rows[0][4],
        "the property witness and returned relation must expose the one staged random() occurrence"
    );
}

#[test]
fn empty_assertion_witness_uses_runtime_assertion_and_explicit_label() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);
    let dql = "#!dql query-sequence\n\
               none(T(*))(*) : T(*), x = 99\n\
               _(x @ 1) !> assert!(none(*), \"no 99\")(*)";
    let response = relay.handle(ClientTerm::Query {
        text: dql.as_bytes().to_vec(),
    });
    let (identity, message) = error_message(response);
    assert_eq!(identity, b"delightql-error://runtime/assertion".to_vec());
    assert!(message.contains("no 99"));
}

#[test]
fn omitted_assert_label_uses_the_synthetic_effect_identity() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, conn);
    let label = Arc::new(Mutex::new(None));
    let label_by_hook = Arc::clone(&label);
    relay.set_hooks(RelayHooks {
        on_verdict: Some(Box::new(move |v| {
            *label_by_hook.lock().unwrap() = v.identity.name.clone();
        })),
        ..RelayHooks::default()
    });
    let dql = "#!dql query-sequence\n\
               one(T(*))(*) : T(*), x = 1\n\
               _(x @ 1) !> assert!(one(*))(*)";
    let response = relay.handle(ClientTerm::Query {
        text: dql.as_bytes().to_vec(),
    });
    let _ = fetch_all(&mut relay, response);
    assert!(label
        .lock()
        .unwrap()
        .as_deref()
        .is_some_and(|name| name.starts_with("assert!#")));
}

/// Non-final shipped sets deliver through on_ship in execution order with
/// their own data; the final shipped set is the response, never a hook
/// call.
#[test]
fn non_final_shipped_deliver_via_on_ship_in_order() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    #[allow(clippy::type_complexity)]
    let ships = Arc::new(Mutex::new(Vec::<(
        Vec<String>,
        Vec<Vec<delightql_protocol::Cell>>,
    )>::new()));
    let ships_in_hook = Arc::clone(&ships);
    relay.set_hooks(RelayHooks {
        on_ship: Some(Box::new(move |cols, rows| {
            ships_in_hook
                .lock()
                .unwrap()
                .push((cols.to_vec(), rows.to_vec()));
        })),
        ..RelayHooks::default()
    });

    let p = plan(vec![
        bare("INSERT INTO t VALUES ('one')"),
        ship("SELECT v AS first_ship FROM t"), // stdout! #1: sees one row
        bare("INSERT INTO t VALUES ('two')"),
        ship("SELECT count(*) AS second_ship FROM t"), // stdout! #2: sees two
        ship("SELECT count(*) AS final_ship FROM t"),  // the return value
    ]);
    let resp = relay.handle_plan(&p);
    let (columns, rows) = fetch_all(&mut relay, resp);
    assert_eq!(columns, vec!["final_ship"]);
    assert_eq!(rows, vec![vec!["2".to_string()]]);

    let seen = ships.lock().unwrap();
    assert_eq!(seen.len(), 2, "exactly the two NON-final shipped sets");
    assert_eq!(seen[0].0, vec!["first_ship"]);
    assert_eq!(seen[0].1, vec![vec![Some(b"one".to_vec())]]);
    assert_eq!(seen[1].0, vec!["second_ship"]);
    assert_eq!(seen[1].1, vec![vec![Some(b"2".to_vec())]]);
}

/// The final shipped statement streams through the backend session (the
/// primary-SQL path: the handle registers in `handles`, not the eager
/// buffers) exactly when it is the plan's last entry on the default
/// connection.
#[test]
fn final_ship_streams_only_when_last_entry() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        bare("CREATE TABLE t (v TEXT)"),
        bare("INSERT INTO t VALUES ('x')"),
        ship("SELECT v FROM t"),
    ]);
    let resp = relay.handle_plan(&p);
    match &resp {
        ServerTerm::Header { handle, .. } => {
            assert!(
                relay.handles.contains_key(handle),
                "last-entry final ship takes the streaming path"
            );
            assert!(!relay.eager_buffers.contains_key(handle));
        }
        other => panic!("expected Header, got {:?}", other),
    }
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["x".to_string()]]);
}

/// A final shipped statement with entries after it (a bracket to close) is
/// buffered eagerly: the trailing entries still execute before the response
/// returns, and the response carries the shipped data.
#[test]
fn final_ship_before_trailing_entries_is_buffered() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        PlanEntry::BeginTransaction {
            connection_id: None,
            comment: None,
        },
        bare("INSERT INTO t VALUES ('inside')"),
        ship("SELECT v FROM t"), // final ship INSIDE the bracket
        PlanEntry::CommitTransaction {
            connection_id: None,
            comment: None,
        },
    ]);
    let resp = relay.handle_plan(&p);
    match &resp {
        ServerTerm::Header { handle, .. } => {
            assert!(
                relay.eager_buffers.contains_key(handle),
                "a non-last final ship must buffer, not stream"
            );
        }
        other => panic!("expected Header, got {:?}", other),
    }
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["inside".to_string()]]);
    assert!(
        conn.lock().unwrap().is_autocommit(),
        "the trailing COMMIT must have executed before the response returned"
    );
    assert_eq!(count_rows(&conn, "t"), 1);
}

/// A plan with no shipped entry at all answers with the empty header — the
/// one-response-per-Query invariant holds even for pure-effect plans.
#[test]
fn plan_with_no_shipped_entry_returns_empty_header() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        bare("CREATE TABLE t (v TEXT)"),
        bare("INSERT INTO t VALUES ('effect only')"),
    ]);
    let resp = relay.handle_plan(&p);
    let (columns, rows) = fetch_all(&mut relay, resp);
    assert!(columns.is_empty());
    assert!(rows.is_empty());
    assert_eq!(count_rows(&conn, "t"), 1, "the effects still ran");
}

// ---------------------------------------------------------------------
// The REPL recovery boundary (R4.2.6): a prompt is never presented over a
// quarantined session. These pins exercise the ACTUAL external-effect
// quarantine road, the typed health report the host reads, and the one
// reset authority that clears — or refuses to clear — the latch.
// ---------------------------------------------------------------------

/// SUCCESSFUL RECOVERY. The session quarantines on the real created-object
/// registration road; the typed report names the incident; the one reset
/// authority replaces the session; and a following `_(1)` succeeds.
#[test]
fn recovery_replaces_a_quarantined_session_and_the_next_query_succeeds() {
    let conn = shared_sqlite();
    let mut system = fresh_system();

    // 1. The actual quarantine road: a plan that created an object whose
    //    catalog registration fails after the run.
    {
        let mut relay = relay_over(&mut system, Arc::clone(&conn));
        let p = CompiledPlan {
            entries: vec![ship("SELECT 1 AS value")],
            exit_probe_sql: None,
            created_objects: vec![crate::pipeline::compiled_query::PlanCreatedObject {
                name: "created".to_string(),
                is_view: false,
                connection_id: None,
            }],
            typed: None,
        };
        let response = relay.play_plan_with_catalog(&p, &CatalogShouldNotRun);
        let (identity, _message) = error_message(response);
        assert_eq!(
            identity,
            b"delightql-error://runtime/session_health/external_effect".to_vec()
        );
        // The latch holds: an ordinary next query is refused, which is the
        // state a REPL must never wrap in another prompt.
        let refused = relay.handle(ClientTerm::Query {
            text: b"_(1)".to_vec(),
        });
        let (identity, _message) = error_message(refused);
        assert_eq!(
            identity,
            b"delightql-error://runtime/session_health/external_effect".to_vec()
        );
    }

    // 2. The TYPED report distinguishes the ruled incident — the host reads
    //    this, never error text.
    let (operation, message) = system
        .health_incident()
        .expect("the quarantine is visible to the typed report");
    assert_eq!(operation, "created-object registration");
    assert!(message.contains("delightql-error://"));

    // 3. The one reset authority clears the latch...
    system
        .reinit_bootstrap()
        .expect("recovery with no pending inverses succeeds");
    assert!(system.health_incident().is_none());

    // 4. ...and the replaced session answers an ordinary `_(1)`.
    let mut relay = relay_over(&mut system, conn);
    let resp = relay.handle(ClientTerm::Query {
        text: b"_(1)".to_vec(),
    });
    let (_columns, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

/// FAILED RECOVERY. A pending inverse that still fails on reset leaves the
/// incident latched: reset refuses, the report still says quarantined, and a
/// new query is still refused — the host's only lawful move is to terminate
/// the connection.
#[test]
fn a_failed_recovery_retains_the_quarantine() {
    let conn = shared_sqlite();
    let mut system = fresh_system();

    // A previously-Empty file that is now MISSING is the conservative
    // file-inverse uncertainty (ruled): restore_empty refuses to recreate it.
    let missing = std::env::temp_dir().join(format!(
        "dql_recovery_pin_missing_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    system.quarantine_session_with_pending(
        "liminal external-effect compensation",
        "test incident: a created file's inverse failed",
        vec![crate::external_effects::ExternalEffect::CreatedFile {
            path: missing,
            prior_state: crate::external_effects::CreatedFilePriorState::Empty,
        }],
    );

    let error = system
        .reinit_bootstrap()
        .expect_err("reset must refuse while an inverse still fails");
    assert!(
        error.error_uri().contains("session_health/external_effect"),
        "got {}",
        error.error_uri()
    );

    // The latch and the typed report both survive the failed reset.
    let (operation, _message) = system
        .health_incident()
        .expect("a failed recovery retains the incident");
    assert_eq!(operation, "liminal external-effect compensation");

    let mut relay = relay_over(&mut system, conn);
    let refused = relay.handle(ClientTerm::Query {
        text: b"_(1)".to_vec(),
    });
    let (identity, _message) = error_message(refused);
    assert_eq!(
        identity,
        b"delightql-error://runtime/session_health/external_effect".to_vec()
    );
}

/// NON-QUARANTINE CONTROL. An ordinary error — here a plain unknown-name
/// resolution failure — leaves the session healthy: the typed report says
/// so and the next query runs without any recovery.
#[test]
fn an_ordinary_error_does_not_trigger_the_recovery_boundary() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let response = relay.handle(ClientTerm::Query {
        text: b"no_such_table(*)".to_vec(),
    });
    let (identity, _message) = error_message(response);
    assert_ne!(
        identity,
        b"delightql-error://runtime/session_health/external_effect".to_vec()
    );
    assert!(relay.system.health_incident().is_none());

    let resp = relay.handle(ClientTerm::Query {
        text: b"_(1)".to_vec(),
    });
    let (_columns, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}
