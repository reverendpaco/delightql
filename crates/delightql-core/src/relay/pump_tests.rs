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
use crate::pipeline::compiled_query::{CompiledPlan, PlanEntry, PlanStatement};
use crate::relay::RelayHooks;
use crate::system::DelightQLSystem;
use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
use delightql_types::test_utils::MockDatabaseConnection;

// ---------------------------------------------------------------------
// Test backend: an eager protocol Handler over a real rusqlite connection.
// ---------------------------------------------------------------------

struct EagerSqliteHandler {
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
                            rusqlite::types::Value::Integer(i) => {
                                Some(i.to_string().into_bytes())
                            }
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

fn fresh_system() -> DelightQLSystem {
    let conn = Arc::new(Mutex::new(MockDatabaseConnection::new()));
    DelightQLSystem::new(conn, Box::new(EmptyIntrospector), "sqlite")
        .expect("fresh in-memory system should build")
}

type TestRelay<'a> = RelayParty<'a, DirectTransport<EagerSqliteHandler>>;

/// Build a relay whose default (connection 2) backend is `conn` — a REAL
/// rusqlite connection the caller keeps a clone of for state inspection.
fn relay_over(
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

fn shared_sqlite() -> Arc<Mutex<rusqlite::Connection>> {
    Arc::new(Mutex::new(
        rusqlite::Connection::open_in_memory().expect("in-memory sqlite"),
    ))
}

fn bare(sql: &str) -> PlanEntry {
    PlanEntry::Statement(PlanStatement::bare(sql))
}

fn ship(sql: &str) -> PlanEntry {
    PlanEntry::ShippedStatement(PlanStatement::bare(sql))
}

fn plan(entries: Vec<PlanEntry>) -> CompiledPlan {
    CompiledPlan {
        entries,
        exit_table: None,
        created_objects: Vec::new(),
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

    // The default backend never saw the table.
    let on_backend: i64 = conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE name = 'pump_route_probe'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(on_backend, 0, "conn-1 entries must not touch the backend");
}

/// Exit peek: once the flag is set, remaining data entries are skipped —
/// including shipped ones ("a shipped entry after exit does NOT ship"). The
/// run answers with the empty header because its final ship never executed.
#[test]
fn exit_peek_skips_remaining_data_entries() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch(
            "CREATE TABLE __exit (hit INTEGER);
             CREATE TABLE t (v TEXT);",
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

    let p = CompiledPlan {
        entries: vec![
            bare("INSERT INTO __exit VALUES (1)"), // first entry sets the flag
            bare("INSERT INTO t VALUES ('must not happen')"),
            ship("SELECT v FROM t"), // non-final ship: must NOT reach on_ship
            ship("SELECT count(*) AS n FROM t"), // final ship: must not execute
        ],
        exit_table: Some("__exit".to_string()),
        created_objects: Vec::new(),
    };
    let resp = relay.handle_plan(&p);
    let (columns, rows) = fetch_all(&mut relay, resp);
    assert!(columns.is_empty(), "post-exit run answers the empty header");
    assert!(rows.is_empty());
    assert_eq!(count_rows(&conn, "t"), 0, "data entry after exit must be skipped");
    assert_eq!(*shipped.lock().unwrap(), 0, "shipped entry after exit must not ship");
}

/// The exit table's own scratch shell is a plan entry, so the run must
/// proceed regardless of peeks that would precede it. Since E-T5 the peek
/// WINDOW opens after the shell entry, so those peeks are not sent at
/// all; the error→unset arm this test originally pinned remains as the
/// defensive fallback for shell-less hand-built plans.
#[test]
fn exit_peek_tolerates_missing_exit_table() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = CompiledPlan {
        entries: vec![
            bare("CREATE TABLE __exit (hit INTEGER)"), // peeks start after this
            ship("SELECT 1 AS ok"),
        ],
        exit_table: Some("__exit".to_string()),
        created_objects: Vec::new(),
    };
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

/// E-T5 (R-T3): the peek WINDOW opens only after the plan entry that
/// CREATEs the exit table has executed. Before it the flag cannot have
/// been set by this run (nothing writes a table that does not exist),
/// and on PG a pre-shell peek would ERROR inside the open bracket —
/// which POISONS the transaction (P1 H5: every later statement answers
/// 25P02 and COMMIT becomes ROLLBACK). The shell entry succeeding +
/// abort-on-first-error means every in-window peek runs in a healthy
/// state: the table exists and the bracket is unpoisoned.
#[test]
fn exit_peek_window_opens_after_the_exit_shell_entry() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let (mut relay, sql_log) = relay_over_with_log(&mut system, Arc::clone(&conn));

    let p = CompiledPlan {
        entries: vec![
            bare("CREATE TABLE t (v TEXT)"),
            bare("CREATE TABLE __exit (hit INTEGER)"), // the exit shell
            bare("INSERT INTO t VALUES ('x')"),
            ship("SELECT v FROM t"),
        ],
        exit_table: Some("__exit".to_string()),
        created_objects: Vec::new(),
    };
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["x".to_string()]]);

    let log = sql_log.lock().unwrap();
    let shell_pos = log
        .iter()
        .position(|sql| sql.starts_with("CREATE TABLE __exit"))
        .expect("the shell entry executed");
    let first_peek = log
        .iter()
        .position(|sql| sql.contains("SELECT count(*) FROM __exit"));
    assert!(
        first_peek.map(|p| p > shell_pos).unwrap_or(true),
        "no peek may run before the exit shell entry (on PG a pre-shell \
         peek errors inside the bracket and poisons it, P1 H5); log:\n{:#?}",
        *log
    );
    assert!(
        first_peek.is_some(),
        "peeks do run once the shell exists; log:\n{:#?}",
        *log
    );
}

/// E-T5 (R-T3): the peek window CLOSES at the bracket's COMMIT — the
/// exit flag is written only by in-bracket data entries, so post-commit
/// peeks can never observe a new latch, and on PG the ON COMMIT DROP
/// shells make a post-commit peek an erroring statement (harmless
/// outside a transaction, but a statement the plan has no reason to
/// send). A flag latched before COMMIT stays latched (`exited` is
/// sticky), so post-commit data entries are still skipped exactly as
/// before.
#[test]
fn exit_peek_window_closes_at_the_bracket_commit() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let (mut relay, sql_log) = relay_over_with_log(&mut system, Arc::clone(&conn));

    let p = CompiledPlan {
        entries: vec![
            bare("CREATE TABLE __exit (hit INTEGER)"), // the exit shell
            PlanEntry::BeginTransaction {
                connection_id: None,
                comment: None,
            },
            bare("CREATE TABLE t (v TEXT)"),
            PlanEntry::CommitTransaction {
                connection_id: None,
                comment: None,
            },
            bare("DROP TABLE IF EXISTS __exit"), // trailing cleanup
            ship("SELECT 1 AS ok"),
        ],
        exit_table: Some("__exit".to_string()),
        created_objects: Vec::new(),
    };
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["1".to_string()]]);

    let log = sql_log.lock().unwrap();
    let commit_pos = log
        .iter()
        .position(|sql| sql == "COMMIT")
        .expect("the bracket committed");
    let peeks_after_commit = log
        .iter()
        .skip(commit_pos + 1)
        .filter(|sql| sql.contains("SELECT count(*) FROM __exit"))
        .count();
    assert_eq!(
        peeks_after_commit, 0,
        "no peek may run after the bracket's COMMIT; log:\n{:#?}",
        *log
    );
    let peeks_in_window = log
        .iter()
        .take(commit_pos)
        .filter(|sql| sql.contains("SELECT count(*) FROM __exit"))
        .count();
    assert!(
        peeks_in_window >= 1,
        "in-window peeks still run; log:\n{:#?}",
        *log
    );
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
fn assertion_failure_mid_plan_aborts_and_rolls_back() {
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
        PlanEntry::Assertion {
            statement: PlanStatement::bare("SELECT 1"),
            source_location: None,
        },
        PlanEntry::Assertion {
            statement: PlanStatement::bare("SELECT 0"),
            source_location: None,
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
    assert_eq!(identity, b"delightql-error://runtime/assertion".to_vec());
    assert!(
        message.contains("Assertion 2 failed"),
        "message must carry today's shape and the assertion's number, got: {}",
        message
    );

    let seen = verdicts.lock().unwrap();
    assert_eq!(seen.len(), 2, "both assertions reach the verdict hook");
    assert!(seen[0].0, "first assertion passes");
    assert!(!seen[1].0, "second assertion fails");

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
fn assertion_abort_skips_later_entries_without_bracket() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let p = plan(vec![
        bare("INSERT INTO t VALUES ('a')"),
        PlanEntry::Assertion {
            statement: PlanStatement::bare("SELECT 0"),
            source_location: None,
        },
        bare("INSERT INTO t VALUES ('b')"),
        ship("SELECT v FROM t"),
    ]);
    let resp = relay.handle_plan(&p);
    let (identity, message) = error_message(resp);
    assert_eq!(identity, b"delightql-error://runtime/assertion".to_vec());
    assert!(message.contains("Assertion 1 failed"), "got: {}", message);

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

/// Non-final shipped sets deliver through on_ship in execution order with
/// their own data; emit entries go to on_emit, never to on_ship; the final
/// shipped set is the response, never a hook call.
#[test]
fn non_final_shipped_deliver_via_on_ship_in_order() {
    let conn = shared_sqlite();
    conn.lock()
        .unwrap()
        .execute_batch("CREATE TABLE t (v TEXT);")
        .unwrap();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let ships = Arc::new(Mutex::new(Vec::<(Vec<String>, Vec<Vec<String>>)>::new()));
    let emits = Arc::new(Mutex::new(Vec::<String>::new()));
    let ships_in_hook = Arc::clone(&ships);
    let emits_in_hook = Arc::clone(&emits);
    relay.set_hooks(RelayHooks {
        on_ship: Some(Box::new(move |cols, rows| {
            ships_in_hook
                .lock()
                .unwrap()
                .push((cols.to_vec(), rows.to_vec()));
        })),
        on_emit: Some(Box::new(move |name, _cols, _rows| {
            emits_in_hook.lock().unwrap().push(name.to_string());
        })),
        ..RelayHooks::default()
    });

    let p = plan(vec![
        bare("INSERT INTO t VALUES ('one')"),
        ship("SELECT v AS first_ship FROM t"), // stdout! #1: sees one row
        bare("INSERT INTO t VALUES ('two')"),
        PlanEntry::Emit {
            name: "audit".to_string(),
            statement: PlanStatement::bare("SELECT v FROM t"),
            source_location: None,
        },
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
    assert_eq!(seen[0].1, vec![vec!["one".to_string()]]);
    assert_eq!(seen[1].0, vec!["second_ship"]);
    assert_eq!(seen[1].1, vec![vec!["2".to_string()]]);

    assert_eq!(
        *emits.lock().unwrap(),
        vec!["audit".to_string()],
        "emit entries keep their own channel"
    );
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

/// Emit execution errors keep today's contract: error-hook verdict, run
/// continues, no abort, no rollback.
#[test]
fn emit_error_is_tolerated_and_run_continues() {
    let conn = shared_sqlite();
    let mut system = fresh_system();
    let mut relay = relay_over(&mut system, Arc::clone(&conn));

    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let errors_in_hook = Arc::clone(&errors);
    relay.set_hooks(RelayHooks {
        on_error_hook: Some(Box::new(move |v| {
            errors_in_hook
                .lock()
                .unwrap()
                .push(v.identity.body_text.clone());
        })),
        ..RelayHooks::default()
    });

    let p = plan(vec![
        PlanEntry::Emit {
            name: "broken".to_string(),
            statement: PlanStatement::bare("SELECT v FROM no_such_emit_table"),
            source_location: None,
        },
        ship("SELECT 1 AS survived"),
    ]);
    let resp = relay.handle_plan(&p);
    let (_cols, rows) = fetch_all(&mut relay, resp);
    assert_eq!(rows, vec![vec!["1".to_string()]], "the run must continue past a failed emit");

    let seen = errors.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert!(
        seen[0].contains("Emit 'broken' execution failed"),
        "got: {}",
        seen[0]
    );
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
