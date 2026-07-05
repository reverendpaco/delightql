// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! PgParty — the Postgres fatboy's protocol handler.
//!
//! The relay-role counterpart of `SqlParty` (delightql-sqlite-relay):
//! receives layer-0 terms, executes against Postgres via the `postgres`
//! crate, answers with layer-0 terms. The engine consumes this through
//! `Box<dyn Handler>` and cannot tell it from any other backend — which
//! is the protocol's "spoken identically on both sides" claim made
//! a type-level fact.
//!
//! Step 2 (ALL-SQL-TARGETING-FATBOY.md): the query path.
//!
//! ## Text-mode execution (the parity decision)
//!
//! Queries run through Postgres's SIMPLE QUERY protocol
//! (`Client::simple_query`), so **Postgres itself renders every value to
//! text** — byte-identical to what psql sees. This is what lets the
//! fatboy reproduce the pipe-bridge-pinned `core--postgres--hash`
//! baselines exactly (step 6's acceptance gate). Bytes-first cells are
//! step 9, the documented recapture event.
//!
//! ## Foreign descriptors
//!
//! The simple-query path doesn't expose column types, so each query is
//! first `prepare`d (parse/plan only — nothing executes) to harvest
//! `Statement::columns()`: real PG type names (`int4`, `text`, `float8`,
//! `numeric`, …) carried as the protocol's opaque `descriptor` bytes —
//! relay-role question #2's first real data. If prepare fails (e.g.
//! multi-statement input), descriptors degrade to empty and
//! `simple_query` remains the single authoritative source of errors.
//!
//! Eager materialization (SqlParty's cursor-streaming worker is the
//! later perf upgrade, not a correctness need).
//!
//! ## The load path (step 7 — Prepare/Offer's first server AND first
//! real exercise anywhere)
//!
//! `Prepare` text must be a `COPY ... FROM STDIN` statement; the
//! layer-1 convention (mirroring catalog discovery being "just Query")
//! is that the statement declares `WITH (FORMAT csv, NULL '\N')` and
//! PgParty renders offered cells accordingly (everything quoted, NULL
//! cell -> unquoted \N). Offers buffer; `Close` on a load handle
//! executes the COPY on the SAME connection as queries — so a
//! `CREATE TEMP TABLE` query followed by a COPY into it works within
//! one relay connection. `Ok.count_hint` on load-close carries the
//! rows actually written (informative; unused by query-close per
//! spec). Streaming through a live CopyInWriter is the later perf
//! upgrade, mirroring the query path's eager stance.

pub use postgres;

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use delightql_protocol::{
    resolve_projection, Cell, ClientTerm, Dimension, ErrorKind, Handle, Handler, MetaItem,
    Orientation, Projection, ServerTerm,
};
use postgres::{Client, NoTls, SimpleQueryMessage};

/// Identity URI namespace for this adapter's own errors. Foreign-engine
/// SQLSTATEs get their full identity mapping in step 8.
const IDENT_UNIMPLEMENTED: &str = "dql/target/postgres/unimplemented";
const IDENT_PG_ERROR: &str = "dql/target/postgres/error";

struct ResultState {
    columns: Vec<String>,
    rows: VecDeque<Vec<Cell>>,
    exec_ms: u64,
}

struct LoadState {
    copy_sql: String,
    rows: Vec<Vec<Cell>>,
}

pub struct PgParty {
    /// Connected on construction; fail-closed — a PgParty that cannot
    /// reach its database never exists.
    client: Client,
    handles: HashMap<Handle, ResultState>,
    loads: HashMap<Handle, LoadState>,
    next_handle_id: u64,
}

impl PgParty {
    /// Connect to Postgres and return a ready handler.
    ///
    /// `conninfo` is a libpq connection string or URI
    /// (e.g. `"host=127.0.0.1 port=5433 user=postgres dbname=dql_core"`).
    pub fn connect(conninfo: &str) -> Result<Self, postgres::Error> {
        Ok(Self {
            client: Client::connect(conninfo, NoTls)?,
            handles: HashMap::new(),
            loads: HashMap::new(),
            next_handle_id: 1,
        })
    }

    fn handle_query(&mut self, text: Vec<u8>) -> ServerTerm {
        let sql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(_) => {
                return error(ErrorKind::Syntax, IDENT_PG_ERROR, b"query is not UTF-8".to_vec())
            }
        };

        // Descriptor harvest: prepare parses and plans, executes nothing.
        // On ANY prepare failure, fall through descriptor-less and let
        // simple_query be the single authoritative error source.
        let prepared: Option<Vec<(String, String)>> = self
            .client
            .prepare(&sql)
            .ok()
            .map(|stmt| {
                stmt.columns()
                    .iter()
                    .map(|c| (c.name().to_string(), c.type_().name().to_string()))
                    .collect()
            });

        // Execute via the simple-query protocol: PG renders all text.
        let started = Instant::now();
        let messages = match self.client.simple_query(&sql) {
            Ok(m) => m,
            Err(e) => return pg_error(&e),
        };
        let exec_ms = started.elapsed().as_millis() as u64;

        let mut rows: VecDeque<Vec<Cell>> = VecDeque::new();
        let mut row_columns: Option<Vec<String>> = None;
        let mut affected: u64 = 0;
        for msg in &messages {
            match msg {
                SimpleQueryMessage::Row(r) => {
                    if row_columns.is_none() {
                        row_columns =
                            Some(r.columns().iter().map(|c| c.name().to_string()).collect());
                    }
                    rows.push_back(
                        (0..r.len())
                            .map(|i| r.get(i).map(|s| s.as_bytes().to_vec()))
                            .collect(),
                    );
                }
                SimpleQueryMessage::CommandComplete(n) => affected = *n,
                _ => {}
            }
        }

        // Column names+descriptors: prepared metadata when available
        // (covers empty result sets), else names from the rows.
        let (names, descriptors): (Vec<String>, Vec<String>) = match (&prepared, &row_columns) {
            (Some(cols), _) if !cols.is_empty() => {
                (cols.iter().map(|(n, _)| n.clone()).collect(),
                 cols.iter().map(|(_, t)| t.clone()).collect())
            }
            (_, Some(names)) => (names.clone(), vec![String::new(); names.len()]),
            // No row description anywhere: a DML/DDL result. Per the
            // spec's appendix, outcomes are ordinary relations:
            // one row, one column, the affected count.
            _ => {
                rows.push_back(vec![Some(affected.to_string().into_bytes())]);
                (vec!["affected_rows".to_string()], vec!["int8".to_string()])
            }
        };

        let handle_id = self.next_handle_id;
        self.next_handle_id += 1;
        let handle: Handle = format!("pg{}", handle_id).into_bytes();

        let dimensions: Vec<Dimension> = names
            .iter()
            .zip(descriptors.iter())
            .enumerate()
            .map(|(i, (name, dtype))| Dimension {
                position: (i + 1) as u64,
                name: name.as_bytes().to_vec(),
                descriptor: dtype.as_bytes().to_vec(),
            })
            .collect();

        self.handles.insert(
            handle.clone(),
            ResultState { columns: names, rows, exec_ms },
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
        if orientation != Orientation::Rows {
            return error(
                ErrorKind::Connection,
                "dql/target/postgres/orientation",
                b"orientation Columns not supported".to_vec(),
            );
        }
        let state = match self.handles.get_mut(&handle) {
            Some(s) => s,
            None => return unknown_handle(),
        };

        let n = std::cmp::min(count as usize, state.rows.len());
        if n == 0 {
            return ServerTerm::End;
        }

        let drained: Vec<Vec<Cell>> = state.rows.drain(..n).collect();
        let col_indices = resolve_projection(&projection, &state.columns);
        let cells: Vec<Vec<Cell>> = drained
            .iter()
            .map(|row| col_indices.iter().map(|&ci| row[ci].clone()).collect())
            .collect();

        ServerTerm::Data { cells }
    }

    fn handle_stat(&self, handle: Handle) -> ServerTerm {
        match self.handles.get(&handle) {
            None => unknown_handle(),
            Some(state) => ServerTerm::Metadata {
                items: vec![
                    MetaItem::Backend(b"postgres".to_vec(), b"pg-fatboy".to_vec()),
                    MetaItem::ExecutionTime(state.exec_ms),
                ],
            },
        }
    }

    fn handle_prepare(&mut self, text: Vec<u8>, dimensions: Vec<Dimension>) -> ServerTerm {
        let copy_sql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(_) => {
                return error(ErrorKind::Syntax, IDENT_PG_ERROR, b"prepare text is not UTF-8".to_vec())
            }
        };
        // Layer-1 convention: the load command IS a COPY ... FROM STDIN.
        let upper = copy_sql.to_uppercase();
        if !(upper.trim_start().starts_with("COPY") && upper.contains("FROM STDIN")) {
            return error(
                ErrorKind::Syntax,
                IDENT_PG_ERROR,
                b"prepare text must be a COPY ... FROM STDIN statement".to_vec(),
            );
        }

        let handle_id = self.next_handle_id;
        self.next_handle_id += 1;
        let handle: Handle = format!("pgload{}", handle_id).into_bytes();
        self.loads.insert(handle.clone(), LoadState { copy_sql, rows: Vec::new() });
        // Echo the client-declared dimensions back, per the spec's
        // load-path script (Prepare -> Header).
        ServerTerm::Header { handle, dimensions }
    }

    fn handle_offer(
        &mut self,
        handle: Handle,
        cells: Vec<Vec<Cell>>,
        orientation: Orientation,
    ) -> ServerTerm {
        if orientation != Orientation::Rows {
            return error(
                ErrorKind::Connection,
                "dql/target/postgres/orientation",
                b"orientation Columns not supported".to_vec(),
            );
        }
        match self.loads.get_mut(&handle) {
            None => unknown_handle(),
            Some(state) => {
                state.rows.extend(cells);
                // Backpressure hint: eager buffering accepts freely.
                ServerTerm::Ok { count_hint: 1024 }
            }
        }
    }

    /// Render one offered row as a CSV line matching the convention
    /// `WITH (FORMAT csv, NULL '\N')`: every present cell quoted
    /// (quotes doubled), NULL cells as unquoted \N.
    fn csv_line(row: &[Cell]) -> String {
        let fields: Vec<String> = row
            .iter()
            .map(|c| match c {
                None => "\\N".to_string(),
                Some(bytes) => {
                    let s = String::from_utf8_lossy(bytes);
                    format!("\"{}\"", s.replace('\"', "\"\""))
                }
            })
            .collect();
        fields.join(",")
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        if self.handles.remove(&handle).is_some() {
            return ServerTerm::Ok { count_hint: 0 };
        }
        // A load handle: Close executes the buffered COPY.
        if let Some(load) = self.loads.remove(&handle) {
            use std::io::Write;
            let mut writer = match self.client.copy_in(load.copy_sql.as_str()) {
                Ok(w) => w,
                Err(e) => return pg_error(&e),
            };
            for row in &load.rows {
                let line = Self::csv_line(row);
                if let Err(e) = writer
                    .write_all(line.as_bytes())
                    .and_then(|_| writer.write_all(b"\n"))
                {
                    return error(
                        ErrorKind::Connection,
                        IDENT_PG_ERROR,
                        format!("copy stream write failed: {e}").into_bytes(),
                    );
                }
            }
            return match writer.finish() {
                // Informative: rows actually written (query-close
                // leaves the count unused per spec; load-close fills it).
                Ok(n) => ServerTerm::Ok { count_hint: n },
                Err(e) => pg_error(&e),
            };
        }
        unknown_handle()
    }
}

fn error(kind: ErrorKind, identity: &str, message: impl Into<Vec<u8>>) -> ServerTerm {
    ServerTerm::Error {
        kind,
        identity: identity.as_bytes().to_vec(),
        message: message.into(),
    }
}

fn unknown_handle() -> ServerTerm {
    error(ErrorKind::Connection, IDENT_PG_ERROR, b"unknown handle".to_vec())
}

/// SQLSTATE → identity-URI class (step 8 — relay-role question 5).
///
/// Identity = `dql/target/postgres/<class>/<sqlstate>` — the class for
/// programmatic matching by prefix, the exact SQLSTATE as the leaf for
/// precision (feeds E11's diagnostics catalog). Exact codes override
/// their class where the class default would mislead.
fn sqlstate_class(code: &str) -> &'static str {
    match code {
        "42601" => "syntax",
        "42804" => "type-mismatch",
        "42501" => "permission",
        _ => match code.get(..2) {
            Some("42") => "undefined-object", // 42883 fn, 42P01 table, 42703 col, …
            Some("22") => "type-mismatch",    // data exceptions (22P02 …)
            Some("23") => "constraint",
            Some("57") => "timeout",          // incl. 57014 statement_timeout
            Some("08") => "connection",
            Some("28") => "permission",
            _ => "error",
        },
    }
}

/// Map a postgres error to a protocol Error term: coarse ErrorKind from
/// the SQLSTATE class, precise identity URI, message preserved.
fn pg_error(e: &postgres::Error) -> ServerTerm {
    let exact = e.code().map(|c| c.code());
    let kind = match (exact, exact.and_then(|c| c.get(..2))) {
        (Some("42501"), _) | (_, Some("28")) => ErrorKind::Permission,
        (_, Some("23")) => ErrorKind::Constraint,
        (_, Some("57")) => ErrorKind::Timeout,
        (_, Some("08")) => ErrorKind::Connection,
        (_, Some(_)) => ErrorKind::Syntax,
        _ => ErrorKind::Connection,
    };
    // postgres::Error's Display is terse ("db error"); the server's
    // actual message (with SQLSTATE) lives in the DbError.
    let (identity, message) = match e.as_db_error() {
        Some(db) => {
            let code = db.code().code();
            (
                format!("dql/target/postgres/{}/{}", sqlstate_class(code), code),
                format!("{}: {}", code, db.message()),
            )
        }
        None => (
            "dql/target/postgres/connection".to_string(),
            e.to_string(),
        ),
    };
    error(kind, &identity, message.into_bytes())
}

impl Handler for PgParty {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        match term {
            // Orientation negotiation per CP-2: intersect with what we
            // support ([Rows], like SqlParty), error on empty agreement.
            // protocol_version/max_message_size/lease_ms echoed — the
            // fatboy grows an opinion about the version string when it
            // becomes a separately-released binary (step 3/4); the lease
            // becomes load-bearing at spawn-on-demand (step 5).
            ClientTerm::Version {
                max_message_size,
                protocol_version,
                lease_ms,
                orientations,
            } => {
                let supported = [Orientation::Rows];
                let agreed: Vec<Orientation> = orientations
                    .iter()
                    .copied()
                    .filter(|o| supported.contains(o))
                    .collect();
                if agreed.is_empty() {
                    error(
                        ErrorKind::Connection,
                        "dql/target/postgres/orientation",
                        b"no common orientation".to_vec(),
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

            ClientTerm::Fetch { handle, projection, count, orientation } => {
                self.handle_fetch(handle, projection, count, orientation)
            }

            ClientTerm::Stat { handle } => self.handle_stat(handle),

            ClientTerm::Close { handle } => self.handle_close(handle),

            ClientTerm::Prepare { text, dimensions } => self.handle_prepare(text, dimensions),

            ClientTerm::Offer { handle, cells, orientation } => {
                self.handle_offer(handle, cells, orientation)
            }
        }
    }
}

/// Term name for error messages, without dumping payload bytes.
fn kind_of(term: &ClientTerm) -> &'static str {
    match term {
        ClientTerm::Version { .. } => "Version",
        ClientTerm::Query { .. } => "Query",
        ClientTerm::Fetch { .. } => "Fetch",
        ClientTerm::Stat { .. } => "Stat",
        ClientTerm::Close { .. } => "Close",
        ClientTerm::Prepare { .. } => "Prepare",
        ClientTerm::Offer { .. } => "Offer",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use delightql_protocol::{
        AgreedOrientation, Client as RelayClient, ColumnRef, DirectTransport, FetchResponse,
        QueryResponse, Session, StatResponse, VersionResult,
    };

    /// The sweep container's loopback (new_test_suite/sweep.py). Tests
    /// SKIP (loudly) when it isn't running — runnable-by-default without
    /// demanding infrastructure.
    fn test_party() -> Option<PgParty> {
        let conninfo = std::env::var("DQL_TEST_PG_CONNINFO").unwrap_or_else(|_| {
            "host=127.0.0.1 port=5433 user=postgres dbname=dql_core".to_string()
        });
        match PgParty::connect(&conninfo) {
            Ok(p) => Some(p),
            Err(e) => {
                eprintln!(
                    "SKIP: postgres not reachable ({e}); \
                     start it with: ./new_test_suite/sweep.py postgres"
                );
                None
            }
        }
    }

    fn session() -> Option<(Session<DirectTransport<PgParty>>, AgreedOrientation)> {
        let party = test_party()?;
        let client = RelayClient::new(DirectTransport::new(party));
        let VersionResult::Accepted(session) = client
            .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Rows])
            .unwrap()
        else {
            panic!("handshake should succeed")
        };
        let rows = session.agreed_orientation(Orientation::Rows).unwrap();
        Some((session, rows))
    }

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn cell(s: &str) -> Cell {
        Some(s.as_bytes().to_vec())
    }

    #[test]
    fn handshake_negotiates_rows() {
        let Some(party) = test_party() else { return };
        let client = RelayClient::new(DirectTransport::new(party));
        let result = client
            .version(
                1_000_000,
                b("relay0"),
                300_000,
                vec![Orientation::Rows, Orientation::Columns],
            )
            .unwrap();
        match result {
            VersionResult::Accepted(session) => {
                assert!(session.agreed_orientation(Orientation::Rows).is_some());
                assert!(session.agreed_orientation(Orientation::Columns).is_none());
            }
            VersionResult::Rejected { message, .. } => {
                panic!("rejected: {}", String::from_utf8_lossy(&message))
            }
        }
    }

    #[test]
    fn handshake_empty_intersection_rejects() {
        let Some(party) = test_party() else { return };
        let client = RelayClient::new(DirectTransport::new(party));
        let result = client
            .version(1_000_000, b("relay0"), 300_000, vec![Orientation::Columns])
            .unwrap();
        assert!(matches!(result, VersionResult::Rejected { .. }));
    }

    #[test]
    fn select_one_with_foreign_descriptor() {
        let Some((mut session, rows_o)) = session() else { return };
        let QueryResponse::Header { handle, dimensions } =
            session.query(b("SELECT 1 AS x")).unwrap()
        else {
            panic!("expected Header")
        };
        assert_eq!(dimensions.len(), 1);
        assert_eq!(dimensions[0].name, b("x"));
        assert_eq!(dimensions[0].descriptor, b("int4")); // foreign descriptor!
        assert_eq!(dimensions[0].position, 1);

        match session.fetch(&handle, Projection::All, 100, rows_o).unwrap() {
            FetchResponse::Data { cells } => assert_eq!(cells, vec![vec![cell("1")]]),
            other => panic!("expected Data, got {:?}", other),
        }
        assert!(matches!(
            session.fetch(&handle, Projection::All, 100, rows_o).unwrap(),
            FetchResponse::End
        ));
        session.close(handle).unwrap();
    }

    /// THE parity probe: PG renders text via the simple-query protocol,
    /// so Bob's 0 balance is "0" (not sqlite's "0.0") — the exact
    /// rendering the pipe bridge pinned into the 897 baselines.
    #[test]
    fn text_mode_is_pg_rendered() {
        let Some((mut session, rows_o)) = session() else { return };
        let QueryResponse::Header { handle, dimensions } = session
            .query(b("SELECT balance FROM users WHERE id IN (1,3) ORDER BY id"))
            .unwrap()
        else {
            panic!("expected Header")
        };
        assert_eq!(dimensions[0].descriptor, b("float8"));
        match session.fetch(&handle, Projection::All, 100, rows_o).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(cells, vec![vec![cell("150.5")], vec![cell("0")]]);
            }
            other => panic!("expected Data, got {:?}", other),
        }
        session.close(handle).unwrap();
    }

    #[test]
    fn projection_by_name_and_index() {
        let Some((mut session, rows_o)) = session() else { return };
        let QueryResponse::Header { handle, .. } = session
            .query(b("SELECT id, first_name, country FROM users WHERE id = 1"))
            .unwrap()
        else {
            panic!("expected Header")
        };
        let proj = Projection::Select(vec![
            ColumnRef::ByName(b("country")),
            ColumnRef::ByIndex(1),
        ]);
        match session.fetch(&handle, proj, 100, rows_o).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(cells, vec![vec![cell("USA"), cell("1")]]);
            }
            other => panic!("expected Data, got {:?}", other),
        }
        session.close(handle).unwrap();
    }

    #[test]
    fn empty_result_set_has_dimensions_then_end() {
        let Some((mut session, rows_o)) = session() else { return };
        let QueryResponse::Header { handle, dimensions } = session
            .query(b("SELECT id, email FROM users WHERE false"))
            .unwrap()
        else {
            panic!("expected Header")
        };
        // prepare-derived metadata: names+types even with zero rows.
        assert_eq!(dimensions.len(), 2);
        assert_eq!(dimensions[1].name, b("email"));
        assert!(matches!(
            session.fetch(&handle, Projection::All, 100, rows_o).unwrap(),
            FetchResponse::End
        ));
        session.close(handle).unwrap();
    }

    #[test]
    fn dml_yields_affected_rows_relation() {
        let Some((mut session, rows_o)) = session() else { return };
        // TEMP table: session-scoped, the shared fixture is untouched.
        let QueryResponse::Header { handle, .. } = session
            .query(b("CREATE TEMP TABLE step2_scratch (x int)"))
            .unwrap()
        else {
            panic!("expected Header for DDL")
        };
        session.close(handle).unwrap();

        let QueryResponse::Header { handle, dimensions } = session
            .query(b("INSERT INTO step2_scratch VALUES (1), (2), (3)"))
            .unwrap()
        else {
            panic!("expected Header for DML")
        };
        assert_eq!(dimensions[0].name, b("affected_rows"));
        match session.fetch(&handle, Projection::All, 10, rows_o).unwrap() {
            FetchResponse::Data { cells } => assert_eq!(cells, vec![vec![cell("3")]]),
            other => panic!("expected Data, got {:?}", other),
        }
        session.close(handle).unwrap();
    }

    #[test]
    fn erroring_query_maps_sqlstate_to_identity() {
        let Some((mut session, _)) = session() else { return };
        // 42P01 undefined_table -> undefined-object class.
        match session.query(b("SELECT * FROM table_that_does_not_exist")).unwrap() {
            QueryResponse::Error { kind, identity, message } => {
                assert_eq!(kind, ErrorKind::Syntax);
                assert_eq!(identity, b("dql/target/postgres/undefined-object/42P01"));
                assert!(String::from_utf8_lossy(&message).contains("table_that_does_not_exist"));
            }
            QueryResponse::Header { .. } => panic!("expected Error"),
        }
        // 42601 -> syntax (exact-code override of the 42 class).
        match session.query(b("SELECTT 1")).unwrap() {
            QueryResponse::Error { identity, .. } => {
                assert_eq!(identity, b("dql/target/postgres/syntax/42601"));
            }
            QueryResponse::Header { .. } => panic!("expected Error"),
        }
        // 42883 undefined_function -> undefined-object class.
        match session.query(b("SELECT json_extract('{}', 'x')")).unwrap() {
            QueryResponse::Error { identity, .. } => {
                assert_eq!(identity, b("dql/target/postgres/undefined-object/42883"));
            }
            QueryResponse::Header { .. } => panic!("expected Error"),
        }
    }

    #[test]
    fn stat_reports_backend_and_timing() {
        let Some((mut session, _)) = session() else { return };
        let QueryResponse::Header { handle, .. } =
            session.query(b("SELECT 1")).unwrap()
        else {
            panic!("expected Header")
        };
        match session.stat(&handle).unwrap() {
            StatResponse::Metadata { items } => {
                assert!(items.iter().any(|i| matches!(
                    i, MetaItem::Backend(name, _) if name == b"postgres"
                )));
                assert!(items.iter().any(|i| matches!(i, MetaItem::ExecutionTime(_))));
            }
            other => panic!("expected Metadata, got {:?}", other),
        }
        session.close(handle).unwrap();
    }

    /// THE MAIDEN VOYAGE: Prepare/Offer's first server and first real
    /// exercise anywhere (the terms had ZERO clients before this).
    /// CREATE TEMP TABLE + COPY work in one relay connection because
    /// load-Close executes on the same PG session as queries.
    #[test]
    fn load_path_maiden_voyage() {
        let Some((mut session, rows_o)) = session() else { return };

        let QueryResponse::Header { handle, .. } = session
            .query(b("CREATE TEMP TABLE step7_cargo (id int, name text)"))
            .unwrap()
        else {
            panic!("expected Header for DDL")
        };
        session.close(handle).unwrap();

        // Prepare -> Header (load handle), echoing declared dimensions.
        let dims = vec![
            delightql_protocol::Dimension {
                position: 1,
                name: b("id"),
                descriptor: b("int4"),
            },
            delightql_protocol::Dimension {
                position: 2,
                name: b("name"),
                descriptor: b("text"),
            },
        ];
        let load = match session
            .prepare(
                b("COPY step7_cargo (id, name) FROM STDIN WITH (FORMAT csv, NULL '\\N')"),
                dims,
            )
            .unwrap()
        {
            delightql_protocol::PrepareResponse::Header { handle, dimensions } => {
                assert_eq!(dimensions.len(), 2);
                handle
            }
            delightql_protocol::PrepareResponse::Error { message, .. } => {
                panic!("prepare rejected: {}", String::from_utf8_lossy(&message))
            }
        };

        // Two Offer batches: a NULL cell, and a value with comma+quote
        // (exercising the CSV rendering).
        match session
            .offer(
                &load,
                vec![
                    vec![cell("1"), cell("alpha")],
                    vec![cell("2"), None],
                ],
                rows_o,
            )
            .unwrap()
        {
            delightql_protocol::OfferResponse::Ok { count_hint } => assert!(count_hint > 0),
            other => panic!("expected Ok, got {:?}", other),
        }
        match session
            .offer(&load, vec![vec![cell("3"), cell("comma, \"quoted\"")]], rows_o)
            .unwrap()
        {
            delightql_protocol::OfferResponse::Ok { .. } => {}
            other => panic!("expected Ok, got {:?}", other),
        }

        // Close executes the COPY; count_hint = rows written.
        match session.close(load).unwrap() {
            delightql_protocol::CloseResponse::Ok => {}
            delightql_protocol::CloseResponse::Error { message, .. } => {
                panic!("load close failed: {}", String::from_utf8_lossy(&message))
            }
        }

        // Verify through the query path: row count, NULL fidelity, and
        // the comma+quote value round-tripped.
        let QueryResponse::Header { handle, .. } = session
            .query(b("SELECT name FROM step7_cargo ORDER BY id"))
            .unwrap()
        else {
            panic!("expected Header")
        };
        match session.fetch(&handle, Projection::All, 100, rows_o).unwrap() {
            FetchResponse::Data { cells } => {
                assert_eq!(
                    cells,
                    vec![
                        vec![cell("alpha")],
                        vec![None],
                        vec![cell("comma, \"quoted\"")],
                    ]
                );
            }
            other => panic!("expected Data, got {:?}", other),
        }
        session.close(handle).unwrap();
    }

    #[test]
    fn prepare_rejects_non_copy_text() {
        let Some((mut session, _)) = session() else { return };
        match session
            .prepare(b("INSERT INTO t VALUES (1)"), vec![])
            .unwrap()
        {
            delightql_protocol::PrepareResponse::Error { message, .. } => {
                assert!(String::from_utf8_lossy(&message).contains("COPY"));
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[test]
    fn load_close_surfaces_copy_errors() {
        let Some((mut session, rows_o)) = session() else { return };
        let QueryResponse::Header { handle, .. } = session
            .query(b("CREATE TEMP TABLE step7_strict (x int)"))
            .unwrap()
        else {
            panic!("expected Header")
        };
        session.close(handle).unwrap();

        let load = match session
            .prepare(
                b("COPY step7_strict (x) FROM STDIN WITH (FORMAT csv, NULL '\\N')"),
                vec![],
            )
            .unwrap()
        {
            delightql_protocol::PrepareResponse::Header { handle, .. } => handle,
            other => panic!("expected Header, got {:?}", other),
        };
        session
            .offer(&load, vec![vec![cell("not-an-int")]], rows_o)
            .unwrap();
        match session.close(load).unwrap() {
            delightql_protocol::CloseResponse::Error { identity, message, .. } => {
                assert!(String::from_utf8_lossy(&message).contains("22P02"));
                assert_eq!(identity, b("dql/target/postgres/type-mismatch/22P02"));
            }
            delightql_protocol::CloseResponse::Ok => {
                panic!("COPY of bad data must error at close")
            }
        }
    }
}
