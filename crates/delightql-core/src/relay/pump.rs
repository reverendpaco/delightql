// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! The pump — plays a `CompiledPlan` entry list (effect algebra, plan §3.2).
//!
//! `handle_query` runs today's fixed assertions → emits → primary sequence
//! for a `CompiledQuery`; the pump is that loop generalized to iterate an
//! ordered `PlanEntry` list (IMPLEMENTATION-ARCHITECTURE §4, "relay
//! handle_query → the pump"). It is a NEW path: nothing routes plain
//! queries through it (pinned at corpus scale — the full suite is
//! outcome-identical with the pump present). Its production callers are the
//! Epic-3.3 entry points (`relay/entry.rs`: `run!` / `run_namespace!` /
//! query-position directives); also exercised by `relay/pump_tests.rs` over
//! hand-constructed plans.
//!
//! Protocol shape (Epic 3 ruling, 2026-07-11): the run's return value is
//! the ONE wire response — the FINAL `ShippedStatement`'s result set,
//! delivered on the existing Query → Header cycle. Every OTHER shipped
//! result set (`stdout!`) delivers live through the hook side channel
//! (`RelayHooks::on_ship`), the same machinery emit streams already ride.
//! No wire-protocol change.

use delightql_protocol::{ErrorKind, QueryResponse, ServerTerm, Transport};

use super::{EagerBuffer, RelayParty};
use crate::pipeline::{
    compiled_query::{CompiledPlan, PlanEntry},
    verdict,
};

/// The connection a plan entry routes to (`None` = the session default,
/// `execute_sql_routed`'s `unwrap_or(2)`).
fn entry_connection(entry: &PlanEntry) -> Option<i64> {
    match entry {
        PlanEntry::Statement(st) | PlanEntry::ShippedStatement(st) => st.connection_id,
        PlanEntry::Assertion { statement, .. } | PlanEntry::Emit { statement, .. } => {
            statement.connection_id
        }
        PlanEntry::BeginTransaction { connection_id, .. }
        | PlanEntry::CommitTransaction { connection_id, .. } => *connection_id,
    }
}

fn connection_error(msg: String) -> ServerTerm {
    ServerTerm::Error {
        kind: ErrorKind::Connection,
        identity: vec![],
        message: msg.into_bytes(),
    }
}

impl<'a, T: Transport> RelayParty<'a, T> {
    /// Play a `CompiledPlan` start to finish (the pump, plan §3.2).
    ///
    /// Behavior, each piece pinned by the named test in
    /// `relay/pump_tests.rs`:
    ///
    /// - Entries execute first to last on their own routed connection
    ///   (`plays_entries_in_order_and_returns_final_ship`,
    ///   `routes_entries_per_connection`).
    /// - Exit peek: when the plan carries `exit_table`, the flag is checked
    ///   before each entry INSIDE THE PEEK WINDOW — the pump's ONLY mid-run
    ///   read; entries stay dumb (their exit guards are compiled into the
    ///   SQL by the planner). Once set, remaining DATA entries are skipped;
    ///   bracket entries still run so an open transaction closes
    ///   (TORTURE-TEST-NORMAL note D1: the conforming driver's
    ///   pre-statement check is what removes post-exit inert DDL). The
    ///   window (E-T5, R-T3): peeks START after the plan entry that CREATEs
    ///   `exit_table` (before it, this run cannot have set the flag — and
    ///   on PG, where the shells sit INSIDE the bracket, a pre-shell peek
    ///   would error mid-bracket and POISON the transaction, P1 H5) and
    ///   STOP at the bracket's COMMIT (only in-bracket data entries write
    ///   the flag; a pre-COMMIT latch is sticky). Within the window the
    ///   peek only ever runs in HEALTHY states — the shell CREATE
    ///   succeeded, every earlier statement succeeded (first error aborts
    ///   the run, below), so the table exists and the bracket is
    ///   unpoisoned; the peek's error→unset arm is defensive, live only
    ///   for hand-built plans with no shell entry (window open from the
    ///   start — today's tolerant behavior, kept)
    ///   (`exit_peek_skips_remaining_data_entries`,
    ///   `exit_peek_tolerates_missing_exit_table`,
    ///   `exit_peek_window_opens_after_the_exit_shell_entry`,
    ///   `exit_peek_window_closes_at_the_bracket_commit`).
    /// - Transaction bracket: `BeginTransaction` / `CommitTransaction`
    ///   execute as `BEGIN` / `COMMIT` on their routed connection — the
    ///   literal words, identical on SQLite/PG/DuckDB (E-T2 confirmed; no
    ///   dialecting). R-T3 discipline, UNIFORM on all engines: the FIRST
    ///   statement error inside a plan is plan-abort — the pump issues
    ///   ROLLBACK on the open bracket's connection, stops executing
    ///   entries, and surfaces the failing statement's TRUE error (E-T3a
    ///   made PgParty unable to mask it as 25P02; post-first-failure
    ///   statements WOULD legitimately answer 25P02 on PG and "transaction
    ///   is aborted" on DuckDB — abort-on-first-error is what prevents
    ///   ever sending one). SQLite tolerates play-on; we don't
    ///   (`bracket_commits_on_success`,
    ///   `bracket_rolls_back_on_mid_bracket_statement_error`; live on PG
    ///   by `pg_aborting_plan_surfaces_the_true_error_and_rolls_back` in
    ///   crates/delightql-cli/tests/effects_on_targets.rs).
    /// - DuckDB single-writable-attach (P3 H3): one DuckDB transaction may
    ///   write only ONE attached catalog. UNREACHABLE through the pump
    ///   today, verified E-T4: every DuckDB mount is its own direct-open
    ///   fatboy child (fatboy_exec.rs: "No ATTACH semantics through the
    ///   fatboy"), and a plan spanning two connections already refuses at
    ///   compile (`effect/plan/cross_connection`). If ATTACH-mounts ever
    ///   arrive, a plan writing two attached catalogs must become a
    ///   compile refusal — capability-matrix territory, not a pump fix.
    /// - Assertions abort the run on failure exactly as `handle_query` does
    ///   today — same verdict hooks, same error identity and message shape
    ///   (`assertion_failure_mid_plan_aborts_and_rolls_back`).
    /// - Emit entries deliver through `on_emit` and TOLERATE execution
    ///   errors exactly as today (error-hook verdict, run continues) — an
    ///   emit failure is not an abort, so it also never triggers a rollback
    ///   (`emit_error_is_tolerated_and_run_continues`).
    /// - Shipped result sets, per the Epic-3 protocol ruling: non-final
    ///   `ShippedStatement`s deliver live through `on_ship` in execution
    ///   order; the FINAL one is the run's return value. It streams through
    ///   `sql_session` (today's primary-SQL path) when it is the plan's
    ///   last entry on the default connection; otherwise it is buffered
    ///   eagerly so the entries after it still execute before the response
    ///   returns (`non_final_shipped_deliver_via_on_ship_in_order`,
    ///   `final_ship_streams_only_when_last_entry`,
    ///   `final_ship_before_trailing_entries_is_buffered`).
    /// - A plan with no shipped entry — or whose final ship was skipped by
    ///   the exit flag — answers with the empty header
    ///   (`plan_with_no_shipped_entry_returns_empty_header`).
    pub fn handle_plan(&mut self, plan: &CompiledPlan) -> ServerTerm {
        let final_ship_idx = plan
            .entries
            .iter()
            .rposition(|e| matches!(e, PlanEntry::ShippedStatement(_)));

        let mut exited = false;
        // The connection of the currently open bracket, if any.
        let mut open_bracket: Option<Option<i64>> = None;
        let mut assertion_no = 0usize;
        // Set when the final shipped entry is buffered eagerly because
        // entries after it still have to run.
        let mut final_response: Option<ServerTerm> = None;

        // The peek window's opening edge: the first entry AFTER the one
        // that CREATEs the exit table (the exit shell — a plan entry
        // itself). No shell entry (hand-built plans) → window open from
        // the start, today's tolerant behavior. See the doc comment's
        // "Exit peek" bullet for why the window exists (PG bracket
        // poisoning, P1 H5); pinned by
        // `exit_peek_window_opens_after_the_exit_shell_entry`.
        let peek_from = plan.exit_table.as_deref().map_or(0, |table| {
            plan.entries
                .iter()
                .position(|e| matches!(
                    e,
                    PlanEntry::Statement(st)
                        if st.sql.trim_start().starts_with("CREATE") && st.sql.contains(table)
                ))
                .map_or(0, |i| i + 1)
        });
        // The window's closing edge: flips at the bracket's COMMIT (only
        // in-bracket entries write the flag; a pre-COMMIT latch is
        // sticky). Pinned by `exit_peek_window_closes_at_the_bracket_commit`.
        let mut peeking_closed = false;

        for (idx, entry) in plan.entries.iter().enumerate() {
            // Exit peek — the pump's ONLY mid-run read. Latched: once the
            // flag is seen set it cannot unset, so peeking stops. Runs
            // only inside the window (see above), where the plan's own
            // discipline guarantees a healthy state: shell created,
            // every earlier statement succeeded.
            if !exited && !peeking_closed && idx >= peek_from {
                if let Some(table) = plan.exit_table.as_deref() {
                    exited = self.exit_flag_set(table, entry_connection(entry));
                }
            }
            let is_bracket = matches!(
                entry,
                PlanEntry::BeginTransaction { .. } | PlanEntry::CommitTransaction { .. }
            );
            if exited && !is_bracket {
                // Data entries stop at the exit flag; bracket entries still
                // run so an open transaction commits (exit! is a graceful
                // exit, not an abort).
                continue;
            }

            match entry {
                PlanEntry::BeginTransaction { connection_id, .. } => {
                    match self.execute_sql_routed("BEGIN", *connection_id) {
                        Ok(_) => open_bracket = Some(*connection_id),
                        Err(msg) => {
                            self.rollback_open_bracket(&mut open_bracket);
                            return connection_error(msg);
                        }
                    }
                }

                PlanEntry::CommitTransaction { connection_id, .. } => {
                    match self.execute_sql_routed("COMMIT", *connection_id) {
                        Ok(_) => {
                            open_bracket = None;
                            // The peek window closes with the bracket: no
                            // later entry can write the flag, and on PG the
                            // ON COMMIT DROP shells are gone.
                            peeking_closed = true;
                        }
                        Err(msg) => {
                            self.rollback_open_bracket(&mut open_bracket);
                            return connection_error(msg);
                        }
                    }
                }

                PlanEntry::Statement(st) => {
                    if let Err(msg) = self.execute_sql_routed(&st.sql, st.connection_id) {
                        self.rollback_open_bracket(&mut open_bracket);
                        return connection_error(msg);
                    }
                }

                PlanEntry::ShippedStatement(st) => {
                    let is_final = Some(idx) == final_ship_idx;
                    if !is_final {
                        // stdout!-style set: deliver live through the hook
                        // side channel (the protocol ruling).
                        match self.execute_sql_routed(&st.sql, st.connection_id) {
                            Ok((columns, rows)) => {
                                if let Some(ref mut hook) = self.hooks.on_ship {
                                    hook(&columns, &rows);
                                }
                            }
                            Err(msg) => {
                                self.rollback_open_bracket(&mut open_bracket);
                                return connection_error(msg);
                            }
                        }
                    } else if idx + 1 == plan.entries.len()
                        && st.connection_id.unwrap_or(2) == 2
                    {
                        // The run's return value, in the position today's
                        // primary SQL occupies: stream through the backend
                        // protocol, exactly like handle_query's primary path.
                        let sql_bytes = st.sql.as_bytes().to_vec();
                        match self.sql_session.query(sql_bytes) {
                            Ok(QueryResponse::Header {
                                handle: backend_handle,
                                dimensions,
                            }) => {
                                let frontend_handle = self.next_handle();
                                self.handles.insert(frontend_handle.clone(), backend_handle);
                                return ServerTerm::Header {
                                    handle: frontend_handle,
                                    dimensions,
                                };
                            }
                            Ok(QueryResponse::Error {
                                kind,
                                identity,
                                message,
                            }) => {
                                self.rollback_open_bracket(&mut open_bracket);
                                return ServerTerm::Error {
                                    kind,
                                    identity,
                                    message,
                                };
                            }
                            Err(e) => {
                                self.rollback_open_bracket(&mut open_bracket);
                                return connection_error(e.message);
                            }
                        }
                    } else {
                        // Final ship with entries still to run after it (or
                        // routed off the streaming connection): buffer
                        // eagerly, answer once the plan finishes.
                        match self.execute_sql_routed(&st.sql, st.connection_id) {
                            Ok((columns, rows)) => {
                                final_response = Some(self.eager_header(&columns, &rows));
                            }
                            Err(msg) => {
                                self.rollback_open_bracket(&mut open_bracket);
                                return connection_error(msg);
                            }
                        }
                    }
                }

                PlanEntry::Assertion { statement, .. } => {
                    assertion_no += 1;
                    match self.execute_sql_routed(&statement.sql, statement.connection_id) {
                        Ok((_cols, rows)) => {
                            let passed = rows
                                .first()
                                .and_then(|row| row.first())
                                .map(|v| matches!(v.as_str(), "1" | "true" | "t"))
                                .unwrap_or(false);

                            if let Some(ref mut hook) = self.hooks.on_verdict {
                                let v = verdict::Verdict {
                                    outcome: if passed {
                                        verdict::VerdictOutcome::Pass
                                    } else {
                                        verdict::VerdictOutcome::Fail
                                    },
                                    identity: verdict::VerdictIdentity {
                                        _name: None,
                                        _source_location: None,
                                        body_text: statement.sql.clone(),
                                    },
                                    detail: if passed {
                                        None
                                    } else {
                                        Some(format!(
                                            "Assertion {} failed\n  SQL: {}",
                                            assertion_no, statement.sql
                                        ))
                                    },
                                    _intent: None,
                                };
                                hook(&v);
                            }

                            if !passed {
                                self.rollback_open_bracket(&mut open_bracket);
                                return ServerTerm::Error {
                                    kind: ErrorKind::Permission,
                                    identity: b"delightql-error://runtime/assertion".to_vec(),
                                    message: format!(
                                        "Assertion {} failed\n  SQL: {}",
                                        assertion_no, statement.sql
                                    )
                                    .into_bytes(),
                                };
                            }
                        }
                        Err(msg) => {
                            self.rollback_open_bracket(&mut open_bracket);
                            return ServerTerm::Error {
                                kind: ErrorKind::Permission,
                                identity: b"delightql-error://runtime/assertion".to_vec(),
                                message: format!(
                                    "Assertion {} execution error: {}",
                                    assertion_no, msg
                                )
                                .into_bytes(),
                            };
                        }
                    }
                }

                PlanEntry::Emit {
                    name, statement, ..
                } => {
                    match self.execute_sql_routed(&statement.sql, statement.connection_id) {
                        Ok((columns, rows)) => {
                            if let Some(ref mut hook) = self.hooks.on_emit {
                                hook(name, &columns, &rows);
                            }
                        }
                        Err(msg) => {
                            // Exactly today's emit contract: notify, never
                            // abort. NOTE (E-T5): on PG a failed SELECT
                            // inside an open bracket DOES poison the
                            // transaction (P1 H5) — this tolerance is safe
                            // only because the effect transformer emits no
                            // Emit entries at all (grep: zero
                            // `PlanEntry::Emit` constructions there), so a
                            // mid-bracket emit exists only in hand-built
                            // plans on the SQLite test backend. If emits
                            // ever become plan entries, a mid-bracket emit
                            // failure must abort like a statement's.
                            if let Some(ref mut hook) = self.hooks.on_error_hook {
                                let v = verdict::Verdict {
                                    outcome: verdict::VerdictOutcome::Fail,
                                    identity: verdict::VerdictIdentity {
                                        _name: Some(name.clone()),
                                        _source_location: None,
                                        body_text: format!(
                                            "Emit '{}' execution failed: {}",
                                            name, msg
                                        ),
                                    },
                                    detail: Some(msg),
                                    _intent: None,
                                };
                                hook(&v);
                            }
                        }
                    }
                }
            }
        }

        final_response.unwrap_or_else(|| self.empty_header_response())
    }

    /// Peek the exit flag on the entry's own connection — the same
    /// connection the entry's compiled exit-guard conjuncts would read it
    /// on. The table name interpolates VERBATIM: the planner spells it
    /// schema-qualified in the DIALECT's spelling (`temp.__exit`,
    /// `pg_temp.__exit` — E-T2, pinned by
    /// `pg_exit_table_and_wrap_guard_spell_pg_temp`), so the peek
    /// structurally cannot false-latch on a user's physical `main.__exit`
    /// (review F3; pinned by the effects ball's
    /// scratch--53_user_exit_table_survives_run). Inside the peek window
    /// (see `handle_plan`) the table exists and the bracket is healthy,
    /// so the error arm is DEFENSIVE: it stays "unreadable counts as
    /// unset" only for hand-built plans with no shell entry, where the
    /// window opens at the start; pinned by
    /// `exit_peek_tolerates_missing_exit_table`.
    fn exit_flag_set(&mut self, exit_table: &str, connection_id: Option<i64>) -> bool {
        // The peek asks for the exit table's CARDINALITY, not for a
        // row's presence. `SELECT count(*)` ALWAYS returns exactly one
        // row carrying a real integer, so the flag reads the same on
        // every backend — including the fatboy relay, where an
        // empty-result SELECT (0 rows, and mid-bracket no prepared
        // column descriptors because E-T3a skips the prepare inside a
        // transaction) is otherwise synthesized as a one-row
        // `affected_rows` relation (delightql-postgres/src/lib.rs:357):
        // a `SELECT 1 … LIMIT 1` peek would then read that synthetic row
        // as presence and FALSE-LATCH exit on the empty table. Reading
        // count(*) sidesteps the ambiguity — count always yields a
        // genuine row description, never the DML synthesis. exit is
        // latched iff the count is a positive integer. Pinned live by
        // `pg_exit_taken_and_not_taken`
        // (crates/delightql-cli/tests/effects_on_targets.rs); the
        // in-process backend path by `exit_peek_skips_remaining_data_entries`.
        let sql = format!("SELECT count(*) FROM {}", exit_table);
        match self.execute_sql_routed(&sql, connection_id) {
            Ok((_cols, rows)) => rows
                .first()
                .and_then(|row| row.first())
                .and_then(|v| v.trim().parse::<i64>().ok())
                .map(|n| n > 0)
                .unwrap_or(false),
            Err(_) => false,
        }
    }

    /// Best-effort ROLLBACK of the open bracket on the abort path. The
    /// original error is what surfaces; a rollback failure has nothing
    /// better to become, so it is deliberately swallowed.
    fn rollback_open_bracket(&mut self, open_bracket: &mut Option<Option<i64>>) {
        if let Some(cid) = open_bracket.take() {
            let _ = self.execute_sql_routed("ROLLBACK", cid);
        }
    }

    /// Buffer an eagerly-executed result set and answer with its Header —
    /// the same shape as handle_query's eager primary path.
    fn eager_header(&mut self, columns: &[String], rows: &[Vec<String>]) -> ServerTerm {
        let (dimensions, cells) = Self::strings_to_eager_buffer(columns, rows);
        let handle = self.next_handle();
        self.eager_buffers.insert(
            handle.clone(),
            EagerBuffer {
                dimensions: dimensions.clone(),
                rows: cells,
                cursor: 0,
            },
        );
        ServerTerm::Header { handle, dimensions }
    }
}
