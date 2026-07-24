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
//! Protocol shape: the run's return value is
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


/// An engine-side execution failure: compilation succeeded and the
/// database refused the SQL (or a transaction statement) at run time.
/// Badged runtime/execution so the error is explainable and
/// annotation-matchable; the protocol-level Connection kind stays for
/// wire compatibility.
fn connection_error(msg: String) -> ServerTerm {
    ServerTerm::Error {
        kind: ErrorKind::Connection,
        identity: b"delightql-error://runtime/execution".to_vec(),
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
    /// - The typed walk (D3a): a typed plan's steps partition the body;
    ///   at each step's first entry the pump samples the step's
    ///   requirement edges at the dependent (Q-D1) through a count(*)
    ///   wrapper, and DECLINES the whole statement stream when any edge
    ///   is closed. exit! is an ordinary Absent edge (Q-D7); one
    ///   pre-COMMIT latch read decides whether the post-COMMIT tail
    ///   (trailing cleanup) runs — bracket entries always run, so an
    ///   exit-taken run still commits (graceful exit, not abort)
    ///   (`exit_absent_edges_skip_later_steps_and_the_tail`,
    ///   `typed_walk_declines_steps_with_closed_present_edges`,
    ///   `untyped_plans_have_no_exit_machinery`).
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
        // THE ONE TYPED PROGRAM (review finding 3): a typed plan is walked
        // directly — setup, control, effect, return, and cleanup are all
        // steps, so the D5 trace covers control failures too. The flat
        // entry list is only ever a projection; the pump never
        // reconstructs ranges from it. Untyped plans (degenerate
        // CompiledQuery conversions, hand-built tests) take the plain
        // entry loop with no gating and no exit machinery.
        match &plan.typed {
            Some(typed) => {
                // D5: per-step outcomes, tracked in memory and
                // materialized once at the boundary — best-effort,
                // because bookkeeping never outranks the run. The
                // reconciliation is sound: execution is sequential and
                // abort-on-first-error, so at most ONE step is mid-flight
                // ("running") when the walk stops.
                let mut trace: Vec<Option<(&'static str, Option<String>)>> =
                    vec![None; typed.steps.len()];
                let term = self.play_typed(plan, typed, &mut trace);
                let is_error = matches!(term, ServerTerm::Error { .. });
                let err_msg = match &term {
                    ServerTerm::Error { message, .. } => {
                        Some(String::from_utf8_lossy(message).to_string())
                    }
                    _ => None,
                };
                let outcomes: Vec<(&'static str, Option<String>)> = trace
                    .into_iter()
                    .map(|slot| match slot {
                        Some(("running", _)) if is_error => ("error", err_msg.clone()),
                        Some(("running", _)) => ("done", None),
                        Some(done_or_skipped) => done_or_skipped,
                        None => ("pending", None),
                    })
                    .collect();
                let _ = self.system.materialize_effect_run(&outcomes);
                term
            }
            None => self.play_entries(plan),
        }
    }

    /// Walk the typed program step by step: sample each step's
    /// requirement edges at the dependent (Q-D1) and decline the whole
    /// action when any edge is closed; execute the action otherwise.
    /// exit! is an ordinary Absent edge on later body steps (Q-D7); ONE
    /// pre-COMMIT latch read decides whether the Cleanup step runs
    /// (graceful exit: brackets always run, exit-taken residue is
    /// drop_plan_scratch's job). The run's return value is the LAST ship
    /// across all steps — the Return step's when present, else the
    /// body-ending stdout ship (body_ending_in_stdout_ships_once) — and
    /// is always buffered eagerly (COMMIT follows every ship by
    /// construction, exactly as the flat walk behaved).
    fn play_typed(
        &mut self,
        plan: &CompiledPlan,
        typed: &crate::pipeline::compiled_query::TypedEffectPlan,
        trace: &mut [Option<(&'static str, Option<String>)>],
    ) -> ServerTerm {
        use crate::pipeline::compiled_query::EffectAction;
        self.last_run_exited = false;
        let mut open_bracket: Option<Option<i64>> = None;
        let mut final_response: Option<ServerTerm> = None;
        let mut exited = false;
        let last_ship = typed
            .steps
            .iter()
            .rposition(|s| s.action.ship().is_some());

        for (idx, step) in typed.steps.iter().enumerate() {
            // "Brackets ALWAYS run" is enforced HERE, not merely by the
            // builder's discipline (review round 3): a Begin/Commit step
            // never samples edges, so no construction can gate the
            // bracket closed and strand an open transaction.
            let bracket = matches!(
                step.action,
                EffectAction::Begin { .. } | EffectAction::Commit { .. }
            );
            if !bracket && !step.requirements.is_empty() {
                match self.step_open(step, &typed.guards) {
                    Ok(None) => {}
                    Ok(Some(closed_detail)) => {
                        trace[idx] = Some(("skipped", Some(closed_detail)));
                        continue;
                    }
                    Err(msg) => {
                        // Review finding 3 (attribution): a guard-sampling
                        // failure IS the step's failure — never "pending".
                        trace[idx] =
                            Some(("error", Some(format!("guard sampling failed: {msg}"))));
                        self.rollback_open_bracket(&mut open_bracket);
                        return connection_error(msg);
                    }
                }
            }
            trace[idx] = Some(("running", None));
            match &step.action {
                EffectAction::Begin { connection_id } => {
                    match self.execute_sql_routed("BEGIN", *connection_id) {
                        Ok(_) => open_bracket = Some(*connection_id),
                        Err(msg) => {
                            self.rollback_open_bracket(&mut open_bracket);
                            return connection_error(msg);
                        }
                    }
                }
                EffectAction::Commit { connection_id } => {
                    // The pre-COMMIT latch read: the flag can only have
                    // been written inside the bracket, and on PG the
                    // ON COMMIT DROP shells vanish at COMMIT — so this is
                    // the one moment the tail decision can be read.
                    if !exited {
                        if let Some(table) = plan.exit_table.as_deref() {
                            exited = self.exit_flag_set(table, *connection_id);
                        }
                    }
                    match self.execute_sql_routed("COMMIT", *connection_id) {
                        Ok(_) => open_bracket = None,
                        Err(msg) => {
                            self.rollback_open_bracket(&mut open_bracket);
                            return connection_error(msg);
                        }
                    }
                }
                // Phase 10 slice b: annotation steps in the typed program —
                // the same verdict/abort and notify-never-abort contracts
                // as the untyped entries (play_entries below).
                EffectAction::Assertion { statement, .. } => {
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
                                            "Assertion failed\n  SQL: {}",
                                            statement.sql
                                        ))
                                    },
                                    _intent: None,
                                };
                                hook(&v);
                            }
                            if !passed {
                                trace[idx] =
                                    Some(("error", Some("assertion failed".to_string())));
                                self.rollback_open_bracket(&mut open_bracket);
                                return ServerTerm::Error {
                                    kind: ErrorKind::Permission,
                                    identity: b"delightql-error://runtime/assertion".to_vec(),
                                    message: format!(
                                        "Assertion failed\n  SQL: {}",
                                        statement.sql
                                    )
                                    .into_bytes(),
                                };
                            }
                        }
                        Err(msg) => {
                            trace[idx] = Some(("error", Some(msg.clone())));
                            self.rollback_open_bracket(&mut open_bracket);
                            return connection_error(msg);
                        }
                    }
                }
                EffectAction::Cleanup(stmts) => {
                    if exited {
                        trace[idx] = Some((
                            "skipped",
                            Some(
                                "exit! taken: cleanup residue is drop_plan_scratch's job"
                                    .to_string(),
                            ),
                        ));
                        continue;
                    }
                    for st in stmts {
                        if let Err(msg) = self.execute_sql_routed(&st.sql, st.connection_id) {
                            self.rollback_open_bracket(&mut open_bracket);
                            return connection_error(msg);
                        }
                    }
                }
                action => {
                    for st in action.statements() {
                        if let Err(msg) = self.execute_sql_routed(&st.sql, st.connection_id) {
                            self.rollback_open_bracket(&mut open_bracket);
                            return connection_error(msg);
                        }
                    }
                    if let Some(ship) = action.ship() {
                        match self.execute_sql_routed(&ship.sql, ship.connection_id) {
                            Ok((columns, rows)) => {
                                if Some(idx) == last_ship {
                                    final_response =
                                        Some(self.eager_header(&columns, &rows));
                                } else if let Some(ref mut hook) = self.hooks.on_ship {
                                    hook(&columns, &rows);
                                }
                            }
                            Err(msg) => {
                                self.rollback_open_bracket(&mut open_bracket);
                                return connection_error(msg);
                            }
                        }
                    }
                }
            }
            trace[idx] = Some(("done", None));
        }

        // F5 (Phase 6 slice 6): the receipt binder reads whether this
        // run answered NO — the exit! latch decides the EMPTY receipt.
        self.last_run_exited = exited;
        final_response.unwrap_or_else(|| self.empty_header_response())
    }

    fn play_entries(&mut self, plan: &CompiledPlan) -> ServerTerm {
        let final_ship_idx = plan
            .entries
            .iter()
            .rposition(|e| matches!(e, PlanEntry::ShippedStatement(_)));

        // The connection of the currently open bracket, if any.
        let mut open_bracket: Option<Option<i64>> = None;
        let mut assertion_no = 0usize;
        // Set when the final shipped entry is buffered eagerly because
        // entries after it still have to run.
        let mut final_response: Option<ServerTerm> = None;

        for (idx, entry) in plan.entries.iter().enumerate() {
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

            }
        }

        final_response.unwrap_or_else(|| self.empty_header_response())
    }

    /// D3a: sample one step's requirement edges at the DEPENDENT (Q-D1).
    /// Every sample reads through a `count(*)` wrapper for the same
    /// reason `exit_flag_set` does: on the fatboy relay an empty
    /// mid-bracket SELECT is synthesized as a one-row `affected_rows`
    /// relation, which a presence read would false-interpret — count
    /// always yields a genuine row. Any closed edge declines the whole
    /// step; a sampling error aborts like a statement error (R-T3) —
    /// the transformer guarantees every referenced shell exists.
    /// `Ok(None)` = every edge open; `Ok(Some(detail))` = the first
    /// closed edge, described for the D5 run trace.
    fn step_open(
        &mut self,
        step: &crate::pipeline::compiled_query::EffectStep,
        guards: &[crate::pipeline::compiled_query::GuardDefinition],
    ) -> Result<Option<String>, String> {
        use crate::pipeline::compiled_query::GuardPolarity;
        for req in &step.requirements {
            let guard = &guards[req.guard_id];
            let sql = format!("SELECT count(*) FROM ({}) AS __g", guard.sql);
            let (_cols, rows) = self.execute_sql_routed(&sql, step.route)?;
            let present = rows
                .first()
                .and_then(|row| row.first())
                .and_then(|v| v.trim().parse::<i64>().ok())
                .map(|n| n > 0)
                .unwrap_or(false);
            let (polarity, open) = match req.polarity {
                GuardPolarity::Present => ("present", present),
                GuardPolarity::Absent => ("absent", !present),
            };
            if !open {
                return Ok(Some(format!(
                    "edge closed: guard {} required {} ({})",
                    req.guard_id, polarity, req.reason
                )));
            }
        }
        Ok(None)
    }

    /// Read the exit latch (the pre-COMMIT tail decision — D3a retired
    /// the per-entry peek window; requirement edges cover the body). The
    /// table name interpolates VERBATIM: the planner spells it
    /// schema-qualified in the DIALECT's spelling (`temp.__exit`,
    /// `pg_temp.__exit` — E-T2, pinned by
    /// `pg_exit_table_and_wrap_guard_spell_pg_temp`), so the read
    /// structurally cannot false-latch on a user's physical `main.__exit`
    /// (review F3; pinned by the effects ball's
    /// scratch--53_user_exit_table_survives_run).
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
    pub(super) fn eager_header(&mut self, columns: &[String], rows: &[Vec<String>]) -> ServerTerm {
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
