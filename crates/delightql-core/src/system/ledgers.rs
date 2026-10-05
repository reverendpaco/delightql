// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
//! Write-only run and observability ledgers: findings, compiler limits, the
//! effect plan and run projections, and assertion verdicts.

use super::DelightQLSystem;
use crate::diagnostic::Runtime;
use crate::error::Result;
use rusqlite::Connection;

impl DelightQLSystem {
    /// The ONE writer of `sys::diagnostics.finding`. Recording never
    /// defeats the caller's real work: a failed insert is dropped, because
    /// the finding is on its way to the caller as an error already.
    pub fn record_finding(
        &self,
        kind: crate::diagnostics::Severity,
        uri: &str,
        message: &str,
        input: Option<&str>,
        provider: &str,
    ) {
        let Ok(conn) = self.bootstrap_connection.lock() else {
            return;
        };
        // The engine stamps the row: RFC 3339 UTC to the millisecond, the
        // same shape the client's tables use, without a time dependency.
        let _ = conn.execute(
            "INSERT INTO finding (occurred_at, kind, uri, message, input, provider)
             VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![kind.as_str(), uri, message, input, provider],
        );
    }

    /// Publish `armed` into `sys::execution.compiler_limit`.
    ///
    /// EVERY row and EVERY column comes from the one typed policy: the rows
    /// are [`crate::compiler_limits::ALL`] walked in order, the policy columns
    /// are each resource's descriptor, and the effective value is what the
    /// CALLING COMPILATION armed for that resource. The schema declares the
    /// table and nothing else: a row copied there by hand is a second
    /// authority that a later safety adjustment can leave stale while both
    /// sides still compile.
    ///
    /// The effective value is the caller's and not a fresh read of process
    /// policy, because those are different numbers whenever a host moves a
    /// setting after a compilation's arena is minted — and the catalog is
    /// supposed to answer the compilation reading it, not the next one.
    ///
    /// Best-effort by construction: a catalog that cannot be written must
    /// not fail the compilation, because publishing the policy is not the
    /// policy. The guards themselves read no SQLite.
    pub(crate) fn publish_compiler_limits(&self, armed: &crate::compiler_limits::ArmedLimits) {
        let Ok(conn) = self.bootstrap_connection.lock() else {
            return;
        };
        for kind in crate::compiler_limits::ALL.iter().copied() {
            let limit = kind.descriptor();
            let _ = conn.execute(
                "INSERT INTO compiler_limit
                     (name, default_value, effective_value, hard_ceiling, unit, error)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(name) DO UPDATE SET
                     default_value   = excluded.default_value,
                     effective_value = excluded.effective_value,
                     hard_ceiling    = excluded.hard_ceiling,
                     unit            = excluded.unit,
                     error           = excluded.error",
                rusqlite::params![
                    limit.name(),
                    limit.default_value() as i64,
                    armed.effective(kind) as i64,
                    limit.ceiling() as i64,
                    limit.unit(),
                    limit.error_identity(),
                ],
            );
        }
    }

    /// Materialize the typed effect plan's OBSERVATIONAL PROJECTION
    /// into the engine-owned sys::execution relations (effect_plan /
    /// effect_guard / effect_requirement — a normalized shape).
    /// Clear-then-insert is the lifecycle: rows persist after a run
    /// for post-mortem inspection and clear at the START of the next
    /// compile (the fresh-scratch-per-run precedent). Only the engine
    /// calls this; the rows execute nothing (the typed Rust plan
    /// stays the single executable source).
    pub fn materialize_effect_plan(
        &self,
        typed: &crate::pipeline::compiled_query::TypedEffectPlan,
    ) -> Result<()> {
        use crate::pipeline::compiled_query::GuardPolarity;
        let conn = self.get_bootstrap_connection();
        let guard = conn.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for effect-plan materialization",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        let step = |msg: &str, e: rusqlite::Error| {
            Runtime::catalog(format!("{msg}: {e}"), "effect-plan materialization")
        };
        // Atomic clear-and-replace: a mid-
        // materialization failure must not leave a partial "canonical"
        // projection behind.
        guard
            .execute_batch(
                "BEGIN; \
                 DELETE FROM effect_run; \
                 DELETE FROM effect_requirement; \
                 DELETE FROM effect_guard; \
                 DELETE FROM effect_plan;",
            )
            .map_err(|e| step("clearing the prior plan", e))?;
        let finish = |guard: &std::sync::MutexGuard<'_, Connection>, r: Result<()>| match r {
            Ok(()) => guard
                .execute_batch("COMMIT")
                .map_err(|e| step("committing", e)),
            Err(e) => {
                let _ = guard.execute_batch("ROLLBACK");
                Err(e)
            }
        };
        let body = (|| -> Result<()> {
            for (ordinal, s) in typed.schedule().iter().enumerate() {
                let (step_kind, action_kind) = s.kind().projection_kinds();
                guard
                    .execute(
                        "INSERT INTO effect_plan (plan_id, step_id, ordinal, occurrence_id, \
                     step_kind, action_kind, operation, route, sql_display) \
                     VALUES (1, ?1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        rusqlite::params![
                            ordinal as i64,
                            s.occurrence(),
                            step_kind,
                            action_kind,
                            s.operation(),
                            s.route(),
                            s.sql_display(),
                        ],
                    )
                    .map_err(|e| step("inserting a step", e))?;
                for r in s.requirements() {
                    guard
                        .execute(
                            "INSERT INTO effect_requirement (plan_id, step_id, guard_id, \
                         polarity, reason) VALUES (1, ?1, ?2, ?3, ?4)",
                            rusqlite::params![
                                ordinal as i64,
                                r.guard_id as i64,
                                match r.polarity {
                                    GuardPolarity::Present => "present",
                                    GuardPolarity::Absent => "absent",
                                },
                                r.reason,
                            ],
                        )
                        .map_err(|e| step("inserting a requirement edge", e))?;
                }
            }
            for g in &typed.guards {
                guard
                    .execute(
                        "INSERT INTO effect_guard (plan_id, guard_id, sql_display) \
                     VALUES (1, ?1, ?2)",
                        rusqlite::params![g.guard_id as i64, g.sql],
                    )
                    .map_err(|e| step("inserting a guard definition", e))?;
            }
            Ok(())
        })();
        finish(&guard, body)
    }

    /// Materialize the run's per-step outcomes:
    /// tracked in memory during the walk, written ONCE at the run's
    /// boundary, persisting for post-mortem inspection until the next
    /// compile clears them with the plan. The caller treats failure as
    /// best-effort — bookkeeping never outranks the run.
    pub fn materialize_effect_run(
        &self,
        outcomes: &[(&'static str, Option<String>)],
    ) -> Result<()> {
        let conn = self.get_bootstrap_connection();
        let guard = conn.lock().map_err(|e| {
            Runtime::poisoned(
                "Failed to acquire bootstrap lock for effect-run materialization",
                format!("Connection was poisoned: {}", e),
            )
        })?;
        // Atomic: a bookkeeping failure must not leave
        // a PARTIAL post-mortem behind — same boundary discipline as the
        // plan materializer.
        guard.execute_batch("BEGIN").map_err(|e| {
            Runtime::catalog(
                format!("beginning the run-outcome batch: {e}"),
                "effect-run materialization",
            )
        })?;
        let body = (|| -> Result<()> {
            for (step_id, (status, detail)) in outcomes.iter().enumerate() {
                guard
                    .execute(
                        "INSERT OR REPLACE INTO effect_run (plan_id, step_id, status, detail) \
                         VALUES (1, ?1, ?2, ?3)",
                        rusqlite::params![step_id as i64, status, detail],
                    )
                    .map_err(|e| {
                        Runtime::catalog(
                            format!("inserting a run outcome: {e}"),
                            "effect-run materialization",
                        )
                    })?;
            }
            Ok(())
        })();
        match body {
            Ok(()) => guard.execute_batch("COMMIT").map_err(|e| {
                Runtime::catalog(
                    format!("committing the run-outcome batch: {e}"),
                    "effect-run materialization",
                )
            }),
            Err(e) => {
                let _ = guard.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Append one assertion verdict to the session ledger. The ledger lives
    /// on bootstrap rather than the run's target connection, so a failing
    /// assertion remains observable after its target transaction rolls back.
    pub fn record_assertion_verdict(
        &self,
        verdict: &crate::pipeline::verdict::Verdict,
        run_id: &str,
    ) -> Result<()> {
        {
            let conn = self.bootstrap_connection.lock().map_err(|e| {
                Runtime::poisoned(
                    "Failed to acquire bootstrap lock for assertion recording",
                    format!("Connection was poisoned: {e}"),
                )
            })?;
            conn.execute(
                "INSERT INTO assertions \
                 (name, source_file, source_line, body, outcome, detail, run_id) \
                 VALUES (?1, NULL, NULL, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    verdict.identity.name,
                    verdict.identity.body_text,
                    match verdict.outcome {
                        crate::pipeline::verdict::VerdictOutcome::Pass => "pass",
                        crate::pipeline::verdict::VerdictOutcome::Fail => "fail",
                    },
                    verdict.detail,
                    run_id,
                ],
            )
            .map_err(|e| {
                Runtime::catalog(
                    format!("recording an assertion verdict: {e}"),
                    "assertion verdict materialization",
                )
            })?;
        }
        Ok(())
    }
}

// =============================================================================
// THE PUBLISHED COMPILER LIMITS
// =============================================================================
#[cfg(test)]
mod compiler_limit_publication_tests {
    use crate::compiler_limits::{
        ArmedLimits, CompilerLimit, ProcessLimitLease, ALL, NESTING, REFINEMENT_DEPTH,
    };
    use crate::system::{DelightQLSystem, ReadySystem};
    use delightql_types::introspect::{DatabaseIntrospector, DiscoveredEntity};
    use delightql_types::test_utils::MockDatabaseConnection;
    use delightql_types::Result;
    use std::sync::{Arc, Mutex};

    struct EmptyIntrospector;

    impl DatabaseIntrospector for EmptyIntrospector {
        fn introspect_entities(&self) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }

        fn introspect_entities_in_schema(&self, _schema: &str) -> Result<Vec<DiscoveredEntity>> {
            Ok(vec![])
        }
    }

    fn fresh_system() -> ReadySystem {
        let connection = Arc::new(Mutex::new(MockDatabaseConnection::new()));
        ReadySystem::new(connection, Box::new(EmptyIntrospector), "sqlite")
            .expect("fresh in-memory system should build")
    }

    /// One published row, read back whole.
    #[derive(Debug, PartialEq, Eq)]
    struct Row {
        name: String,
        default_value: i64,
        effective_value: i64,
        hard_ceiling: i64,
        unit: String,
        error: String,
    }

    fn published(system: &DelightQLSystem) -> Vec<Row> {
        let conn = system
            .bootstrap_connection()
            .lock()
            .expect("bootstrap lock");
        let mut statement = conn
            .prepare(
                "SELECT name, default_value, effective_value, hard_ceiling, unit, error
                 FROM compiler_limit ORDER BY rowid",
            )
            .expect("the schema declares the relation");
        let rows = statement
            .query_map([], |row| {
                Ok(Row {
                    name: row.get(0)?,
                    default_value: row.get(1)?,
                    effective_value: row.get(2)?,
                    hard_ceiling: row.get(3)?,
                    unit: row.get(4)?,
                    error: row.get(5)?,
                })
            })
            .expect("read the published policy")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("every column is NOT NULL");
        rows
    }

    fn described(limit: &CompilerLimit, effective: usize) -> Row {
        Row {
            name: limit.name().to_string(),
            default_value: limit.default_value() as i64,
            effective_value: effective as i64,
            hard_ceiling: limit.ceiling() as i64,
            unit: limit.unit().to_string(),
            error: limit.error_identity(),
        }
    }

    /// The catalog says what the guards enforce, FIELD FOR FIELD.
    ///
    /// This is the whole reason the policy is typed once. A default, ceiling,
    /// unit, identity or name that moved on only one side used to be a
    /// disagreement both halves still compiled through; here it is a failure
    /// naming the column.
    #[test]
    fn every_published_row_is_its_runtime_descriptor() {
        let _lease = ProcessLimitLease::take();
        let system = fresh_system();
        let armed = ArmedLimits::from_policy();
        system.publish_compiler_limits(&armed);

        let expected = vec![
            described(&NESTING, armed.nesting().levels()),
            described(&REFINEMENT_DEPTH, armed.refinement().max()),
        ];
        assert_eq!(
            published(&system),
            expected,
            "the catalog and the typed policy are one description"
        );
    }

    /// Every bounded resource has a row, and no row outlives its resource.
    /// A limit added to the typed policy without reaching publication would
    /// leave `compiler_limit(*)` a partial answer to a total question.
    #[test]
    fn the_published_rows_are_exactly_the_bounded_resources() {
        let _lease = ProcessLimitLease::take();
        let system = fresh_system();
        system.publish_compiler_limits(&ArmedLimits::from_policy());

        let published: Vec<String> = published(&system).into_iter().map(|row| row.name).collect();
        let described: Vec<String> = ALL
            .iter()
            .map(|kind| kind.descriptor().name().to_string())
            .collect();
        assert_eq!(published, described);
    }

    /// Publication is idempotent and repairs, rather than accumulating. A
    /// second write of ONE compilation's limits restores the row it already
    /// wrote, and does not add another.
    ///
    /// One `ArmedLimits` for both writes, under the lease: the claim is about
    /// republication, so re-arming between the two would make the comparison
    /// depend on whatever a neighbouring test had stored in the process cells
    /// in that instant.
    #[test]
    fn republishing_repairs_the_row_rather_than_adding_one() {
        let _lease = ProcessLimitLease::take();
        let system = fresh_system();
        let armed = ArmedLimits::from_policy();
        system.publish_compiler_limits(&armed);
        let once = published(&system);

        {
            let conn = system
                .bootstrap_connection()
                .lock()
                .expect("bootstrap lock");
            conn.execute(
                "UPDATE compiler_limit SET hard_ceiling = 1, unit = 'stale', error = 'stale'",
                [],
            )
            .expect("corrupt the published policy");
        }

        system.publish_compiler_limits(&armed);
        assert_eq!(
            published(&system),
            once,
            "a stale ceiling, unit or identity is corrected, not preserved"
        );
    }

}
