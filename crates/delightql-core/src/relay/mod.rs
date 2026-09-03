// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Daniel Eklund
// RelayParty — Front-End Seam (Epoch 6)
//
// RelayParty is the front-end seam: DQL in, protocol terms out.
// The back-end seam (SqlParty, SisoParty, etc.) handles SQL execution.
//
// Generic over T: Transport so it can wrap any backend party via the
// protocol stack. SqlParty uses streaming cursors (rusqlite); SisoParty
// uses the DatabaseConnection trait (eager, buffered).

use std::collections::HashMap;

use delightql_protocol::{
    ByteSeq, Cell, ClientTerm, CloseResponse, Dimension, ErrorKind, FetchResponse, Handle, Handler,
    MetaItem, Orientation, Projection, QueryHandle, QueryResponse, ServerTerm, Session, Transport,
};
#[cfg(not(target_arch = "wasm32"))]
use rusqlite;

use crate::{
    pipeline::{self, resolver::ResolutionConfig, verdict, Pipeline},
    system::DelightQLSystem,
};

/// Buffered eager results for non-streaming connections (bootstrap, imported).
struct EagerBuffer {
    dimensions: Vec<Dimension>,
    rows: Vec<Vec<Cell>>,
    cursor: usize,
}

/// Compiler-created relations one statement's execution staged, and the
/// statements that retire them.
///
/// Carried as a value so that no execution path can end without deciding
/// what becomes of them.
#[derive(Default)]
struct Staged {
    drops: Vec<String>,
    connection_id: Option<i64>,
}

#[cfg(test)]
mod tests;

mod entry;
mod pump;
#[cfg(test)]
mod pump_tests;

// --- Hooks ---

/// Hooks for non-relational side effects during query execution.
///
/// The CLI wires these to print verdicts, ship result sets, etc.
/// If no hook is set, the relay handles the effect internally (assertions
/// become protocol errors).
pub struct RelayHooks {
    /// Called for each assertion verdict (pass or fail).
    pub on_verdict: Option<Box<dyn FnMut(&verdict::Verdict)>>,

    /// Called when an error hook fires (compile-time or runtime).
    pub on_error_hook: Option<Box<dyn FnMut(&verdict::Verdict)>>,

    /// Called by the pump for each NON-FINAL shipped result set (`stdout!`),
    /// in execution order, as each entry executes: mid-run result sets ride
    /// the hook side channel; the FINAL shipped
    /// statement is the run's one wire response and never passes through
    /// here). Args: (columns, rows). If unset, non-final shipped sets are
    /// executed and discarded.
    /// Delivery order pinned by
    /// `pump_tests::non_final_shipped_deliver_via_on_ship_in_order`.
    pub on_ship: Option<Box<dyn FnMut(&[String], &[Vec<Cell>])>>,
}

/// The engine's `rusqlite` vocabulary read into the shared one. The
/// bootstrap store is a raw rusqlite connection rather than a
/// `DatabaseConnection`, and core cannot reach `delightql-backends`, so
/// this is where the bootstrap road joins the carrier every other road
/// already speaks.
#[cfg(not(target_arch = "wasm32"))]
fn bootstrap_value(value: rusqlite::types::Value) -> delightql_types::DbValue {
    use delightql_types::DbValue;
    match value {
        rusqlite::types::Value::Null => DbValue::Null,
        rusqlite::types::Value::Integer(i) => DbValue::Integer(i),
        rusqlite::types::Value::Real(f) => DbValue::Real(f),
        rusqlite::types::Value::Text(s) => DbValue::Text(s),
        rusqlite::types::Value::Blob(b) => DbValue::Blob(b),
    }
}

/// Whether a check's one cell says yes. An absent cell is not a yes: a
/// check that answered NULL did not hold.
fn cell_says_yes(row: Option<&Vec<Cell>>) -> bool {
    row.and_then(|r| r.first())
        .and_then(|cell| cell.as_deref())
        .map(|bytes| matches!(bytes, b"1" | b"true" | b"t"))
        .unwrap_or(false)
}

/// The cardinality a `count(*)` probe reports, when it reports one.
fn cell_count(row: Option<&Vec<Cell>>) -> Option<i64> {
    let bytes = row.and_then(|r| r.first())?.as_deref()?;
    std::str::from_utf8(bytes).ok()?.trim().parse::<i64>().ok()
}

impl Default for RelayHooks {
    fn default() -> Self {
        Self {
            on_verdict: None,
            on_error_hook: None,
            on_ship: None,
        }
    }
}

// --- RelayParty ---

pub struct RelayParty<'a, T: Transport> {
    system: &'a mut DelightQLSystem,
    sql_session: Session<T>,
    handles: HashMap<Handle, QueryHandle>, // frontend handle → backend QueryHandle
    eager_buffers: HashMap<Handle, EagerBuffer>, // frontend handle → eager results
    /// What a live result still owes: the compiler-created relations its
    /// statement staged, waiting for the rows to be done with.
    staged_by_handle: HashMap<Handle, Staged>,
    /// The submission each live handle came from, so an error reported at
    /// fetch or close names the input that caused it.
    text_by_handle: HashMap<Handle, String>,
    /// The submission being handled right now; `next_handle` binds it to
    /// the handle it mints.
    current_text: Option<String>,
    next_handle_id: u64,
    next_effect_run_id: u64,
    danger_overrides: Vec<pipeline::ast_unresolved::DangerSpec>,
    option_overrides: Vec<pipeline::ast_unresolved::OptionSpec>,
    sql_optimization_level: pipeline::sql_optimizer::OptimizationLevel,
    /// Whether the most recent typed-plan run took its exit! latch —
    /// read by the F5 receipt binder (a NO run ships the empty receipt).
    last_run_exited: bool,
    hooks: RelayHooks,
}

/// A refusal weighed against what the submission DECLARED it expects.
///
/// `None` means the submission declared nothing, so the refusal is the
/// caller's ordinary business. The hook is read from the tree because the
/// road that normally collects it is the road that just refused.
fn judge_declared(
    tree: &crate::pipeline::syntax::SyntaxTree,
    owner: Option<&std::ops::Range<usize>>,
    error: &crate::error::DelightQLError,
) -> Option<GoalRefusal> {
    // A HOOK BELONGS TO THE QUERY IT STANDS IN. A prompt carries one goal, so
    // that goal's extent is the only place a declaration about it can be —
    // and a text that shows no single goal shows no owner, which is the
    // closed answer.
    let expected = crate::pipeline::normalize::declared_error_within(tree, owner?)?;
    let actual = error.error_uri();
    if expected.matches(&actual) {
        return Some(GoalRefusal::AsDeclared {
            declared: expected.display_uri(),
            detail: format!("{actual}: {error}"),
        });
    }
    Some(GoalRefusal::Reported(ServerTerm::Error {
        kind: ErrorKind::Constraint,
        identity: actual.into_bytes(),
        message: format!("expected error {} but got: {error}", expected.display_uri()).into_bytes(),
    }))
}

/// What reading ONE goal can end in, short of the goal.
///
/// `Reported` is a term the caller sends on. `AsDeclared` is the submission
/// getting exactly the refusal it declared — an outcome, not a failure, and
/// the one the caller answers with an empty result.
enum GoalRefusal {
    Reported(ServerTerm),
    /// The refusal the submission declared. The verdict is still OWED — a
    /// judged outcome that reports nothing is indistinguishable from a
    /// statement that simply ran.
    AsDeclared {
        declared: String,
        detail: String,
    },
}

impl<'a, T: Transport> RelayParty<'a, T> {
    pub fn new(system: &'a mut DelightQLSystem, sql_session: Session<T>) -> Self {
        RelayParty {
            system,
            sql_session,
            handles: HashMap::new(),
            eager_buffers: HashMap::new(),
            staged_by_handle: HashMap::new(),
            text_by_handle: HashMap::new(),
            current_text: None,
            next_handle_id: 1,
            next_effect_run_id: 1,
            danger_overrides: Vec::new(),
            option_overrides: Vec::new(),
            sql_optimization_level: pipeline::sql_optimizer::OptimizationLevel::Basic,
            last_run_exited: false,
            hooks: RelayHooks::default(),
        }
    }

    /// Install the side-channel hooks (verdicts, shipped sets).
    /// Production caller: `open.rs::session_with_hooks` — the CLI's console
    /// sink for `stdout!` rides through it; also exercised by
    /// `relay/pump_tests.rs`.
    pub fn set_hooks(&mut self, hooks: RelayHooks) {
        self.hooks = hooks;
    }

    /// Install the session-baseline danger overrides (CLI `--danger`).
    /// Specs arrive already validated (`parse_cli_danger_spec` refuses
    /// unknown gates and non-CLI-overridable ones); each query's pipeline
    /// re-validates as defense in depth.
    pub fn set_danger_overrides(&mut self, specs: Vec<pipeline::ast_unresolved::DangerSpec>) {
        self.danger_overrides = specs;
    }

    /// Handle a Reset control operation: close all open handles and reinit the system.
    pub fn handle_reset(&mut self) -> Result<(), crate::error::DelightQLError> {
        for (_frontend, backend) in self.handles.drain() {
            let _ = self.sql_session.close(backend);
        }
        for staged in std::mem::take(&mut self.staged_by_handle).into_values() {
            self.retire_staged(staged);
        }
        self.eager_buffers.clear();
        self.next_handle_id = 1;
        self.system.reinit_bootstrap()
    }

    /// The ONE goal a protocol Query term carries.
    ///
    /// A term is one statement by contract, so an unmarked submission takes
    /// the prompt entrance and a second statement has no derivation in it. When
    /// that is why the parse failed, the sequence entrance is asked — not to
    /// run the text, but so the refusal can say "send each query as its own
    /// term" instead of pointing at a syntax error the author did not make.
    fn read_one_goal(
        &self,
        dql: &str,
        registry: &std::rc::Rc<crate::names::Registry>,
    ) -> std::result::Result<crate::pipeline::normalize::Goal, GoalRefusal> {
        let syntax_error = |error: crate::error::DelightQLError| ServerTerm::Error {
            kind: ErrorKind::Syntax,
            identity: error.error_uri().into_bytes(),
            message: error.to_string().into_bytes(),
        };
        let tree = match pipeline::parse::submission_attributed(dql, registry.limits().nesting()) {
            Ok(tree) => tree,
            Err(refusal) => {
                if let Some(count) = query_count_if_a_sequence(dql) {
                    if count > 1 {
                        return Err(GoalRefusal::Reported(ServerTerm::Error {
                            kind: ErrorKind::Syntax,
                            identity: b"delightql-error://parse/multi_query".to_vec(),
                            message: format!(
                                "multi-query input rejected: found {count} queries in a single \
                                 Query term (send each query as a separate Query message)"
                            )
                            .into_bytes(),
                        }));
                    }
                }
                // A defective parse still carries the declaration: an error
                // hook DECORATES a position, and the position it decorates is
                // usually not the part that failed to read. The extent that
                // chose the message is the extent that owns it — the entrance
                // decided both at once.
                if let Some(judgment) =
                    judge_declared(&refusal.tree, refusal.query.as_ref(), &refusal.error)
                {
                    return Err(judgment);
                }
                return Err(GoalRefusal::Reported(syntax_error(refusal.error)));
            }
        };
        // A REFUSAL THE SUBMISSION DECLARED IS THE SUBMISSION'S OWN OUTCOME.
        // Normalization is where the error hook is ordinarily collected, so a
        // refusal made DURING it would otherwise reach the caller with the
        // declaration it was meant to be judged against still unread. The
        // hook DECORATES a position and is never a step, so it is read from
        // the tree and the refusal is weighed the way every other one is.
        // A goal that PARSED shows its own extent; a normalization refusal
        // inside it belongs to it and to nothing else.
        let owner = pipeline::parse::submission_extent(&tree);
        let judged = |error: crate::error::DelightQLError| {
            judge_declared(&tree, owner.as_ref(), &error)
                .unwrap_or_else(|| GoalRefusal::Reported(syntax_error(error)))
        };
        let normalized =
            pipeline::normalize::submission(&tree, std::rc::Rc::clone(registry)).map_err(judged)?;
        pipeline::one_goal(normalized).map_err(|error| GoalRefusal::Reported(syntax_error(error)))
    }

    fn handle_query(&mut self, text: ByteSeq) -> ServerTerm {
        if let Err(error) = self.system.require_healthy() {
            return ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: error.error_uri().into_bytes(),
                message: error.to_string().into_bytes(),
            };
        }

        let dql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(e) => {
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: vec![],
                    message: format!("invalid UTF-8 in query text: {}", e).into_bytes(),
                }
            }
        };

        // ONE reading of the submission. The error hook, the effect-entry
        // classification and the compilation all ask questions about the same
        // goal, and asking the parser three times is how they come to
        // disagree.
        let registry = crate::relation::Planning::open(crate::names::Registry::new(&[]));
        let goal = match self.read_one_goal(&dql, registry.shared()) {
            Ok(goal) => goal,
            Err(GoalRefusal::Reported(term)) => return term,
            Err(GoalRefusal::AsDeclared { declared, detail }) => {
                if let Some(ref mut hook) = self.hooks.on_error_hook {
                    hook(&verdict::Verdict {
                        outcome: verdict::VerdictOutcome::Pass,
                        identity: verdict::VerdictIdentity {
                            name: None,
                            body_text: declared,
                        },
                        detail: Some(detail),
                    });
                }
                return self.empty_header_response();
            }
        };

        // Error hook path: handle both compile-time and runtime error hooks
        if let Some(expected) = goal.declared.expected_error.clone() {
            return self.handle_error_hook_query(&dql, goal, expected, registry);
        }

        // The effect-chain entry points: run!/run_namespace!/query-position
        // directives take the transformer → pump road. The classifier
        // declines annotated statements, and DML/DDL statements under CLI
        // danger/option overrides keep the ordinary compilation path (the
        // plan compiler applies default gates only) — see relay/entry.rs.
        let allow_adhoc = self.danger_overrides.is_empty() && self.option_overrides.is_empty();
        let goal = match entry::classify_effect_entry(goal, allow_adhoc) {
            Ok(effect_entry) => return self.handle_effect_entry(effect_entry),
            Err(goal) => goal,
        };

        // Normal single-query path: compile DQL → SQL via the pipeline
        let mut pipeline = Pipeline::from_goal(
            goal,
            &dql,
            &mut *self.system,
            ResolutionConfig::default(),
            self.sql_optimization_level,
            registry,
        );

        // Apply CLI-level overrides
        if let Err(e) = pipeline.set_cli_danger_overrides(self.danger_overrides.clone()) {
            return ServerTerm::Error {
                kind: ErrorKind::Syntax,
                identity: e.error_uri().into_bytes(),
                message: format!("{}", e).into_bytes(),
            };
        }
        pipeline.set_cli_option_overrides(self.option_overrides.clone());

        let compiled = match pipeline.compile() {
            Ok(c) => c,
            Err(e) => {
                return ServerTerm::Error {
                    kind: ErrorKind::Syntax,
                    identity: e.error_uri().into_bytes(),
                    message: format!("{}", e).into_bytes(),
                }
            }
        };

        let compiled = compiled;
        // Drop pipeline to release borrow on self.system
        drop(pipeline);

        let (term, staged) = self.execute_compiled(compiled);
        self.settle_staged(term, staged)
    }

    /// Run one compiled statement: its authored preconditions, the staging
    /// its source needs, the checks it may not run without, and the
    /// statement itself — handing back whatever it staged so the caller can
    /// settle it.
    ///
    /// The order is the plan's order. Authored assertions come FIRST: a
    /// false precondition is the program's own answer, and it must be
    /// reached before a volatile or external source has been evaluated or
    /// any compiler state created.
    fn execute_compiled(
        &mut self,
        compiled: crate::pipeline::compiled_query::CompiledQuery,
    ) -> (ServerTerm, Staged) {
        let obligations = compiled.obligations;
        let prepare_sqls = compiled.prepare_sqls;
        let connection_id = compiled.connection_id;
        let primary_sql = compiled.primary_sql;
        let compiled_cleanup = compiled.cleanup_sqls;
        let mut staged = Staged {
            drops: Vec::new(),
            connection_id,
        };

        // Only now is the source evaluated: staged once, so the check and
        // the statement read one relation. Two evaluations of one source are
        // two relations whenever it is volatile, reads outside this engine,
        // or is written concurrently.
        staged.drops = compiled_cleanup;
        if let Err(refusal) = self.stage_source(&prepare_sqls, connection_id) {
            return (refusal, staged);
        }

        if let Some(refusal) = self.unmet_obligation(&obligations, connection_id) {
            return (refusal, staged);
        }

        // Route primary SQL based on connection_id
        let cid = connection_id.unwrap_or(2);
        let term = if cid == 2 {
            // Streaming path: forward to sql_session
            let sql_bytes = primary_sql.as_bytes().to_vec();
            match self.sql_session.query(sql_bytes) {
                Ok(QueryResponse::Header {
                    handle: backend_handle,
                    dimensions,
                }) => {
                    let frontend_handle = self.next_handle();
                    self.handles.insert(frontend_handle.clone(), backend_handle);
                    ServerTerm::Header {
                        handle: frontend_handle,
                        dimensions,
                    }
                }
                Ok(QueryResponse::Error {
                    kind,
                    identity,
                    message,
                }) => ServerTerm::Error {
                    kind,
                    identity,
                    message,
                },
                Err(e) => ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: b"delightql-error://runtime/execution".to_vec(),
                    message: teach_runtime_message(e.message).into_bytes(),
                },
            }
        } else {
            // Eager path: execute on bootstrap or imported connection, buffer results
            match self.execute_sql_routed(&primary_sql, connection_id) {
                Ok((columns, rows)) => {
                    let dimensions = Self::eager_dimensions(&columns);
                    let handle = self.next_handle();
                    self.eager_buffers.insert(
                        handle.clone(),
                        EagerBuffer {
                            dimensions: dimensions.clone(),
                            rows,
                            cursor: 0,
                        },
                    );
                    ServerTerm::Header { handle, dimensions }
                }
                Err(msg) => ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: b"delightql-error://runtime/execution".to_vec(),
                    message: teach_runtime_message(msg).into_bytes(),
                },
            }
        };
        (term, staged)
    }

    /// Stage what a statement reads. `Err` is the refusal to return.
    fn stage_source(
        &mut self,
        prepare_sqls: &[String],
        connection_id: Option<i64>,
    ) -> std::result::Result<(), ServerTerm> {
        for sql in prepare_sqls {
            if let Err(msg) = self.execute_sql_routed(sql, connection_id) {
                return Err(ServerTerm::Error {
                    kind: ErrorKind::Permission,
                    identity: b"delightql-error://runtime/execution".to_vec(),
                    message: format!("staging the statement's source failed: {msg}").into_bytes(),
                });
            }
        }
        Ok(())
    }

    /// Read what a statement may not run without. `Some` is the refusal.
    ///
    /// Nobody wrote these — the compiler attached them because the
    /// statement's meaning depends on a fact about the data — so a false
    /// verdict is the statement being REFUSED, under its own identifier,
    /// before it runs.
    fn unmet_obligation(
        &mut self,
        obligations: &[crate::pipeline::compiled_query::CompiledObligation],
        connection_id: Option<i64>,
    ) -> Option<ServerTerm> {
        for obligation in obligations {
            match self.execute_sql_routed(&obligation.sql, connection_id) {
                Ok((_cols, rows)) => {
                    let held = cell_says_yes(rows.first());
                    if !held {
                        return Some(ServerTerm::Error {
                            kind: ErrorKind::Permission,
                            identity: format!("delightql-error://{}", obligation.refusal.identity)
                                .into_bytes(),
                            message: obligation.refusal.message.clone().into_bytes(),
                        });
                    }
                }
                Err(msg) => {
                    return Some(ServerTerm::Error {
                        kind: ErrorKind::Permission,
                        identity: b"delightql-error://runtime/execution".to_vec(),
                        message: format!(
                            "the check this statement may not run without could not be \
                             evaluated: {msg}"
                        )
                        .into_bytes(),
                    })
                }
            }
        }
        None
    }

    /// The compiler-created relations a statement's execution left behind,
    /// and the statements that retire them.
    ///
    /// It travels WITH the outcome rather than beside it: `execute_compiled`
    /// cannot return without handing this back, and one place decides its
    /// fate — retired now, or owed by the result that is still reading it.
    /// A new early return therefore cannot forget it by omission.
    fn retire_staged(&mut self, staged: Staged) {
        for sql in &staged.drops {
            // A retirement that fails leaves a relation the next run of the
            // same statement drops before it creates. Nothing the program
            // asked for depends on it, so it is not an answer.
            let _ = self.execute_sql_routed(sql, staged.connection_id);
        }
    }

    /// Settle what an outcome owes: a result still being read keeps its
    /// staged relations until it is exhausted or closed; everything else —
    /// success without a handle, a refusal, an error — retires them now.
    fn settle_staged(&mut self, term: ServerTerm, staged: Staged) -> ServerTerm {
        if staged.drops.is_empty() {
            return term;
        }
        match &term {
            ServerTerm::Header { handle, .. } => {
                self.staged_by_handle.insert(handle.clone(), staged);
            }
            _ => self.retire_staged(staged),
        }
        term
    }

    /// Judge an ordinary execution's outcome against an error hook.
    ///
    /// The hook judges; it does not execute. The statement has already run
    /// exactly as an unannotated one does — authored assertions, staging,
    /// the checks it may not run without, the statement, its handle and its
    /// cleanup — and all that is left is to compare what came back with what
    /// was expected. A second choreography here is how an annotation came to
    /// change the way a statement executes.
    ///
    /// `Ok` is a matched expectation. `Err` carries the refusal to return.
    fn judge_against_hook(
        &mut self,
        term: ServerTerm,
        expected: &verdict::ExpectedError,
        identity: verdict::VerdictIdentity,
    ) -> std::result::Result<(), ServerTerm> {
        // A streaming result reports its engine failures while it is being
        // READ, not when it is opened. Judging the outcome therefore means
        // reading it: an unannotated client would meet the same error on its
        // first fetch, and a hook that judged the header alone would call a
        // failing statement a success.
        let term = match term {
            ServerTerm::Header { handle, dimensions } => match self.drain_for_verdict(&handle) {
                Some(failure) => {
                    let _ = self.handle_close(handle);
                    failure
                }
                None => ServerTerm::Header { handle, dimensions },
            },
            other => other,
        };
        let (outcome, detail) = match &term {
            ServerTerm::Error {
                identity: uri,
                message,
                ..
            } => {
                // An engine that refused and named nothing is the one
                // failure this system has no minted identity for; it is
                // reported as a bug in the statement rather than as an
                // anonymous error, which is the name the hook road has
                // always judged it under.
                let actual = if uri.is_empty() {
                    "delightql-error://runtime/bug".to_string()
                } else {
                    String::from_utf8_lossy(uri).to_string()
                };
                (
                    expected.matches(&actual),
                    format!("{}: {}", actual, String::from_utf8_lossy(message)),
                )
            }
            _ => (false, "statement succeeded; expected an error".to_string()),
        };
        // A result nobody will read still owes what it staged, and closing
        // it is what pays that.
        if let ServerTerm::Header { handle, .. } = &term {
            let handle = handle.clone();
            let _ = self.handle_close(handle);
        }
        let v = verdict::Verdict {
            outcome: if outcome {
                verdict::VerdictOutcome::Pass
            } else {
                verdict::VerdictOutcome::Fail
            },
            identity,
            detail: Some(detail.clone()),
        };
        if let Some(ref mut hook) = self.hooks.on_error_hook {
            hook(&v);
        }
        if outcome {
            return Ok(());
        }
        Err(ServerTerm::Error {
            kind: ErrorKind::Constraint,
            identity: expected.display_uri().into_bytes(),
            message: match &term {
                ServerTerm::Error { .. } => {
                    format!(
                        "expected error {} but got: {}",
                        expected.display_uri(),
                        detail
                    )
                }
                _ => "statement succeeded; expected an error".to_string(),
            }
            .into_bytes(),
        })
    }

    /// Read a result to its end, reporting the first failure it meets.
    ///
    /// Only for judging an error hook. An eager result has already met any
    /// failure at execution, so it has nothing left to report here.
    fn drain_for_verdict(&mut self, handle: &Handle) -> Option<ServerTerm> {
        if self.eager_buffers.contains_key(handle) {
            return None;
        }
        let backend = self.handles.get(handle)?;
        let agreed = self.sql_session.agreed_orientation(Orientation::Rows)?;
        loop {
            match self
                .sql_session
                .fetch(backend, Projection::All, u64::MAX, agreed)
            {
                Ok(FetchResponse::Data { .. }) => {}
                Ok(FetchResponse::End) => return None,
                Ok(FetchResponse::Error {
                    kind,
                    identity,
                    message,
                }) => {
                    return Some(ServerTerm::Error {
                        kind,
                        identity,
                        message,
                    })
                }
                Err(e) => {
                    return Some(ServerTerm::Error {
                        kind: ErrorKind::Connection,
                        identity: b"delightql-error://runtime/execution".to_vec(),
                        message: teach_runtime_message(e.message).into_bytes(),
                    })
                }
            }
        }
    }

    /// Retire what a handle owed, if anything. Called where a result stops
    /// being read: exhaustion, close, and reset.
    fn retire_handle_staging(&mut self, handle: &Handle) {
        if let Some(staged) = self.staged_by_handle.remove(handle) {
            self.retire_staged(staged);
        }
    }

    /// Handle a single query with an error hook annotation.
    ///
    /// Supports both compile-time error hooks (query fails to compile) and
    /// runtime error hooks (query compiles but fails at execution or assertion).
    fn handle_error_hook_query(
        &mut self,
        dql: &str,
        goal: crate::pipeline::normalize::Goal,
        expected: verdict::ExpectedError,
        registry: crate::relation::Planning,
    ) -> ServerTerm {
        let identity = verdict::VerdictIdentity {
            name: None,
            body_text: expected.display_uri(),
        };

        // ANNOTATION TRANSPARENCY: an error hook must not change
        // how a statement EXECUTES — only how its outcome is judged. A
        // statement the effect chain would own (a directive tail) takes the
        // effect chain here too, so its diagnostic class is the same with
        // and without the annotation. Its error (or unexpected success) is
        // matched against the expected URI exactly like the pipeline path.
        let allow_adhoc = self.danger_overrides.is_empty() && self.option_overrides.is_empty();
        let goal = match entry::classify_effect_entry(goal, allow_adhoc) {
            Err(goal) => goal,
            Ok(effect_entry) => {
                let term = self.handle_effect_entry(effect_entry);
                return match term {
                    ServerTerm::Error {
                        identity: err_identity,
                        message,
                        ..
                    } => {
                        let actual_uri = String::from_utf8_lossy(&err_identity).to_string();
                        let v = verdict::Verdict {
                            outcome: if expected.matches(&actual_uri) {
                                verdict::VerdictOutcome::Pass
                            } else {
                                verdict::VerdictOutcome::Fail
                            },
                            identity,
                            detail: Some(format!(
                                "{}: {}",
                                actual_uri,
                                String::from_utf8_lossy(&message)
                            )),
                        };
                        if let Some(ref mut hook) = self.hooks.on_error_hook {
                            hook(&v);
                        }
                        match v.outcome {
                            verdict::VerdictOutcome::Pass => self.empty_header_response(),
                            _ => ServerTerm::Error {
                                kind: ErrorKind::Constraint,
                                identity: actual_uri.into_bytes(),
                                message: format!(
                                    "expected error {} but got: {}",
                                    expected.display_uri(),
                                    String::from_utf8_lossy(&message)
                                )
                                .into_bytes(),
                            },
                        }
                    }
                    other => {
                        // The statement succeeded where an error was expected.
                        let v = verdict::Verdict {
                            outcome: verdict::VerdictOutcome::Fail,
                            identity,
                            detail: Some("statement succeeded; expected an error".to_string()),
                        };
                        if let Some(ref mut hook) = self.hooks.on_error_hook {
                            hook(&v);
                        }
                        let _ = other;
                        ServerTerm::Error {
                            kind: ErrorKind::Constraint,
                            identity: expected.display_uri().into_bytes(),
                            message: "statement succeeded; expected an error"
                                .to_string()
                                .into_bytes(),
                        }
                    }
                };
            }
        };

        // Try to compile the query
        let mut pipeline = Pipeline::from_goal(
            goal,
            dql,
            &mut *self.system,
            ResolutionConfig::default(),
            self.sql_optimization_level,
            registry,
        );
        if let Err(e) = pipeline.set_cli_danger_overrides(self.danger_overrides.clone()) {
            return ServerTerm::Error {
                kind: ErrorKind::Syntax,
                identity: e.error_uri().into_bytes(),
                message: format!("{}", e).into_bytes(),
            };
        }
        pipeline.set_cli_option_overrides(self.option_overrides.clone());

        let compiled = match pipeline.compile() {
            Err(e) => {
                // Compile error — match against expected error URI
                let actual_uri = e.error_uri();
                let v = verdict::Verdict {
                    outcome: if expected.matches(&actual_uri) {
                        verdict::VerdictOutcome::Pass
                    } else {
                        verdict::VerdictOutcome::Fail
                    },
                    identity,
                    detail: Some(format!("{}: {}", actual_uri, e)),
                };
                if let Some(ref mut hook) = self.hooks.on_error_hook {
                    hook(&v);
                }
                return self.verdict_response(&v);
            }
            Ok(c) => c,
        };

        // The annotation judges; it does not execute. The statement runs
        // through the one execution authority, exactly as an unannotated one
        // does, and the hook compares the outcome with what was expected.
        let (term, staged) = self.execute_compiled(compiled);
        let term = self.settle_staged(term, staged);
        match self.judge_against_hook(term, &expected, identity) {
            Ok(()) => self.empty_header_response(),
            Err(refusal) => refusal,
        }
    }

    fn handle_fetch(
        &mut self,
        handle: Handle,
        projection: Projection,
        count: u64,
        orientation: Orientation,
    ) -> ServerTerm {
        // Check eager buffers first (bootstrap/imported connections)
        if let Some(buffer) = self.eager_buffers.get_mut(&handle) {
            if buffer.cursor >= buffer.rows.len() {
                // The rows are done with; whatever the statement staged for
                // them can go.
                self.retire_handle_staging(&handle);
                return ServerTerm::End;
            }
            let end = std::cmp::min(buffer.cursor + count as usize, buffer.rows.len());
            let batch = buffer.rows[buffer.cursor..end].to_vec();
            buffer.cursor = end;
            return ServerTerm::Data { cells: batch };
        }

        // Streaming path: forward to sql_session
        let backend_handle = match self.handles.get(&handle) {
            Some(bh) => bh,
            None => {
                return ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"unknown handle".to_vec(),
                }
            }
        };

        let agreed = match self.sql_session.agreed_orientation(orientation) {
            Some(a) => a,
            None => {
                return ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: vec![],
                    message: b"orientation not agreed".to_vec(),
                }
            }
        };

        let fetched = self
            .sql_session
            .fetch(backend_handle, projection, count, agreed);
        // Both endings are endings: no further row comes back from a fetch
        // that returned End or reported an error, so what the statement
        // staged for those rows can go. A later Close still arrives and
        // finds nothing left to retire.
        if matches!(
            fetched,
            Ok(FetchResponse::End) | Ok(FetchResponse::Error { .. }) | Err(_)
        ) {
            self.retire_handle_staging(&handle);
        }
        match fetched {
            Ok(FetchResponse::Data { cells }) => ServerTerm::Data { cells },
            Ok(FetchResponse::End) => ServerTerm::End,
            Ok(FetchResponse::Error {
                kind,
                identity,
                message,
            }) => ServerTerm::Error {
                kind,
                identity,
                message,
            },
            Err(e) => ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: b"delightql-error://runtime/execution".to_vec(),
                message: teach_runtime_message(e.message).into_bytes(),
            },
        }
    }

    fn handle_stat(&self, handle: Handle) -> ServerTerm {
        if self.eager_buffers.contains_key(&handle) {
            return ServerTerm::Metadata {
                items: vec![MetaItem::Backend(
                    b"sqlite".to_vec(),
                    b"relay-eager".to_vec(),
                )],
            };
        }
        if !self.handles.contains_key(&handle) {
            return ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b"unknown handle".to_vec(),
            };
        }
        ServerTerm::Metadata {
            items: vec![MetaItem::Backend(
                b"sqlite".to_vec(),
                b"relay-epoch6".to_vec(),
            )],
        }
    }

    fn handle_close(&mut self, handle: Handle) -> ServerTerm {
        self.retire_handle_staging(&handle);
        // Check eager buffers first
        if self.eager_buffers.remove(&handle).is_some() {
            return ServerTerm::Ok { count_hint: 0 };
        }

        match self.handles.remove(&handle) {
            Some(backend_handle) => match self.sql_session.close(backend_handle) {
                Ok(CloseResponse::Ok) => ServerTerm::Ok { count_hint: 0 },
                Ok(CloseResponse::Error {
                    kind,
                    identity,
                    message,
                }) => ServerTerm::Error {
                    kind,
                    identity,
                    message,
                },
                Err(e) => ServerTerm::Error {
                    kind: ErrorKind::Connection,
                    identity: b"delightql-error://runtime/execution".to_vec(),
                    message: teach_runtime_message(e.message).into_bytes(),
                },
            },
            None => ServerTerm::Error {
                kind: ErrorKind::Connection,
                identity: vec![],
                message: b"unknown handle".to_vec(),
            },
        }
    }

    fn next_handle(&mut self) -> Handle {
        let id = self.next_handle_id;
        self.next_handle_id += 1;
        let handle: Handle = format!("h{}", id).into_bytes();
        if let Some(text) = &self.current_text {
            self.text_by_handle.insert(handle.clone(), text.clone());
        }
        handle
    }

    /// The session boundary is where every error a client sees crosses,
    /// and the ONE place a reported error becomes a
    /// `sys::diagnostics.finding` row: an error a hook consumed is data,
    /// not a finding; an error the client is about to receive is.
    fn record_reported_error(&self, term: &ServerTerm, input: Option<&str>) {
        let ServerTerm::Error {
            identity, message, ..
        } = term
        else {
            return;
        };
        let uri = String::from_utf8_lossy(identity);
        let uri: &str = if uri.is_empty() {
            crate::uri_registry::INTERNAL_UNBADGED
        } else {
            &uri
        };
        self.system.record_finding(
            crate::diagnostics::Severity::Error,
            uri,
            &String::from_utf8_lossy(message),
            input,
            "session",
        );
    }

    /// Return the appropriate protocol response for an error hook verdict.
    /// Pass → empty header (the hook matched). Fail → protocol error.
    fn verdict_response(&mut self, v: &verdict::Verdict) -> ServerTerm {
        match v.outcome {
            verdict::VerdictOutcome::Pass => self.empty_header_response(),
            verdict::VerdictOutcome::Fail => ServerTerm::Error {
                kind: ErrorKind::Constraint,
                identity: vec![],
                message: v
                    .detail
                    .as_deref()
                    .unwrap_or("Error hook verdict: FAIL")
                    .as_bytes()
                    .to_vec(),
            },
        }
    }

    fn empty_header_response(&mut self) -> ServerTerm {
        let handle = self.next_handle();
        self.eager_buffers.insert(
            handle.clone(),
            EagerBuffer {
                dimensions: vec![],
                rows: vec![],
                cursor: 0,
            },
        );
        ServerTerm::Header {
            handle,
            dimensions: vec![],
        }
    }

    // --- Connection routing ---

    /// Execute SQL eagerly on the bootstrap connection (connection_id=1).
    #[cfg(not(target_arch = "wasm32"))]
    fn execute_eager_on_bootstrap(
        &self,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<Cell>>), String> {
        let conn = self.system.get_bootstrap_connection();
        let conn_guard = conn.lock().map_err(|e| format!("Bootstrap lock: {}", e))?;
        let mut stmt = conn_guard
            .prepare(sql)
            .map_err(|e| format!("Bootstrap prepare: {}", e))?;
        let col_count = stmt.column_count();
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let rows_result = stmt
            .query_map([], |row| {
                let mut values = Vec::with_capacity(col_count);
                for idx in 0..col_count {
                    values.push(bootstrap_value(row.get(idx)?).into_wire_bytes());
                }
                Ok(values)
            })
            .map_err(|e| format!("Bootstrap query: {}", e))?;
        let mut result_rows = Vec::new();
        for r in rows_result {
            result_rows.push(r.map_err(|e| format!("Bootstrap fetch: {}", e))?);
        }
        Ok((column_names, result_rows))
    }

    /// Execute SQL eagerly on an imported connection (connection_id >= 3).
    fn execute_eager_on_imported(
        &self,
        sql: &str,
        connection_id: i64,
    ) -> Result<(Vec<String>, Vec<Vec<Cell>>), String> {
        let conn_arc = self
            .system
            .get_connection(connection_id)
            .map_err(|e| format!("{}", e))?;
        let conn_guard = conn_arc
            .lock()
            .map_err(|e| format!("Connection {} lock: {}", connection_id, e))?;
        let (columns, rows) = conn_guard
            .query_all_rows(sql, &[])
            .map_err(|e| format!("{}", e))?;
        Ok((
            columns,
            rows.into_iter()
                .map(|row| row.into_iter().map(|v| v.into_wire_bytes()).collect())
                .collect(),
        ))
    }

    /// Execute SQL on the appropriate connection based on connection_id.
    ///
    /// - `None` or `2`: route through the streaming backend protocol (sql_session)
    /// - `1`: execute eagerly on the bootstrap connection
    /// - `>= 3`: execute eagerly on an imported connection
    fn execute_sql_routed(
        &mut self,
        sql: &str,
        connection_id: Option<i64>,
    ) -> Result<(Vec<String>, Vec<Vec<Cell>>), String> {
        match connection_id.unwrap_or(2) {
            2 => self.execute_eager_through_protocol(sql),
            1 => {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    self.execute_eager_on_bootstrap(sql)
                }
                #[cfg(target_arch = "wasm32")]
                {
                    let _ = sql;
                    Err("bootstrap queries not supported on wasm32".to_string())
                }
            }
            id => self.execute_eager_on_imported(sql, id),
        }
    }

    /// The heading an eagerly-buffered result answers with.
    ///
    /// The cells are already the engine's own answer — an eager result is
    /// buffered, never re-read — so only the heading is built here.
    fn eager_dimensions(columns: &[String]) -> Vec<Dimension> {
        columns
            .iter()
            .enumerate()
            .map(|(i, name)| Dimension {
                position: i as u64,
                name: name.as_bytes().to_vec(),
                descriptor: b"TEXT".to_vec(),
            })
            .collect()
    }

    /// Execute SQL through the backend protocol and return (columns, rows).
    fn execute_eager_through_protocol(
        &mut self,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<Cell>>), String> {
        let rows_orient = self
            .sql_session
            .agreed_orientation(Orientation::Rows)
            .ok_or_else(|| "Rows orientation not agreed".to_string())?;

        let resp = self
            .sql_session
            .query(sql.as_bytes().to_vec())
            .map_err(|e| e.message)?;

        let (handle, dimensions) = match resp {
            QueryResponse::Header { handle, dimensions } => (handle, dimensions),
            QueryResponse::Error { message, .. } => {
                return Err(String::from_utf8_lossy(&message).to_string());
            }
        };

        let columns: Vec<String> = dimensions
            .iter()
            .map(|d| String::from_utf8_lossy(&d.name).to_string())
            .collect();

        let mut all_rows = Vec::new();
        loop {
            let fetch_resp =
                match self
                    .sql_session
                    .fetch(&handle, Projection::All, u64::MAX, rows_orient)
                {
                    Ok(resp) => resp,
                    Err(e) => {
                        let _ = self.sql_session.close(handle);
                        return Err(e.message);
                    }
                };

            match fetch_resp {
                FetchResponse::Data { cells } => all_rows.extend(cells),
                FetchResponse::End => break,
                FetchResponse::Error { message, .. } => {
                    let _ = self.sql_session.close(handle);
                    return Err(String::from_utf8_lossy(&message).to_string());
                }
            }
        }

        let _ = self.sql_session.close(handle);
        Ok((columns, all_rows))
    }
}

impl<'a, T: Transport> crate::api::ServerRelay for RelayParty<'a, T> {
    fn handle_reset(&mut self) -> Result<(), String> {
        RelayParty::handle_reset(self).map_err(|e| e.to_string())
    }
}

impl<'a, T: Transport> Handler for RelayParty<'a, T> {
    fn handle(&mut self, term: ClientTerm) -> ServerTerm {
        match term {
            ClientTerm::Version {
                max_message_size,
                protocol_version,
                lease_ms,
                orientations,
            } => {
                let supported = vec![Orientation::Rows];
                let agreed: Vec<Orientation> = orientations
                    .iter()
                    .copied()
                    .filter(|o| supported.contains(o))
                    .collect();
                if agreed.is_empty() {
                    ServerTerm::Error {
                        kind: ErrorKind::Connection,
                        identity: vec![],
                        message: b"no common orientation".to_vec(),
                    }
                } else {
                    ServerTerm::Version {
                        max_message_size,
                        protocol_version,
                        lease_ms,
                        orientations: agreed,
                    }
                }
            }

            ClientTerm::Query { text } => {
                self.current_text = Some(String::from_utf8_lossy(&text).into_owned());
                let term = self.handle_query(text);
                let input = self.current_text.take();
                self.record_reported_error(&term, input.as_deref());
                term
            }

            ClientTerm::Fetch {
                handle,
                projection,
                count,
                orientation,
            } => {
                let input = self.text_by_handle.get(&handle).cloned();
                let term = self.handle_fetch(handle, projection, count, orientation);
                self.record_reported_error(&term, input.as_deref());
                term
            }

            ClientTerm::Stat { handle } => self.handle_stat(handle),

            ClientTerm::Close { handle } => {
                let input = self.text_by_handle.remove(&handle);
                let term = self.handle_close(handle);
                self.record_reported_error(&term, input.as_deref());
                term
            }

            ClientTerm::Prepare { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Prepare not implemented".to_vec(),
            },

            ClientTerm::Offer { .. } => ServerTerm::Error {
                kind: ErrorKind::Permission,
                identity: vec![],
                message: b"Offer not implemented".to_vec(),
            },
        }
    }
}

pub(crate) use delightql_types::teach_runtime_message;

/// How many statements the text holds when read as a SEQUENCE — `None` when
/// it is not a well-formed one. A diagnostic only: it runs on the failure
/// path, after the term's own entrance has already refused.
fn query_count_if_a_sequence(dql: &str) -> Option<usize> {
    let tree = pipeline::parse::query_sequence(dql).ok()?;
    Some(pipeline::parse::query_spans(&tree).len())
}
