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

use crate::{
    diagnostic::{DelightQLError, ErrorSelector, Runtime},
    host::CompilerHost,
    pipeline::{self, verdict},
    system::ReadySystem,
};
use delightql_protocol::{
    ByteSeq, Cell, ClientTerm, CloseResponse, Dimension, FetchResponse, Handle, Handler, MetaItem,
    Orientation, Projection, QueryHandle, QueryResponse, ReceivedError, ServerTerm, Session,
    Transport, WireError,
};

/// What an execution can fail with: a diagnostic this process minted, or a
/// backend party's occurrence ADMITTED at ingress ([`admitted`]) — never a
/// party's bytes carried raw. Every failure projects to the wire through the
/// one boundary conversion.
pub(crate) type ExecutionFailure = DelightQLError;

/// THE ingress judgment for an error a PARTY answered with. The party's
/// identity bytes are decoded once against the declared tree: a declared
/// occurrence is carried on as that typed fact (`DelightQLError::Received`),
/// a party that refused and named nothing is judged under the one identity
/// the hook road has always used for that case, and an identity this build
/// does not declare is a protocol violation — never an occurrence, and never
/// a family match by the shape of its text.
pub(crate) fn admitted(received: ReceivedError) -> DelightQLError {
    admitted_bytes(received.identity(), received.message())
}

fn admitted_bytes(identity: &[u8], message: &[u8]) -> DelightQLError {
    if identity.is_empty() {
        return Runtime::Bug {
            message: String::from_utf8_lossy(message).into_owned(),
        }
        .into();
    }
    match crate::diagnostic::received(identity, message) {
        Some(occurrence) => occurrence.into(),
        None => Runtime::Protocol {
            message: format!(
                "the party answered with an identity this build does not declare: {} — {}",
                String::from_utf8_lossy(identity),
                String::from_utf8_lossy(message)
            ),
        }
        .into(),
    }
}

/// The one projection of a typed diagnostic onto the wire.
pub(crate) fn error_term(diagnostic: &DelightQLError) -> ServerTerm {
    ServerTerm::Error(WireError::of(diagnostic))
}

/// ONE EXECUTED RESULT, BUFFERED WHOLE: the dimensions the engine elected
/// and the cells they describe, produced together by the road that ran the
/// statement. There is no entrance that takes a name list and rebuilds the
/// descriptors, so a result cannot be re-emitted under a heading its cells
/// were never read under.
pub(crate) struct BufferedResult {
    dimensions: Vec<Dimension>,
    rows: Vec<Vec<Cell>>,
}

impl BufferedResult {
    /// A result no statement produced: the empty relation with no heading.
    fn empty() -> Self {
        BufferedResult {
            dimensions: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// A result the relay COMPOSES rather than reads — a receipt. Every
    /// field's descriptor is stated by the composer beside its name; nothing
    /// is inferred from the cells.
    fn composed(columns: Vec<(String, &'static str)>, rows: Vec<Vec<Cell>>) -> Self {
        let dimensions = columns
            .into_iter()
            .enumerate()
            .map(|(position, (name, descriptor))| Dimension {
                position: position as u64,
                name: name.into_bytes(),
                descriptor: descriptor.as_bytes().to_vec(),
                naming: delightql_protocol::Naming::Authored,
            })
            .collect();
        BufferedResult { dimensions, rows }
    }

    /// A result read on a connection that answers with typed values and no
    /// declared heading: the descriptors are elected from the cells by the
    /// same law the streaming party applies — each column takes the storage
    /// class of its first non-NULL value, and a column with none declares
    /// nothing. A declared type, where the engine reports one, wins.
    fn elected(
        columns: Vec<String>,
        declared: Vec<Option<String>>,
        rows: Vec<Vec<delightql_types::DbValue>>,
    ) -> Self {
        let mut descriptors: Vec<String> = declared
            .into_iter()
            .map(|declared| declared.unwrap_or_default())
            .collect();
        descriptors.resize(columns.len(), String::new());
        for row in &rows {
            for (index, value) in row.iter().enumerate() {
                if descriptors[index].is_empty() {
                    descriptors[index] = value.storage_class().to_string();
                }
            }
            if descriptors.iter().all(|descriptor| !descriptor.is_empty()) {
                break;
            }
        }
        let dimensions = columns
            .into_iter()
            .zip(descriptors)
            .enumerate()
            .map(|(position, (name, descriptor))| Dimension {
                position: position as u64,
                name: name.into_bytes(),
                descriptor: descriptor.into_bytes(),
                naming: delightql_protocol::Naming::Authored,
            })
            .collect();
        let rows = rows
            .into_iter()
            .map(|row| row.into_iter().map(|v| v.into_wire_bytes()).collect())
            .collect();
        BufferedResult { dimensions, rows }
    }

    /// The column names, for a consumer that speaks names alone.
    pub(crate) fn names(&self) -> Vec<String> {
        self.dimensions
            .iter()
            .map(|d| String::from_utf8_lossy(&d.name).into_owned())
            .collect()
    }

    pub(crate) fn rows(&self) -> &[Vec<Cell>] {
        &self.rows
    }

    /// Every row's cells as text, a NULL as `None`.
    pub(crate) fn text_rows(&self) -> Vec<Vec<Option<String>>> {
        self.rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell.as_ref().map(|bytes| String::from_utf8_lossy(bytes).into_owned()))
                    .collect()
            })
            .collect()
    }

    /// The first row, for a probe that reads one cell.
    fn first_row(&self) -> Option<&Vec<Cell>> {
        self.rows.first()
    }

    /// This result under the naming its compilation recorded.
    pub(crate) fn named(
        mut self,
        naming: Option<&[delightql_protocol::Naming]>,
    ) -> Result<Self, DelightQLError> {
        stamp_naming(&mut self.dimensions, naming)?;
        Ok(self)
    }
}

/// Say, per column, what the compilation recorded about who chose its name.
///
/// A party reading an engine sees only the characters the engine answered,
/// so it states every column `Authored`; the compilation is what knows which
/// of them the mint drew, and it answers by position. `None` means the
/// compilation saw no heading, and the party's statement stands. A heading
/// whose width the engine does not match cannot be laid over it, and is
/// refused rather than guessed at.
pub(crate) fn stamp_naming(
    dimensions: &mut [Dimension],
    naming: Option<&[delightql_protocol::Naming]>,
) -> Result<(), DelightQLError> {
    let Some(naming) = naming else {
        return Ok(());
    };
    if naming.len() != dimensions.len() {
        return Err(crate::diagnostic::Internal::invariant(
            "relay heading naming",
            format!(
                "the compiled heading has {} columns; the engine answered {}",
                naming.len(),
                dimensions.len()
            ),
        ));
    }
    for (dimension, naming) in dimensions.iter_mut().zip(naming) {
        dimension.naming = *naming;
    }
    Ok(())
}

/// Buffered eager results for non-streaming connections (bootstrap, imported).
struct EagerBuffer {
    rows: Vec<Vec<Cell>>,
    cursor: usize,
}

impl EagerBuffer {
    fn buffered(result: BufferedResult) -> Self {
        EagerBuffer {
            rows: result.rows,
            cursor: 0,
        }
    }
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
#[cfg(test)]
mod exact_receipt_tests;
#[cfg(test)]
mod linear_admission_tests;
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
    /// The reset-capable system: every compiler operation is reached through
    /// its host, and `handle_reset` through it alone.
    system: &'a mut ReadySystem,
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
    hooks: RelayHooks,
    /// What this session may do to the world, fixed at construction: an
    /// observing session mints observing compilations, and its every
    /// statement is judged by the admission they carry.
    admission: crate::compiler_limits::Admission,
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
    let expected = match crate::pipeline::normalize::declared_error_within(tree, owner?) {
        Ok(Some(expected)) => expected,
        Ok(None) => return None,
        // The hook itself is the typo: that refusal is the answer.
        Err(refusal) => return Some(GoalRefusal::Reported(error_term(&refusal))),
    };
    let actual = error.id();
    if expected.matches(&actual) {
        return Some(GoalRefusal::AsDeclared {
            declared: expected.display(),
            detail: format!("{actual}: {error}"),
        });
    }
    Some(GoalRefusal::Reported(error_term(&unmet_expectation(
        &expected,
        format!(
            "expected error {} but got: {actual}: {error}",
            expected.display()
        ),
    ))))
}

/// The declared expectation did not come: the one diagnostic a hook
/// mismatch reports, whatever the statement did instead.
fn unmet_expectation(expected: &ErrorSelector, outcome: String) -> DelightQLError {
    Runtime::Expectation {
        declared: expected.display(),
        outcome,
    }
    .into()
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
    pub fn new(system: &'a mut ReadySystem, sql_session: Session<T>) -> Self {
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
            hooks: RelayHooks::default(),
            admission: crate::compiler_limits::Admission::Execute,
        }
    }

    /// A session that OBSERVES: a pure statement runs and answers exactly as
    /// it would under `new`; a statement that would execute an effect is
    /// refused before any dispatcher runs. The admission is fixed here and
    /// has no setter — an observing session cannot be talked into executing.
    pub fn observing(system: &'a mut ReadySystem, sql_session: Session<T>) -> Self {
        RelayParty {
            admission: crate::compiler_limits::Admission::Observe,
            ..RelayParty::new(system, sql_session)
        }
    }

    /// One compilation's registry, armed with this session's admission.
    fn mint_registry(&self) -> std::rc::Rc<crate::names::Registry> {
        std::rc::Rc::new(match self.admission {
            crate::compiler_limits::Admission::Execute => crate::names::Registry::new(&[]),
            crate::compiler_limits::Admission::Observe => crate::names::Registry::observing(&[]),
        })
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
    fn read_submission(
        &self,
        dql: &str,
        registry: &std::rc::Rc<crate::names::Registry>,
    ) -> std::result::Result<pipeline::Submission, GoalRefusal> {
        let syntax_error = |error: crate::error::DelightQLError| error_term(&error);
        let tree = match pipeline::parse::submission_attributed(dql, registry.limits().nesting()) {
            Ok(tree) => tree,
            Err(refusal) => {
                if let Some(count) = query_count_if_a_sequence(&refusal.tree) {
                    if count > 1 {
                        return Err(GoalRefusal::Reported(error_term(
                            &crate::diagnostic::Parse::MultiQuery { count }.into(),
                        )));
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
        pipeline::one_submission(normalized)
            .map_err(|error| GoalRefusal::Reported(syntax_error(error)))
    }

    /// A submission of definitions and no goal: admitted as the unnamed
    /// prompt block it is — into `home`, with a block's replacement of an
    /// earlier definition — and answered with what it defined.
    fn admit_definitions(
        &mut self,
        block: crate::pipeline::ast_unresolved::InlineDdlSpec,
    ) -> ServerTerm {
        if let Err(error) = self.admission.admit(
            "a submission of definitions",
            crate::bin_cartridge::ExecutionClass::Effect,
        ) {
            return error_term(&error);
        }
        let defined = crate::pipeline::inline_ddl::prompt_block_entities(&block);
        if let Err(error) =
            crate::pipeline::inline_ddl::register_prompt_blocks([block], self.system)
        {
            return error_term(&error);
        }
        let rows = defined
            .into_iter()
            .map(|(namespace, entity)| {
                vec![Some(namespace.into_bytes()), Some(entity.into_bytes())]
            })
            .collect();
        self.eager_header(BufferedResult::composed(
            vec![
                ("namespace".to_string(), "TEXT"),
                ("entity".to_string(), "TEXT"),
            ],
            rows,
        ))
    }

    /// The runtime's consultation of the file a whole-statement `run!`
    /// runs, under the session's admission.
    fn consult_for_run(&mut self, goal: &crate::pipeline::normalize::Goal) -> crate::error::Result<()> {
        let Some(path) = crate::pipeline::middle::api::run_file(goal) else {
            return Ok(());
        };
        self.admission.admit("run!", crate::bin_cartridge::ExecutionClass::Effect)?;
        self.system.consult_for_run(&path).map(|_| ())
    }

    fn handle_query(&mut self, text: ByteSeq) -> ServerTerm {
        if let Err(error) = self.system.require_healthy() {
            return error_term(&error);
        }

        let dql = match String::from_utf8(text) {
            Ok(s) => s,
            Err(e) => {
                return error_term(
                    &Runtime::Protocol {
                        message: format!("invalid UTF-8 in query text: {}", e),
                    }
                    .into(),
                )
            }
        };

        // ONE reading of the submission. The error hook, the effect-entry
        // classification and the compilation all ask questions about the same
        // goal, and asking the parser three times is how they come to
        // disagree.
        let registry = self.mint_registry();
        let goal = match self.read_submission(&dql, &registry) {
            Ok(pipeline::Submission::Goal(goal)) => goal,
            Ok(pipeline::Submission::Definitions(block)) => return self.admit_definitions(block),
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
            return self.handle_error_hook_query(&dql, goal, expected);
        }

        // Every goal is compiled by the new middle and nowhere else.
        use crate::pipeline::middle::api::{statement, Route};
        let overridden = !self.danger_overrides.is_empty() || !self.option_overrides.is_empty();
        // `run!` consults its file before its statement is compiled.
        if let Err(error) = self.consult_for_run(&goal) {
            return error_term(&error);
        }
        match statement(&mut *self.system, &dql, goal, self.admission, overridden) {
            Route::Query(compiled) => {
                let (term, staged) = self.execute_compiled(compiled);
                self.settle_staged(term, staged)
            }
            Route::Program(plan, trailing) => {
                let term = self.play_plan(&plan);
                self.admit_trailing(term, trailing)
            }
            Route::Refused(error) => error_term(&error),
        }
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
        let crate::pipeline::compiled_query::CompiledQuery {
            primary_sql,
            kind: _,
            obligations,
            prepare_sqls,
            cleanup_sqls: compiled_cleanup,
            connection_id,
            naming,
            trailing,
        } = compiled;
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
                    mut dimensions,
                }) => match stamp_naming(&mut dimensions, naming.as_deref()) {
                    Ok(()) => {
                        let frontend_handle = self.next_handle();
                        self.handles.insert(frontend_handle.clone(), backend_handle);
                        ServerTerm::Header {
                            handle: frontend_handle,
                            dimensions,
                        }
                    }
                    Err(refusal) => {
                        let _ = self.sql_session.close(backend_handle);
                        error_term(&refusal)
                    }
                },
                Ok(QueryResponse::Error(received)) => error_term(&admitted(received)),
                Err(e) => error_term(&Runtime::Transport { message: e.message }.into()),
            }
        } else {
            // Eager path: execute on bootstrap or imported connection, buffer results
            match self
                .execute_sql_routed(&primary_sql, connection_id)
                .and_then(|result| result.named(naming.as_deref()))
            {
                Ok(result) => self.eager_header(result),
                Err(failure) => error_term(&failure),
            }
        };
        (self.admit_trailing(term, trailing), staged)
    }

    /// The statement has run: admit the blocks written after it.
    ///
    /// A refused statement admits none — its program stops at the refusal.
    /// A block that cannot be admitted after a statement that succeeded
    /// is the call's answer, and the statement's handle is closed; the
    /// statement's effects stand, as they would had the block been the
    /// next call.
    pub(super) fn admit_trailing(
        &mut self,
        term: ServerTerm,
        trailing: crate::pipeline::inline_ddl::Trailing,
    ) -> ServerTerm {
        if trailing.is_empty() || matches!(term, ServerTerm::Error(_)) {
            return term;
        }
        match trailing.admit(self.system) {
            Ok(()) => term,
            Err(error) => {
                if let ServerTerm::Header { handle, .. } = &term {
                    let _ = self.handle_close(handle.clone());
                }
                error_term(&error)
            }
        }
    }

    /// Stage what a statement reads. `Err` is the refusal to return.
    fn stage_source(
        &mut self,
        prepare_sqls: &[String],
        connection_id: Option<i64>,
    ) -> std::result::Result<(), ServerTerm> {
        for sql in prepare_sqls {
            if let Err(failure) = self.execute_sql_routed(sql, connection_id) {
                return Err(error_term(&failure));
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
                Ok(result) => {
                    let held = cell_says_yes(result.first_row());
                    if !held {
                        return Some(error_term(&obligation.refusal));
                    }
                }
                Err(failure) => return Some(error_term(&failure)),
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
        expected: &crate::diagnostic::ErrorSelector,
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
            ServerTerm::Error(wire) => judge_wire(expected, wire),
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
        Err(error_term(&unmet_expectation(
            expected,
            match &term {
                ServerTerm::Error(_) => {
                    format!("expected error {} but got: {}", expected.display(), detail)
                }
                _ => "statement succeeded; expected an error".to_string(),
            },
        )))
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
                Ok(FetchResponse::Error(received)) => return Some(error_term(&admitted(received))),
                Err(e) => {
                    return Some(error_term(
                        &Runtime::Transport { message: e.message }.into(),
                    ))
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
        expected: ErrorSelector,
    ) -> ServerTerm {
        let identity = verdict::VerdictIdentity {
            name: None,
            body_text: expected.display(),
        };

        // Every goal is compiled by the new middle and nowhere else; the hook
        // judges its outcome as it judges any other.
        use crate::pipeline::middle::api::{statement, Route};
        let overridden = !self.danger_overrides.is_empty() || !self.option_overrides.is_empty();
        // `run!` consults its file before its statement is compiled; a
        // refused consultation is the statement's outcome.
        let consulted = self.consult_for_run(&goal);
        let term = match consulted.map(|()| statement(&mut *self.system, dql, goal, self.admission, overridden)) {
            Err(error) | Ok(Route::Refused(error)) => Err(error),
            Ok(Route::Query(compiled)) => {
                let (term, staged) = self.execute_compiled(compiled);
                Ok(self.settle_staged(term, staged))
            }
            Ok(Route::Program(plan, trailing)) => {
                let term = self.play_plan(&plan);
                Ok(self.admit_trailing(term, trailing))
            }
        };
        match term {
            Err(error) => {
                let actual = error.id();
                let v = verdict::Verdict {
                    outcome: if expected.matches(&actual) {
                        verdict::VerdictOutcome::Pass
                    } else {
                        verdict::VerdictOutcome::Fail
                    },
                    identity,
                    detail: Some(format!("{}: {}", actual, error)),
                };
                if let Some(ref mut hook) = self.hooks.on_error_hook {
                    hook(&v);
                }
                self.verdict_response(&expected, &v)
            }
            Ok(term) => match self.judge_against_hook(term, &expected, identity) {
                Ok(()) => self.empty_header_response(),
                Err(refusal) => refusal,
            },
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
            None => return error_term(&unknown_handle()),
        };

        let agreed = match self.sql_session.agreed_orientation(orientation) {
            Some(a) => a,
            None => {
                return error_term(
                    &Runtime::Protocol {
                        message: "orientation not agreed".to_string(),
                    }
                    .into(),
                )
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
            Ok(FetchResponse::Error(received)) => error_term(&admitted(received)),
            Err(e) => error_term(&Runtime::Transport { message: e.message }.into()),
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
            return error_term(&unknown_handle());
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
                Ok(CloseResponse::Error(received)) => error_term(&admitted(received)),
                Err(e) => error_term(&Runtime::Transport { message: e.message }.into()),
            },
            None => error_term(&unknown_handle()),
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
        let ServerTerm::Error(wire) = term else {
            return;
        };
        // The term about to leave is this process's own projection of a typed
        // diagnostic; the finding is that occurrence, read back through the
        // one admission judgment rather than as bytes.
        let occurrence = admitted_bytes(wire.identity(), wire.message());
        self.system.record_finding(
            crate::diagnostics::Severity::Error,
            &occurrence.error_uri(),
            &occurrence.to_string(),
            input,
            "session",
        );
    }

    /// Return the appropriate protocol response for an error hook verdict.
    /// Pass → empty header (the hook matched). Fail → the unmet expectation.
    fn verdict_response(&mut self, expected: &ErrorSelector, v: &verdict::Verdict) -> ServerTerm {
        match v.outcome {
            verdict::VerdictOutcome::Pass => self.empty_header_response(),
            verdict::VerdictOutcome::Fail => error_term(&unmet_expectation(
                expected,
                format!(
                    "expected error {} but got: {}",
                    expected.display(),
                    v.detail.as_deref().unwrap_or("Error hook verdict: FAIL")
                ),
            )),
        }
    }

    fn empty_header_response(&mut self) -> ServerTerm {
        self.eager_header(BufferedResult::empty())
    }

    // --- Connection routing ---

    /// Execute SQL eagerly through the host's session-catalog capability.
    fn execute_eager_on_bootstrap(&self, sql: &str) -> Result<BufferedResult, ExecutionFailure> {
        let result = self.system.query_session_catalog(sql)?;
        Ok(BufferedResult::elected(
            result.columns,
            result.declared,
            result.rows,
        ))
    }

    /// Execute SQL eagerly on an imported connection (connection_id >= 3).
    fn execute_eager_on_imported(
        &self,
        sql: &str,
        connection_id: i64,
    ) -> Result<BufferedResult, ExecutionFailure> {
        let conn_arc = self.system.get_connection(connection_id)?;
        let conn_guard = conn_arc
            .lock()
            .map_err(|e| Runtime::poisoned(format!("Connection {} lock", connection_id), e))?;
        let (columns, rows) = conn_guard.query_all_rows(sql, &[])?;
        // The connection trait reports names and typed values, never a
        // declared heading: every column elects.
        let declared = vec![None; columns.len()];
        Ok(BufferedResult::elected(columns, declared, rows))
    }

    /// Execute SQL on the appropriate connection based on connection_id.
    ///
    /// - `None` or `2`: route through the streaming backend protocol (sql_session)
    /// - `1`: execute eagerly on the bootstrap connection
    /// - `>= 3`: execute eagerly on an imported connection
    pub(crate) fn execute_sql_routed(
        &mut self,
        sql: &str,
        connection_id: Option<i64>,
    ) -> Result<BufferedResult, ExecutionFailure> {
        match connection_id.unwrap_or(2) {
            2 => self.execute_eager_through_protocol(sql),
            1 => self.execute_eager_on_bootstrap(sql),
            id => self.execute_eager_on_imported(sql, id),
        }
    }

    /// Execute SQL through the backend protocol and buffer the whole result
    /// under the dimensions the party elected for it.
    fn execute_eager_through_protocol(
        &mut self,
        sql: &str,
    ) -> Result<BufferedResult, ExecutionFailure> {
        let rows_orient = self
            .sql_session
            .agreed_orientation(Orientation::Rows)
            .ok_or_else(|| {
                DelightQLError::from(Runtime::Protocol {
                    message: "Rows orientation not agreed".to_string(),
                })
            })?;

        let resp = self
            .sql_session
            .query(sql.as_bytes().to_vec())
            .map_err(|e| DelightQLError::from(Runtime::Transport { message: e.message }))?;

        let (handle, dimensions) = match resp {
            QueryResponse::Header { handle, dimensions } => (handle, dimensions),
            QueryResponse::Error(received) => return Err(admitted(received)),
        };

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
                        return Err(Runtime::Transport { message: e.message }.into());
                    }
                };

            match fetch_resp {
                FetchResponse::Data { cells } => all_rows.extend(cells),
                FetchResponse::End => break,
                FetchResponse::Error(received) => {
                    let _ = self.sql_session.close(handle);
                    return Err(admitted(received));
                }
            }
        }

        let _ = self.sql_session.close(handle);
        Ok(BufferedResult {
            dimensions,
            rows: all_rows,
        })
    }
}

impl<'a, T: Transport> crate::api::ServerRelay for RelayParty<'a, T> {
    fn handle_reset(&mut self) -> Result<(), crate::error::DelightQLError> {
        RelayParty::handle_reset(self)
    }

    fn set_session_setting(
        &mut self,
        key: &str,
        value: Option<&str>,
    ) -> Result<(), crate::error::DelightQLError> {
        self.system.set_session_setting(key, value)
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
                if let Some(refusal) = delightql_protocol::version_refusal(&protocol_version) {
                    return ServerTerm::Error(delightql_protocol::WireError::of(&refusal));
                }
                let supported = vec![Orientation::Rows];
                let agreed: Vec<Orientation> = orientations
                    .iter()
                    .copied()
                    .filter(|o| supported.contains(o))
                    .collect();
                if agreed.is_empty() {
                    error_term(
                        &Runtime::Protocol {
                            message: "no common orientation".to_string(),
                        }
                        .into(),
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

            ClientTerm::Prepare { .. } => error_term(
                &Runtime::Unsupported {
                    message: "Prepare not implemented".to_string(),
                }
                .into(),
            ),

            ClientTerm::Offer { .. } => error_term(
                &Runtime::Unsupported {
                    message: "Offer not implemented".to_string(),
                }
                .into(),
            ),
        }
    }
}

/// The relay's own "no such handle": the client named a result this session
/// does not hold, which is a protocol fault of the conversation.
fn unknown_handle() -> DelightQLError {
    Runtime::Protocol {
        message: "unknown handle".to_string(),
    }
    .into()
}

/// Judge a selector against an error term: the term's bytes pass the same
/// ingress judgment as any received error ([`admitted`]) and only the typed
/// occurrence that judgment yields is matched — so an undeclared identity
/// can satisfy no family, whatever prefix its text wears.
fn judge_wire(expected: &ErrorSelector, wire: &WireError) -> (bool, String) {
    judge_bytes(expected, wire.identity(), wire.message())
}

fn judge_bytes(expected: &ErrorSelector, identity: &[u8], message: &[u8]) -> (bool, String) {
    let occurrence = admitted_bytes(identity, message);
    (
        expected.matches(&occurrence.id()),
        format!("{}: {}", occurrence.id(), occurrence),
    )
}

/// How many statements a refused submission's goal text holds when read as a
/// SEQUENCE — `None` when it is not a well-formed one. A diagnostic only: it
/// runs on the failure path, after the term's own entrance has already
/// refused.
///
/// The goal text is what follows the submission's leading goal marker: a host
/// that wraps what a user typed at a prompt writes ONE marker, in front of
/// everything typed, so two queries typed together stand behind one marker.
fn query_count_if_a_sequence(refused: &crate::pipeline::syntax::SyntaxTree) -> Option<usize> {
    let source = refused.source();
    let goal_text = match refused.entrance() {
        crate::pipeline::syntax::Root::QuerySequence => source,
        crate::pipeline::syntax::Root::DefinitionFile
        | crate::pipeline::syntax::Root::CompanionCell => {
            let marker = refused.tokens().into_iter().find(|token| !token.extra)?;
            if marker.text != "?-" {
                return None;
            }
            &source[marker.end..]
        }
    };
    let tree = pipeline::parse::query_sequence(goal_text).ok()?;
    Some(pipeline::parse::query_spans(&tree).len())
}
